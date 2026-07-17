"""Token Console: an Airflow plugin that visualizes LLM token usage and cost.

The Common AI provider does not push token usage to XCom and reports no dollar
figure. It emits one structured log line per AI task:

    LLM run complete: model=<name>, requests=<n>, tool_calls=<n>,
        input_tokens=<n>, output_tokens=<n>, total_tokens=<n>

This plugin reads recent runs of the demo DAGs via the Airflow REST API, scrapes
that line from each task instance's logs (across retries), applies its own price
table (the provider gives tokens, you attach pricing), and serves a small
self-contained dashboard under a "Token Console" nav entry.

Production note: with OpenTelemetry (`[common.ai] otel_export_enabled = True`) the
same usage is emitted as `gen_ai.aggregated_usage.*` spans, which is the cleaner
structured source when a tracing backend is configured. This demo uses log
scraping so it works with zero setup.
"""

from __future__ import annotations

import asyncio
import os
import re
import threading
import time
from pathlib import Path

import requests
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent

# DAGs whose LLM token usage we track.
DAG_IDS = ["ask_astrotrips", "agent_investigation", "retry_policy_demo"]
RUNS_PER_DAG = 5

# Cost estimates come from Pydantic's genai-prices dataset (bundled, offline).
try:
    from genai_prices import Usage as _GenaiUsage, calc_price as _calc_price
    _GENAI_OK = True
except Exception:
    _GenaiUsage = _calc_price = None
    _GENAI_OK = False

# Provider to attribute usage to when pricing (this demo uses openai:gpt-5-mini).
PRICING_PROVIDER = os.environ.get("TOKEN_CONSOLE_PROVIDER", "openai")

AIRFLOW_HOST = os.environ.get("TOKEN_CONSOLE_HOST", "http://localhost:8080").rstrip("/")
AIRFLOW_USER = os.environ.get("TOKEN_CONSOLE_USERNAME", "admin")
AIRFLOW_PASS = os.environ.get("TOKEN_CONSOLE_PASSWORD", "admin")
AIRFLOW_TOKEN = os.environ.get("TOKEN_CONSOLE_TOKEN")  # Astro: Deployment API token

_LOG_RE = re.compile(
    r"LLM run complete: model=(?P<model>.+?), requests=(?P<requests>\d+|None), "
    r"tool_calls=(?P<tool_calls>\d+|None), input_tokens=(?P<input>\d+|None), "
    r"output_tokens=(?P<output>\d+|None), total_tokens=(?P<total>\d+|None)"
)

_cached_token: str | None = None
_token_expires_at: float = 0.0
_token_lock = threading.Lock()


def _fetch_fresh_token() -> str:
    resp = requests.post(
        f"{AIRFLOW_HOST}/auth/token",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_token() -> str:
    if AIRFLOW_TOKEN:
        return AIRFLOW_TOKEN
    global _cached_token, _token_expires_at
    now = time.monotonic()
    if _cached_token and now < _token_expires_at:
        return _cached_token
    with _token_lock:
        if _cached_token and now < _token_expires_at:
            return _cached_token
        _cached_token = _fetch_fresh_token()
        _token_expires_at = now + 55 * 60
    return _cached_token


def _int(value: str) -> int:
    return 0 if value == "None" else int(value)


def _cost(model: str, inp: int, out: int) -> float:
    """Estimate USD via genai-prices. 0.0 if the model is unknown or lib missing."""
    if not _GENAI_OK or not model:
        return 0.0
    ref = model.split(":")[-1]  # strip any "openai:" style provider prefix
    try:
        pd = _calc_price(_GenaiUsage(input_tokens=inp, output_tokens=out),
                         model_ref=ref, provider_id=PRICING_PROVIDER)
        return round(float(pd.total_price), 6)
    except Exception:
        return 0.0


def _log_text(content) -> str:
    """The REST log endpoint returns {"content": [{"event": "..."}, ...]} or a string."""
    if isinstance(content, list):
        return "\n".join(
            (e.get("event", "") if isinstance(e, dict) else str(e)) for e in content
        )
    return str(content or "")


def _collect_usage() -> dict:
    """Blocking: walk recent runs via /api/v2, scrape the token line, aggregate."""
    base = f"{AIRFLOW_HOST}/api/v2"
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {_get_token()}"

    def _get(path: str, **params):
        r = session.get(f"{base}{path}", params=params or None, timeout=20)
        r.raise_for_status()
        return r.json()

    runs_out: list[dict] = []
    for dag_id in DAG_IDS:
        try:
            dag_runs = _get(f"/dags/{dag_id}/dagRuns", limit=RUNS_PER_DAG, order_by="-start_date")
        except Exception:
            continue  # DAG never ran yet, or not found
        for run in dag_runs.get("dag_runs", []):
            run_id = run["dag_run_id"]
            try:
                tis = _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances").get("task_instances", [])
            except Exception:
                tis = []
            tasks_out: list[dict] = []
            for ti in tis:
                tries = ti.get("try_number") or 0
                agg = {"input": 0, "output": 0, "total": 0, "requests": 0,
                       "tool_calls": 0, "model": None, "calls": 0}
                for attempt in range(1, tries + 1):
                    try:
                        log = _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{ti['task_id']}/logs/{attempt}")
                    except Exception:
                        continue
                    for m in _LOG_RE.finditer(_log_text(log.get("content"))):
                        agg["model"] = m.group("model")
                        agg["input"] += _int(m.group("input"))
                        agg["output"] += _int(m.group("output"))
                        agg["total"] += _int(m.group("total"))
                        agg["requests"] += _int(m.group("requests"))
                        agg["tool_calls"] += _int(m.group("tool_calls"))
                        agg["calls"] += 1
                if agg["calls"]:
                    tasks_out.append({
                        "task_id": ti["task_id"],
                        "model": agg["model"],
                        "input_tokens": agg["input"],
                        "output_tokens": agg["output"],
                        "total_tokens": agg["total"],
                        "requests": agg["requests"],
                        "tool_calls": agg["tool_calls"],
                        "cost_usd": _cost(agg["model"] or "", agg["input"], agg["output"]),
                    })
            if tasks_out:
                runs_out.append({
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "state": str(run.get("state") or "").lower(),
                    "start_date": str(run.get("start_date") or ""),
                    "tasks": tasks_out,
                })

    runs_out.sort(key=lambda r: r["start_date"])
    total_in = sum(t["input_tokens"] for r in runs_out for t in r["tasks"])
    total_out = sum(t["output_tokens"] for r in runs_out for t in r["tasks"])
    total_cost = round(sum(t["cost_usd"] for r in runs_out for t in r["tasks"]), 6)
    return {
        "totals": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "total_tokens": total_in + total_out,
            "cost_usd": total_cost,
            "runs": len(runs_out),
        },
        "runs": runs_out,
        "pricing": {"source": "pydantic genai-prices", "available": _GENAI_OK, "provider": PRICING_PROVIDER},
    }


app = FastAPI(title="Token Console")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((BASE_DIR / "static" / "index.html").read_text())


@app.get("/api/usage")
async def usage() -> JSONResponse:
    try:
        data = await asyncio.to_thread(_collect_usage)
    except Exception as exc:  # surface auth/connection problems to the UI
        raise HTTPException(status_code=500, detail=str(exc))
    return JSONResponse(data)


class TokenConsolePlugin(AirflowPlugin):
    name = "token_console"

    fastapi_apps = [{
        "app": app,
        "url_prefix": "/token-console",
        "name": "Token Console",
    }]
    external_views = [{
        "name": "Token",
        "href": "token-console/",
        "destination": "nav",
        "url_route": "token-console",
        "icon": "/token-console/static/icon.svg",
        "nav_top_level": True,
    }]
