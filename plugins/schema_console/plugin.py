"""Schema: a static ER diagram of the AstroTrips warehouse.

A companion to the Token Console with the same look and feel. No data access --
it renders the fixed schema (tables, columns, PK/FK relationships) defined in
include/sql/schema.sql as a self-contained diagram under a "Schema" nav item.
"""

from __future__ import annotations

from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent

app = FastAPI(title="Schema")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((BASE_DIR / "static" / "index.html").read_text())


class SchemaPlugin(AirflowPlugin):
    name = "schema_console"

    fastapi_apps = [{
        "app": app,
        "url_prefix": "/schema-console",
        "name": "Schema",
    }]
    external_views = [{
        "name": "Schema",
        "href": "schema-console/",
        "destination": "nav",
        "url_route": "schema-console",
        "icon": "/schema-console/static/icon.svg",
        "nav_top_level": True,
    }]
