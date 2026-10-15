"""Fake exchange-rate API for the ETL/ELT demo. Standard library only.

GET  /rates        {"base": "USD", "rates": {...}}: units of each currency one US dollar buys
GET  /mode         the current mode
POST /mode/<mode>  switch modes (used by ./demo.sh api-mode)

Modes:
    ok       /rates always answers 200
    flaky    the next two calls to /rates answer 429, then the mode goes back to ok
    bad-key  every call to /rates answers 403 until the mode changes
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Must match include/demo_tools/generate_orders.py and warehouse/init.sql.
RATES = {"USD": 1.0, "GBP": 0.75, "EUR": 0.86, "JPY": 151.2, "CAD": 1.39, "AUD": 1.53, "INR": 88.6}
MODES = ("ok", "flaky", "bad-key")
FLAKY_FAILURES = 2

_lock = threading.Lock()
_state = {"mode": "ok", "failures_left": 0}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?")[0]
        if path == "/mode":
            self._send(200, {"mode": _state["mode"]})
            return
        if path != "/rates":
            self._send(404, {"error": "not found"})
            return

        with _lock:
            mode = _state["mode"]
            if mode == "flaky":
                _state["failures_left"] -= 1
                if _state["failures_left"] <= 0:
                    _state["mode"] = "ok"

        if mode == "flaky":
            self._send(429, {"error": "rate limit exceeded"}, {"Retry-After": "5"})
        elif mode == "bad-key":
            self._send(403, {"error": "invalid API key"})
        else:
            self._send(200, {"base": "USD", "rates": RATES})

    def do_POST(self) -> None:
        parts = self.path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "mode" or parts[1] not in MODES:
            self._send(400, {"error": f"use POST /mode/<{'|'.join(MODES)}>"})
            return
        with _lock:
            _state["mode"] = parts[1]
            _state["failures_left"] = FLAKY_FAILURES if parts[1] == "flaky" else 0
        self._send(200, {"mode": parts[1]})

    def _send(self, status: int, body: dict, headers: dict[str, str] | None = None) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    print("fake exchange-rate API listening on :8000")
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
