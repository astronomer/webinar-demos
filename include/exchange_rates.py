"""Client for the exchange-rate API, shared by every pipeline version.

It raises a different exception for each failure mode so a retry policy (v4) can treat a
rate limit and a rejected API key differently. Otherwise both are plain HTTP errors.
"""

from __future__ import annotations

from airflow.providers.http.hooks.http import HttpHook


class RateLimitedError(Exception):
    """The API asked us to slow down (HTTP 429)."""


class ForbiddenError(Exception):
    """The API rejected our credentials (HTTP 403)."""


def fetch_rates() -> dict[str, float]:
    """Return how many units of each currency one US dollar buys."""
    response = HttpHook(method="GET", http_conn_id="exchange_rates_api").run(
        "/rates", extra_options={"check_response": False}
    )
    if response.status_code == 429:
        raise RateLimitedError("Exchange-rate API: rate limit exceeded (HTTP 429)")
    if response.status_code == 403:
        raise ForbiddenError("Exchange-rate API: API key rejected (HTTP 403)")
    response.raise_for_status()
    return response.json()["rates"]
