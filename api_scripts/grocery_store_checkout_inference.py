import os
import requests

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

_LOCAL_HOST = os.getenv("AIRFLOW_HOST", "http://localhost:8080")
_LOCAL_USERNAME = "admin"
_LOCAL_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

_ASTRO_HOST = os.getenv("ASTRO_HOST")
_DEPLOYMENT_API_TOKEN = os.getenv("DEPLOYMENT_API_TOKEN")

DAG_ID = "inference_dag"


def _get_headers() -> dict:
    if ENVIRONMENT == "astro":
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {_DEPLOYMENT_API_TOKEN}",
        }

    token_url = f"{_LOCAL_HOST}/auth/token"
    response = requests.post(
        token_url,
        json={"username": _LOCAL_USERNAME, "password": _LOCAL_PASSWORD},
        headers={"Content-Type": "application/json"},
    )
    token = response.json().get("access_token")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }


def _get_base_url() -> str:
    if ENVIRONMENT == "astro":
        return _ASTRO_HOST
    return _LOCAL_HOST


def trigger_dag(partial_grocery_list: str) -> dict:
    base_url = _get_base_url()
    url = f"{base_url}/api/v2/dags/{DAG_ID}/dagRuns"
    headers = _get_headers()
    headers["Content-Type"] = "application/json"

    from datetime import datetime, timezone

    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": {
            "partial_grocery_list": partial_grocery_list,
        },
    }

    response = requests.post(url, json=payload, headers=headers)
    if not response.ok:
        print(f"Error {response.status_code}: {response.text}")
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    result = trigger_dag("tofu, soy sauce, rice, mozzarella, tomatoes")
    print(result)
