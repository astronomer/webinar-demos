import os
import requests
from airflow.sdk import dag, task, Param, Asset

_DEPLOYMENT_API_TOKEN = os.getenv("DEPLOYMENT_API_TOKEN", "")
_AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "")

_IS_ASTRO = bool(_DEPLOYMENT_API_TOKEN and _AIRFLOW_HOST)

_USERNAME = "admin"
_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
_LOCAL_HOST = "http://api-server:8080"

SOURCE_DAG_ID = "ai_support_ticket_system"


def _get_host() -> str:
    if _IS_ASTRO:
        host = _AIRFLOW_HOST.rstrip("/")
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        return host
    return _LOCAL_HOST


def _get_jwt_token() -> str:
    token_url = f"{_LOCAL_HOST}/auth/token"
    payload = {"username": _USERNAME, "password": _PASSWORD}
    headers = {"Content-Type": "application/json"}
    response = requests.post(token_url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    token = response.json().get("access_token")
    return token


def _get_auth_headers() -> dict:
    if _IS_ASTRO:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {_DEPLOYMENT_API_TOKEN}",
        }
    return {"Authorization": f"Bearer {_get_jwt_token()}"}


def make_api_request(endpoint: str) -> dict:
    url = f"{_get_host()}/api/v2{endpoint}"
    headers = _get_auth_headers()
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


@dag(
    schedule=[Asset("new_decision")],
    params={
        "dag_id": Param(
            type="string",
            default=SOURCE_DAG_ID,
            description="The dag_id to query decision traces from",
        ),
    },
)
def gathering_decision_trace():

    @task
    def get_latest_dag_run_id(dag_id: str) -> str:
        endpoint = f"/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1"
        result = make_api_request(endpoint)
        dag_runs = result.get("dag_runs", [])
        dag_run_id = dag_runs[0].get("dag_run_id")
        print(f"Latest dag_run_id for {dag_id}: {dag_run_id}")
        return dag_run_id

    @task
    def query_decisions(dag_id: str, dag_run_id: str) -> list:
        endpoint = f"/dags/{dag_id}/dagRuns/{dag_run_id}/hitlDetails"
        result = make_api_request(endpoint)

        hitl_details_list = []
        for hitl in result.get("hitl_details", []):
            task_instance = hitl.get("task_instance", {})
            hitl_details_list.append({
                "task_id": task_instance.get("task_id"),
                "map_index": task_instance.get("map_index"),
                "chosen_options": hitl.get("chosen_options", []),
                "params_input": hitl.get("params_input", {}),
                "responded_at": hitl.get("responded_at"),
                "responded_by_user": hitl.get("responded_by_user"),
                "response_received": hitl.get("response_received"),
                "subject": hitl.get("subject"),
                "body": hitl.get("body"),
                "options": hitl.get("options"),
            })

        print(f"Retrieved {len(hitl_details_list)} HITL decisions: {hitl_details_list}")
        return hitl_details_list

    @task
    def compile_decision_trace(dag_id: str, dag_run_id: str, hitl_decisions: list) -> str:
        from include.custom_functions import save_as_markdown

        decision_trace = {
            "source_dag": dag_id,
            "dag_run_id": dag_run_id,
            "hitl_decisions": [
                {
                    "task_id": hitl.get("task_id"),
                    "subject": hitl.get("subject"),
                    "body": hitl.get("body"),
                    "options": hitl.get("options"),
                    "chosen_options": hitl.get("chosen_options"),
                    "params_input": hitl.get("params_input", {}),
                    "responded_at": hitl.get("responded_at"),
                    "responded_by_user": hitl.get("responded_by_user"),
                    "response_received": hitl.get("response_received"),
                }
                for hitl in hitl_decisions
            ],
        }

        file_path = save_as_markdown(decision_trace)
        print(f"Decision trace written to {file_path}")

        return file_path

    dag_id = "{{ params.dag_id }}"
    dag_run_id = get_latest_dag_run_id(dag_id=dag_id)
    hitl_decisions = query_decisions(dag_id=dag_id, dag_run_id=dag_run_id)

    compile_decision_trace(dag_id=dag_id, dag_run_id=dag_run_id, hitl_decisions=hitl_decisions)


gathering_decision_trace()
