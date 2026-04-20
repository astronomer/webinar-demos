import logging
import os
import requests
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from fastapi import FastAPI

logger = logging.getLogger(__name__)

LAMBDA_URL = os.environ.get("HITL_LAMBDA_URL", "")
WEBHOOK_SECRET = os.environ.get("HITL_WEBHOOK_SECRET", "")

app = FastAPI(title="HITL Slack Plugin", version="1.0.0")


@app.get("/ping")
async def ping():
    return {
        "message": "pong",
        "lambda_url": LAMBDA_URL[:50] + "..." if LAMBDA_URL else None,
        "webhook_secret_configured": bool(WEBHOOK_SECRET),
    }


@app.post("/test-lambda")
async def test_lambda():
    if not LAMBDA_URL:
        return {"error": "HITL_LAMBDA_URL not configured"}
    if not WEBHOOK_SECRET:
        return {"error": "HITL_WEBHOOK_SECRET not configured"}

    test_payload = {
        "dag_id": "test_dag",
        "dag_run_id": "test_run_123",
        "task_id": "test_task",
        "subject": "Test HITL Message",
        "body": "This is a test from the Airflow plugin",
        "options": ["Option A", "Option B"],
        "params": {},
    }

    headers = {"X-Webhook-Secret": WEBHOOK_SECRET, "Content-Type": "application/json"}

    try:
        resp = requests.post(
            f"{LAMBDA_URL}/from-airflow", json=test_payload, headers=headers, timeout=10
        )
        return {
            "message": "Sent to Lambda",
            "lambda_status": resp.status_code,
            "lambda_response": resp.json(),
        }
    except requests.RequestException as e:
        return {"error": f"Failed to call Lambda: {e}"}


def send_hitl_to_lambda(dag_id, dag_run_id, task_id, subject, body, options, params):
    logger.info(
        "send_hitl_to_lambda called: dag=%s task=%s LAMBDA_URL=%s WEBHOOK_SECRET=%s",
        dag_id,
        task_id,
        bool(LAMBDA_URL),
        bool(WEBHOOK_SECRET),
    )
    if not LAMBDA_URL or not WEBHOOK_SECRET:
        logger.warning(
            "Lambda not configured, skipping notification for %s/%s", dag_id, task_id
        )
        return

    payload = {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "task_id": task_id,
        "subject": subject,
        "body": body,
        "options": options,
        "params": params or {},
    }

    headers = {"X-Webhook-Secret": WEBHOOK_SECRET, "Content-Type": "application/json"}

    try:
        resp = requests.post(
            f"{LAMBDA_URL}/from-airflow", json=payload, headers=headers, timeout=10
        )
        logger.info("Sent to Lambda: %s", resp.status_code)
    except requests.RequestException as e:
        logger.error("Failed to send to Lambda: %s", e)


class HITLSlackListener:
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance):
        try:
            logger.info(
                "HITL Listener triggered: dag=%s task=%s previous_state=%s",
                task_instance.dag_id,
                task_instance.task_id,
                previous_state,
            )

            state_value = getattr(previous_state, "value", str(previous_state)).lower()
            if state_value != "queued":
                logger.info("Skipping - previous_state=%s (not queued)", previous_state)
                return

            context = task_instance.get_template_context()
            task = context.get("task")

            hitl_operators = {
                "HITLOperator",
                "ApprovalOperator",
                "HITLBranchOperator",
                "HITLEntryOperator",
            }
            task_class_name = type(task).__name__
            logger.info("Task class: %s", task_class_name)

            if task_class_name not in hitl_operators:
                logger.info("Skipping - not a HITL operator: %s", task_class_name)
                return

            logger.info(
                "Detected HITL task: %s/%s, previous_state=%s",
                task_instance.dag_id,
                task_instance.task_id,
                previous_state,
            )

            raw_params = getattr(task, "params", {})
            if hasattr(raw_params, "dump"):
                params = raw_params.dump()
            elif hasattr(raw_params, "to_dict"):
                params = raw_params.to_dict()
            else:
                try:
                    params = dict(raw_params) if raw_params else {}
                except (TypeError, ValueError):
                    params = {}

            send_hitl_to_lambda(
                dag_id=task_instance.dag_id,
                dag_run_id=task_instance.run_id,
                task_id=task_instance.task_id,
                subject=task.subject,
                body=task.body,
                options=list(task.options),
                params=params,
            )

        except Exception as e:
            logger.exception("Error in listener: %s", e)


class HITLSlackPlugin(AirflowPlugin):
    name = "hitl_slack_plugin"

    fastapi_apps = [
        {"app": app, "url_prefix": "/hitl-slack", "name": "HITL Slack Plugin"}
    ]

    listeners = [HITLSlackListener()]
