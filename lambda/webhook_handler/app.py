import hashlib
import hmac
import json
import logging
import os
import time
from urllib.parse import parse_qs, quote
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ASTRO_API_URL = os.environ.get("ASTRO_API_URL", "")
ASTRO_API_TOKEN = os.environ.get("ASTRO_API_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "")


def markdown_to_mrkdwn(text):
    import re

    if not text:
        return text
    result = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
    result = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"_\1_", result)
    result = re.sub(r"^-{3,}$", "─" * 30, result, flags=re.MULTILINE)
    result = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", result, flags=re.MULTILINE)
    return result


def lambda_handler(event, context):
    path = event.get("path", "")
    http_method = event.get("httpMethod", "")

    logger.info("Received request: %s %s", http_method, path)

    if path == "/ping" and http_method == "GET":
        return handle_ping(event)
    elif path == "/from-airflow" and http_method == "POST":
        return handle_from_airflow(event)
    elif path == "/from-slack" and http_method == "POST":
        return handle_from_slack(event)
    else:
        return response(404, {"error": "Not found", "path": path})


def handle_ping(event):
    is_valid, error_msg = verify_webhook_secret(event)
    if not is_valid:
        return response(401, {"error": "Unauthorized", "detail": error_msg})

    return response(
        200,
        {
            "message": "pong",
            "astro_configured": bool(ASTRO_API_URL and ASTRO_API_TOKEN),
            "webhook_secret_configured": bool(WEBHOOK_SECRET),
            "slack_configured": bool(
                SLACK_SIGNING_SECRET and SLACK_BOT_TOKEN and SLACK_CHANNEL
            ),
        },
    )


def verify_webhook_secret(event):
    if not WEBHOOK_SECRET:
        return False, "WEBHOOK_SECRET not configured on server"

    headers = event.get("headers", {})
    provided_secret = headers.get("X-Webhook-Secret") or headers.get("x-webhook-secret")

    if not provided_secret:
        return False, "Missing X-Webhook-Secret header"

    if provided_secret != WEBHOOK_SECRET:
        return False, "Invalid webhook secret"

    return True, None


def verify_slack_signature(event):
    if not SLACK_SIGNING_SECRET:
        return False, "SLACK_SIGNING_SECRET not configured on server"

    headers = event.get("headers", {})
    body = event.get("body", "")

    slack_signature = headers.get("X-Slack-Signature") or headers.get(
        "x-slack-signature"
    )
    slack_timestamp = headers.get("X-Slack-Request-Timestamp") or headers.get(
        "x-slack-request-timestamp"
    )

    if not slack_signature or not slack_timestamp:
        return False, "Missing Slack signature headers"

    try:
        timestamp_int = int(slack_timestamp)
        if abs(time.time() - timestamp_int) > 60 * 5:
            return False, "Slack request timestamp too old"
    except ValueError:
        return False, "Invalid timestamp"

    sig_basestring = f"v0:{slack_timestamp}:{body}"

    my_signature = (
        "v0="
        + hmac.new(
            SLACK_SIGNING_SECRET.encode(), sig_basestring.encode(), hashlib.sha256
        ).hexdigest()
    )

    if not hmac.compare_digest(my_signature, slack_signature):
        return False, "Invalid Slack signature"

    return True, None


def check_hitl_response_received(dag_id, dag_run_id, task_id, map_index=-1):
    if not ASTRO_API_URL or not ASTRO_API_TOKEN:
        logger.warning("Astro API not configured, cannot check HITL status")
        return False

    url = f"{ASTRO_API_URL}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(dag_run_id, safe='')}/taskInstances/{quote(task_id, safe='')}/{map_index}/hitlDetails"
    headers = {
        "Authorization": f"Bearer {ASTRO_API_TOKEN}",
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            response_received = data.get("response_received", False)
            logger.debug(
                "HITL status for %s/%s: response_received=%s",
                dag_id,
                task_id,
                response_received,
            )
            return response_received
        else:
            logger.warning(
                "Failed to get HITL status: %s %s", resp.status_code, resp.text
            )
            return False
    except requests.RequestException as e:
        logger.error("Error checking HITL status: %s", e)
        return False


def handle_from_airflow(event):
    is_valid, error_msg = verify_webhook_secret(event)
    if not is_valid:
        return response(401, {"error": "Unauthorized", "detail": error_msg})

    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return response(400, {"error": "Invalid JSON"})

    dag_id = body.get("dag_id")
    dag_run_id = body.get("dag_run_id")
    task_id = body.get("task_id")
    subject = body.get("subject", "Action Required")
    hitl_body = body.get("body", "")
    options = body.get("options", [])
    params = body.get("params", {})

    if not all([dag_id, dag_run_id, task_id]):
        return response(
            400,
            {
                "error": "Missing required fields",
                "required": ["dag_id", "dag_run_id", "task_id"],
            },
        )

    if not options:
        return response(400, {"error": "options cannot be empty"})

    if check_hitl_response_received(dag_id, dag_run_id, task_id):
        logger.info(
            "HITL already has response, skipping Slack notification for %s/%s",
            dag_id,
            task_id,
        )
        return response(
            200,
            {
                "message": "Skipped - HITL already has response",
                "dag_id": dag_id,
                "task_id": task_id,
            },
        )

    result = send_slack_message(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        subject=subject,
        body=hitl_body,
        options=options,
        params=params,
    )

    return response(result.get("status_code", 200), result)


def handle_from_slack(event):
    logger.debug("handle_from_slack called")

    is_valid, error_msg = verify_slack_signature(event)
    if not is_valid:
        logger.warning("Slack signature verification failed: %s", error_msg)
        return response(401, {"error": "Unauthorized", "detail": error_msg})

    raw_body = event.get("body", "")
    try:
        parsed = parse_qs(raw_body)
        payload_json = parsed.get("payload", ["{}"])[0]
        payload = json.loads(payload_json)
    except (json.JSONDecodeError, KeyError) as e:
        return response(400, {"error": f"Invalid Slack payload: {e}"})

    interaction_type = payload.get("type")
    logger.debug("Interaction type: %s", interaction_type)

    if interaction_type == "block_actions":
        return handle_button_click(payload)
    elif interaction_type == "view_submission":
        return handle_modal_submit(payload)
    else:
        return response(
            400, {"error": f"Unsupported interaction type: {interaction_type}"}
        )


def handle_button_click(payload):
    actions = payload.get("actions", [])
    if not actions:
        return response(400, {"error": "No actions in payload"})

    action = actions[0]
    action_value = action.get("value", "")

    try:
        parts = action_value.split("|", 5)
        if len(parts) < 5:
            raise ValueError(f"Expected at least 5 parts, got {len(parts)}")

        dag_id = parts[0]
        dag_run_id = parts[1]
        task_id = parts[2]
        chosen_option = parts[3]
        metadata_json = parts[4]
        params_json = parts[5] if len(parts) > 5 else ""

        try:
            metadata = json.loads(metadata_json)
        except json.JSONDecodeError:
            metadata = {}

        subject = metadata.get("subject", "")
        body = metadata.get("body", "")

    except ValueError as e:
        return response(400, {"error": f"Invalid action value format: {e}"})

    user = payload.get("user", {})
    user_name = user.get("name", "Someone")
    trigger_id = payload.get("trigger_id")
    response_url = payload.get("response_url")

    logger.info(
        "Button click: dag_id=%s, option=%s, has_params=%s",
        dag_id,
        chosen_option,
        bool(params_json),
    )

    if params_json:
        try:
            params = json.loads(params_json)
        except json.JSONDecodeError:
            params = {}

        if params:
            return open_params_modal(
                trigger_id=trigger_id,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                chosen_option=chosen_option,
                params=params,
                subject=subject,
                body=body,
                response_url=response_url,
            )

    result = call_astro_hitl_response(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        chosen_options=[chosen_option],
        params_input={},
    )

    logger.info("Astro API result: %s", result)

    if response_url and result.get("status_code") == 200:
        update_slack_message(
            response_url, user_name, chosen_option, {}, subject, body, dag_id, task_id
        )

    return response(200, {})


def handle_modal_submit(payload):
    view = payload.get("view", {})

    private_metadata = view.get("private_metadata", "{}")
    try:
        metadata = json.loads(private_metadata)
    except json.JSONDecodeError:
        return response(400, {"error": "Invalid modal metadata"})

    dag_id = metadata.get("dag_id")
    dag_run_id = metadata.get("dag_run_id")
    task_id = metadata.get("task_id")
    chosen_option = metadata.get("chosen_option")
    response_url = metadata.get("response_url")
    subject = metadata.get("subject", "")
    body = metadata.get("body", "")

    if not all([dag_id, dag_run_id, task_id, chosen_option]):
        return response(400, {"error": "Missing metadata in modal"})

    state_values = view.get("state", {}).get("values", {})
    params_input = {}

    for block_id, block_data in state_values.items():
        for action_id, action_data in block_data.items():
            if action_id.startswith("param_"):
                param_name = action_id[6:]
                params_input[param_name] = action_data.get("value", "")

    user = payload.get("user", {})
    user_name = user.get("name", "Someone")

    logger.info(
        "Modal submit: dag_id=%s, option=%s, params=%s",
        dag_id,
        chosen_option,
        params_input,
    )

    result = call_astro_hitl_response(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        chosen_options=[chosen_option],
        params_input=params_input,
    )

    logger.info("Astro API result: %s", result)

    if response_url and result.get("status_code") == 200:
        update_slack_message(
            response_url,
            user_name,
            chosen_option,
            params_input,
            subject,
            body,
            dag_id,
            task_id,
        )

    return response(200, {})


def open_params_modal(
    trigger_id,
    dag_id,
    dag_run_id,
    task_id,
    chosen_option,
    params,
    subject="",
    body="",
    response_url="",
):
    input_blocks = []
    for param_name, param_info in params.items():
        if isinstance(param_info, dict):
            label = param_info.get("label", param_name)
            placeholder = param_info.get("placeholder", f"Enter {param_name}")
        else:
            label = param_name
            placeholder = f"Enter {param_name}"

        input_blocks.append(
            {
                "type": "input",
                "block_id": f"block_{param_name}",
                "label": {"type": "plain_text", "text": str(label)},
                "element": {
                    "type": "plain_text_input",
                    "action_id": f"param_{param_name}",
                    "placeholder": {"type": "plain_text", "text": str(placeholder)},
                },
            }
        )

    private_metadata = json.dumps(
        {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "chosen_option": chosen_option,
            "subject": subject,
            "body": body,
            "response_url": response_url,
        }
    )

    modal = {
        "type": "modal",
        "title": {"type": "plain_text", "text": "HITL Parameters"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "private_metadata": private_metadata,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Selected option:* `{chosen_option}`",
                },
            },
            {"type": "divider"},
            *input_blocks,
        ],
    }

    url = "https://slack.com/api/views.open"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"trigger_id": trigger_id, "view": modal}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp_data = resp.json()
        logger.debug("Modal open response: %s", resp_data)
        if not resp_data.get("ok"):
            logger.error("Failed to open modal: %s", resp_data.get("error"))
    except requests.RequestException as e:
        logger.error("Failed to open modal: %s", e)

    return response(200, {})


def send_slack_message(dag_id, dag_run_id, task_id, subject, body, options, params):
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL:
        return {
            "status_code": 500,
            "error": "Slack not configured",
            "message": "Set SLACK_BOT_TOKEN and SLACK_CHANNEL environment variables",
        }

    slack_body = markdown_to_mrkdwn(body)
    params_json = json.dumps(params) if params else ""
    metadata = json.dumps({"subject": subject, "body": body})

    buttons = []
    for option in options:
        if params_json:
            value = f"{dag_id}|{dag_run_id}|{task_id}|{option}|{metadata}|{params_json}"
        else:
            value = f"{dag_id}|{dag_run_id}|{task_id}|{option}|{metadata}"

        buttons.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": option},
                "value": value,
                "action_id": f"hitl_option_{option.replace(' ', '_')}",
            }
        )

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": subject}},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": slack_body if slack_body else "_No additional details_",
            },
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Dag:* `{dag_id}` | *Task:* `{task_id}`"}
            ],
        },
        {"type": "divider"},
        {"type": "actions", "elements": buttons},
    ]

    if params:
        param_names = ", ".join(params.keys())
        blocks.insert(
            3,
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f":pencil: *Required params:* {param_names}",
                    }
                ],
            },
        )

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"channel": SLACK_CHANNEL, "text": f"HITL: {subject}", "blocks": blocks}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp_data = resp.json()
        if resp_data.get("ok"):
            return {
                "status_code": 200,
                "message": "Sent to Slack",
                "slack_ts": resp_data.get("ts"),
                "channel": resp_data.get("channel"),
            }
        else:
            return {
                "status_code": 400,
                "error": "Slack API error",
                "detail": resp_data.get("error"),
            }
    except requests.RequestException as e:
        return {
            "status_code": 502,
            "error": "Failed to call Slack API",
            "detail": str(e),
        }


def update_slack_message(
    response_url,
    user_name,
    chosen_option,
    params_input,
    subject="",
    body="",
    dag_id="",
    task_id="",
):
    slack_body = markdown_to_mrkdwn(body)
    if params_input:
        params_str = ", ".join(f"{k}=`{v}`" for k, v in params_input.items())
        response_text = f":white_check_mark: *Answered by {user_name}*\n*Choice:* `{chosen_option}`\n*Params:* {params_str}"
    else:
        response_text = (
            f":white_check_mark: *Answered by {user_name}*\n*Choice:* `{chosen_option}`"
        )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": subject if subject else "HITL Response",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": slack_body if slack_body else "_No additional details_",
            },
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"*Dag:* `{dag_id}` | *Task:* `{task_id}`"}
            ],
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": response_text}},
    ]

    payload = {
        "replace_original": True,
        "text": f"Answered by {user_name}: {chosen_option}",
        "blocks": blocks,
    }

    try:
        requests.post(response_url, json=payload, timeout=10)
    except requests.RequestException:
        pass


def call_astro_hitl_response(dag_id, dag_run_id, task_id, chosen_options, params_input):
    if not ASTRO_API_URL or not ASTRO_API_TOKEN:
        return {
            "status_code": 500,
            "error": "Astro API not configured",
            "message": "Set ASTRO_API_URL and ASTRO_API_TOKEN environment variables",
        }

    map_index = -1
    url = f"{ASTRO_API_URL}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(dag_run_id, safe='')}/taskInstances/{quote(task_id, safe='')}/{map_index}/hitlDetails"
    logger.info("Calling Astro API: PATCH %s", url)
    headers = {
        "Authorization": f"Bearer {ASTRO_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"chosen_options": chosen_options, "params_input": params_input}

    try:
        resp = requests.patch(url, headers=headers, json=payload, timeout=10)
        return {
            "status_code": resp.status_code,
            "message": "Forwarded to Astro",
            "astro_response": resp.json() if resp.content else None,
        }
    except requests.RequestException as e:
        return {
            "status_code": 502,
            "error": "Failed to call Astro API",
            "details": str(e),
        }


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
