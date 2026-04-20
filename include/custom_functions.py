import random
from pathlib import Path


DECISION_TRACES_DIR = Path(__file__).parent / "decision_traces"


def check_the_roadmap(feature: str) -> str:

    roadmap = [
        {
            "feature": "Dark mode",
            "status": "In progress",
            "eta": "Q1 2026",
        },
        {
            "feature": "RBAC",
            "status": "Planned",
            "eta": "Q1 2026",
        },
        {
            "feature": "Granual cost attribution",
            "status": "Planned",
            "eta": "Q3 2026",
        },

    ]
    return roadmap

def get_decision_traces_dir() -> Path:
    DECISION_TRACES_DIR.mkdir(exist_ok=True)
    return DECISION_TRACES_DIR


def read_all_decision_traces() -> list[dict]:
    traces_dir = get_decision_traces_dir()
    traces = []

    for md_file in sorted(traces_dir.glob("*.md")):
        traces.append({
            "filename": md_file.name,
            "path": str(md_file),
            "content": md_file.read_text(),
        })

    return traces


def save_as_markdown(decision_trace: dict, output_dir: Path = None) -> str:
    if output_dir is None:
        output_dir = get_decision_traces_dir()
    else:
        output_dir = output_dir / "decision_traces"
        output_dir.mkdir(exist_ok=True)

    dag_id = decision_trace["source_dag"]
    dag_run_id = decision_trace["dag_run_id"]

    md_content = f"# Decision Trace\n\n"
    md_content += f"**DAG:** {dag_id}\n\n"
    md_content += f"**DAG Run ID:** {dag_run_id}\n\n"
    md_content += f"---\n\n"

    for i, hitl in enumerate(decision_trace["hitl_decisions"], 1):
        md_content += f"## Decision {i}: {hitl['task_id']}\n\n"

        md_content += f"### Subject\n\n{hitl['subject']}\n\n"

        body = hitl['body'] or ""
        body = body.replace("\\n", "\n").replace("\n\n", "\n\n").strip()
        md_content += f"### Body\n\n{body}\n\n"

        options = hitl['options'] or []
        md_content += f"### Options\n\n"
        for opt in options:
            md_content += f"- {opt}\n"
        md_content += "\n"

        md_content += f"### Response\n\n"
        chosen = hitl['chosen_options'] or []
        md_content += f"- **Chosen Options:** {', '.join(chosen) if chosen else 'None'}\n"
        md_content += f"- **Responded At:** {hitl['responded_at'] or 'Pending'}\n"

        user = hitl['responded_by_user']
        if user:
            md_content += f"- **Responded By:** {user.get('name', 'Unknown')} ({user.get('id', '')})\n"
        else:
            md_content += f"- **Responded By:** Pending\n"

        md_content += f"- **Response Received:** {hitl['response_received']}\n"

        params_input = hitl.get('params_input') or {}
        if params_input:
            md_content += f"\n### Reviewer Input\n\n"
            for key, value in params_input.items():
                md_content += f"**{key}:**\n\n{value}\n\n"

        md_content += "---\n\n"

    safe_dag_run_id = dag_run_id.replace("/", "_").replace(":", "_")
    file_path = output_dir / f"decision_trace_{dag_id}_{safe_dag_run_id}.md"
    file_path.write_text(md_content)

    return str(file_path)


def get_open_tickets(num: int) -> list[dict]:
    sample_tickets = [
                        {
            "ticket_id": "TKT-12349",
            "customer": "marie.doe@example.com",
            "subject": "Feature request: Granual cost attribution",
            "message": "Love your app! Would it be possible to add a granual cost attribution option? I would like to know the cost of each feature in my account. Thanks!",
            "priority": "low",
        },
                {
            "ticket_id": "TKT-12347",
            "customer": "john.doe@example.com",
            "subject": "Feature request: Dark mode",
            "message": "Love your app! Would it be possible to add a dark mode option? I use the app frequently in the evening and it would be much easier on the eyes. Thanks!",
            "priority": "low",
        },
        {
            "ticket_id": "TKT-12345",
            "customer": "john.doe@example.com",
            "subject": "Unable to reset password",
            "message": "I've been trying to reset my password for 2 hours but the reset email never arrives. Can you help?",
            "priority": "high",
        },
        {
            "ticket_id": "TKT-12346",
            "customer": "john.doe@example.com",
            "subject": "Billing discrepancy on latest invoice",
            "message": "Hi, I noticed an extra charge of $49.99 on my latest invoice that I don't recognize. Could you please explain what this charge is for? I haven't made any recent purchases.",
            "priority": "medium",
        },

        {
            "ticket_id": "TKT-12348",
            "customer": "john.doe@example.com",
            "subject": "Data export not working",
            "message": "I'm trying to export my data using the Export feature but it keeps failing with an error 'Export timeout'. I have about 3 years of data. Is there a limit or workaround?",
            "priority": "medium",
        },
        {
            "ticket_id": "TKT-12349",
            "customer": "john.doe@example.com",
            "subject": "Account locked after failed login attempts",
            "message": "My account got locked after I entered the wrong password a few times. I know my correct password now but can't access my account. How can I unlock it?",
            "priority": "high",
        },
    ]
    return random.sample(sample_tickets, num)
