from datetime import timedelta
from typing import Literal

from airflow.providers.standard.operators.hitl import HITLBranchOperator, HITLEntryOperator
from airflow.sdk import dag, task, Param, chain
from airflow_ai_sdk.models.base import BaseModel


class TicketResponse(BaseModel):
    summary: str
    response: str
    priority: Literal["low", "medium", "high"]
    confidence_score: float
    suggested_tags: list[str]


@dag
def ai_support_ticket_system():

    # -- Fetch Ticket and Generate AI Response --

    @task
    def fetch_pending_ticket() -> dict:
        from include.ticket_system import get_open_tickets

        tickets = get_open_tickets(1)
        return tickets[0]

    @task.llm(
        model="gpt-4o-mini",
        system_prompt="""
            You are friendly and helpful support agent generating answers to tickets.
            Make sure to address the customer by name in the response.
        """,
        output_type=TicketResponse,
    )
    def generate_ai_response(ticket: dict):
        import json

        ticket_str = json.dumps(ticket)
        return ticket_str

    _pending_ticket = fetch_pending_ticket()

    # noinspection PyTypeChecker
    _ai_response: dict = generate_ai_response(ticket=_pending_ticket)

    @task
    def format_response(ai_response: dict, ticket: dict):
        return {
            "ticket_info": f"**Ticket:** {ticket['ticket_id']}\n**Customer:** {ticket['customer']}\n**Subject:** {ticket['subject']}\n**Priority:** {ticket['priority']}",
            "summary": ai_response["summary"],
            "ai_response": ai_response["response"],
            "confidence": ai_response["confidence_score"],
            "priority": ai_response["priority"],
            "suggested_tags": ai_response["suggested_tags"],
            "metadata": ai_response,
            "ticket": ticket,
        }

    _formatted_response = format_response(ai_response=_ai_response, ticket=_pending_ticket)

    # Human approval step
    _review_ai_response = HITLBranchOperator(
        task_id="review_ai_response",
        subject="🎫 AI Support Response Ready for Review",
        body="""
**Please review the AI-generated support ticket response below:**

{{ ti.xcom_pull(task_ids='format_response')['ticket_info'] }}

**AI Summary:**
{{ ti.xcom_pull(task_ids='format_response')['summary'] }}

**AI Suggested Priority:** {{ ti.xcom_pull(task_ids='format_response')['priority'] }}
**AI Confidence:** {{ macros.my_macro_plugin.format_confidence(ti.xcom_pull(task_ids='format_response')['confidence']) }}
**Suggested Tags:** {{ ti.xcom_pull(task_ids='format_response')['suggested_tags'] | join(', ') }}

**AI Response:**
```
{{ ti.xcom_pull(task_ids='format_response')['ai_response'] }}
```

**Instructions:**
- **Approve**: Send this response to the customer
- **Reject**: Route to human agent for manual response

Please review for accuracy, tone, and completeness.
        """,
        options=["Approve AI Response", "Respond Manually", "Escalate"],
        options_mapping={
            "Approve AI Response": "approve_ai_response",
            "Respond Manually": "respond_manually",
            "Escalate": "escalate",
        },
        defaults=["Escalate"],
        multiple=True,
        execution_timeout=timedelta(hours=4),
    )

    # -- Approve AI Response --

    @task
    def approve_ai_response(ai_response: dict):
        print("Processing ticket:", ai_response["ticket"]["ticket_id"])
        print("Sending Approved AI Response to customer:", ai_response["ai_response"])

    _approve_ai_response = approve_ai_response(ai_response=_formatted_response)

    # -- Respond Manually --

    _respond_manually = HITLEntryOperator(
        task_id="respond_manually",
        subject="🎫 Manual Response",
        body="""
**Please enter the manual response to the customer:**
```
{{ ti.xcom_pull(task_ids='format_response')['ticket']['message'] }}
```
        """,
        params={
            "manual_response": Param("None", type=["string"]),
        },
    )

    @task
    def process_manual_response(ticket: dict, manual_response: str):
        print("Processing ticket:", ticket["ticket_id"])
        print("Sending Manual Response to customer:", manual_response["params_input"]["manual_response"])

    _process_manual_response = process_manual_response(
        ticket=_pending_ticket,
        manual_response=_respond_manually.output,
    )

    chain(
        _respond_manually,
        _process_manual_response,
    )

    # -- Escalate --

    @task
    def escalate(ticket: dict):
        print("Processing ticket:", ticket["ticket_id"])
        print("Escalating to Customer Reliability Engineer or Customer Success Manager")

    _escalate = escalate(ticket=_pending_ticket)

    # -- Response Chain --

    chain(
        _formatted_response,
        _review_ai_response,
        [_approve_ai_response, _respond_manually, _escalate],
    )


ai_support_ticket_system()
