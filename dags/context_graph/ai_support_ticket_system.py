from airflow.providers.standard.operators.hitl import (
    HITLOperator,
)
from airflow.sdk import dag, task, Param, chain, Asset
from datetime import timedelta
import random
from typing import Literal
from pydantic import BaseModel
from include.custom_functions import read_all_decision_traces, check_the_roadmap


class TicketResponse(BaseModel):
    summary: str
    response: str
    priority: Literal["low", "medium", "high"]
    confidence_score: float
    suggested_tags: list[str]


class AIReviewedTicketResponse(BaseModel):
    accuracy_rating: float
    tone_rating: float
    completeness_rating: float
    helpfulness_rating: float
    suggested_improvements: list[str]


@dag
def ai_support_ticket_system():

    @task
    def fetch_pending_ticket() -> list[dict]:
        from include.custom_functions import get_open_tickets

        tickets = get_open_tickets(1)
        return tickets[0]

    _fetch_pending_ticket = fetch_pending_ticket()

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="""
        You are friendly and helpful support agent generating answers to tickets.
        Make sure to address the customer by name in the response.

        Tools:
        - `read_all_decision_traces`: Read all decision traces for previous reviews of your responses.
        - `check_the_roadmap`: Check the roadmap for the requested feature.
        """,
        output_type=TicketResponse,
        agent_params={"tools": [read_all_decision_traces, check_the_roadmap]},
    )
    def generate_ai_response(ticket: dict):
        import json

        ticket_str = json.dumps(ticket)
        return ticket_str

    _generate_ai_response = generate_ai_response(ticket=_fetch_pending_ticket)

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="""
        You are a helpful assistant reviewing the accuracy, tone,
        and completeness of the AI-generated support ticket response.
        Rate the response on a scale of 1 to 10 for each criterion:
        - Accuracy: Is the information given in the response accurate?
        - Tone: Is the tone of the response friendly and appropriate?
        - Completeness: Is the response complete?
        - Helpfulness: Is the response helpful and does it include actionable steps for the customer to take?
        Provide a list of suggested improvements for the response.
        Don't use placeholders like [Your Name] or [Customer Name].
        """,
        output_type=AIReviewedTicketResponse,
        agent_params={"tools": [read_all_decision_traces, check_the_roadmap]},
    )
    def ai_as_a_judge(ai_response: dict, original_ticket: dict):
        import json

        ticket_str = json.dumps(original_ticket)
        ai_response_str = json.dumps(ai_response)

        user_prompt = f"""
        The original ticket is:
        {ticket_str}

        The AI response is:
        {ai_response_str}
        """

        return user_prompt

    _ai_as_a_judge = ai_as_a_judge(
        ai_response=_generate_ai_response, original_ticket=_fetch_pending_ticket
    )

    @task
    def format_approval_request(
        ai_response: dict, original_ticket: dict, ai_as_a_judge_response: dict
    ):
        return {
            "ticket_info": f"**Ticket:** {original_ticket['ticket_id']}\n**Customer:** {original_ticket['customer']}\n**Subject:** {original_ticket['subject']}\n**Priority:** {original_ticket['priority']}",
            "summary": ai_response["summary"],
            "ai_response": ai_response["response"],
            "confidence": ai_response["confidence_score"],
            "priority": ai_response["priority"],
            "suggested_tags": ai_response["suggested_tags"],
            "metadata": ai_response,
            "original_ticket": original_ticket,
            "ai_as_a_judge_accuracy_rating": ai_as_a_judge_response["accuracy_rating"],
            "ai_as_a_judge_tone_rating": ai_as_a_judge_response["tone_rating"],
            "ai_as_a_judge_completeness_rating": ai_as_a_judge_response[
                "completeness_rating"
            ],
            "ai_as_a_judge_helpfulness_rating": ai_as_a_judge_response[
                "helpfulness_rating"
            ],
            "ai_as_a_judge_suggested_improvements": ai_as_a_judge_response[
                "suggested_improvements"
            ],
        }

    _format_approval_request = format_approval_request(
        ai_response=_generate_ai_response,
        original_ticket=_fetch_pending_ticket,
        ai_as_a_judge_response=_ai_as_a_judge,
    )

    _human_approval = HITLOperator(
        task_id="human_approval",
        subject="🎫 AI Support Response Ready for Review",
        body="""\
{% set data = ti.xcom_pull(task_ids='format_approval_request') %}

## Ticket Details

{{ data['summary'] }}

{{ data['original_ticket'] }}

---

## AI Summary

{{ data['summary'] }}

| | |
|---|---|
| **Suggested Priority** | {{ data['priority'] }} |
| **Confidence** | {{ "%.0f" | format(data['confidence'] * 100) }}% |
| **Suggested Tags** | {{ data['suggested_tags'] | join(', ') }} |

---

## AI Response

> {{ data['ai_response'] | replace('\n', '\n> ') }}

---

## AI Judge Evaluation

| | |
|---|---|
| **Accuracy** | {{ "%.1f" | format(data['ai_as_a_judge_accuracy_rating']) }} / 10 |
| **Tone** | {{ "%.1f" | format(data['ai_as_a_judge_tone_rating']) }} / 10 |
| **Completeness** | {{ "%.1f" | format(data['ai_as_a_judge_completeness_rating']) }} / 10 |
| **Helpfulness** | {{ "%.1f" | format(data['ai_as_a_judge_helpfulness_rating']) }} / 10 |

**Suggested Improvements:**
{% for improvement in data['ai_as_a_judge_suggested_improvements'] %}
- {{ improvement }}
{% endfor %}

""",
        options=[
            "Approve AI Response",
            "Respond Manually",
            "Escalate to CRE",
        ],
        multiple=False,
        defaults=["Escalate to CRE"],
        params={
            "Reason for decision": Param(type=["string", "null"], default="..."),
            "Manual response": Param(type=["string", "null"], default="..."),
        },
        execution_timeout=timedelta(
            hours=4
        ),  # after 4 hours the default option will be selected
    )

    @task(outlets=[Asset("new_decision")])
    def process_response(hitl_output: dict):
        if hitl_output["chosen_options"][0] == "Respond Manually":
            print("Respond Manually")
            print(hitl_output["params_input"]["Reason for decision"])
            print(hitl_output["params_input"]["Manual response"])
            return
        if hitl_output["chosen_options"][0] == "Approve AI Response":
            print("Approved AI Response")
            print(hitl_output["params_input"]["Reason for decision"])
            return
        if hitl_output["chosen_options"][0] == "Escalate to CRE":
            print("Escalate to CRE")
            print(hitl_output["params_input"]["Reason for decision"])
            return

    _process_response = process_response(hitl_output=_human_approval.output)

    chain(
        _format_approval_request,
        _human_approval,
        _process_response,
    )


ai_support_ticket_system()
