"""
Ask AstroTrips: a governed natural-language analytics gateway.

Trigger with a natural-language ``question`` param. It understands the question,
judges whether the warehouse can answer it, writes SQL for a human to approve,
and then frames the answer.

Try it with:
    answerable   -> "Which planet route earned the most net revenue in 2025?"
    unanswerable -> "What is our average customer satisfaction score?"
"""

from datetime import timedelta
from typing import Any

from pydantic_ai.usage import UsageLimits

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, dag, get_current_context, task

from include.models import Answer, QuerySpec

_LLM_CONN_ID = "pydanticai_default"
_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"

# Tables the SQL generator is allowed to reason about.
_TABLES = [
    "planets", "routes", "customers", "promo_codes",
    "bookings", "payments", "cancellations", "daily_planet_report",
]

# Guardrail against runaway spend on any single LLM step.
_USAGE_LIMITS = UsageLimits(request_limit=8, input_tokens_limit=20_000, output_tokens_limit=4_000)

# LLM-classified retries: rate limits back off and retry, permanent errors fail fast.
# Requires Airflow 3.3+ (Runtime 3.3-2 ships 3.3.0); degrade gracefully otherwise.
try:
    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule

    _LLM_RETRY = LLMRetryPolicy(
        llm_conn_id=_LLM_CONN_ID,
        fallback_rules=[
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )
    _RETRY_KWARGS: dict[str, Any] = {"retries": 3, "retry_delay": timedelta(seconds=15), "retry_policy": _LLM_RETRY}
except ImportError:
    _RETRY_KWARGS = {"retries": 2, "retry_delay": timedelta(seconds=15)}


def _spec_get(spec: Any, key: str) -> Any:
    """QuerySpec output may arrive as a model instance or a dict across XCom."""
    if isinstance(spec, dict):
        return spec.get(key)
    return getattr(spec, key, None)


def _question() -> str:
    return get_current_context()["params"]["question"]


@dag(
    tags=["common-ai", "demo", "gateway"],
    params={
        "question": Param(
            "How did net revenue break down by planet in 2025, and which planet performed best?",
            type="string",
            description="A natural-language analytics question about AstroTrips.",
        )
    },
)
def ask_astrotrips():

    @task.llm(
        llm_conn_id=_LLM_CONN_ID,
        output_type=QuerySpec,
        usage_limits=_USAGE_LIMITS,
        system_prompt=(
            "You are an analytics intake assistant for AstroTrips, an interplanetary "
            "travel company. Turn a business user's question into a QuerySpec. Decide "
            "is_answerable=True only if the question can be answered from booking, "
            "payment, cancellation, or revenue data. Questions about satisfaction, "
            "reviews, NPS, weather, or anything not in a sales/revenue warehouse are "
            "is_answerable=False with a one-sentence reason. Do not compute anything."
        ),
        **_RETRY_KWARGS,
    )
    def understand_question():
        return f"Business user's question: {_question()!r}"

    @task.branch
    def gate(spec: Any) -> str:
        return "generate_sql" if _spec_get(spec, "is_answerable") else "cannot_answer"

    @task.llm_sql(
        llm_conn_id=_LLM_CONN_ID,
        db_conn_id=_SNOWFLAKE_CONN_ID,
        table_names=_TABLES,
        validate_sql=True,
        dialect="snowflake",
        require_approval=True,
        allow_modifications=True,
        approval_timeout=timedelta(hours=1),
        **_RETRY_KWARGS,
    )
    def generate_sql(spec: Any):
        return (
            f"{_question()}\n\n"
            f"Interpretation hints (from intake): "
            f"type={_spec_get(spec, 'question_type')}, "
            f"metrics={_spec_get(spec, 'metrics')}, "
            f"dimensions={_spec_get(spec, 'dimensions')}, "
            f"time_range={_spec_get(spec, 'time_range')!r}.\n\n"
            "Guidance for the query:\n"
            "- Net revenue = payments received minus cancellation refunds "
            "(SUM(payments.amount_usd) - SUM(cancellations.refund_amount_usd)). "
            "Gross fare = passengers * routes.base_fare_usd * planets.base_multiplier.\n"
            "- promo_codes.discount_pct is a FRACTION (0.10 = 10%). Discount amount = "
            "gross fare * discount_pct; do NOT divide by 100.\n"
            "- Interpret a calendar year (e.g. 2025) by the booking date bookings.booked_at, "
            "not departure_date.\n"
            "- When the question ranks or compares (most/top/best/by planet), return ONE ROW "
            "PER GROUP ordered by the metric. Do NOT use LIMIT 1, so the full comparison is "
            "visible for the answer.\n"
            "- Read-only single SELECT."
        )

    run_sql = SQLExecuteQueryOperator(
        task_id="run_sql",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="{{ ti.xcom_pull(task_ids='generate_sql') }}",
        show_return_value_in_logs=True,
    )

    @task.llm(
        llm_conn_id=_LLM_CONN_ID,
        output_type=Answer,
        usage_limits=_USAGE_LIMITS,
        system_prompt=(
            "You are a data analyst for AstroTrips. Answer the user's question using "
            "ONLY the provided result rows. Never invent numbers. If the rows do not "
            "fully answer the question, say so in the caveats."
        ),
        **_RETRY_KWARGS,
    )
    def compose_answer(rows: Any):
        return f"Question: {_question()}\n\nSQL result rows:\n{rows}"

    @task
    def cannot_answer(spec: Any) -> str:
        reason = _spec_get(spec, "reason") or "That question is outside what our data covers."
        return f"Cannot answer from the AstroTrips warehouse: {reason}"

    spec = understand_question()
    decision = gate(spec)
    sql = generate_sql(spec)

    decision >> [sql, cannot_answer(spec)]
    sql >> run_sql
    compose_answer(run_sql.output)


ask_astrotrips()
