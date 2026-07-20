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
from airflow.sdk import Param, dag, task, chain

from include.models import Answer, QuerySpec


_LLM_CONN_ID = "pydanticai_default"
_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"

# Tables the SQL generator is allowed to reason about
_TABLES = [
    "planets", "routes", "customers", "promo_codes",
    "bookings", "payments", "cancellations", "daily_planet_report",
]

# Guardrails
_USAGE_LIMITS = UsageLimits(request_limit=8, input_tokens_limit=20_000, output_tokens_limit=4_000)


@dag(
    tags=["common-ai", "demo", "gateway"],
    params={
        "question": Param(
            "How did net revenue break down by planet in 2025, and which planet performed best?",
            type="string",
            description="A natural-language analytics question about AstroTrips.",
        )
    },
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)}
)
def ask_astrotrips():

    @task.llm(
        llm_conn_id=_LLM_CONN_ID,
        output_type=QuerySpec,  # model instance will be pushed to XCom
        usage_limits=_USAGE_LIMITS,
        system_prompt=(
            "You are an analytics intake assistant for AstroTrips, an interplanetary "
            "travel company. Turn a business user's question into a QuerySpec. Decide "
            "is_answerable=True only if the question can be answered from booking, "
            "payment, cancellation, or revenue data. Questions about satisfaction, "
            "reviews, NPS, weather, or anything not in a sales/revenue warehouse are "
            "is_answerable=False with a one-sentence reason. Do not compute anything."
        ),
    )
    def understand_question(params: dict[str, Any] | None = None):
        question = params["question"]
        return f"Business user's question: {question}"

    @task.branch
    def gate(spec: QuerySpec) -> str:
        return "generate_sql" if spec.is_answerable else "cannot_answer"

    @task.llm_sql(
        llm_conn_id=_LLM_CONN_ID,
        db_conn_id=_SNOWFLAKE_CONN_ID,
        table_names=_TABLES,
        validate_sql=True,  # via AST parsing
        require_approval=True,
        allow_modifications=True,
        approval_timeout=timedelta(hours=1),
    )
    def generate_sql(spec: QuerySpec, params: dict[str, Any] | None = None):
        question = params["question"]
        return (
            f"{question}\n\n"
            f"Interpretation hints (from intake): "
            f"type={spec.question_type}, "
            f"metrics={spec.metrics}, "
            f"dimensions={spec.dimensions}, "
            f"time_range={spec.time_range}.\n\n"
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
    )
    def compose_answer(rows: Any, params: dict[str, Any] | None = None):
        question = params["question"]
        return f"Question: {question}\n\nSQL result rows:\n{rows}"

    @task
    def cannot_answer(spec: QuerySpec) -> str:
        reason = spec.reason or "That question is outside what our data covers."
        return f"Cannot answer from the AstroTrips warehouse: {reason}"

    spec = understand_question()
    decision = gate(spec)
    sql = generate_sql(spec)

    chain(decision, [sql, cannot_answer(spec)])
    chain(sql, run_sql)
    compose_answer(run_sql.output)


ask_astrotrips()
