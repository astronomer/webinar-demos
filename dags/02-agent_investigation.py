"""
Autonomous revenue investigation via an agent.

The opposite posture to dags/01. Instead of drafting one query for a human to
approve, this hands the agent a governed, read-only SQLToolset over the AstroTrips
warehouse and lets it investigate on its own: inspect schemas, run several
exploratory queries, form and verify a hypothesis, and return a structured
Finding. It should uncover the Europa launch-delay cancellations planted by setup.

- durable=True: on a retry the agent replays cached model/tool steps instead of
  re-issuing (and re-paying for) them.
- SQLToolset is read-only and table-allow-listed, so the agent cannot roam or write.
- usage_limits caps runaway spend; enable_tool_logging makes every query visible
  (and feeds the Token Console plugin).

Note: durable=True and enable_hitl_review=True are mutually exclusive in the
provider. This demo uses durable; the interactive review loop is a separate variant.
"""

from datetime import timedelta

from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task

from include.models import Finding

_LLM_CONN_ID = "pydanticai_default"
_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"

_ALLOWED_TABLES = [
    "planets", "routes", "customers", "promo_codes",
    "bookings", "payments", "cancellations", "daily_planet_report",
]


@dag(
    tags=["common-ai", "demo", "agent"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
)
def agent_investigation():

    @task.agent(
        llm_conn_id=_LLM_CONN_ID,
        output_type=Finding,
        durable=True,
        enable_tool_logging=True,
        usage_limits=UsageLimits(request_limit=20, input_tokens_limit=60_000, output_tokens_limit=8_000),
        toolsets=[
            SQLToolset(
                db_conn_id=_SNOWFLAKE_CONN_ID,
                allowed_tables=_ALLOWED_TABLES,
                max_rows=50,
            )
        ],
        system_prompt=(
            "You are a revenue analyst for AstroTrips, an interplanetary travel company. "
            "You have read-only SQL tools over the warehouse. Investigate the question by "
            "exploring the data yourself: list tables, inspect schemas, then run focused "
            "queries. Form a hypothesis and verify it with a query before concluding. "
            "Never state a number you have not queried. Business rules: gross fare = "
            "passengers * routes.base_fare_usd * planets.base_multiplier; realized revenue "
            "= daily_planet_report.total_paid_usd - total_refunds_usd. Return a Finding with "
            "the single most likely root cause and the evidence that supports it."
        ),
    )
    def investigate():
        return (
            "Europa's net revenue climbed steadily through the second half of 2025 and then "
            "fell sharply at the start of 2026, while Moon and Mars stayed steady. Investigate "
            "why Europa's realized revenue dropped and identify the single main driver. "
            "Look across bookings, payments, cancellations, and daily_planet_report."
        )

    investigate()


agent_investigation()
