"""v5"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import Asset, dag, task

DAILY_REVENUE = Asset("daily_revenue")


class RevenueReport(BaseModel):
    summary: str = Field(description="Two or three sentences for a business reader.")
    anomalies: list[str] = Field(description="One short sentence per store that needs attention.")


@dag(schedule=[DAILY_REVENUE], start_date=datetime(2026, 10, 1), catchup=False, tags=["etl-elt", "ai"])
def revenue_agent():
    @task.agent(
        llm_conn_id="llm",
        system_prompt=(
            "You are a retail analyst. The database is PostgreSQL. "
            "The daily_revenue table has one row per store and day. Columns: order_date, store_id, "
            "order_count (number of orders), revenue_usd (revenue in US dollars), "
            "avg_order_usd (average order value in US dollars). "
            "Write plain sentences, without markdown or em dashes."
        ),
        toolsets=[SQLToolset(db_conn_id="warehouse_agent", allowed_tables=["daily_revenue"], max_rows=200)],
        output_type=RevenueReport,
        serialize_output=True,
    )
    def analyze_revenue() -> str:
        return (
            "Compare each store's latest day with its average over the previous days. "
            "Summarize what changed and list anything unusual."
        )

    analyze_revenue()


revenue_agent()
