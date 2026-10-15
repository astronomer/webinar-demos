"""v5"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task


class RevenueReport(BaseModel):
    summary: str = Field(description="Two or three sentences for a business reader.")
    anomalies: list[str] = Field(description="One short sentence per store that needs attention.")


@dag(schedule=None, start_date=datetime(2026, 10, 1), catchup=False, tags=["etl-elt", "ai", "optional"])
def revenue_agent_cryptic():
    @task.agent(
        llm_conn_id="llm",
        system_prompt=(
            "You are a retail analyst. The database is PostgreSQL. Answer with data from the rev_x table. "
            "Write plain sentences, without markdown or em dashes."
        ),
        toolsets=[SQLToolset(db_conn_id="warehouse_agent", allowed_tables=["rev_x"], max_rows=200)],
        output_type=RevenueReport,
        serialize_output=True,
    )
    def analyze_revenue() -> str:
        return (
            "Compare each store's latest day with its average over the previous days. "
            "Summarize what changed and list anything unusual."
        )

    analyze_revenue()


revenue_agent_cryptic()
