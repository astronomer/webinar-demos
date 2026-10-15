"""v4"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLExecuteQueryOperator
from airflow.sdk import Asset, dag

RAW_ORDERS = Asset("raw_orders")
DAILY_REVENUE = Asset("daily_revenue")


@dag(schedule=[RAW_ORDERS], start_date=datetime(2026, 10, 1), catchup=False, tags=["etl-elt"])
def transform_revenue():
    write = SQLExecuteQueryOperator(
        task_id="write_staging",
        conn_id="warehouse",
        sql=[
            "DROP TABLE IF EXISTS daily_revenue_staging",
            """
            CREATE TABLE daily_revenue_staging AS
            SELECT
                order_date,
                store_id,
                count(*) AS order_count,
                sum(amount_usd) AS revenue_usd,
                round(avg(amount_usd), 2) AS avg_order_usd
            FROM raw_orders
            GROUP BY order_date, store_id
            """,
        ],
    )

    audit = SQLColumnCheckOperator(
        task_id="audit_staging",
        conn_id="warehouse",
        table="daily_revenue_staging",
        column_mapping={
            "store_id": {"null_check": {"equal_to": 0}},
            "order_count": {"min": {"greater_than": 0}},
            "revenue_usd": {"min": {"geq_to": 0}},
        },
    )

    publish = SQLExecuteQueryOperator(
        task_id="publish",
        conn_id="warehouse",
        # Both statements run in one transaction, so readers never see an empty table.
        sql=[
            "DELETE FROM daily_revenue",
            "INSERT INTO daily_revenue SELECT * FROM daily_revenue_staging",
        ],
        outlets=[DAILY_REVENUE],
    )

    write >> audit >> publish


transform_revenue()
