"""
## Transform data in Snowflake to create reporting tables

This DAG transforms the data in Snowflake to create reporting tables.
"""

import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.sdk import dag, chain, Asset, task_group
from pendulum import datetime

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

dag_directory = os.path.dirname(os.path.abspath(__file__))


@dag(
    dag_display_name="4 - Transform data in ❄️",
    start_date=datetime(2025, 11, 1),
    schedule=Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}"),
    max_consecutive_failed_dag_runs=3,  # auto-pauses the DAG after 3 consecutive failed runs
    template_searchpath="/usr/local/airflow/include/sql",
    doc_md=__doc__,
    default_args={"retries": 3}
)
def transform_data_in_snowflake():

    @task_group
    def enriched_sales():
        _create_enriched_sales = SQLExecuteQueryOperator(
            task_id="create_enriched_sales",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_enriched_sales.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _upsert_enriched_sales = SQLExecuteQueryOperator(
            task_id="upsert_enriched_sales",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="upsert_enriched_sales.sql",
            show_return_value_in_logs=False,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _vital_checks_enriched_sales_table = SQLColumnCheckOperator(
            task_id="vital_checks_enriched_sales_table",
            conn_id=SNOWFLAKE_CONN_ID,
            database=SNOWFLAKE_DB_NAME,
            table=f"{SNOWFLAKE_SCHEMA_NAME}.enriched_sales",
            column_mapping={
                f"SALE_ID": {
                    "unique_check": {"equal_to": 0},  # primary key check
                    "null_check": {"equal_to": 0},
                }
            },
            outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.enriched_sales")],
        )

        chain(_create_enriched_sales, _upsert_enriched_sales, _vital_checks_enriched_sales_table)

    @task_group
    def revenue_by_tea_type():
        _create_revenue_by_tea_type = SQLExecuteQueryOperator(
            task_id="create_revenue_by_tea_type",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_revenue_by_tea_type.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _upsert_revenue_by_tea_type = SQLExecuteQueryOperator(
            task_id="upsert_revenue_by_tea_type",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="upsert_revenue_by_tea_type.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.revenue_by_tea_type")],
        )

        chain(_create_revenue_by_tea_type, _upsert_revenue_by_tea_type)

    @task_group
    def user_purchase_summary():
        _create_user_purchase_summary = SQLExecuteQueryOperator(
            task_id="create_user_purchase_summary",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_user_purchase_summary.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _upsert_user_purchase_summary = SQLExecuteQueryOperator(
            task_id=f"upsert_user_purchase_summary",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="upsert_user_purchase_summary.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.user_purchase_summary")],
        )

        chain(_create_user_purchase_summary, _upsert_user_purchase_summary)

    @task_group
    def top_users_by_spending():
        _create_top_users_by_spending = SQLExecuteQueryOperator(
            task_id=f"create_top_users_by_spending",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_top_users_by_spending.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _upsert_top_users_by_spending = SQLExecuteQueryOperator(
            task_id=f"upsert_top_users_by_spending",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="upsert_top_users_by_spending.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.top_users_by_spending")],
        )

        chain(_create_top_users_by_spending, _upsert_top_users_by_spending)

    @task_group
    def sales_funnel_analysis():
        _create_sales_funnel_analysis = SQLExecuteQueryOperator(
            task_id=f"create_sales_funnel_analysis",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_sales_funnel_analysis.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _upsert_sales_funnel_analysis = SQLExecuteQueryOperator(
            task_id=f"upsert_sales_funnel_analysis",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="upsert_sales_funnel_analysis.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.sales_funnel_analysis")],
        )

        chain(_create_sales_funnel_analysis, _upsert_sales_funnel_analysis)

    chain(enriched_sales(),
        [
            revenue_by_tea_type(),
            user_purchase_summary(),
            top_users_by_spending(),
            sales_funnel_analysis()
        ]
    )


transform_data_in_snowflake()
