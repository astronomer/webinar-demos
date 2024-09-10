"""
## Transform data in Snowflake to create reporting tables

This DAG transforms the data in Snowflake to create reporting tables. 

It creates the following tables:
- `USER_PURCHASE_SUMMARY`
- `REVENUE_BY_TEA_TYPE`
- `SALES_FUNNEL_ANALYSIS`
- `TOP_USERS_BY_SPENDING`
"""

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain

from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
import logging
import os

_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

dag_directory = os.path.dirname(os.path.abspath(__file__))


@dag(
    dag_display_name="Transform data in ❄️",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 8, 1),  # date after which the DAG can be scheduled
    schedule=Dataset(
        f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}"
    ),  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data team",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(minutes=1),  # tasks wait 1 minute in between retries
    },
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    description="ETL",  # description next to the DAG name in the UI
    tags=["Snowflake"],  # add tags in the UI
    template_searchpath=[
        os.path.join(dag_directory, "../include/sql")
    ],  # path to the SQL templates
)
def transform_data_in_snowflake():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_enriched_sales = SQLExecuteQueryOperator(
        task_id="create_enriched_sales",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="create_enriched_sales.sql",
        show_return_value_in_logs=True,
        params={
            "db_name": _SNOWFLAKE_DB_NAME,
            "schema_name": _SNOWFLAKE_SCHEMA_NAME,
        },
    )

    upsert_enriched_sales = SQLExecuteQueryOperator(
        task_id="upsert_enriched_sales",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="upsert_enriched_sales.sql",
        show_return_value_in_logs=False,
        params={
            "db_name": _SNOWFLAKE_DB_NAME,
            "schema_name": _SNOWFLAKE_SCHEMA_NAME,
        },
    )

    vital_checks_enriched_sales_table = SQLColumnCheckOperator(
        task_id="vital_checks_enriched_sales_table",
        conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB_NAME,
        table=f"{_SNOWFLAKE_SCHEMA_NAME}.enriched_sales",
        column_mapping={
            f"SALE_ID": {
                "unique_check": {"equal_to": 0},  # primary key check
                "null_check": {"equal_to": 0},
            }
        },
        outlets=[
            Dataset(
                f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.enriched_sales"
            )
        ],
    )

    with TaskGroup(group_id="create_tables") as create_tables:

        create_user_purchase_summary = SQLExecuteQueryOperator(
            task_id="create_user_purchase_summary",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_user_purchase_summary.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

        create_revenue_by_tea_type = SQLExecuteQueryOperator(
            task_id="create_revenue_by_tea_type",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_revenue_by_tea_type.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

        create_sales_funnel_analysis = SQLExecuteQueryOperator(
            task_id=f"create_sales_funnel_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_sales_funnel_analysis.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

        create_top_users_by_spending = SQLExecuteQueryOperator(
            task_id=f"create_top_users_by_spending",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_top_users_by_spending.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

    with TaskGroup(group_id="upsert_tables") as upsert_tables:

        upsert_user_purchase_summary = SQLExecuteQueryOperator(
            task_id=f"upsert_user_purchase_summary",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_user_purchase_summary.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[
                Dataset(
                    f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.user_purchase_summary"
                )
            ],
        )

        upsert_revenue_by_tea_type = SQLExecuteQueryOperator(
            task_id="upsert_revenue_by_tea_type",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_revenue_by_tea_type.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[
                Dataset(
                    f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.revenue_by_tea_type"
                )
            ],
        )

        upsert_top_users_by_spending = SQLExecuteQueryOperator(
            task_id=f"upsert_top_users_by_spending",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_top_users_by_spending.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[
                Dataset(
                    f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.top_users_by_spending"
                )
            ],
        )

        upsert_sales_funnel_analysis = SQLExecuteQueryOperator(
            task_id=f"upsert_sales_funnel_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_sales_funnel_analysis.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
            outlets=[
                Dataset(
                    f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.sales_funnel_analysis"
                )
            ],
        )

    chain(
        start,
        create_enriched_sales,
        upsert_enriched_sales,
        vital_checks_enriched_sales_table,
        [
            create_user_purchase_summary,
            create_revenue_by_tea_type,
            create_sales_funnel_analysis,
        ],
    )
    chain(create_user_purchase_summary, create_top_users_by_spending)
    chain(create_user_purchase_summary, upsert_user_purchase_summary)
    chain(create_revenue_by_tea_type, upsert_revenue_by_tea_type)
    chain(create_sales_funnel_analysis, upsert_sales_funnel_analysis)
    chain(create_top_users_by_spending, upsert_top_users_by_spending)
    chain(
        [
            upsert_user_purchase_summary,
            upsert_revenue_by_tea_type,
            upsert_top_users_by_spending,
            upsert_sales_funnel_analysis,
        ],
        end,
    )


transform_data_in_snowflake()
