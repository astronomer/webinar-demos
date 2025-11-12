"""
## S3 to Snowflake ETL DAG

This DAG extracts data CSV files stored in an S3 bucket and loads it
into a newly crated Snowflake table using a Snowflake Stage and the
CopyFromExternalStageToSnowflakeOperator.

The DAG parallelizes the loading of the CSV files into the Snowflake table.
Based on the folder structure in the S3 bucket to enable loading at scale.

To use this DAG, you need to have a Snowflake stage configured with a connection
to your S3 bucket.
"""

import logging
import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator, \
    SQLTableCheckOperator
from airflow.providers.discord.notifications.discord import DiscordNotifier
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import ObjectStoragePath, dag, task_group, chain, Asset
from pendulum import datetime

logger = logging.getLogger("airflow.task")

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "etl-with-snowflake-demo-1234")

# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
PATH_STAGE = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-stage", conn_id=AWS_CONN_ID)

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")
SNOWFLAKE_STAGE_NAME = os.getenv("SNOWFLAKE_STAGE_NAME", "ETL_STAGE")

BASE_TABLES = ["users", "teas", "utms"]


@dag(
    dag_display_name="3 - Load sales data from S3 to ❄️",
    start_date=datetime(2025, 11, 1),
    schedule=[Asset(PATH_STAGE.as_uri())],
    max_consecutive_failed_dag_runs=3,  # auto-pauses the DAG after 3 consecutive failed runs
    template_searchpath="/usr/local/airflow/include/sql",
    doc_md=__doc__,
    default_args={"retries": 3}
)
def load_to_snowflake():

    _base_tables_ready = EmptyOperator(task_id="base_tables_ready")

    for table in BASE_TABLES:

        @task_group(group_id=f"ingest_{table}_data")
        def ingest_data():
            _create_table = SQLExecuteQueryOperator(
                task_id=f"create_table_{table}_if_not_exists",
                conn_id=SNOWFLAKE_CONN_ID,
                sql=f"create_{table}_table.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": SNOWFLAKE_DB_NAME,
                    "schema_name": SNOWFLAKE_SCHEMA_NAME,
                },
            )

            _copy_into_table = CopyFromExternalStageToSnowflakeOperator(
                task_id=f"copy_into_{table}_table",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                database=SNOWFLAKE_DB_NAME,
                schema=SNOWFLAKE_SCHEMA_NAME,
                table=table,
                stage=SNOWFLAKE_STAGE_NAME,
                prefix=f"{table}/",
                file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
            )

            _deduplicate_records = SQLExecuteQueryOperator(
                task_id=f"deduplicate_records_{table}",
                conn_id=SNOWFLAKE_CONN_ID,
                sql=f"remove_duplicate_{table}.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": SNOWFLAKE_DB_NAME,
                    "schema_name": SNOWFLAKE_SCHEMA_NAME,
                },
            )

            _vital_dq_checks = SQLColumnCheckOperator(
                task_id=f"vital_checks_{table}_table",
                conn_id=SNOWFLAKE_CONN_ID,
                database=SNOWFLAKE_DB_NAME,
                table=f"{SNOWFLAKE_SCHEMA_NAME}.{table}",
                column_mapping={
                    f"{table[:-1]}_ID": {
                        "unique_check": {"equal_to": 0},  # primary key check
                        "null_check": {"equal_to": 0},
                    }
                },
                outlets=[Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{table}")],
            )

            chain(
                _create_table,
                _copy_into_table,
                _deduplicate_records,
                _vital_dq_checks,
                _base_tables_ready,
            )

        ingest_data()

    @task_group(
        default_args={
            "retries": 0,
            "on_failure_callback": DiscordNotifier(
                discord_conn_id="discord_default",
                text="""
                Data quality checks failed for table: `{{ task.table }}`!
                ```
                {{ exception }}
                ```
                """
            ),
        },
    )
    def additional_dq_checks():
        from include.data_quality_checks import column_mappings

        SQLColumnCheckOperator(
            task_id="additional_dq_checks_teas_col",
            conn_id=SNOWFLAKE_CONN_ID,
            database=SNOWFLAKE_DB_NAME,
            table=f"{SNOWFLAKE_SCHEMA_NAME}.teas",
            column_mapping={
                "TEA_NAME": {
                    "null_check": {"equal_to": 0},
                    "distinct_check": {"geq_to": 20},
                },
                "TEA_TYPE": {
                    "null_check": {"equal_to": 0},
                    "distinct_check": {"equal_to": 6},
                },
                "PRICE": {
                    "null_check": {"equal_to": 0},
                    "min": {"geq_to": 0},
                    "max": {"leq_to": 30},
                    # "max": {"leq_to": 3},  # demo qa check failure
                },
            },
        )

        SQLTableCheckOperator(
            task_id="additional_dq_checks_teas_table",
            conn_id=SNOWFLAKE_CONN_ID,
            database=SNOWFLAKE_DB_NAME,
            table=f"{SNOWFLAKE_SCHEMA_NAME}.teas",
            checks={
                "tea_type_check": {
                    "check_statement": "TEA_TYPE IN ('Green', 'Black', 'Oolong', 'Floral', 'Herbal', 'Chai')"
                },
                "at_least_3_affordable_teas": {
                    "check_statement": "COUNT(DISTINCT TEA_ID) >=3 ",
                    "partition_clause": "PRICE < 12",  # a WHERE statement without the WHERE keyword
                },
            },
        )

        for table, mapping in column_mappings.items():
            SQLColumnCheckOperator(
                task_id=f"additional_dq_checks_{table}_col",
                conn_id=SNOWFLAKE_CONN_ID,
                database=SNOWFLAKE_DB_NAME,
                table=f"{SNOWFLAKE_SCHEMA_NAME}.{table}",
                column_mapping=mapping,
            )

    @task_group
    def ingest_sales_data():
        _create_table_sales_if_not_exists = SQLExecuteQueryOperator(
            task_id=f"create_table_sales_if_not_exists",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="create_sales_table.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": SNOWFLAKE_DB_NAME,
                "schema_name": SNOWFLAKE_SCHEMA_NAME,
            },
        )

        _copy_into_sales_table = CopyFromExternalStageToSnowflakeOperator(
            task_id="copy_into_sales_table",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            database=SNOWFLAKE_DB_NAME,
            schema=SNOWFLAKE_SCHEMA_NAME,
            table="sales",
            stage=SNOWFLAKE_STAGE_NAME,
            prefix="sales/",
            file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
        )

        _vital_dq_checks_sales_table = SQLColumnCheckOperator(
            task_id=f"vital_checks_sales_table",
            conn_id=SNOWFLAKE_CONN_ID,
            database=SNOWFLAKE_DB_NAME,
            table=f"{SNOWFLAKE_SCHEMA_NAME}.sales",
            column_mapping={
                "SALE_ID": {
                    "unique_check": {"equal_to": 0},  # SALE_ID is the primary key
                    "null_check": {"equal_to": 0},
                }
            },
            outlets=[
                Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}"),
                Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.sales")
            ],
        )

        chain(
            _create_table_sales_if_not_exists,
            _copy_into_sales_table,
            _vital_dq_checks_sales_table,
        )

    _ingest_sales_data = ingest_sales_data()

    chain(
        _base_tables_ready,
        _ingest_sales_data,
        additional_dq_checks()
    )


load_to_snowflake()
