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

from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.providers.slack.notifications.slack import SlackNotifier
from pendulum import datetime, duration
import logging
import os


## SET YOUR OWN BUCKET NAME HERE
_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "my-bucket")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "tea-sales-stage")

# Get the Airflow task logger, print statements work as well to log at level INFO
t_log = logging.getLogger("airflow.task")

# Snowflake variables - REPLACE with your own values
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")
_SNOWFLAKE_STAGE_NAME = os.getenv("SNOWFLAKE_STAGE_NAME", "ETL_STAGE")

LIST_OF_BASE_TABLE_NAMES = ["users", "teas", "utms"]

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = os.getenv("CONN_ID_AWS", "aws_default")
KEY_SRC = f"{_S3_BUCKET_NAME}/{_STAGE_FOLDER_NAME}"

# Create the ObjectStoragePath object
base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}/", conn_id=CONN_ID_SRC)

dag_directory = os.path.dirname(os.path.abspath(__file__))


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="Load sales data from S3 to ❄️",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 8, 1),  # date after which the DAG can be scheduled
    schedule=[
        Dataset(base_src.as_uri())
    ],  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data team",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(minutes=1),  # tasks wait 1 minute in between retries
    },
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    description="ETL",  # description next to the DAG name in the UI
    tags=["S3", "Snowflake"],  # add tags in the UI
    template_searchpath=[
        os.path.join(dag_directory, "../include/sql")
    ],  # path to the SQL templates
)
def load_to_snowflake():

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    # optional starting task to structure the DAG, does not do anything
    start = EmptyOperator(task_id="start")
    base_tables_ready = EmptyOperator(task_id="base_tables_ready")

    for _TABLE in LIST_OF_BASE_TABLE_NAMES:

        # A task group is a way to group tasks in the Airflow UI
        # see: https://www.astronomer.io/docs/learn/task-groups

        @task_group(group_id=f"ingest_{_TABLE}_data")
        def ingest_data():

            create_table_if_not_exists = SQLExecuteQueryOperator(
                task_id=f"create_table_{_TABLE}_if_not_exists",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"create_{_TABLE}_table.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": _SNOWFLAKE_DB_NAME,
                    "schema_name": _SNOWFLAKE_SCHEMA_NAME,
                },
            )

            copy_into_table = CopyFromExternalStageToSnowflakeOperator(
                task_id=f"copy_into_{_TABLE}_table",
                snowflake_conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                schema=_SNOWFLAKE_SCHEMA_NAME,
                table=_TABLE,
                stage=_SNOWFLAKE_STAGE_NAME,
                prefix=f"{_TABLE}/",
                file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
            )

            deduplicate_records = SQLExecuteQueryOperator(
                task_id=f"deduplicate_records_{_TABLE}",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"remove_duplicate_{_TABLE}.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": _SNOWFLAKE_DB_NAME,
                    "schema_name": _SNOWFLAKE_SCHEMA_NAME,
                },
            )

            vital_dq_checks = SQLColumnCheckOperator(
                task_id=f"vital_checks_{_TABLE}_table",
                conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                table=f"{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}",
                column_mapping={
                    f"{_TABLE[:-1]}_ID": {
                        "unique_check": {"equal_to": 0},  # primary key check
                        "null_check": {"equal_to": 0},
                    }
                },
                outlets=[
                    Dataset(
                        f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}"
                    )
                ],
            )

            chain(
                start,
                create_table_if_not_exists,
                copy_into_table,
                deduplicate_records,
                vital_dq_checks,
                base_tables_ready,
            )

        ingest_data()

    @task_group(
        default_args={
            "owner": "DQ team",
            "retries": 0,
            "on_failure_callback": SlackNotifier(
                slack_conn_id="slack_conn",
                text="Data quality checks failed for the {{ task.table }} table!",
                channel="#alerts",
            ),
        },
    )
    def additional_dq_checks():

        from include.data_quality_checks import column_mappings

        SQLColumnCheckOperator(
            task_id="additional_dq_checks_teas_col",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            table=f"{_SNOWFLAKE_SCHEMA_NAME}.teas",
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
                    "max": {"leq_to": 19},
                },
            },
        )

        SQLTableCheckOperator(
            task_id="additional_dq_checks_teas_table",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            table=f"{_SNOWFLAKE_SCHEMA_NAME}.teas",
            checks={
                "tea_type_check": {
                    "check_statement": "TEA_TYPE IN ('Green', 'Black', 'Oolong', 'Floral', 'Herbal', 'Chai')"
                },
                "at_least_3_affordable_teas": {
                    "check_statement": "COUNT(DISTINCT TEA_ID) >=3",
                    "partition_clause": "PRICE < 10",  # a WHERE statement without the WHERE keyword
                },
            },
        )

        for _TABLE, _COLUMN_MAPPING in column_mappings.items():
            SQLColumnCheckOperator(
                task_id=f"additional_dq_checks_{_TABLE}_col",
                conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                table=f"{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}",
                column_mapping=_COLUMN_MAPPING,
            )

    @task_group
    def ingest_sales_data():

        create_table_sales_if_not_exists = SQLExecuteQueryOperator(
            task_id=f"create_table_sales_if_not_exists",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_sales_table.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

        copy_into_sales_table = CopyFromExternalStageToSnowflakeOperator(
            task_id="copy_into_sales_table",
            snowflake_conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            schema=_SNOWFLAKE_SCHEMA_NAME,
            table="sales",
            stage=_SNOWFLAKE_STAGE_NAME,
            prefix="sales/",
            file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
        )

        vital_dq_checks_sales_table = SQLColumnCheckOperator(
            task_id=f"vital_checks_sales_table",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            table=f"{_SNOWFLAKE_SCHEMA_NAME}.sales",
            column_mapping={
                "SALE_ID": {
                    "unique_check": {"equal_to": 0},  # SALES_ID is the primary key
                    "null_check": {"equal_to": 0},
                }
            },
            outlets=[
                Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}")
            ],
        )

        chain(
            create_table_sales_if_not_exists,
            copy_into_sales_table,
            vital_dq_checks_sales_table,
        )

    ingest_sales_data_obj = ingest_sales_data()

    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done",
    )

    # ------------------- #
    # Define dependencies #
    # ------------------- #

    chain(
        base_tables_ready,
        ingest_sales_data_obj,
        additional_dq_checks(),
        end,
    )


load_to_snowflake()
