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

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
import logging
import os


## SET YOUR OWN BUCKET NAME HERE
_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "my-bucket")

# Get the Airflow task logger, print statements work as well to log at level INFO
t_log = logging.getLogger("airflow.task")

# Snowflake variables - REPLACE with your own values
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "DEMO_DB")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEMO_SCHEMA")
_SNOWFLAKE_STAGE_NAME = os.getenv("SNOWFLAKE_STAGE_NAME", "DEMO_STAGE")
_SNOWFLAKE_TABLE_NAME = os.getenv("SNOWFLAKE_TABLE_NAME_SNEAKERS_DATA", "DEMO_TABLE")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = os.getenv("CONN_ID_AWS", "aws_default")
KEY_SRC = f"{_S3_BUCKET_NAME}/my_stage/"
URI = f"{OBJECT_STORAGE_SRC}://{KEY_SRC}"

# Create the ObjectStoragePath object
base_src = ObjectStoragePath(URI, conn_id=CONN_ID_SRC)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Load product info from S3 to Snowflake",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 8, 1),  # date after which the DAG can be scheduled
    schedule="@daily",  # see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data Engineering team",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(minutes=1),  # tasks wait 1 minute in between retries
    },
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    description="ETL",  # description next to the DAG name in the UI
    tags=["S3", "Snowflake"],  # add tags in the UI
)
def s3_to_snowflake_example():

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

    @task
    def list_keys(path: ObjectStoragePath) -> list[ObjectStoragePath]:
        """List all subfolders in a specific S3 location."""
        keys = [f.name + "/" for f in path.iterdir() if f.is_dir()]
        return keys

    key_list = list_keys(path=base_src)

    create_table_if_not_exists = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=f"""
                CREATE TABLE IF NOT EXISTS 
                {_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME} (
                    title STRING,
                    description STRING,
                    price FLOAT,
                    category STRING,
                    file_path STRING,
                    uuid STRING PRIMARY KEY
                );
            """,
        show_return_value_in_logs=True,
    )

    copy_into_table = CopyFromExternalStageToSnowflakeOperator.partial(
        task_id="copy_into_table",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB_NAME,
        schema=_SNOWFLAKE_SCHEMA_NAME,
        table=_SNOWFLAKE_TABLE_NAME,
        stage=_SNOWFLAKE_STAGE_NAME,
        file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
        map_index_template="Ingesting files from the year {{ task.prefix }}",
    ).expand(prefix=key_list)

    end = EmptyOperator(
        task_id="end",
        outlets=[
            Dataset(
                f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_SNOWFLAKE_TABLE_NAME}"
            )
        ],
    )

    # ------------------- #
    # Define dependencies #
    # ------------------- #

    chain(start, [create_table_if_not_exists, key_list], copy_into_table, end)


s3_to_snowflake_example()
