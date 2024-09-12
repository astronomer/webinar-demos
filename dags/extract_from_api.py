"""
## Extract data from an API and load it to S3

To use a different remote storage option replace the S3CreateBucketOperator,
as well as change the OBJECT_STORAGE_DST, CONN_ID_DST and KEY_DST
parameters.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.models.param import Param
from pendulum import datetime, duration
import pandas as pd
import logging
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "my-bucket")
_BUCKET_REGION = os.getenv("BUCKET_REGION", "us-east-1")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "tea-sales-ingest")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET_NAME + "/" + _INGEST_FOLDER_NAME

base_dst = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="📊 Extract data from the internal API and load it to S3",
    start_date=datetime(2024, 8, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    params={
        "num_sales": Param(
            100,
            description="The number of sales to fetch from the API.",
            type="number",
        ),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ETL", "S3", "Internal API"],
)
def extract_from_api():

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

    # create the S3 bucket if it does not exist yet
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        aws_conn_id=_AWS_CONN_ID,
        bucket_name=_S3_BUCKET_NAME,
        region_name=_BUCKET_REGION,
    )

    @task
    def get_new_sales_from_api(**context) -> list[pd.DataFrame]:
        """
        Get new sales data from an internal API.
        Args:
            num_sales (int): The number of sales to fetch.
        Returns:
            list[pd.DataFrame]: A list of DataFrames containing data relating
            to the newest sales.
        """
        num_sales = context["params"]["num_sales"]
        date = context["ts"]
        from include.api_functions import get_new_sales_from_internal_api

        sales_df, users_df, teas_df, utm_df = get_new_sales_from_internal_api(
            num_sales, date
        )

        t_log.info(f"Fetching {num_sales} new sales from the internal API.")
        t_log.info(f"Head of the new sales data: {sales_df.head()}")
        t_log.info(f"Head of the new users data: {users_df.head()}")
        t_log.info(f"Head of the new teas data: {teas_df.head()}")
        t_log.info(f"Head of the new utm data: {utm_df.head()}")

        return [
            {"name": "sales", "data": sales_df},
            {"name": "users", "data": users_df},
            {"name": "teas", "data": teas_df},
            {"name": "utms", "data": utm_df},
        ]

    get_new_sales_from_api_obj = get_new_sales_from_api()

    @task(map_index_template="{{ my_custom_map_index }}")
    def write_to_s3(
        data_to_write: pd.DataFrame, base_dst: ObjectStoragePath, **context
    ):
        """
        Write the data to an S3 bucket.
        Args:
            data_to_write (pd.DataFrame): The data to write to S3.
            base_dst (ObjectStoragePath): The base path to write the data to.
        """
        import io

        data = data_to_write["data"]
        name = data_to_write["name"]
        dag_run_id = context["dag_run"].run_id

        csv_buffer = io.BytesIO()
        data.to_csv(csv_buffer, index=False)

        csv_bytes = csv_buffer.getvalue()

        path_dst = base_dst / name / f"{dag_run_id}.csv"

        # Write the bytes to the S3 bucket using your existing method
        path_dst.write_bytes(csv_bytes)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Wrote new {name} data to S3."

    write_to_s3_obj = write_to_s3.partial(base_dst=base_dst).expand(
        data_to_write=get_new_sales_from_api_obj
    )

    @task(
        outlets=[
            Dataset(base_dst.as_uri()),
        ]
    )
    def confirm_ingest(base_path):
        """List files in remote object storage."""
        path = base_path
        folders = [f for f in path.iterdir() if f.is_dir()]
        for folder in folders:
            t_log.info(f"Folder: {folder}")
            files = [f for f in folder.iterdir() if f.is_file()]
            for file in files:
                t_log.info(f"File: {file}")

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        create_bucket,
        get_new_sales_from_api_obj,
        write_to_s3_obj,
        confirm_ingest(base_path=base_dst),
    )


extract_from_api()
