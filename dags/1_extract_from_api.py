import logging
import os

import pandas as pd
from airflow.models import DagRun
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.sdk import ObjectStoragePath, Param, task, chain, dag, Asset
from pendulum import datetime

logger = logging.getLogger("airflow.task")

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "etl-with-snowflake-demo-1234")

# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
PATH_INGEST = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-ingest", conn_id=AWS_CONN_ID)


@dag(
    dag_display_name="1 - Extract data and load it to S3",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=3,  # auto-pauses the DAG after 3 consecutive failed runs
    params={
        "num_sales": Param(
            100,
            description="The number of sales to fetch from the API.",
            type="number",
        ),
    }
)
def extract_from_api():

    # create the S3 bucket if it does not exist yet
    _create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )

    @task
    def get_new_sales_from_api() -> list[pd.DataFrame]:
        from airflow.sdk import get_current_context
        context = get_current_context()

        num_sales = context["params"]["num_sales"]
        date = context["ts"]
        from include.api_functions import get_new_sales_from_internal_api

        sales_df, users_df, teas_df, utm_df = get_new_sales_from_internal_api(num_sales, date)

        logger.info(f"Fetching {num_sales} new sales from the internal API.")
        logger.info(f"Head of the new sales data: {sales_df.head()}")
        logger.info(f"Head of the new users data: {users_df.head()}")
        logger.info(f"Head of the new teas data: {teas_df.head()}")
        logger.info(f"Head of the new utm data: {utm_df.head()}")

        return [
            {"name": "sales", "data": sales_df},
            {"name": "users", "data": users_df},
            {"name": "teas", "data": teas_df},
            {"name": "utms", "data": utm_df},
        ]

    @task(map_index_template="{{ custom_map_index }}")
    def write_to_s3(base_dst: ObjectStoragePath, data_to_write: pd.DataFrame, dag_run: DagRun):
        import io
        from airflow.sdk import get_current_context

        data = data_to_write["data"]
        name = data_to_write["name"]

        csv_buffer = io.BytesIO()
        data.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue()

        path_dst = base_dst / name / f"{dag_run.run_id}.csv"
        path_dst.write_bytes(csv_bytes)

        context = get_current_context()
        context["custom_map_index"] = f"{name} data"

    _get_new_sales_from_api = get_new_sales_from_api()
    _write_to_s3 = write_to_s3.partial(base_dst=PATH_INGEST).expand(
        data_to_write=_get_new_sales_from_api
    )

    @task(
        outlets=[Asset(PATH_INGEST.as_uri())]
    )
    def log_files(base_path) -> None:
        path = base_path
        folders = [f for f in path.iterdir() if f.is_dir()]
        for folder in folders:
            logger.info(f"Folder: {folder}")
            files = [f for f in folder.iterdir() if f.is_file()]
            for file in files:
                logger.info(f"File: {file}")

    chain(
        _create_bucket,
        _get_new_sales_from_api,
        _write_to_s3,
        log_files(base_path=PATH_INGEST),
    )


extract_from_api()
