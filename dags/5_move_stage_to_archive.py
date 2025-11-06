import logging
import os

from airflow.sdk import ObjectStoragePath, dag, task, chain, Asset
from pendulum import datetime

from include.utils import get_all_files, copy_recursive

logger = logging.getLogger("airflow.task")

SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "etl-with-snowflake-demo-1234")

# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
PATH_INGEST = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-ingest", conn_id=AWS_CONN_ID)
PATH_STAGE = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-stage", conn_id=AWS_CONN_ID)
PATH_ARCHIVE = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-archive", conn_id=AWS_CONN_ID)

@dag(
    dag_display_name="5 - Archive raw sales data",
    start_date=datetime(2024, 8, 1),
    schedule=Asset(f"snowflake://{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}")
)
def move_stage_to_archive():

    @task
    def list_folders(base_path: ObjectStoragePath) -> list[ObjectStoragePath]:
        path = base_path
        return [f for f in path.iterdir() if f.is_dir()]

    @task(map_index_template="{{ custom_map_index }}")
    def copy_folder(
        path_src: ObjectStoragePath, base_dst: ObjectStoragePath
    ) -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        context["custom_map_index"] = os.path.join(*path_src.parts[-2:])

        copy_recursive(path_src, base_dst)

    @task
    def delete_files(base_src: ObjectStoragePath):
        for f in get_all_files(base_src):
            f.unlink()

    _copy_stage_to_archive = copy_folder.partial(base_dst=PATH_ARCHIVE).expand(
        path_src=list_folders(base_path=PATH_STAGE)
    )
    _delete_stage_files = delete_files(base_src=PATH_STAGE)

    chain(_copy_stage_to_archive, _delete_stage_files)


move_stage_to_archive()
