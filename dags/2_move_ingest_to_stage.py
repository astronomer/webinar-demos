import logging
import os

from airflow.sdk import ObjectStoragePath, dag, task, chain, Asset
from pendulum import datetime

from include.utils import get_all_files, copy_recursive

logger = logging.getLogger("airflow.task")

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "etl-with-snowflake-demo-1234")

# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
PATH_INGEST = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-ingest", conn_id=AWS_CONN_ID)
PATH_STAGE = ObjectStoragePath(f"s3://{S3_BUCKET_NAME}/tea-sales-stage", conn_id=AWS_CONN_ID)


@dag(
    dag_display_name="2 - Move data from ingest to stage",
    start_date=datetime(2025, 11, 1),
    schedule=Asset(PATH_INGEST.as_uri())  # trigger on new ingest data
)
def move_ingest_to_stage():

    @task
    def list_ingest_folders(base_path: ObjectStoragePath) -> list[ObjectStoragePath]:
        path = base_path
        folders = [f for f in path.iterdir() if f.is_dir()]
        return folders

    @task(
        map_index_template="{{ custom_map_index }}",
        outlets=[Asset(PATH_STAGE.as_uri())]
    )
    def copy_ingest_to_stage(path_src: ObjectStoragePath, base_dst: ObjectStoragePath) -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        context["custom_map_index"] = os.path.join(*path_src.parts[-2:])

        copy_recursive(path_src, base_dst)

    @task
    def del_files_from_ingest(base_src: ObjectStoragePath) -> None:
        for f in get_all_files(base_src):
            f.unlink()

    _copy_ingest_to_stage = copy_ingest_to_stage.partial(base_dst=PATH_STAGE).expand(
        path_src=list_ingest_folders(base_path=PATH_INGEST)
    )

    _del_files_from_ingest = del_files_from_ingest(base_src=PATH_INGEST)
    chain(_copy_ingest_to_stage, _del_files_from_ingest)


move_ingest_to_stage()
