"""
## Load local files to S3

Helper DAG to set up the demo. 
"""

import logging
import os

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from pendulum import datetime

t_log = logging.getLogger("airflow.task")

_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "your-bucket")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "your-ingest-folder")

OBJECT_STORAGE_SRC = "file"
CONN_ID_SRC = None
KEY_SRC = "include/data"

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
base_dst = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)


@dag(
    dag_display_name="Load sample data to S3",
    start_date=datetime(2024, 11, 6),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["helper"],
)
def setup_sample_data():

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket", aws_conn_id=_AWS_CONN_ID, bucket_name=_S3_BUCKET
    )

    @task
    def list_folders_sample_data(
        path_src: ObjectStoragePath,
    ) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        files = [f for f in path_src.iterdir()]
        return files

    folders_sample_data = list_folders_sample_data(path_src=base_src)

    @task(map_index_template="{{ custom_map_index }}")
    def copy_local_to_remote(path_src: ObjectStoragePath, base_dst: ObjectStoragePath):
        """Copy files from local storage to remote object storage."""

        full_key = base_dst / os.path.join(*path_src.parts[-1:])
        path_src.copy(dst=full_key)
        t_log.info(f"Successfully wrote {full_key} to remote storage!")

        # set custom map index
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["custom_map_index"] = f"File copied to: {full_key}"

    @task
    def sample_data_in():
        t_log.info("Sample data loaded to remote storage!")

    chain(
        [create_bucket, folders_sample_data],
        copy_local_to_remote.partial(base_dst=base_dst).expand(
            path_src=folders_sample_data
        ),
        sample_data_in(),
    )


setup_sample_data()
