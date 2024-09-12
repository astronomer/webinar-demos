"""
### ETL: Move data from ingest to stage
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import logging
import os

# import modularized functions from the include folder
from include.utils import get_all_files, get_all_checksums, compare_checksums

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET_NAME")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "tea-sales-ingest")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "tea-sales-stage")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME


BASE_SRC = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="Move data from ingest to stage",
    start_date=datetime(2024, 8, 1),
    schedule=[
        Dataset(BASE_SRC.as_uri()),
    ],
    catchup=False,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["S3"],
)
def move_ingest_to_stage():

    @task
    def list_ingest_folders(base_path: ObjectStoragePath) -> list[ObjectStoragePath]:
        """List files in remote object storage."""
        path = base_path
        folders = [f for f in path.iterdir() if f.is_dir()]
        return folders

    @task(map_index_template="{{ my_custom_map_index }}")
    def copy_ingest_to_stage(
        path_src: ObjectStoragePath, base_dst: ObjectStoragePath
    ) -> None:
        """Copy a file from remote to local storage.
        The file is streamed in chunks using shutil.copyobj"""

        for f in path_src.iterdir():
            full_key = base_dst / os.path.join(*f.parts[-2:])
            t_log.info(f"Copying {f} to {full_key}")
            f.copy(dst=full_key)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = (
            f"Copying files from {os.path.join(*path_src.parts[-2:])}."
        )

    @task(outlets=Dataset(BASE_DST.as_uri() + "/"))
    def verify_checksum(
        base_src: ObjectStoragePath,
        base_dst: ObjectStoragePath,
        folder_name_src: str,
        folder_name_dst: str,
    ):
        """Compares checksums to verify correct file copy to stage.
        Raises an exception in case of any mismatches"""

        folder_src = base_src
        folder_dst = base_dst

        src_files = get_all_files(folder_src)
        dst_files = get_all_files(folder_dst)

        src_checksums = get_all_checksums(path=folder_src, files=src_files)
        dst_checksums = get_all_checksums(path=folder_dst, files=dst_files)

        compare_checksums(
            src_checksums=src_checksums,
            dst_checksums=dst_checksums,
            folder_name_src=folder_name_src,
            folder_name_dst=folder_name_dst,
        )

    @task
    def del_files_from_ingest(base_src: ObjectStoragePath):
        path = base_src
        files = get_all_files(path)
        for f in files:
            f.unlink()

    folders = list_ingest_folders(base_path=BASE_SRC)
    copy_ingest_to_stage_obj = copy_ingest_to_stage.partial(base_dst=BASE_DST).expand(
        path_src=folders
    )
    verify_checksum_obj = verify_checksum(
        base_src=BASE_SRC,
        base_dst=BASE_DST,
        folder_name_src=_INGEST_FOLDER_NAME,
        folder_name_dst=_STAGE_FOLDER_NAME,
    )
    del_files_from_ingest_obj = del_files_from_ingest(base_src=BASE_SRC)

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(copy_ingest_to_stage_obj, verify_checksum_obj, del_files_from_ingest_obj)


move_ingest_to_stage()
