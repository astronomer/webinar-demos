"""
### ETL: Archive raw sales data
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

# Snowflake variables
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

# S3 variables
_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET_NAME")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "tea-sales-ingest")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "tea-sales-stage")
_ARCHIVE_FOLDER_NAME = os.getenv("ARCHIVE_FOLDER_NAME", "tea-sales-archive")

# Creating ObjectStoragePath objects
# See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html
# for more information on the Airflow Object Storage feature
OBJECT_STORAGE_INGEST = "s3"
CONN_ID_INGEST = _AWS_CONN_ID
KEY_INGEST = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

OBJECT_STORAGE_SRC = "s3"
CONN_ID_SRC = _AWS_CONN_ID
KEY_SRC = _S3_BUCKET + "/" + _STAGE_FOLDER_NAME

OBJECT_STORAGE_DST = "s3"
CONN_ID_DST = _AWS_CONN_ID
KEY_DST = _S3_BUCKET + "/" + _ARCHIVE_FOLDER_NAME


BASE_SRC = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}", conn_id=CONN_ID_SRC)
BASE_DST = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)
BASE_INGEST = ObjectStoragePath(
    f"{OBJECT_STORAGE_INGEST}://{KEY_INGEST}", conn_id=CONN_ID_INGEST
)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🗄️ Archive raw sales data",
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}")],
    catchup=False,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["archive", "S3"],
)
def move_stage_to_archive():

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
    def del_all_files_from_stage(base_src: ObjectStoragePath):
        path = base_src
        files = get_all_files(path)
        for f in files:
            f.unlink()

    @task
    def del_processed_files_from_ingest(
        base_ingest: ObjectStoragePath, base_archive: ObjectStoragePath
    ):
        # delete all files that are in the archive folder from the ingest folder
        ingest_files = get_all_files(base_ingest)
        archive_files = get_all_files(base_archive)
        for f in ingest_files:
            if f in archive_files:
                f.unlink()

    folders = list_ingest_folders(base_path=BASE_SRC)
    copy_ingest_to_stage_obj = copy_ingest_to_stage.partial(base_dst=BASE_DST).expand(
        path_src=folders
    )
    verify_checksum_obj = verify_checksum(
        base_src=BASE_SRC,
        base_dst=BASE_DST,
        folder_name_src=_STAGE_FOLDER_NAME,
        folder_name_dst=_ARCHIVE_FOLDER_NAME,
    )
    del_all_files_from_stage_obj = del_all_files_from_stage(base_src=BASE_SRC)
    del_processed_files_from_ingest_obj = del_processed_files_from_ingest(
        base_ingest=BASE_INGEST, base_archive=BASE_DST
    )

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        copy_ingest_to_stage_obj,
        verify_checksum_obj,
        [del_all_files_from_stage_obj, del_processed_files_from_ingest_obj],
    )


move_stage_to_archive()
