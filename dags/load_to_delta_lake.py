"""
## Load data from S3 to Databricks Delta Lake

This DAG retrieves a csv file from S3, uses the column naming conventions
to infer the schema, and creates a Delta Lake table in Databricks.
Subsequently, the data is copied into the Delta Lake table.
"""

import logging
import os

import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.databricks.operators.databricks_sql import (
    DatabricksCopyIntoOperator,
    DatabricksSqlOperator,
)
from pendulum import datetime

t_log = logging.getLogger("airflow.task")

_AWS_CONN_ID = os.getenv("AWS_CONN_ID")
_S3_BUCKET = os.getenv("S3_BUCKET")
_DBX_CONN_ID = os.getenv("DBX_CONN_ID")
_DBX_WH_HTTP_PATH = os.getenv("DBX_WH_HTTP_PATH")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")
_S3_BUCKET_ACCESS_ROLE_ARN = os.getenv("S3_BUCKET_ACCESS_ROLE_ARN")

OBJECT_STORAGE_DST = "s3"
KEY_DST = _S3_BUCKET + "/" + _INGEST_FOLDER_NAME

base_s3 = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=_AWS_CONN_ID)


@dag(
    dag_display_name="Load data S3 to Delta Lake 🐟",
    start_date=datetime(2024, 11, 6),
    schedule="@daily",
    catchup=False,
    tags=["DBX"],
)
def load_to_delta_lake():

    @task
    def get_filepaths_s3(
        path_src: ObjectStoragePath,
    ) -> list[ObjectStoragePath]:
        """List files in local object storage."""
        files = [f for f in path_src.iterdir()]
        return files

    get_file_paths_s3_obj = get_filepaths_s3(path_src=base_s3)

    @task_group
    def load_s3_to_delta_lake(file_path):

        @task
        def get_table_name(file_path):
            """Use file name from S3 as DBX table name"""
            return file_path.parts[-1:][0].split(".")[0]

        get_table_name_obj = get_table_name(file_path=file_path)

        @task
        def get_schema(
            file_path: ObjectStoragePath,
        ) -> list[ObjectStoragePath]:
            """List files in local object storage."""
            from io import StringIO

            bytes = file_path.read_block(offset=0, length=None)
            content = bytes.decode("utf-8")

            df = pd.read_csv(StringIO(content))

            columns = []
            for col in df.columns:
                if col.startswith("Is"):
                    col_type = "BOOLEAN"
                elif col.startswith("Amt"):
                    col_type = "DOUBLE"
                elif col.startswith("TS"):
                    col_type = "DATE"
                else:
                    col_type = "STRING"
                columns.append(f"{col} {col_type}")

            columns_str = ",\n    ".join(columns)

            return columns_str

        get_schema_obj = get_schema(file_path=file_path)

        @task
        def get_sql_table_creation(table_name, schema):

            sql = (
                f"""
                    CREATE table IF NOT EXISTS DEFAULT.{table_name}(
                    {schema}
                    )
                    USING DELTA
                """,
            )

            return sql

        get_sql_table_creation_obj = get_sql_table_creation(
            table_name=get_table_name_obj, schema=get_schema_obj
        )

        @task
        def get_file_path_uri(file_path):
            return file_path.as_uri()

        @task
        def get_tmp_creds(role_arn):
            from airflow.models import Variable

            session_name = "airflow-session"
            hook = AwsGenericHook(aws_conn_id="aws_default")
            client = hook.get_session().client("sts")
            response = client.assume_role(
                RoleArn=role_arn, RoleSessionName=session_name
            )

            credentials = response["Credentials"]

            Variable.set(key="AWSACCESSKEYTMP", value=credentials["AccessKeyId"])
            Variable.set(key="AWSSECRETKEYTMP", value=credentials["SecretAccessKey"])
            Variable.set(key="AWSSESSIONTOKEN", value=credentials["SessionToken"])

        get_file_path_uri_obj = get_file_path_uri(file_path=file_path)

        get_tmp_creds_obj = get_tmp_creds(role_arn=_S3_BUCKET_ACCESS_ROLE_ARN)

        create_table_delta_lake = DatabricksSqlOperator(
            task_id="create_table_delta_lake",
            databricks_conn_id=_DBX_CONN_ID,
            http_path=_DBX_WH_HTTP_PATH,
            sql=get_sql_table_creation_obj,
        )

        s3_to_delta_lake = DatabricksCopyIntoOperator(
            task_id="s3_to_delta_lake",
            databricks_conn_id=_DBX_CONN_ID,
            table_name=get_table_name_obj,
            file_location=get_file_path_uri_obj,
            file_format="CSV",
            format_options={"header": "true", "inferSchema": "true"},
            force_copy=True,
            http_path=_DBX_WH_HTTP_PATH,
            credential={
                "AWS_ACCESS_KEY": Variable.get("AWSACCESSKEYTMP", "Notset"),
                "AWS_SECRET_KEY": Variable.get("AWSSECRETKEYTMP", "Notset"),
                "AWS_SESSION_TOKEN": Variable.get("AWSSESSIONTOKEN", "Notset"),
            },
            copy_options={"mergeSchema": "true"},
            outlets=[Dataset("dbx://hive_metastore.default.facilityefficiency")],
        )

        chain(
            get_sql_table_creation_obj,
            [create_table_delta_lake, get_tmp_creds_obj],
            s3_to_delta_lake,
        )

    load_s3_to_delta_lake.expand(file_path=get_file_paths_s3_obj)


load_to_delta_lake()
