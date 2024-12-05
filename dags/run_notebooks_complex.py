"""
### Run notebooks in databricks as a Databricks Workflow using the Astro Databricks provider

This DAG runs two Databricks notebooks as a Databricks workflow.
"""

import os

from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
)
from pendulum import datetime

from include.custom_operators import SnowflakeOperator
from include.sql_statements import ex_sql

_DATABRICKS_FOLDER = os.getenv("DATABRICKS_FOLDER", "default")

_DATABRICKS_NOTEBOOK_PATH_EX_GREEN = (
    f"/Users/{_DATABRICKS_FOLDER}/extract_green_manufacturing"
)
_DATABRICKS_NOTEBOOK_PATH_EX_NOT_GREEN = (
    f"/Users/{_DATABRICKS_FOLDER}/extract_notgreen_manufacturing"
)
_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_GREEN = (
    f"/Users/{_DATABRICKS_FOLDER}/transform_green_manufacturing"
)
_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_NOT_GREEN = (
    f"/Users/{_DATABRICKS_FOLDER}/transform_notgreen_manufacturing"
)
_DATABRICKS_NOTEBOOK_PATH_ANALYTICS = f"/Users/{_DATABRICKS_FOLDER}/analytics"


DATABRICKS_JOB_CLUSTER_KEY = "test-cluster-3"

_DBX_CONN_ID = os.getenv("DBX_CONN_ID", "databricks_default")
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_SQL = os.getenv("SNOWFLAKE_SQL", "SELECT 1")


job_cluster_spec = [
    {
        "job_cluster_key": DATABRICKS_JOB_CLUSTER_KEY,
        "new_cluster": {
            "cluster_name": "",
            "spark_version": "15.3.x-cpu-ml-scala2.12",
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "eu-central-1",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "i3.xlarge",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "enable_elastic_disk": False,
            "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
            "runtime_engine": "STANDARD",
            "num_workers": 1,
        },
    }
]


@dag(
    dag_display_name="Run DBX Workflow - Fun version",
    start_date=datetime(2024, 11, 6),
    schedule=[Dataset("dbx://hive_metastore.default.facilityefficiency")],
    catchup=False,
    doc_md=__doc__,
    tags=["DBX"],
    params={
        "extraction_department": Param(
            "Manufacturing",
            type="string",
            enum=[
                "Manufacturing",
                "Quality Control",
                "Research & Development",
                "Logistics",
            ],
        ),
    },
)
def run_notebooks_complex():

    dbx_workflow_task_group = DatabricksWorkflowTaskGroup(
        group_id="databricks_workflow",
        databricks_conn_id=_DBX_CONN_ID,
        job_clusters=job_cluster_spec,
        notebook_params={"extraction_department": "{{ params.extraction_department }}"},
    )

    with dbx_workflow_task_group:

        @task_group
        def green():
            extract_green = DatabricksNotebookOperator(
                task_id="extract_green",
                databricks_conn_id=_DBX_CONN_ID,
                notebook_path=_DATABRICKS_NOTEBOOK_PATH_EX_GREEN,
                source="WORKSPACE",
                job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            )

            transform_green = DatabricksNotebookOperator(
                task_id="transform_green",
                databricks_conn_id=_DBX_CONN_ID,
                notebook_path=_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_GREEN,
                source="WORKSPACE",
                job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            )

            chain(extract_green, transform_green)

            return extract_green, transform_green

        green_obj = green()

        @task_group
        def notgreen():

            extract_notgreen = DatabricksNotebookOperator(
                task_id="extract_notgreen",
                databricks_conn_id=_DBX_CONN_ID,
                notebook_path=_DATABRICKS_NOTEBOOK_PATH_EX_NOT_GREEN,
                source="WORKSPACE",
                job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            )

            transform_notgreen = DatabricksNotebookOperator(
                task_id="transform_notgreen",
                databricks_conn_id=_DBX_CONN_ID,
                notebook_path=_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_NOT_GREEN,
                source="WORKSPACE",
                job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            )
            chain(extract_notgreen, transform_notgreen)

            return extract_notgreen, transform_notgreen

        notgreen_obj = notgreen()

        analytics = DatabricksNotebookOperator(
            task_id="analytics",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_ANALYTICS,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            outlets=[Dataset("dbx://analytics")],
        )

        chain([green_obj[1], notgreen_obj[1]], analytics)

    @task
    def jdbc_to_snowflake_green(data_uri):
        from include.custom_func import run_jdbc_conn_dbx_snowflake

        run_jdbc_conn_dbx_snowflake(data_uri)

    stage_in_snowflake_green = SnowflakeOperator(
        task_id="stage_in_snowflake_green",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql=_SNOWFLAKE_SQL,
    )

    @task
    def jdbc_to_snowflake_notgreen(data_uri):
        from include.custom_func import run_jdbc_conn_dbx_snowflake

        run_jdbc_conn_dbx_snowflake(data_uri)

    stage_in_snowflake_notgreen = SnowflakeOperator(
        task_id="stage_in_snowflake_notgreen",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql=_SNOWFLAKE_SQL,
    )

    prep_report_snowflake = SnowflakeOperator(
        task_id="prep_report_snowflake",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql=ex_sql,
    )

    chain(
        green_obj[0],
        jdbc_to_snowflake_green(data_uri=_DATABRICKS_NOTEBOOK_PATH_EX_GREEN),
        stage_in_snowflake_green,
    )

    chain(
        notgreen_obj[1],
        jdbc_to_snowflake_notgreen(
            data_uri=_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_NOT_GREEN
        ),
        stage_in_snowflake_notgreen,
    )

    chain(
        [stage_in_snowflake_green, stage_in_snowflake_notgreen], prep_report_snowflake
    )


run_notebooks_complex()
