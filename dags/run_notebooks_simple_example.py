"""
### Run notebooks in databricks as a Databricks Workflow using the Astro Databricks provider

This DAG runs two Databricks notebooks as a Databricks workflow.
"""

import os

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
)
from pendulum import datetime

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


DATABRICKS_JOB_CLUSTER_KEY = "test-cluster"

_DBX_CONN_ID = os.getenv("DBX_CONN_ID")


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
    dag_display_name="Run DBX Workflow - Simple Example",
    start_date=datetime(2024, 11, 6),
    schedule=[Dataset("dbx://hive_metastore.default.facilityefficiency")],
    catchup=False,
    doc_md=__doc__,
    tags=["DBX"],
)
def run_notebooks_simple_example():
    dbx_workflow_task_group = DatabricksWorkflowTaskGroup(
        group_id="databricks_workflow",
        databricks_conn_id=_DBX_CONN_ID,
        job_clusters=job_cluster_spec,
    )

    with dbx_workflow_task_group:

        extract_green_manufacturing = DatabricksNotebookOperator(
            task_id="extract_green_manufacturing",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_EX_GREEN,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

        transform_green_manufacturing = DatabricksNotebookOperator(
            task_id="transform_green_manufacturing",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_GREEN,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

        extract_notgreen_manufacturing = DatabricksNotebookOperator(
            task_id="extract_notgreen_manufacturing",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_EX_NOT_GREEN,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

        transform_notgreen_manufacturing = DatabricksNotebookOperator(
            task_id="transform_notgreen_manufacturing",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_TRANSFORM_NOT_GREEN,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )

        analytics = DatabricksNotebookOperator(
            task_id="analytics",
            databricks_conn_id=_DBX_CONN_ID,
            notebook_path=_DATABRICKS_NOTEBOOK_PATH_ANALYTICS,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
            outlets=[Dataset("dbx://analytics")],
        )

        chain(extract_green_manufacturing, transform_green_manufacturing)

        chain(extract_notgreen_manufacturing, transform_notgreen_manufacturing)
        chain(
            [transform_green_manufacturing, transform_notgreen_manufacturing], analytics
        )


run_notebooks_simple_example()
