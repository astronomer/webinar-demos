"""
## Run notebooks in databricks as a Databricks Workflow using the Astro Databricks provider

This DAG runs two Databricks notebooks as a Databricks workflow.
"""

from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_workflow import (
    DatabricksWorkflowTaskGroup,
)
from pendulum import datetime

DATABRICKS_LOGIN_EMAIL = "<your-email>"
DATABRICKS_NOTEBOOK_NAME_1 = "notebook1"
DATABRICKS_NOTEBOOK_NAME_2 = "notebook2"
DATABRICKS_NOTEBOOK_PATH_1 = (
    f"/Users/{DATABRICKS_LOGIN_EMAIL}/{DATABRICKS_NOTEBOOK_NAME_1}"
)
DATABRICKS_NOTEBOOK_PATH_2 = (
    f"/Users/{DATABRICKS_LOGIN_EMAIL}/{DATABRICKS_NOTEBOOK_NAME_2}"
)
DATABRICKS_JOB_CLUSTER_KEY = "tutorial-cluster"
DATABRICKS_CONN_ID = "databricks_default"


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


@dag(start_date=datetime(2024, 11, 6), schedule=None, catchup=False)
def my_simple_databricks_dag():
    task_group = DatabricksWorkflowTaskGroup(
        group_id="databricks_workflow",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_spec,
    )

    with task_group:
        notebook_1 = DatabricksNotebookOperator(
            task_id="notebook1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=DATABRICKS_NOTEBOOK_PATH_1,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )
        notebook_2 = DatabricksNotebookOperator(
            task_id="notebook2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path=DATABRICKS_NOTEBOOK_PATH_2,
            source="WORKSPACE",
            job_cluster_key=DATABRICKS_JOB_CLUSTER_KEY,
        )
        notebook_1 >> notebook_2


my_simple_databricks_dag()
