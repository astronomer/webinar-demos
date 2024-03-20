"""
## Orchestrate Azure Data Factory ingestion pipelines

This DAG makes a param-based decision on whether or not an ADF
pipeline should be run that ingests data from a blob storage to an Azure SQL database.
Afterwards a data quality check is performed and an Airflow dataset is updated.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
from pendulum import datetime


@dag(
    start_date=datetime(2024, 3, 1),
    schedule="@daily",
    catchup=False,
    params={
        "run_adf_ingestion": Param(
            True,
            type="boolean",
            description_md="Set to True to run ADF ingestion, False to skip it. Default is True.",
        )
    },
)
def ingest_bunny_data():

    @task.branch
    def get_adf_update_decision(**context):
        adf_update_decision = context["params"]["run_adf_ingestion"]
        if adf_update_decision:
            return "get_pipelines_to_run"
        else:
            return "skip_adf_update"

    skip_adf_update = EmptyOperator(task_id="skip_adf_update")

    @task
    def get_pipelines_to_run():
        import json

        file_path = "include/data_sources.json"
        with open(file_path, "r") as file:
            bunny_list = json.load(file)

        return bunny_list

    get_pipelines_to_run_obj = get_pipelines_to_run()

    chain(get_adf_update_decision(), [get_pipelines_to_run_obj, skip_adf_update])

    run_extraction_pipelines = AzureDataFactoryRunPipelineOperator.partial(
        task_id="run_extraction_pipelines",
        azure_data_factory_conn_id="azure_data_factory",
        factory_name="BunnyDF",
        resource_group_name="airflow-adf-webinar",
    ).expand(pipeline_name=get_pipelines_to_run_obj)

    check_fav_food = SQLTableCheckOperator(
        task_id="check_fav_food",
        conn_id="azure_sql_bunnydb",
        table="BunnyObservations",
        checks={
            "fav_foods_check": {
                "check_statement": "FavFood IN ('carrots', 'lettuce', 'kale', 'apples')",
            },
        },
        trigger_rule="none_failed",
        on_failure_callback=lambda context: print("FavFood check failed"),
        outlets=[Dataset("azuresql://bunny_data")],
    )

    chain(
        get_pipelines_to_run_obj,
        run_extraction_pipelines,
        check_fav_food,
    )

    chain(skip_adf_update, check_fav_food)


ingest_bunny_data()
