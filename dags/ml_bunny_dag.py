"""
## Run a Machine Learning pipeline in Azure Data Factory

This DAG runs a pipeline in Azure Data Factory that trains a regression model 
to predict the number of hops per hour for bunnies based on their attributes.
The feature importance is then extracted and stored in a file share and 
the results are read back into Airflow.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.models.baseoperator import chain
from pendulum import datetime
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook
import os

FILESHARE_NAME = os.getenv("AZURE_FILE_SHARE_NAME")
FILESHARE_RESULTS_PATH = os.getenv("AZURE_FILE_SHARE_RESULTS_PATH")


@dag(
    start_date=datetime(2024, 3, 1),
    schedule=[Dataset("azuresql://bunny_data")],
    catchup=False,
)
def ml_bunny_dag():

    run_ml_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_ml_pipeline",
        azure_data_factory_conn_id="azure_data_factory",
        factory_name="BunnyDF",
        resource_group_name="airflow-adf-webinar",
        pipeline_name="BunnyML",
    )

    @task
    def read_file_from_azure(
        file_path: str, share_name: str, azure_fileshare_conn_id="azure_fileshare"
    ) -> str:
        """
        Reads a file from an Azure File Share and returns its content.
        Args:
            file_path (str): The path to the file in the file share.
            share_name (str): The name of the file share.
            azure_fileshare_conn_id (str): The connection id for the Azure File Share.
        Returns:
            str: The content of the file.
        """
        import os

        hook = AzureFileShareHook(
            azure_fileshare_conn_id=azure_fileshare_conn_id,
            share_name=share_name,
            file_path=file_path,
        )

        os.makedirs("include", exist_ok=True)
        with open("include/results.txt", "w") as file:
            file.write("")

        hook.get_file(file_path="include/results.txt")

        with open("include/results.txt", "r") as file:
            content = file.read()

        return content

    chain(
        run_ml_pipeline,
        read_file_from_azure(
            file_path=FILESHARE_RESULTS_PATH, share_name=FILESHARE_NAME
        ),
    )


ml_bunny_dag()
