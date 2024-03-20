# How to orchestrate Azure Data Factory jobs with Airflow - webinar demo

This repository contains the code for the webinar demo shown in: How to orchestrate Azure Data Factory jobs with Airflow

[Watch the webinar here for free!](https://www.astronomer.io/events/webinars/how-to-orchestrate-azure-data-factory-jobs-with-airflow-video/)

## Content

This repository contains:

- A DAG orchestrating an Ingestion pipeline from Azure Data Factory with dynamic task mapping to start pipelines listed in a configuration file. The pipeline moves data from Azure Blob Storage to an Azure SQL Database, then performs a data quality check. 
- A DAG orchestrating a ML pipeline in Azure Data Factory. The feature importance values are saved to a Azure FileShare and retrieved into Airflow.

## How to orchestrate your own Azure Data Factory pipelines with Airflow

1. Clone the repository.
2. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed and that [Docker](https://www.docker.com/products/docker-desktop) is running.
3. Copy the `.env.example` file to a new file called `.env` and fill in your own credentials for the `AIRFLOW_CONN_AZURE_DATA_FACTORY` connection. You will need to create an Application for Airflow to connect to. See [Create an Azure Data Factory connection in Airflow](https://docs.astronomer.io/learn/connections/azure-data-factory).
4. Use the tasks defined with the AzureDataFactoryRunPipelineOperator and provide your own pipeline name and configurations.
5. Run `astro dev start` to start the Airflow instance. The webserver with the Airflow UI will be available at `localhost:8080`. Log in with the credentials `admin:admin`.
6. Trigger the DAG run manually by clicking the play button in the Airflow UI.


## Resources

- [Create an Azure Data Factory connection in Airflow](https://docs.astronomer.io/learn/connections/azure-data-factory).
- [Run Azure Data Factory pipelines with Airflow](https://docs.astronomer.io/learn/airflow-azure-data-factory-integration)
- [Azure Data Factory documentation](https://learn.microsoft.com/en-us/azure/data-factory/)