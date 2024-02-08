"""
## Load to Snowflake

This DAG loads data from the loading location in S3 to Snowflake.
To use this DAG you need a stage for each source in Snowflake.

For example for 3 sources you would need to create these 3 stages:

```sql
    CREATE STAGE sales_reports_stage
    URL = 's3://ce-2-8-examples-bucket/load/sales_reports/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

    CREATE STAGE customer_feedback_stage
    URL = 's3://ce-2-8-examples-bucket/load/customer_feedback/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

    CREATE STAGE customer_data_stage
    URL = 's3://ce-2-8-examples-bucket/load/customer_data/'
    CREDENTIALS = (AWS_KEY_ID = '<your aws key id>' AWS_SECRET_KEY = '<your aws secret>')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

"""

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
import json
import os

with open("include/dynamic_dag_generation/ingestion_source_config.json", "r") as f:
    config = json.load(f)

    ingestion_datasets = [
        Dataset(source["dataset_uri"]) for source in config["sources"]
    ]

SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_de_team")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=ingestion_datasets,
    catchup=False,
    tags=["load", "HappyWoofs"],
    default_args={"owner": "Piglet", "retries": 3, "retry_delay": 5},
    description="Load data from S3 to Snowflake",
    doc_md=__doc__,
)
def load_to_snowflake():
    create_file_format = SnowflakeOperator(
        task_id="create_file_format",
        sql="""
            CREATE FILE FORMAT IF NOT EXISTS my_csv_format 
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    for source_name, table_creation_sql in [
        (source["source_name"], source["table_creation_sql"])
        for source in config["sources"]
    ]:
        create_table_if_not_exists = SnowflakeOperator(
            task_id=f"create_table_if_not_exists_{source_name}",
            sql=table_creation_sql,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
        )

        load_data = SnowflakeOperator(
            task_id=f"load_{source_name}",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
            COPY INTO {source_name}_table
            FROM @{source_name}_stage
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
            """,
            outlets=[Dataset(f"snowflake://{source_name}_table")],
        )

        chain(
            create_file_format,
            create_table_if_not_exists,
            load_data,
        )


load_to_snowflake()
