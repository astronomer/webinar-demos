from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json

def process_response(response):
    print("Processing Astronaut data")


with DAG(
    'api_to_snowflake',
    start_date=None,
    description='A DAG to fetch data from API and store in Snowflake',
    schedule=None,
) as dag:
    
    fetch_data = HttpOperator(
        task_id='fetch_data',
        http_conn_id='api_conn',
        method='GET',
        headers={"Content-Type": "application/json"}
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_response,
        op_args=['{{ ti.xcom_pull(task_ids="fetch_data") }}'],
    )

    save_to_snowflake = SnowflakeOperator(
        task_id='save_to_snowflake',
        snowflake_conn_id='snowflake_dev_conn',
        sql='your-sql-query',
        parameters={
            'data': '{{ ti.xcom_pull(task_ids="process_data") }}'
        }
    )

    fetch_data >> process_data >> save_to_snowflake