from airflow.sdk import dag, task, Asset, chain
from pendulum import datetime


@dag(start_date=datetime(2025, 7, 1), schedule="@daily")
def etl_pii_sensitive_dag():

    @task(queue="sensitive-on-prem")
    def clean_pii_from_db():
        print("Cleaning PII from DB")

    @task(queue="default", outlets=[Asset("cleaned_data_ready")])
    def process_cleaned_data():
        print("Processing cleaned data")

    chain(clean_pii_from_db(), process_cleaned_data())


etl_pii_sensitive_dag()
