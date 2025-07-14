from airflow.sdk import dag, task, Asset
from pendulum import datetime

@dag(start_date=datetime(2025, 7, 1), schedule="@daily") 
def etl_pii_sensitive_dag():

    @task(queue="sensitive-on-prem", outlets=[Asset("pii_cleaned")])
    def clean_pii_from_db():
        print("Cleaning PII from DB")

    clean_pii_from_db()

etl_pii_sensitive_dag()