from airflow.sdk import dag, task
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily", tags=["good_dag", "idempotency"]) 
def good_dag_4_idempotency():

    @task
    def get_today_date(ds=None):
        return ds
    
    get_today_date()
    
good_dag_4_idempotency()