from airflow.sdk import dag, task
from datetime import datetime

@dag(start_date=datetime(2025, 1, 1), schedule="@daily", tags=["bad_dag", "idempotency"]) 
def bad_dag_4():

    @task 
    def get_today_date():
        return datetime.now().date()
    
    get_today_date()
    
bad_dag_4()