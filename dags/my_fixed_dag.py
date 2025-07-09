"""Fixed version of my_legacy_dag.py. Only runs in Airflow 3"""


from airflow.sdk import dag, task, Asset
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

URL = "https://manateejokesapi.herokuapp.com/manatees/random"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False, # Not needed in Airflow 3.0 :) 
)
def my_fixed_dag():

    @task
    def extract(logical_date):
        import requests
        response = requests.get(URL)
        response["timestamp"] = logical_date.isoformat()
        return response.json()
    
    @task
    def transform(data):
        return data["joke"]
    
    _extract = extract()
    _transform = transform(_extract)
    
    _print_joke = BashOperator(
        task_id="print_joke",
        bash_command=f"echo {_transform}",
        outlets=[Asset("my_manatee_joke")]
    )

my_fixed_dag()

    

    
    
