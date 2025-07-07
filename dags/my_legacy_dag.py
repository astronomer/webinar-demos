"""Legacy DAG, runs in Airflow 2, not in Airflow 3!"""

from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset

URL = "https://manateejokesapi.herokuapp.com/manatees/random"


@dag(
    start_date=days_ago(1),
    schedule_interval="@daily",
    default_view="grid",
    catchup=False,
)
def my_legacy_dag():

    @task
    def extract(execution_date):
        import requests

        response = requests.get(URL)
        response_json = response.json() 
        response_json["timestamp"] = execution_date.isoformat()
        return response_json

    @task
    def transform(data):
        return {"setup": data["setup"], "punchline": data["punchline"]}

    _extract = extract()
    _transform = transform(_extract)

    _print_joke = BashOperator(
        task_id="print_joke",
        bash_command=f"echo {_transform}",
        outlets=[Dataset("my_manatee_joke")],
    )

    _transform >> _print_joke


my_legacy_dag()
