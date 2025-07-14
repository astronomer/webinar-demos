from airflow.sdk import dag, task, Asset
from pendulum import datetime


@dag(start_date=datetime(2025, 7, 1), schedule=[Asset("pii_cleaned")])
def etl_dag():

    @task
    def extract():
        return {"a": 1, "b": 2}

    @task
    def transform(data):
        return data["a"] + data["b"]

    @task(outlets=[Asset("etl_done")])
    def load(data):
        print(data)

    _extract = extract()
    _transform = transform(_extract)
    _load = load(_transform)


etl_dag()
