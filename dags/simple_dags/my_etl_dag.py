from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["Simple DAGs", "Datasets"],
)
def my_etl_dag():
    @task(
        outlets=[Dataset("s3://bucket/data.csv")],
    )
    def my_task():
        # code that updates the dataset (or not!)
        pass

    my_task()


my_etl_dag()
