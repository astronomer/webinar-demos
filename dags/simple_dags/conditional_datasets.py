from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime

@dag(
    start_date=datetime(2024, 3, 1),
    schedule=(
        (Dataset("dataset1") | Dataset("dataset2"))
        & (Dataset("dataset3") | Dataset("dataset4"))
    ),  # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False,
    tags=["Simple DAGs", "Datasets"],
)
def conditional_datasets():

    @task
    def task1():
        pass

    task1()

conditional_datasets()