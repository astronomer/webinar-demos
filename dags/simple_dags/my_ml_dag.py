from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime


@dag(
    schedule=[Dataset("s3://bucket/data.csv")],
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=["Simple DAGs", "Datasets"],
)
def my_ml_dag():
    @task(
        outlets=[Dataset("snowflake://my_table/")],
    )
    def my_task():
        # code that updates the snowflake table (or not!)
        import time 

        time.sleep(20)
        pass

    my_task()


my_ml_dag()
