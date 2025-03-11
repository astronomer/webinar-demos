"""
### Basic Dataset Schedule Example DAG - Downstream

This DAG is scheduled to run as soon as the
dataset s3://bucket/data.csv is updated.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from pendulum import datetime
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2024, 11, 1),
    schedule=[Dataset("s3://bucket/data.csv")], 
    catchup=False,
    doc_md=__doc__,
    tags=["0_basic_dataset_schedule"],
)
def my_ml_dag():

    @task
    def downstream_task():
        pass

    downstream_task()


my_ml_dag()
