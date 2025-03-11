"""
### Basic Dataset Schedule Example DAG - Upstream

This DAG constains a task that updates one dataset
with the URI s3://bucket/data.csv.

Commented out:
A traditional BashOperator that updates the same dataset.
"""

import logging

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.dataset import Dataset
from airflow.operators.bash import BashOperator
from pendulum import datetime

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2024, 11, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["0_basic_dataset_schedule"],
)
def my_etl_dag():

    @task(outlets=[Dataset("s3://bucket/data.csv")])
    def producer():
        return "hi"

    _producer = producer()

    # _producer_traditional = BashOperator(
    #     task_id="producer_traditional",
    #     bash_command='echo "I update a dataset!"',
    #     outlets=[Dataset("s3://bucket/data.csv")],
    # )

    # chain(_producer, _producer_traditional)


my_etl_dag()
