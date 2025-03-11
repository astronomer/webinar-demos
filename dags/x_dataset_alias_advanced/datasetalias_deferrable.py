"""
### Dataset Alias in a traditional operator
"""

from airflow.decorators import dag
from airflow.datasets import DatasetAlias

from pendulum import datetime
import logging


t_log = logging.getLogger("airflow.task")


my_alias_name = "my_alias_10"

from include.deferrable_operator import MyOperator


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["x_dataset_alias_advanced"],
)
def datasetalias_deferrable():

    MyOperator(
        task_id="t1",
        my_alias_name=my_alias_name,
        wait_for_completion=True,
        deferrable=True,
        poke_interval=10,
        outlets=[DatasetAlias(my_alias_name)],
    )


datasetalias_deferrable()
