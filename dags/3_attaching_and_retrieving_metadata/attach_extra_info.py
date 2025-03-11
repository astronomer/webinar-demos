"""
### Attach additional information to Airflow Datasets

This DAG attaches extra information to an Airflow Dataset from within the
producing task using two different methods.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime

# import the Metadata class
from airflow.datasets.metadata import Metadata

my_dataset_2 = Dataset("x-dataset-metadata-2")


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    tags=["3_attaching_and_retrieving_metadata"],
    default_args={"retries": 2},
)
def attach_extra_info():

    @task(outlets=[Dataset("x-dataset-metadata-1")])
    def attach_extra_using_metadata():
        num = 23
        yield Metadata(
            Dataset("x-dataset-metadata-1"),
            {"myNum": num}
        )

        return "hello :)"

    attach_extra_using_metadata()

    # alternative way to attach extra information

    @task(outlets=[my_dataset_2])
    def use_outlet_events(**context):
        num = 42
        context["outlet_events"][my_dataset_2].extra = {
            "myNum": num,
            "myStr": "Lemons!",
        }

        return "hello :)"

    use_outlet_events()


attach_extra_info()
