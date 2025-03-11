"""
### Fetch extra information from a Dataset Events

This DAG shows how to fetch extra information from both
triggering and inlet Dataset events.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from pendulum import datetime

my_dataset_1 = Dataset("x-dataset-metadata-1")
my_dataset_2 = Dataset("x-dataset-metadata-2")


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=[my_dataset_1],
    catchup=False,
    tags=["3_attaching_and_retrieving_metadata"],
)
def fetch_extra_info():

    # ------------- #
    # Task Flow API #
    # ------------- #

    @task
    def get_extra_triggering_run(**context):
        # all events that triggered this specific DAG run
        triggering_dataset_events = context["triggering_dataset_events"]
        # the loop below wont run if the DAG is manually triggered
        for dataset, dataset_event_list in triggering_dataset_events.items():
            print(dataset)
            print(dataset_event_list)
            print(dataset_event_list[0].extra["myNum"])
            # dataset_list[0].source_dag_run.run_id  # you can also fetch the run_id of the upstream DAG, this will AttributeError if the Trigger was the API!

    get_extra_triggering_run()

    # Note that my_dataset_2 is NOT a Dataset this DAG is scheduled upon, any existing Dataset can be used as an inlet in any task
    @task(inlets=[my_dataset_2])
    def get_extra_inlet(**context):
        # inlet_events are listed earliest to latest by timestamp
        events = context["inlet_events"][my_dataset_2]
        # protect against no previous events
        if len(events) == 0:
            print(f"No events for {my_dataset_2.uri}")
        else:
            myNum = events[-1].extra.get("myNum", None)
            print(myNum)

    get_extra_inlet()

    # ----------------------------- #
    # Traditional Operators - Jinja #
    # ----------------------------- #

    get_extra_inlet_bash_jinja = BashOperator(
        task_id="get_extra_inlet_bash_jinja",
        bash_command="echo {{ inlet_events['x-dataset-metadata-2'][-1].extra['myNum'] }} ",  # task will fail if the Dataset never had updates to it
        # The below version returns an empty string if there are no previous dataset events or the extra is not present
        # bash_command="echo {{ (inlet_events['x-dataset2'] | default([]) | last | default({})).extra.get('myNum', '') if (inlet_events['x-dataset2'] | default([]) | last | default({})).extra is defined else '' }}",  # Version that should never error
        inlets=[my_dataset_2],
    )

    get_extra_triggering_run_bash_jinja = BashOperator(
        task_id="get_extra_triggering_run_bash_jinja",
        bash_command="echo {{ (triggering_dataset_events.values() | first | first).extra['myNum'] }} ",  # This statement errors when there are no triggering events, for example in a manual run!
        # The below version returns an empty string if there are no triggering dataset events or the extra is not present
        # bash_command="echo {{ (triggering_dataset_events.values() | default([]) | first | default({}) | first | default({})).extra.get('myNum', '') if (triggering_dataset_events.values() | default([]) | first | default({}) | first | default({})).extra is defined else '' }}",  # Version that should never error
    )


fetch_extra_info()
