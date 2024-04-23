from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    start_date=datetime(2024, 3, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=(Dataset("dataset3") | Dataset("dataset4")),
        # Use () instead of [] to be able to use conditional dataset scheduling!
    ),
    catchup=False,
    tags=["Simple DAGs", "Datasets"],
)
def dataset_and_time_schedule():

    @task
    def task1():
        pass

    task1()

dataset_and_time_schedule()
