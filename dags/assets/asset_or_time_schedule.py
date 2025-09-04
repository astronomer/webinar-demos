from airflow.sdk import dag, task, Asset
from pendulum import datetime
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    start_date=datetime(2025, 8, 1),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
        assets=(Asset("x-asset3") | Asset("x-asset4")),
    ),  # Runs every hour and when either of the assets are updated
)
def asset_or_time_schedule():
    @task
    def say_hello() -> None:
        import time

        time.sleep(10)
        print("Hello")

    say_hello()


asset_or_time_schedule()