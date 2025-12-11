from airflow.sdk import dag, task 
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=CronDataIntervalTimetable(cron="0 0 * * *", timezone="UTC"),
) 
def daily_dag_cron_data_interval_timetable():

    @task 
    def print_context(**context):
        print(f"logical_date: {context['logical_date']}")
        print(f"run_id: {context['run_id']}")
        print(f"data_interval_start: {context['data_interval_start']}")
        print(f"data_interval_end: {context['data_interval_end']}")

    print_context()

daily_dag_cron_data_interval_timetable()