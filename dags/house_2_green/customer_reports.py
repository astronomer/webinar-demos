import time
from pathlib import Path

from airflow.sdk import dag, task

if Path("/usr/local/airflow/include/house_2_green/slow_parse_on").exists():
    time.sleep(35)


@dag(schedule=None, tags=["house_2_green"])
def customer_reports():

    @task
    def build_report():
        print("Building customer report.")

    build_report()


customer_reports()
