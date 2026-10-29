import logging
import time

from airflow.sdk import dag, task

log = logging.getLogger("airflow.task")


@dag(schedule=None, tags=["no-logs-test"])
def log_visibility_check():

    @task
    def say_hello() -> None:
        log.info("hello from log_visibility_check")
        time.sleep(5)

    say_hello()


log_visibility_check()
