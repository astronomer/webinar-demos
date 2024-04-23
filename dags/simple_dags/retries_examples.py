from airflow.decorators import dag, task
from pendulum import datetime, duration

# Airflow configs for retries with their defaults
# AIRFLOW__CORE__DEFAULT_TASK_RETRIES = 0
# AIRFLOW__CORE__DEFAULT_TASK_RETRY_DELAY = 300  # seconds
# AIRFLOW__CORE__MAX_TASK_RETRY_DELAY = 86400  # seconds

@dag(
    start_date=datetime(2023, 10, 18),
    schedule="@daily",
    tags=["Simple DAGs", "retries"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
        "retry_exponential_backoff": True,  # default is False
    },  # you can override the config defaults at the DAG level
    # counts for all tasks in the DAG
    catchup=False,
)
def retries_examples():

    @task(
        retries=2, retry_delay=duration(minutes=3), retry_exponential_backoff=False
    )  # you can override the defaults at
    def say_hi_1(name: str = "") -> str:
        # raise ValueError("This is an example error.")  # uncomment to test retries
        return f"Hi {name}!"

    say_hi_obj = say_hi_1("Astra")


retries_examples()
