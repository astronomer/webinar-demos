from airflow.sdk import dag, task
from datetime import timedelta

def _get_divisor():
    import random

    return random.randint(0, 2)

def _on_failure_callback(context):
    # placeholder for code that sends a notification
    print(f"Task failed: {context['task_instance'].task_id}")
    print(f"Error: {context['exception']}")

@dag(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "on_failure_callback": _on_failure_callback,
        "execution_timeout": timedelta(seconds=20),
    },
    max_consecutive_failed_dag_runs=10,
    dagrun_timeout=timedelta(minutes=2),
    tags=["good_dag"],
)
def good_dag_2():

    @task
    def get_number():
        import random
        return random.randint(0, 10)

    @task
    def divide_by_number(number):
        divisor = _get_divisor()
        return number / divisor

    _get_number = get_number()
    divide_by_number(_get_number)


good_dag_2()


