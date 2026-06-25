from pendulum import duration
from airflow.sdk import dag, task, get_current_context

@dag(
    default_args={"retries": 3, "retry_delay": duration(seconds=2)}
)
def demo1():

   @task.bash
   def get_dt():
       return "date +%Y-%m-%d"

   @task
   def log_dt(dt_value):
       context = get_current_context()
       try_number = context["ti"].try_number # pyright: ignore[reportTypedDictNotRequiredAccess]
       if try_number < 3:
           raise RuntimeError(f"Simulated failure")
       print(f"Date received: {dt_value}")

   dt = get_dt()
   log_dt(dt)

demo1()
