from airflow.models import TaskInstance
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
   def log_dt(dt_value, ti: TaskInstance | None =None):
       try_number = ti.try_number # type: ignore
       if try_number < 3:
           raise RuntimeError("Simulated failure")
       print(f"Date received: {dt_value}")

   dt = get_dt()
   log_dt(dt)

demo1()
