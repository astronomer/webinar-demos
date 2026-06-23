from airflow.sdk import dag, task

@dag
def demo1():

   @task.bash
   def get_dt():
       return "date +%Y-%m-%d"

   @task
   def log_dt(dt_value):
       print(f"Date received: {dt_value}")

   dt = get_dt()
   log_dt(dt)

demo1()
