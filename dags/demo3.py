from airflow.sdk import dag, task, Asset

@dag(schedule="@daily")  # same as "0 0 * * *"
def demo3_producer():

   @task(outlets=[Asset("data")])
   def materialize_asset():
       return 1

   materialize_asset()

demo3_producer()

# ---

@dag(schedule=Asset("data"))
def demo3_consumer():

   @task
   def some_task():
       print("Hi webinar!")

   some_task()

demo3_consumer()
