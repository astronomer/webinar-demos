from airflow.sdk import dag, task, Asset

@dag(schedule="@daily")  # same as "0 0 * * *"
def demo2_producer():

   @task(outlets=[Asset("data")])
   def materialize_asset():
       return 1

   materialize_asset()

demo2_producer()

# ---

@dag(schedule=Asset("data"))
def demo2_consumer():

   @task
   def some_task():
       print("Hi webinar!")

   some_task()

demo2_consumer()
