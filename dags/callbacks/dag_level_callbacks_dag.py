from airflow.sdk import dag, task

def my_success_callback_function(context):
    pass

def my_failure_callback_function(context):
    pass

@dag(
    on_success_callback=my_success_callback_function,
    on_failure_callback=my_failure_callback_function
)
def dag_level_callbacks_dag():

    @task 
    def my_task():
        pass 

    my_task()

dag_level_callbacks_dag()