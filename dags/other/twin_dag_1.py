from airflow.sdk import dag, task

@dag(tags=["twin_dag"]) 
def twin_dag():

    @task 
    def my_task():
        pass 

    my_task()


twin_dag()