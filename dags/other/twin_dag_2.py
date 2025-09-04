from airflow.sdk import dag, task


@dag(tags=["twin_dag"]) 
def twin_dag():

    @task 
    def my_task():
        pass 

    @task 
    def my_task_2():
        pass 

    @task 
    def my_task_3():
        pass 

    my_task() >> my_task_2() >> my_task_3()


twin_dag()
