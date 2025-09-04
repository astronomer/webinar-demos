from airflow.decorators import dag, task

@dag 
def in_salesforce():
    @task
    def placeholder():
        pass 

    placeholder()

in_salesforce()

@dag 
def in_stripe():
    @task
    def placeholder():
        pass 

    placeholder()

in_stripe()

@dag 
def in_google_analytics():
    @task
    def placeholder():
        pass 

    placeholder()

in_google_analytics()