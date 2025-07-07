"""BAD PRACTICE: Direct metadatabase access (works in <3.0, removed in 3.0)"""

from airflow.decorators import dag, task 
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def my_direct_db_access_dag():
    @task
    def access_db():
        # BAD PRACTICE (and does not work in Airflow 3.0)
        from airflow.utils.db import provide_session
        from airflow.models import DagRun
        
        @provide_session
        def get_dag_runs_directly(session=None):
            dag_runs = session.query(DagRun).filter(
                DagRun.dag_id == 'my_direct_db_access_dag'
            ).all()
            
            print(f"Found {len(dag_runs)} dag runs")
            print(f"Dag runs: {dag_runs}")

        get_dag_runs_directly()            
        


    access_db()

my_direct_db_access_dag()
        