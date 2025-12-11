from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.python import PythonOperator
from sqlalchemy import text


class MyCustomDirectDBAccessOperator(BaseOperator):
    from airflow.utils.session import provide_session

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @provide_session
    def execute(self, context, session):
        dags = session.execute(text("SELECT * FROM public.dag LIMIT 10")).fetchall()
        print(dags)


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
)
def test_direct_db_access_block():
    my_task_1 = MyCustomDirectDBAccessOperator(
        task_id="my_task_1",
    )

    def my_task_1_func():
        from airflow.utils.session import create_session
        with create_session() as session:
            dags = session.execute(text("SELECT * FROM public.dag LIMIT 10")).fetchall()
            print(dags)


    my_task_1_python = PythonOperator(
        task_id="my_task_1_python",
        python_callable=my_task_1_func,
    )

    @task
    def my_task_2():
        from airflow.settings import Session

        with Session() as session:
            dags = session.execute(text("SELECT * FROM public.dag LIMIT 10")).fetchall()
            print(dags)

    my_task_2()

    @task
    def my_task_3():
        from sqlalchemy.orm.session import Session
        from airflow.settings import engine

        with Session(bind=engine) as session:
            dags = session.execute(text("SELECT * FROM public.dag LIMIT 10")).fetchall()
            print(dags)

    my_task_3()

    @task
    def my_task_4():
        from airflow.utils.session import create_session

        with create_session() as session:
            dags = session.execute(text("SELECT * FROM public.dag LIMIT 10")).fetchall()
            print(dags)

    my_task_4()

    @task
    def my_task_5():
        from airflow.settings import engine

        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM public.dag LIMIT 10"))
            dags = result.fetchall()
            print(dags)

    my_task_5()

    @task
    def my_task_6():
        from airflow.models import DagModel
        from airflow.settings import Session

        with Session() as session:
            dags = session.query(DagModel).limit(10).all()
            for dag in dags:
                print(f"DAG: {dag.dag_id}, is_paused: {dag.is_paused}")

    my_task_6()

    @task
    def my_task_7():
        # this method wont work in SQLAlchemy 2.0 
        from airflow.settings import engine
        dags = engine.execute("SELECT * FROM public.dag LIMIT 10").fetchall()
        print(dags)

    my_task_7()


    @task 
    def my_task_8(): 
        from airflow.models import DagRun
        dags = DagRun.find(limit=10)
        print(dags)

    my_task_8()

test_direct_db_access_block()
