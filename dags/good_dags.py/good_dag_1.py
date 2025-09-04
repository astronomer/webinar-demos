"""WARNING: This DAG is used as an example for _bad_ Airflow practices. Do not
use this DAG."""

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

_PG_CONN_ID = "postgres_default"

@dag(
    tags=["bad_dag"],
)
def good_dag_1():

    @task
    def get_sql_queries():
        hook = PostgresHook(_PG_CONN_ID)
        results = hook.get_records("SELECT * FROM in_table;")

        sql_queries = []

        for result in results:
            id = result[0]
            name = result[1]
            sql_query = f"INSERT INTO out_table VALUES ({id}, '{name}');"

            sql_queries.append(sql_query)
        
        return sql_queries
    
    _get_sql_queries = get_sql_queries()

    SQLExecuteQueryOperator.partial(
        task_id="insert_records",
        conn_id=_PG_CONN_ID,
    ).expand(sql=_get_sql_queries)


good_dag_1()



