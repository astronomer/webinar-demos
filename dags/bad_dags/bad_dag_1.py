# """WARNING: This DAG is used as an example for _bad_ Airflow practices. Do not
# use this DAG."""

# from airflow.sdk import dag
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# _PG_CONN_ID = "postgres_default"

# @dag(
#     tags=["bad_dag"],
# )
# def bad_dag_1():

#     hook = PostgresHook(_PG_CONN_ID)
#     results = hook.get_records("SELECT * FROM in_table;")

#     sql_queries = []

#     for result in results:
#         id = result[0]
#         name = result[1]
#         sql_query = f"INSERT INTO out_table VALUES ({id}, '{name}');"

#         sql_queries.append(sql_query)

#     SQLExecuteQueryOperator.partial(
#         task_id="insert_records",
#         conn_id=_PG_CONN_ID,
#     ).expand(sql=sql_queries)


# bad_dag_1()




