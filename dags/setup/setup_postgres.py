from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_PG_CONN_ID = "postgres_default"

@dag(
    tags=["helper_dag"],
)
def setup_postgres():
    setup_up_in_table = SQLExecuteQueryOperator(
        task_id="setup_up_in_table",
        conn_id=_PG_CONN_ID,
        sql="CREATE TABLE IF NOT EXISTS in_table (id INT, name VARCHAR(255));",
    )

    setup_up_out_table = SQLExecuteQueryOperator(
        task_id="setup_up_out_table",
        conn_id=_PG_CONN_ID,
        sql="CREATE TABLE IF NOT EXISTS out_table (id INT, name VARCHAR(255));",
    )

    fill_in_table = SQLExecuteQueryOperator(
        task_id="fill_in_table",
        conn_id=_PG_CONN_ID,
        sql="INSERT INTO in_table (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Jim');",
    )

    chain(
        setup_up_in_table,
        setup_up_out_table,
        fill_in_table,
    )

setup_postgres()