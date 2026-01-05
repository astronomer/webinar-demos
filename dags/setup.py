from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_DUCKDB_CONN_ID = "duckdb_astrotrips"

@dag(template_searchpath="/usr/local/airflow/include/sql")
def setup():

    _cleanup = SQLExecuteQueryOperator(
        task_id="cleanup",
        conn_id=_DUCKDB_CONN_ID,
        sql="cleanup.sql"
    )

    _schema = SQLExecuteQueryOperator(
        task_id="schema",
        conn_id=_DUCKDB_CONN_ID,
        sql="schema.sql"
    )

    _fixtures = SQLExecuteQueryOperator(
        task_id="fixtures",
        conn_id=_DUCKDB_CONN_ID,
        sql="fixtures.sql"
    )

    chain(_cleanup, _schema, _fixtures)

setup()
