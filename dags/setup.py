from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, chain

_DUCKDB_CONN_ID = "duckdb_astrotrips"

@dag(
    tags=["astrotrips", "setup"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql"
)
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

setup_dag = setup()

if __name__ == "__main__":
    setup_dag.test(
        conn_file_path="include/connections.yaml"
    )
