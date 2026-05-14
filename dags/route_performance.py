from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task, chain
from pendulum import datetime

_SNOWFLAKE_CONN_ID = "snowflake_astrotrips"

@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    tags=["astrotrips", "reporting"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
)
def route_performance():

    _aggregate = SQLExecuteQueryOperator(
        task_id="aggregate",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="route_performance.sql",
    )

    _get_performance = SQLExecuteQueryOperator(
        task_id="get_performance",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=(
            "SELECT route_id, planet_name, total_bookings, total_passengers, total_revenue_usd "
            "FROM route_performance "
            "WHERE report_date = '{{ ds }}'::DATE "
            "ORDER BY total_revenue_usd DESC"
        ),
        requires_result_fetch=True,
    )

    @task
    def print_performance(ti=None):
        rows = ti.xcom_pull(task_ids="get_performance") or []

        print("::group::Route Performance")

        print("Route | Planet | Bookings | Passengers | Revenue USD")
        print("-" * 60)

        for row in rows:
            route_id, planet_name, bookings, passengers, revenue = row
            print(
                f"{route_id} | "
                f"{planet_name} | "
                f"{bookings} | "
                f"{passengers} | "
                f"{revenue:,}"
            )

        print("::endgroup::")

    chain(_aggregate, _get_performance, print_performance())

route_performance_dag = route_performance()

if __name__ == "__main__":
    route_performance_dag.test(
        logical_date=datetime(2026, 1, 1),
        conn_file_path="include/connections.yaml",
    )
