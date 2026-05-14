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
def customer_stats():

    _aggregate = SQLExecuteQueryOperator(
        task_id="aggregate",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="customer_stats.sql",
    )

    _get_stats = SQLExecuteQueryOperator(
        task_id="get_stats",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=(
            "SELECT customer_id, full_name, total_bookings, total_passengers, "
            "total_paid_usd, first_booking_date, last_booking_date "
            "FROM customer_lifetime_stats ORDER BY total_paid_usd DESC"
        ),
        requires_result_fetch=True,
    )

    @task
    def print_stats(ti=None):
        rows = ti.xcom_pull(task_ids="get_stats") or []

        print("::group::Customer Lifetime Stats")

        print("Customer | Bookings | Passengers | Paid USD | First | Last")
        print("-" * 70)

        for row in rows:
            customer_id, full_name, bookings, passengers, paid, first, last = row
            print(
                f"{full_name} | "
                f"{bookings} | "
                f"{passengers} | "
                f"{paid:,} | "
                f"{first} | "
                f"{last}"
            )

        print("::endgroup::")

    chain(_aggregate, _get_stats, print_stats())

customer_stats_dag = customer_stats()

if __name__ == "__main__":
    customer_stats_dag.test(
        logical_date=datetime(2026, 1, 1),
        conn_file_path="include/connections.yaml",
    )
