from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import task, chain
from pendulum import datetime, duration

from include.utils import print_report_row

_DUCKDB_CONN_ID = "duckdb_astrotrips"

@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    tags=["astrotrips", "reporting"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql",
    # default_args={
    #     "retries": 2,
    #     "retry_delay": duration(minutes=1),
    # }
)
def daily_report():

    _ingest_data = SQLExecuteQueryOperator(
        task_id="ingest",
        conn_id=_DUCKDB_CONN_ID,
        sql="generate.sql",
        params={ "n_bookings": 5 }
    )

    @task
    def consume_memory(target_kb: int = 800, chunk_kb: int = 50, sleep_s: float = 0.1):
        import time
        blocks = []
        allocated = 0

        print(f"allocating {target_kb}k memory...")

        while allocated < target_kb:
            blocks.append(b"x" * (chunk_kb * 1024 * 1024))
            allocated += chunk_kb
            print(f"allocated {allocated}k")
            time.sleep(sleep_s)

        print("done allocating, holding memory for 3s...")
        time.sleep(3)

    _remove_existing_report = SQLExecuteQueryOperator(
        task_id="remove_existing_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="DELETE FROM daily_planet_report WHERE report_date = $reportDate::DATE",
        parameters={ "reportDate": "{{ ds_nodash }}" }
    )

    _generate_report = SQLExecuteQueryOperator(
        task_id="generate_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="report.sql",
        parameters={ "reportDate": "{{ ds_nodash }}" }
    )

    _get_report = SQLExecuteQueryOperator(
        task_id="get_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="SELECT * FROM daily_planet_report WHERE report_date = $reportDate::DATE",
        parameters={ "reportDate": "{{ ds_nodash }}" },
        requires_result_fetch=True
    )

    @task
    def print_report(ti = None):
        rows = ti.xcom_pull(task_ids="get_report") or []

        print("::group::Daily Planet Report")

        print("Planet | Passengers | Active | Done | Gross USD | Discount | Net USD")
        print("-" * 65)

        for row in rows:
            print_report_row(row)

        print("::endgroup::")

    chain(
        _ingest_data,
        consume_memory(target_kb=5*1024),
        _remove_existing_report,
        _generate_report,
        _get_report,
        print_report()
    )

# daily_report_dag = daily_report()

# if __name__ == "__main__":
#     daily_report_dag.test(
#         logical_date=datetime(2026, 1, 1),
#         conn_file_path="include/connections.yaml"
#     )
