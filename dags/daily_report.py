from airflow.sdk import dag, task, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

_DUCKDB_CONN_ID = "duckdb_astrotrips"

@dag(template_searchpath="/usr/local/airflow/include/sql")
def daily_report():

    _ingest_data = SQLExecuteQueryOperator(
        task_id="ingest",
        conn_id=_DUCKDB_CONN_ID,
        sql="generate.sql",
        params={ "n_bookings": 5 }
    )

    @task
    def consume_memory(target_kb: int = 800, chunk_kb: int = 50, sleep_s: float = 0.2):
        import time
        blocks = []
        allocated = 0

        print(f"allocating {target_kb}k memory...")

        while allocated < target_kb:
            blocks.append(b"x" * (chunk_kb * 1024))
            allocated += chunk_kb
            print(f"allocated {allocated}k")
            time.sleep(sleep_s)

        print("done allocating, holding memory for 10s...")
        time.sleep(10)

    _remove_existing_report = SQLExecuteQueryOperator(
        task_id="remove_existing_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="DELETE FROM daily_planet_report WHERE report_date = $reportDate::DATE",
        parameters={ "reportDate": "{{ ds }}" }
    )

    _generate_report = SQLExecuteQueryOperator(
        task_id="generate_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="report.sql",
        parameters={ "reportDate": "{{ ds }}" }
    )

    _get_report = SQLExecuteQueryOperator(
        task_id="get_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="SELECT * FROM daily_planet_report WHERE report_date = $reportDate::DATE",
        parameters={ "reportDate": "{{ ds }}" },
        requires_result_fetch=True
    )

    @task
    def print_report(ti = None):
        rows = ti.xcom_pull(task_ids="get_report") or []

        print("Planet | Passengers | Active | Done | Gross USD | Discount | Net USD")
        print("-" * 65)

        for(
            _,
            planet,
            passengers,
            active,
            completed,
            gross,
            discount,
            net,
            _,
        ) in rows:
            print(
                f"{planet} | "
                f"{passengers} | "
                f"{active} | "
                f"{completed} | "
                f"{gross:,} | "
                f"{discount:,} | "
                f"{net:,}"
            )

        print()

    chain(
        _ingest_data,
        _remove_existing_report,
        _generate_report,
        _get_report,
        print_report()
    )

daily_report_dag = daily_report()

if __name__ == "__main__":
    daily_report_dag.test()
