from datetime import timezone

import pendulum

from airflow.sdk import Asset, CronPartitionTimetable, dag, task

from include.house_3_yellow.store import connect, order_amount

HOURLY_ORDERS = Asset("berlin_hourly_orders_with_offset")
KEY_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


@dag(
    schedule=CronPartitionTimetable("0 * * * *", timezone="Europe/Berlin", key_format=KEY_FORMAT),
    start_date=pendulum.datetime(2026, 10, 24, 22, tz="UTC"),
    end_date=pendulum.datetime(2026, 10, 25, 4, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["house_3_yellow"],
)
def berlin_hourly_orders_with_offset():

    @task(outlets=[HOURLY_ORDERS])
    def land_orders(dag_run=None):
        utc = dag_run.run_after.astimezone(timezone.utc)
        amount = order_amount(utc.hour)
        with connect() as conn:
            conn.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?)",
                ("berlin_hourly_orders_with_offset", dag_run.partition_key, utc.strftime("%Y-%m-%dT%H:00Z"), amount),
            )
        print(f"Landed EUR {amount:,.2f} of orders for partition {dag_run.partition_key}.")

    land_orders()


berlin_hourly_orders_with_offset()
