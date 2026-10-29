from datetime import datetime, timedelta

from airflow.sdk import Asset, PartitionedAssetTimetable, StartOfHourMapper, dag, task

from include.house_3_yellow.store import connect

HOURLY_ORDERS = Asset("berlin_hourly_orders")
BERLIN_TO_NEW_YORK = timedelta(hours=-6)


@dag(
    schedule=PartitionedAssetTimetable(
        assets=HOURLY_ORDERS,
        default_partition_mapper=StartOfHourMapper(timezone="Europe/Berlin"),
    ),
    tags=["house_3_yellow"],
)
def us_hourly_revenue():

    @task
    def compute_revenue(dag_run=None):
        berlin_hour = dag_run.partition_key
        new_york_hour = (datetime.strptime(berlin_hour, "%Y-%m-%dT%H") + BERLIN_TO_NEW_YORK).strftime(
            "%Y-%m-%dT%H"
        )
        with connect() as conn:
            (revenue,) = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE dag_id = ? AND partition_key LIKE ?",
                ("berlin_hourly_orders", f"{berlin_hour}%"),
            ).fetchone()
            conn.execute(
                "INSERT INTO revenue VALUES (?, ?, ?, ?)",
                ("us_hourly_revenue", new_york_hour, revenue, dag_run.run_id),
            )
        print(f"Revenue for New York hour {new_york_hour}: EUR {revenue:,.2f}")

    compute_revenue()


us_hourly_revenue()
