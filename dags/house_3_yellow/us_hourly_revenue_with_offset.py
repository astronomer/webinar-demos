from datetime import datetime, timezone

from airflow.sdk import Asset, PartitionedAssetTimetable, StartOfHourMapper, dag, task

from include.house_3_yellow.store import connect

HOURLY_ORDERS = Asset("berlin_hourly_orders_with_offset")
KEY_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
HOUR_FORMAT = "%Y-%m-%dT%H%z"


@dag(
    schedule=PartitionedAssetTimetable(
        assets=HOURLY_ORDERS,
        default_partition_mapper=StartOfHourMapper(
            timezone="America/New_York", input_format=KEY_FORMAT, output_format=HOUR_FORMAT
        ),
    ),
    tags=["house_3_yellow"],
)
def us_hourly_revenue_with_offset():

    @task
    def compute_revenue(dag_run=None):
        new_york_hour = dag_run.partition_key
        utc_hour = datetime.strptime(new_york_hour, HOUR_FORMAT).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:00Z")
        with connect() as conn:
            (revenue,) = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE dag_id = ? AND utc_hour = ?",
                ("berlin_hourly_orders_with_offset", utc_hour),
            ).fetchone()
            conn.execute(
                "INSERT INTO revenue VALUES (?, ?, ?, ?)",
                ("us_hourly_revenue_with_offset", new_york_hour, revenue, dag_run.run_id),
            )
        print(f"Revenue for New York hour {new_york_hour}: EUR {revenue:,.2f}")

    compute_revenue()


us_hourly_revenue_with_offset()
