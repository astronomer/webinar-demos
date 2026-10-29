from airflow.sdk import Asset, PartitionedAssetTimetable, StartOfHourMapper, dag, task

from include.house_3_yellow.store import connect

HOURLY_ORDERS = Asset("berlin_hourly_orders")


@dag(
    schedule=PartitionedAssetTimetable(
        assets=HOURLY_ORDERS,
        default_partition_mapper=StartOfHourMapper(timezone="Europe/Berlin"),
    ),
    tags=["house_3_yellow"],
)
def eu_hourly_revenue():

    @task
    def compute_revenue(dag_run=None):
        berlin_hour = dag_run.partition_key
        with connect() as conn:
            (revenue,) = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE dag_id = ? AND partition_key LIKE ?",
                ("berlin_hourly_orders", f"{berlin_hour}%"),
            ).fetchone()
            conn.execute(
                "INSERT INTO revenue VALUES (?, ?, ?, ?)",
                ("eu_hourly_revenue", berlin_hour, revenue, dag_run.run_id),
            )
        print(f"Revenue for Berlin hour {berlin_hour}: EUR {revenue:,.2f}")

    compute_revenue()


eu_hourly_revenue()
