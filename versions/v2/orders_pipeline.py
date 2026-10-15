"""v2"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import ObjectStoragePath, dag, get_current_context, task

from include.exchange_rates import fetch_rates

LANDING = ObjectStoragePath("s3://landing/orders/", conn_id="minio")


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, tags=["etl-elt"])
def orders_pipeline():
    @task
    def list_files() -> list[ObjectStoragePath]:
        return [path for path in LANDING.iterdir() if path.name.endswith(".csv")]

    # One task per file, named after its store in the UI. At most 4 run at the same time.
    @task(map_index_template="{{ store }}", max_active_tis_per_dagrun=4)
    def extract(path: ObjectStoragePath) -> pd.DataFrame:
        get_current_context()["store"] = path.stem.removeprefix("store_")
        with path.open() as f:
            return pd.read_csv(f)

    @task(retries=2)
    def transform(frames: list[pd.DataFrame]) -> pd.DataFrame:
        orders = pd.concat(frames)
        rates = fetch_rates()
        orders["amount_usd"] = orders["quantity"] * orders["unit_price"] / orders["currency"].map(rates)
        daily = orders.groupby(["order_date", "store_id"], as_index=False).agg(
            order_count=("order_id", "count"),
            revenue_usd=("amount_usd", "sum"),
        )
        daily["avg_order_usd"] = daily["revenue_usd"] / daily["order_count"]
        return daily.round(2)

    @task
    def load(daily: pd.DataFrame) -> None:
        # An upsert: loading the same day twice replaces its rows instead of duplicating them.
        PostgresHook(postgres_conn_id="warehouse").insert_rows(
            table="daily_revenue",
            rows=daily.to_records(index=False).tolist(),
            target_fields=list(daily.columns),
            replace=True,
            replace_index=["order_date", "store_id"],
        )

    load(transform(extract.expand(path=list_files())))


orders_pipeline()
