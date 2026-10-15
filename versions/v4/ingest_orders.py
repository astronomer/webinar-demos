"""v4"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import (
    Asset,
    ExceptionRetryPolicy,
    ObjectStoragePath,
    RetryAction,
    RetryRule,
    dag,
    get_current_context,
    task,
)

from include.exchange_rates import ForbiddenError, RateLimitedError, fetch_rates

LANDING = ObjectStoragePath("s3://landing/orders/", conn_id="minio")
RAW_ORDERS = Asset("raw_orders")

EXCHANGE_API_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(
            exception=RateLimitedError,
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=5),
            reason="Rate limited: worth another try",
        ),
        RetryRule(
            exception=ForbiddenError,
            action=RetryAction.FAIL,
            reason="API key rejected: retrying will not help",
        ),
    ]
)


@dag(schedule="@daily", start_date=datetime(2026, 10, 1), catchup=False, tags=["etl-elt"])
def ingest_orders():
    @task(retries=4, retry_delay=timedelta(seconds=30), retry_policy=EXCHANGE_API_RETRY_POLICY)
    def get_exchange_rates() -> dict[str, float]:
        return fetch_rates()

    @task
    def list_files() -> list[ObjectStoragePath]:
        return [path for path in LANDING.iterdir() if path.name.endswith(".csv")]

    @task(map_index_template="{{ store }}", max_active_tis_per_dagrun=4)
    def extract_and_load(path: ObjectStoragePath, rates: dict[str, float]) -> int:
        store = path.stem.removeprefix("store_")
        get_current_context()["store"] = store
        with path.open() as f:
            orders = pd.read_csv(f)

        orders["customer_email"] = orders["customer_email"].map(
            lambda email: hashlib.sha256(email.encode()).hexdigest()[:16]
        )
        orders = orders.rename(columns={"customer_email": "customer_hash"})
        orders["amount_usd"] = (
            orders["quantity"] * orders["unit_price"] / orders["currency"].map(rates)
        ).round(2)

        dates = sorted(orders["order_date"].unique().tolist())
        # Delete, then COPY, in one transaction: a rerun replaces this store's days instead of
        # duplicating them.
        with PostgresHook(postgres_conn_id="warehouse").get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw_orders WHERE store_id = %s AND order_date = ANY(%s::date[])",
                (store, dates),
            )
            with cur.copy(
                f"COPY raw_orders ({', '.join(orders.columns)}) FROM STDIN WITH (FORMAT csv)"
            ) as copy:
                copy.write(orders.to_csv(index=False, header=False))
        return len(orders)

    @task(outlets=[RAW_ORDERS])
    def publish_raw_orders(row_counts: list[int]) -> None:
        print(f"Loaded {sum(row_counts):,} orders from {len(row_counts)} stores")

    rates = get_exchange_rates()
    row_counts = extract_and_load.partial(rates=rates).expand(path=list_files())
    publish_raw_orders(row_counts)


ingest_orders()
