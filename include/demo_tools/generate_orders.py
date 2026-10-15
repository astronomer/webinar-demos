"""Write the demo's store order files to the MinIO landing bucket.

Run by ./demo.sh inside the scheduler container, which already has boto3:

    python -m include.demo_tools.generate_orders reset      # only the four original stores
    python -m include.demo_tools.generate_orders new-store  # the next store that opens
    python -m include.demo_tools.generate_orders big-file   # the online store's much bigger file
    python -m include.demo_tools.generate_orders list

The files are deterministic: a store always gets the same orders. Products, prices and
exchange rates match warehouse/init.sql and fake_api/server.py.
"""

from __future__ import annotations

import csv
import io
import random
import sys

import boto3

BUCKET = "landing"
PREFIX = "orders/"
BUSINESS_DATE = "2026-10-13"

PRODUCTS = [
    ("Coffee mug", 14.0),
    ("T-shirt", 25.0),
    ("Hoodie", 59.0),
    ("Backpack", 89.0),
    ("Sneakers", 120.0),
]
RATES = {"USD": 1.0, "GBP": 0.75, "EUR": 0.86, "JPY": 151.2, "CAD": 1.39, "AUD": 1.53, "INR": 88.6}

# store_id: (currency, number of orders on BUSINESS_DATE). Berlin usually gets about 210 orders
# a day (see warehouse/init.sql): the drop to 86 is the anomaly the v5 agent should find.
ORIGINAL_STORES = {"nyc": ("USD", 318), "london": ("GBP", 244), "berlin": ("EUR", 86), "tokyo": ("JPY", 183)}
NEW_STORES = {"toronto": ("CAD", 152), "sydney": ("AUD", 131), "paris": ("EUR", 168), "mumbai": ("INR", 175)}
BIG_STORE = ("online", "USD", 400_000)


def get_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio-admin",
        aws_secret_access_key="minio-demo-2026",
        region_name="us-east-1",
    )


def build_csv(store: str, currency: str, order_count: int) -> bytes:
    rng = random.Random(f"{store}-{BUSINESS_DATE}")
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        [
            "order_id",
            "order_date",
            "store_id",
            "customer_email",
            "product",
            "quantity",
            "unit_price",
            "currency",
        ]
    )
    for i in range(1, order_count + 1):
        product, price_usd = rng.choice(PRODUCTS)
        writer.writerow(
            [
                f"{store}-{BUSINESS_DATE.replace('-', '')}-{i:06d}",
                BUSINESS_DATE,
                store,
                f"customer{rng.randint(1, 99_999)}@example.com",
                product,
                rng.randint(1, 3),
                f"{price_usd * RATES[currency]:.2f}",
                currency,
            ]
        )
    return buffer.getvalue().encode()


def format_size(size: int) -> str:
    return f"{size / 1e6:.1f} MB" if size >= 1e6 else f"{size / 1e3:.0f} KB"


def upload_store(client, store: str, currency: str, order_count: int) -> None:
    body = build_csv(store, currency, order_count)
    client.put_object(Bucket=BUCKET, Key=f"{PREFIX}store_{store}.csv", Body=body)
    print(f"store_{store}.csv: {order_count:,} orders ({format_size(len(body))})")


def list_keys(client) -> dict[str, int]:
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX)
    return {obj["Key"]: obj["Size"] for page in pages for obj in page.get("Contents", [])}


def reset(client) -> None:
    for key in list_keys(client):
        client.delete_object(Bucket=BUCKET, Key=key)
    for store, (currency, order_count) in ORIGINAL_STORES.items():
        upload_store(client, store, currency, order_count)


def add_new_store(client) -> None:
    existing = list_keys(client)
    for store, (currency, order_count) in NEW_STORES.items():
        if f"{PREFIX}store_{store}.csv" not in existing:
            upload_store(client, store, currency, order_count)
            return
    print("Every new store has already opened. Run ./demo.sh reset to start over.")


def list_files(client) -> None:
    for key, size in sorted(list_keys(client).items()):
        print(f"{key.removeprefix(PREFIX):<24} {format_size(size):>9}")


def main(command: str) -> None:
    client = get_client()
    if command == "reset":
        reset(client)
    elif command == "new-store":
        add_new_store(client)
    elif command == "big-file":
        upload_store(client, *BIG_STORE)
    elif command == "list":
        list_files(client)
    else:
        sys.exit(f"unknown command {command!r}: use reset, new-store, big-file or list")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "")
