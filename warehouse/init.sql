-- Warehouse for the ETL/ELT demo.
-- Runs on the first start of the warehouse container and again on every ./demo.sh reset, so
-- every statement must be safe to rerun. Products, prices and exchange rates match
-- include/demo_tools/generate_orders.py and fake_api/server.py.

DROP VIEW IF EXISTS rev_x;
DROP TABLE IF EXISTS daily_revenue_staging;
DROP TABLE IF EXISTS daily_revenue;
DROP TABLE IF EXISTS raw_orders;

-- One row per order. Loaded by ingest_orders from v4 on; customer emails are hashed first.
CREATE TABLE raw_orders (
    order_id      TEXT PRIMARY KEY,
    order_date    DATE NOT NULL,
    store_id      TEXT NOT NULL,
    customer_hash TEXT NOT NULL,
    product       TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(12, 2) NOT NULL,
    currency      TEXT NOT NULL,
    amount_usd    NUMERIC(12, 2) NOT NULL
);

-- What analysts (every version) and the agent (v5) read. One row per store and day.
CREATE TABLE daily_revenue (
    order_date    DATE NOT NULL,
    store_id      TEXT NOT NULL,
    order_count   INTEGER NOT NULL,
    revenue_usd   NUMERIC(14, 2) NOT NULL,
    avg_order_usd NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (order_date, store_id)
);

-- History: 13 days of orders for the four original stores, up to the day before the store
-- files' business date (2026-10-13). Each store gets its usual volume, plus or minus 10%.
-- Values come from hashes of store, day and order number instead of random(): Postgres may
-- evaluate (or memoize) a LATERAL subquery once per store, which gave every day the same orders.
WITH store_days AS (
    SELECT
        s.store_id,
        s.currency,
        s.rate,
        d::date AS order_date,
        (s.daily_orders * (0.9 + (abs(hashtext(s.store_id || d::date)) % 1000) / 5000.0))::int AS order_count
    FROM (
        VALUES
            ('nyc', 'USD', 1.0, 320),
            ('london', 'GBP', 0.75, 240),
            ('berlin', 'EUR', 0.86, 210),
            ('tokyo', 'JPY', 151.2, 180)
    ) AS s (store_id, currency, rate, daily_orders)
    CROSS JOIN generate_series(date '2026-09-30', date '2026-10-12', interval '1 day') AS d
),
orders AS (
    SELECT
        sd.*,
        n,
        1 + abs(hashtext(sd.store_id || sd.order_date || n || 'product')) % 5 AS product_idx,
        1 + abs(hashtext(sd.store_id || sd.order_date || n || 'quantity')) % 3 AS quantity
    FROM store_days AS sd
    CROSS JOIN LATERAL generate_series(1, sd.order_count) AS n
)
INSERT INTO raw_orders
SELECT
    store_id || '-' || to_char(order_date, 'YYYYMMDD') || '-' || lpad(n::text, 6, '0'),
    order_date,
    store_id,
    substr(md5(store_id || order_date || n), 1, 16),
    (ARRAY['Coffee mug', 'T-shirt', 'Hoodie', 'Backpack', 'Sneakers'])[product_idx],
    quantity,
    round(((ARRAY[14.0, 25.0, 59.0, 89.0, 120.0])[product_idx] * rate)::numeric, 2),
    currency,
    round((quantity * (ARRAY[14.0, 25.0, 59.0, 89.0, 120.0])[product_idx])::numeric, 2)
FROM orders;

INSERT INTO daily_revenue
SELECT order_date, store_id, count(*), sum(amount_usd), round(avg(amount_usd), 2)
FROM raw_orders
GROUP BY order_date, store_id;

-- v5, optional comparison: the same data behind cryptic column names.
CREATE VIEW rev_x AS
SELECT
    order_date AS dt,
    store_id AS st_cd,
    order_count AS n,
    revenue_usd AS amt_x,
    avg_order_usd AS amt_y
FROM daily_revenue;

-- v5: the agent's role. It can read daily_revenue and rev_x, nothing else.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'agent_reader') THEN
        CREATE ROLE agent_reader LOGIN PASSWORD 'agent-demo-2026';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE warehouse TO agent_reader;
GRANT USAGE ON SCHEMA public TO agent_reader;
GRANT SELECT ON daily_revenue, rev_x TO agent_reader;
