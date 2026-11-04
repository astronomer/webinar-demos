-- The returns table for the third demo. Postgres runs this once, when the shop-db volume is first created.
-- ./demo.sh reset runs it again, so it starts from scratch every time.
-- The warehouse team's scanner writes one row per returned parcel. Nothing publishes an event:
-- the only way to know about a new return is to look at the table.

DROP TABLE IF EXISTS returns;

CREATE TABLE returns (
    id         SERIAL PRIMARY KEY,
    order_id   INTEGER NOT NULL,
    sku        TEXT NOT NULL,
    reason     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Two returns from before the watcher existed.
INSERT INTO returns (order_id, sku, reason) VALUES
    (1001, 'TSHIRT-BLUE-M', 'wrong size'),
    (1002, 'MUG-LOGO', 'damaged in transit');
