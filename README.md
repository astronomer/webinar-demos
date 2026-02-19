# Stopping bad data before it breaks your Airflow pipelines

## Setup

Run the `setup` DAG to initialize the demo. It is safe to re-run at any time:
it creates the tables if they don't exist yet (`IF NOT EXISTS`), truncates all
data, and re-inserts the seed fixtures.

### Hard reset (drop everything)

If you need to remove the tables entirely, for example to apply a schema
change, run this in Snowflake and then re-run the `setup` DAG:

```sql
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS daily_planet_report;
DROP TABLE IF EXISTS promo_codes;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS planets;
```

## Making checks fail

### `check_planet_count` (SQLValueCheckOperator)

Expects exactly 3 planets. Adding a fourth makes `COUNT(*) = 4 ≠ 3`.

```sql
-- FAIL
INSERT INTO planets VALUES (9999, 'Pluto', 2.00);

-- REVERT
DELETE FROM planets WHERE planet_id = 9999;
```

### `check_booking_columns` (SQLColumnCheckOperator)

Expects `passengers >= 1`. Setting it to 0 violates the `min` check.

```sql
-- FAIL
UPDATE bookings SET passengers = 0 WHERE booking_id = 1;

-- REVERT  (original value from fixtures: 2 passengers)
UPDATE bookings SET passengers = 2 WHERE booking_id = 1;
```

### `check_no_orphaned_payments` (SQLCheckOperator)

Snowflake defines but does not enforce FK constraints, so this insert succeeds
and creates a payment with no matching booking.

```sql
-- FAIL
INSERT INTO payments (booking_id, paid_at, amount_usd)
VALUES (99999, CURRENT_TIMESTAMP, 5000);

-- REVERT
DELETE FROM payments WHERE booking_id = 99999;
```

### `check_avg_payment_in_range` (SQLThresholdCheckOperator)

Expects `AVG(amount_usd)` between 4 000 and 200 000. One absurdly large payment
pulls the average far above the upper bound.

```sql
-- FAIL
INSERT INTO payments (booking_id, paid_at, amount_usd)
VALUES (1, CURRENT_TIMESTAMP, 999999999);

-- REVERT
DELETE FROM payments WHERE amount_usd = 999999999;
```

### `check_report_revenue_vs_last_week` (SQLIntervalCheckOperator)

Expects `SUM(total_net_fare_usd)` for today vs 7 days ago to stay within 2×.
Multiplying today's net fare by 10 pushes the ratio to ~10×.

_Requires data for both today and 7 days ago, use backfill first if needed._

```sql
-- FAIL
UPDATE daily_planet_report
SET total_net_fare_usd = total_net_fare_usd * 10
WHERE report_date = CURRENT_DATE;

-- REVERT
UPDATE daily_planet_report
SET total_net_fare_usd = total_net_fare_usd / 10
WHERE report_date = CURRENT_DATE;
```

### `check_report_business_rules` (SQLTableCheckOperator)

**`net_fare_not_negative`**: set one planet's net fare below zero:
```sql
-- FAIL
UPDATE daily_planet_report
SET total_net_fare_usd = -1
WHERE report_date = CURRENT_DATE AND planet_name = 'Moon';

-- REVERT
UPDATE daily_planet_report
SET total_net_fare_usd = ABS(total_net_fare_usd)
WHERE report_date = CURRENT_DATE AND planet_name = 'Moon';
```

**`discounts_leq_gross`**: make discounts exceed the gross fare:
```sql
-- FAIL
UPDATE daily_planet_report
SET total_discounts_usd = total_gross_fare_usd + 1
WHERE report_date = CURRENT_DATE AND planet_name = 'Mars';

-- REVERT
UPDATE daily_planet_report
SET total_discounts_usd = total_gross_fare_usd - 1
WHERE report_date = CURRENT_DATE AND planet_name = 'Mars';
```

**`has_rows_for_today`**: delete all of today's report rows:
```sql
-- FAIL
DELETE FROM daily_planet_report WHERE report_date = CURRENT_DATE;

-- REVERT  (re-trigger the build_report task in the daily_report DAG)
```
