{% for i in range(params.n_bookings) %}

INSERT INTO bookings (
  customer_id,
  route_id,
  booked_at,
  departure_date,
  return_date,
  passengers,
  promo_code
)
WITH picked AS (
  SELECT
    (SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1) AS customer_id,
    (SELECT route_id FROM routes ORDER BY RANDOM() LIMIT 1) AS route_id,
    DATEADD(
      minute,
      CAST(floor(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) * 24 * 60) AS BIGINT),
      DATEADD(day, -({{ 100 + i }}), '{{ ds }}'::TIMESTAMP)
    ) AS booked_at,
    {{ 1 + (i % 4) }} AS passengers
),
dated AS (
  SELECT
    customer_id,
    route_id,
    booked_at,
    passengers,
    DATEADD(
      day,
      3 + CAST(floor(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) * 58) AS INTEGER),
      CAST(booked_at AS DATE)
    ) AS departure_date
  FROM picked
)
SELECT
  d.customer_id,
  d.route_id,
  d.booked_at,
  d.departure_date,
  DATEADD(
    day,
    3 + CAST(floor(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) * 397) AS INTEGER),
    d.departure_date
  ) AS return_date,
  d.passengers,
  CASE
    WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) < 0.20 -- 20% chance to apply a random promo code
     AND (SELECT COUNT(*) FROM promo_codes) > 0
    THEN (SELECT promo_code FROM promo_codes ORDER BY RANDOM() LIMIT 1)
    ELSE NULL
  END AS promo_code
FROM dated d;

INSERT INTO payments (booking_id, paid_at, amount_usd)
SELECT
  b.booking_id,
  DATEADD(
    minute,
    5 + CAST(floor(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) * 55) AS BIGINT),
    b.booked_at
  ) AS paid_at,
  CAST(ROUND(
    (b.passengers * r.base_fare_usd * p.base_multiplier)
    * (1 - COALESCE(pc.discount_pct, 0))
  ) AS INTEGER) AS amount_usd
FROM bookings b
JOIN routes r ON r.route_id = b.route_id
JOIN planets p ON p.planet_id = r.destination_id
LEFT JOIN promo_codes pc ON pc.promo_code = b.promo_code
WHERE b.booking_id = (SELECT MAX(booking_id) FROM bookings);

{% endfor %}
