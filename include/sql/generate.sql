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
    (SELECT customer_id FROM customers ORDER BY random() LIMIT 1) AS customer_id,
    (SELECT route_id FROM routes ORDER BY random() LIMIT 1) AS route_id,
    (
      '{{ ds }}'::TIMESTAMP
      - INTERVAL '{{ 100 + i }} day'
      + CAST(floor(random() * 24 * 60) AS BIGINT) * INTERVAL 1 MINUTE
    ) AS booked_at,
    {{ 1 + (i % 4) }} AS passengers
),
dated AS (
  SELECT
    customer_id,
    route_id,
    booked_at,
    passengers,
    (CAST(booked_at AS DATE) + (3 + CAST(floor(random() * 58) AS INTEGER))) AS departure_date
  FROM picked
)
SELECT
  d.customer_id,
  d.route_id,
  d.booked_at,
  d.departure_date,
  (d.departure_date + (3 + CAST(floor(random() * 397) AS INTEGER))) AS return_date,
  d.passengers,
  CASE
    WHEN random() < 0.20 -- 20% chance to apply a random promo code
     AND (SELECT COUNT(*) FROM promo_codes) > 0
    THEN (SELECT promo_code FROM promo_codes ORDER BY random() LIMIT 1)
    ELSE NULL
  END AS promo_code
FROM dated d;

INSERT INTO payments (booking_id, paid_at, amount_usd)
SELECT
  b.booking_id,
  b.booked_at + (5 + CAST(floor(random() * 55) AS BIGINT)) * INTERVAL 1 MINUTE AS paid_at,

  CAST(ROUND(
    (b.passengers * r.base_fare_usd * p.base_multiplier)
    * (1 - COALESCE(pc.discount_pct, 0))
  ) AS INTEGER) AS amount_usd
FROM bookings b
JOIN routes r ON r.route_id = b.route_id
JOIN planets p ON p.planet_id = r.destination_id
LEFT JOIN promo_codes pc ON pc.promo_code = b.promo_code
WHERE b.booking_id = currval('booking_id_seq');

{% endfor %}
