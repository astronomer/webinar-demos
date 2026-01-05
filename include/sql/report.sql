INSERT INTO daily_planet_report
WITH bookings AS (
    SELECT
        $reportDate::DATE AS report_date,
        b.booking_id,
        p.planet_name,
        b.passengers,
        CASE
            WHEN b.return_date < $reportDate::DATE THEN 'completed'
            ELSE 'active'
        END AS state,
        CAST(ROUND(b.passengers * r.base_fare_usd * p.base_multiplier) AS INTEGER) AS gross_fare_usd,
        CAST(ROUND(
            (b.passengers * r.base_fare_usd * p.base_multiplier)
            * COALESCE(pc.discount_pct, 0)
        ) AS INTEGER) AS discount_usd,
        CAST(ROUND(
            (b.passengers * r.base_fare_usd * p.base_multiplier)
            * (1 - COALESCE(pc.discount_pct, 0))
        ) AS INTEGER) AS net_fare_usd,
        pay.amount_usd AS paid_usd
    FROM bookings b
    LEFT JOIN promo_codes pc ON pc.promo_code = b.promo_code
    JOIN payments pay ON pay.booking_id = b.booking_id
    JOIN routes r ON r.route_id = b.route_id
    JOIN planets p ON p.planet_id = r.destination_id
    WHERE DATE(b.booked_at) <= $reportDate::DATE
)
SELECT
    report_date,
    planet_name,
    SUM(passengers) AS total_passengers,
    SUM(IF(state = 'active', 1, 0)) AS active_trips,
    SUM(IF(state = 'completed', 1, 0)) AS completed_trips,
    SUM(gross_fare_usd) AS total_gross_fare_usd,
    SUM(discount_usd) AS total_discounts_usd,
    SUM(net_fare_usd) AS total_net_fare_usd,
    SUM(paid_usd) AS total_paid_usd
FROM bookings
GROUP BY report_date, planet_name;
