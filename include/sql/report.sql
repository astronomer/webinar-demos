{% set report_date = ds | default(dag_run.start_date.strftime("%Y-%m-%d"), true) %}

MERGE INTO daily_planet_report AS target
USING (
    WITH booking_facts AS (
        SELECT
            '{{ report_date }}'::DATE AS report_date,
            b.booking_id,
            p.planet_name,
            b.passengers,
            IFF(b.return_date < '{{ report_date }}'::DATE, 'completed', 'active') AS state,
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
        WHERE CAST(b.booked_at AS DATE) <= '{{ ds }}'::DATE
    )
    SELECT
        report_date,
        planet_name,
        SUM(passengers) AS total_passengers,
        SUM(IFF(state = 'active', 1, 0)) AS active_trips,
        SUM(IFF(state = 'completed', 1, 0)) AS completed_trips,
        SUM(gross_fare_usd) AS total_gross_fare_usd,
        SUM(discount_usd) AS total_discounts_usd,
        SUM(net_fare_usd) AS total_net_fare_usd,
        SUM(paid_usd) AS total_paid_usd
    FROM booking_facts
    GROUP BY report_date, planet_name
) AS source
ON target.report_date = source.report_date
   AND target.planet_name = source.planet_name
WHEN MATCHED THEN UPDATE SET
    total_passengers     = source.total_passengers,
    active_trips         = source.active_trips,
    completed_trips      = source.completed_trips,
    total_gross_fare_usd = source.total_gross_fare_usd,
    total_discounts_usd  = source.total_discounts_usd,
    total_net_fare_usd   = source.total_net_fare_usd,
    total_paid_usd       = source.total_paid_usd
WHEN NOT MATCHED THEN INSERT (
    report_date,
    planet_name,
    total_passengers,
    active_trips,
    completed_trips,
    total_gross_fare_usd,
    total_discounts_usd,
    total_net_fare_usd,
    total_paid_usd
) VALUES (
    source.report_date,
    source.planet_name,
    source.total_passengers,
    source.active_trips,
    source.completed_trips,
    source.total_gross_fare_usd,
    source.total_discounts_usd,
    source.total_net_fare_usd,
    source.total_paid_usd
);
