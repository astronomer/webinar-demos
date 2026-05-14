MERGE INTO route_performance AS target
USING (
    SELECT
        '{{ ds }}'::DATE AS report_date,
        r.route_id,
        p.planet_name,
        COUNT(b.booking_id) AS total_bookings,
        COALESCE(SUM(b.passengers), 0) AS total_passengers,
        COALESCE(SUM(pay.amount_usd), 0) AS total_revenue_usd
    FROM routes r
    JOIN planets p ON p.planet_id = r.destination_id
    LEFT JOIN bookings b
        ON b.route_id = r.route_id
        AND CAST(b.booked_at AS DATE) <= '{{ ds }}'::DATE
    LEFT JOIN payments pay ON pay.booking_id = b.booking_id
    GROUP BY r.route_id, p.planet_name
) AS source
ON target.report_date = source.report_date
   AND target.route_id = source.route_id
WHEN MATCHED THEN UPDATE SET
    planet_name       = source.planet_name,
    total_bookings    = source.total_bookings,
    total_passengers  = source.total_passengers,
    total_revenue_usd = source.total_revenue_usd
WHEN NOT MATCHED THEN INSERT (
    report_date,
    route_id,
    planet_name,
    total_bookings,
    total_passengers,
    total_revenue_usd
) VALUES (
    source.report_date,
    source.route_id,
    source.planet_name,
    source.total_bookings,
    source.total_passengers,
    source.total_revenue_usd
);
