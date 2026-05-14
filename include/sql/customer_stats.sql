MERGE INTO customer_lifetime_stats AS target
USING (
    SELECT
        c.customer_id,
        c.full_name,
        COUNT(b.booking_id) AS total_bookings,
        COALESCE(SUM(b.passengers), 0) AS total_passengers,
        COALESCE(SUM(pay.amount_usd), 0) AS total_paid_usd,
        MIN(CAST(b.booked_at AS DATE)) AS first_booking_date,
        MAX(CAST(b.booked_at AS DATE)) AS last_booking_date
    FROM customers c
    LEFT JOIN bookings b
        ON b.customer_id = c.customer_id
        AND CAST(b.booked_at AS DATE) <= '{{ ds }}'::DATE
    LEFT JOIN payments pay ON pay.booking_id = b.booking_id
    GROUP BY c.customer_id, c.full_name
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    full_name          = source.full_name,
    total_bookings     = source.total_bookings,
    total_passengers   = source.total_passengers,
    total_paid_usd     = source.total_paid_usd,
    first_booking_date = source.first_booking_date,
    last_booking_date  = source.last_booking_date
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    full_name,
    total_bookings,
    total_passengers,
    total_paid_usd,
    first_booking_date,
    last_booking_date
) VALUES (
    source.customer_id,
    source.full_name,
    source.total_bookings,
    source.total_passengers,
    source.total_paid_usd,
    source.first_booking_date,
    source.last_booking_date
);
