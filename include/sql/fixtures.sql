INSERT INTO planets VALUES
(1001, 'Moon', 0.80),
(1002, 'Mars', 1.20),
(1003, 'Europa', 1.50);

INSERT INTO routes VALUES
(1001, 1001, 5000),
(1002, 1002, 25000),
(1003, 1003, 40000);

INSERT INTO promo_codes VALUES
('ASTRO10', 0.10),
('ASTRO20', 0.20);

INSERT INTO customers VALUES
(1001, 'Ava Chen'),
(1002, 'Noah Patel'),
(1003, 'Mia Rodriguez'),
(1004, 'Liam Okafor');

INSERT INTO bookings (customer_id, route_id, booked_at, departure_date, return_date, passengers, promo_code) VALUES
(1001, 1001, '2025-10-01 09:10:00', '2025-10-15', '2025-10-18', 2, 'ASTRO10'),
(1002, 1002, '2025-12-05 14:45:00', '2026-03-01', '2026-09-01', 1, NULL),
(1003, 1001, '2025-12-20 11:30:00', '2026-01-10', '2026-01-13', 3, NULL),
(1004, 1003, '2025-12-28 16:20:00', '2026-06-15', '2027-06-15', 2, 'ASTRO20');

INSERT INTO payments (booking_id, paid_at, amount_usd) VALUES
(1, '2025-10-01 09:20:00', 7200),
(2, '2025-12-05 15:00:00', 30000),
(3, '2025-12-20 11:45:00', 12000),
(4, '2025-12-28 16:30:00', 96000);
