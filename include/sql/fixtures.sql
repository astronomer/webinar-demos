INSERT INTO planets VALUES
(1001, 'Moon',   0.80),
(1002, 'Mars',   1.20),
(1003, 'Europa', 1.50);

INSERT INTO routes VALUES
(1001, 1001,  5000),  -- Moon,   effective 4000/pax
(1002, 1002, 25000),  -- Mars,   effective 30000/pax
(1003, 1003, 40000);  -- Europa, effective 60000/pax

INSERT INTO promo_codes VALUES
('ASTRO10', 0.10),
('ASTRO20', 0.20);

INSERT INTO customers VALUES
(1001, 'Ava Chen'),
(1002, 'Noah Patel'),
(1003, 'Mia Rodriguez'),
(1004, 'Liam Okafor'),
(1005, 'Zara Kim'),
(1006, 'Omar Hassan');

-- Gross fare = passengers × base_fare_usd × base_multiplier
INSERT INTO bookings (customer_id, route_id, booked_at, departure_date, return_date, passengers, promo_code) VALUES
-- October
(1001, 1001, '2025-10-01 09:10:00', '2025-10-15', '2025-10-18', 2, 'ASTRO10'), -- #1  Moon,   completed Oct 18
(1005, 1002, '2025-10-15 11:00:00', '2026-01-15', '2026-07-15', 2, 'ASTRO10'), -- #2  Mars,   long-haul
(1006, 1001, '2025-10-22 14:30:00', '2025-11-01', '2025-11-05', 1, NULL),      -- #3  Moon,   completed Nov 5
-- November
(1001, 1003, '2025-11-03 10:15:00', '2026-04-01', '2027-04-01', 1, NULL),      -- #4  Europa, long-haul
(1002, 1001, '2025-11-18 08:45:00', '2025-12-01', '2025-12-05', 4, 'ASTRO20'), -- #5  Moon,   completed Dec 5
(1003, 1002, '2025-11-25 16:00:00', '2026-02-10', '2026-08-10', 2, NULL),      -- #6  Mars,   long-haul
-- December
(1002, 1002, '2025-12-05 14:45:00', '2026-03-01', '2026-09-01', 1, NULL),      -- #7  Mars,   long-haul
(1004, 1003, '2025-12-10 09:00:00', '2026-08-01', '2027-08-01', 2, 'ASTRO20'), -- #8  Europa, long-haul
(1003, 1001, '2025-12-20 11:30:00', '2026-01-10', '2026-01-13', 3, NULL),      -- #9  Moon,   completed Jan 13
(1004, 1001, '2025-12-28 16:20:00', '2026-02-01', '2026-02-07', 3, 'ASTRO10'), -- #10 Moon,   active Dec, completed Feb
-- January
(1005, 1003, '2026-01-05 13:00:00', '2026-09-01', '2027-09-01', 2, 'ASTRO20'), -- #11 Europa, long-haul
(1006, 1002, '2026-01-15 10:30:00', '2026-05-01', '2026-11-01', 1, NULL);      -- #12 Mars,   long-haul

-- Payments (amount = passengers × base_fare × multiplier × (1 − discount))
-- JOIN on booked_at (unique per fixture row) avoids assuming AUTOINCREMENT
-- starts at 1 — Snowflake TRUNCATE does not reliably reset sequences.
INSERT INTO payments (booking_id, paid_at, amount_usd)
WITH fp (booked_at, paid_at, amount_usd) AS (
  SELECT v.booked_at::TIMESTAMP, v.paid_at::TIMESTAMP, v.amount_usd
  FROM (VALUES
    ('2025-10-01 09:10:00', '2025-10-01 09:20:00',  7200),  -- 2 × 4000  × 0.90 =  7200
    ('2025-10-15 11:00:00', '2025-10-15 11:15:00', 54000),  -- 2 × 30000 × 0.90 = 54000
    ('2025-10-22 14:30:00', '2025-10-22 14:45:00',  4000),  -- 1 × 4000         =  4000
    ('2025-11-03 10:15:00', '2025-11-03 10:30:00', 60000),  -- 1 × 60000        = 60000
    ('2025-11-18 08:45:00', '2025-11-18 09:00:00', 12800),  -- 4 × 4000  × 0.80 = 12800
    ('2025-11-25 16:00:00', '2025-11-25 16:15:00', 60000),  -- 2 × 30000        = 60000
    ('2025-12-05 14:45:00', '2025-12-05 15:00:00', 30000),  -- 1 × 30000        = 30000
    ('2025-12-10 09:00:00', '2025-12-10 09:15:00', 96000),  -- 2 × 60000 × 0.80 = 96000
    ('2025-12-20 11:30:00', '2025-12-20 11:45:00', 12000),  -- 3 × 4000         = 12000
    ('2025-12-28 16:20:00', '2025-12-28 16:30:00', 10800),  -- 3 × 4000  × 0.90 = 10800
    ('2026-01-05 13:00:00', '2026-01-05 13:15:00', 96000),  -- 2 × 60000 × 0.80 = 96000
    ('2026-01-15 10:30:00', '2026-01-15 10:45:00', 30000)   -- 1 × 30000        = 30000
  ) v (booked_at, paid_at, amount_usd)
)
SELECT b.booking_id, fp.paid_at, fp.amount_usd
FROM fp
JOIN bookings b ON b.booked_at = fp.booked_at;

-- ─── Daily planet reports ──────────────────────────────────────────────────
-- Columns: report_date, planet_name,
--          total_passengers, active_trips, completed_trips,
--          total_gross_fare_usd, total_discounts_usd, total_net_fare_usd, total_paid_usd
--
-- State: completed if return_date < report_date, else active.
-- Only bookings with DATE(booked_at) <= report_date are included.

-- 2025-10-31 ─ bookings #1 (Moon, done), #2 (Mars, active), #3 (Moon, active)
INSERT INTO daily_planet_report VALUES
('2025-10-31', 'Moon',   3, 1, 1,  12000,   800,  11200,  11200),
('2025-10-31', 'Mars',   2, 1, 0,  60000,  6000,  54000,  54000);

-- 2025-11-30 ─ adds #4 (Europa, active), #5 (Moon, active), #6 (Mars, active)
--              #3 completed Nov 5
INSERT INTO daily_planet_report VALUES
('2025-11-30', 'Moon',   7, 1, 2,  28000,  4000,  24000,  24000),
('2025-11-30', 'Mars',   4, 2, 0, 120000,  6000, 114000, 114000),
('2025-11-30', 'Europa', 1, 1, 0,  60000,     0,  60000,  60000);

-- 2025-12-31 ─ adds #7 (Mars, active), #8 (Europa, active), #9 (Moon, active), #10 (Moon, active)
--              #5 completed Dec 5
--   Moon:   #1(done), #3(done), #5(done), #9(active), #10(active)  → 13 pax, 2 active, 3 done
--   Mars:   #2(active), #6(active), #7(active)                     →  5 pax, 3 active, 0 done
--   Europa: #4(active), #8(active)                                 →  3 pax, 2 active, 0 done
INSERT INTO daily_planet_report VALUES
('2025-12-31', 'Moon',   13, 2, 3,  52000,  5200,  46800,  46800),
('2025-12-31', 'Mars',    5, 3, 0, 150000,  6000, 144000, 144000),
('2025-12-31', 'Europa',  3, 2, 0, 180000, 24000, 156000, 156000);

-- 2026-01-31 ─ adds #11 (Europa, active), #12 (Mars, active)
--              #9 completed Jan 13
--   Moon:   #1(done), #3(done), #5(done), #9(done), #10(active)   → 13 pax, 1 active, 4 done
--   Mars:   #2(active), #6(active), #7(active), #12(active)       →  6 pax, 4 active, 0 done
--   Europa: #4(active), #8(active), #11(active)                   →  5 pax, 3 active, 0 done
INSERT INTO daily_planet_report VALUES
('2026-01-31', 'Moon',   13, 1, 4,  52000,  5200,  46800,  46800),
('2026-01-31', 'Mars',    6, 4, 0, 180000,  6000, 174000, 174000),
('2026-01-31', 'Europa',  5, 3, 0, 300000, 48000, 252000, 252000);
