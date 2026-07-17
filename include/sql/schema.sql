-- Drop first (referencing tables before referenced ones) so this setup owns the
-- schema even if an older AstroTrips demo left tables with a different column set.
DROP TABLE IF EXISTS daily_planet_report;
DROP TABLE IF EXISTS cancellations;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS promo_codes;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS planets;

CREATE TABLE IF NOT EXISTS planets (
  planet_id       INTEGER PRIMARY KEY,
  planet_name     VARCHAR NOT NULL,
  base_multiplier FLOAT NOT NULL -- cost multiplier for trips to this planet (e.g. higher landing difficulty)
);

CREATE TABLE IF NOT EXISTS routes (
  route_id       INTEGER PRIMARY KEY,
  destination_id INTEGER NOT NULL REFERENCES planets(planet_id),
  base_fare_usd  INTEGER NOT NULL -- costs for the journey to this route's destination
);

CREATE TABLE IF NOT EXISTS customers (
  customer_id INTEGER PRIMARY KEY,
  full_name   VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS promo_codes (
  promo_code    VARCHAR PRIMARY KEY,
  discount_pct  FLOAT NOT NULL -- 0.10 = 10%
);

CREATE TABLE IF NOT EXISTS bookings (
  booking_id     INTEGER AUTOINCREMENT PRIMARY KEY,
  customer_id    INTEGER NOT NULL REFERENCES customers(customer_id),
  route_id       INTEGER NOT NULL REFERENCES routes(route_id),
  booked_at      TIMESTAMP NOT NULL,
  departure_date DATE NOT NULL,
  return_date    DATE NOT NULL,
  passengers     INTEGER NOT NULL,
  promo_code     VARCHAR
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id  INTEGER AUTOINCREMENT PRIMARY KEY,
  booking_id  INTEGER NOT NULL REFERENCES bookings(booking_id),
  paid_at     TIMESTAMP NOT NULL,
  amount_usd  INTEGER NOT NULL
);

-- New for the Common AI demo: cancellations drive the "why did Europa revenue drop"
-- investigation. reason_code stays categorical on purpose (honest SQL aggregation, no
-- free text for the model to parse).
CREATE TABLE IF NOT EXISTS cancellations (
  cancellation_id   INTEGER AUTOINCREMENT PRIMARY KEY,
  booking_id        INTEGER NOT NULL REFERENCES bookings(booking_id),
  cancelled_at      TIMESTAMP NOT NULL,
  reason_code       VARCHAR NOT NULL, -- LAUNCH_DELAY, WEATHER, CUSTOMER_REQUEST, MEDICAL, SCHEDULING
  refund_amount_usd INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_planet_report (
  report_date              DATE NOT NULL,
  planet_name              VARCHAR NOT NULL,

  total_passengers         BIGINT NOT NULL,
  active_trips             BIGINT NOT NULL,
  completed_trips          BIGINT NOT NULL,
  cancelled_trips          BIGINT NOT NULL, -- new: trips cancelled on/before report_date

  total_gross_fare_usd     BIGINT NOT NULL,
  total_discounts_usd      BIGINT NOT NULL,
  total_net_fare_usd       BIGINT NOT NULL,
  total_paid_usd           BIGINT NOT NULL,
  total_refunds_usd        BIGINT NOT NULL, -- new: refunds issued on/before report_date

  -- realized revenue = total_paid_usd - total_refunds_usd
  PRIMARY KEY (report_date, planet_name)
);
