CREATE SEQUENCE IF NOT EXISTS booking_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS payment_id_seq START 1;

CREATE TABLE IF NOT EXISTS planets (
  planet_id       INTEGER PRIMARY KEY,
  planet_name     VARCHAR NOT NULL,
  base_multiplier DOUBLE NOT NULL -- cost multiplier for trips to this planet (e.g. higher landing dificulty)
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
  discount_pct  DOUBLE NOT NULL -- 0.10 = 10%
);

CREATE TABLE IF NOT EXISTS bookings (
  booking_id     INTEGER PRIMARY KEY DEFAULT nextval('booking_id_seq'),
  customer_id    INTEGER NOT NULL REFERENCES customers(customer_id),
  route_id       INTEGER NOT NULL REFERENCES routes(route_id),
  booked_at      TIMESTAMP NOT NULL,
  departure_date DATE NOT NULL,
  return_date    DATE NOT NULL,
  passengers     INTEGER NOT NULL,
  promo_code     VARCHAR
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id  INTEGER PRIMARY KEY DEFAULT nextval('payment_id_seq'),
  booking_id  INTEGER NOT NULL REFERENCES bookings(booking_id),
  paid_at     TIMESTAMP NOT NULL,
  amount_usd  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_planet_report (
  report_date              DATE NOT NULL,
  planet_name              VARCHAR NOT NULL,

  total_passengers         BIGINT NOT NULL,
  active_trips             BIGINT NOT NULL,
  completed_trips          BIGINT NOT NULL,

  total_gross_fare_usd     BIGINT NOT NULL,
  total_discounts_usd      BIGINT NOT NULL,
  total_net_fare_usd       BIGINT NOT NULL,
  total_paid_usd           BIGINT NOT NULL,

  PRIMARY KEY (report_date, planet_name)
);
