CREATE TABLE IF NOT EXISTS planets (
  planet_id       INTEGER PRIMARY KEY,
  planet_name     VARCHAR NOT NULL,
  base_multiplier FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
  route_id       INTEGER PRIMARY KEY,
  destination_id INTEGER NOT NULL REFERENCES planets(planet_id),
  base_fare_usd  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
  customer_id INTEGER PRIMARY KEY,
  full_name   VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS promo_codes (
  promo_code    VARCHAR PRIMARY KEY,
  discount_pct  FLOAT NOT NULL
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

CREATE TABLE IF NOT EXISTS daily_planet_report (
  report_date              DATE NOT NULL,
  planet_name              VARCHAR NOT NULL,

  total_passengers         INTEGER NOT NULL,
  active_trips             INTEGER NOT NULL,
  completed_trips          INTEGER NOT NULL,

  total_gross_fare_usd     INTEGER NOT NULL,
  total_discounts_usd      INTEGER NOT NULL,
  total_net_fare_usd       INTEGER NOT NULL,
  total_paid_usd           INTEGER NOT NULL,

  PRIMARY KEY (report_date, planet_name)
);

CREATE TABLE IF NOT EXISTS customer_lifetime_stats (
  customer_id          INTEGER PRIMARY KEY,
  full_name            VARCHAR NOT NULL,

  total_bookings       INTEGER NOT NULL,
  total_passengers     INTEGER NOT NULL,
  total_paid_usd       INTEGER NOT NULL,

  first_booking_date   DATE,
  last_booking_date    DATE
);

CREATE TABLE IF NOT EXISTS route_performance (
  report_date          DATE NOT NULL,
  route_id             INTEGER NOT NULL,
  planet_name          VARCHAR NOT NULL,

  total_bookings       INTEGER NOT NULL,
  total_passengers     INTEGER NOT NULL,
  total_revenue_usd    INTEGER NOT NULL,

  PRIMARY KEY (report_date, route_id)
);
