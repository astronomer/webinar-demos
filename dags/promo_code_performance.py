# TODO: Promo code performance Dag.
# Aggregates per-promo-code KPIs from bookings + payments: total redemptions,
# total passengers booked under each code, gross fare before discount, total
# discount given, and net revenue. Targets a `promo_code_performance` table
# keyed by (report_date, promo_code). Daily snapshot, MERGE pattern, same
# shape as the other reporting Dags.
