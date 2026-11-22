-- Runs once, the first time the warehouse container starts.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.yellow_trips (
    trip_month          char(7)       NOT NULL,  -- 'YYYY-MM', the load partition
    vendor_id           int,
    pickup_at           timestamp     NOT NULL,
    dropoff_at          timestamp     NOT NULL,
    passenger_count     int,
    trip_distance       numeric(8,2),
    pickup_location_id  int           NOT NULL,
    dropoff_location_id int           NOT NULL,
    payment_type        int,
    fare_amount         numeric(10,2),
    tip_amount          numeric(10,2),
    total_amount        numeric(10,2)
);
CREATE INDEX IF NOT EXISTS ix_yellow_trips_month ON raw.yellow_trips (trip_month);

CREATE TABLE IF NOT EXISTS raw.taxi_zones (
    location_id  int PRIMARY KEY,
    borough      text,
    zone         text,
    service_zone text
);

CREATE TABLE IF NOT EXISTS raw.load_audit (
    id             serial PRIMARY KEY,
    trip_month     char(7)     NOT NULL,
    rows_loaded    bigint      NOT NULL,
    load_seconds   numeric(8,1),
    cleaning_stats jsonb,
    loaded_at      timestamptz NOT NULL DEFAULT now()
);
