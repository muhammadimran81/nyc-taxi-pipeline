"""Load step: bulk-load cleaned trips into PostgreSQL with COPY (idempotent per month)."""
import io
import json
import logging
import time

import psycopg2
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

TRIP_COLUMNS = [
    "trip_month", "vendor_id", "pickup_at", "dropoff_at", "passenger_count", "trip_distance",
    "pickup_location_id", "dropoff_location_id", "payment_type", "fare_amount", "tip_amount",
    "total_amount",
]


def load_month(clean_path: str, month: str, warehouse_uri: str, stats: dict | None = None) -> int:
    """Replace one month in raw.yellow_trips, then write an audit row. Safe to re-run."""
    started = time.time()
    table = pq.ParquetFile(clean_path)
    cols = ", ".join(TRIP_COLUMNS)
    rows = 0
    with psycopg2.connect(warehouse_uri) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM raw.yellow_trips WHERE trip_month = %s", (month,))
        log.info("Deleted %s existing rows for %s", cur.rowcount, month)
        for batch in table.iter_batches(batch_size=250_000, columns=TRIP_COLUMNS):
            buf = io.StringIO()
            batch.to_pandas().to_csv(buf, header=False, index=False)
            buf.seek(0)
            cur.copy_expert(f"COPY raw.yellow_trips ({cols}) FROM STDIN WITH (FORMAT csv)", buf)
            rows += batch.num_rows
        seconds = round(time.time() - started, 1)
        cur.execute(
            "INSERT INTO raw.load_audit (trip_month, rows_loaded, load_seconds, cleaning_stats) "
            "VALUES (%s, %s, %s, %s)",
            (month, rows, seconds, json.dumps(stats or {})),
        )
    log.info("Loaded %s rows for %s in %ss", rows, month, seconds)
    return rows


def load_zones(csv_path: str, warehouse_uri: str) -> int:
    with psycopg2.connect(warehouse_uri) as conn, conn.cursor() as cur, open(csv_path) as f:
        cur.execute("TRUNCATE raw.taxi_zones")
        cur.copy_expert(
            "COPY raw.taxi_zones (location_id, borough, zone, service_zone) "
            "FROM STDIN WITH (FORMAT csv, HEADER true)", f)
        cur.execute("SELECT count(*) FROM raw.taxi_zones")
        return cur.fetchone()[0]
