# NYC Taxi ELT Pipeline — Airflow, Docker, PostgreSQL, dbt

An end-to-end batch data pipeline that ingests NYC yellow-taxi trip records each month, cleans them in Python with a data-quality gate, bulk-loads them into a PostgreSQL warehouse, and builds tested analytics tables with dbt. Everything runs locally with one `docker compose up`.

## Architecture

```
NYC TLC (Parquet, ~3M trips/month)
        │  extract   – download month file + zone lookup (skips if already downloaded)
        ▼
  data/raw/*.parquet
        │  transform – pandas cleaning rules, rejected-row counts, fail if < 80% rows pass
        ▼
  data/clean/*.parquet
        │  load      – idempotent: DELETE month + PostgreSQL COPY in 250k-row batches, audit row
        ▼
  warehouse.raw.yellow_trips ── raw.taxi_zones ── raw.load_audit
        │  dbt build – staging view + 2 mart tables + 10 data tests
        ▼
  warehouse.analytics.fct_daily_trips
  warehouse.analytics.fct_monthly_zone_revenue
```

Orchestrated by **Apache Airflow 3** (`@monthly` schedule, backfill Jan–Jun 2024, retries, one run per month).

## Tech stack

Python (pandas, PyArrow, psycopg2) · Apache Airflow 3 · PostgreSQL 16 · dbt Core 1.11 · Docker Compose · pytest

## Run it

Requirements: Docker Desktop with about 6 GB of memory available.

```bash
cp .env.example .env          # then change the passwords
docker compose up -d --build  # first build takes a few minutes
```

- Airflow UI: http://localhost:8080 (no login in local dev mode). Turn on `nyc_taxi_pipeline`; it backfills six months.
- Warehouse: connect any SQL client (DBeaver, pgAdmin, Azure Data Studio) to `localhost:5433`, database `warehouse`, user `dw`.
- Process one specific month: Trigger DAG with config `{"month": "2024-07"}`.

Run the unit tests (no Docker needed):

```bash
pip install pandas pyarrow pytest
pytest tests/
```

## Data quality

| Layer | Check |
|---|---|
| Python (transform) | Pickup inside the run month, dropoff after pickup, distance 0–200 miles, no negative fare/total, pickup and dropoff zone present, duplicates removed. Each rejection is counted. |
| Python (gate) | Run fails if fewer than 80% of rows survive cleaning or the file is empty. |
| Load audit | Every load writes rows loaded, duration and cleaning stats (JSON) to `raw.load_audit`. |
| dbt tests | not_null, unique, relationships to the zone lookup, and a custom test that no day has negative revenue. |

## Example queries

```sql
-- Row counts and load times per month
select trip_month, rows_loaded, load_seconds, cleaning_stats->>'keep_ratio' as keep_ratio
from raw.load_audit order by loaded_at desc;

-- Top 10 pickup zones by revenue
select zone, borough, sum(total_revenue) as revenue
from analytics.fct_monthly_zone_revenue
group by 1, 2 order by revenue desc limit 10;
```

## Results

_Fill in after your first full run:_ rows processed, total pipeline time, rows rejected per rule.

## Data source

[NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (public).
