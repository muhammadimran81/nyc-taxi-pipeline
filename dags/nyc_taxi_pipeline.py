"""
NYC Taxi monthly ELT pipeline.

extract (download) -> transform (clean + quality gate) -> load (COPY into Postgres)
    -> dbt build (staging + marts + tests)

One DAG run = one month of data. Runs are idempotent: re-running a month replaces it.
Trigger manually with {"month": "2024-03"} to process a specific month.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data")
WAREHOUSE_URI = os.environ["WAREHOUSE_URI"]


def _month(context) -> str:
    override = context["params"].get("month")
    if override:
        return override
    return context["logical_date"].strftime("%Y-%m")


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),    
    catchup=False,          # backfills Jan-Jun 2024 on first start
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={"month": Param("", type="string", description="Optional YYYY-MM override")},
    tags=["portfolio", "elt", "nyc-taxi"],
)
def nyc_taxi_pipeline():

    @task
    def extract(**context) -> dict:
        from pipeline.extract import download_month, download_zones
        month = _month(context)
        return {
            "month": month,
            "raw_path": download_month(month, DATA_DIR),
            "zones_path": download_zones(DATA_DIR),
        }

    @task
    def transform(extracted: dict) -> dict:
        from pipeline.transform import transform_month
        stats = transform_month(extracted["raw_path"], extracted["month"], DATA_DIR)
        return {**extracted, **stats}

    @task
    def load(transformed: dict) -> int:
        from pipeline.load import load_month, load_zones
        load_zones(transformed["zones_path"], WAREHOUSE_URI)
        stats = {k: v for k, v in transformed.items() if k.startswith(("rows_", "rejected_", "keep_"))}
        return load_month(transformed["clean_path"], transformed["month"], WAREHOUSE_URI, stats)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt/nyc_taxi && "
            "/opt/dbt_venv/bin/dbt build --profiles-dir . "
            "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
        ),
    )

    load(transform(extract())) >> dbt_build


nyc_taxi_pipeline()
