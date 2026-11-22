"""Transform step: clean one month of trips and enforce a data-quality gate."""
import logging
import os

import pandas as pd
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# Source column -> warehouse column
COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}

# Fail the run if fewer than this share of rows survive cleaning.
MIN_KEEP_RATIO = 0.80


class DataQualityError(Exception):
    pass


def clean_trips(df: pd.DataFrame, month: str) -> tuple[pd.DataFrame, dict]:
    """Apply cleaning rules. Returns (clean_df, stats). Pure function, easy to unit test."""
    df = df.rename(columns=COLUMNS)[list(COLUMNS.values())]
    stats = {"month": month, "rows_in": len(df)}

    start = pd.Timestamp(f"{month}-01")
    end = start + pd.offsets.MonthBegin(1)

    rules = {
        "pickup_outside_month": ~df["pickup_at"].between(start, end, inclusive="left"),
        "dropoff_before_pickup": df["dropoff_at"] < df["pickup_at"],
        "bad_distance": ~df["trip_distance"].between(0, 200),
        "negative_amount": (df["fare_amount"] < 0) | (df["total_amount"] < 0),
        "missing_location": df["pickup_location_id"].isna() | df["dropoff_location_id"].isna(),
    }
    bad = pd.Series(False, index=df.index)
    for name, mask in rules.items():
        stats[f"rejected_{name}"] = int((mask & ~bad).sum())  # count each row once
        bad |= mask
    df = df[~bad]

    before = len(df)
    df = df.drop_duplicates()
    stats["rejected_duplicates"] = before - len(df)

    df = df.astype({"vendor_id": "Int64", "passenger_count": "Int64", "payment_type": "Int64",
                    "pickup_location_id": "Int64", "dropoff_location_id": "Int64"})
    df.insert(0, "trip_month", month)

    stats["rows_out"] = len(df)
    stats["keep_ratio"] = round(stats["rows_out"] / max(stats["rows_in"], 1), 4)
    return df, stats


def transform_month(raw_path: str, month: str, data_dir: str) -> dict:
    df = pq.read_table(raw_path, columns=list(COLUMNS)).to_pandas()
    clean, stats = clean_trips(df, month)
    log.info("Cleaning stats: %s", stats)

    if stats["rows_in"] == 0:
        raise DataQualityError(f"{month}: source file is empty")
    if stats["keep_ratio"] < MIN_KEEP_RATIO:
        raise DataQualityError(f"{month}: only {stats['keep_ratio']:.1%} of rows passed cleaning")

    out = os.path.join(data_dir, "clean", f"yellow_trips_{month}.parquet")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    clean.to_parquet(out, index=False)
    stats["clean_path"] = out
    return stats
