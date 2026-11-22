"""Unit tests for the cleaning rules. Run: pytest tests/"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from pipeline.transform import clean_trips  # noqa: E402


def make_df(rows):
    base = {"VendorID": 1, "tpep_pickup_datetime": "2024-01-10 08:00", "tpep_dropoff_datetime": "2024-01-10 08:20",
            "passenger_count": 1.0, "trip_distance": 3.2, "PULocationID": 132, "DOLocationID": 236,
            "payment_type": 1, "fare_amount": 20.0, "tip_amount": 4.0, "total_amount": 28.5}
    df = pd.DataFrame([{**base, **r} for r in rows])
    for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[c] = pd.to_datetime(df[c])
    return df


def test_good_row_is_kept():
    clean, stats = clean_trips(make_df([{}]), "2024-01")
    assert stats["rows_out"] == 1
    assert clean.iloc[0]["trip_month"] == "2024-01"


def test_bad_rows_are_rejected_and_counted():
    df = make_df([
        {},                                                   # good
        {"tpep_pickup_datetime": "2023-12-31 23:00",          # wrong month
         "tpep_dropoff_datetime": "2023-12-31 23:10"},
        {"tpep_dropoff_datetime": "2024-01-10 07:00"},        # dropoff before pickup
        {"trip_distance": -1},                                # bad distance
        {"total_amount": -5},                                 # negative amount
        {"PULocationID": None},                               # missing location
        {},                                                   # duplicate of row 0
    ])
    clean, stats = clean_trips(df, "2024-01")
    assert stats["rows_in"] == 7
    assert stats["rows_out"] == 1
    assert stats["rejected_pickup_outside_month"] == 1
    assert stats["rejected_dropoff_before_pickup"] == 1
    assert stats["rejected_bad_distance"] == 1
    assert stats["rejected_negative_amount"] == 1
    assert stats["rejected_missing_location"] == 1
    assert stats["rejected_duplicates"] == 1


def test_null_passenger_count_is_allowed():
    clean, stats = clean_trips(make_df([{"passenger_count": None}]), "2024-01")
    assert stats["rows_out"] == 1
    assert pd.isna(clean.iloc[0]["passenger_count"])
