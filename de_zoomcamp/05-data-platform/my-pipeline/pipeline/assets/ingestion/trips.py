"""@bruin

# Python ingestion asset: fetch NYC TLC taxi trip data from public parquet endpoint.
# Source: https://d37ci6vzurychx.cloudfront.net/trip-data/
# Naming: <taxi_type>_tripdata_<year>-<month>.parquet (e.g. yellow_tripdata_2022-03.parquet)

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

# Key columns used by staging (time_interval on pickup_datetime). Raw parquet columns are preserved.
columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start time (normalized from tpep_ or lpep_ column)
  - name: dropoff_datetime
    type: timestamp
    description: Trip end time (normalized from tpep_ or lpep_ column)
  - name: taxi_type
    type: string
    description: Source taxi type (yellow, green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when this batch was extracted (lineage/debugging)

@bruin"""

import os
import json
from datetime import datetime

import pandas as pd


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD to datetime at start of day."""
    return datetime.strptime(s.strip()[:10], "%Y-%m-%d")


def _taxi_type_datetime_columns(taxi_type: str) -> tuple[str, str]:
    """Return (pickup_col, dropoff_col) for the given taxi type."""
    if taxi_type == "yellow":
        return "tpep_pickup_datetime", "tpep_dropoff_datetime"
    return "lpep_pickup_datetime", "lpep_dropoff_datetime"


def materialize():
    """
    Fetch NYC TLC trip parquet files for the run's date range and taxi types.
    Uses BRUIN_START_DATE, BRUIN_END_DATE and taxi_types from BRUIN_VARS.
    Normalizes pickup/dropoff datetime column names and adds taxi_type + extracted_at.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    start_s = os.environ.get("BRUIN_START_DATE", "")
    end_s = os.environ.get("BRUIN_END_DATE", "")
    vars_s = os.environ.get("BRUIN_VARS", "{}")

    start_date = _parse_date(start_s)
    end_date = _parse_date(end_s)
    try:
        vars_data = json.loads(vars_s) if vars_s else {}
    except json.JSONDecodeError:
        vars_data = {}
    taxi_types = vars_data.get("taxi_types", ["yellow"])

    if not isinstance(taxi_types, list):
        taxi_types = [taxi_types]

    extracted_at = pd.Timestamp.utcnow()
    frames = []

    current = datetime(start_date.year, start_date.month, 1)
    end_month = datetime(end_date.year, end_date.month, 1)

    while current <= end_month:
        year = current.year
        month = current.month
        for taxi_type in taxi_types:
            taxi_type = str(taxi_type).strip().lower()
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{base_url}/{filename}"
            try:
                df = pd.read_parquet(url)
            except Exception as e:
                # Skip missing or invalid files (e.g. future months, bad response)
                raise RuntimeError(f"Failed to fetch {url}: {e}") from e

            if df.empty:
                continue

            pickup_col, dropoff_col = _taxi_type_datetime_columns(taxi_type)
            if pickup_col in df.columns:
                df = df.rename(columns={pickup_col: "pickup_datetime", dropoff_col: "dropoff_datetime"})
            else:
                # Already normalized or single type
                if "pickup_datetime" not in df.columns and "tpep_pickup_datetime" in df.columns:
                    df = df.rename(columns={"tpep_pickup_datetime": "pickup_datetime", "tpep_dropoff_datetime": "dropoff_datetime"})
                elif "pickup_datetime" not in df.columns and "lpep_pickup_datetime" in df.columns:
                    df = df.rename(columns={"lpep_pickup_datetime": "pickup_datetime", "lpep_dropoff_datetime": "dropoff_datetime"})

            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)

        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)

    if not frames:
        return pd.DataFrame(
            {
                "pickup_datetime": pd.Series(dtype="datetime64[ns]"),
                "dropoff_datetime": pd.Series(dtype="datetime64[ns]"),
                "taxi_type": pd.Series(dtype="string"),
                "extracted_at": pd.Series(dtype="datetime64[ns]"),
            }
        )

    return pd.concat(frames, ignore_index=True)
