import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def load(data_path: str) -> Optional[int]:
    """
    Idempotent load of flattened weather data into PostgreSQL.
    Deduplicates by (city, observed_at, provider).
    """

    # --- 1. Load pickle ---
    if not isinstance(data_path, str):
        logging.error("Invalid data_path provided; expected a string.")
        return None

    try:
        df = pd.read_pickle(data_path)
        logging.info(f"Loaded {len(df)} records from {data_path}")
    except Exception as e:
        logging.error(f"Failed to read pickle file: {e}")
        return None

    if df.empty:
        logging.warning("No data to load (empty DataFrame).")
        return 0

    # --- 2. Enforce correct dtypes ---
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df = df.dropna(subset=["observed_at"])

    numeric_cols = [
        "lat", "lon", "temp_c", "humidity_pct",
        "wind_speed_ms", "rain_1h_mm", "snow_1h_mm"
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # --- 3. Deduplicate in-memory ---
    before = len(df)
    df = df.drop_duplicates(subset=["city", "observed_at", "provider"])
    after = len(df)
    logging.info(f"Deduplicated DataFrame: {before} → {after}")

    # --- 4. PostgreSQL connection ---
    conn_string: Optional[str] = os.getenv("POSTGRES_STRING")
    if not conn_string:
        logging.error("POSTGRES_STRING environment variable not set.")
        return None

    engine: Engine = create_engine(conn_string)

    # --- 5. Idempotent bulk UPSERT ---
    insert_sql = text("""
        INSERT INTO weather_readings (
            city,
            country,
            lat,
            lon,
            observed_at,
            temp_c,
            humidity_pct,
            wind_speed_ms,
            rain_1h_mm,
            snow_1h_mm,
            condition,
            description,
            provider
        )
        VALUES (
            :city,
            :country,
            :lat,
            :lon,
            :observed_at,
            :temp_c,
            :humidity_pct,
            :wind_speed_ms,
            :rain_1h_mm,
            :snow_1h_mm,
            :condition,
            :description,
            :provider
        )
        ON CONFLICT (city, observed_at, provider)
        DO NOTHING;
    """)

    records = df.to_dict(orient="records")

    try:
        with engine.begin() as connection:
            result = connection.execute(insert_sql, records)
            rows_inserted = result.rowcount or 0

        logging.info(
            f"Load complete. Inserted {rows_inserted} new rows "
            f"(duplicates ignored: {len(records) - rows_inserted})."
        )

        return rows_inserted

    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
        return None
