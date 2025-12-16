import os
import logging
import pandas as pd
from sqlalchemy import create_engine
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
    Loads flattened weather data from a pickle file into PostgreSQL.

    Args:
        data_path: Path to the pickle file produced by the transform task.

    Returns:
        Number of rows loaded, or None if loading failed.
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

    # --- 2. PostgreSQL connection ---
    conn_string: Optional[str] = os.getenv("POSTGRES_STRING")
    if not conn_string:
        logging.error("POSTGRES_STRING environment variable not set.")
        return None

    engine: Engine = create_engine(conn_string)

    # --- 3. Load into PostgreSQL ---
    try:
        with engine.begin() as connection:
            logging.info("PostgreSQL connection established.")

            df.to_sql(
                name="weather_readings",
                con=connection,
                if_exists="append",
                index=False
            )

        logging.info(f"Successfully loaded {len(df)} rows into weather_readings.")
        return len(df)

    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
        return None
