import os
import logging
import pandas as pd
from pathlib import Path
from pymongo import MongoClient
from datetime import timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def transform() -> str:
    # 1. Config
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    coll_name = os.getenv("MONGO_COLLECTION")
    output_path = "/tmp/weather_pipeline/weather_flattened.pkl"

    if not all([uri, db_name, coll_name]):
        raise ValueError("Missing MongoDB environment variables")

    # 2. Extract from MongoDB (✅ FIXED)
    with MongoClient(uri) as client:
        raw_docs = list(client[db_name][coll_name].find({}))

    if not raw_docs:
        logging.warning("MongoDB collection is empty. No data to transform.")
        return ""

    logging.info(f"Fetched {len(raw_docs)} raw documents")

    # 3. Transform
    processed_rows = []

    for doc in raw_docs:
        main = doc.get("main", {})
        wind = doc.get("wind", {})
        weather_list = doc.get("weather", [])
        weather = weather_list[0] if weather_list else {}

        processed_rows.append({
            "city": doc.get("name"),
            "country": doc.get("sys", {}).get("country"),
            "lat": doc.get("coord", {}).get("lat"),
            "lon": doc.get("coord", {}).get("lon"),
            "observed_at": pd.to_datetime(
                doc.get("dt"), unit="s", utc=True
            ) if doc.get("dt") else None,
            "temp_c": main.get("temp"),
            "humidity_pct": main.get("humidity"),
            "wind_speed_ms": wind.get("speed"),
            "rain_1h_mm": doc.get("rain", {}).get("1h", 0.0),
            "snow_1h_mm": doc.get("snow", {}).get("1h", 0.0),
            "condition": weather.get("main"),
            "description": weather.get("description"),
            "provider": "openweather"
        })

    # 4. Save to pickle
    df = pd.DataFrame(processed_rows)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(output_path)

    logging.info(f"Saved {len(df)} rows to {output_path}")

    return output_path


if __name__ == "__main__":
    transform()
