from datetime import UTC, datetime

import pandas as pd
import requests

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "wind_gusts_10m",
]


def fetch_weather(
    location_id: str,
    city: str,
    state: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARIABLES,
        "timezone": "auto",
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame(data["hourly"])

    df["time"] = pd.to_datetime(df["time"])
    
    df["location_id"] = location_id
    df["city"] = city
    df["state"] = state

    df["requested_latitude"] = latitude
    df["requested_longitude"] = longitude
    df["source_latitude"] = data["latitude"]
    df["source_longitude"] = data["longitude"]
    df["elevation"] = data["elevation"]
    df["timezone"] = data["timezone"]

    df["source"] = "open_meteo"
    df["ingested_at"] = datetime.now(UTC)

    return df