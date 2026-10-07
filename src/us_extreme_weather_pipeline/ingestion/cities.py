from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CITIES_PATH = PROJECT_ROOT / "config" / "cities.csv"

EXPECTED_CITY_COUNT = 50
REQUIRED_COLUMNS = {
    "city",
    "state",
    "latitude",
    "longitude",
    "climate_group",
}


def load_cities() -> list[dict]:
    """Load and validate city configuration."""

    cities = pd.read_csv(CITIES_PATH)

    missing_columns = REQUIRED_COLUMNS - set(cities.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns in cities.csv: {sorted(missing_columns)}"
        )

    if len(cities) != EXPECTED_CITY_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_CITY_COUNT} cities, found {len(cities)}"
        )

    if cities[list(REQUIRED_COLUMNS)].isnull().any().any():
        raise ValueError("cities.csv contains missing values")

    if cities.duplicated(subset=["city", "state"]).any():
        raise ValueError("cities.csv contains duplicate city/state pairs")

    if not cities["latitude"].between(-90, 90).all():
        raise ValueError("cities.csv contains invalid latitude values")

    if not cities["longitude"].between(-180, 180).all():
        raise ValueError("cities.csv contains invalid longitude values")

    return cities.to_dict(orient="records")


CITIES = load_cities()