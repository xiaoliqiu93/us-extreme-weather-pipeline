import re
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


def make_location_id(city: str, state: str) -> str:
    """Create a deterministic location ID from city and state."""
    city_slug = re.sub(r"[^a-z0-9]+", "_", city.strip().lower()).strip("_")
    state_slug = state.strip().lower()

    return f"{city_slug}_{state_slug}"


def load_cities() -> list[dict]:
    """Load, validate, and enrich the city configuration."""

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

    cities["location_id"] = cities.apply(
        lambda row: make_location_id(row["city"], row["state"]),
        axis=1,
    )

    if cities["location_id"].duplicated().any():
        raise ValueError("Generated location_id values are not unique")

    return cities.to_dict(orient="records")


CITIES = load_cities()