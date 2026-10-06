from pathlib import Path

import dagster as dg
import pandas as pd

from us_extreme_weather_pipeline.ingestion.cities import CITIES
from us_extreme_weather_pipeline.ingestion.open_meteo import fetch_weather

year_partitions = dg.StaticPartitionsDefinition(
    [str(year) for year in range(2005, 2026)]
)


def get_weather_path(year: str) -> Path:
    """Return the local path for a yearly raw weather Parquet file."""
    return Path("data/raw/open_meteo") / f"year={year}" / "weather.parquet"


@dg.asset(partitions_def=year_partitions)
def raw_weather(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch hourly weather data for all configured cities for one year."""

    year = context.partition_key

    frames = []

    for city in CITIES:
        context.log.info(
            f"Fetching {year} weather for {city['city']}, {city['state']}"
        )

        df = fetch_weather(
            **city,
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31",
        )

        frames.append(df)

    weather = pd.concat(frames, ignore_index=True)

    output_path = get_weather_path(year)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    weather.to_parquet(output_path, index=False)

    return dg.MaterializeResult(
        metadata={
            "year": year,
            "rows": len(weather),
            "cities": len(CITIES),
            "output_path": str(output_path),
        }
    )


@dg.asset_check(
    asset=raw_weather,
    name="not_empty",
)
def raw_weather_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Check that the raw weather Parquet file contains records."""

    year = context.partition_key

    weather = pd.read_parquet(get_weather_path(year))

    row_count = len(weather)

    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={
            "row_count": row_count,
        },
    )

@dg.asset_check(
    asset=raw_weather,
    name="all_cities_present",
)
def raw_weather_all_cities_present(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Check that all configured cities are present in the raw weather data."""

    year = context.partition_key

    weather = pd.read_parquet(get_weather_path(year))

    expected_cities = {city["city"] for city in CITIES}
    actual_cities = set(weather["city"].unique())

    missing_cities = expected_cities - actual_cities

    return dg.AssetCheckResult(
        passed=len(missing_cities) == 0,
        metadata={
            "expected_city_count": len(expected_cities),
            "actual_city_count": len(actual_cities),
            "missing_cities": sorted(missing_cities),
        },
    )