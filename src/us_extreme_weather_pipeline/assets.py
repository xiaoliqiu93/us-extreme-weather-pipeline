import calendar
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
            city=city["city"],
            state=city["state"],
            latitude=city["latitude"],
            longitude=city["longitude"],
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
    """Check that all configured city/state locations are present."""

    year = context.partition_key

    weather = pd.read_parquet(get_weather_path(year))

    expected_locations = {
        (city["city"], city["state"])
        for city in CITIES
    }

    actual_locations = set(
        weather[["city", "state"]].itertuples(index=False, name=None)
    )

    missing_locations = expected_locations - actual_locations

    return dg.AssetCheckResult(
        passed=len(missing_locations) == 0,
        metadata={
            "expected_location_count": len(expected_locations),
            "actual_location_count": len(actual_locations),
            "missing_locations": [
                f"{city}, {state}"
                for city, state in sorted(missing_locations)
            ],
        },
    )

@dg.asset_check(
    asset=raw_weather,
    name="hourly_completeness",
)
def raw_weather_hourly_completeness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    """Check that every city has the expected number of hourly records."""

    year = int(context.partition_key)

    weather = pd.read_parquet(get_weather_path(str(year)))

    days_in_year = 366 if calendar.isleap(year) else 365
    expected_rows_per_city = days_in_year * 24

    rows_per_location = weather.groupby(["city", "state"]).size()

    incomplete_locations = {
        f"{city}, {state}": int(row_count)
        for (city, state), row_count in rows_per_location.items()
        if row_count != expected_rows_per_city
    }

    return dg.AssetCheckResult(
        passed=len(incomplete_locations) == 0,
        metadata={
            "expected_rows_per_location": expected_rows_per_city,
            "incomplete_locations": incomplete_locations,
        },
    )