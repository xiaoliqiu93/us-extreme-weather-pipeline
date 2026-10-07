import calendar
from pathlib import Path

import dagster as dg
import pandas as pd

from us_extreme_weather_pipeline.ingestion.cities import CITIES
from us_extreme_weather_pipeline.ingestion.open_meteo import fetch_weather

year_partitions = dg.StaticPartitionsDefinition(
    [str(year) for year in range(2005, 2026)]
)


def get_weather_path(year: str, location_id: str) -> Path:
    """Return the raw Parquet path for one location and year."""
    return (
        Path("data/raw/open_meteo")
        / f"year={year}"
        / f"location={location_id}"
        / "weather.parquet"
    )


@dg.asset(partitions_def=year_partitions)
def raw_weather(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch hourly weather data for all configured locations for one year."""

    year = context.partition_key
    downloaded_locations = 0
    skipped_locations = 0

    for city in CITIES:
        location_id = city["location_id"]
        output_path = get_weather_path(year, location_id)

        if output_path.exists():
            context.log.info(
                f"Skipping {city['city']}, {city['state']} "
                f"({location_id}): raw file already exists"
            )
            skipped_locations += 1
            continue

        context.log.info(
            f"Fetching {year} weather for "
            f"{city['city']}, {city['state']} ({location_id})"
        )

        weather = fetch_weather(
            location_id=location_id,
            city=city["city"],
            state=city["state"],
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31",
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        weather.to_parquet(output_path, index=False)

        downloaded_locations += 1

    return dg.MaterializeResult(
        metadata={
            "year": year,
            "expected_locations": len(CITIES),
            "downloaded_locations": downloaded_locations,
            "skipped_locations": skipped_locations,
        }
    )


@dg.asset_check(asset=raw_weather, name="not_empty")
def raw_weather_not_empty(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    year = context.partition_key

    total_rows = 0
    files_checked = 0

    for city in CITIES:
        location_id = city["location_id"]
        path = get_weather_path(year, location_id)

        if path.exists():
            weather = pd.read_parquet(path)
            total_rows += len(weather)
            files_checked += 1

    return dg.AssetCheckResult(
        passed=total_rows > 0,
        metadata={
            "total_rows": total_rows,
            "files_checked": files_checked,
        },
    )


@dg.asset_check(asset=raw_weather, name="all_cities_present")
def raw_weather_all_cities_present(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    year = context.partition_key

    expected_location_ids = {
        city["location_id"]
        for city in CITIES
    }

    present_location_ids = {
        location_id
        for location_id in expected_location_ids
        if get_weather_path(year, location_id).exists()
    }

    missing_location_ids = expected_location_ids - present_location_ids

    return dg.AssetCheckResult(
        passed=len(missing_location_ids) == 0,
        metadata={
            "expected_location_count": len(expected_location_ids),
            "present_location_count": len(present_location_ids),
            "missing_location_ids": sorted(missing_location_ids),
        },
    )


@dg.asset_check(asset=raw_weather, name="hourly_completeness")
def raw_weather_hourly_completeness(
    context: dg.AssetCheckExecutionContext,
) -> dg.AssetCheckResult:
    year = int(context.partition_key)

    days_in_year = 366 if calendar.isleap(year) else 365
    expected_rows_per_location = days_in_year * 24

    incomplete_locations = {}

    for city in CITIES:
        location_id = city["location_id"]
        path = get_weather_path(str(year), location_id)

        if not path.exists():
            incomplete_locations[location_id] = "file_missing"
            continue

        weather = pd.read_parquet(path)
        row_count = len(weather)

        if row_count != expected_rows_per_location:
            incomplete_locations[location_id] = row_count

    return dg.AssetCheckResult(
        passed=len(incomplete_locations) == 0,
        metadata={
            "expected_rows_per_location": expected_rows_per_location,
            "incomplete_locations": incomplete_locations,
        },
    )