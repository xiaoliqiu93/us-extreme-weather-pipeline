import dagster as dg

from us_extreme_weather_pipeline.assets import (
    raw_weather,
    raw_weather_all_cities_present,
    raw_weather_not_empty,
)

defs = dg.Definitions(
    assets=[raw_weather],
    asset_checks=[
        raw_weather_not_empty,
        raw_weather_all_cities_present,
    ],
)