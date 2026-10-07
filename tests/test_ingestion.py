from us_extreme_weather_pipeline.ingestion.open_meteo import fetch_weather


def test_fetch_weather():
    df = fetch_weather(
        location_id="lubbock_tx",
        city="Lubbock",
        state="TX",
        latitude=33.5779,
        longitude=-101.8552,
        start_date="2025-01-01",
        end_date="2025-01-02",
    )

    assert not df.empty
    assert "temperature_2m" in df.columns
    assert "location_id" in df.columns
    assert "city" in df.columns

    assert df["location_id"].eq("lubbock_tx").all()
    assert df["city"].eq("Lubbock").all()