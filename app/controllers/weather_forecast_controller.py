"""Controller for weather forecast data fetching."""

import logging
from datetime import date, timedelta
from typing import Any, List, Literal, cast

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session

from app.models import WeatherForecast

logger = logging.getLogger(__name__)

# Konfiguracja API Open-Meteo
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=cast(Any, retry_session))

# ============================================================================
# CONSTANTS - Shared across weather forecast components
# ============================================================================

# Współrzędne (Warszawa)
WARSAW_LATITUDE = 52.2297
WARSAW_LONGITUDE = 21.0122
TIMEZONE = "Europe/Warsaw"
MINUTELY_VARIABLES_15 = [
    "temperature_2m",
    "rain",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "shortwave_radiation_instant",
    "direct_radiation_instant",
    "diffuse_radiation_instant",
    "direct_normal_irradiance_instant",
    "terrestrial_radiation_instant",
    "global_tilted_irradiance_instant",
]

# ============================================================================
# UTILITY FUNCTIONS - Shared across weather forecast components
# ============================================================================


def build_client() -> openmeteo_requests.Client:
    """Create Open-Meteo client with local cache and retry policy."""
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=cast(Any, retry_session))


def parse_minutely_response(response, columns: list[str], prefix: str) -> pd.DataFrame:
    """Convert Open-Meteo daily response to DataFrame with prefixed columns."""
    minutely = response.Minutely15()
    data = {
        "datetime": pd.date_range(
            start=pd.to_datetime(
                minutely.Time() + response.UtcOffsetSeconds(), unit="s", utc=True
            ),
            end=pd.to_datetime(
                minutely.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=True
            ),
            freq=pd.Timedelta(seconds=minutely.Interval()),
            inclusive="left",
        ).tz_convert(TIMEZONE),
    }

    for idx, column in enumerate(columns):
        data[f"{prefix}_{column}"] = minutely.Variables(idx).ValuesAsNumpy()

    return pd.DataFrame(data)


def get_last_year_date_range() -> tuple[str, str]:
    """Return YYYY-MM-DD range for the last 365 full days (without today)."""
    return get_date_range(365)


def get_date_range(end: int) -> tuple[str, str]:
    """Return YYYY-MM-DD range for the last `end` days (without today)."""
    _end = date.today() - timedelta(days=1)
    start = _end - timedelta(days=end)
    return start.isoformat(), _end.isoformat()


def fetch_forecast(
    engine,
    num_days: int,
    interval: Literal["15min", "daily"] = "15min",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    if interval != "15min":
        raise ValueError("Currently only '15min' interval is supported.")

    if start_date is None or end_date is None:
        start_date, end_date = get_date_range(num_days)

    client = build_client()

    forecast_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"

    params = {
        "latitude": WARSAW_LATITUDE,
        "longitude": WARSAW_LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": TIMEZONE,
        "minutely_15": MINUTELY_VARIABLES_15,
    }

    forecast_response = client.weather_api(forecast_url, params=params)[0]

    df = parse_minutely_response(forecast_response, MINUTELY_VARIABLES_15, "forecast")

    models = df_to_models(df)
    save_to_db(engine, models)

    logger.info("Zapisano %d rekordów do bazy", len(models))

    return df


def df_to_models(df: pd.DataFrame) -> list[WeatherForecast]:
    """Convert DataFrame to list of PogodaPrognoza models."""
    models: list[WeatherForecast] = []
    for _, row in df.iterrows():
        forecast_dt = row["datetime"]
        if hasattr(forecast_dt, "to_pydatetime"):
            forecast_dt = forecast_dt.to_pydatetime()

        model = WeatherForecast(
            latitude=WARSAW_LATITUDE,
            longitude=WARSAW_LONGITUDE,
            forecast_datetime=forecast_dt,
            temperature_2m=row.get("forecast_temperature_2m"),
            rain=row.get("forecast_rain"),
            relative_humidity_2m=row.get("forecast_relative_humidity_2m"),
            precipitation=row.get("forecast_precipitation"),
            wind_speed_10m=row.get("forecast_wind_speed_10m"),
            wind_direction_10m=row.get("forecast_wind_direction_10m"),
            ghi=row.get("forecast_shortwave_radiation_instant"),
            dni=row.get("forecast_direct_normal_irradiance_instant"),
            dri=row.get("forecast_diffuse_radiation_instant"),
            gti=row.get("forecast_global_tilted_irradiance_instant"),
            terrestrial_radiation=row.get("forecast_terrestrial_radiation_instant"),
            datasource="openmeteo",
        )
        models.append(model)
    return models


def save_to_db(engine, records: List[WeatherForecast]):
    """Zapisuje prognozy do bazy (używając ON CONFLICT DO UPDATE)."""
    if not records:
        logger.warning("Brak rekordów do zapisu.")
        return

    logger.info("Zapisuję %d rekordów do bazy...", len(records))
    try:
        with Session(engine) as session:
            # Convert models to dicts for bulk insert
            records_dicts = [
                record.dict(exclude={"id", "created_at"}) for record in records
            ]

            stmt = insert(WeatherForecast).values(records_dicts)

            # Columns to update if the record already exists
            update_columns = [
                "temperature_2m",
                "rain",
                "relative_humidity_2m",
                "precipitation",
                "wind_speed_10m",
                "wind_direction_10m",
                "ghi",
                "dni",
                "dri",
                "gti",
                "terrestrial_radiation",
                "datasource",
            ]

            set_dict = {col: getattr(stmt.excluded, col) for col in update_columns}

            # Upsert relying on unique constraint "uq_weather_forecast_coords_time"
            stmt = stmt.on_conflict_do_update(
                index_elements=["latitude", "longitude", "forecast_datetime"],
                set_=set_dict,
            )

            session.exec(stmt)
            session.commit()
            logger.info("Sukces. Zapisano/zaktualizowano %d rekordów.", len(records))

    except Exception:
        logger.exception("Błąd zapisu do bazy danych.")
