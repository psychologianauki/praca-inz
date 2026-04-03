import logging
from datetime import date, timedelta
from typing import Any, cast

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from sqlmodel import Session, create_engine, select

from app.core.config import settings
from app.models import WeatherForecast

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Warszawa
LATITUDE = 52.237049
LONGITUDE = 21.017532
TIMEZONE = "Europe/Warsaw"

# Dzienne zmienne do porownania prognozy vs rzeczywiste
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
    """Return YYYY-MM-DD range starting from 2025-03-03 until yesterday."""
    end = date.today() - timedelta(days=1)
    start = date(2025, 3, 3)
    return start.isoformat(), end.isoformat()


def evaluate_daily_forecast_last_year() -> pd.DataFrame:
    """Fetch and evaluate daily weather forecasts against historical observations."""
    start_date, end_date = get_last_year_date_range()
    logger.info("%s, %s", start_date, end_date)
    client = build_client()

    forecast_url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    # archive_url = "https://archive-api.open-meteo.com/v1/archive"

    common_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": TIMEZONE,
        "minutely_15": MINUTELY_VARIABLES_15,
    }

    logger.info("Zakres analizy: %s -> %s", start_date, end_date)

    forecast_response = client.weather_api(forecast_url, params=common_params)[0]
    # observed_response = client.weather_api(archive_url, params=common_params)[0]
    print(forecast_response)

    df = parse_minutely_response(forecast_response, MINUTELY_VARIABLES_15, "forecast")
    # forecast_df = parse_minutely_response(
    #     forecast_response, MINUTELY_VARIABLES_15, "forecast"
    # )
    # observed_df = parse_minutely_response(
    #     observed_response, MINUTELY_VARIABLES_15, "actual"
    # )

    # df = forecast_df.merge(observed_df, on="date", how="inner")

    # for variable in MINUTELY_VARIABLES_15:
    #     fc_col = f"forecast_{variable}"
    #     ac_col = f"actual_{variable}"
    #     error_col = f"error_{variable}"
    #     abs_error_col = f"abs_error_{variable}"

    #     df[error_col] = df[fc_col] - df[ac_col]
    #     df[abs_error_col] = df[error_col].abs()

    # metrics = {}
    # for variable in MINUTELY_VARIABLES_15:
    #     abs_error_col = f"abs_error_{variable}"
    #     metrics[variable] = df[abs_error_col].mean()

    # logger.info("Sredni blad bezwzgledny (MAE) dla ostatniego roku:")
    # for variable, mae in metrics.items():
    #     logger.info("- %s: %.3f", variable, mae)

    output_file = "data/minutely_weather_forecast_check_last_year.csv"
    df.to_csv(output_file, index=False)
    logger.info("Zapisano szczegoly do pliku: %s", output_file)
    logger.info("Liczba porownanych dni: %d", len(df))
    logger.info(df)
    return df


def save_csv_to_db(csv_path: str):
    """Odczytuje plik CSV i zapisuje dane do bazy danych."""
    logger.info("Odczytywanie danych z pliku: %s", csv_path)
    df = pd.read_csv(csv_path)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))

    with Session(engine) as session:
        logger.info("Pobieranie istniejących rekordów...")
        import datetime

        # Pobieramy daty i normalizujemy do UTC dla pewności przy porównaniu
        db_times = session.exec(
            select(WeatherForecast.forecast_datetime)
            .where(WeatherForecast.latitude == LATITUDE)
            .where(WeatherForecast.longitude == LONGITUDE)
        ).all()

        existing_times = set()
        for ts in db_times:
            if ts is not None:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                existing_times.add(ts)

        models = []
        seen_in_batch = set()

        logger.info("Konwersja na modele...")
        for _, row in df.iterrows():
            dt = row["datetime"]
            if hasattr(dt, "to_pydatetime"):
                dt = dt.to_pydatetime()

            # Upewniamy się, że dt jest UTC-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)

            if dt in existing_times or dt in seen_in_batch:
                continue

            logger.info(dt)
            print(row)
            model = WeatherForecast(
                latitude=LATITUDE,
                longitude=LONGITUDE,
                forecast_datetime=dt,
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
            seen_in_batch.add(dt)

        if models:
            logger.info("Zapisywanie %d nowych rekordów...", len(models))
            logger.info(models[0])
            try:
                session.add_all(models)
                session.commit()
                logger.info("Gotowe.")
            except Exception as e:
                session.rollback()
                logger.error("Błąd zapisu do bazy: %s", e)
                raise
        else:
            logger.info("Brak nowych rekordów do dodania.")


if __name__ == "__main__":
    # 1. Pobierz dane (jeśli potrzebujesz odświeżyć CSV)
    evaluate_daily_forecast_last_year()

    # 2. Zapisz z CSV do bazy
    save_csv_to_db("data/minutely_weather_forecast_check_last_year.csv")
