from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any, cast

import requests
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, create_engine, select

from app.controllers.weather_forecast_controller import (
    TIMEZONE,
    WARSAW_LATITUDE,
    WARSAW_LONGITUDE,
    _to_float,
    _to_int,
)
from app.core.config import settings
from app.models import WeatherForecast

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI), pool_pre_ping=True)


def get_session():
    with Session(engine) as session:
        yield session


router = APIRouter(tags=["data"])
WF = cast(Any, WeatherForecast).__table__.c

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def _open_meteo_hourly_fields() -> list[str]:
    return [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "precipitation_probability",
        "precipitation",
        "surface_pressure",
        "cloud_cover",
    ]


def fetch_open_meteo_forecast_warsaw(*, days: int) -> list[dict[str, Any]]:
    if days < 1 or days > 16:
        # Open-Meteo supports up to 16 forecast days on the free endpoint
        raise ValueError("days must be between 1 and 16")

    params: dict[str, Any] = {
        "latitude": WARSAW_LATITUDE,
        "longitude": WARSAW_LONGITUDE,
        "forecast_days": days,
        "hourly": ",".join(_open_meteo_hourly_fields()),
        "timezone": TIMEZONE,
    }

    try:
        resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=30)
    except Exception as e:
        raise RuntimeError(f"Open-Meteo request failed: {e}") from e

    if resp.status_code != 200:
        raise RuntimeError(f"Open-Meteo error {resp.status_code}: {resp.text[:500]}")

    payload = resp.json()
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise RuntimeError("Unexpected Open-Meteo response: missing 'hourly'")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise RuntimeError("Unexpected Open-Meteo response: missing 'hourly.time'")

    download_time = datetime.now()

    records: list[dict[str, Any]] = []
    for i, t in enumerate(times):
        # With timezone=Europe/Warsaw, Open-Meteo returns local time like '2026-01-12T13:00'
        ts = datetime.fromisoformat(t)

        def get_value(key: str):
            arr = hourly.get(key)
            if not isinstance(arr, list) or i >= len(arr):
                return None
            return arr[i]

        temp = get_value("temperature_2m")
        hum = get_value("relative_humidity_2m")
        wind_speed = get_value("wind_speed_10m")

        if temp is None or hum is None or wind_speed is None:
            # Skip incomplete rows
            continue

        records.append(
            {
                "latitude": WARSAW_LATITUDE,
                "longitude": WARSAW_LONGITUDE,
                "forecast_datetime": ts,
                "temperature_2m": _to_float(temp),
                "rain": _to_float(get_value("precipitation_probability")),
                "relative_humidity_2m": _to_float(hum),
                "precipitation": _to_float(get_value("precipitation")),
                "wind_speed_10m": _to_float(wind_speed),
                "wind_direction_10m": _to_float(
                    _to_int(get_value("wind_direction_10m"))
                ),
                "ghi": _to_float(get_value("shortwave_radiation")),
                "dni": None,
                "dri": None,
                "gti": None,
                "terrestrial_radiation": None,
                "datasource": "openmeteo:warsaw",
                "created_at": download_time,
            }
        )

    return records


def save_forecast(session: Session, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0

    statement = insert(WeatherForecast).values(records)
    statement = statement.on_conflict_do_nothing(
        index_elements=["latitude", "longitude", "forecast_datetime"]
    )
    session.exec(statement)
    session.commit()
    return len(records)


@router.get("/fetch-weather-forecast")
def fetch_weather_forecast(
    session: Session = Depends(get_session),
    days: int = 2,
    save: bool = True,
    tomorrow_only: bool = True,
):
    """Fetch *forecast/prediction* for Warsaw (Open-Meteo).

    - `days`: 1..16 (Open-Meteo forecast horizon)
    - `save`: if true, persists into table `weather_forecast`
    - `tomorrow_only`: if true, filters data to include only tomorrow's forecast
    """

    try:
        records = fetch_open_meteo_forecast_warsaw(days=days)

        # Filtruj dane tylko na jutro jeśli tomorrow_only=True
        if tomorrow_only:
            from datetime import date, timedelta

            tomorrow_date = date.today() + timedelta(days=1)
            records = [
                record
                for record in records
                if record["forecast_datetime"].date() == tomorrow_date
            ]
            print(
                f"[WeatherForecast] Przefiltrowano na jutro ({tomorrow_date}): {len(records)} rekordów"
            )

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    saved = 0
    if save:
        try:
            saved = save_forecast(session, records)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"DB save failed: {e}")

    return {
        "city": "Warsaw",
        "country": "PL",
        "source": "Open-Meteo",
        "days": days,
        "rows_fetched": len(records),
        "rows_saved": saved,
        "data": records,
    }


@router.get("/weather-forecast/latest")
def get_latest_weather_forecast(session: Session = Depends(get_session)):
    """Return the latest saved forecast batch for Warsaw."""

    latest_download = session.exec(
        select(func.max(WF.created_at)).where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
        )
    ).one()

    if not latest_download:
        return {"rows": 0, "data": []}

    rows = session.exec(
        select(WeatherForecast)
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == latest_download,
        )
        .order_by(WF.forecast_datetime)
    ).all()

    return {
        "rows": len(rows),
        "download_time": latest_download,
        "data": rows,
    }


@router.get("/weather-forecast/batches")
def list_weather_forecast_batches(
    session: Session = Depends(get_session),
    station: str = "Warszawa",
    limit: int = 30,
):
    """List historical forecast batches saved in DB.

    Each batch corresponds to one `data_pobrania_prognozy` (when the forecast was downloaded).
    """

    if limit < 1 or limit > 200:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 200")

    rows = session.exec(
        select(
            WF.created_at,
            func.count().label("rows"),
        )
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
        )
        .group_by(WF.created_at)
        .order_by(WF.created_at.desc())
        .limit(limit)
    ).all()

    return {
        "station": station,
        "limit": limit,
        "batches": [
            {"download_time": download_time, "rows": int(count)}
            for (download_time, count) in rows
        ],
    }


@router.get("/weather-forecast/by-download-time")
def get_weather_forecast_by_download_time(
    session: Session = Depends(get_session),
    download_time: datetime | str = "",
    station: str = "Warszawa",
):
    """Fetch a specific historical forecast batch by `download_time`.

    `download_time` should match `data_pobrania_prognozy` stored in DB (ISO string works).
    """

    if not download_time:
        raise HTTPException(status_code=422, detail="download_time is required")

    try:
        dt = (
            download_time
            if isinstance(download_time, datetime)
            else datetime.fromisoformat(str(download_time))
        )
    except Exception:
        raise HTTPException(
            status_code=422,
            detail="download_time must be ISO datetime, e.g. 2026-01-12T09:50:42.226712",
        )

    rows = session.exec(
        select(WeatherForecast)
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == dt,
        )
        .order_by(WF.forecast_datetime)
    ).all()

    return {
        "station": station,
        "download_time": dt,
        "rows": len(rows),
        "data": rows,
    }


@router.get("/weather-forecast/historical")
def get_historical_forecast_for_time(
    session: Session = Depends(get_session),
    target_time: datetime | str = "",
    days_before: int = 1,
    station: str = "Warszawa",
    tolerance_minutes: int = 90,
):
    """Return the forecast for `target_time` as it was downloaded `days_before` days earlier.

    This is the practical definition of "historyczna prognoza":
    we select a saved batch (`data_pobrania_prognozy`) from the day before,
    then pick the forecast row for the requested hour.
    """

    if not target_time:
        raise HTTPException(status_code=422, detail="target_time is required")
    if days_before < 0 or days_before > 30:
        raise HTTPException(
            status_code=422, detail="days_before must be between 0 and 30"
        )
    if tolerance_minutes < 0 or tolerance_minutes > 24 * 60:
        raise HTTPException(status_code=422, detail="tolerance_minutes out of range")

    try:
        tt = (
            target_time
            if isinstance(target_time, datetime)
            else datetime.fromisoformat(str(target_time))
        )
    except Exception:
        raise HTTPException(
            status_code=422,
            detail="target_time must be ISO datetime, e.g. 2026-01-12T13:00:00",
        )

    desired_day = tt.date() - timedelta(days=days_before)
    day_start = datetime.combine(desired_day, time.min)
    day_end = day_start + timedelta(days=1)

    # Prefer a batch downloaded on the desired day; fall back to latest <= day_end.
    batch_dt = session.exec(
        select(func.max(WF.created_at)).where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at >= day_start,
            WF.created_at < day_end,
        )
    ).one()

    if not batch_dt:
        batch_dt = session.exec(
            select(func.max(WF.created_at)).where(
                WF.latitude == WARSAW_LATITUDE,
                WF.longitude == WARSAW_LONGITUDE,
                WF.created_at <= day_end,
            )
        ).one()

    if not batch_dt:
        raise HTTPException(
            status_code=404, detail="No saved forecast batches for this station"
        )

    # Try exact match first.
    exact = session.exec(
        select(WeatherForecast).where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == batch_dt,
            WF.forecast_datetime == tt,
        )
    ).first()

    if exact:
        return {
            "station": station,
            "target_time": tt,
            "days_before": days_before,
            "batch_download_time": batch_dt,
            "match": "exact",
            "delta_minutes": 0,
            "data": exact,
        }

    # Otherwise choose the nearest hour within tolerance.
    delta_seconds = func.abs(func.extract("epoch", (WF.forecast_datetime - tt)))
    nearest = session.exec(
        select(WeatherForecast)
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == batch_dt,
        )
        .order_by(delta_seconds)
        .limit(1)
    ).first()

    if not nearest:
        raise HTTPException(status_code=404, detail="No rows in selected batch")

    if nearest.forecast_datetime is None:
        raise HTTPException(status_code=404, detail="Nearest forecast has no datetime")

    delta = abs((nearest.forecast_datetime - tt).total_seconds())
    if delta > tolerance_minutes * 60:
        raise HTTPException(
            status_code=404,
            detail="No forecast row close enough to target_time within tolerance",
        )

    return {
        "station": station,
        "target_time": tt,
        "days_before": days_before,
        "batch_download_time": batch_dt,
        "match": "nearest",
        "delta_minutes": round(delta / 60.0, 3),
        "data": nearest,
    }


@router.get("/weather-forecast/tomorrow")
def get_tomorrow_weather_forecast(
    session: Session = Depends(get_session),
    station: str = "Warszawa",
):
    """Zwraca prognozę pogody na dzień jutrzejszy z najnowszej dostępnej prognozy.

    Endpoint filtruje dane prognozy, aby zwrócić tylko te dotyczące dnia jutrzejszego.
    Użyj tego endpointu, gdy potrzebujesz prognozy pogody na jutro.
    """

    from datetime import date, datetime, time, timedelta

    # Oblicz datę jutrzejszą
    tomorrow_date = date.today() + timedelta(days=1)
    tomorrow_start = datetime.combine(tomorrow_date, time.min)
    tomorrow_end = datetime.combine(tomorrow_date, time.max)

    # Znajdź najnowszą dostępną prognozę
    latest_download = session.exec(
        select(func.max(WF.created_at)).where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
        )
    ).one()

    if not latest_download:
        raise HTTPException(
            status_code=404,
            detail=f"Brak dostępnych prognoz pogody dla stacji: {station}",
        )

    # Pobierz prognozy na dzień jutrzejszy z najnowszej dostępnej prognozy
    tomorrow_forecasts = session.exec(
        select(WeatherForecast)
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == latest_download,
            WF.forecast_datetime >= tomorrow_start,
            WF.forecast_datetime <= tomorrow_end,
        )
        .order_by(WF.forecast_datetime)
    ).all()

    if not tomorrow_forecasts:
        raise HTTPException(
            status_code=404,
            detail=f"Brak prognozy pogody na dzień jutrzejszy ({tomorrow_date}) dla stacji: {station}",
        )

    # Oblicz średnie wartości dla całego dnia jutrzejszego
    temps = [
        f.temperature_2m for f in tomorrow_forecasts if f.temperature_2m is not None
    ]
    humidity = [
        f.relative_humidity_2m
        for f in tomorrow_forecasts
        if f.relative_humidity_2m is not None
    ]
    wind_speeds = [
        f.wind_speed_10m for f in tomorrow_forecasts if f.wind_speed_10m is not None
    ]
    precipitation_probs = [f.rain for f in tomorrow_forecasts if f.rain is not None]
    precipitation_amounts = [
        f.precipitation for f in tomorrow_forecasts if f.precipitation is not None
    ]
    pressures: list[float] = []
    clouds: list[float] = []

    summary = {
        "avg_temperature": round(sum(temps) / len(temps), 1) if temps else None,
        "min_temperature": round(min(temps), 1) if temps else None,
        "max_temperature": round(max(temps), 1) if temps else None,
        "avg_humidity": round(sum(humidity) / len(humidity), 1) if humidity else None,
        "avg_wind_speed": round(sum(wind_speeds) / len(wind_speeds), 1)
        if wind_speeds
        else None,
        "max_wind_speed": round(max(wind_speeds), 1) if wind_speeds else None,
        "max_precipitation_probability": round(max(precipitation_probs), 1)
        if precipitation_probs
        else None,
        "total_precipitation": round(sum(precipitation_amounts), 1)
        if precipitation_amounts
        else None,
        "avg_pressure": round(sum(pressures) / len(pressures), 1)
        if pressures
        else None,
        "avg_cloud_cover": round(sum(clouds) / len(clouds), 1) if clouds else None,
    }

    return {
        "station": station,
        "forecast_date": tomorrow_date,
        "forecast_download_time": latest_download,
        "hourly_forecasts_count": len(tomorrow_forecasts),
        "summary": summary,
        "hourly_data": tomorrow_forecasts,
    }


@router.get("/weather-forecast/tomorrow/summary")
def get_tomorrow_weather_summary(
    session: Session = Depends(get_session),
    station: str = "Warszawa",
):
    """Zwraca skrócone podsumowanie prognozy pogody na dzień jutrzejszy.

    Endpoint zwraca tylko statystyki pogodowe bez szczegółowych danych godzinowych.
    Idealny do szybkiego podglądu warunków pogodowych na jutro.
    """

    from datetime import date, datetime, time, timedelta

    # Oblicz datę jutrzejszą
    tomorrow_date = date.today() + timedelta(days=1)
    tomorrow_start = datetime.combine(tomorrow_date, time.min)
    tomorrow_end = datetime.combine(tomorrow_date, time.max)

    # Znajdź najnowszą dostępną prognozę
    latest_download = session.exec(
        select(func.max(WF.created_at)).where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
        )
    ).one()

    if not latest_download:
        raise HTTPException(
            status_code=404,
            detail=f"Brak dostępnych prognoz pogody dla stacji: {station}",
        )

    # Pobierz prognozy na dzień jutrzejszy
    tomorrow_forecasts = session.exec(
        select(WeatherForecast)
        .where(
            WF.latitude == WARSAW_LATITUDE,
            WF.longitude == WARSAW_LONGITUDE,
            WF.created_at == latest_download,
            WF.forecast_datetime >= tomorrow_start,
            WF.forecast_datetime <= tomorrow_end,
        )
        .order_by(WF.forecast_datetime)
    ).all()

    if not tomorrow_forecasts:
        raise HTTPException(
            status_code=404,
            detail=f"Brak prognozy pogody na dzień jutrzejszy ({tomorrow_date}) dla stacji: {station}",
        )

    # Oblicz statystyki
    temps = [
        f.temperature_2m for f in tomorrow_forecasts if f.temperature_2m is not None
    ]
    humidity = [
        f.relative_humidity_2m
        for f in tomorrow_forecasts
        if f.relative_humidity_2m is not None
    ]
    wind_speeds = [
        f.wind_speed_10m for f in tomorrow_forecasts if f.wind_speed_10m is not None
    ]
    precipitation_probs = [f.rain for f in tomorrow_forecasts if f.rain is not None]
    precipitation_amounts = [
        f.precipitation for f in tomorrow_forecasts if f.precipitation is not None
    ]
    pressures: list[float] = []
    clouds: list[float] = []

    # Znajdź godziny z najlepszymi i najgorszymi warunkami
    min_temp_hour = min(
        tomorrow_forecasts, key=lambda x: x.temperature_2m or float("inf")
    )
    max_temp_hour = max(
        tomorrow_forecasts, key=lambda x: x.temperature_2m or float("-inf")
    )
    max_wind_hour = max(tomorrow_forecasts, key=lambda x: x.wind_speed_10m or 0)
    max_precip_hour = max(tomorrow_forecasts, key=lambda x: x.rain or 0)

    min_temp_time = (
        min_temp_hour.forecast_datetime.strftime("%H:%M")
        if min_temp_hour.forecast_datetime
        else None
    )
    max_temp_time = (
        max_temp_hour.forecast_datetime.strftime("%H:%M")
        if max_temp_hour.forecast_datetime
        else None
    )
    max_wind_time = (
        max_wind_hour.forecast_datetime.strftime("%H:%M")
        if max_wind_hour.forecast_datetime
        else None
    )
    max_precip_time = (
        max_precip_hour.forecast_datetime.strftime("%H:%M")
        if max_precip_hour.forecast_datetime
        and max_precip_hour.rain
        and max_precip_hour.rain > 0
        else None
    )

    summary = {
        "avg_temperature": round(sum(temps) / len(temps), 1) if temps else None,
        "min_temperature": round(min(temps), 1) if temps else None,
        "max_temperature": round(max(temps), 1) if temps else None,
        "min_temp_time": min_temp_time,
        "max_temp_time": max_temp_time,
        "avg_humidity": round(sum(humidity) / len(humidity), 1) if humidity else None,
        "avg_wind_speed": round(sum(wind_speeds) / len(wind_speeds), 1)
        if wind_speeds
        else None,
        "max_wind_speed": round(max(wind_speeds), 1) if wind_speeds else None,
        "max_wind_time": max_wind_time,
        "max_precipitation_probability": round(max(precipitation_probs), 1)
        if precipitation_probs
        else None,
        "max_precip_time": max_precip_time,
        "total_precipitation": round(sum(precipitation_amounts), 1)
        if precipitation_amounts
        else None,
        "avg_pressure": round(sum(pressures) / len(pressures), 1)
        if pressures
        else None,
        "avg_cloud_cover": round(sum(clouds) / len(clouds), 1) if clouds else None,
    }

    # Określ ogólne warunki pogodowe
    weather_condition = "słonecznie"
    if summary["avg_cloud_cover"] and summary["avg_cloud_cover"] > 80:
        weather_condition = "pochmurno"
    elif summary["avg_cloud_cover"] and summary["avg_cloud_cover"] > 50:
        weather_condition = "częściowo pochmurno"

    if (
        summary["max_precipitation_probability"]
        and summary["max_precipitation_probability"] > 50
    ):
        weather_condition += " z opadami"
    elif (
        summary["max_precipitation_probability"]
        and summary["max_precipitation_probability"] > 20
    ):
        weather_condition += " z możliwymi opadami"

    return {
        "station": station,
        "forecast_date": tomorrow_date,
        "forecast_download_time": latest_download,
        "weather_condition": weather_condition,
        "summary": summary,
        "recommendation": {
            "umbrella_needed": summary["max_precipitation_probability"]
            and summary["max_precipitation_probability"] > 30,
            "jacket_needed": summary["min_temperature"]
            and summary["min_temperature"] < 10,
            "wind_warning": summary["max_wind_speed"]
            and summary["max_wind_speed"] > 15,
        },
    }
