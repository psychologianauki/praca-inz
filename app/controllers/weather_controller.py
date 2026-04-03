"""Controller for historical weather data fetching."""

from datetime import datetime
from typing import Any, Dict, List

import requests
from sqlalchemy.dialects.mysql import insert
from sqlmodel import Session

from app.models import WeatherData

BATCH_SIZE = 5000


def flush_weather_buffer(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje dane pogodowe do bazy."""
    if not buffer:
        return

    print(f"[Weather] Zapisywanie {len(buffer)} rekordów...")
    try:
        with Session(engine) as session:
            for i in range(0, len(buffer), BATCH_SIZE):
                batch = buffer[i : i + BATCH_SIZE]

                statement = insert(WeatherData).values(batch)
                statement = statement.prefix_with("IGNORE")

                session.exec(statement)
                session.commit()

            print("[Weather] Zapisano pomyślnie.")
    except Exception as e:
        print(f"[Weather]  Błąd zapisu: {e}")


def pobierz_dane_historyczne_open_meteo(
    engine, szerokosc: float, dlugosc: float, data_od: str, data_do: str, stacja: str
):
    """Pobiera dane historyczne z Open-Meteo API."""
    print(f"[Weather] Pobieranie danych historycznych dla {stacja}")
    print(f"   Okres: {data_od} - {data_do}\n")

    parametry_lista = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "wind_direction_10m",
        "pressure_msl",
        "cloud_cover",
        "shortwave_radiation",
        "weather_code",
    ]
    parametry_str = ",".join(parametry_lista)

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={szerokosc}&longitude={dlugosc}&"
        f"start_date={data_od}&end_date={data_do}&"
        f"hourly={parametry_str}&timezone=Europe/Warsaw"
    )

    try:
        print(f"[Weather] Pobieranie: {url[:100]}...")
        response = requests.get(url)

        if response.status_code != 200:
            print(f"[Weather] Błąd API: {response.status_code}")
            return []

        data = response.json()

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        count = len(times)

        print(f"[Weather] Pobrano {count} godzin danych")
        print(f"   To około {round(count / 24)} dni historii\n")

        weather_history: List[Dict[str, Any]] = []

        for i in range(count):
            time_str = times[i]
            czas_dt = datetime.fromisoformat(time_str)

            row = {
                "stacja": stacja,
                "szerokosc": szerokosc,
                "dlugosc": dlugosc,
                "czas": czas_dt,
                "data": czas_dt.date().isoformat(),
                "godzina": czas_dt.hour,
                "temperatura": hourly["temperature_2m"][i],
                "wilgotnosc": int(hourly["relative_humidity_2m"][i])
                if hourly["relative_humidity_2m"][i] is not None
                else None,
                "opad": hourly["precipitation"][i],
                "predkosc_wiatru": hourly["wind_speed_10m"][i],
                "kierunek_wiatru": int(hourly["wind_direction_10m"][i])
                if hourly["wind_direction_10m"][i] is not None
                else None,
                "cisnienie": hourly["pressure_msl"][i],
                "zachmurzenie": int(hourly["cloud_cover"][i])
                if hourly["cloud_cover"][i] is not None
                else None,
                "natezenie_swiatla": int(hourly["shortwave_radiation"][i])
                if hourly["shortwave_radiation"][i] is not None
                else None,
                "weather_code": int(hourly["weather_code"][i])
                if hourly["weather_code"][i] is not None
                else None,
            }
            weather_history.append(row)

        if weather_history:
            flush_weather_buffer(engine, weather_history)

        return weather_history

    except Exception as error:
        print(f"[Weather] Błąd pobierania/przetwarzania: {error}")
        return []
