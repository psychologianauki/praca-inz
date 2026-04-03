"""Controller for CO2 price data fetching."""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, func, select

from app.models import Co2Price

BATCH_SIZE = 1000


def flush_co2_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor cen CO2 do bazy."""
    if not buffer:
        return

    print(f"[CO2] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            statement = insert(Co2Price).values(buffer)
            statement = statement.on_conflict_do_nothing(
                index_elements=["business_date"]
            )

            session.exec(statement)
            session.commit()

            print("[CO2] Zapisano pomyślnie.")

    except Exception as e:
        print(f"[CO2]  Błąd zapisu: {e}")
        import traceback

        traceback.print_exc()


def pobierz_dane_co2_i_wyslij_do_bazy(engine, data_od: str, data_do: str = None):
    """Pobiera ceny uprawnień CO2 (RCCO2) z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()

    start_date = datetime.fromisoformat(str(data_od)).date()
    end_date = datetime.fromisoformat(str(data_do)).date()
    current_date = start_date

    records_buffer: List[Dict[str, Any]] = []

    print(f"[CO2] Rozpoczynam pobieranie od {data_od} do {data_do}...")

    while current_date <= end_date:
        date_string = current_date.isoformat()
        url = f"https://api.raporty.pse.pl/api/rcco2?$filter=business_date%20eq%20'{date_string}'"
        print(f"[CO2] Pobieranie danych dla {date_string}...")
        print(f"[CO2] URL: {url}")

        try:
            response = requests.get(url, headers={"Accept": "application/json"})

            if response.status_code != 200:
                print(f"[CO2]  Błąd dla {date_string}: {response.reason}")
            else:
                data = response.json()
                items = data.get("value", [])
                print(f"[CO2]  Pobrano {len(items)} rekordów z API.")
                if items:
                    print(f"[CO2]  Przykładowy rekord: {items[0]}")

                if items and isinstance(items, list):
                    for item in items:
                        records_buffer.append(
                            {
                                "rcco2_eur": float(item["rcco2_eur"]),
                                "rcco2_pln": float(item["rcco2_pln"]),
                                "business_date": datetime.fromisoformat(
                                    str(item["business_date"])
                                ),
                                "source_datetime": datetime.fromisoformat(
                                    str(item["publication_ts"])
                                ),
                            }
                        )
                    print(
                        f"[CO2]  Bufor zawiera teraz {len(records_buffer)} rekordów."
                    )

        except Exception as error:
            print(f"[CO2] Błąd przetwarzania dnia {date_string}: {error}")

        if len(records_buffer) >= BATCH_SIZE:
            print(f"[CO2]  Zapisywanie {len(records_buffer)} rekordów do bazy...")
            flush_co2_buffer_to_db(engine, records_buffer)
            records_buffer = []

        current_date += timedelta(days=1)

    if records_buffer:
        print(
            f"[CO2]  Zapisywanie końcowych {len(records_buffer)} rekordów do bazy..."
        )
        flush_co2_buffer_to_db(engine, records_buffer)

    print("[CO2] Zakończono proces pobierania.")


def uzupelnij_brakujace_dane_co2(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane CO2."""
    print("[CO2] Sprawdzanie ostatniej daty w bazie CO2...")
    try:
        with Session(engine) as session:
            statement = select(func.max(Co2Price.business_date))
            last_entry_date = session.exec(statement).first()
            print(f"[CO2] Ostatnia data w bazie: {last_entry_date}")

            today_str = datetime.now().date().isoformat()
            start_date_str = "2025-03-03"

            if last_entry_date:
                if isinstance(last_entry_date, datetime):
                    last_date = last_entry_date.date()
                elif isinstance(last_entry_date, str):
                    last_date = datetime.fromisoformat(last_entry_date).date()
                else:
                    last_date = last_entry_date

                start_date_str = last_date.isoformat()

            if start_date:
                start_date_str = max(start_date_str, start_date)

            if start_date_str > today_str:
                print("[CO2] Baza jest aktualna.")
                return

            print(f"[CO2] Rozpoczęcie pobierania od {start_date_str} do {today_str}")
            pobierz_dane_co2_i_wyslij_do_bazy(engine, start_date_str, today_str)

    except Exception as e:
        print(f"[CO2] Błąd w funkcji uzupełniania: {e}")
        import traceback

        traceback.print_exc()
