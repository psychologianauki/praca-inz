"""Controller for energy price data fetching and management."""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, create_engine, func, select

from app.core.config import settings
from app.models import EnergyPrice

BATCH_SIZE = 2000


def flush_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor do bazy."""
    if not buffer:
        return

    print(f"[EnergyPrice]  Zapisywanie {len(buffer)} rekordów do bazy...")

    try:
        with Session(engine) as session:
            statement = (
                insert(EnergyPrice)
                .values(buffer)
                .on_conflict_do_nothing(index_elements=["doba", "godzina"])
            )
            result = session.exec(statement)
            session.commit()
            print(f"[EnergyPrice] Zapisano pomyślnie {len(buffer)} rekordów.")
    except Exception as e:
        print(f"[EnergyPrice]  Błąd zapisu do bazy: {e}")
        import traceback

        traceback.print_exc()


def pobierz_dane_i_wyslij_do_bazy(engine, data_od: str, data_do: str = None):
    # Print EnergyPrice table columns
    from app.models import EnergyPrice

    print("\n" + "=" * 60)
    print("[EnergyPrice] Kolumny tabeli energy_prices:")
    print("=" * 60)
    for column_name in EnergyPrice.__fields__.keys():
        print(f"  - {column_name}")
    print("=" * 60 + "\n")

    if data_do is None:
        data_do = datetime.now().date().isoformat()

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()
    current_date = start_date

    records_buffer: List[Dict[str, Any]] = []

    print(f"[EnergyPrice] Pobieranie od {data_od} do {data_do}...")

    while current_date <= end_date:
        date_string = current_date.isoformat()

        url = (
            "https://api.raporty.pse.pl/api/rce-pln"
            f"?$filter=business_date eq '{date_string}'"
        )
        print(f"[EnergyPrice] Pobieranie danych dla {date_string}...")
        print(url)

        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json"},
                timeout=30,
            )

            if response.status_code == 200:
                items = response.json().get("value", [])
                print(f"[EnergyPrice]  Pobrano {len(items)} rekordów z API.")

                if items and len(items) > 0:
                    # Show first item as sample
                    print(f"[EnergyPrice]  Przykładowy rekord: {items[0]}")

                for item in items:
                    records_buffer.append(
                        {
                            "doba": datetime.fromisoformat(item["business_date"]),
                            "godzina": item["period"],
                            "cena_mwh": float(item["rce_pln"]),
                            "business_date": datetime.fromisoformat(
                                item["business_date"]
                            ),
                            "source_datetime": datetime.fromisoformat(
                                item["publication_ts"]
                            ),
                        }
                    )

                print(
                    f"[EnergyPrice]  Bufor zawiera teraz {len(records_buffer)} rekordów."
                )
            else:
                print(
                    f"[EnergyPrice]  API zwróciło status {response.status_code} dla {date_string}"
                )
                print(f"[EnergyPrice] Response: {response.text[:200]}")

        except Exception as error:
            print(f"[EnergyPrice]  {date_string}: {error}")

        if len(records_buffer) >= BATCH_SIZE:
            flush_buffer_to_db(engine, records_buffer)
            records_buffer.clear()

        current_date += timedelta(days=1)

    if records_buffer:
        flush_buffer_to_db(engine, records_buffer)

    print("[EnergyPrice] Zakończono pobieranie.")


def uzupelnij_brakujace_dane(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane od ostatniej daty w bazie do dziś."""
    try:
        with Session(engine) as session:
            statement = select(func.max(EnergyPrice.business_date))
            last_entry_date = session.exec(statement).first()

            today_str = datetime.now().date().isoformat()
            start_date_str: str

            if not last_entry_date:
                start_date_str = "2025-03-03"
            else:
                last_date = (
                    last_entry_date.date()
                    if isinstance(last_entry_date, datetime)
                    else last_entry_date
                )
                start_date_str = last_date.isoformat()

            if start_date:
                start_date_str = max(start_date_str, start_date)

            if start_date_str > today_str:
                print("[EnergyPrice] Baza aktualna.")
                return

            pobierz_dane_i_wyslij_do_bazy(engine, start_date_str, today_str)

    except Exception as e:
        print(f"[EnergyPrice] Błąd uzupełniania: {e}")
