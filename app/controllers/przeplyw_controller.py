"""Controller for power flow (przepływ mocy) data fetching."""
from urllib.parse import unquote, urlencode

import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

from sqlmodel import Session, select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import PrzeplywMocyJednostek


BATCH_SIZE = 1000


def flush_przeplyw_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor przepływów do bazy z obsługą duplikatów."""
    if not buffer:
        return

    print(f"[Przeplyw] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            statement = insert(PrzeplywMocyJednostek).values(buffer)
            
            # Obsługa duplikatów - aktualizacja istniejących rekordów
            upsert_statement = statement.on_conflict_do_update(
                constraint="uq_power_flows_section_dtime_biz",
                set_={
                    "value": statement.excluded.value,
                    "period": statement.excluded.period,
                    "period_utc": statement.excluded.period_utc,
                    "publication_ts": statement.excluded.publication_ts,
                    "dtime_utc": statement.excluded.dtime_utc,
                    "updated_at": func.now(),
                }
            )
            
            session.exec(upsert_statement)
            session.commit()
            
            print("[Przeplyw] Zapisano pomyślnie.")
            
    except Exception as e:
        print(f"[Przeplyw]  Błąd zapisu: {e}")


def pobierz_dane_przeplyw_i_wyslij_do_bazy(engine, data_od: str, data_do: str = None):
    """Pobiera przepływy mocy jednostek z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()

    start_date = datetime.fromisoformat(str(data_od)).date()
    end_date = datetime.fromisoformat(str(data_do)).date()
    current_date = start_date

    records_buffer: List[Dict[str, Any]] = []

    print(f"[Przeplyw] Rozpoczynam pobieranie od {data_od} do {data_do}...")

    while current_date <= end_date:
        date_string = current_date.isoformat()
        url = f"https://api.raporty.pse.pl/api/przeplywy-mocy?$filter=business_date%20eq%20'{date_string}'"

        print("URL:", url)
        
        # Retry logic
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                headers = {
                    "Accept": "application/json",
                    "User-Agent": "PSE-Data-Fetcher/1.0"
                }
                
                response = requests.get(
                    url, 
                    headers=headers,
                    timeout=30,
                    verify=True
                )
                
                if response.status_code != 200:
                    print(f"[Przeplyw]  Błąd dla {date_string}: {response.status_code} - {response.reason}")
                    if attempt < max_retries - 1:
                        print(f"[Przeplyw]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        continue
                    break
                else:
                    data = response.json()
                    items = data.get("value", [])
                    print(f"[Przeplyw]  Pobrano {len(items)} rekordów z API.")
                    if items:
                        print(f"[Przeplyw]  Przykładowy rekord: {items[0]}")

                    if items and isinstance(items, list):
                        for item in items:
                            records_buffer.append({
                                "dtime": datetime.fromisoformat(str(item["dtime"])),
                                "dtime_utc": datetime.fromisoformat(str(item["dtime_utc"])),
                                "value": float(item["value"]),
                                "period": item["period"],
                                "period_utc": item["period_utc"],
                                "business_date": datetime.fromisoformat(str(item["business_date"])),
                                "section_code": item["section_code"],
                                "publication_ts": datetime.fromisoformat(str(item["publication_ts"])),
                            })
                        print(f"[Przeplyw]  Bufor zawiera teraz {len(records_buffer)} rekordów.")
                    break

            except Exception as error:
                print(f"[Przeplyw] Błąd przetwarzania dnia {date_string} (próba {attempt + 1}): {error}")
                if attempt < max_retries - 1:
                    print(f"[Przeplyw]  Ponowienie próby za {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    print(f"[Przeplyw]  Wszystkie próby nieudane dla daty {date_string}")

        if len(records_buffer) >= BATCH_SIZE:
            print(f"[Przeplyw]  Zapisywanie {len(records_buffer)} rekordów do bazy...")
            flush_przeplyw_buffer_to_db(engine, records_buffer)
            records_buffer = []

        current_date += timedelta(days=1)

    if records_buffer:
        print(f"[Przeplyw]  Zapisywanie końcowych {len(records_buffer)} rekordów do bazy...")
        flush_przeplyw_buffer_to_db(engine, records_buffer)

    print("[Przeplyw] Zakończono proces pobierania.")


def uzupelnij_brakujace_dane_przeplyw(engine):
    """Uzupełnia brakujące dane przepływów."""
    print("[Przeplyw] Sprawdzanie ostatniej daty w bazie przepływów...")
    try:
        with Session(engine) as session:
            statement = select(func.max(PrzeplywMocyJednostek.business_date))
            last_entry_date = session.exec(statement).first()
            print(f"[Przeplyw] Ostatnia data w bazie: {last_entry_date}")

            start_date_str: str
            today_str = datetime.now().date().isoformat()

            if not last_entry_date:
                print("[Przeplyw] Baza pusta. Start od 2024-06-15")
                start_date_str = "2025-03-03" 
            else:
                if isinstance(last_entry_date, datetime):
                    last_date = last_entry_date.date()
                else:
                    last_date = last_entry_date 
                
                next_day = last_date + timedelta(days=1)
                start_date_str = next_day.isoformat()

            if start_date_str > today_str:
                print("[Przeplyw] Baza jest aktualna.")
                return

            print(f"[Przeplyw] Rozpoczęcie pobierania od {start_date_str} do {today_str}")
            pobierz_dane_przeplyw_i_wyslij_do_bazy(engine, start_date_str, today_str)

    except Exception as e:
        print(f"[Przeplyw] Błąd w funkcji uzupełniania: {e}")
