"""Controller for Cross Border Flows data fetching."""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, func, select

from app.models import CrossBorderFlows

BATCH_SIZE = 2000


def flush_flows_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor danych przepływów do bazy."""
    if not buffer:
        return

    print(f"[FLOWS] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            stmt = insert(CrossBorderFlows).values(buffer)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["dtime", "section_code", "business_date"]
            )
            session.exec(stmt)
            session.commit()

            print("[FLOWS] Zapis zakończony.")

    except Exception as e:
        print(f"[FLOWS]  Błąd zapisu: {e}")
        import traceback

        traceback.print_exc()


def pobierz_dane_flows(engine, data_od: str, data_do: str = None):
    """Pobiera dane przepływów międzysystemowych z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()

    buffer = []

    print(f"[FLOWS] Pobieranie danych: {data_od} → {data_do}")

    current = start_date
    while current <= end_date:
        dstr = current.isoformat()
        url = f"https://api.raporty.pse.pl/api/przeplywy-mocy?$filter=business_date%20eq%20'{dstr}'"
        print(f"[FLOWS] Pobieranie danych dla {dstr}...")
        print(f"[FLOWS] URL: {url}")

        try:
            headers = {
                "Accept": "application/json",
                "User-Agent": "PSE-Data-Fetcher/1.0",
            }

            # Retry logic
            max_retries = 3
            retry_delay = 5
            success = False

            for attempt in range(max_retries):
                try:
                    r = requests.get(url, headers=headers, timeout=30, verify=True)

                    if r.status_code != 200:
                        print(f"[FLOWS]  Błąd {dstr}: {r.status_code} - {r.reason}")
                        if attempt < max_retries - 1:
                            print(
                                f"[FLOWS]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})"
                            )
                            time.sleep(retry_delay)
                            continue
                    else:
                        items = r.json().get("value", [])
                        print(f"[FLOWS]  Pobrano {len(items)} rekordów z API.")
                        if items:
                            print(f"[FLOWS]  Przykładowy rekord: {items[0]}")

                        for item in items:
                            dtime_value = (
                                datetime.fromisoformat(item["dtime"].replace(" ", "T"))
                                if item.get("dtime")
                                else None
                            )
                            if dtime_value and dtime_value > now_cutoff:
                                continue

                            buffer.append(
                                {
                                    "dtime": dtime_value,
                                    "dtime_utc": datetime.fromisoformat(
                                        item["dtime_utc"].replace(" ", "T")
                                    )
                                    if item.get("dtime_utc")
                                    else None,
                                    "period": item.get("period"),
                                    "period_utc": item.get("period_utc"),
                                    "business_date": datetime.fromisoformat(
                                        item["business_date"]
                                    ),
                                    # Przepływy międzysystemowe
                                    "section_code": item.get("section_code", ""),
                                    "value": float(item.get("value", 0))
                                    if item.get("value") is not None
                                    else 0.0,
                                    # Znaczniki publikacji
                                    "publication_ts": datetime.fromisoformat(
                                        item["publication_ts"].replace(" ", "T")
                                    )
                                    if item.get("publication_ts")
                                    else None,
                                    "publication_ts_utc": datetime.fromisoformat(
                                        item["publication_ts_utc"].replace(" ", "T")
                                    )
                                    if item.get("publication_ts_utc")
                                    else None,
                                }
                            )
                        print(f"[FLOWS]  Bufor zawiera teraz {len(buffer)} rekordów.")
                        success = True
                        break

                except requests.exceptions.RequestException as req_err:
                    print(f"[FLOWS] Błąd połączenia (próba {attempt + 1}): {req_err}")
                    if attempt < max_retries - 1:
                        print(f"[FLOWS]  Ponowienie próby za {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[FLOWS]  Wszystkie próby nieudane dla daty {dstr}")

            if not success:
                print(f"[FLOWS]  Pomijanie daty {dstr} z powodu błędów")

        except Exception as err:
            print(f"[FLOWS] ERROR: {err}")

        if len(buffer) >= BATCH_SIZE:
            print(f"[FLOWS]  Zapisywanie {len(buffer)} rekordów do bazy...")
            flush_flows_buffer_to_db(engine, buffer)
            buffer = []

        current += timedelta(days=1)

    if buffer:
        print(f"[FLOWS]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_flows_buffer_to_db(engine, buffer)

    print("[FLOWS] Zakończono pobieranie.")


def uzupelnij_flows(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane przepływów."""
    print("[FLOWS] Sprawdzanie ostatniej daty w bazie przepływów...")
    try:
        with Session(engine) as session:
            stmt = select(func.max(CrossBorderFlows.business_date))
            last_date = session.exec(stmt).first()
            print(f"[FLOWS] Ostatnia data w bazie: {last_date}")

            today = datetime.now().date()
            start = "2025-03-03"

            if last_date:
                if isinstance(last_date, datetime):
                    start = last_date.date().isoformat()
                else:
                    start = last_date.isoformat()

            if start_date:
                start = max(start, start_date)

            if start > today.isoformat():
                print("[FLOWS] Baza aktualna.")
                return

            print(f"[FLOWS] Rozpoczęcie pobierania od {start} do {today.isoformat()}")
            pobierz_dane_flows(engine, start, today.isoformat())

    except Exception as e:
        print(f"[FLOWS] Błąd uzupełniania: {e}")
        import traceback

        traceback.print_exc()
