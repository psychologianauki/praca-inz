"""Controller for CRB (dane bilansujące) data fetching."""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, func, select

from app.models import CrbRozliczenia

BATCH_SIZE = 2000


def flush_crb_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor danych CRB do bazy z obsługą duplikatów."""
    if not buffer:
        return

    print(f"[CRB] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            stmt = insert(CrbRozliczenia).values(buffer)

            # Obsługa duplikatów - aktualizacja istniejących rekordów
            upsert_stmt = stmt.on_conflict_do_update(
                constraint="uq_crb_dtime_period",
                set_={
                    "business_date": stmt.excluded.business_date,
                    "dtime_utc": stmt.excluded.dtime_utc,
                    "period_utc": stmt.excluded.period_utc,
                    "cen_cost": stmt.excluded.cen_cost,
                    "ckoeb_cost": stmt.excluded.ckoeb_cost,
                    "ceb_pp_cost": stmt.excluded.ceb_pp_cost,
                    "ceb_sr_cost": stmt.excluded.ceb_sr_cost,
                    "ceb_sr_afrrd_cost": stmt.excluded.ceb_sr_afrrd_cost,
                    "ceb_sr_afrrg_cost": stmt.excluded.ceb_sr_afrrg_cost,
                    "publication_ts": stmt.excluded.publication_ts,
                    "publication_ts_utc": stmt.excluded.publication_ts_utc,
                },
            )

            session.exec(upsert_stmt)
            session.commit()

            print("[CRB] Zapis zakończony.")

    except Exception as e:
        print(f"[CRB]  Błąd zapisu: {e}")
        import traceback

        traceback.print_exc()


def pobierz_dane_crb(engine, data_od: str, data_do: str = None):
    """Pobiera dane CRB (rozliczenia) z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()

    buffer = []

    print(f"[CRB] Pobieranie danych: {data_od} → {data_do}")

    current = start_date
    while current <= end_date:
        dstr = current.isoformat()
        url = f"https://api.raporty.pse.pl/api/crb-rozl?$filter=business_date%20eq%20'{dstr}'"
        print(f"[CRB] Pobieranie danych dla {dstr}...")
        print(f"[CRB] URL: {url}")

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
                        print(f"[CRB]  Błąd {dstr}: {r.status_code} - {r.reason}")
                        if attempt < max_retries - 1:
                            print(
                                f"[CRB]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})"
                            )
                            time.sleep(retry_delay)
                            continue
                    else:
                        items = r.json().get("value", [])
                        print(f"[CRB]  Pobrano {len(items)} rekordów z API.")
                        if items:
                            print(f"[CRB]  Przykładowy rekord: {items[0]}")

                        for item in items:
                            dtime_value = datetime.fromisoformat(
                                item["dtime"].replace(" ", "T")
                            )
                            if dtime_value > now_cutoff:
                                continue

                            buffer.append(
                                {
                                    "dtime": dtime_value,
                                    "dtime_utc": datetime.fromisoformat(
                                        item["dtime_utc"].replace(" ", "T")
                                    ),
                                    "period": item["period"],
                                    "period_utc": item["period_utc"],
                                    "business_date": datetime.fromisoformat(
                                        item["business_date"]
                                    ),
                                    # Koszty i ceny (z obsługą wartości null/None)
                                    "cen_cost": float(item["cen_cost"])
                                    if item.get("cen_cost") is not None
                                    else 0.0,
                                    "ckoeb_cost": float(item["ckoeb_cost"])
                                    if item.get("ckoeb_cost") is not None
                                    else None,
                                    "ceb_pp_cost": float(item["ceb_pp_cost"])
                                    if item.get("ceb_pp_cost") is not None
                                    else None,
                                    "ceb_sr_cost": float(item["ceb_sr_cost"])
                                    if item.get("ceb_sr_cost") is not None
                                    else None,
                                    # Nowe pola aFRR
                                    "ceb_sr_afrrd_cost": float(
                                        item["ceb_sr_afrrd_cost"]
                                    )
                                    if item.get("ceb_sr_afrrd_cost") is not None
                                    else None,
                                    "ceb_sr_afrrg_cost": float(
                                        item["ceb_sr_afrrg_cost"]
                                    )
                                    if item.get("ceb_sr_afrrg_cost") is not None
                                    else None,
                                    # Znaczniki publikacji
                                    "publication_ts": datetime.fromisoformat(
                                        item["publication_ts"].replace(" ", "T")
                                    ),
                                    "publication_ts_utc": datetime.fromisoformat(
                                        item["publication_ts_utc"].replace(" ", "T")
                                    ),
                                }
                            )
                        print(f"[CRB]  Bufor zawiera teraz {len(buffer)} rekordów.")
                        success = True
                        break

                except requests.exceptions.RequestException as req_err:
                    print(f"[CRB] Błąd połączenia (próba {attempt + 1}): {req_err}")
                    if attempt < max_retries - 1:
                        print(f"[CRB]  Ponowienie próby za {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[CRB]  Wszystkie próby nieudane dla daty {dstr}")

            if not success:
                print(f"[CRB]  Pomijanie daty {dstr} z powodu błędów")

        except Exception as err:
            print(f"[CRB] ERROR: {err}")

        if len(buffer) >= BATCH_SIZE:
            print(f"[CRB]  Zapisywanie {len(buffer)} rekordów do bazy...")
            flush_crb_buffer_to_db(engine, buffer)
            buffer = []

        current += timedelta(days=1)

    if buffer:
        print(f"[CRB]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_crb_buffer_to_db(engine, buffer)

    print("[CRB] Zakończono pobieranie.")


def uzupelnij_crb(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane CRB."""
    print("[CRB] Sprawdzanie ostatniej daty w bazie CRB...")
    try:
        with Session(engine) as session:
            stmt = select(func.max(CrbRozliczenia.business_date))
            last_date = session.exec(stmt).first()
            print(f"[CRB] Ostatnia data w bazie: {last_date}")

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
                print("[CRB] Baza aktualna.")
                return

            print(f"[CRB] Rozpoczęcie pobierania od {start} do {today.isoformat()}")
            pobierz_dane_crb(engine, start, today.isoformat())

    except Exception as e:
        print(f"[CRB] Błąd uzupełniania: {e}")
        import traceback

        traceback.print_exc()
