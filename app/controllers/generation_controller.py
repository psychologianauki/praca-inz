"""Controller for Generation by Source data fetching."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

from sqlmodel import Session, select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import GenerationBySource


BATCH_SIZE = 2000


def flush_generation_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor danych generacji do bazy."""
    if not buffer:
        return

    print(f"[GENERATION] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            stmt = insert(GenerationBySource).values(buffer)
            stmt = stmt.on_conflict_do_nothing(index_elements=["dtime", "business_date"])
            session.exec(stmt)
            session.commit()

            print("[GENERATION] Zapis zakończony.")

    except Exception as e:
        print(f"[GENERATION]  Błąd zapisu: {e}")
        import traceback
        traceback.print_exc()


def pobierz_dane_generation(engine, data_od: str, data_do: str = None):
    """Pobiera dane generacji według źródeł z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()

    buffer = []

    print(f"[GENERATION] Pobieranie danych: {data_od} → {data_do}")

    current = start_date
    while current <= end_date:
        dstr = current.isoformat()
        url = f"https://api.raporty.pse.pl/api/his-wlk-cal?$filter=business_date%20eq%20'{dstr}'"
        print(f"[GENERATION] Pobieranie danych dla {dstr}...")
        print(f"[GENERATION] URL: {url}")

        try:
            headers = {
                "Accept": "application/json",
                "User-Agent": "PSE-Data-Fetcher/1.0"
            }
            
            # Retry logic
            max_retries = 3
            retry_delay = 5
            success = False
            
            for attempt in range(max_retries):
                try:
                    r = requests.get(
                        url, 
                        headers=headers,
                        timeout=30,
                        verify=True
                    )
                    
                    if r.status_code != 200:
                        print(f"[GENERATION]  Błąd {dstr}: {r.status_code} - {r.reason}")
                        if attempt < max_retries - 1:
                            print(f"[GENERATION]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                    else:
                        items = r.json().get("value", [])
                        print(f"[GENERATION]  Pobrano {len(items)} rekordów z API.")
                        if items:
                            print(f"[GENERATION]  Przykładowy rekord: {items[0]}")

                        for item in items:
                            dtime_value = datetime.fromisoformat(item["dtime"].replace(" ", "T")) if item.get("dtime") else None
                            if dtime_value and dtime_value > now_cutoff:
                                continue

                            buffer.append({
                                "dtime": dtime_value,
                                "dtime_utc": datetime.fromisoformat(item["dtime_utc"].replace(" ", "T")) if item.get("dtime_utc") else None,
                                "period": item.get("period"),
                                "period_utc": item.get("period_utc"),
                                "business_date": datetime.fromisoformat(item["business_date"]),
                                
                                # Rzeczywiste pola z PSE API
                                "jgw1": float(item.get("jgw1", 0)) if item.get("jgw1") is not None else None,
                                "jgw2": float(item.get("jgw2", 0)) if item.get("jgw2") is not None else None,
                                "jgm1": float(item.get("jgm1", 0)) if item.get("jgm1") is not None else None,
                                "jgm2": float(item.get("jgm2", 0)) if item.get("jgm2") is not None else None,
                                "jgz1": float(item.get("jgz1", 0)) if item.get("jgz1") is not None else None,
                                "jgz2": float(item.get("jgz2", 0)) if item.get("jgz2") is not None else None,
                                "jgz3": float(item.get("jgz3", 0)) if item.get("jgz3") is not None else None,
                                "jga": float(item.get("jga", 0)) if item.get("jga") is not None else None,
                                "jgo": float(item.get("jgo", 0)) if item.get("jgo") is not None else None,
                                "jnwrb": float(item.get("jnwrb", 0)) if item.get("jnwrb") is not None else None,
                                "wi": float(item.get("wi", 0)) if item.get("wi") is not None else None,
                                "pv": float(item.get("pv", 0)) if item.get("pv") is not None else None,
                                
                                # Bilans - wymagane pola
                                "zapotrzebowanie": float(item.get("demand", 0)) if item.get("demand") is not None else 0.0,
                                "swm_p": float(item.get("swm_p", 0)) if item.get("swm_p") is not None else None,
                                "swm_np": float(item.get("swm_np", 0)) if item.get("swm_np") is not None else None,
                                "jg": float(item.get("jg", 0)) if item.get("jg") is not None else None,
                                
                                # Znaczniki publikacji
                                "publication_ts": datetime.fromisoformat(item["publication_ts"].replace(" ", "T")) if item.get("publication_ts") else None,
                                "publication_ts_utc": datetime.fromisoformat(item["publication_ts_utc"].replace(" ", "T")) if item.get("publication_ts_utc") else None,
                            })  
                        print(f"[GENERATION]  Bufor zawiera teraz {len(buffer)} rekordów.")
                        success = True
                        break
                        
                except requests.exceptions.RequestException as req_err:
                    print(f"[GENERATION] Błąd połączenia (próba {attempt + 1}): {req_err}")
                    if attempt < max_retries - 1:
                        print(f"[GENERATION]  Ponowienie próby za {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[GENERATION]  Wszystkie próby nieudane dla daty {dstr}")
                        
            if not success:
                print(f"[GENERATION]  Pomijanie daty {dstr} z powodu błędów")

        except Exception as err:
            print(f"[GENERATION] ERROR: {err}")

        if len(buffer) >= BATCH_SIZE:
            print(f"[GENERATION]  Zapisywanie {len(buffer)} rekordów do bazy...")
            flush_generation_buffer_to_db(engine, buffer)
            buffer = []

        current += timedelta(days=1)

    if buffer:
        print(f"[GENERATION]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_generation_buffer_to_db(engine, buffer)

    print("[GENERATION] Zakończono pobieranie.")


def uzupelnij_generation(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane generacji."""
    print("[GENERATION] Sprawdzanie ostatniej daty w bazie generacji...")
    try:
        with Session(engine) as session:
            stmt = select(func.max(GenerationBySource.business_date))
            last_date = session.exec(stmt).first()
            print(f"[GENERATION] Ostatnia data w bazie: {last_date}")

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
                print("[GENERATION] Baza aktualna.")
                return

            print(f"[GENERATION] Rozpoczęcie pobierania od {start} do {today.isoformat()}")
            pobierz_dane_generation(engine, start, today.isoformat())

    except Exception as e:
        print(f"[GENERATION] Błąd uzupełniania: {e}")
        import traceback
        traceback.print_exc()