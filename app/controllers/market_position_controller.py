"""Controller for Aggregated Market Position data fetching."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

from sqlmodel import Session, select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import AggregatedMarketPosition


BATCH_SIZE = 2000


def flush_market_position_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor danych pozycji rynkowej do bazy."""
    if not buffer:
        return

    print(f"[MARKET] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            stmt = insert(AggregatedMarketPosition).values(buffer)
            stmt = stmt.on_conflict_do_nothing(index_elements=["dtime", "period"])
            session.exec(stmt)
            session.commit()

            print("[MARKET] Zapis zakończony.")

    except Exception as e:
        print(f"[MARKET]  Błąd zapisu: {e}")
        import traceback
        traceback.print_exc()


def pobierz_dane_market_position(engine, data_od: str, data_do: str = None):
    """Pobiera dane pozycji rynkowej z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()

    buffer = []

    print(f"[MARKET] Pobieranie danych: {data_od} → {data_do}")

    current = start_date
    while current <= end_date:
        dstr = current.isoformat()
        url = f"https://api.raporty.pse.pl/api/sk?$filter=business_date%20eq%20'{dstr}'"
        print(f"[MARKET] Pobieranie danych dla {dstr}...")
        print(f"[MARKET] URL: {url}")

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
                        print(f"[MARKET]  Błąd {dstr}: {r.status_code} - {r.reason}")
                        if attempt < max_retries - 1:
                            print(f"[MARKET]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                    else:
                        items = r.json().get("value", [])
                        print(f"[MARKET]  Pobrano {len(items)} rekordów z API.")
                        if items:
                            print(f"[MARKET]  Przykładowy rekord: {items[0]}")

                        for item in items:
                            dtime_value = datetime.fromisoformat(item["dtime"].replace(" ", "T")) if item.get("dtime") else None
                            if dtime_value and dtime_value > now_cutoff:
                                continue

                            sk_cost_raw = item.get("sk_cost")
                            sk_cost_value = float(sk_cost_raw) if sk_cost_raw is not None else 0.0

                            buffer.append({
                                "dtime": dtime_value,
                                "dtime_utc": datetime.fromisoformat(item["dtime_utc"].replace(" ", "T")) if item.get("dtime_utc") else None,
                                "period": item.get("period"),
                                "period_utc": item.get("period_utc"),
                                "business_date": datetime.fromisoformat(item["business_date"]),
                                
                                # Pozycja rynkowa
                                "sk_cost": sk_cost_value,
                                "sk_d1_fcst": float(item.get("sk_d1_fcst", 0)) if item.get("sk_d1_fcst") is not None else None,
                                "sk_d_fcst": float(item.get("sk_d_fcst", 0)) if item.get("sk_d_fcst") is not None else None,
                                "contracting_status": "long" if sk_cost_value > 0 else "short" if sk_cost_value < 0 else "",
                                
                                # Znaczniki publikacji
                                "publication_ts": datetime.fromisoformat(item["publication_ts"].replace(" ", "T")) if item.get("publication_ts") else None,
                                "publication_ts_utc": datetime.fromisoformat(item["publication_ts_utc"].replace(" ", "T")) if item.get("publication_ts_utc") else None,
                            })  
                        print(f"[MARKET]  Bufor zawiera teraz {len(buffer)} rekordów.")
                        success = True
                        break
                        
                except requests.exceptions.RequestException as req_err:
                    print(f"[MARKET] Błąd połączenia (próba {attempt + 1}): {req_err}")
                    if attempt < max_retries - 1:
                        print(f"[MARKET]  Ponowienie próby za {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        print(f"[MARKET]  Wszystkie próby nieudane dla daty {dstr}")
                        
            if not success:
                print(f"[MARKET]  Pomijanie daty {dstr} z powodu błędów")

        except Exception as err:
            print(f"[MARKET] ERROR: {err}")

        if len(buffer) >= BATCH_SIZE:
            print(f"[MARKET]  Zapisywanie {len(buffer)} rekordów do bazy...")
            flush_market_position_buffer_to_db(engine, buffer)
            buffer = []

        current += timedelta(days=1)

    if buffer:
        print(f"[MARKET]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_market_position_buffer_to_db(engine, buffer)

    print("[MARKET] Zakończono pobieranie.")


def uzupelnij_market_position(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane pozycji rynkowej."""
    print("[MARKET] Sprawdzanie ostatniej daty w bazie pozycji rynkowej...")
    try:
        with Session(engine) as session:
            stmt = select(func.max(AggregatedMarketPosition.business_date))
            last_date = session.exec(stmt).first()
            print(f"[MARKET] Ostatnia data w bazie: {last_date}", flush=True)

            today = datetime.now().date()
            start = "2025-03-03"
            print(f"[MARKET] Dzisiejsza data: {today.isoformat()}", flush=True)
            if last_date:
                if isinstance(last_date, datetime):
                    start = last_date.date().isoformat()
                else:
                    start = last_date.isoformat()

            if start_date:
                start = max(start, start_date)

            if start > today.isoformat():
                print("[MARKET] Baza aktualna.")
                return

            print(f"[MARKET] Rozpoczęcie pobierania od {start} do {today.isoformat()}")
            pobierz_dane_market_position(engine, start, today.isoformat())

    except Exception as e:
        print(f"[MARKET] Błąd uzupełniania: {e}")
        import traceback
        traceback.print_exc()