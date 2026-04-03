"""Controller for Intraday Trading Volume data fetching."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import time

from sqlmodel import Session, select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import IntradayTradingVolume


BATCH_SIZE = 2000


def flush_intraday_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor danych obrotu USE do bazy."""
    if not buffer:
        return

    print(f"[INTRADAY] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            stmt = insert(IntradayTradingVolume).values(buffer)
            stmt = stmt.on_conflict_do_update(
                index_elements=["dtime", "market_type"],
                set_={
                    "day_ahead_tr_vol": stmt.excluded.day_ahead_tr_vol,
                    "sprz_volume": stmt.excluded.sprz_volume,
                    "publication_ts": stmt.excluded.publication_ts,
                    "publication_ts_utc": stmt.excluded.publication_ts_utc,
                }
            )
            session.exec(stmt)
            session.commit()

            print("[INTRADAY] Zapis zakończony.")

    except Exception as e:
        print(f"[INTRADAY]  Błąd zapisu: {e}")
        import traceback
        traceback.print_exc()


def pobierz_dane_intraday(engine, data_od: str, data_do: str = None):
    """Pobiera dane obrotu USE z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()

    buffer = []

    print(f"[INTRADAY] Pobieranie danych: {data_od} → {data_do}")

    current = start_date
    while current <= end_date:
        dstr = current.isoformat()
        # Używamy endpointu do obrotu USE - sprawdzimy oba warianty
        urls = [
            f"https://api.raporty.pse.pl/api/use-sprz-rbn?$filter=business_date%20eq%20'{dstr}'",
            f"https://api.raporty.pse.pl/api/use-sprz-rbb?$filter=business_date%20eq%20'{dstr}'"
        ]
        
        for url_idx, url in enumerate(urls):
            endpoint_name = "RBN" if url_idx == 0 else "RBB"
            print(f"[INTRADAY] Pobieranie danych {endpoint_name} dla {dstr}...")
            print(f"[INTRADAY] URL: {url}")

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
                            print(f"[INTRADAY]  Błąd {dstr} ({endpoint_name}): {r.status_code} - {r.reason}")
                            if attempt < max_retries - 1:
                                print(f"[INTRADAY]  Ponowienie próby za {retry_delay}s... (próba {attempt + 1}/{max_retries})")
                                time.sleep(retry_delay)
                                continue
                        else:
                            items = r.json().get("value", [])
                            print(f"[INTRADAY]  Pobrano {len(items)} rekordów z API ({endpoint_name}).")
                            if items:
                                print(f"[INTRADAY]  Przykładowy rekord: {items[0]}")

                            for item in items:
                                market_type = endpoint_name  # "RBN" lub "RBB"
                                dtime_value = datetime.fromisoformat(item["dtime"].replace(" ", "T")) if item.get("dtime") else None
                                if dtime_value and dtime_value > now_cutoff:
                                    continue
                                
                                buffer.append({
                                    "dtime": dtime_value,
                                    "dtime_utc": datetime.fromisoformat(item["dtime_utc"].replace(" ", "T")) if item.get("dtime_utc") else None,
                                    "business_date": datetime.fromisoformat(item["business_date"]),
                                    
                                    # Typ rynku
                                    "market_type": market_type,
                                    
                                    # Dane obrotu - różne pola w zależności od endpointu
                                    "day_ahead_tr_vol": float(item.get("day_ahead_tr_vol", 0)) if market_type == "RBN" and item.get("day_ahead_tr_vol") is not None else None,
                                    "sprz_volume": float(item.get("sprz_volume", 0)) if market_type == "RBB" and item.get("sprz_volume") is not None else None,
                                    
                                    # Znaczniki publikacji
                                    "publication_ts": datetime.fromisoformat(item["publication_ts"].replace(" ", "T")) if item.get("publication_ts") else None,
                                    "publication_ts_utc": datetime.fromisoformat(item["publication_ts_utc"].replace(" ", "T")) if item.get("publication_ts_utc") else None,
                                })  
                            print(f"[INTRADAY]  Bufor zawiera teraz {len(buffer)} rekordów.")
                            success = True
                            break
                            
                    except requests.exceptions.RequestException as req_err:
                        print(f"[INTRADAY] Błąd połączenia ({endpoint_name}, próba {attempt + 1}): {req_err}")
                        if attempt < max_retries - 1:
                            print(f"[INTRADAY]  Ponowienie próby za {retry_delay}s...")
                            time.sleep(retry_delay)
                        else:
                            print(f"[INTRADAY]  Wszystkie próby nieudane dla {endpoint_name} daty {dstr}")
                            
                if not success:
                    print(f"[INTRADAY]  Pomijanie {endpoint_name} dla daty {dstr} z powodu błędów")

            except Exception as err:
                print(f"[INTRADAY] ERROR ({endpoint_name}): {err}")

            if len(buffer) >= BATCH_SIZE:
                print(f"[INTRADAY]  Zapisywanie {len(buffer)} rekordów do bazy...")
                flush_intraday_buffer_to_db(engine, buffer)
                buffer = []

        current += timedelta(days=1)

    if buffer:
        print(f"[INTRADAY]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_intraday_buffer_to_db(engine, buffer)

    print("[INTRADAY] Zakończono pobieranie.")


def uzupelnij_intraday(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane obrotu USE."""
    print("[INTRADAY] Sprawdzanie ostatniej daty w bazie USE...")
    try:
        with Session(engine) as session:
            stmt = select(func.max(IntradayTradingVolume.business_date))
            last_date = session.exec(stmt).first()
            print(f"[INTRADAY] Ostatnia data w bazie: {last_date}")

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
                print("[INTRADAY] Baza aktualna.")
                return

            print(f"[INTRADAY] Rozpoczęcie pobierania od {start} do {today.isoformat()}")
            pobierz_dane_intraday(engine, start, today.isoformat())

    except Exception as e:
        print(f"[INTRADAY] Błąd uzupełniania: {e}")
        import traceback
        traceback.print_exc()