"""Controller for power losses (ubytki mocy) data fetching."""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any

from sqlmodel import Session, select, func
from sqlalchemy.dialects.postgresql import insert

from app.models import UbytkiMocyJednostek


BATCH_SIZE = 1000


def flush_ubytki_buffer_to_db(engine, buffer: List[Dict[str, Any]]):
    """Zapisuje bufor ubytków do bazy."""
    if not buffer:
        return

    print(f"[Ubytki] Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            statement = insert(UbytkiMocyJednostek).values(buffer)
            
            session.exec(statement)
            session.commit()
            
            print("[Ubytki] Zapisano pomyślnie.")
            
    except Exception as e:
        print(f"[Ubytki]  Błąd zapisu: {e}")


def pobierz_dane_ubytki_i_wyslij_do_bazy(engine, data_od: str, data_do: str = None, last_dtime: datetime | None = None):
    """Pobiera ubytki mocy jednostek z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(str(data_od)).date()
    end_date = datetime.fromisoformat(str(data_do)).date()
    current_date = start_date

    records_buffer: List[Dict[str, Any]] = []

    print(f"[Ubytki] Pobieram dane od {data_od} do {data_do}")

    while current_date <= end_date:
        date_string = current_date.isoformat()
        url = f"https://api.raporty.pse.pl/api/pdwkseub?$filter=business_date%20eq%20'{date_string}'"
        print(f"[Ubytki] Pobieranie danych dla {date_string}...")
        
        try:
            response = requests.get(
                url,
                headers={"Accept": "application/json", "User-Agent": "PSE-Data-Fetcher/1.0"},
                timeout=30,
            )
            
            if response.status_code != 200:
                print(f"[Ubytki]  Błąd API dla daty {date_string}: {response.reason}")
            else:
                data = response.json()
                items = data.get("value", [])
                print(f"[Ubytki]  Pobrano {len(items)} rekordów z API.")
                if items:
                    print(f"[Ubytki]  Przykładowy rekord: {items[0]}")

                if items and isinstance(items, list):
                    for item in items:
                        dtime_value = datetime.fromisoformat(item["dtime"].replace(" ", "T"))
                        if dtime_value > now_cutoff:
                            continue
                        if last_dtime and dtime_value <= last_dtime:
                            continue

                        records_buffer.append({
                            "resource_code": item["resource_code"],
                            "power_plant": item["power_plant"],
                            "dtime": dtime_value,
                            "dtime_utc": datetime.fromisoformat(item["dtime_utc"].replace(" ", "T")),
                            "period": item["period"],
                            "period_utc": item["period_utc"],
                            "business_date": datetime.fromisoformat(item["business_date"]),
                            "grid_lim": float(item["grid_lim"]),
                            "non_us_cap": float(item["non_us_cap"]),
                            "available_capacity": float(item["available_capacity"]),
                            "publication_ts": datetime.fromisoformat(item["publication_ts"].replace(" ", "T")),
                            "publication_ts_utc": datetime.fromisoformat(item["publication_ts_utc"].replace(" ", "T")),
                        })
                    print(f"[Ubytki]  Bufor zawiera teraz {len(records_buffer)} rekordów.")

        except Exception as error:
            print(f"[Ubytki] Błąd przetwarzania dnia {date_string}: {error}")
        
        if len(records_buffer) >= BATCH_SIZE:
            flush_ubytki_buffer_to_db(engine, records_buffer)
            records_buffer = []

        current_date += timedelta(days=1)

    if records_buffer:
        flush_ubytki_buffer_to_db(engine, records_buffer)

    print("[Ubytki] Koniec pobierania.")


def uzupelnij_brakujace_dane_ubytki(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane ubytków."""
    print("[Ubytki] Sprawdzanie ostatniej daty w bazie ubytków...")
    try:
        with Session(engine) as session:
            statement = select(func.max(UbytkiMocyJednostek.business_date))
            last_entry_date = session.exec(statement).first()
            last_dtime = session.exec(select(func.max(UbytkiMocyJednostek.dtime))).first()
            print(f"[Ubytki] Ostatnia data w bazie: {last_entry_date}")
            print(f"[Ubytki] Ostatni znacznik czasu w bazie: {last_dtime}")

            start_date_str: str
            today_str = datetime.now().date().isoformat()

            if not last_entry_date:
                start_date_str = "2025-03-03"
            else:
                if isinstance(last_entry_date, datetime):
                    last_date = last_entry_date.date()
                else:
                    last_date = last_entry_date 
                
                start_date_str = last_date.isoformat()

            if start_date:
                start_date_str = max(start_date_str, start_date)

            if start_date_str > today_str:
                print("[Ubytki] Baza jest aktualna.")
                return

            print(f"[Ubytki] Rozpoczęcie pobierania od {start_date_str} do {today_str}")
            pobierz_dane_ubytki_i_wyslij_do_bazy(engine, start_date_str, today_str, last_dtime)

    except Exception as e:
        print(f"[Ubytki] Wystąpił błąd: {e}")
