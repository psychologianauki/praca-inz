import requests
from datetime import date, datetime, timedelta
from typing import List, Dict, Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from sqlmodel import Session, select, func, create_engine
from sqlalchemy.dialects.postgresql import insert

from app.core.config import settings
from app.models import EnergyPrice

# Engine używany przez importer
import_engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True # Warto dodać, żeby odświeżać połączenie
)

BATCH_SIZE = 2000

router = APIRouter(tags=["data"])

# --- FUNKCJE LOGIKI BIZNESOWEJ (Helpery) ---

def flush_buffer_to_db(buffer: List[Dict[str, Any]]):
    """Zapisuje bufor do bazy.

    Dla Postgresa używamy ON CONFLICT DO NOTHING na unikalnym kluczu (doba, godzina).
    """
    if not buffer:
        return

    print(f"[Import] Trwa zapisywanie {len(buffer)} rekordów do bazy...")

    try:
        with Session(import_engine) as session:
            statement = (
                insert(EnergyPrice)
                .values(buffer)
                .on_conflict_do_nothing(index_elements=["doba", "godzina"])
            )
            session.exec(statement)
            session.commit()
            print("[Import] Zapisano pomyślnie rekordy.")
    except Exception as e:
        print(f"[Import Error] Błąd krytyczny podczas zapisu: {e}")

def process_dates_range(data_od: str, data_do: str):
    """Logika pętli pobierającej dane z API PSE."""
    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()
    current_date = start_date

    records_buffer: List[Dict[str, Any]] = []

    print(f"[Import] Rozpoczynam pobieranie od {data_od} do {data_do}...")

    while current_date <= end_date:
        date_string = current_date.isoformat()
        url = f"https://api.raporty.pse.pl/api/rce-pln?$filter=doba eq {date_string}"

        try:
            # Timeout jest ważny, żeby nie wisiało w nieskończoność
            response = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("value", [])

                if items:
                    for item in items:
                        start_time = item["udtczas_oreb"].split(" - ")[0]
                        if start_time.endswith(":00"):
                            records_buffer.append({
                                "doba": datetime.fromisoformat(item["doba"]),
                                "cena_mwh": float(item["rce_pln"]),
                                "godzina": item["udtczas_oreb"],
                                "business_date": datetime.fromisoformat(item["business_date"]),
                                "source_datetime": datetime.fromisoformat(item["source_datetime"]),
                            })
                else:
                    print(f"[Import Info] Brak danych w API dla daty {date_string}")
            else:
                print(f"[Import API Error] {response.status_code} dla {date_string}")

        except Exception as error:
            print(f"[Import Error] Błąd przetwarzania dnia {date_string}: {error}")

        # Zrzut bufora jeśli pełny
        if len(records_buffer) >= BATCH_SIZE:
            flush_buffer_to_db(records_buffer)
            records_buffer = [] 

        current_date += timedelta(days=1)

    # Zrzut resztek bufora
    if records_buffer:
        flush_buffer_to_db(records_buffer)
    
    print("[Import] Zakończono proces.")

def run_energy_price_import():
    """Główna funkcja uruchamiana w tle."""
    try:
        print("[Import] Uruchamianie zadania w tle...")
        with Session(import_engine) as session:
            # Sprawdzamy ostatnią datę
            statement = select(func.max(EnergyPrice.doba))
            last_entry_date = session.exec(statement).first()

            today_str = datetime.now().date().isoformat()
            start_date_str: str

            if not last_entry_date:
                print("[Import] Baza pusta. Start od 2025-01-01")
                start_date_str = "2026-03-03" 
            else:
                # Obsługa formatu daty (zależnie od sterownika może być str lub date)
                last_date = (
                    last_entry_date
                    if isinstance(last_entry_date, (date, datetime))
                    else datetime.fromisoformat(str(last_entry_date)).date()
                )
                
                next_day = last_date + timedelta(days=1)
                start_date_str = next_day.isoformat()

            if start_date_str > today_str:
                print("[Import] Baza jest aktualna. Brak działań.")
                return

            process_dates_range(start_date_str, today_str)

    except Exception as e:
        print(f"[Import Critical] Błąd w zadaniu w tle: {e}")


# --- ENDPOINT ---

@router.get("/fetch-energy-prices")
def fetch_energy_prices(background_tasks: BackgroundTasks):
    """
    Uruchamia proces aktualizacji cen energii (PSE) w tle.
    Sprawdza ostatnią datę w bazie i dociąga brakujące dni do dzisiaj.
    """
    # Przekazujemy funkcję do wykonania w tle (po zwróceniu odpowiedzi HTTP)
    background_tasks.add_task(run_energy_price_import)
    
    return {
        "status": "success",
        "message": "Rozpoczęto proces aktualizacji danych w tle. Sprawdź logi serwera."
    }


@router.get("/energy-prices/latest")
def energy_prices_latest(): 
    """Quick health/status endpoint for saved energy prices."""
    try:
        with Session(import_engine) as session:
            latest = session.exec(select(func.max(EnergyPrice.doba))).one()
        return {"latest_doba": latest}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))