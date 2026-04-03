"""Controller for demand (zapotrzebowanie mocy KSE) data fetching."""

from datetime import datetime, timedelta
from typing import Any

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, func, select

from app.models import ZapotrzebowanieMocyKSE

BATCH_SIZE = 1000


def flush_demand_buffer_to_db(engine, buffer: list[dict[str, Any]]):
    """Zapisuje bufor zapotrzebowania do bazy."""
    if not buffer:
        return

    print(f"[Demand]  Zapisywanie {len(buffer)} rekordów...")

    try:
        with Session(engine) as session:
            statement = insert(ZapotrzebowanieMocyKSE).values(buffer)
            statement = statement.on_conflict_do_nothing(
                index_elements=["doba", "udtczas"]
            )

            session.exec(statement)
            session.commit()

            print("[Demand] Zapis zakończony.")

    except Exception as e:
        print(f"[Demand]  Błąd zapisu: {e}")
        import traceback

        traceback.print_exc()


def pobierz_demand_kse_i_wyslij_do_bazy(engine, data_od: str, data_do: str = None):
    """Pobiera zapotrzebowanie mocy KSE z PSE API."""
    if data_do is None:
        data_do = datetime.now().date().isoformat()
    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    start_date = datetime.fromisoformat(data_od).date()
    end_date = datetime.fromisoformat(data_do).date()
    current_date = start_date

    buffer: list[dict[str, Any]] = []

    print(f"[Demand] Pobieranie zapotrzebowania od {data_od} do {data_do}...")

    while current_date <= end_date:
        date_str = current_date.isoformat()
        url = f"https://api.raporty.pse.pl/api/kse-load?$filter=business_date eq '{date_str}'"
        print(f"[Demand] Pobieranie danych dla {date_str}...")
        print(f"[Demand] URL: {url}")

        try:
            response = requests.get(url, headers={"Accept": "application/json"})

            if response.status_code != 200:
                print(f"[Demand]  Błąd {response.status_code}: {response.reason}")
            else:
                data = response.json()
                items = data.get("value", [])
                print(f"[Demand]  Pobrano {len(items)} rekordów z API.")
                if items:
                    print(f"[Demand]  Przykładowy rekord: {items[0]}")

                for item in items:
                    dtime_value = datetime.fromisoformat(item["dtime"])
                    if dtime_value > now_cutoff:
                        continue

                    buffer.append(
                        {
                            "doba": dtime_value,
                            "udtczas": dtime_value,  # same as dtime for this API
                            "obciazenie": float(item["load_actual"]),
                            "business_date": datetime.fromisoformat(
                                item["business_date"]
                            ),
                            "source_datetime": datetime.fromisoformat(
                                item["publication_ts"]
                            ),
                        }
                    )
                print(f"[Demand]  Bufor zawiera teraz {len(buffer)} rekordów.")

        except Exception as error:
            print(f"[Demand]  Błąd przetwarzania {date_str}: {error}")

        if len(buffer) >= BATCH_SIZE:
            print(f"[Demand]  Zapisywanie {len(buffer)} rekordów do bazy...")
            flush_demand_buffer_to_db(engine, buffer)
            buffer = []

        current_date += timedelta(days=1)

    if buffer:
        print(f"[Demand]  Zapisywanie końcowych {len(buffer)} rekordów do bazy...")
        flush_demand_buffer_to_db(engine, buffer)

    print("[Demand] Zakończono pobieranie zapotrzebowania.")


def uzupelnij_brakujace_demand_kse(engine, start_date: str | None = None):
    """Uzupełnia brakujące dane zapotrzebowania."""
    print("[Demand] Sprawdzanie ostatniej daty w bazie...")
    try:
        with Session(engine) as session:
            statement = select(func.max(ZapotrzebowanieMocyKSE.business_date))
            last_date_raw = session.exec(statement).first()
            print(f"[Demand] Ostatnia data w bazie: {last_date_raw}")

            today = datetime.now().date()
            start_date_str = "2025-03-03"

            if last_date_raw:
                if isinstance(last_date_raw, datetime):
                    last_date = last_date_raw.date()
                else:
                    last_date = last_date_raw

                start_date_str = last_date.isoformat()

            if start_date:
                start_date_str = max(start_date_str, start_date)

            if start_date_str > today.isoformat():
                print("[Demand] Wszystkie dane aktualne.")
                return

            print(
                f"[Demand] Rozpoczęcie pobierania od {start_date_str} do {today.isoformat()}"
            )
            pobierz_demand_kse_i_wyslij_do_bazy(
                engine, start_date_str, today.isoformat()
            )

    except Exception as e:
        print(f"[Demand] Błąd uzupełniania danych: {e}")
        import traceback

        traceback.print_exc()
