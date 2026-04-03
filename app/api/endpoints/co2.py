"""
Endpoint for CO2 price data operations.
"""
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, select, func, create_engine

from app.core.config import settings
from app.controllers import co2_controller
from app.models import Co2Price

router = APIRouter(tags=["co2"])

# Create engine for data operations
engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True
)


def fetch_co2_task():
    """Background task for fetching CO2 data."""
    try:
        print("[CO2 API] Starting CO2 data fetch...")
        co2_controller.uzupelnij_brakujace_dane_co2(engine)
        print("[CO2 API] CO2 data fetch completed.")
    except Exception as e:
        print(f"[CO2 API] Error: {e}")


@router.post("/fetch-co2")
def fetch_co2_data(background_tasks: BackgroundTasks):
    """
    Fetch missing CO2 price data up to current date.
    Updates database with latest available CO2 price data from PSE API.
    """
    background_tasks.add_task(fetch_co2_task)
    
    return {
        "status": "success",
        "message": "Started fetching CO2 data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "data_source": "PSE RCCO2 API"
    }


@router.post("/fetch-co2-range")
def fetch_co2_range(
    background_tasks: BackgroundTasks,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date (YYYY-MM-DD), if not provided uses today")
):
    """
    Fetch CO2 price data for specific date range.
    """
    def fetch_range_task():
        try:
            print(f"[CO2 API] Fetching CO2 data from {date_from} to {date_to}...")
            co2_controller.pobierz_dane_co2_i_wyslij_do_bazy(engine, date_from, date_to)
            print("[CO2 API] Range fetch completed.")
        except Exception as e:
            print(f"[CO2 API] Range fetch error: {e}")
    
    background_tasks.add_task(fetch_range_task)
    
    return {
        "status": "success", 
        "message": f"Started fetching CO2 data from {date_from} to {date_to or 'today'} in background.",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/co2/latest")
def get_latest_co2():
    """Get the latest CO2 record date and count."""
    try:
        with Session(engine) as session:
            latest_date = session.exec(select(func.max(Co2Price.doba))).one()
            count = session.exec(select(func.count(Co2Price.id))).one()
            
        return {
            "latest_date": latest_date,
            "total_records": count,
            "table": "co2_prices"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/co2/stats")
def get_co2_stats():
    """Get CO2 price statistics."""
    try:
        with Session(engine) as session:
            stats = session.exec(
                select(
                    func.count(Co2Price.id).label('count'),
                    func.min(Co2Price.doba).label('min_date'),
                    func.max(Co2Price.doba).label('max_date'),
                    func.avg(Co2Price.cena_euro_mwh).label('avg_price'),
                    func.max(Co2Price.cena_euro_mwh).label('max_price'),
                    func.min(Co2Price.cena_euro_mwh).label('min_price')
                )
            ).first()
            
        return {
            "table": "co2_prices",
            "record_count": stats.count,
            "date_range": {
                "from": stats.min_date,
                "to": stats.max_date
            },
            "price_statistics_eur_mwh": {
                "average": float(stats.avg_price) if stats.avg_price else None,
                "maximum": float(stats.max_price) if stats.max_price else None,
                "minimum": float(stats.min_price) if stats.min_price else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))