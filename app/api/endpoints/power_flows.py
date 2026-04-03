"""
Endpoint for power flows data operations.
"""
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, select, func, create_engine

from app.core.config import settings
from app.controllers import przeplyw_controller
from app.models import PrzeplywMocyJednostek

router = APIRouter(tags=["power-flows"])

# Create engine for data operations
engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True
)


def fetch_power_flows_task():
    """Background task for fetching power flows data."""
    try:
        print("[Power Flows API] Starting power flows data fetch...")
        przeplyw_controller.uzupelnij_brakujace_dane_przeplyw(engine)
        print("[Power Flows API] Power flows data fetch completed.")
    except Exception as e:
        print(f"[Power Flows API] Error: {e}")


@router.post("/fetch-power-flows")
def fetch_power_flows_data(background_tasks: BackgroundTasks):
    """
    Fetch missing power flows data up to current date.
    Updates database with latest available power flows data from PSE API.
    """
    background_tasks.add_task(fetch_power_flows_task)
    
    return {
        "status": "success",
        "message": "Started fetching power flows data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "data_source": "PSE Power Flows API"
    }


@router.post("/fetch-power-flows-range")
def fetch_power_flows_range(
    background_tasks: BackgroundTasks,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date (YYYY-MM-DD), if not provided uses today")
):
    """
    Fetch power flows data for specific date range.
    """
    def fetch_range_task():
        try:
            print(f"[Power Flows API] Fetching power flows data from {date_from} to {date_to}...")
            przeplyw_controller.pobierz_dane_przeplyw_i_wyslij_do_bazy(engine, date_from, date_to)
            print("[Power Flows API] Range fetch completed.")
        except Exception as e:
            print(f"[Power Flows API] Range fetch error: {e}")
    
    background_tasks.add_task(fetch_range_task)
    
    return {
        "status": "success", 
        "message": f"Started fetching power flows data from {date_from} to {date_to or 'today'} in background.",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/power-flows/latest")
def get_latest_power_flows():
    """Get the latest power flows record date and count."""
    try:
        with Session(engine) as session:
            latest_date = session.exec(select(func.max(PrzeplywMocyJednostek.business_date))).one()
            count = session.exec(select(func.count(PrzeplywMocyJednostek.id))).one()
            
        return {
            "latest_date": latest_date,
            "total_records": count,
            "table": "unit_power_flows"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/power-flows/stats")
def get_power_flows_stats():
    """Get power flows statistics."""
    try:
        with Session(engine) as session:
            stats = session.exec(
                select(
                    func.count(PrzeplywMocyJednostek.id).label('count'),
                    func.min(PrzeplywMocyJednostek.business_date).label('min_date'),
                    func.max(PrzeplywMocyJednostek.business_date).label('max_date'),
                    func.count(func.distinct(PrzeplywMocyJednostek.nazwa_przekroju)).label('unique_sections')
                )
            ).first()
            
        return {
            "table": "unit_power_flows",
            "record_count": stats.count,
            "date_range": {
                "from": stats.min_date,
                "to": stats.max_date
            },
            "unique_power_sections": stats.unique_sections
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))