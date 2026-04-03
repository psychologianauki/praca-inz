"""
Endpoint for power losses data operations.
"""
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, select, func, create_engine

from app.core.config import settings
from app.controllers import ubytki_controller
from app.models import UbytkiMocyJednostek

router = APIRouter(tags=["power-losses"])

# Create engine for data operations
engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True
)


def fetch_power_losses_task():
    """Background task for fetching power losses data."""
    try:
        print("[Power Losses API] Starting power losses data fetch...")
        ubytki_controller.uzupelnij_brakujace_dane_ubytki(engine)
        print("[Power Losses API] Power losses data fetch completed.")
    except Exception as e:
        print(f"[Power Losses API] Error: {e}")


@router.post("/fetch-power-losses")
def fetch_power_losses_data(background_tasks: BackgroundTasks):
    """
    Fetch missing power losses data up to current date.
    Updates database with latest available power losses data from PSE API.
    """
    background_tasks.add_task(fetch_power_losses_task)
    
    return {
        "status": "success",
        "message": "Started fetching power losses data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "data_source": "PSE Power Losses API"
    }


@router.post("/fetch-power-losses-range")
def fetch_power_losses_range(
    background_tasks: BackgroundTasks,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date (YYYY-MM-DD), if not provided uses today")
):
    """
    Fetch power losses data for specific date range.
    """
    def fetch_range_task():
        try:
            print(f"[Power Losses API] Fetching power losses data from {date_from} to {date_to}...")
            ubytki_controller.pobierz_dane_ubytki_i_wyslij_do_bazy(engine, date_from, date_to)
            print("[Power Losses API] Range fetch completed.")
        except Exception as e:
            print(f"[Power Losses API] Range fetch error: {e}")
    
    background_tasks.add_task(fetch_range_task)
    
    return {
        "status": "success", 
        "message": f"Started fetching power losses data from {date_from} to {date_to or 'today'} in background.",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/power-losses/latest")
def get_latest_power_losses():
    """Get the latest power losses record date and count."""
    try:
        with Session(engine) as session:
            latest_date = session.exec(select(func.max(UbytkiMocyJednostek.business_date))).one()
            count = session.exec(select(func.count(UbytkiMocyJednostek.id))).one()
            
        return {
            "latest_date": latest_date,
            "total_records": count,
            "table": "unit_power_losses"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/power-losses/stats")
def get_power_losses_stats():
    """Get power losses statistics."""
    try:
        with Session(engine) as session:
            stats = session.exec(
                select(
                    func.count(UbytkiMocyJednostek.id).label('count'),
                    func.min(UbytkiMocyJednostek.business_date).label('min_date'),
                    func.max(UbytkiMocyJednostek.business_date).label('max_date'),
                    func.count(func.distinct(UbytkiMocyJednostek.nazwa_jednostki)).label('unique_units')
                )
            ).first()
            
        return {
            "table": "unit_power_losses",
            "record_count": stats.count,
            "date_range": {
                "from": stats.min_date,
                "to": stats.max_date
            },
            "unique_power_units": stats.unique_units
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))