"""
Endpoint for CRB (balancing settlements) data operations.
"""
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, select, func, create_engine

from app.core.config import settings
from app.controllers import crb_controller
from app.models import CrbRozliczenia

router = APIRouter(tags=["crb-balancing"])

# Create engine for data operations
engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True
)


def fetch_crb_task():
    """Background task for fetching CRB data."""
    try:
        print("[CRB API] Starting CRB balancing data fetch...")
        crb_controller.uzupelnij_crb(engine)
        print("[CRB API] CRB balancing data fetch completed.")
    except Exception as e:
        print(f"[CRB API] Error: {e}")


@router.post("/fetch-crb")
def fetch_crb_data(background_tasks: BackgroundTasks):
    """
    Fetch missing CRB (balancing settlements) data up to current date.
    Updates database with latest available CRB data from PSE API.
    """
    background_tasks.add_task(fetch_crb_task)
    
    return {
        "status": "success",
        "message": "Started fetching CRB balancing data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "data_source": "PSE CRB Balancing API"
    }


@router.post("/fetch-crb-range")
def fetch_crb_range(
    background_tasks: BackgroundTasks,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date (YYYY-MM-DD), if not provided uses today")
):
    """
    Fetch CRB data for specific date range.
    """
    def fetch_range_task():
        try:
            print(f"[CRB API] Fetching CRB data from {date_from} to {date_to}...")
            crb_controller.pobierz_dane_crb(engine, date_from, date_to)
            print("[CRB API] Range fetch completed.")
        except Exception as e:
            print(f"[CRB API] Range fetch error: {e}")
    
    background_tasks.add_task(fetch_range_task)
    
    return {
        "status": "success", 
        "message": f"Started fetching CRB data from {date_from} to {date_to or 'today'} in background.",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/crb/latest")
def get_latest_crb():
    """Get the latest CRB record date and count."""
    try:
        with Session(engine) as session:
            latest_date = session.exec(select(func.max(CrbRozliczenia.doba))).one()
            count = session.exec(select(func.count(CrbRozliczenia.id))).one()
            
        return {
            "latest_date": latest_date,
            "total_records": count,
            "table": "crb_rozliczenia"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/crb/stats")
def get_crb_stats():
    """Get CRB balancing statistics."""
    try:
        with Session(engine) as session:
            stats = session.exec(
                select(
                    func.count(CrbRozliczenia.id).label('count'),
                    func.min(CrbRozliczenia.doba).label('min_date'),
                    func.max(CrbRozliczenia.doba).label('max_date'),
                    func.avg(CrbRozliczenia.crb_rozl).label('avg_crb'),
                    func.max(CrbRozliczenia.crb_rozl).label('max_crb'),
                    func.min(CrbRozliczenia.crb_rozl).label('min_crb')
                )
            ).first()
            
        return {
            "table": "crb_rozliczenia",
            "record_count": stats.count,
            "date_range": {
                "from": stats.min_date,
                "to": stats.max_date
            },
            "crb_statistics": {
                "average": float(stats.avg_crb) if stats.avg_crb else None,
                "maximum": float(stats.max_crb) if stats.max_crb else None,
                "minimum": float(stats.min_crb) if stats.min_crb else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))