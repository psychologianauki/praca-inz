"""
Endpoint for demand (KSE load) data operations.
"""
from datetime import datetime, date
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, select, func, create_engine

from app.core.config import settings
from app.controllers import demand_controller
from app.models import ZapotrzebowanieMocyKSE

router = APIRouter(tags=["demand"])

# Create engine for data operations
engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI),
    pool_pre_ping=True
)


def fetch_demand_task():
    """Background task for fetching demand data."""
    try:
        print("[Demand API] Starting demand data fetch...")
        demand_controller.uzupelnij_brakujace_demand_kse(engine)
        print("[Demand API] Demand data fetch completed.")
    except Exception as e:
        print(f"[Demand API] Error: {e}")


@router.post("/fetch-demand")
def fetch_demand_data(background_tasks: BackgroundTasks):
    """
    Fetch missing demand (KSE load) data up to current date.
    Updates database with latest available demand data from PSE API.
    """
    background_tasks.add_task(fetch_demand_task)
    
    return {
        "status": "success",
        "message": "Started fetching demand data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "data_source": "PSE KSE Load API"
    }


@router.post("/fetch-demand-range")
def fetch_demand_range(
    background_tasks: BackgroundTasks,
    date_from: str = Query(..., description="Start date (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date (YYYY-MM-DD), if not provided uses today")
):
    """
    Fetch demand data for specific date range.
    """
    def fetch_range_task():
        try:
            print(f"[Demand API] Fetching demand data from {date_from} to {date_to}...")
            demand_controller.pobierz_demand_kse_i_wyslij_do_bazy(engine, date_from, date_to)
            print("[Demand API] Range fetch completed.")
        except Exception as e:
            print(f"[Demand API] Range fetch error: {e}")
    
    background_tasks.add_task(fetch_range_task)
    
    return {
        "status": "success", 
        "message": f"Started fetching demand data from {date_from} to {date_to or 'today'} in background.",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/demand/latest")
def get_latest_demand():
    """Get the latest demand record date and count."""
    try:
        with Session(engine) as session:
            latest_date = session.exec(select(func.max(ZapotrzebowanieMocyKSE.doba))).one()
            count = session.exec(select(func.count(ZapotrzebowanieMocyKSE.id))).one()
            
        return {
            "latest_date": latest_date,
            "total_records": count,
            "table": "demand_kse"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/demand/stats")
def get_demand_stats():
    """Get demand data statistics."""
    try:
        with Session(engine) as session:
            stats = session.exec(
                select(
                    func.count(ZapotrzebowanieMocyKSE.id).label('count'),
                    func.min(ZapotrzebowanieMocyKSE.doba).label('min_date'),
                    func.max(ZapotrzebowanieMocyKSE.doba).label('max_date'),
                    func.avg(ZapotrzebowanieMocyKSE.obciazenie).label('avg_load'),
                    func.max(ZapotrzebowanieMocyKSE.obciazenie).label('max_load'),
                    func.min(ZapotrzebowanieMocyKSE.obciazenie).label('min_load')
                )
            ).first()
            
        return {
            "table": "demand_kse",
            "record_count": stats.count,
            "date_range": {
                "from": stats.min_date,
                "to": stats.max_date
            },
            "load_statistics": {
                "average": float(stats.avg_load) if stats.avg_load else None,
                "maximum": float(stats.max_load) if stats.max_load else None,
                "minimum": float(stats.min_load) if stats.min_load else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))