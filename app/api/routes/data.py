"""
Data endpoints that include individual controllers and the combined fetch-all endpoint.
"""
from fastapi import APIRouter

from ..endpoints import (
    fetch_all, 
    energy, 
    weather_forecast,
    demand,
    co2,
    power_flows,
    power_losses,
    crb
)

router = APIRouter()

# Include all data endpoints
router.include_router(fetch_all.router, tags=["data-fetch"])
router.include_router(energy.router, tags=["energy"])
router.include_router(weather_forecast.router, tags=["weather"])
router.include_router(demand.router, tags=["demand"])
router.include_router(co2.router, tags=["co2"])
router.include_router(power_flows.router, tags=["power-flows"])
router.include_router(power_losses.router, tags=["power-losses"])
router.include_router(crb.router, tags=["crb-balancing"])