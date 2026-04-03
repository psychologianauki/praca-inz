"""
Endpoint for fetching all energy and weather data to current date.
This endpoint orchestrates all data fetching controllers.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlmodel import Session, create_engine, func, select

from app.controllers import (
    co2_controller,
    crb_controller,
    cross_border_controller,
    demand_controller,
    energy_price_controller,
    gas_controller,
    generation_controller,
    intraday_controller,
    market_position_controller,
    ml_feature_store_controller,
    oil_controller,
    sdac_controller,
    ubytki_controller,
    weather_forecast_controller,
)
from app.core.config import settings
from app.models import (
    AggregatedMarketPosition,
    Co2Price,
    CrbRozliczenia,
    CrossBorderFlows,
    EnergyPrice,
    GasPrices,
    GenerationBySource,
    IntradayTradingVolume,
    OilPrices,
    PrzeplywMocyJednostek,
    SDACPrices,
    UbytkiMocyJednostek,
    WeatherForecast,
    ZapotrzebowanieMocyKSE,
)

router = APIRouter(tags=["data"])

# Create engine for data fetching
engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI), pool_pre_ping=True)
FETCH_ALL_START_DATE = "2026-01-01"

SCHEDULER_STATE = {"enabled": False}


def _validate_start_date(start_date: str) -> str:
    try:
        date.fromisoformat(start_date)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail="Invalid start_date format. Use YYYY-MM-DD.",
        ) from e
    return start_date


def fetch_all_data_task(start_date: str | None = None):
    """
    Background task that fetches all available data to current date.
    This includes:
    - Energy prices (RCE-PLN)
    - Demand (zapotrzebowanie mocy KSE)
    - Power flows (przepływy mocy)
    - Power losses (ubytki mocy)
    - CO2 prices
    - CRB balancing data
    - Weather forecast
    - Generation by source (struktura generacji)
    - Cross-border flows (przepływy międzysystemowe)
    - Market position (pozycja rynkowa KSE)
    - SDAC prices (ceny SDAC)
    - Intraday trading volume (obrót USE)
    - Gas prices (ceny gazu)
    - Oil prices (ceny ropy naftowej Brent)
    """
    print("\n" + "=" * 60, flush=True)
    print(
        f"[FETCH ALL] Starting comprehensive data fetch at {datetime.now()}", flush=True
    )
    effective_start_date = start_date or FETCH_ALL_START_DATE
    print(f"[FETCH ALL] Global start_date: {effective_start_date}", flush=True)
    print("=" * 60 + "\n", flush=True)

    try:
        # 1. Energy Prices
        print("\n--- 1/15: Energy Prices ---", flush=True)
        try:
            energy_price_controller.uzupelnij_brakujace_dane(
                engine, start_date=effective_start_date
            )
            print("Energy Prices - COMPLETED", flush=True)
        except Exception as e:
            print(f" Energy Prices - FAILED: {e}", flush=True)

        # 2. Demand (Zapotrzebowanie)
        print("\n--- 2/15: Demand (Zapotrzebowanie) ---", flush=True)
        try:
            print("[FETCH ALL]  Starting demand controller...", flush=True)
            demand_controller.uzupelnij_brakujace_demand_kse(
                engine, start_date=effective_start_date
            )
            print("Demand - COMPLETED", flush=True)
        except Exception as e:
            print(f" Demand - FAILED: {e}", flush=True)
            import traceback

            traceback.print_exc()

        # 3. Power Losses (Ubytki)
        print("\n--- 3/15: Power Losses (Ubytki) ---", flush=True)
        try:
            ubytki_controller.uzupelnij_brakujace_dane_ubytki(
                engine, start_date=effective_start_date
            )
            print("Power Losses - COMPLETED", flush=True)
        except Exception as e:
            print(f" Power Losses - FAILED: {e}", flush=True)

        # 4. CO2 Prices
        print("\n--- 4/15: CO2 Prices ---", flush=True)
        try:
            co2_controller.uzupelnij_brakujace_dane_co2(
                engine, start_date=effective_start_date
            )
            print("CO2 Prices - COMPLETED", flush=True)
        except Exception as e:
            print(f" CO2 Prices - FAILED: {e}", flush=True)

        # 5. CRB Balancing Data
        print("\n--- 5/15: CRB Balancing Data ---", flush=True)
        try:
            crb_controller.uzupelnij_crb(engine, start_date=effective_start_date)
            print("CRB Balancing - COMPLETED", flush=True)
        except Exception as e:
            print(f" CRB Balancing - FAILED: {e}", flush=True)

        # 6. Weather Forecast
        print("\n--- 6/15: Weather Forecast ---", flush=True)
        try:
            weather_forecast_controller.fetch_forecast(
                engine,
                num_days=1,
                start_date=effective_start_date,
                end_date=datetime.now().date().isoformat(),
            )
            print("Weather Forecast - COMPLETED", flush=True)
        except Exception as e:
            print(f" Weather Forecast - FAILED: {e}", flush=True)

        # 7. Generation by Source
        print("\n--- 7/15: Generation by Source ---", flush=True)
        try:
            generation_controller.uzupelnij_generation(
                engine, start_date=effective_start_date
            )
            print("Generation by Source - COMPLETED", flush=True)
        except Exception as e:
            print(f" Generation by Source - FAILED: {e}", flush=True)

        # 8. Cross-Border Flows
        print("\n--- 8/15: Cross-Border Flows ---", flush=True)
        try:
            cross_border_controller.uzupelnij_flows(
                engine, start_date=effective_start_date
            )
            print("Cross-Border Flows - COMPLETED", flush=True)
        except Exception as e:
            print(f" Cross-Border Flows - FAILED: {e}", flush=True)

        # 9. Market Position
        print("\n--- 9/15: Market Position ---", flush=True)
        try:
            market_position_controller.uzupelnij_market_position(
                engine, start_date=effective_start_date
            )
            print("Market Position - COMPLETED", flush=True)
        except Exception as e:
            print(f" Market Position - FAILED: {e}", flush=True)

        # 10. SDAC Prices
        print("\n--- 10/15: SDAC Prices ---", flush=True)
        try:
            sdac_controller.uzupelnij_sdac(engine, start_date=effective_start_date)
            print("SDAC Prices - COMPLETED", flush=True)
        except Exception as e:
            print(f" SDAC Prices - FAILED: {e}", flush=True)

        # 11. Intraday Trading Volume
        print("\n--- 11/15: Intraday Trading Volume ---", flush=True)
        try:
            intraday_controller.uzupelnij_intraday(
                engine, start_date=effective_start_date
            )
            print("Intraday Trading Volume - COMPLETED", flush=True)
        except Exception as e:
            print(f" Intraday Trading Volume - FAILED: {e}", flush=True)

        # 12. Gas Prices
        print("\n--- 12/15: Gas Prices ---", flush=True)
        try:
            gas_controller.uzupelnij_gas_prices(engine, start_date=effective_start_date)
            print("Gas Prices - COMPLETED", flush=True)
        except Exception as e:
            print(f" Gas Prices - FAILED: {e}", flush=True)

        # 13. Oil Prices
        print("\n--- 13/15: Oil Prices ---", flush=True)
        try:
            oil_controller.uzupelnij_oil_prices(engine, start_date=effective_start_date)
            print("Oil Prices - COMPLETED", flush=True)
        except Exception as e:
            print(f" Oil Prices - FAILED: {e}", flush=True)

        # 14. ML Feature Store (scala wszystkie źródła w jeden szereg 15-minutowy)
        print("\n--- 14/15: ML Feature Store ---", flush=True)
        try:
            ml_feature_store_controller.uzupelnij_ml_features(
                engine, start_date=effective_start_date
            )
            print("ML Feature Store - COMPLETED", flush=True)
        except Exception as e:
            print(f" ML Feature Store - FAILED: {e}", flush=True)
        # 15. Przetwórz dane do uczenia modeli
        print("\n--- 15/15: ML Data Preparation ---", flush=True)
        try:
            ml_feature_store_controller.przetwoz_dane_do_uczenia_maszynowego(engine)
            print("ML Data Preparation - COMPLETED", flush=True)
        except Exception as e:
            print(f" ML Data Preparation - FAILED: {e}", flush=True)

        print("\n" + "=" * 60, flush=True)
        print(
            f"[FETCH ALL] All data fetching completed at {datetime.now()}",
            flush=True,
        )
        print("=" * 60 + "\n", flush=True)

    except Exception as e:
        print(f"\n[FETCH ALL]  Critical error: {e}", flush=True)
        import traceback

        traceback.print_exc()


@router.get("/fetch-all")
def fetch_all(
    background_tasks: BackgroundTasks,
    start_date: str | None = Query(
        default=None,
        description="Optional start date (YYYY-MM-DD) for all data sources.",
    ),
) -> dict[str, Any]:
    """
    Fetches all available energy and weather data to current date.

    This endpoint triggers a background task that:
    1. Fetches energy prices (RCE-PLN) from PSE
    2. Fetches demand data (zapotrzebowanie mocy KSE)
    3. Fetches power flows (przepływy mocy)
    4. Fetches power losses (ubytki mocy)
    5. Fetches CO2 prices
    6. Fetches CRB balancing data
    7. Fetches weather forecast
    8. Fetches generation by source (struktura generacji PSE)
    9. Fetches cross-border flows (przepływy międzysystemowe PSE)
    10. Fetches market position (pozycja rynkowa KSE)
    11. Fetches SDAC prices (ceny SDAC PSE)
    12. Fetches intraday trading volume (obrót USE PSE)
    13. Fetches gas prices (ceny gazu)
    14. Fetches oil prices (ceny ropy naftowej Brent)

    Each data source is updated from the last available date in the database
    to the current date.

    Returns:
        dict: Status message indicating the background task has started
    """
    if start_date is not None:
        start_date = _validate_start_date(start_date)

    background_tasks.add_task(fetch_all_data_task, start_date)

    effective_start_date = start_date or FETCH_ALL_START_DATE
    return {
        "status": "success",
        "message": "Started fetching all data in background. Check server logs for progress.",
        "timestamp": datetime.now().isoformat(),
        "start_date": effective_start_date,
        "data_sources": [
            "energy_prices",
            "demand_kse",
            "power_flows",
            "power_losses",
            "co2_prices",
            "crb_balancing",
            "weather_forecast",
            "generation_by_source",
            "cross_border_flows",
            "market_position",
            "sdac_prices",
            "intraday_trading_volume",
            "gas_prices",
            "oil_prices",
        ],
    }


@router.post("/fetch-all/scheduler/enable")
def enable_scheduler() -> dict[str, Any]:
    SCHEDULER_STATE["enabled"] = True
    return {
        "status": "success",
        "scheduler_enabled": True,
        "timestamp": datetime.now().isoformat(),
    }


@router.post("/fetch-all/scheduler/disable")
def disable_scheduler() -> dict[str, Any]:
    SCHEDULER_STATE["enabled"] = False
    return {
        "status": "success",
        "scheduler_enabled": False,
        "timestamp": datetime.now().isoformat(),
    }


@router.get("/fetch-all/scheduler/status")
def scheduler_status() -> dict[str, Any]:
    return {
        "status": "success",
        "scheduler_enabled": SCHEDULER_STATE["enabled"],
        "default_start_date": FETCH_ALL_START_DATE,
        "timestamp": datetime.now().isoformat(),
    }


@router.get("/fetch-all/status")
def get_fetch_status() -> dict[str, Any]:
    """
    Get the latest dates for all data sources.

    Returns:
        dict: Latest dates for each data source
    """
    status = {}

    try:
        with Session(engine) as session:
            # Energy Prices
            latest_energy = session.exec(
                select(func.max(EnergyPrice.business_date))
            ).first()
            status["energy_prices"] = (
                latest_energy.isoformat() if latest_energy else None
            )

            # Demand
            latest_demand = session.exec(
                select(func.max(ZapotrzebowanieMocyKSE.business_date))
            ).first()
            status["demand_kse"] = latest_demand.isoformat() if latest_demand else None

            # Power Flows
            latest_flows = session.exec(
                select(func.max(PrzeplywMocyJednostek.business_date))
            ).first()
            status["power_flows"] = latest_flows.isoformat() if latest_flows else None

            # Power Losses
            latest_losses = session.exec(
                select(func.max(UbytkiMocyJednostek.business_date))
            ).first()
            status["power_losses"] = (
                latest_losses.isoformat() if latest_losses else None
            )

            # CO2
            latest_co2 = session.exec(select(func.max(Co2Price.business_date))).first()
            status["co2_prices"] = latest_co2.isoformat() if latest_co2 else None

            # CRB
            latest_crb = session.exec(
                select(func.max(CrbRozliczenia.business_date))
            ).first()
            status["crb_balancing"] = latest_crb.isoformat() if latest_crb else None

            # Weather Forecast
            latest_forecast = session.exec(
                select(func.max(WeatherForecast.forecast_datetime))
            ).first()
            status["weather_forecast"] = (
                latest_forecast.isoformat() if latest_forecast else None
            )

            # Generation by Source
            latest_generation = session.exec(
                select(func.max(GenerationBySource.business_date))
            ).first()
            status["generation_by_source"] = (
                latest_generation.isoformat() if latest_generation else None
            )

            # Cross-Border Flows
            latest_flows_cross = session.exec(
                select(func.max(CrossBorderFlows.business_date))
            ).first()
            status["cross_border_flows"] = (
                latest_flows_cross.isoformat() if latest_flows_cross else None
            )

            # Market Position
            latest_market_pos = session.exec(
                select(func.max(AggregatedMarketPosition.business_date))
            ).first()
            status["market_position"] = (
                latest_market_pos.isoformat() if latest_market_pos else None
            )

            # SDAC Prices
            latest_sdac = session.exec(
                select(func.max(SDACPrices.business_date))
            ).first()
            status["sdac_prices"] = latest_sdac.isoformat() if latest_sdac else None

            # Intraday Trading Volume
            latest_intraday = session.exec(
                select(func.max(IntradayTradingVolume.business_date))
            ).first()
            status["intraday_trading_volume"] = (
                latest_intraday.isoformat() if latest_intraday else None
            )

            # Gas Prices
            latest_gas = session.exec(select(func.max(GasPrices.data))).first()
            status["gas_prices"] = latest_gas.isoformat() if latest_gas else None

            # Oil Prices
            latest_oil = session.exec(select(func.max(OilPrices.data))).first()
            status["oil_prices"] = latest_oil.isoformat() if latest_oil else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "success",
        "current_date": datetime.now().date().isoformat(),
        "latest_data": status,
    }
