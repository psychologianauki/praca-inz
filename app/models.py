from datetime import UTC, date, datetime

from sqlalchemy import Index, UniqueConstraint
from sqlmodel import Field, SQLModel


class EnergyPrice(SQLModel, table=True):
    __tablename__ = "energy_prices"
    __table_args__ = (
        UniqueConstraint("doba", "godzina", name="uq_energy_price_doba_godzina"),
        Index("idx_energy_price_doba_biz", "doba", "business_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    doba: datetime
    cena_mwh: float
    godzina: str
    business_date: datetime
    source_datetime: datetime

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now, sa_column_kwargs={"onupdate": datetime.now}
    )


class WeatherForecast(SQLModel, table=True):
    __tablename__ = "weather_forecast"
    __table_args__ = (
        UniqueConstraint(
            "latitude",
            "longitude",
            "forecast_datetime",
            name="uq_weather_forecast_coords_time",
        ),
        Index("idx_weather_forecast_coords", "latitude", "longitude"),
        Index("idx_weather_forecast_datetime", "forecast_datetime"),
    )

    id: int | None = Field(default=None, primary_key=True)
    latitude: float | None = None
    longitude: float | None = None

    forecast_datetime: datetime | None = None
    temperature_2m: float | None = None
    rain: float | None = None
    relative_humidity_2m: float | None = None
    precipitation: float | None = None
    wind_speed_10m: float | None = None
    wind_direction_10m: float | None = None

    # Solar radiation
    ghi: float | None = None  # shortwave_radiation_instant
    dni: float | None = None  # direct_normal_irradiance_instant
    dri: float | None = None  # diffuse_radiation_instant
    gti: float | None = None  # global_tilted_irradiance_instant
    terrestrial_radiation: float | None = None

    datasource: str = "openmeteo"

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Co2Price(SQLModel, table=True):
    __tablename__ = "co2_prices"
    __table_args__ = (
        UniqueConstraint("business_date", name="uq_co2_prices_business_date"),
        Index("idx_co2_prices_business_date", "business_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    rcco2_eur: float
    rcco2_pln: float
    business_date: datetime
    source_datetime: datetime

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now, sa_column_kwargs={"onupdate": datetime.now}
    )


class UbytkiMocyJednostek(SQLModel, table=True):
    __tablename__ = "unit_power_losses"

    id: int | None = Field(default=None, primary_key=True)

    # Dane identyfikacyjne
    resource_code: str = Field(index=True)  # np. "BEL_2-02"
    power_plant: str  # np. "Bełchatów"

    # Czas i okresy
    dtime: datetime  # np. "2024-06-14 00:00"
    dtime_utc: datetime  # np. "2024-06-13 22:00:00"
    period: str  # np. "23:45 - 24:00"
    period_utc: str  # np. "21:45 - 22:00"
    business_date: datetime  # np. "2024-06-13"

    # Wartości liczbowe (moce i ograniczenia)
    grid_lim: float  # Ograniczenia sieciowe
    non_us_cap: float  # Ubytki elektrowniane
    available_capacity: float  # Dostępna moc

    # Znaczniki publikacji (z milisekundami)
    publication_ts: datetime  # Lokalny czas publikacji
    publication_ts_utc: datetime  # Czas UTC publikacji


class PrzeplywMocyJednostek(SQLModel, table=True):
    __tablename__ = "unit_power_flows"
    __table_args__ = (
        UniqueConstraint(
            "section_code",
            "dtime",
            "business_date",
            name="uq_power_flows_section_dtime_biz",
        ),
        Index("idx_power_flows_section", "section_code"),
        Index("idx_power_flows_business_date", "business_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    section_code: str = Field(index=True)  # np. "string"

    # Czas i okresy
    dtime: datetime  # np. "2024-06-14 00:15"
    dtime_utc: datetime  # np. "2024-06-13 22:15:00"
    period: str  # np. "00:00 - 00:15"
    period_utc: str  # np. "22:00 - 22:15"
    business_date: datetime = Field(index=True)  # np. "2024-06-14"

    # Wartość (liczba)
    value: float  # np. 0

    # Znaczniki publikacji
    publication_ts: datetime
    publication_ts_utc: datetime | None = None


class ZapotrzebowanieMocyKSE(SQLModel, table=True):
    __tablename__ = "demand_kse"
    __table_args__ = (
        UniqueConstraint("doba", "udtczas", name="uq_demand_kse_doba_udtczas"),
        Index("idx_demand_kse_doba_biz", "doba", "business_date"),
    )

    id: int | None = Field(default=None, primary_key=True)

    doba: datetime = Field(index=True)
    udtczas: datetime
    obciazenie: float  # Rzeczywiste zapotrzebowanie KSE
    udtczas_oreb: str | None = None

    business_date: datetime
    source_datetime: datetime

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )


class CrbRozliczenia(SQLModel, table=True):
    __tablename__ = "crb_rozliczenia"
    __table_args__ = (
        Index("idx_crb_dtime_biz", "dtime", "business_date"),
        UniqueConstraint("dtime", "period", name="uq_crb_dtime_period"),
    )

    id: int | None = Field(default=None, primary_key=True)

    dtime: datetime = Field(index=True)  # np. "2024-06-14 00:15"
    dtime_utc: datetime  # np. "2024-06-13 22:15:00"
    period: str  # np. "00:00 - 00:15"
    period_utc: str  # np. "22:00 - 22:15"
    business_date: datetime  # np. "2024-06-14"

    # Koszty i ceny (używamy float | None dla pól, które mogą być null)
    cen_cost: float  # Cena energii niezbilansowania
    ckoeb_cost: float  # Cena kosztów odchyleń
    ceb_pp_cost: float  # Cena energii bilansującej - PP
    ceb_sr_cost: float  # Cena energii bilansującej - SR

    # Nowe pola (obsługa null z JSON-a)
    ceb_sr_afrrd_cost: float | None = None  # Rezerwa aFRR dół
    ceb_sr_afrrg_cost: float | None = None  # Rezerwa aFRR góra

    # Znaczniki publikacji
    publication_ts: datetime
    publication_ts_utc: datetime


class GasPrices(SQLModel, table=True):
    __tablename__ = "gas_prices"
    __table_args__ = (
        UniqueConstraint("data", "source", name="uq_gas_prices_data_source"),
        Index("idx_gas_prices_data", "data"),
    )

    id: int | None = Field(default=None, primary_key=True)
    data: datetime = Field(index=True)
    cena_pln: float | None = Field(default=None)
    cena_eur: float | None = Field(default=None)
    volume: float | None = Field(default=None)
    source: str = Field(default="")

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )


class OilPrices(SQLModel, table=True):
    __tablename__ = "oil_prices"
    __table_args__ = (
        UniqueConstraint("data", "source", name="uq_oil_prices_data_source"),
        Index("idx_oil_prices_data", "data"),
    )

    id: int | None = Field(default=None, primary_key=True)
    data: datetime = Field(index=True)
    cena_usd: float | None = Field(default=None)
    cena_pln: float | None = Field(default=None)
    source: str = Field(default="")

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )


class EnergyPricePredictions(SQLModel, table=True):
    __tablename__ = "energy_price_predictions"
    __table_args__ = (
        UniqueConstraint("prediction_date", "target_date", name="uq_predictions_dates"),
        Index("idx_predictions_target_date", "target_date"),
        Index("idx_predictions_created", "prediction_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    prediction_date: datetime = Field(index=True)  # Kiedy została wykonana predykcja
    target_date: datetime = Field(index=True)  # Na jaki dzień jest predykcja
    predicted_price: float = Field(description="Przewidywana cena PLN/MWh")
    confidence_lower: float | None = Field(
        default=None, description="Dolna granica przedziału ufności"
    )
    confidence_upper: float | None = Field(
        default=None, description="Górna granica przedziału ufności"
    )
    model_version: str = Field(
        default="v1.0", description="Wersja modelu użytego do predykcji"
    )
    model_accuracy: float | None = Field(
        default=None, description="Dokładność modelu (R²)"
    )

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )


class GenerationBySource(SQLModel, table=True):
    """Struktura generacji według źródeł z PSE API - kluczowa dla cen"""

    __tablename__ = "generation_by_source"
    __table_args__ = (
        UniqueConstraint("dtime", "business_date", name="uq_generation_dtime_biz"),
        Index("idx_generation_dtime", "dtime"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    period: str
    period_utc: str
    business_date: datetime = Field(index=True)

    # Struktura generacji (z his-wlk-cal endpoint)
    jgw1: float | None = None  # JGw ZAK=1 (węgiel głównie)
    jgw2: float | None = None  # JGw ZAK=2
    jgm1: float | None = None  # Magazyny ZAK=1
    jgm2: float | None = None  # Magazyny ZAK=2
    jgz1: float | None = None  # OZE ZAK=1
    jgz2: float | None = None  # OZE ZAK=2
    jgz3: float | None = None  # OZE ZAK=3
    jga: float | None = None  # Agregaty
    jgo: float | None = None  # Odbiór
    jnwrb: float | None = None  # Poza rynkiem bilansującym
    wi: float | None = None  # Wiatr
    pv: float | None = None  # Fotowoltaika

    # Bilans
    zapotrzebowanie: float  # Główny driver cen
    swm_p: float | None = None  # Wymiana równoległa
    swm_np: float | None = None  # Wymiana nierównoległa
    jg: float | None = None  # Suma generacji

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class DemandAndRenewableForecasts(SQLModel, table=True):
    __tablename__ = "demand_renewable_forecasts"
    __table_args__ = (
        UniqueConstraint("dtime", "forecast_type", name="uq_demand_forecasts"),
        Index("idx_demand_forecasts_dtime", "dtime"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    period: str
    business_date: datetime
    forecast_type: str  # "prog-obc" lub "his-obc"

    # Z prog-obc / his-obc endpoints
    load_fcst: float | None = None  # Prognoza zapotrzebowania
    load_actual: float | None = None  # Rzeczywiste zapotrzebowanie

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class CrossBorderFlows(SQLModel, table=True):
    """Przepływy międzysystemowe z PSE"""

    __tablename__ = "cross_border_flows"
    __table_args__ = (
        UniqueConstraint("dtime", "section_code", "business_date", name="uq_flows"),
        Index("idx_flows_dtime", "dtime"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    period: str
    period_utc: str
    business_date: datetime

    section_code: str = Field(index=True)
    value: float  # MW (+import, -export)

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class AggregatedMarketPosition(SQLModel, table=True):
    """Stan zakontraktowania KSE - sk-rozl endpoint"""

    __tablename__ = "aggregated_market_position"
    __table_args__ = (UniqueConstraint("dtime", "period", name="uq_market_position"),)

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    period: str
    period_utc: str
    business_date: datetime

    sk_cost: float  # Stan zakontraktowania (+long, -short)
    sk_d1_fcst: float | None = None  # Prognoza SK D-1
    sk_d_fcst: float | None = None  # Prognoza SK D-0
    contracting_status: str = Field(default="")  # "long" or "short"

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class SDACPrices(SQLModel, table=True):
    """Ceny SDAC - europejski coupling"""

    __tablename__ = "sdac_prices"
    __table_args__ = (UniqueConstraint("dtime", "period", name="uq_sdac_prices"),)

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    period: str
    period_utc: str
    business_date: datetime

    csdac_pln: float  # Cena SDAC w PLN/MWh

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class IntradayTradingVolume(SQLModel, table=True):
    """Wolumen obrotu USE - aktywność rynku"""

    __tablename__ = "intraday_trading_volume"
    __table_args__ = (
        UniqueConstraint("dtime", "market_type", name="uq_intraday_volume"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dtime: datetime = Field(index=True)
    dtime_utc: datetime
    business_date: datetime
    market_type: str  # "RBN" lub "RBB"

    day_ahead_tr_vol: float | None = None  # RBN
    sprz_volume: float | None = None  # RBB

    publication_ts: datetime
    publication_ts_utc: datetime

    created_at: datetime = Field(default_factory=datetime.now)


class MLFeatureStore(SQLModel, table=True):
    """
    Szereg czasowy co 15 minut łączący wszystkie źródła danych.
    Gotowy do użycia jako input do modeli ML.
    """

    __tablename__ = "ml_features"
    __table_args__ = (
        UniqueConstraint("ts", name="uq_ml_features_ts"),
        Index("idx_ml_features_ts", "ts"),
        Index("idx_ml_features_business_date", "business_date"),
    )

    id: int | None = Field(default=None, primary_key=True)

    # Znacznik czasu (główna oś szeregu czasowego)
    ts: datetime = Field(index=True)
    business_date: datetime

    # Cechy kalendarzowe
    hour: int
    minute: int
    day_of_week: int  # 0=Pon, 6=Nie
    month: int
    is_weekend: bool
    is_peak_hour: bool  # True dla 7:00-21:00

    # === Zapotrzebowanie (demand_kse) ===
    demand_mw: float | None = None

    # === Ceny energii (energy_prices, co godzinę → fill co 15 min) ===
    rce_pln: float | None = None

    # === Struktura generacji (generation_by_source) ===
    gen_wind_mw: float | None = None
    gen_solar_mw: float | None = None
    gen_coal_mw: float | None = None
    gen_renewables_mw: float | None = None
    gen_total_mw: float | None = None
    renewable_share: float | None = None  # (wiatr+PV+OZE) / generacja całkowita

    # === Przepływy transgraniczne (cross_border_flows) ===
    flow_pl_de: float | None = None
    flow_pl_cz: float | None = None
    flow_pl_sk: float | None = None
    flow_pl_se: float | None = None
    flow_pl_lt: float | None = None
    flow_net: float | None = None  # Saldo wymiany (+import, -eksport)

    # === Pozycja rynkowa KSE (aggregated_market_position) ===
    market_position_sk: float | None = None

    # === Ceny SDAC (sdac_prices) ===
    sdac_pln: float | None = None

    # === Wolumen intraday (intraday_trading_volume) ===
    intraday_rbn_vol: float | None = None
    intraday_rbb_vol: float | None = None

    # === Rozliczenia CRB (crb_rozliczenia) ===
    crb_cen_cost: float | None = None
    crb_ckoeb_cost: float | None = None

    # === Ceny CO2 (co2_prices, dzienne → LOCF) ===
    co2_pln: float | None = None

    # === Ceny gazu (gas_prices, dzienne → LOCF) ===
    gas_eur: float | None = None

    # === Ceny ropy (oil_prices, tygodniowe → LOCF) ===
    oil_usd: float | None = None

    # === Prognoza pogody (weather_forecast) ===
    temp_forecast: float | None = None
    wind_speed_forecast: float | None = None
    humidity_forecast: float | None = None
    rain_forecast: float | None = None
    precipitation_forecast: float | None = None
    wind_direction_forecast: float | None = None

    # Solar radiation
    terrestrial_radiation_forecast: float | None = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)},
    )
