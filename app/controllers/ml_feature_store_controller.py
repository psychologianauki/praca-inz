"""
ML Feature Store Controller
===========================
Buduje tabelę ml_features – szereg czasowy co 15 minut zawierający
scalone cechy ze wszystkich źródeł danych. Gotowy do użycia jako
input do modeli ML.

Źródła danych:
  - demand_kse            (15 min)
  - energy_prices         (godzinowe → forward-fill do 15 min)
  - generation_by_source  (15 min)
  - cross_border_flows    (15 min, pivot wg kierunku)
  - aggregated_market_position (15 min)
  - sdac_prices           (15 min)
  - intraday_trading_volume   (15 min)
  - crb_rozliczenia       (15 min)
  - co2_prices            (dzienne → LOCF)
  - gas_prices            (dzienne → LOCF)
  - oil_prices            (tygodniowe → LOCF)
  - weather_forecast      (co kilka godzin → LOCF)
"""

import logging
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, RobustScaler
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, func, select

from app.models import MLFeatureStore

BATCH_SIZE = 5000

logger = logging.getLogger(__name__)


def _q(engine, sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def _fetch_demand(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', udtczas) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM udtczas) / 15) AS ts,
            AVG(obciazenie) AS demand_mw
        FROM demand_kse
        WHERE udtczas >= :start AND udtczas <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_energy_prices(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', doba) AS ts,
            AVG(cena_mwh) AS rce_pln
        FROM energy_prices
        WHERE doba >= :start AND doba <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_generation(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            AVG(COALESCE(wi,  0))                                        AS gen_wind_mw,
            AVG(COALESCE(pv,  0))                                        AS gen_solar_mw,
            AVG(COALESCE(jgw1,0) + COALESCE(jgw2,0))                    AS gen_coal_mw,
            AVG(COALESCE(jgz1,0) + COALESCE(jgz2,0) + COALESCE(jgz3,0)) AS gen_renewables_mw,
            AVG(COALESCE(jg,  0))                                        AS gen_total_mw
        FROM generation_by_source
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_flows(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            SUM(CASE WHEN section_code ILIKE '%PL-DE%' OR section_code ILIKE '%DE-PL%'
                     THEN value ELSE 0 END) AS flow_pl_de,
            SUM(CASE WHEN section_code ILIKE '%PL-CZ%' OR section_code ILIKE '%CZ-PL%'
                     THEN value ELSE 0 END) AS flow_pl_cz,
            SUM(CASE WHEN section_code ILIKE '%PL-SK%' OR section_code ILIKE '%SK-PL%'
                     THEN value ELSE 0 END) AS flow_pl_sk,
            SUM(CASE WHEN section_code ILIKE '%PL-SE%' OR section_code ILIKE '%SE-PL%'
                     THEN value ELSE 0 END) AS flow_pl_se,
            SUM(CASE WHEN section_code ILIKE '%PL-LT%' OR section_code ILIKE '%LT-PL%'
                     THEN value ELSE 0 END) AS flow_pl_lt,
            SUM(value)                                                    AS flow_net
        FROM cross_border_flows
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_market_position(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            AVG(sk_cost) AS market_position_sk
        FROM aggregated_market_position
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_sdac(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            AVG(csdac_pln) AS sdac_pln
        FROM sdac_prices
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_intraday(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            AVG(CASE WHEN market_type = 'RBN' THEN COALESCE(day_ahead_tr_vol, 0) END) AS intraday_rbn_vol,
            AVG(CASE WHEN market_type = 'RBB' THEN COALESCE(sprz_volume,      0) END) AS intraday_rbb_vol
        FROM intraday_trading_volume
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_crb(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', dtime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM dtime) / 15) AS ts,
            AVG(cen_cost)   AS crb_cen_cost,
            AVG(ckoeb_cost) AS crb_ckoeb_cost
        FROM crb_rozliczenia
        WHERE dtime >= :start AND dtime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_co2(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('day', business_date) AS ts,
            AVG(rcco2_pln) AS co2_pln
        FROM co2_prices
        WHERE business_date >= :start AND business_date <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start.date(), "end": end.date()},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_gas(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('day', data) AS ts,
            AVG(cena_eur) AS gas_eur
        FROM gas_prices
        WHERE data >= :start AND data <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start.date(), "end": end.date()},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_oil(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('day', data) AS ts,
            AVG(cena_usd) AS oil_usd
        FROM oil_prices
        WHERE data >= :start AND data <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start.date(), "end": end.date()},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


def _fetch_weather(engine, start, end) -> pd.DataFrame:
    df = _q(
        engine,
        """
        SELECT
            date_trunc('hour', forecast_datetime) +
            INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM forecast_datetime) / 15) AS ts,
            AVG(temperature_2m)         AS temp_forecast,
            AVG(wind_speed_10m)         AS wind_speed_forecast,
            AVG(relative_humidity_2m)   AS humidity_forecast,
            AVG(rain)                   AS rain_forecast,
            AVG(precipitation)          AS precipitation_forecast,
            AVG(wind_direction_10m)     AS wind_direction_forecast,
            AVG(terrestrial_radiation)  AS terrestrial_radiation_forecast
        FROM weather_forecast
        WHERE forecast_datetime >= :start AND forecast_datetime <= :end
        GROUP BY 1 ORDER BY 1
    """,
        {"start": start, "end": end},
    )
    df["ts"] = pd.to_datetime(df["ts"])
    return df.set_index("ts")


# ─────────────────────────────────────────────────────────────
# Główna logika budowania cech
# ─────────────────────────────────────────────────────────────


def build_ml_features(engine, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    """
    Łączy wszystkie źródła danych w jeden DataFrame 15-minutowy.
    Uzupełnia braki metodą LOCF (forward-fill).
    Dodaje cechy pochodne (renewable_share, cechy kalendarzowe).
    """
    logger.info("Budowanie cech: %s - %s", start_ts, end_ts)

    # 7-dniowe okno lookback do rozgrzania LOCF
    lookback = start_ts - timedelta(days=7)

    extended_idx = pd.date_range(
        start=lookback.replace(second=0, microsecond=0),
        end=end_ts.replace(second=0, microsecond=0),
        freq="15min",
        name="ts",
    )
    target_idx = pd.date_range(
        start=start_ts.replace(second=0, microsecond=0),
        end=end_ts.replace(second=0, microsecond=0),
        freq="15min",
        name="ts",
    )

    if target_idx.empty:
        logger.warning("Pusty zakres dat.")
        return pd.DataFrame()

    logger.info("Zakres docelowy: %s przedziałów", len(target_idx))

    # ── Pobierz wszystkie źródła ──────────────────────────────
    fetchers = {
        "demand": _fetch_demand,
        "energy_prices": _fetch_energy_prices,
        "generation": _fetch_generation,
        "flows": _fetch_flows,
        "market_pos": _fetch_market_position,
        "sdac": _fetch_sdac,
        "intraday": _fetch_intraday,
        "crb": _fetch_crb,
        "co2": _fetch_co2,
        "gas": _fetch_gas,
        "oil": _fetch_oil,
        "weather": _fetch_weather,
    }

    sources = {}
    for name, fn in fetchers.items():
        try:
            sources[name] = fn(engine, lookback, end_ts)
            logger.info("%s: %s rekordów", name, len(sources[name]))
        except Exception as e:
            logger.warning("%s: błąd pobierania – %s", name, e)
            sources[name] = pd.DataFrame()

    # ── Zbuduj ramkę na rozszerzonym indeksie ────────────────
    df = pd.DataFrame(index=extended_idx)

    # 15-minutowe źródła – bezpośredni join
    for name in (
        "demand",
        "generation",
        "flows",
        "market_pos",
        "sdac",
        "intraday",
        "crb",
    ):
        if not sources[name].empty:
            df = df.join(sources[name])

    # Godzinowe ceny energii – reindex z ffill na poziomie godzin
    if not sources["energy_prices"].empty:
        ep = sources["energy_prices"].reindex(extended_idx, method="ffill")
        df = df.join(ep)

    # Dzienne / tygodniowe – reindex z ffill
    for name in ("co2", "gas", "oil"):
        if not sources[name].empty:
            daily = sources[name].reindex(extended_idx, method="ffill")
            df = df.join(daily)

    # Prognoza pogody – join + ffill razem z resztą
    if not sources["weather"].empty:
        df = df.join(sources["weather"])

    # ── LOCF dla wszystkich kolumn ───────────────────────────
    df = df.ffill()

    # ── Przytnij do zakresu docelowego ───────────────────────
    df = df.reindex(target_idx)

    # ── Cechy kalendarzowe ───────────────────────────────────
    df["business_date"] = df.index.normalize()
    df["hour"] = df.index.hour
    df["minute"] = df.index.minute
    df["day_of_week"] = df.index.dayofweek
    df["month"] = df.index.month
    df["is_weekend"] = df["day_of_week"] >= 5
    df["is_peak_hour"] = df["hour"].between(7, 20)

    # ── Udział OZE w generacji ───────────────────────────────
    total = df.get("gen_total_mw", pd.Series(0, index=df.index)).fillna(0)
    renewables = (
        df.get("gen_wind_mw", pd.Series(0, index=df.index)).fillna(0)
        + df.get("gen_solar_mw", pd.Series(0, index=df.index)).fillna(0)
        + df.get("gen_renewables_mw", pd.Series(0, index=df.index)).fillna(0)
    )
    df["renewable_share"] = np.where(total > 0, renewables / total, np.nan)

    logger.info("Zbudowano %s wierszy cech ML", len(df))
    return df


# ─────────────────────────────────────────────────────────────
# Punkt wejścia
# ─────────────────────────────────────────────────────────────


def uzupelnij_ml_features(engine, start_date: str | None = None):
    """Aktualizuje tabelę ml_features do bieżącej minuty."""

    logger.info("Uruchamianie aktualizacji ML Feature Store...")

    now_cutoff = datetime.now().replace(second=0, microsecond=0)

    configured_start_ts = (
        datetime.fromisoformat(f"{start_date}T00:00:00") if start_date else None
    )

    # Wyznacz punkt startowy
    with Session(engine) as session:
        last_ts = session.exec(select(func.max(MLFeatureStore.ts))).first()

    if last_ts is None:
        # Pierwsze uruchomienie – szukaj najwcześniejszych danych
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("""
                    SELECT LEAST(
                        (SELECT MIN(udtczas) FROM demand_kse),
                        (SELECT MIN(dtime)   FROM generation_by_source)
                    ) AS earliest
                """)
                ).fetchone()
            earliest = (
                row[0] if row and row[0] else (datetime.now() - timedelta(days=30))
            )
        except Exception:
            earliest = datetime.now() - timedelta(days=30)

        start_ts = earliest.replace(second=0, microsecond=0)
        if configured_start_ts:
            start_ts = max(start_ts, configured_start_ts)
        logger.info("Pierwsze uruchomienie → start od %s", start_ts)
    else:
        start_ts = last_ts + timedelta(minutes=15)
        if configured_start_ts:
            start_ts = max(start_ts, configured_start_ts)
        logger.info("Kontynuacja od %s", start_ts)

    if start_ts >= now_cutoff:
        logger.info("Dane są aktualne – nic do dodania.")
        return

    # Buduj cechy
    df = build_ml_features(engine, start_ts, now_cutoff)

    if df.empty:
        logger.warning("Brak danych do zapisania.")
        return

    # Przygotuj rekordy
    df = df.reset_index()  # ts jako kolumna
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.where(pd.notnull(df), other=None)  # NaN → None (SQL NULL)

    records = df.to_dict(orient="records")

    # Upsert partiami
    total_inserted = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        try:
            with Session(engine) as session:
                stmt = insert(MLFeatureStore).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ts"],
                    set_={
                        c.name: stmt.excluded[c.name]
                        for c in MLFeatureStore.__table__.columns
                        if c.name not in ["id", "ts", "created_at"]
                    },
                )
                session.exec(stmt)
                session.commit()
                total_inserted += len(batch)
                logger.info(
                    "Batch %s: %s rekordów",
                    i // BATCH_SIZE + 1,
                    len(batch),
                )
        except Exception as e:
            logger.error("Błąd batch %s: %s", i // BATCH_SIZE + 1, e)
    logger.info(
        "Łącznie zapisano %s rekordów do ml_features",
        total_inserted,
    )


def przetwoz_dane_do_uczenia_maszynowego(
    engine, output_path="/app/data/ml_ready_data.csv"
):
    """
    Pobiera dane z tabeli ml_features, uzupełnia braki,
    dokonuje inżynierii cech (sin/cos dla czasu i wiatru),
    oraz skaluje dane przy użyciu RobustScaler i MinMaxScaler.
    Na koniec zapisuje gotowe dane do pliku CSV.
    """

    logger.info(
        "Rozpoczęto przetwarzanie danych do ML... (zapis do %s)",
        output_path,
    )

    sprawdz_nulle_w_ml_features(engine)

    # 1. Pobranie danych bez kolumn id, created_at, updated_at
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM ml_features ORDER BY ts", conn)

    if df.empty:
        logger.warning("Brak danych w tabeli ml_features.")
        return

    df = df.set_index("ts")
    df = df.drop(
        columns=["id", "business_date", "created_at", "updated_at"], errors="ignore"
    )

    # 2. Uzupełnienie braków - ffill idzie do przodu po szeregu czasowym, bfill uzupełnia początek
    logger.info("Uzupełnianie braków danych...")
    df = df.ffill().bfill()

    # Zamiana typu boolean na int (0, 1)
    for col in ["is_weekend", "is_peak_hour"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    # 3. Zmienne cykliczne - inżynieria cech trygonometrycznych
    logger.info("Inżynieria cech (sin/cos dla cykli)...")

    # Kierunek wiatru (stopnie na radiany -> sin, cos)
    if "wind_direction_forecast" in df.columns:
        wind_rad = np.deg2rad(df["wind_direction_forecast"])
        df["wind_direction_sin"] = np.sin(wind_rad)
        df["wind_direction_cos"] = np.cos(wind_rad)
        df = df.drop(columns=["wind_direction_forecast"])

    # 4. Skalowanie
    logger.info("Skalowanie danych (RobustScaler + MinMaxScaler)...")

    # Wartości potencjalnie nieprzewidywalne i podatne na anomalię przepuszczamy przez RobustScaler
    robust_features = [
        "demand_mw",
        "rce_pln",
        "sdac_pln",
        "gen_total_mw",
        "flow_net",
        "flow_pl_de",
        "flow_pl_cz",
        "flow_pl_sk",
        "flow_pl_se",
        "flow_pl_lt",
        "market_position_sk",
        "intraday_rbn_vol",
        "intraday_rbb_vol",
        "crb_cen_cost",
        "crb_ckoeb_cost",
        "co2_pln",
        "gas_eur",
        "oil_usd",
    ]
    robust_cols = [c for c in robust_features if c in df.columns]

    if robust_cols:
        rs = RobustScaler()
        df[robust_cols] = rs.fit_transform(df[robust_cols])

    # Na samym końcu używamy MinMaxScaler dla całego zbioru do formatu 0-1
    minmax = MinMaxScaler(feature_range=(0, 1))
    df[df.columns] = minmax.fit_transform(df[df.columns])

    # 5. Eksport danych
    logger.info("Zapisywanie przetworzonych danych...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path)

    logger.info(
        "Zakończono pomyślnie. Plik znajduje się pod %s",
        output_path,
    )


def sprawdz_nulle_w_ml_features(engine):
    """
    Pobiera całą tabelę ml_features i sprawdza wyświetlając liczbę oraz
    procent wartości NULL w poszczególnych kolumnach.
    """
    logger.info("Rozpoczęto sprawdzanie nulli w tabeli ml_features...")

    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM ml_features", conn)

    if df.empty:
        logger.warning("Brak danych w tabeli ml_features.")
        return None

    null_counts = df.isnull().sum()
    total_rows = len(df)

    null_stats = null_counts[null_counts > 0]

    if null_stats.empty:
        logger.info(
            "Przeanalizowano %s wierszy. Nie znaleziono żadnych wartości NULL w tabeli.",
            total_rows,
        )
    else:
        logger.warning(
            "Znaleziono wartości NULL (całkowita liczba wierszy: %s):",
            total_rows,
        )
        for col, count in null_stats.items():
            percent = (count / total_rows) * 100
            logger.warning("  - %s: %s nulli (%.2f%%)", col, count, percent)

    return null_counts


def analiza_timescaledb_ml_features(
    engine,
    hours_back: int = 24 * 14,
    top_rows: int = 20,
) -> dict[str, pd.DataFrame]:
    """
    Analiza szeregów czasowych oparta o TimescaleDB:
      1) aktywacja extension + konwersja tabeli `ml_features` do hypertable,
      2) downsampling przez `time_bucket`,
      3) continuous aggregate (1h),
      4) zapytania typu "ostatnie X godzin".

    Zwraca słownik DataFrame'ów i jednocześnie drukuje wyniki.
    """
    logger.info("Start analizy TimescaleDB dla ml_features...")
    results: dict[str, pd.DataFrame] = {}

    analysis_end = datetime.now().replace(second=0, microsecond=0)
    analysis_start = analysis_end - timedelta(hours=hours_back)

    try:
        analytics_table = "ml_features_ts_analytics"
        cagg_name = "ml_features_ts_analytics_1h_cagg"

        with engine.begin() as conn:
            # 1) Extension + dedykowana hypertable do analiz
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {analytics_table} (
                        ts TIMESTAMP PRIMARY KEY,
                        rce_pln DOUBLE PRECISION,
                        demand_mw DOUBLE PRECISION,
                        gas_eur DOUBLE PRECISION,
                        co2_pln DOUBLE PRECISION,
                        renewable_share DOUBLE PRECISION,
                        intraday_rbn_vol DOUBLE PRECISION,
                        intraday_rbb_vol DOUBLE PRECISION
                    )
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    SELECT create_hypertable(
                        '{analytics_table}',
                        'ts',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    )
                    """
                )
            )

            # synchronizacja danych z ml_features (tylko analizowane okno)
            conn.execute(
                text(
                    f"""
                    INSERT INTO {analytics_table} (
                        ts,
                        rce_pln,
                        demand_mw,
                        gas_eur,
                        co2_pln,
                        renewable_share,
                        intraday_rbn_vol,
                        intraday_rbb_vol
                    )
                    SELECT
                        ts,
                        rce_pln,
                        demand_mw,
                        gas_eur,
                        co2_pln,
                        renewable_share,
                        intraday_rbn_vol,
                        intraday_rbb_vol
                    FROM ml_features
                    WHERE ts >= :start_ts AND ts <= :end_ts
                    ON CONFLICT (ts) DO UPDATE SET
                        rce_pln = EXCLUDED.rce_pln,
                        demand_mw = EXCLUDED.demand_mw,
                        gas_eur = EXCLUDED.gas_eur,
                        co2_pln = EXCLUDED.co2_pln,
                        renewable_share = EXCLUDED.renewable_share,
                        intraday_rbn_vol = EXCLUDED.intraday_rbn_vol,
                        intraday_rbb_vol = EXCLUDED.intraday_rbb_vol
                    """
                ),
                {"start_ts": analysis_start, "end_ts": analysis_end},
            )

            # 2) Continuous aggregate (godzinowa agregacja)
            conn.execute(
                text(
                    f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {cagg_name}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        time_bucket(INTERVAL '1 hour', ts) AS bucket,
                        AVG(rce_pln) AS avg_rce_pln,
                        AVG(demand_mw) AS avg_demand_mw,
                        AVG(gas_eur) AS avg_gas_eur,
                        AVG(co2_pln) AS avg_co2_pln,
                        AVG(renewable_share) AS avg_renewable_share,
                        SUM(COALESCE(intraday_rbn_vol, 0)) AS sum_intraday_rbn,
                        SUM(COALESCE(intraday_rbb_vol, 0)) AS sum_intraday_rbb
                    FROM {analytics_table}
                    GROUP BY bucket
                    WITH NO DATA
                    """
                )
            )

        # refresh continuous aggregate poza transakcją
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(
                text(
                    f"""
                    CALL refresh_continuous_aggregate(
                        '{cagg_name}',
                        :start_ts,
                        :end_ts
                    )
                    """
                ),
                {
                    "start_ts": analysis_start,
                    "end_ts": analysis_end,
                },
            )

        # 3) Zapytanie "ostatnie X godzin" (z hypertable analitycznej)
        with engine.connect() as conn:
            last_window_df = pd.read_sql(
                text(
                    f"""
                    SELECT
                        ts,
                        rce_pln,
                        demand_mw,
                        gas_eur,
                        co2_pln,
                        renewable_share
                    FROM {analytics_table}
                    WHERE ts >= :start_ts AND ts <= :end_ts
                    ORDER BY ts DESC
                    LIMIT :lim
                    """
                ),
                conn,
                params={
                    "start_ts": analysis_start,
                    "end_ts": analysis_end,
                    "lim": top_rows,
                },
            )
            results["last_window"] = last_window_df

            # 4) Downsampling (4h) przez time_bucket
            downsample_df = pd.read_sql(
                text(
                    f"""
                    SELECT
                        time_bucket(INTERVAL '4 hour', ts) AS bucket_4h,
                        AVG(rce_pln) AS avg_rce_pln,
                        MIN(rce_pln) AS min_rce_pln,
                        MAX(rce_pln) AS max_rce_pln,
                        AVG(demand_mw) AS avg_demand_mw,
                        AVG(renewable_share) AS avg_renewable_share
                    FROM {analytics_table}
                    WHERE ts >= :start_ts AND ts <= :end_ts
                    GROUP BY bucket_4h
                    ORDER BY bucket_4h DESC
                    LIMIT :lim
                    """
                ),
                conn,
                params={
                    "start_ts": analysis_start,
                    "end_ts": analysis_end,
                    "lim": top_rows,
                },
            )
            results["downsample_4h"] = downsample_df

            # 5) Odczyt z continuous aggregate
            cagg_df = pd.read_sql(
                text(
                    f"""
                    SELECT
                        bucket,
                        avg_rce_pln,
                        avg_demand_mw,
                        avg_gas_eur,
                        avg_co2_pln,
                        avg_renewable_share,
                        sum_intraday_rbn,
                        sum_intraday_rbb
                    FROM {cagg_name}
                    WHERE bucket >= :start_ts AND bucket <= :end_ts
                    ORDER BY bucket DESC
                    LIMIT :lim
                    """
                ),
                conn,
                params={
                    "start_ts": analysis_start,
                    "end_ts": analysis_end,
                    "lim": top_rows,
                },
            )
            results["cagg_1h"] = cagg_df

        logger.info("Analiza zakończona.")

        return results

    except Exception:
        logger.exception("Błąd analizy TimescaleDB: %s")
        return results
