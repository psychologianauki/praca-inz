"""
Feature Engineering dla predykcji cen energii wykorzystujący TimescaleDB
Zawiera zaawansowane techniki przetwarzania danych czasowych dla ML
"""

import logging
from datetime import timedelta
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import scipy
from sqlalchemy import text
from sqlmodel import Session

logger = logging.getLogger(__name__)


class TimescaleFeatureEngine:
    """
    Klasa do zaawansowanego przetwarzania danych czasowych używając TimescaleDB
    Wykorzystuje native funkcje TimescaleDB dla wydajności
    """

    def __init__(self, engine):
        self.engine = engine
        self.session = Session(engine)

    def scale_features(
        self,
        df: pd.DataFrame,
        columns: list,
        method: str = "zscore",
        fit: bool = True,
        params: dict = None,
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Skaluje wybrane kolumny za pomocą z-score lub Median-MAD.
        fit=True: oblicza parametry na podanym df (np. train), fit=False: używa podanych params (np. test/val)
        Zwraca: zeskalowany df oraz parametry (mean/std lub median/MAD)
        """
        df_scaled = df.copy()
        scaling_params = {} if params is None else params.copy()
        for col in columns:
            if method == "zscore":
                if fit:
                    mean = df[col].mean()
                    std = df[col].std()
                    scaling_params[col] = {"mean": mean, "std": std}
                else:
                    mean = scaling_params[col]["mean"]
                    std = scaling_params[col]["std"]
                df_scaled[col + "_zscore"] = (df[col] - mean) / std if std != 0 else 0
            elif method == "mad":
                if fit:
                    median = df[col].median()
                    mad = (df[col] - median).abs().median()
                    scaling_params[col] = {"median": median, "mad": mad}
                else:
                    median = scaling_params[col]["median"]
                    mad = scaling_params[col]["mad"]
                df_scaled[col + "_mad"] = (df[col] - median) / mad if mad != 0 else 0
        return df_scaled, scaling_params

    def variance_stabilizing_transform(
        self,
        df: pd.DataFrame,
        columns: list,
        method: str = "asinh",
        fit: bool = True,
        params: dict = None,
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Wykonuje transformację stabilizującą wariancję (VST) na wybranych kolumnach.
        Dostępne metody: 'asinh', 'mlog', 'boxcox'.
        fit=True: oblicza parametry (np. lambda dla boxcox) na podanym df, fit=False: używa podanych params.
        Zwraca: ztransformowany df oraz parametry.
        """
        df_trans = df.copy()
        vst_params = {} if params is None else params.copy()
        for col in columns:
            if method == "asinh":
                df_trans[col + "_asinh"] = np.arcsinh(df[col])
            elif method == "mlog":
                df_trans[col + "_mlog"] = np.sign(df[col]) * np.log1p(np.abs(df[col]))
            elif method == "boxcox":
                min_val = df[col].min()
                shift = 1 - min_val if min_val <= 0 else 0
                if fit:
                    trans, lmbda = scipy.stats.boxcox(df[col] + shift)
                    vst_params[col] = {"lambda": lmbda, "shift": shift}
                else:
                    lmbda = vst_params[col]["lambda"]
                    shift = vst_params[col]["shift"]
                    trans = scipy.stats.boxcox(df[col] + shift, lmbda)
                df_trans[col + "_boxcox"] = trans
        return df_trans, vst_params

    def feature_selection(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        method: str = "lasso",
        top_n: int = 20,
        alpha: float = 0.01,
    ) -> list:
        """
        Wybiera najważniejsze cechy za pomocą LASSO lub Mutual Information.
        X: DataFrame z cechami
        y: target
        method: 'lasso' lub 'mutual_info'
        top_n: liczba cech do zwrócenia (dla MI)
        alpha: współczynnik kary dla LASSO
        Zwraca: listę wybranych nazw cech
        """
        from sklearn.feature_selection import mutual_info_regression
        from sklearn.linear_model import Lasso

        if method == "lasso":
            lasso = Lasso(alpha=alpha)
            lasso.fit(X, y)
            selected = list(X.columns[lasso.coef_ != 0])
            return selected
        elif method == "mutual_info":
            mi = mutual_info_regression(X, y)
            idx = np.argsort(mi)[-top_n:]
            selected = list(X.columns[idx])
            return selected
        else:
            raise ValueError("method must be 'lasso' or 'mutual_info'")

    """
    Klasa do zaawansowanego przetwarzania danych czasowych używając TimescaleDB
    Wykorzystuje native funkcje TimescaleDB dla wydajności
    """

    def create_ml_dataset(
        self,
        start_date: str,
        end_date: str,
        target_column: str = "price",
        time_horizon_hours: int = 24,
        include_lags: bool = True,
        include_rolling_stats: bool = True,
        include_fourier_features: bool = True,
    ) -> pd.DataFrame:
        """
        Tworzy kompletny dataset do ML z wykorzystaniem TimescaleDB funkcji
        """

        logger.info(f" Tworzenie ML dataset: {start_date} → {end_date}")

        # 1. BAZOWY DATASET Z CENAMI (cel predykcji)
        base_df = self._get_base_price_data(start_date, end_date)

        # 2. DANE GENERACJI (kluczowe dla cen)
        generation_features = self._get_generation_features(start_date, end_date)
        base_df = self._merge_timeseries(base_df, generation_features, "dtime")

        # 3. DANE POGODOWE (wpływ na OZE)
        weather_features = self._get_weather_features(start_date, end_date)
        base_df = self._merge_timeseries(base_df, weather_features, "dtime")

        # 4. PRZEPŁYWY MIĘDZYSYSTEMOWE
        border_features = self._get_cross_border_features(start_date, end_date)
        base_df = self._merge_timeseries(base_df, border_features, "dtime")

        # 5. CENY PALIW (fundamentals)
        fuel_features = self._get_fuel_price_features(start_date, end_date)
        base_df = self._merge_timeseries(base_df, fuel_features, "dtime")

        # 6. DANE RYNKOWE (SDAC, intraday)
        market_features = self._get_market_features(start_date, end_date)
        base_df = self._merge_timeseries(base_df, market_features, "dtime")

        if include_lags:
            base_df = self._add_lag_features(base_df, target_column)

        if include_rolling_stats:
            base_df = self._add_rolling_statistics(base_df, target_column)

        if include_fourier_features:
            base_df = self._add_temporal_features(base_df)

        # 7. ADVANCED TIMESCALE FEATURES
        base_df = self._add_timescale_aggregations(base_df, start_date, end_date)

        logger.info(
            f"Dataset utworzony: {len(base_df)} rekordów, {len(base_df.columns)} cech"
        )
        return base_df

    def _get_base_price_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Pobiera bazowe dane cen z TimescaleDB"""
        query = text("""
        SELECT 
            dtime_utc as dtime,
            price,
            volume_mwh,
            EXTRACT(hour FROM dtime_utc) as hour_of_day,
            EXTRACT(dow FROM dtime_utc) as day_of_week,
            EXTRACT(month FROM dtime_utc) as month,
            CASE WHEN EXTRACT(dow FROM dtime_utc) IN (0,6) THEN 1 ELSE 0 END as is_weekend
        FROM energy_prices 
        WHERE dtime_utc BETWEEN :start_date AND :end_date
        ORDER BY dtime_utc
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_date": start_date, "end_date": end_date}
            )

        df["dtime"] = pd.to_datetime(df["dtime"])
        df.set_index("dtime", inplace=True)
        return df

    def _get_generation_features(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Pobiera cechy generacji z zaawansowanymi kalkulacjami"""
        query = text("""
        WITH gen_enriched AS (
            SELECT 
                dtime_utc as dtime,
                demand,
                jg as total_generation,
                wi as wind_generation,
                pv as solar_generation,
                jgw1 + jgw2 as fossil_generation,
                jgz1 + jgz2 + jgz3 as renewable_generation,
                swm_p as cross_border_balance,
                
                -- Kalkulowane wskaźniki
                CASE WHEN demand > 0 THEN (wi + pv) / demand ELSE 0 END as renewable_share,
                CASE WHEN jg > 0 THEN demand / jg ELSE 0 END as supply_demand_ratio,
                demand - jg as generation_deficit,
                
                -- Marginalne jednostki (proxy dla ceny krańcowej)
                jgw1 + jgw2 + jga as dispatchable_generation
            
            FROM generation_by_source
            WHERE dtime_utc BETWEEN :start_date AND :end_date
        )
        SELECT *,
            -- Moving ratios (TimescaleDB window functions)
            AVG(renewable_share) OVER (
                ORDER BY dtime 
                ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
            ) as renewable_share_24h_avg,
            
            AVG(supply_demand_ratio) OVER (
                ORDER BY dtime 
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW  
            ) as supply_demand_12h_avg
            
        FROM gen_enriched
        ORDER BY dtime
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_date": start_date, "end_date": end_date}
            )

        df["dtime"] = pd.to_datetime(df["dtime"])
        df.set_index("dtime", inplace=True)
        return df

    def _get_weather_features(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Pobiera cechy pogodowe wpływające na generację OZE"""
        query = text("""
        SELECT 
            czas_prognozy as dtime,
            temperatura_prognoza as temperature_2m_celsius,
            predkosc_wiatru_prognoza as wind_speed_10m_kmh,
            kierunek_wiatru_prognoza as wind_direction_10m_degrees,
            wielkosc_opadu_prognoza as precipitation_mm,
            zachmurzenie_prognoza as cloud_cover_percent,
            
            -- Pochodne cechy wpływające na generację
            CASE 
                WHEN predkosc_wiatru_prognoza > 50 THEN 1 -- Za silny wiatr (odłączenia)
                WHEN predkosc_wiatru_prognoza < 7 THEN 1  -- Za słaby wiatr
                ELSE 0 
            END as wind_generation_risk,
            
            -- Szacowana generacja wiatrowa (uproszczona)
            CASE 
                WHEN predkosc_wiatru_prognoza BETWEEN 7 AND 50 
                THEN POWER(predkosc_wiatru_prognoza, 3) * 0.001  -- Cubic wind law
                ELSE 0 
            END as estimated_wind_potential
            
        FROM weather_forecast
        WHERE czas_prognozy BETWEEN :start_date AND :end_date
        ORDER BY czas_prognozy
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_date": start_date, "end_date": end_date}
            )

        if len(df) > 0:
            df["dtime"] = pd.to_datetime(df["dtime"])
            df.set_index("dtime", inplace=True)

        return df

    def _get_cross_border_features(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Pobiera agregowane przepływy międzysystemowe"""
        query = text("""
        WITH border_agg AS (
            SELECT 
                dtime_utc as dtime,
                SUM(CASE WHEN value > 0 THEN value ELSE 0 END) as total_imports,
                SUM(CASE WHEN value < 0 THEN value ELSE 0 END) as total_exports,
                SUM(value) as net_cross_border_flow,
                COUNT(DISTINCT section_code) as active_connections,
                
                -- Najważniejsze połączenia
                SUM(CASE WHEN section_code = 'PL-DE' THEN value ELSE 0 END) as flow_pl_de,
                SUM(CASE WHEN section_code = 'PL-CZ' THEN value ELSE 0 END) as flow_pl_cz,
                SUM(CASE WHEN section_code = 'PL-SK' THEN value ELSE 0 END) as flow_pl_sk
                
            FROM cross_border_flows
            WHERE dtime_utc BETWEEN :start_date AND :end_date
            GROUP BY dtime_utc
        )
        SELECT *,
            -- Wskaźniki zależności od importu
            CASE WHEN total_imports + ABS(total_exports) > 0 
                 THEN total_imports / (total_imports + ABS(total_exports))
                 ELSE 0 END as import_dependency_ratio,
            
            -- Przewidywanie direction flows
            SIGN(flow_pl_de) as de_flow_direction,
            ABS(flow_pl_de) as de_flow_magnitude
            
        FROM border_agg
        ORDER BY dtime
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_date": start_date, "end_date": end_date}
            )

        if len(df) > 0:
            df["dtime"] = pd.to_datetime(df["dtime"])
            df.set_index("dtime", inplace=True)

        return df

    def _get_fuel_price_features(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Pobiera ceny paliw z interpolacją dla brakujących wartości"""

        # Pobierz ceny CO2 (dzienne) i interpoluj
        co2_query = text("""
        SELECT 
            date_column as date,
            price as co2_price_eur_ton
        FROM co2_prices  
        WHERE date_column BETWEEN :start_date::date AND :end_date::date
        ORDER BY date_column
        """)

        # Pobierz ceny gazu (jeśli dostępne)
        gas_query = text("""
        SELECT 
            date_column as date,
            price as gas_price_eur_mwh
        FROM gas_prices
        WHERE date_column BETWEEN :start_date::date AND :end_date::date
        ORDER BY date_column
        """)

        fuel_data = []

        with self.engine.connect() as conn:
            # CO2 prices
            co2_df = pd.read_sql(
                co2_query, conn, params={"start_date": start_date, "end_date": end_date}
            )

            if len(co2_df) > 0:
                co2_df["date"] = pd.to_datetime(co2_df["date"])
                fuel_data.append(co2_df)

            # Gas prices
            try:
                gas_df = pd.read_sql(
                    gas_query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )
                if len(gas_df) > 0:
                    gas_df["date"] = pd.to_datetime(gas_df["date"])
                    fuel_data.append(gas_df)
            except:
                pass  # Tabela może być pusta

        if not fuel_data:
            return pd.DataFrame()

        # Merge fuel data
        fuel_df = fuel_data[0]
        for df in fuel_data[1:]:
            fuel_df = fuel_df.merge(df, on="date", how="outer")

        # Interpolacja dla godzinowych danych
        hourly_range = pd.date_range(
            start=start_date, end=end_date, freq="H", name="dtime"
        )

        # Expand daily data to hourly
        expanded_rows = []
        for _, row in fuel_df.iterrows():
            day_start = pd.Timestamp(row["date"]).floor("D")
            for hour in range(24):
                hourly_time = day_start + timedelta(hours=hour)
                if hourly_time in hourly_range:
                    new_row = row.drop("date").to_dict()
                    new_row["dtime"] = hourly_time
                    expanded_rows.append(new_row)

        if expanded_rows:
            result_df = pd.DataFrame(expanded_rows)
            result_df.set_index("dtime", inplace=True)
            return result_df

        return pd.DataFrame()

    def _get_market_features(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Pobiera dane rynkowe (SDAC, intraday, market position)"""

        # SDAC prices
        sdac_query = text("""
        SELECT 
            dtime_utc as dtime,
            csdac_pln as sdac_price_pln
        FROM sdac_prices
        WHERE dtime_utc BETWEEN :start_date AND :end_date
        """)

        # Intraday volume
        intraday_query = text("""
        SELECT 
            dtime_utc as dtime,
            SUM(volume) as total_intraday_volume,
            COUNT(DISTINCT market_type) as active_intraday_markets
        FROM intraday_trading_volume
        WHERE dtime_utc BETWEEN :start_date AND :end_date
        GROUP BY dtime_utc
        """)

        # Market position
        position_query = text("""
        SELECT 
            dtime_utc as dtime,
            sk_cost as market_position_mwh
        FROM aggregated_market_position 
        WHERE dtime_utc BETWEEN :start_date AND :end_date
        """)

        market_data = []

        with self.engine.connect() as conn:
            # SDAC
            try:
                sdac_df = pd.read_sql(
                    sdac_query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )
                if len(sdac_df) > 0:
                    sdac_df["dtime"] = pd.to_datetime(sdac_df["dtime"])
                    sdac_df.set_index("dtime", inplace=True)
                    market_data.append(sdac_df)
            except Exception as e:
                logger.warning(f"SDAC data error: {e}")

            # Intraday
            try:
                intraday_df = pd.read_sql(
                    intraday_query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )
                if len(intraday_df) > 0:
                    intraday_df["dtime"] = pd.to_datetime(intraday_df["dtime"])
                    intraday_df.set_index("dtime", inplace=True)
                    market_data.append(intraday_df)
            except Exception as e:
                logger.warning(f"Intraday data error: {e}")

            # Market position
            try:
                position_df = pd.read_sql(
                    position_query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )
                if len(position_df) > 0:
                    position_df["dtime"] = pd.to_datetime(position_df["dtime"])
                    position_df.set_index("dtime", inplace=True)
                    market_data.append(position_df)
            except Exception as e:
                logger.warning(f"Market position data error: {e}")

        # Merge all market data
        if market_data:
            result_df = market_data[0]
            for df in market_data[1:]:
                result_df = result_df.join(df, how="outer")
            return result_df

        return pd.DataFrame()

    def _add_lag_features(self, df: pd.DataFrame, target_col: str) -> pd.DataFrame:
        """Dodaje opóźnione cechy (lags) istotne dla szeregów czasowych"""

        # Price lags (poprzednie ceny)
        for lag in [1, 2, 3, 6, 12, 24, 48, 168]:  # 1h, 2h, 3h, 6h, 12h, 1d, 2d, 1w
            df[f"{target_col}_lag_{lag}h"] = df[target_col].shift(lag)

        # Demand lags (jeśli dostępne)
        if "demand" in df.columns:
            for lag in [1, 24, 168]:
                df[f"demand_lag_{lag}h"] = df["demand"].shift(lag)

        # Generation lags
        if "total_generation" in df.columns:
            for lag in [1, 24]:
                df[f"generation_lag_{lag}h"] = df["total_generation"].shift(lag)

        return df

    def _add_rolling_statistics(
        self, df: pd.DataFrame, target_col: str
    ) -> pd.DataFrame:
        """Dodaje rolling statistics używając wydajnych funkcji pandas"""

        windows = [6, 12, 24, 48, 168]  # 6h, 12h, 24h, 48h, 168h (week)

        for window in windows:
            # Price statistics
            df[f"{target_col}_mean_{window}h"] = df[target_col].rolling(window).mean()
            df[f"{target_col}_std_{window}h"] = df[target_col].rolling(window).std()
            df[f"{target_col}_min_{window}h"] = df[target_col].rolling(window).min()
            df[f"{target_col}_max_{window}h"] = df[target_col].rolling(window).max()

            # Demand statistics (jeśli dostępne)
            if "demand" in df.columns:
                df[f"demand_mean_{window}h"] = df["demand"].rolling(window).mean()
                df[f"demand_volatility_{window}h"] = df["demand"].rolling(window).std()

        # Price momentum and changes
        df[f"{target_col}_change_1h"] = df[target_col].pct_change(1)
        df[f"{target_col}_change_24h"] = df[target_col].pct_change(24)

        # Volatility indicators
        df[f"{target_col}_volatility_24h"] = df[target_col].rolling(24).std()

        return df

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dodaje cechy czasowe i Fouriera dla sezonowości"""

        # Już mamy podstawowe cechy czasowe z base data

        # Fourier features dla sezonowości
        # Daily seasonality (24h cycle)
        df["sin_hour"] = np.sin(2 * np.pi * df["hour_of_day"] / 24)
        df["cos_hour"] = np.cos(2 * np.pi * df["hour_of_day"] / 24)

        # Weekly seasonality
        df["sin_dow"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
        df["cos_dow"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

        # Monthly seasonality
        df["sin_month"] = np.sin(2 * np.pi * df["month"] / 12)
        df["cos_month"] = np.cos(2 * np.pi * df["month"] / 12)

        # Peak/Off-peak indicators
        df["is_peak_hours"] = df["hour_of_day"].apply(
            lambda x: 1 if 7 <= x <= 22 else 0
        )
        df["is_super_peak"] = df["hour_of_day"].apply(
            lambda x: 1 if 17 <= x <= 20 else 0  # Evening peak
        )

        return df

    def _add_timescale_aggregations(
        self, df: pd.DataFrame, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Dodaje zaawansowane agregacje używając TimescaleDB"""

        # Time bucket aggregations dla większych okien czasowych
        time_bucket_query = text("""
        WITH daily_stats AS (
            SELECT 
                time_bucket('1 day', dtime_utc) AS day_bucket,
                AVG(price) as daily_avg_price,
                MAX(price) as daily_max_price,
                MIN(price) as daily_min_price,
                STDDEV(price) as daily_price_volatility
            FROM energy_prices
            WHERE dtime_utc BETWEEN :start_date AND :end_date
            GROUP BY day_bucket
        ),
        weekly_stats AS (
            SELECT 
                time_bucket('7 days', dtime_utc) AS week_bucket,
                AVG(price) as weekly_avg_price,
                MAX(price) as weekly_max_price
            FROM energy_prices  
            WHERE dtime_utc BETWEEN :start_date AND :end_date
            GROUP BY week_bucket
        )
        SELECT 
            ep.dtime_utc as dtime,
            ds.daily_avg_price,
            ds.daily_max_price,
            ds.daily_min_price,
            ds.daily_price_volatility,
            ws.weekly_avg_price,
            ws.weekly_max_price,
            
            -- Z-score normalization vs daily mean
            CASE WHEN ds.daily_price_volatility > 0 
                 THEN (ep.price - ds.daily_avg_price) / ds.daily_price_volatility
                 ELSE 0 END as price_zscore_daily
                 
        FROM energy_prices ep
        LEFT JOIN daily_stats ds ON time_bucket('1 day', ep.dtime_utc) = ds.day_bucket  
        LEFT JOIN weekly_stats ws ON time_bucket('7 days', ep.dtime_utc) = ws.week_bucket
        WHERE ep.dtime_utc BETWEEN :start_date AND :end_date
        ORDER BY ep.dtime_utc
        """)

        try:
            with self.engine.connect() as conn:
                agg_df = pd.read_sql(
                    time_bucket_query,
                    conn,
                    params={"start_date": start_date, "end_date": end_date},
                )

            if len(agg_df) > 0:
                agg_df["dtime"] = pd.to_datetime(agg_df["dtime"])
                agg_df.set_index("dtime", inplace=True)
                agg_df = agg_df.drop(["dtime"], axis=1, errors="ignore")

                # Merge with main dataframe
                df = df.join(agg_df, how="left")

        except Exception as e:
            logger.warning(f"TimescaleDB aggregation error: {e}")

        return df

    def _merge_timeseries(
        self, base_df: pd.DataFrame, feature_df: pd.DataFrame, on_col: str
    ) -> pd.DataFrame:
        """Łączy szeregi czasowe z zachowaniem indeksu czasowego"""
        if len(feature_df) == 0:
            return base_df

        return base_df.join(feature_df, how="left")

    def get_feature_importance_analysis(
        self, df: pd.DataFrame, target_col: str
    ) -> Dict[str, float]:
        """Analiza ważności cech dla target variable"""
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.feature_selection import mutual_info_regression

        # Prepare data
        feature_cols = [col for col in df.columns if col != target_col]
        X = df[feature_cols].fillna(0)
        y = df[target_col].fillna(method="ffill")

        # Mutual information
        mi_scores = mutual_info_regression(X, y)
        mi_importance = dict(zip(feature_cols, mi_scores))

        # Random Forest importance
        rf = RandomForestRegressor(n_estimators=100, random_state=42)
        rf.fit(X, y)
        rf_importance = dict(zip(feature_cols, rf.feature_importances_))

        # Combined importance score
        combined_importance = {}
        for feature in feature_cols:
            combined_importance[feature] = (
                mi_importance.get(feature, 0) * 0.5
                + rf_importance.get(feature, 0) * 0.5
            )

        return dict(
            sorted(combined_importance.items(), key=lambda x: x[1], reverse=True)
        )

    def export_ml_dataset(self, df: pd.DataFrame, output_path: str):
        """Eksportuje gotowy dataset do ML"""
        # Clean up data
        df_clean = df.copy()

        # Handle infinities and NaN
        df_clean = df_clean.replace([np.inf, -np.inf], np.nan)
        df_clean = df_clean.fillna(method="ffill").fillna(0)

        # Export to multiple formats
        df_clean.to_csv(f"{output_path}.csv")
        df_clean.to_parquet(f"{output_path}.parquet")  # Better for large datasets

        # Export metadata
        metadata = {
            "rows": len(df_clean),
            "columns": len(df_clean.columns),
            "date_range": {
                "start": str(df_clean.index.min()),
                "end": str(df_clean.index.max()),
            },
            "missing_data_pct": (
                df_clean.isnull().sum() / len(df_clean) * 100
            ).to_dict(),
            "column_types": df_clean.dtypes.astype(str).to_dict(),
        }

        import json

        with open(f"{output_path}_metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(
            f"Dataset exported: {output_path} ({len(df_clean)} rows, {len(df_clean.columns)} features)"
        )

        return df_clean


# Utility functions dla szybkich analiz
def quick_correlation_analysis(engine, start_date: str, end_date: str):
    """Szybka analiza korelacji między kluczowymi zmiennymi"""

    query = text("""
    SELECT 
        ep.dtime_utc,
        ep.price,
        gbs.demand,
        gbs.wi as wind_generation,
        gbs.pv as solar_generation,
        gbs.swm_p as cross_border_flow,
        wf.temperatura_prognoza as temperature_2m_celsius,
        wf.predkosc_wiatru_prognoza as wind_speed_10m_kmh
    FROM energy_prices ep
    LEFT JOIN generation_by_source gbs ON ep.dtime_utc = gbs.dtime_utc
    LEFT JOIN weather_forecast wf ON DATE(ep.dtime_utc) = DATE(wf.czas_prognozy)
    WHERE ep.dtime_utc BETWEEN :start_date AND :end_date
    ORDER BY ep.dtime_utc
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query, conn, params={"start_date": start_date, "end_date": end_date}
        )

    # Correlation matrix
    corr_matrix = df.select_dtypes(include=[np.number]).corr()

    print(" KORELACJA Z CENĄ ENERGII:")
    price_corr = corr_matrix["price"].abs().sort_values(ascending=False)
    for var, corr in price_corr.items():
        if var != "price":
            print(f"   {var}: {corr:.3f}")

    return corr_matrix


def analyze_price_patterns(engine):
    """Analizuje wzorce cen w danych"""

    query = text("""
    SELECT 
        EXTRACT(hour FROM dtime_utc) as hour,
        EXTRACT(dow FROM dtime_utc) as dow,
        AVG(price) as avg_price,
        STDDEV(price) as price_volatility,
        COUNT(*) as observations
    FROM energy_prices
    GROUP BY EXTRACT(hour FROM dtime_utc), EXTRACT(dow FROM dtime_utc)
    ORDER BY dow, hour
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    print(" WZORCE CEN:")
    print("Średnia cena według godziny:")
    hourly_avg = df.groupby("hour")["avg_price"].mean().round(2)
    for hour, price in hourly_avg.items():
        print(f"   {hour:02d}:00 - {price} PLN/MWh")

    print("\nŚrednia cena według dnia tygodnia:")
    dow_names = [
        "Niedziela",
        "Poniedziałek",
        "Wtorek",
        "Środa",
        "Czwartek",
        "Piątek",
        "Sobota",
    ]
    daily_avg = df.groupby("dow")["avg_price"].mean().round(2)
    for dow, price in daily_avg.items():
        print(f"   {dow_names[int(dow)]}: {price} PLN/MWh")

    return df
