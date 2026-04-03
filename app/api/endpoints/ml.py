import os
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sqlmodel import create_engine

from app.core.config import settings

router = APIRouter(tags=["machine-learning"])
engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI), pool_pre_ping=True)


class MLPrepareRequest(BaseModel):
    start_date: str | None = None  # Format m.in. YYYY-MM-DD
    end_date: str | None = None  # Format m.in. YYYY-MM-DD
    target_column: str = "all"
    test_size: float = 0.2
    val_size: float = 0.1
    shuffle: bool = False  # Dla szeregów czasowych shuffle=False


@router.post("/prepare_data")
def prepare_data(req: MLPrepareRequest):
    """
    Endpoint pobierający dane z bazy za określony czas,
    dzielący je na train/test/val oraz zapisujący obiekty (.pkl) gotowe do treningu.
    """
    if not req.start_date or not req.end_date:
        query = "SELECT * FROM ml_features ORDER BY ts"
        start_ts = None
        end_ts = None
        save_dir = Path("/app/data/ml_ready_objects/all_data")
    else:
        try:
            start_ts = datetime.fromisoformat(req.start_date)
            end_ts = datetime.fromisoformat(req.end_date)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Zły format daty. Użyj YYYY-MM-DD",
            )
        query = f"SELECT * FROM ml_features WHERE ts >= '{start_ts}' AND ts <= '{end_ts}' ORDER BY ts"
        save_dir = Path(f"/app/data/ml_ready_objects/{start_ts.date()}-{end_ts.date()}")

    if save_dir.exists() and start_ts and end_ts:
        return {
            "status": "success",
            "message": f"Dane ML z przedziału {start_ts} do {end_ts}. Już istnieją, nie nadpisano.",
            "save_dir": save_dir,
        }
    save_dir.mkdir(parents=True, exist_ok=False)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        raise HTTPException(status_code=404, detail="Brak danych we wskazanym okresie")

    df = df.set_index("ts")
    df = df.drop(
        columns=["id", "business_date", "created_at", "updated_at"], errors="ignore"
    )

    # Czyszczenie
    df = df.ffill().bfill()
    for col in ["is_weekend", "is_peak_hour"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    # Cykliczne
    if "wind_direction_forecast" in df.columns:
        wind_rad = np.deg2rad(df["wind_direction_forecast"])
        df["wind_direction_sin"] = np.sin(wind_rad)
        df["wind_direction_cos"] = np.cos(wind_rad)
        df = df.drop(columns=["wind_direction_forecast"])

    # X i y
    if req.target_column not in df.columns:
        raise HTTPException(
            status_code=400,
            detail=f"Kolumna docelowa {req.target_column} nie istnieje.",
        )

    X = df.drop(columns=[req.target_column])
    y = df[req.target_column]

    # 2. Podział na zbiory (Train, Val, Test)
    # Najpierw wydzielamy Test
    X_train_val, X_test, y_train_val, y_test = train_test_split(
        X, y, test_size=req.test_size, shuffle=req.shuffle
    )

    # Z pozostałości (Train+Val) wydzielamy Val (proporcjonalnie)
    val_relative_size = req.val_size / (1.0 - req.test_size)
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_val, y_train_val, test_size=val_relative_size, shuffle=req.shuffle
    )

    # 3. Normalizacja / Skalowanie
    # Robimy fit_transform TYLKO na zbiorze treningowym, aby uniknąć Data Leakage (wycieku informacji z przyszłości)
    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()

    # Skalujemy cechy ciągłe X
    X_train_scaled = scaler_X.fit_transform(X_train)
    X_val_scaled = scaler_X.transform(X_val)
    X_test_scaled = scaler_X.transform(X_test)

    # Skalujemy cel y (wymaga reshape ponieważ MinMaxScaler oczekuje (n_samples, n_features))
    y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1)).flatten()
    y_val_scaled = scaler_y.transform(y_val.values.reshape(-1, 1)).flatten()
    y_test_scaled = scaler_y.transform(y_test.values.reshape(-1, 1)).flatten()

    # Opakowanie w DataFrames dla wygody (opcjonalne)
    X_train = pd.DataFrame(X_train_scaled, index=X_train.index, columns=X_train.columns)
    X_val = pd.DataFrame(X_val_scaled, index=X_val.index, columns=X_val.columns)
    X_test = pd.DataFrame(X_test_scaled, index=X_test.index, columns=X_test.columns)

    # 4. Zapis do plików PKL
    datasets = {
        "X_train.pkl": X_train,
        "X_val.pkl": X_val,
        "X_test.pkl": X_test,
        "y_train.pkl": pd.Series(
            y_train_scaled, index=y_train.index, name=y_train.name
        ),
        "y_val.pkl": pd.Series(y_val_scaled, index=y_val.index, name=y_val.name),
        "y_test.pkl": pd.Series(y_test_scaled, index=y_test.index, name=y_test.name),
    }

    for filename, data in datasets.items():
        filepath = os.path.join(save_dir, filename)
        joblib.dump(data, filepath)

    # Zapis skalera (niezbędne do odwrócenia predykcji -> inverse_transform)
    joblib.dump(scaler_X, os.path.join(save_dir, "scaler_X.pkl"))
    joblib.dump(scaler_y, os.path.join(save_dir, "scaler_y.pkl"))

    return {
        "status": "success",
        "message": f"Dane ML z przedziału {start_ts} do {end_ts} podzielono na {len(X_train)} (train), {len(X_val)} (val), {len(X_test)} (test) wierszy i zapisano.",
        "save_dir": save_dir,
        "train_samples": len(X_train),
        "val_samples": len(X_val),
        "test_samples": len(X_test),
    }
