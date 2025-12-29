from __future__ import annotations

from datetime import datetime

import numpy as np
from sqlalchemy.engine import Connection
from sklearn.linear_model import LinearRegression, Ridge

from iot_ingest_services.ml_service.config.ml_config import RegressionConfig
from iot_ingest_services.ml_service.models.regression_model import RegressionModel
from iot_ingest_services.ml_service.repository.sensor_repository import SensorSeries, load_sensor_series


def _build_sklearn_model(model_type: str):
    if model_type == "linear":
        return LinearRegression()
    if model_type == "ridge":
        return Ridge(alpha=1.0)
    raise ValueError(f"Unsupported model_type: {model_type}")


def _series_to_time_features(series: SensorSeries) -> tuple[np.ndarray, np.ndarray, float]:
    """Convierte la serie en X, y y devuelve minutos del último punto.

    X: minutos desde el primer timestamp.
    y: valores del sensor.
    """
    if not series.timestamps:
        raise ValueError("Serie vacía")

    t0: datetime = series.timestamps[0]
    last_ts: datetime = series.timestamps[-1]

    xs: list[list[float]] = []
    for ts in series.timestamps:
        delta = ts - t0
        minutes = delta.total_seconds() / 60.0
        xs.append([minutes])

    X = np.asarray(xs, dtype=float)
    y = np.asarray(series.values, dtype=float)

    last_minutes = (last_ts - t0).total_seconds() / 60.0
    return X, y, last_minutes


def train_regression_for_sensor(
    conn: Connection,
    sensor_id: int,
    cfg: RegressionConfig,
) -> tuple[RegressionModel | None, float | None]:
    """Entrena regresión Linear/Ridge para un sensor.

    Devuelve (modelo, last_minutes). Si no hay suficientes puntos, devuelve (None, None).
    """
    series = load_sensor_series(conn, sensor_id, limit_points=cfg.window_points)
    if len(series.values) < cfg.min_points:
        return None, None

    X, y, last_minutes = _series_to_time_features(series)
    model = _build_sklearn_model(cfg.model_type)
    model.fit(X, y)

    r2 = float(model.score(X, y)) if X.shape[0] >= 2 else 0.0

    reg_model = RegressionModel(
        sensor_id=sensor_id,
        coef_=float(model.coef_[0]),
        intercept_=float(model.intercept_),
        r2=r2,
        horizon_minutes=cfg.horizon_minutes,
    )

    return reg_model, last_minutes


def predict_future_value(model: RegressionModel, last_minutes: float) -> float:
    """Predice el próximo valor N minutos adelante usando el modelo lineal."""
    t_future = last_minutes + model.horizon_minutes
    y_hat = model.intercept_ + model.coef_ * t_future
    return float(y_hat)
