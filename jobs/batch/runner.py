"""Batch runner orchestrator — parallel ML batch runner (E-4 fix)."""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

from iot_ingest_services.common.db import get_engine
from iot_machine_learning.infrastructure.ml.engines.baseline_engine import (
    BaselineConfig,
    predict_moving_average,
    BASELINE_MOVING_AVERAGE,
)
from iot_machine_learning.ml_service.config.feature_flags import FeatureFlags
from iot_machine_learning.ml_service.metrics.performance_metrics import MetricsCollector
from iot_machine_learning.ml_service.runners.bridge_config.batch_flags import (
    should_use_enterprise,
)

from .config import RunnerConfig
from .db_queries import (
    utc_now,
    ensure_watermark,
    get_last_reading_id,
    get_sensor_max_reading_id,
    load_recent_values,
    load_recent_values_with_timestamps,
    load_first_actual_after_timestamp,
    get_device_id_for_sensor,
    get_or_create_active_model_id,
    update_watermark,
    list_active_sensors,
)
from .prediction import insert_prediction
from .threshold_events import eval_pred_threshold_and_create_event
from .enterprise import (
    get_enterprise_adapter, predict_enterprise, predict_enterprise_with_window,
)

logger = logging.getLogger(__name__)

_DEFAULT_PARALLEL_WORKERS = 1  # 1 = sequential (backward compat)


def _enterprise_preloaded(adapter, sensor_id: int, vt: list[tuple[float, float]]):
    """Build SensorWindow from preloaded data and predict."""
    from iot_machine_learning.domain.entities.sensor_reading import (
        SensorReading, SensorWindow,
    )
    readings = [
        SensorReading(sensor_id=sensor_id, value=v, timestamp=t)
        for v, t in vt if v == v and t > 0
    ]
    if len(readings) < 2:
        return None
    window = SensorWindow(sensor_id=sensor_id, readings=readings)
    return predict_enterprise_with_window(adapter, window)


def _process_sensor(engine, cfg, flags, adapter, sensor_id):
    """Process ONE sensor in its own transaction. Returns 'enterprise'|'baseline'|None."""
    with engine.begin() as conn:
        ensure_watermark(conn, sensor_id)
        last_id = get_last_reading_id(conn, sensor_id)
        max_id = get_sensor_max_reading_id(conn, sensor_id)
        if max_id is None:
            return None
        if last_id is not None and max_id <= last_id:
            return None

        use_ent = adapter is not None and should_use_enterprise(sensor_id, flags)
        use_pre = use_ent and getattr(flags, "ML_ENTERPRISE_USE_PRELOADED_DATA", True)

        predicted_value, confidence, engine_tag = _predict_sensor(
            conn, cfg, adapter, sensor_id, use_ent, use_pre,
        )
        if predicted_value is None:
            update_watermark(conn, sensor_id=sensor_id, last_reading_id=max_id)
            return None

        model_id = get_or_create_active_model_id(conn, sensor_id, BASELINE_MOVING_AVERAGE)
        device_id = get_device_id_for_sensor(conn, sensor_id)
        target_ts = utc_now() + timedelta(minutes=cfg.horizon_minutes)

        pred_id = insert_prediction(
            conn, model_id=model_id, sensor_id=sensor_id, device_id=device_id,
            predicted_value=predicted_value, confidence=confidence,
            target_ts_utc=target_ts,
        )
        eval_pred_threshold_and_create_event(
            conn, sensor_id=sensor_id, device_id=device_id,
            prediction_id=pred_id, predicted_value=predicted_value,
            dedupe_minutes=cfg.dedupe_minutes,
        )
        update_watermark(conn, sensor_id=sensor_id, last_reading_id=max_id)

        if adapter is not None and engine_tag == "enterprise":
            try:
                pred_ts = target_ts.timestamp()
                actual = load_first_actual_after_timestamp(conn, sensor_id, pred_ts)
                if actual is not None:
                    adapter.record_actual(
                        actual_value=actual,
                        series_id=str(sensor_id),
                    )
            except Exception as _fb_err:
                logger.debug("feedback_record_actual failed sensor=%d: %s", sensor_id, _fb_err)

        return engine_tag


def _predict_sensor(conn, cfg, adapter, sensor_id, use_ent, use_pre):
    """Run prediction for a sensor. Returns (value, confidence, tag) or (None,None,None)."""
    if use_pre:
        vt = load_recent_values_with_timestamps(conn, sensor_id, cfg.window)
        if len(vt) < 2:
            return None, None, None
        values = [v for v, _ in vt]
        result = _enterprise_preloaded(adapter, sensor_id, vt)
        if result is not None:
            return result[0], result[1], "enterprise"
        bc = BaselineConfig(window=cfg.window)
        pv, c = predict_moving_average(values, bc)
        return pv, c, "baseline"

    values = load_recent_values(conn, sensor_id, cfg.window)
    if len(values) < 2:
        return None, None, None
    if use_ent:
        result = predict_enterprise(adapter, sensor_id, cfg.window)
        if result is not None:
            return result[0], result[1], "enterprise"
    bc = BaselineConfig(window=cfg.window)
    pv, c = predict_moving_average(values, bc)
    return pv, c, "baseline"


def run_once(cfg: RunnerConfig, flags: FeatureFlags | None = None) -> None:
    """Batch cycle — parallel sensor processing."""
    engine = get_engine()
    if flags is None:
        flags = FeatureFlags()

    any_enterprise = (
        flags.ML_BATCH_USE_ENTERPRISE or flags.ML_BATCH_ENTERPRISE_SENSORS
    ) and not flags.ML_ROLLBACK_TO_BASELINE
    adapter = get_enterprise_adapter(engine, flags) if any_enterprise else None

    num_workers = int(os.getenv("ML_BATCH_PARALLEL_WORKERS",
                                str(_DEFAULT_PARALLEL_WORKERS)))
    num_workers = max(1, num_workers)

    with engine.begin() as conn:
        sensor_ids = list_active_sensors(conn)

    t0 = time.monotonic()
    success, failed, ent_count, base_count = 0, 0, 0, 0

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {
            pool.submit(_process_sensor, engine, cfg, flags, adapter, sid): sid
            for sid in sensor_ids
        }
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                tag = fut.result()
                success += 1
                if tag == "enterprise":
                    ent_count += 1
                elif tag == "baseline":
                    base_count += 1
            except Exception as exc:
                failed += 1
                logger.error("batch_sensor_failed sensor=%d err=%s", sid, exc)

    cycle_ms = (time.monotonic() - t0) * 1000
    mc = MetricsCollector.get_instance()
    mc.record_reading_processed(cycle_ms)
    for _ in range(ent_count + base_count):
        mc.record_prediction(cycle_ms / max(ent_count + base_count, 1))
    for _ in range(failed):
        mc.record_error()
    logger.info(
        "batch_cycle ms=%.1f sensors=%d ok=%d fail=%d enterprise=%d baseline=%d workers=%d",
        cycle_ms, len(sensor_ids), success, failed, ent_count, base_count, num_workers,
    )


def main() -> None:
    from .cli import main as _cli_main
    _cli_main()

if __name__ == "__main__":
    main()
