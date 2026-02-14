"""Enterprise bridge for batch runner (feature-flag controlled).

Lazy-init enterprise prediction adapter from iot_machine_learning.
Falls back gracefully if unavailable.
"""

from __future__ import annotations

import logging
from typing import Optional

from iot_machine_learning.ml_service.config.feature_flags import FeatureFlags

logger = logging.getLogger(__name__)

_enterprise_adapter = None
_enterprise_init_failed = False


def get_enterprise_adapter(engine, flags: FeatureFlags):
    """Lazy-init enterprise prediction adapter. Returns None if unavailable."""
    global _enterprise_adapter, _enterprise_init_failed
    if _enterprise_init_failed:
        return None
    if _enterprise_adapter is not None:
        return _enterprise_adapter
    try:
        from iot_machine_learning.ml_service.runners.wiring.container import (
            BatchEnterpriseContainer,
        )
        container = BatchEnterpriseContainer(engine=engine, flags=flags)
        _enterprise_adapter = container.get_prediction_adapter()
        logger.info("Enterprise prediction adapter initialized")
        return _enterprise_adapter
    except Exception as e:
        _enterprise_init_failed = True
        logger.warning("Enterprise adapter init failed: %s", e)
        return None


def predict_enterprise(
    adapter, sensor_id: int, window: int
) -> Optional[tuple[float, float]]:
    """Call enterprise stack. Returns (predicted_value, confidence) or None."""
    try:
        result = adapter.predict(sensor_id=sensor_id, window_size=window)
        return result.predicted_value, result.confidence
    except Exception as e:
        logger.error("Enterprise prediction failed sensor=%s: %s", sensor_id, e)
        return None


def predict_enterprise_with_window(
    adapter, sensor_window,
) -> Optional[tuple[float, float]]:
    """Call enterprise stack with pre-loaded SensorWindow (no double query).

    Returns (predicted_value, confidence) or None on failure.
    """
    try:
        result = adapter.predict_with_window(sensor_window=sensor_window)
        return result.predicted_value, result.confidence
    except Exception as e:
        logger.error(
            "Enterprise prediction (preloaded) failed sensor=%s: %s",
            sensor_window.sensor_id, e,
        )
        return None


def reset_enterprise_state() -> None:
    """Reset module state (for testing)."""
    global _enterprise_adapter, _enterprise_init_failed
    _enterprise_adapter = None
    _enterprise_init_failed = False
