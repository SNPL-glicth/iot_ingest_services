"""CLI entry point for the batch runner."""

from __future__ import annotations

import argparse
import logging
import time

from iot_machine_learning.ml_service.config.feature_flags import get_feature_flags

from .config import RunnerConfig
from .runner import run_once

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    p = argparse.ArgumentParser(description="ML batch runner (predictions + ml_events)")
    p.add_argument("--window", type=int, default=60)
    p.add_argument("--horizon-minutes", type=int, default=10)
    p.add_argument("--dedupe-minutes", type=int, default=10)
    p.add_argument("--sleep-seconds", type=float, default=60.0)
    p.add_argument("--once", action="store_true", help="run a single iteration and exit")
    args = p.parse_args()

    cfg = RunnerConfig(
        window=args.window,
        horizon_minutes=args.horizon_minutes,
        dedupe_minutes=args.dedupe_minutes,
        sleep_seconds=args.sleep_seconds,
        once=bool(args.once),
    )

    flags = get_feature_flags()
    logger.info("ML Batch Runner started")
    logger.info("Config: window=%d, horizon=%dmin, sleep=%.1fs", cfg.window, cfg.horizon_minutes, cfg.sleep_seconds)
    logger.info(
        "Enterprise flags: use=%s sensors=%s rollback=%s",
        flags.ML_BATCH_USE_ENTERPRISE,
        flags.ML_BATCH_ENTERPRISE_SENSORS,
        flags.ML_ROLLBACK_TO_BASELINE,
    )

    while True:
        try:
            run_once(cfg, flags=flags)
            if cfg.once:
                return
            logger.info("Iteración completada, esperando %.1fs...", cfg.sleep_seconds)
            time.sleep(cfg.sleep_seconds)
        except Exception as e:
            logger.error("Error en iteración: %s", e)
            if cfg.once:
                raise
            logger.info("Continuando con siguiente iteración...")
            time.sleep(cfg.sleep_seconds)


if __name__ == "__main__":
    main()
