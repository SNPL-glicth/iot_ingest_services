from __future__ import annotations

"""Job batch que enriquece predicciones anómalas con explicaciones vía ai-explainer.

Este job:
- NO ejecuta modelos de ML.
- SOLO lee salidas ya calculadas en dbo.predictions.
- Llama al microservicio ai-explainer (Ollama) BAJO CONDICIÓN de anomalía.
- Escribe la explicación estructurada como JSON en el campo `explanation`.

De esta forma, el pipeline principal de ingestión y ML no depende de Ollama
ni se bloquea si el servicio de explicación falla.
"""

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Connection

from iot_ingest_services.common.db import get_engine


@dataclass
class RunnerConfig:
    anomaly_threshold: float = 0.8
    batch_size: int = 50
    once: bool = True


def _get_db_conn() -> Connection:
    engine = get_engine()
    return engine.connect()  # caller se encarga de cerrar


def _iter_anomalous_predictions(conn: Connection, cfg: RunnerConfig) -> Iterable[Dict[str, Any]]:
    """Selecciona predicciones candidatas para explicación.

    Criterio inicial:
    - is_anomaly = 1
    - anomaly_score >= :threshold
    - explanation IS NULL OR explanation = '' (aún sin explicar)
    """

    rows = conn.execute(
        text(
            """
            SELECT TOP (:limit)
              id,
              sensor_id,
              device_id,
              predicted_value,
              confidence,
              anomaly_score,
              trend,
              target_timestamp
            FROM dbo.predictions
            WHERE is_anomaly = 1
              AND anomaly_score >= :threshold
              AND (explanation IS NULL OR LTRIM(RTRIM(CAST(explanation AS nvarchar(max)))) = '')
            ORDER BY target_timestamp DESC
            """
        ),
        {"threshold": cfg.anomaly_threshold, "limit": cfg.batch_size},
    ).mappings()

    for r in rows:
        yield dict(r)


def _build_expected_range(conn: Connection, sensor_id: int) -> str:
    """Intenta derivar el rango esperado a partir de alert_thresholds.

    Si no hay configuración, devuelve "unknown" para que el LLM
    rebaje la confianza y lo haga explícito.
    """

    thr = conn.execute(
        text(
            """
            SELECT TOP 1 threshold_value_min, threshold_value_max
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id AND is_active = 1
            ORDER BY id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not thr:
        return "unknown"

    vmin, vmax = thr
    if vmin is not None and vmax is not None:
        return f"{float(vmin)}-{float(vmax)}"
    if vmin is not None:
        return f">= {float(vmin)}"
    if vmax is not None:
        return f"<= {float(vmax)}"
    return "unknown"


async def _call_ai_explainer(payload: Dict[str, Any]) -> Dict[str, Any]:
    base_url = os.getenv("AI_EXPLAINER_URL", "http://localhost:8003")
    url = base_url.rstrip("/") + "/explain/anomaly"

    async with httpx.AsyncClient(timeout=1.0) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


async def _process_batch(conn: Connection, cfg: RunnerConfig) -> int:
    to_process = list(_iter_anomalous_predictions(conn, cfg))
    if not to_process:
        return 0

    updated = 0
    for row in to_process:
        pred_id = int(row["id"])
        sensor_id = int(row["sensor_id"])

        expected_range = _build_expected_range(conn, sensor_id)

        model_output = {
            "metric": "generic",  # el backend UI no depende de este valor exacto
            "observed_value": float(row["predicted_value"]),
            "expected_range": expected_range,
            "anomaly_score": float(row["anomaly_score"]),
            "model": "sklearn_regression_iforest",
            "model_version": "1.0.0",
        }

        explainer_input = {
            "context": "industrial_iot_monitoring",
            "model_output": model_output,
        }

        try:
            explanation_json = await _call_ai_explainer(explainer_input)
        except Exception as exc:  # noqa: BLE001
            # Si falla ai-explainer no bloqueamos el pipeline; solo registramos el error.
            print(f"[ai_explainer_runner] error llamando ai-explainer pred_id={pred_id}: {exc}")
            continue

        # Guardamos la explicación estructurada como JSON serializado en predictions.explanation
        conn.execute(
            text(
                """
                UPDATE dbo.predictions
                SET explanation = :explanation
                WHERE id = :id
                """
            ),
            {"id": pred_id, "explanation": json.dumps(explanation_json, ensure_ascii=False)},
        )

        updated += 1

    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Enriquece predicciones anómalas con ai-explainer")
    parser.add_argument("--anomaly-threshold", type=float, default=0.8)
    parser.add_argument("--batch-size", type=int, default=50)

    args = parser.parse_args()
    cfg = RunnerConfig(
        anomaly_threshold=args.anomaly_threshold,
        batch_size=args.batch_size,
        once=True,
    )

    import asyncio

    with _get_db_conn() as conn:
        updated = asyncio.run(_process_batch(conn, cfg))
        print(f"[ai_explainer_runner] explicaciones generadas: {updated}")


if __name__ == "__main__":  # pragma: no cover
    main()
