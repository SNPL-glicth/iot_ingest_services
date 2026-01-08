from __future__ import annotations

"""Runner sencillo para consumo online de lecturas por ML.

Este módulo demuestra cómo el ML online depende solo de la interfaz
`ReadingBroker` y de la base de datos, sin conocer la implementación
concreta del broker ni detalles de transporte.

En un MVP puedes ejecutarlo con el broker en memoria, y en el futuro
cambiar a otro broker sin tocar la lógica de ML.
"""

import logging
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from iot_ingest_services.common.db import get_engine
from iot_ingest_services.ml_service.config.ml_config import (
    DEFAULT_ML_CONFIG,
    OnlineBehaviorConfig,
)
from iot_ingest_services.ml_service.reading_broker import Reading, ReadingBroker
from iot_ingest_services.ml_service.sliding_window_buffer import (
    SlidingWindowBuffer,
    WindowStats,
)
from iot_ingest_services.ml_service.repository.sensor_repository import (
    get_device_id_for_sensor,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Estado por sensor y análisis de comportamiento
# ---------------------------------------------------------------------------


@dataclass
class SensorState:
    severity: str
    recommended_action: str
    behavior_pattern: str
    in_transient_anomaly: bool
    last_event_code: Optional[str] = None


@dataclass
class OnlineAnalysis:
    behavior_pattern: str
    is_curve_anomalous: bool
    has_microvariation: bool
    microvariation_delta: float
    new_transient_anomaly: bool
    recovered_transient: bool
    baseline_mean: float
    baseline_std: float
    last_value: float
    z_score_last: float
    slope_short: float
    slope_medium: float
    slope_long: float
    accel_short_vs_medium: float
    accel_medium_vs_long: float


class SimpleMlOnlineProcessor:
    """Procesador online usando SlidingWindowBuffer.

    - Mantiene ventanas deslizantes 1s/5s/10s por sensor.
    - Calcula agregados (avg, min, max, trend, std_dev) por ventana.
    - Modela el COMPORTAMIENTO del sensor (patrón + anomalías).
    - Solo inserta en ml_events cuando cambia el estado o el tipo de evento.
    """

    def __init__(self, cfg: OnlineBehaviorConfig | None = None) -> None:
        self._cfg: OnlineBehaviorConfig = cfg or DEFAULT_ML_CONFIG.online
        self._last_state: Dict[int, SensorState] = {}
        self._buffer = SlidingWindowBuffer(max_horizon_seconds=10.0)
        self._device_cache: Dict[int, int] = {}

    def handle_reading(self, reading: Reading) -> None:
        """Procesa una lectura y emite eventos en BD cuando cambia el estado.

        El procesamiento es puramente online: ventanas deslizantes en memoria y
        escritura de eventos en `ml_events` + `alert_notifications`.
        """

        sensor_id = reading.sensor_id
        stats_by_window = self._buffer.add_reading(
            sensor_id=sensor_id,
            value=float(reading.value),
            timestamp=float(reading.timestamp),
            windows=(1.0, 5.0, 10.0),
        )

        if not stats_by_window:
            # Todavía no hay datos suficientes
            return

        prev_state = self._last_state.get(sensor_id)
        analysis = self._analyze_windows(stats_by_window, prev_state)

        severity, action, explanation = self._build_explanation(
            sensor_type=reading.sensor_type,
            analysis=analysis,
        )

        # Actualizar estado lógico (incluyendo flag de anomalía transitoria)
        prev_in_transient = prev_state.in_transient_anomaly if prev_state else False
        in_transient = prev_in_transient or analysis.new_transient_anomaly
        if analysis.recovered_transient:
            in_transient = False

        new_state = SensorState(
            severity=severity,
            recommended_action=action,
            behavior_pattern=analysis.behavior_pattern,
            in_transient_anomaly=in_transient,
            last_event_code=prev_state.last_event_code if prev_state else None,
        )

        # Decidir si emitimos un evento de estado/comportamiento
        should_emit, event_code = self._should_emit_state_event(
            prev_state=prev_state,
            new_state=new_state,
            analysis=analysis,
        )

        if should_emit:
            event_type = self._map_severity_to_event_type(severity)
            title = self._build_title(event_code, severity, analysis.behavior_pattern)

            self._insert_ml_event(
                sensor_id=sensor_id,
                sensor_type=reading.sensor_type,
                severity_label=severity,
                event_type=event_type,
                event_code=event_code,
                title=title,
                explanation=explanation,
                recommended_action=action,
                analysis=analysis,
                ts_utc=float(reading.timestamp),
                prediction_id=None,
                extra_payload=None,
            )

            new_state.last_event_code = event_code

        # Guardar siempre el último estado para analizar patrones / transitorios
        self._last_state[sensor_id] = new_state

        # Autovalidación de predicción vs valor real (evento PREDICTION_DEVIATION)
        try:
            self._check_prediction_deviation(sensor_id=sensor_id, reading=reading, analysis=analysis)
        except Exception:
            logger.exception(
                "Error evaluando desviación de predicción para sensor_id=%s", sensor_id
            )

    # ------------------------------------------------------------------
    # Análisis de ventanas y clasificación de patrón
    # ------------------------------------------------------------------

    def _analyze_windows(
        self,
        stats_by_window: dict[str, WindowStats],
        prev_state: Optional[SensorState],
    ) -> OnlineAnalysis:
        cfg = self._cfg

        w1 = stats_by_window.get("w1")
        w5 = stats_by_window.get("w5")
        w10 = stats_by_window.get("w10")

        baseline = w10 or w5 or w1 or next(iter(stats_by_window.values()))

        baseline_mean = baseline.mean
        baseline_std = baseline.std_dev
        last_value = (w1 or baseline).last_value

        z_score = 0.0
        if baseline_std > 0:
            z_score = (last_value - baseline_mean) / baseline_std

        slope_short = w1.trend if w1 else 0.0
        slope_medium = w5.trend if w5 else slope_short
        slope_long = w10.trend if w10 else slope_medium

        accel_short_vs_medium = slope_short - slope_medium
        accel_medium_vs_long = slope_medium - slope_long

        is_curve_anomalous = any(
            abs(s) >= cfg.slope_anomaly_threshold
            for s in (slope_short, slope_medium, slope_long)
        ) or any(
            abs(a) >= cfg.accel_anomaly_threshold
            for a in (accel_short_vs_medium, accel_medium_vs_long)
        )

        delta = last_value - baseline_mean
        has_microvariation = (
            abs(delta) >= cfg.microvariation_min_delta
            and abs(z_score) >= cfg.microvariation_z_score
            and not is_curve_anomalous
        )

        prev_in_transient = prev_state.in_transient_anomaly if prev_state else False
        outlier_for_transient = abs(z_score) >= cfg.transient_z_score

        new_transient = not prev_in_transient and outlier_for_transient
        recovered = prev_in_transient and not outlier_for_transient

        behavior_pattern = self._classify_pattern(
            w1=w1,
            w5=w5,
            w10=w10,
            baseline_mean=baseline_mean,
            z_score_last=z_score,
            is_curve_anomalous=is_curve_anomalous,
            has_microvariation=has_microvariation,
        )

        return OnlineAnalysis(
            behavior_pattern=behavior_pattern,
            is_curve_anomalous=is_curve_anomalous,
            has_microvariation=has_microvariation,
            microvariation_delta=delta,
            new_transient_anomaly=new_transient,
            recovered_transient=recovered,
            baseline_mean=baseline_mean,
            baseline_std=baseline_std,
            last_value=last_value,
            z_score_last=z_score,
            slope_short=slope_short,
            slope_medium=slope_medium,
            slope_long=slope_long,
            accel_short_vs_medium=accel_short_vs_medium,
            accel_medium_vs_long=accel_medium_vs_long,
        )

    def _classify_pattern(
        self,
        *,
        w1: Optional[WindowStats],
        w5: Optional[WindowStats],
        w10: Optional[WindowStats],
        baseline_mean: float,
        z_score_last: float,
        is_curve_anomalous: bool,
        has_microvariation: bool,
    ) -> str:
        """Clasificación cualitativa del patrón de comportamiento del sensor."""

        # Usamos principalmente la ventana más larga disponible para estabilidad
        ref = w10 or w5 or w1
        if ref is None:
            return "STABLE"

        var_long = ref.std_dev
        trend_long = ref.trend

        # Heurísticas simples por patrón
        if abs(trend_long) < 0.05 and var_long < 0.01 and abs(z_score_last) < 1.0:
            return "STABLE"

        # Oscilación: alta variabilidad y cambios de signo en la pendiente
        if var_long >= 0.05:
            t1 = w1.trend if w1 else trend_long
            t5 = w5.trend if w5 else trend_long
            if t1 * t5 < 0:
                return "OSCILLATING"

        # Drift: tendencia suave pero sostenida
        if 0.05 <= abs(trend_long) < self._cfg.slope_anomaly_threshold and var_long < 0.1:
            return "DRIFTING"

        # Spikes: curva muy anómala pero localizada
        if is_curve_anomalous and has_microvariation and abs(z_score_last) >= self._cfg.microvariation_z_score:
            return "SPIKING"

        # Exponencial (rise/fall aproximado): pendiente creciente en ventanas más cortas
        s_short = w1.trend if w1 else trend_long
        s_med = w5.trend if w5 else trend_long
        if is_curve_anomalous:
            if s_short > s_med and s_med > trend_long and trend_long > 0:
                return "EXPONENTIAL_RISE"
            if s_short < s_med and s_med < trend_long and trend_long < 0:
                return "EXPONENTIAL_FALL"

        # Fallback genérico
        return "DRIFTING" if abs(trend_long) >= 0.05 else "OSCILLATING"

    # ------------------------------------------------------------------
    # Construcción de explicación y decisión de eventos
    # ------------------------------------------------------------------

    def _build_explanation(
        self,
        *,
        sensor_type: str,
        analysis: OnlineAnalysis,
    ) -> tuple[str, str, str]:
        """Devuelve (severity_label, recommended_action, explanation)."""

        pattern = analysis.behavior_pattern

        if pattern == "STABLE":
            severity = "NORMAL"
            action = "none"
            human = "Comportamiento estable del sensor, dentro de su patrón histórico."
        elif pattern in {"OSCILLATING", "DRIFTING"}:
            severity = "WARN"
            action = "check_soon"
            human = (
                "El sensor muestra oscilaciones o una deriva sostenida; "
                "revisar calibración y condiciones ambientales a la brevedad."
            )
        else:
            severity = "CRITICAL"
            action = "immediate_intervention"
            human = (
                "Comportamiento fuertemente anómalo (spikes o crecimiento/caída exponencial); "
                "se recomienda intervención inmediata sobre el dispositivo."
            )

        trend_desc: str
        if analysis.slope_long > 0.1:
            trend_desc = "en aumento"
        elif analysis.slope_long < -0.1:
            trend_desc = "en descenso"
        else:
            trend_desc = "estable"

        parts: list[str] = []
        parts.append(
            f"Sensor tipo {sensor_type or 'desconocido'} con severidad actual {severity} "
            f"y patrón {pattern}. Acción recomendada: {action}. "
        )
        parts.append(
            "Ventanas 1s/5s/10s: "
            f"media_base={analysis.baseline_mean:.4f}, "
            f"último_valor={analysis.last_value:.4f}, "
            f"z_score={analysis.z_score_last:.2f}, "
            f"tendencia_larga={trend_desc}. "
        )

        explanation = human + " " + "".join(parts)
        return severity, action, explanation

    def _should_emit_state_event(
        self,
        *,
        prev_state: Optional[SensorState],
        new_state: SensorState,
        analysis: OnlineAnalysis,
    ) -> tuple[bool, str]:
        """Decide si se debe insertar un evento de estado y qué event_code usar."""

        # Eventos explícitos para anomalías transitorias
        if analysis.new_transient_anomaly:
            return True, "TRANSIENT_ANOMALY"
        if analysis.recovered_transient:
            return True, "RECOVERED_ANOMALY"

        # Primer evento para el sensor
        if prev_state is None:
            if analysis.is_curve_anomalous:
                return True, "CURVE_ANOMALY"
            if analysis.has_microvariation:
                return True, "MICRO_VARIATION"
            if new_state.behavior_pattern != "STABLE":
                return True, "PATTERN_CHANGE"
            return True, "ML_BEHAVIOR_STATE"

        state_changed = (
            prev_state.severity != new_state.severity
            or prev_state.recommended_action != new_state.recommended_action
            or prev_state.behavior_pattern != new_state.behavior_pattern
        )

        if not state_changed and not analysis.is_curve_anomalous and not analysis.has_microvariation:
            # Sin cambio relevante ni anomalías adicionales
            return False, prev_state.last_event_code or "ML_BEHAVIOR_STATE"

        # Priorizar tipo de evento según la anomalía detectada
        if analysis.is_curve_anomalous:
            code = "CURVE_ANOMALY"
        elif analysis.has_microvariation:
            code = "MICRO_VARIATION"
        elif prev_state.behavior_pattern != new_state.behavior_pattern:
            code = "PATTERN_CHANGE"
        else:
            code = "ML_BEHAVIOR_STATE"

        # Evitar repetir el mismo código si el estado lógico no cambió
        if (
            not state_changed
            and prev_state.last_event_code == code
        ):
            return False, code

        return True, code

    def _map_severity_to_event_type(self, severity: str) -> str:
        """Mapea severidad lógica (NORMAL/WARN/CRITICAL) a event_type."""

        sev = severity.upper()
        if sev == "CRITICAL":
            return "critical"
        if sev == "WARN":
            return "warning"
        return "notice"

    def _build_title(self, event_code: str, severity: str, pattern: str) -> str:
        return f"ML online: {event_code} (estado {severity}, patrón {pattern})"

    # ------------------------------------------------------------------
    # Persistencia de eventos ML + notificaciones
    # ------------------------------------------------------------------

    def _get_device_id(self, conn: Connection, sensor_id: int) -> int:
        if sensor_id in self._device_cache:
            return self._device_cache[sensor_id]
        device_id = get_device_id_for_sensor(conn, sensor_id)
        self._device_cache[sensor_id] = device_id
        return device_id

    def _insert_ml_event(
        self,
        *,
        sensor_id: int,
        sensor_type: str,
        severity_label: str,
        event_type: str,
        event_code: str,
        title: str,
        explanation: str,
        recommended_action: str,
        analysis: OnlineAnalysis,
        ts_utc: float,
        prediction_id: Optional[int],
        extra_payload: Optional[dict],
    ) -> None:
        """Inserta un ml_event y crea una notificación asociada.

        - ml_events: registro detallado del evento ML.
        - alert_notifications: estado de notificación (read/unread).
        """

        engine = get_engine()
        with engine.begin() as conn:  # type: ignore[call-arg]
            device_id = self._get_device_id(conn, sensor_id)

            base_payload: dict = {
                "severity": severity_label,
                "behavior_pattern": analysis.behavior_pattern,
                "recommended_action": recommended_action,
                "sensor_type": sensor_type,
                "baseline_mean": analysis.baseline_mean,
                "last_value": analysis.last_value,
                "z_score_last": analysis.z_score_last,
                "is_curve_anomalous": analysis.is_curve_anomalous,
                "has_microvariation": analysis.has_microvariation,
                "microvariation_delta": analysis.microvariation_delta,
            }
            if extra_payload:
                base_payload.update(extra_payload)

            payload_json = json.dumps(base_payload, ensure_ascii=False)

            # 1) Insertar evento ML y obtener su ID
            row = conn.execute(
                text(
                    """
                    INSERT INTO dbo.ml_events (
                      device_id,
                      sensor_id,
                      prediction_id,
                      event_type,
                      event_code,
                      title,
                      message,
                      status,
                      created_at,
                      payload
                    )
                    OUTPUT INSERTED.id
                    VALUES (
                      :device_id,
                      :sensor_id,
                      :prediction_id,
                      :event_type,
                      :event_code,
                      :title,
                      :message,
                      'active',
                      DATEADD(second, :ts_utc, '1970-01-01'),
                      :payload
                    )
                    """
                ),
                {
                    "device_id": device_id,
                    "sensor_id": sensor_id,
                    "prediction_id": prediction_id,
                    "event_type": event_type,
                    "event_code": event_code,
                    "title": title,
                    "message": explanation,
                    "ts_utc": ts_utc,
                    "payload": payload_json,
                },
            ).fetchone()

            if not row:
                return

            event_id = int(row[0])

            # 2) Crear notificación no leída (separada de ml_events)
            #
            # Tabla esperada (MVP): dbo.alert_notifications
            #   - id (identity)
            #   - source (ej: 'ml_event')
            #   - source_event_id (FK lógico a ml_events.id)
            #   - severity
            #   - title
            #   - message
            #   - is_read (bit)
            #   - created_at (datetime)
            try:
                conn.execute(
                    text(
                        """
                        INSERT INTO dbo.alert_notifications (
                          source,
                          source_event_id,
                          severity,
                          title,
                          message,
                          is_read,
                          created_at
                        )
                        VALUES (
                          :source,
                          :source_event_id,
                          :severity,
                          :title,
                          :message,
                          0,
                          GETDATE()
                        )
                        """
                    ),
                    {
                        "source": "ml_event",
                        "source_event_id": event_id,
                        "severity": event_type,
                        "title": title,
                        "message": explanation,
                    },
                )
            except Exception:
                # Si la tabla aún no existe o hay un problema de esquema,
                # no rompemos el flujo de ML; solo dejamos pasar.
                logger.exception("No se pudo insertar en alert_notifications (ML online)")

    # ------------------------------------------------------------------
    # Autovalidación de predicciones (PREDICTION_DEVIATION)
    # ------------------------------------------------------------------

    def _should_dedupe_prediction_deviation(
        self,
        conn: Connection,
        *,
        sensor_id: int,
    ) -> bool:
        cfg = self._cfg
        row = conn.execute(
            text(
                """
                SELECT TOP 1 1
                FROM dbo.ml_events
                WHERE sensor_id = :sensor_id
                  AND event_code = 'PREDICTION_DEVIATION'
                  AND status IN ('active', 'acknowledged')
                  AND created_at >= DATEADD(minute, -:mins, GETDATE())
                ORDER BY created_at DESC
                """
            ),
            {"sensor_id": sensor_id, "mins": cfg.dedupe_minutes_prediction_deviation},
        ).fetchone()
        return row is not None

    def _check_prediction_deviation(
        self,
        *,
        sensor_id: int,
        reading: Reading,
        analysis: OnlineAnalysis,
    ) -> None:
        cfg = self._cfg
        engine = get_engine()

        reading_dt = datetime.fromtimestamp(float(reading.timestamp), tz=timezone.utc)

        with engine.connect() as conn:  # type: ignore[call-arg]
            # FIX PUNTO 2.3: Eliminar CAST AS float, mantener precisión DECIMAL(15,5)
            row = conn.execute(
                text(
                    """
                    SELECT TOP 1 id, [predicted_value], target_timestamp
                    FROM dbo.predictions
                    WHERE sensor_id = :sensor_id
                      AND ABS(DATEDIFF(second, target_timestamp, :ts)) <= :tol
                    ORDER BY ABS(DATEDIFF(second, target_timestamp, :ts)) ASC
                    """
                ),
                {
                    "sensor_id": sensor_id,
                    "ts": reading_dt.replace(tzinfo=None),
                    "tol": cfg.prediction_time_tolerance_seconds,
                },
            ).fetchone()

            if not row:
                return

            prediction_id, predicted_value, _target_ts = row
            # Python Decimal → float mantiene mejor precisión que SQL CAST
            predicted_value_f = float(predicted_value) if predicted_value is not None else 0.0

            error_abs = abs(float(reading.value) - predicted_value_f)
            denom = max(abs(predicted_value_f), 1e-6)
            error_rel = error_abs / denom

            if (
                error_abs < cfg.prediction_error_absolute
                and error_rel < cfg.prediction_error_relative
            ):
                return

            if self._should_dedupe_prediction_deviation(conn, sensor_id=sensor_id):
                return

        # Si llegamos aquí, generamos el evento usando una nueva transacción
        severity_label = "WARN" if error_rel < 0.5 else "CRITICAL"
        event_type = self._map_severity_to_event_type(severity_label)

        explanation = (
            "Desviación significativa entre la predicción del modelo y el valor real. "
            f"real={float(reading.value):.4f} predicho={predicted_value_f:.4f} "
            f"error_abs={error_abs:.4f} error_rel={error_rel:.2%}."
        )
        recommended_action = (
            "Revisar la configuración del modelo y las condiciones del sensor. "
            "Si la desviación persiste, considerar recalibrar o reentrenar el modelo."
        )

        extra_payload = {
            "prediction_id": int(prediction_id),
            "predicted_value": predicted_value_f,
            "error_abs": error_abs,
            "error_rel": error_rel,
        }

        self._insert_ml_event(
            sensor_id=sensor_id,
            sensor_type=reading.sensor_type,
            severity_label=severity_label,
            event_type=event_type,
            event_code="PREDICTION_DEVIATION",
            title="ML online: PREDICTION_DEVIATION",
            explanation=explanation,
            recommended_action=recommended_action,
            analysis=analysis,
            ts_utc=float(reading.timestamp),
            prediction_id=int(prediction_id),
            extra_payload=extra_payload,
        )


def run_stream(broker: ReadingBroker) -> None:
    """Entry point del runner online.

    Recibe un `ReadingBroker` y se subscribe con un handler simple.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    logger.info("[ML-STREAM] Iniciando runner ML online (broker abstracto)")

    processor = SimpleMlOnlineProcessor()

    def handler(reading: Reading) -> None:
        processor.handle_reading(reading)

    broker.subscribe(handler)


def main() -> None:
    """Punto de entrada CLI de ejemplo usando InMemoryReadingBroker.

    Nota: esto asume que el broker se comparte en el mismo proceso.
    Para otros brokers, solo cambias la construcción aquí.
    """

    from iot_ingest_services.ml_service.in_memory_broker import InMemoryReadingBroker

    broker = InMemoryReadingBroker()
    run_stream(broker)


if __name__ == "__main__":
    main()
