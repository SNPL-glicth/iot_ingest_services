"""Módulo de clasificación de lecturas por propósito.

Este módulo clasifica las lecturas ANTES de persistir o enviar a otros servicios,
dividiéndolas en 3 flujos lógicos:

1. ALERTAS (hard rules): Violaciones de rango físico del sensor
2. ADVERTENCIAS (delta/spike): Cambios rápidos detectados por delta/slope
3. ML/PREDICCIÓN: Datos limpios para machine learning

Reglas críticas:
- Nunca puede existir valor fuera de rango físico con severity = low
- Si hay violación física, ML NO puede bajar severidad
- Si hay DELTA_SPIKE reciente, severity ≥ warning
- Una alerta/advertencia/predicción por sensor, no acumulable
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

# FIX FASE2: Importar funciones canónicas de precisión
from iot_machine_learning.ml_service.utils.numeric_precision import safe_float, is_valid_sensor_value

# FIX MODELO DE ESTADOS: Importar gestor de estado operacional
from .sensor_state import SensorStateManager, SensorOperationalState


@dataclass
class CanonicalThresholds:
    """Umbrales canónicos WARNING/ALERT para subordinar semántica de delta spike."""

    warning_min: Optional[float] = None
    warning_max: Optional[float] = None
    alert_min: Optional[float] = None
    alert_max: Optional[float] = None


class ReadingClass(Enum):
    """Clasificación de una lectura según su propósito."""

    ALERT = "alert"  # Violación de rango físico
    WARNING = "warning"  # Delta spike detectado
    ML_PREDICTION = "ml_prediction"  # Dato limpio para ML


@dataclass
class PhysicalRange:
    """Rango físico del sensor (hard limits)."""

    min_value: Optional[float]
    max_value: Optional[float]
    threshold_id: Optional[int] = None


@dataclass
class DeltaThreshold:
    """Umbrales de detección de delta/spike."""

    abs_delta: Optional[float] = None
    rel_delta: Optional[float] = None
    abs_slope: Optional[float] = None
    rel_slope: Optional[float] = None
    severity: str = "warning"


@dataclass
class LastReading:
    """Última lectura conocida del sensor."""

    value: float
    timestamp: datetime
    reading_id: Optional[int] = None


@dataclass
class ClassifiedReading:
    """Resultado de la clasificación de una lectura."""

    sensor_id: int
    value: float
    device_timestamp: Optional[datetime]
    classification: ReadingClass
    physical_range: Optional[PhysicalRange] = None
    delta_info: Optional[dict] = None
    reason: str = ""


class ReadingClassifier:
    """Clasificador de lecturas por propósito.
    
    MODELO DE ESTADOS:
    Usa SensorStateManager como único punto de decisión para determinar
    si un sensor puede generar WARNING/ALERT.
    
    Regla de dominio:
    - Sensor en INITIALIZING → NUNCA genera eventos
    - Sensor en STALE → NUNCA genera eventos
    - Sensor en NORMAL → puede generar WARNING/ALERT
    """

    def __init__(self, db: Session | Connection) -> None:
        """Inicializa el clasificador con una Session o Connection."""
        self._db = db
        self._range_cache: dict[int, Optional[PhysicalRange]] = {}
        self._delta_cache: dict[int, Optional[DeltaThreshold]] = {}
        self._last_reading_cache: dict[int, Optional[LastReading]] = {}
        self._thresholds_cache: dict[int, Optional[CanonicalThresholds]] = {}
        # FIX MODELO DE ESTADOS: Gestor de estado operacional
        self._state_manager = SensorStateManager(db)

    def _get_canonical_thresholds(self, sensor_id: int) -> Optional[CanonicalThresholds]:
        """Obtiene umbrales WARNING/ALERT (min/max) desde alert_thresholds.

        Nota: Se usan solo para subordinar delta spikes al rango WARNING.
        """
        if sensor_id in self._thresholds_cache:
            return self._thresholds_cache[sensor_id]

        rows = self._db.execute(
            text(
                """
                SELECT
                    severity,
                    threshold_value_min,
                    threshold_value_max
                FROM dbo.alert_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                  AND condition_type = 'out_of_range'
                  AND severity IN ('warning', 'critical')
                ORDER BY CASE severity WHEN 'critical' THEN 0 ELSE 1 END, id ASC
                """
            ),
            {"sensor_id": sensor_id},
        ).fetchall()

        if not rows:
            self._thresholds_cache[sensor_id] = None
            return None

        warning_min: Optional[float] = None
        warning_max: Optional[float] = None
        alert_min: Optional[float] = None
        alert_max: Optional[float] = None

        for r in rows:
            sev = str(getattr(r, "severity", "") or "").lower()
            min_v = safe_float(getattr(r, "threshold_value_min", None), None)
            max_v = safe_float(getattr(r, "threshold_value_max", None), None)
            if sev == "warning" and warning_min is None and warning_max is None:
                warning_min, warning_max = min_v, max_v
            elif sev == "critical" and alert_min is None and alert_max is None:
                alert_min, alert_max = min_v, max_v

        th = CanonicalThresholds(
            warning_min=warning_min,
            warning_max=warning_max,
            alert_min=alert_min,
            alert_max=alert_max,
        )
        self._thresholds_cache[sensor_id] = th
        return th

    def _is_within_warning_band(self, value: float, th: Optional[CanonicalThresholds]) -> bool:
        if th is None:
            return False

        if th.warning_min is not None and value < th.warning_min:
            return False
        if th.warning_max is not None and value > th.warning_max:
            return False

        # Si no hay límites configurados, no podemos afirmar que está dentro del rango.
        if th.warning_min is None and th.warning_max is None:
            return False

        return True

    # FIX REFACTOR: Configuración de persistencia de estado AGNÓSTICA al tipo de sensor
    # El sistema NO conoce qué mide el sensor, solo:
    # - Valores numéricos
    # - Umbrales definidos
    # - Reglas temporales
    #
    # Regla: Se requieren N lecturas CONSECUTIVAS fuera del umbral para cambiar de estado
    # Esto es configurable por sensor en la BD (tabla alert_thresholds.consecutive_readings)
    # Si no está configurado, se usa el default.
    DEFAULT_CONSECUTIVE_READINGS = 2  # Lecturas consecutivas fuera de umbral para alertar
    
    # Cache de estado de lecturas consecutivas por sensor
    # Estructura: {sensor_id: {'count': int, 'last_state': str, 'last_value': float}}
    _consecutive_state_cache: dict = {}

    def classify(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime] = None,
        ingest_timestamp: Optional[datetime] = None,
    ) -> ClassifiedReading:
        """Clasifica una lectura según su propósito.

        Orden de evaluación:
        0. MODELO DE ESTADOS: Verificar si sensor puede generar eventos
        1. Verificar violación de rango físico → ALERT (solo si estado permite)
        2. Verificar delta spike → WARNING (solo si estado permite)
        3. Resto → ML_PREDICTION (dato limpio)

        REGLA DE DOMINIO CRÍTICA:
        El sensor DEBE estar en estado NORMAL antes de poder generar WARNING/ALERT.
        Sensores en INITIALIZING o STALE NUNCA generan eventos.

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            device_timestamp: Timestamp del dispositivo (opcional)
            ingest_timestamp: Timestamp de ingesta (opcional, default: ahora)

        Returns:
            ClassifiedReading con la clasificación y metadata
        """
        if ingest_timestamp is None:
            ingest_timestamp = datetime.now(timezone.utc)

        # FIX FASE2: Validación NaN/Infinity antes de procesar
        if not is_valid_sensor_value(value):
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=safe_float(value, 0.0),
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Valor inválido (NaN/Infinity/None): {value} - descartado",
            )

        # =====================================================================
        # PASO 0: MODELO DE ESTADOS - Verificar si sensor puede generar eventos
        # =====================================================================
        # Esta es la ÚNICA fuente de verdad para decidir si se pueden generar
        # WARNING/ALERT. Bloquea TOTALMENTE eventos mientras no esté en NORMAL.
        
        # Registrar lectura válida (incrementa contador de warm-up si aplica)
        state_info = self._state_manager.register_valid_reading(sensor_id)
        
        # Verificar si puede generar eventos
        can_generate, state_reason = self._state_manager.can_generate_events(sensor_id)
        
        if not can_generate:
            # Sensor NO puede generar eventos (INITIALIZING, STALE, UNKNOWN)
            # Retornar ML_PREDICTION sin evaluar umbrales
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Sensor bloqueado para eventos: {state_reason}",
            )

        # =====================================================================
        # PASO 1: Verificar violación de rango físico (hard rule)
        # =====================================================================
        # Solo se evalúa si el sensor está en estado que permite eventos
        physical_range = self._get_physical_range(sensor_id)
        if physical_range and self._violates_physical_range(value, physical_range):
            # FIX REFACTOR: Enfoque agnóstico basado en lecturas consecutivas
            # No alertar hasta que haya N lecturas CONSECUTIVAS fuera de umbral
            consecutive_required = self._get_consecutive_readings_required(sensor_id)
            consecutive_count = self._update_consecutive_state(sensor_id, 'ALERT', value)
            
            if consecutive_count < consecutive_required:
                return ClassifiedReading(
                    sensor_id=sensor_id,
                    value=value,
                    device_timestamp=device_timestamp,
                    classification=ReadingClass.ML_PREDICTION,
                    reason=f"Valor {value} fuera de rango, pero solo {consecutive_count}/{consecutive_required} lecturas consecutivas",
                )
            
            # Transicionar estado del sensor a ALERT
            self._state_manager.transition_to(sensor_id, SensorOperationalState.ALERT)
            
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ALERT,
                physical_range=physical_range,
                reason=f"Valor {value} fuera de rango físico [{physical_range.min_value}, {physical_range.max_value}] ({consecutive_count} lecturas consecutivas)",
            )

        # FIX REFACTOR: Si el valor está dentro del rango, resetear contador de consecutivos
        # Esto es crítico para el enfoque agnóstico: el estado se resetea cuando vuelve a normal
        self._update_consecutive_state(sensor_id, 'NORMAL', value)

        # =====================================================================
        # REGLA DE DOMINIO CRÍTICA: Delta spike subordinado a umbrales del usuario
        # =====================================================================
        # Si el valor actual está dentro de [warningMin, warningMax], NO se marca
        # delta spike. El usuario definió ese rango como "normal".
        # 
        # CASO ESPECIAL: Si el sensor empieza a reportar desde un valor estable
        # dentro del rango (ej: 10 dentro de 6-13), NO es un spike aunque la
        # última lectura guardada fuera diferente.
        thresholds = self._get_canonical_thresholds(sensor_id)
        if self._is_within_warning_band(value, thresholds):
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason="Dato dentro de rango WARNING del usuario; delta spike no aplica",
            )

        # =====================================================================
        # PASO 2: Verificar delta spike (cambios rápidos)
        # =====================================================================
        # NOTA: El warm-up ya fue validado por SensorStateManager en PASO 0.
        # Si llegamos aquí, el sensor está en estado NORMAL y puede generar eventos.
        
        # Obtener última lectura para comparación de delta
        last_reading = self._get_last_reading(sensor_id)
        
        # Sin historial reciente → no hay delta spike posible (dato limpio)
        if not last_reading:
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason="Sin historial reciente para evaluar delta spike",
            )
        
        # Validar que el historial sea RECIENTE (máximo 10 minutos)
        last_ts = last_reading.timestamp
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        ingest_ts_utc = ingest_timestamp
        if ingest_ts_utc.tzinfo is None:
            ingest_ts_utc = ingest_ts_utc.replace(tzinfo=timezone.utc)
        
        last_reading_age_seconds = (ingest_ts_utc - last_ts).total_seconds()
        max_valid_age_seconds = 600  # 10 minutos máximo
        
        if last_reading_age_seconds > max_valid_age_seconds:
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Historial muy viejo ({last_reading_age_seconds:.0f}s), delta spike no evaluado",
            )
        
        # Evaluar delta spike
        # NOTA: Si llegamos aquí, el valor actual está FUERA del rango WARNING.
        # Un delta spike es válido si hay un cambio abrupto significativo.
        delta_info = self._check_delta_spike(
            sensor_id=sensor_id,
            current_value=value,
            current_ts=ingest_timestamp,
            last_reading=last_reading,
        )
        if delta_info and delta_info.get("is_spike", False):
            # Verificar cooldown antes de generar advertencia
            if self._is_in_cooldown(sensor_id, 'WARNING'):
                return ClassifiedReading(
                    sensor_id=sensor_id,
                    value=value,
                    device_timestamp=device_timestamp,
                    classification=ReadingClass.ML_PREDICTION,
                    reason="Delta spike detectado pero en cooldown, ignorado",
                )
            
            # Transicionar estado del sensor a WARNING
            self._state_manager.transition_to(sensor_id, SensorOperationalState.WARNING)
            
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.WARNING,
                delta_info=delta_info,
                reason=f"Delta spike detectado: {delta_info.get('reason', '')}",
            )

        # 3. Dato limpio para ML (sin violación física ni delta spike fuerte)
        return ClassifiedReading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            classification=ReadingClass.ML_PREDICTION,
            reason="Dato dentro de rango físico y sin delta spike significativo",
        )

    def _get_physical_range(self, sensor_id: int) -> Optional[PhysicalRange]:
        """Obtiene el rango físico del sensor desde alert_thresholds.

        Busca un threshold activo con condition_type='out_of_range' que define
        los límites físicos del sensor.
        """
        if sensor_id in self._range_cache:
            return self._range_cache[sensor_id]

        row = self._db.execute(
            text(
                """
                SELECT TOP 1
                    id,
                    threshold_value_min,
                    threshold_value_max
                FROM dbo.alert_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                  AND condition_type = 'out_of_range'
                ORDER BY id ASC
                """
            ),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._range_cache[sensor_id] = None
            return None

        # FIX FASE2: Usar safe_float en lugar de float() directo
        min_val = safe_float(row.threshold_value_min, None) if row.threshold_value_min is not None else None
        max_val = safe_float(row.threshold_value_max, None) if row.threshold_value_max is not None else None

        if min_val is None and max_val is None:
            self._range_cache[sensor_id] = None
            return None

        physical_range = PhysicalRange(
            min_value=min_val,
            max_value=max_val,
            threshold_id=int(row.id),
        )
        self._range_cache[sensor_id] = physical_range
        return physical_range

    def _violates_physical_range(self, value: float, physical_range: PhysicalRange) -> bool:
        """Verifica si un valor viola el rango físico."""
        if physical_range.min_value is not None and value < physical_range.min_value:
            return True
        if physical_range.max_value is not None and value > physical_range.max_value:
            return True
        return False

    def _get_delta_threshold(self, sensor_id: int) -> Optional[DeltaThreshold]:
        """Obtiene los umbrales de delta para el sensor."""
        if sensor_id in self._delta_cache:
            return self._delta_cache[sensor_id]

        row = self._db.execute(
            text(
                """
                SELECT TOP 1
                    abs_delta,
                    rel_delta,
                    abs_slope,
                    rel_slope,
                    severity
                FROM dbo.delta_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                ORDER BY id ASC
                """
            ),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._delta_cache[sensor_id] = None
            return None

        # FIX FASE2: Usar safe_float en lugar de float() directo
        delta_threshold = DeltaThreshold(
            abs_delta=safe_float(row.abs_delta, None) if row.abs_delta is not None else None,
            rel_delta=safe_float(row.rel_delta, None) if row.rel_delta is not None else None,
            abs_slope=safe_float(row.abs_slope, None) if row.abs_slope is not None else None,
            rel_slope=safe_float(row.rel_slope, None) if row.rel_slope is not None else None,
            severity=str(row.severity or "warning"),
        )
        self._delta_cache[sensor_id] = delta_threshold
        return delta_threshold

    def _get_last_reading(self, sensor_id: int) -> Optional[LastReading]:
        """Obtiene la última lectura del sensor desde sensor_readings_latest."""
        if sensor_id in self._last_reading_cache:
            return self._last_reading_cache[sensor_id]

        row = self._db.execute(
            text(
                """
                SELECT TOP 1
                    latest_value,
                    latest_timestamp
                FROM dbo.sensor_readings_latest
                WHERE sensor_id = :sensor_id
                """
            ),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._last_reading_cache[sensor_id] = None
            return None

        # FIX FASE2: Usar safe_float con validación NaN
        latest_val = safe_float(row.latest_value, None)
        if latest_val is None:
            self._last_reading_cache[sensor_id] = None
            return None
        
        last_reading = LastReading(
            value=latest_val,
            timestamp=row.latest_timestamp,
        )
        self._last_reading_cache[sensor_id] = last_reading
        return last_reading

    # FIX CRÍTICO: Umbrales de ruido POR TIPO DE SENSOR
    # Cada tipo de sensor tiene características de ruido diferentes.
    # Estructura: (noise_floor_abs, noise_floor_rel)
    SENSOR_TYPE_NOISE_THRESHOLDS = {
        'temperature': (0.5, 0.02),    # 0.5°C abs, 2% rel - sensible
        'humidity': (2.0, 0.03),        # 2% abs, 3% rel - ruido medio
        'air_quality': (50.0, 0.10),    # 50 ppm abs, 10% rel - ruido alto
        'voltage': (1.0, 0.05),         # 1V abs, 5% rel - fluctuaciones normales
        'power': (10.0, 0.10),          # 10W abs, 10% rel - picos normales
        'pressure': (0.5, 0.005),       # 0.5 hPa abs, 0.5% rel - muy estable
        'default': (0.1, 0.01),         # Conservador para tipos desconocidos
    }
    
    # Cache de tipos de sensor
    _sensor_type_cache: dict = {}
    
    # Fallback legacy (para compatibilidad)
    NOISE_FLOOR_ABS = 0.05
    NOISE_FLOOR_REL = 0.005

    def _get_sensor_type(self, sensor_id: int) -> str:
        """Obtiene el tipo de sensor desde la BD con cache."""
        if sensor_id in self._sensor_type_cache:
            return self._sensor_type_cache[sensor_id]
        
        try:
            row = self._db.execute(
                text(
                    """
                    SELECT sensor_type
                    FROM dbo.sensors
                    WHERE id = :sensor_id
                    """
                ),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            sensor_type = 'default'
            if row and row[0]:
                sensor_type = str(row[0]).lower().strip()
            
            self._sensor_type_cache[sensor_id] = sensor_type
            return sensor_type
        except Exception:
            return 'default'

    def _get_consecutive_readings_required(self, sensor_id: int) -> int:
        """Obtiene el número de lecturas consecutivas requeridas para alertar.
        
        AGNÓSTICO AL TIPO DE SENSOR: El valor viene de la configuración del umbral
        en la BD (alert_thresholds.consecutive_readings), no del tipo de sensor.
        
        Returns:
            Número de lecturas consecutivas requeridas (default: 2)
        """
        # Intentar obtener de la BD (configurable por sensor)
        try:
            row = self._db.execute(
                text(
                    """
                    SELECT TOP 1 consecutive_readings
                    FROM dbo.alert_thresholds
                    WHERE sensor_id = :sensor_id
                      AND is_active = 1
                      AND consecutive_readings IS NOT NULL
                    ORDER BY id ASC
                    """
                ),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            if row and row[0]:
                return int(row[0])
        except Exception:
            pass
        
        return self.DEFAULT_CONSECUTIVE_READINGS

    def _update_consecutive_state(self, sensor_id: int, new_state: str, value: float) -> int:
        """Actualiza y retorna el conteo de lecturas consecutivas en el mismo estado.
        
        AGNÓSTICO AL TIPO DE SENSOR: Solo cuenta lecturas consecutivas en el mismo estado.
        
        Reglas:
        - Si el estado cambia (ej: NORMAL → ALERT), resetea contador a 1
        - Si el estado se mantiene (ej: ALERT → ALERT), incrementa contador
        - Si vuelve a NORMAL, resetea contador a 0
        
        Args:
            sensor_id: ID del sensor
            new_state: Nuevo estado ('NORMAL', 'WARNING', 'ALERT')
            value: Valor actual de la lectura
            
        Returns:
            Número de lecturas consecutivas en el estado actual
        """
        current = self._consecutive_state_cache.get(sensor_id, {
            'count': 0,
            'last_state': 'NORMAL',
            'last_value': None
        })
        
        if new_state == 'NORMAL':
            # Volver a normal resetea todo
            self._consecutive_state_cache[sensor_id] = {
                'count': 0,
                'last_state': 'NORMAL',
                'last_value': value
            }
            return 0
        
        if current['last_state'] == new_state:
            # Mismo estado, incrementar contador
            new_count = current['count'] + 1
        else:
            # Cambio de estado, empezar desde 1
            new_count = 1
        
        self._consecutive_state_cache[sensor_id] = {
            'count': new_count,
            'last_state': new_state,
            'last_value': value
        }
        
        return new_count

    # FIX PROBLEMA 1: Cache de conteo de lecturas por sensor
    _reading_count_cache: dict = {}
    
    def _get_sensor_reading_count(self, sensor_id: int) -> int:
        """Obtiene el número de lecturas del sensor en las últimas 2 horas.
        
        Usado para warm-up: no evaluar delta spikes hasta tener suficientes lecturas.
        
        Returns:
            Número de lecturas recientes del sensor
        """
        if sensor_id in self._reading_count_cache:
            return self._reading_count_cache[sensor_id]
        
        try:
            row = self._db.execute(
                text(
                    """
                    SELECT COUNT(*) as cnt
                    FROM dbo.sensor_readings
                    WHERE sensor_id = :sensor_id
                      AND timestamp >= DATEADD(hour, -2, GETDATE())
                    """
                ),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            count = int(row[0]) if row and row[0] else 0
            self._reading_count_cache[sensor_id] = count
            return count
        except Exception:
            return 0

    # FIX PROBLEMA 2: Cache de cooldown por sensor y tipo de evento
    # Estructura: {sensor_id: {'WARNING': datetime, 'ALERT': datetime}}
    _cooldown_cache: dict = {}
    COOLDOWN_SECONDS = 300  # 5 minutos de cooldown entre eventos del mismo tipo
    
    def _is_in_cooldown(self, sensor_id: int, event_type: str) -> bool:
        """Verifica si el sensor está en período de cooldown para el tipo de evento.
        
        Evita generar múltiples advertencias/alertas en corto tiempo.
        
        Args:
            sensor_id: ID del sensor
            event_type: Tipo de evento ('WARNING', 'ALERT')
            
        Returns:
            True si está en cooldown, False si puede generar evento
        """
        now = datetime.now(timezone.utc)
        
        if sensor_id not in self._cooldown_cache:
            self._cooldown_cache[sensor_id] = {}
        
        last_event = self._cooldown_cache[sensor_id].get(event_type)
        if last_event:
            elapsed = (now - last_event).total_seconds()
            if elapsed < self.COOLDOWN_SECONDS:
                return True
        
        # No está en cooldown, registrar este evento
        self._cooldown_cache[sensor_id][event_type] = now
        return False
    
    def _clear_cooldown(self, sensor_id: int, event_type: str = None) -> None:
        """Limpia el cooldown de un sensor.
        
        Llamar cuando se atiende/resuelve una alerta para permitir nuevos eventos.
        """
        if sensor_id in self._cooldown_cache:
            if event_type:
                self._cooldown_cache[sensor_id].pop(event_type, None)
            else:
                self._cooldown_cache[sensor_id] = {}

    def _check_delta_spike(
        self,
        sensor_id: int,
        current_value: float,
        current_ts: datetime,
        last_reading: LastReading,
    ) -> Optional[dict]:
        """Verifica si hay un delta spike usando umbrales de delta/slope.

        Calcula:
        - Delta absoluto: |current - last|
        - Delta relativo: |delta| / |last|
        - Slope absoluto: |delta| / dt (unidades/segundo)
        - Slope relativo: (|delta|/|last|) / dt (1/segundo)

        FIX CRÍTICO: Aplica umbrales de ruido ESPECÍFICOS POR TIPO DE SENSOR.
        Cada tipo de sensor tiene características de ruido diferentes.

        Returns:
            dict con is_spike=True si se detecta spike, None en caso contrario
        """
        delta_threshold = self._get_delta_threshold(sensor_id)
        if not delta_threshold:
            # Sin umbrales de delta configurados, no detectamos spikes
            return None

        # FIX CRÍTICO: Obtener umbrales de ruido específicos para el tipo de sensor
        sensor_type = self._get_sensor_type(sensor_id)
        noise_abs, noise_rel = self.SENSOR_TYPE_NOISE_THRESHOLDS.get(
            sensor_type,
            self.SENSOR_TYPE_NOISE_THRESHOLDS['default']
        )

        # Calcular delta y dt
        delta_abs = abs(current_value - last_reading.value)
        dt_seconds = (current_ts - last_reading.timestamp).total_seconds()

        # Calcular delta relativo
        delta_rel = 0.0
        if abs(last_reading.value) > 1e-6:
            delta_rel = abs(delta_abs / last_reading.value)

        # FIX CRÍTICO: Filtrar ruido usando umbrales específicos del tipo de sensor.
        # REGLA CORRECTA: Solo es ruido si AMBOS deltas están por debajo del umbral.
        # Si al menos uno supera el umbral de ruido, puede ser un spike válido.
        # 
        # Lógica anterior (incorrecta): OR causaba falsos negativos
        # Lógica correcta: AND - solo filtrar si AMBOS son insignificantes
        is_noise = delta_abs < noise_abs and delta_rel < noise_rel
        if is_noise:
            return None  # Variación dentro del ruido normal del sensor

        # Evitar división por cero o dt negativo
        if dt_seconds <= 0:
            dt_seconds = 0.001  # 1ms mínimo para cálculos

        slope_abs = delta_abs / dt_seconds
        slope_rel = delta_rel / dt_seconds if dt_seconds > 0 else 0.0

        # Verificar umbrales
        triggered = []
        reason_parts = []

        if delta_threshold.abs_delta is not None and delta_abs >= delta_threshold.abs_delta:
            triggered.append("abs_delta")
            reason_parts.append(f"delta_abs={delta_abs:.4f} >= {delta_threshold.abs_delta:.4f}")

        if delta_threshold.rel_delta is not None and delta_rel >= delta_threshold.rel_delta:
            triggered.append("rel_delta")
            reason_parts.append(f"delta_rel={delta_rel:.4%} >= {delta_threshold.rel_delta:.4%}")

        if delta_threshold.abs_slope is not None and slope_abs >= delta_threshold.abs_slope:
            triggered.append("abs_slope")
            reason_parts.append(f"slope_abs={slope_abs:.4f} >= {delta_threshold.abs_slope:.4f}")

        if delta_threshold.rel_slope is not None and slope_rel >= delta_threshold.rel_slope:
            triggered.append("rel_slope")
            reason_parts.append(f"slope_rel={slope_rel:.4f} >= {delta_threshold.rel_slope:.4f}")

        if not triggered:
            return None

        return {
            "is_spike": True,
            "delta_abs": delta_abs,
            "delta_rel": delta_rel,
            "slope_abs": slope_abs,
            "slope_rel": slope_rel,
            "dt_seconds": dt_seconds,
            "last_value": last_reading.value,
            "triggered_thresholds": triggered,
            "severity": delta_threshold.severity,
            "reason": "; ".join(reason_parts),
        }

