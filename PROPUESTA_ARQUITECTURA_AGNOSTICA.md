# PROPUESTA ARQUITECTÓNICA: SERVICIO DE INGESTA AGNÓSTICO MULTI-TRANSPORTE

**Fecha:** 2026-02-25  
**Estado:** PROPUESTA PARA REVISIÓN  
**Objetivo:** Convertir `iot_ingest_services` en un servicio de ingesta universal que soporte múltiples dominios y transportes

---

## 1. HALLAZGOS — ACOPLAMIENTO IoT-ESPECÍFICO

### 1.1 Conceptos IoT Hardcodeados en Schemas

**Archivo:** `ingest_api/schemas.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 11-15 | `SensorReadingIn.sensor_id: int` | Asume ID numérico de sensor |
| 22-27 | `SensorReadingUuidIn.sensor_uuid: UUID` | Asume UUID de sensor IoT |
| 29-32 | `DevicePacketIn.device_uuid: UUID` | Asume concepto de "dispositivo" IoT |
| 44-48 | `SensorFinalState` enum | Estados específicos de sensores |
| 51-60 | `ActiveAlert.sensor_id, device_id` | Relación sensor-dispositivo hardcodeada |
| 75-83 | `CurrentPrediction.sensor_id, model_id` | Asume predicción por sensor |

**Diagnóstico:** Todos los DTOs están acoplados a la jerarquía `device → sensor → reading`.

---

### 1.2 Clasificador Acoplado a Rangos Físicos de Sensores

**Archivo:** `ingest_api/classification/reading_classifier.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 43-54 | `ReadingClassifier` class | Nombre específico de dominio IoT |
| 63-69 | `classify(sensor_id: int, ...)` | Firma específica de sensor |
| 104-108 | `_check_physical_range()` | Asume rangos físicos de sensores |
| 137-169 | Validación contra `PhysicalRange` | Hardcoded para sensores físicos |
| 171-220 | `_check_delta_spike()` | Lógica específica de sensores IoT |

**Diagnóstico:** El clasificador conoce conceptos de "sensor", "dispositivo", "rango físico", "delta spike". No es reutilizable para métricas de servidor, datos financieros, etc.

---

### 1.3 Thresholds Manager Acoplado a Tablas IoT

**Archivo:** `ingest_api/classification/thresholds.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 30-39 | `ThresholdManager` caches por `sensor_id` | Asume sensores |
| 45-56 | Query a `dbo.alert_thresholds` | Tabla específica IoT |
| 86-96 | Query a `dbo.alert_thresholds` | Mismo esquema IoT |
| 120-128 | Query a `dbo.delta_thresholds` | Tabla específica IoT |
| 149-156 | Query a `dbo.sensor_readings_latest` | Tabla específica IoT |

**Diagnóstico:** Todas las queries SQL asumen esquema IoT (`sensors`, `devices`, `alert_thresholds`, `delta_thresholds`).

---

### 1.4 Router Acoplado a Sensores

**Archivo:** `ingest_api/pipelines/router.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 25-29 | `ReadingRouter` class | Nombre específico de dominio |
| 61-63 | `_get_sensor_type()` | Asume tipos de sensor |
| 74-84 | `classify_and_route(sensor_id: int, ...)` | Firma específica de sensor |
| 148-150 | Llama SP `sp_insert_reading_and_check_threshold` | SP específico IoT |
| 205-214 | `_publish_to_broker()` usa `sensor_type` | Concepto IoT |

**Diagnóstico:** El router está completamente acoplado al concepto de "sensor".

---

### 1.5 MQTT Receiver Acoplado a Topic IoT

**Archivo:** `ingest_api/mqtt/receiver.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 137-139 | `topic = "iot/sensors/+/readings"` | Topic hardcodeado IoT |
| 149-152 | `_on_message()` procesa lecturas de sensores | Lógica específica |

**Archivo:** `ingest_api/mqtt/processor.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 48-51 | `sensor_id = payload.sensor_id_int` | Asume sensor_id |
| 111-126 | `_execute_sp()` llama SP IoT | SP específico de sensores |
| 133-143 | `_publish_to_ml()` usa `sensor_id`, `sensor_type` | Conceptos IoT |

**Diagnóstico:** MQTT está hardcodeado para topic `iot/sensors/{id}/readings` y procesa solo lecturas de sensores.

---

### 1.6 Contratos de Dominio Acoplados

**Archivo:** `ingest_api/core/domain/contracts.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 10-38 | `MQTTPayload.sensorId, sensor_type, device_uuid` | Campos específicos IoT |
| 55-59 | `REQUIRED_FIELDS = ["sensor_id", "value", "timestamp"]` | Asume sensor_id |
| 60-65 | `OPTIONAL_FIELDS` incluye `sensor_type`, `device_uuid` | Conceptos IoT |

**Archivo:** `ingest_api/core/domain/broker_interface.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 14-24 | `Reading.sensor_id, sensor_type, device_id` | Campos específicos IoT |

**Diagnóstico:** Los contratos de dominio están completamente acoplados a la jerarquía IoT.

---

### 1.7 Endpoints HTTP Acoplados

**Archivo:** `ingest_api/endpoints/packet_ingest.py`

| Línea | Concepto IoT | Impacto |
|-------|--------------|---------|
| 32-39 | `ingest_packet(payload: DevicePacketIn, ...)` | DTO específico IoT |
| 66-67 | `validate_device_access()` | Validación específica de dispositivos |
| 74-77 | `resolve_sensor_id(device_uuid, sensor_uuid)` | Resolución IoT |

**Diagnóstico:** El endpoint `/ingest/packets` está completamente acoplado al modelo `device → sensor → reading`.

---

### 1.8 Resumen de Acoplamiento

**Total de archivos con acoplamiento IoT:** 10+  
**Total de conceptos hardcodeados identificados:** 47+

**Conceptos IoT que deben abstraerse:**
1. `sensor_id` → `stream_id`
2. `device_uuid` → `source_id`
3. `sensor_type` → `stream_type`
4. `sensor_uuid` → `stream_uuid`
5. `device_id` → `source_id`
6. `PhysicalRange` → `ValueConstraints`
7. `alert_thresholds` → `stream_constraints`
8. `delta_thresholds` → `rate_constraints`
9. `sensor_readings_latest` → `stream_latest`
10. Topic `iot/sensors/+/readings` → `{domain}/{source}/{stream}`

---

## 2. CONTRATOS PROPUESTOS — DOMINIO AGNÓSTICO

### 2.1 DataPoint Universal

```python
"""Contrato universal de punto de datos agnóstico al dominio."""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID


@dataclass
class DataPoint:
    """Punto de datos universal agnóstico al dominio.
    
    Puede representar:
    - Lectura de sensor IoT (temperatura, presión, etc.)
    - Métrica de servidor (CPU, memoria, latencia)
    - Dato financiero (precio, volumen, spread)
    - Lectura médica (frecuencia cardíaca, presión arterial)
    - Fila de CSV con timestamp
    
    Campos obligatorios:
    - stream_id: Identificador único del flujo de datos
    - value: Valor numérico del punto de datos
    - timestamp: Timestamp del punto de datos
    
    Campos opcionales:
    - source_id: Identificador de la fuente (device, server, exchange, patient)
    - stream_type: Tipo de stream (temperature, cpu_usage, stock_price, heart_rate)
    - metadata: Información adicional específica del dominio
    """
    
    # Campos obligatorios
    stream_id: str
    value: float
    timestamp: float  # Unix epoch
    
    # Campos opcionales
    source_id: Optional[str] = None
    stream_type: Optional[str] = None
    sequence: Optional[int] = None
    
    # Metadata extensible para información específica del dominio
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Timestamps adicionales para observabilidad
    ingested_at: Optional[float] = None
    processed_at: Optional[float] = None
    
    @property
    def timestamp_datetime(self) -> datetime:
        """Convierte timestamp Unix a datetime."""
        return datetime.fromtimestamp(self.timestamp)
    
    @property
    def latency_ms(self) -> Optional[float]:
        """Calcula latencia de ingesta en milisegundos."""
        if self.ingested_at is not None:
            return (self.ingested_at - self.timestamp) * 1000
        return None
    
    def to_dict(self) -> dict:
        """Serializa a diccionario."""
        return {
            "stream_id": self.stream_id,
            "value": self.value,
            "timestamp": self.timestamp,
            "source_id": self.source_id,
            "stream_type": self.stream_type,
            "sequence": self.sequence,
            "metadata": self.metadata,
            "ingested_at": self.ingested_at,
            "processed_at": self.processed_at,
        }
    
    @classmethod
    def from_iot_reading(
        cls,
        sensor_id: int,
        value: float,
        timestamp: float,
        device_uuid: Optional[str] = None,
        sensor_type: Optional[str] = None,
        **kwargs
    ) -> DataPoint:
        """Factory para crear DataPoint desde lectura IoT (adapter)."""
        return cls(
            stream_id=str(sensor_id),
            value=value,
            timestamp=timestamp,
            source_id=device_uuid,
            stream_type=sensor_type,
            metadata={"domain": "iot", "sensor_id": sensor_id},
            **kwargs
        )
    
    @classmethod
    def from_server_metric(
        cls,
        server_id: str,
        metric_name: str,
        value: float,
        timestamp: float,
        **kwargs
    ) -> DataPoint:
        """Factory para crear DataPoint desde métrica de servidor."""
        return cls(
            stream_id=f"{server_id}:{metric_name}",
            value=value,
            timestamp=timestamp,
            source_id=server_id,
            stream_type=metric_name,
            metadata={"domain": "infrastructure"},
            **kwargs
        )
    
    @classmethod
    def from_financial_tick(
        cls,
        symbol: str,
        field: str,  # price, volume, bid, ask
        value: float,
        timestamp: float,
        exchange: Optional[str] = None,
        **kwargs
    ) -> DataPoint:
        """Factory para crear DataPoint desde tick financiero."""
        return cls(
            stream_id=f"{symbol}:{field}",
            value=value,
            timestamp=timestamp,
            source_id=exchange,
            stream_type=field,
            metadata={"domain": "finance", "symbol": symbol},
            **kwargs
        )
```

---

### 2.2 StreamConfig — Configuración de Stream

```python
"""Configuración de stream agnóstica al dominio."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class ValueConstraints:
    """Restricciones de valor para un stream (reemplaza PhysicalRange).
    
    Puede representar:
    - Rango físico de sensor (0-100°C)
    - Límites de CPU (0-100%)
    - Rango de precio (min_price, max_price)
    - Límites médicos (60-100 bpm)
    """
    
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    
    # Umbrales de advertencia (WARNING)
    warning_min: Optional[float] = None
    warning_max: Optional[float] = None
    
    # Umbrales de alerta (ALERT/CRITICAL)
    alert_min: Optional[float] = None
    alert_max: Optional[float] = None
    
    def violates_hard_limits(self, value: float) -> bool:
        """Verifica si el valor viola límites duros (min/max)."""
        if self.min_value is not None and value < self.min_value:
            return True
        if self.max_value is not None and value > self.max_value:
            return True
        return False
    
    def in_warning_zone(self, value: float) -> bool:
        """Verifica si el valor está en zona de advertencia."""
        if self.warning_min is not None and value < self.warning_min:
            return True
        if self.warning_max is not None and value > self.warning_max:
            return True
        return False
    
    def in_alert_zone(self, value: float) -> bool:
        """Verifica si el valor está en zona de alerta."""
        if self.alert_min is not None and value < self.alert_min:
            return True
        if self.alert_max is not None and value > self.alert_max:
            return True
        return False


@dataclass
class RateConstraints:
    """Restricciones de tasa de cambio (reemplaza DeltaThreshold).
    
    Detecta cambios anormales en la tasa de cambio:
    - Delta spike en sensores IoT
    - CPU spike en servidores
    - Price spike en mercados financieros
    - Heart rate spike en pacientes
    """
    
    abs_delta: Optional[float] = None  # Cambio absoluto máximo
    rel_delta: Optional[float] = None  # Cambio relativo máximo (%)
    abs_slope: Optional[float] = None  # Pendiente absoluta máxima
    rel_slope: Optional[float] = None  # Pendiente relativa máxima (%)
    
    severity: str = "warning"  # warning | alert | critical


@dataclass
class StreamConfig:
    """Configuración completa de un stream de datos.
    
    Agnóstico al dominio — funciona para IoT, infra, finanzas, salud, etc.
    """
    
    stream_id: str
    stream_type: str
    source_id: Optional[str] = None
    
    # Restricciones de valor
    value_constraints: Optional[ValueConstraints] = None
    
    # Restricciones de tasa de cambio
    rate_constraints: Optional[RateConstraints] = None
    
    # Configuración de clasificación
    consecutive_violations_required: int = 3
    cooldown_seconds: float = 300.0
    
    # Metadata extensible
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
```

---

### 2.3 ClassifiedDataPoint — Resultado de Clasificación

```python
"""Resultado de clasificación agnóstico al dominio."""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any


class DataPointClass(Enum):
    """Clasificación de un punto de datos según su propósito.
    
    Reemplaza ReadingClass con nombres agnósticos.
    """
    
    ALERT = "alert"  # Violación de límites duros
    WARNING = "warning"  # Violación de umbrales de advertencia
    ANOMALY = "anomaly"  # Anomalía detectada (rate spike, etc.)
    NORMAL = "normal"  # Dato limpio para análisis/ML


@dataclass
class ClassifiedDataPoint:
    """Resultado de clasificación de un punto de datos.
    
    Agnóstico al dominio — funciona para cualquier tipo de stream.
    """
    
    stream_id: str
    value: float
    timestamp: float
    classification: DataPointClass
    
    # Información de violación (si aplica)
    violated_constraints: Optional[ValueConstraints] = None
    violated_rate: Optional[Dict[str, Any]] = None
    
    # Razón de la clasificación
    reason: str = ""
    
    # Metadata adicional
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
```

---

## 3. ARQUITECTURA MULTI-TRANSPORTE

### 3.1 Diagrama de Capas

```
┌─────────────────────────────────────────────────────────────────┐
│                    CAPA DE TRANSPORTE                           │
│  (Recibe datos en formato nativo, transforma a DataPoint)      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ HTTP REST    │  │ MQTT         │  │ WebSocket    │         │
│  │ Adapter      │  │ Adapter      │  │ Adapter      │         │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │
│         │                  │                  │                 │
│         │                  │                  │                 │
│  ┌──────────────┐  ┌──────────────┐                            │
│  │ CSV Upload   │  │ gRPC         │  (Extensible)              │
│  │ Adapter      │  │ Adapter      │                            │
│  └──────┬───────┘  └──────┬───────┘                            │
│         │                  │                                    │
│         └──────────┬───────┴──────────┬──────────┘             │
│                    │                  │                         │
│                    ▼                  ▼                         │
├─────────────────────────────────────────────────────────────────┤
│              CAPA DE CLASIFICACIÓN AGNÓSTICA                    │
│  (Recibe DataPoint, aplica constraints configurables)          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ DataPointClassifier                                      │  │
│  │ - Valida ValueConstraints                               │  │
│  │ - Detecta violaciones de RateConstraints                │  │
│  │ - Retorna ClassifiedDataPoint                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│                    │                                            │
│                    ▼                                            │
├─────────────────────────────────────────────────────────────────┤
│                  CAPA DE PIPELINE                               │
│  (Router agnóstico: ALERT | WARNING | ANOMALY | NORMAL)        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ DataPointRouter                                          │  │
│  │ - Enruta según clasificación                            │  │
│  │ - Publica a broker ML (solo NORMAL)                     │  │
│  │ - Dispara alertas/warnings                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│                    │                                            │
│                    ▼                                            │
├─────────────────────────────────────────────────────────────────┤
│               CAPA DE PUBLICACIÓN                               │
│  (Publica DataPoints a brokers downstream)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Redis Stream │  │ MQTT Broker  │  │ Kafka        │         │
│  │ Publisher    │  │ Publisher    │  │ Publisher    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### 3.2 TRANSPORTE 1 — HTTP REST (Refactor)

**Endpoint actual:** `POST /ingest/packets`  
**Endpoint agnóstico propuesto:** `POST /ingest/datapoints`

**Cambios necesarios:**

```python
# NUEVO DTO agnóstico
class DataPointBatchIn(BaseModel):
    """Batch de puntos de datos agnóstico al dominio."""
    
    source_id: Optional[str] = None  # device_uuid, server_id, exchange, etc.
    domain: str = "iot"  # iot | infrastructure | finance | health
    datapoints: List[DataPointIn] = Field(default_factory=list)
    
    class Config:
        schema_extra = {
            "example": {
                "source_id": "device-123",
                "domain": "iot",
                "datapoints": [
                    {
                        "stream_id": "sensor-456",
                        "value": 23.5,
                        "timestamp": 1708900000.123,
                        "stream_type": "temperature"
                    }
                ]
            }
        }


class DataPointIn(BaseModel):
    """Punto de datos individual."""
    
    stream_id: str
    value: float
    timestamp: float
    stream_type: Optional[str] = None
    sequence: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


# NUEVO ENDPOINT agnóstico
@router.post("/ingest/datapoints", response_model=IngestResult)
def ingest_datapoints(
    payload: DataPointBatchIn,
    request: Request,
    db: Session = Depends(get_db),
    api_key: str = Depends(require_api_key),
):
    """Endpoint agnóstico para ingesta de puntos de datos.
    
    Soporta múltiples dominios:
    - iot: Lecturas de sensores
    - infrastructure: Métricas de servidores
    - finance: Ticks financieros
    - health: Lecturas médicas
    """
    # Validar rate limiting
    limiter = get_rate_limiter()
    limiter.check_all(source_id=payload.source_id, ip=get_client_ip(request))
    
    # Transformar a DataPoints internos
    datapoints = [
        DataPoint(
            stream_id=dp.stream_id,
            value=dp.value,
            timestamp=dp.timestamp,
            source_id=payload.source_id,
            stream_type=dp.stream_type,
            sequence=dp.sequence,
            metadata=dp.metadata or {},
            ingested_at=time.time(),
        )
        for dp in payload.datapoints
    ]
    
    # Procesar con pipeline agnóstico
    handler = DataPointBatchHandler(db, get_broker())
    result = handler.ingest(datapoints, domain=payload.domain)
    
    return IngestResult(inserted=result.inserted)
```

**Headers para identificar dominio:**
- `X-Domain: iot | infrastructure | finance | health`
- `X-Source-ID: {source_identifier}`
- `X-API-Key: {api_key}`

---

### 3.3 TRANSPORTE 2 — MQTT (Refactor)

**Topic actual:** `iot/sensors/+/readings`  
**Topic agnóstico propuesto:** `{domain}/{source}/{stream}`

**Ejemplos:**
- IoT: `iot/device-123/sensor-456`
- Infra: `infrastructure/server-prod-01/cpu_usage`
- Finance: `finance/binance/BTCUSDT:price`
- Health: `health/patient-789/heart_rate`

**Cambios necesarios:**

```python
# NUEVO receptor MQTT agnóstico
class AgnosticMQTTReceiver:
    """Receptor MQTT agnóstico que soporta múltiples dominios."""
    
    def __init__(self, broker_host: str, broker_port: int):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._client = None
        self._processor = None
    
    def start(self):
        """Inicia el receptor y se suscribe a topics agnósticos."""
        self._client = mqtt.Client(...)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.connect(self.broker_host, self.broker_port)
        self._client.loop_start()
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Se suscribe a topics multi-dominio."""
        # Suscribirse a múltiples dominios
        topics = [
            ("iot/+/+", 1),
            ("infrastructure/+/+", 1),
            ("finance/+/+", 1),
            ("health/+/+", 1),
        ]
        for topic, qos in topics:
            client.subscribe(topic, qos=qos)
            logger.info(f"[MQTT] Subscribed to {topic}")
    
    def _on_message(self, client, userdata, msg):
        """Procesa mensaje agnóstico."""
        # Parsear topic: {domain}/{source}/{stream}
        parts = msg.topic.split('/')
        if len(parts) != 3:
            logger.warning(f"[MQTT] Invalid topic format: {msg.topic}")
            return
        
        domain, source_id, stream_id = parts
        
        # Parsear payload JSON
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            logger.error(f"[MQTT] Invalid JSON payload: {msg.payload}")
            return
        
        # Crear DataPoint
        datapoint = DataPoint(
            stream_id=stream_id,
            value=float(payload['value']),
            timestamp=float(payload['timestamp']),
            source_id=source_id,
            stream_type=payload.get('stream_type'),
            metadata={"domain": domain},
            ingested_at=time.time(),
        )
        
        # Procesar
        self._processor.process(datapoint, domain=domain)
```

**Payload MQTT agnóstico:**

```json
{
  "value": 23.5,
  "timestamp": 1708900000.123,
  "stream_type": "temperature",
  "metadata": {
    "unit": "celsius",
    "location": "room-A"
  }
}
```

---

### 3.4 TRANSPORTE 3 — WebSocket (NUEVO)

**Endpoint propuesto:** `ws://host:port/ingest/stream`

**Flujo:**
1. Cliente abre conexión WebSocket
2. Cliente envía mensaje de autenticación con `source_id` y `domain`
3. Cliente envía stream de DataPoints en tiempo real
4. Servidor procesa cada DataPoint y responde con ACK
5. Conexión persistente para baja latencia

**Implementación propuesta:**

```python
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict


class WebSocketManager:
    """Gestiona conexiones WebSocket persistentes."""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, source_id: str):
        """Acepta nueva conexión."""
        await websocket.accept()
        self.active_connections[source_id] = websocket
    
    def disconnect(self, source_id: str):
        """Cierra conexión."""
        self.active_connections.pop(source_id, None)
    
    async def send_ack(self, source_id: str, sequence: int, status: str):
        """Envía ACK al cliente."""
        if source_id in self.active_connections:
            ws = self.active_connections[source_id]
            await ws.send_json({"sequence": sequence, "status": status})


manager = WebSocketManager()


@app.websocket("/ingest/stream")
async def websocket_ingest(websocket: WebSocket):
    """Endpoint WebSocket para ingesta en tiempo real."""
    
    # Esperar mensaje de autenticación
    auth_msg = await websocket.receive_json()
    source_id = auth_msg.get("source_id")
    domain = auth_msg.get("domain", "iot")
    api_key = auth_msg.get("api_key")
    
    # Validar API key
    if not validate_api_key(api_key):
        await websocket.close(code=1008, reason="Invalid API key")
        return
    
    # Aceptar conexión
    await manager.connect(websocket, source_id)
    logger.info(f"[WS] Connected: source={source_id} domain={domain}")
    
    try:
        while True:
            # Recibir DataPoint
            data = await websocket.receive_json()
            
            # Crear DataPoint
            datapoint = DataPoint(
                stream_id=data["stream_id"],
                value=float(data["value"]),
                timestamp=float(data["timestamp"]),
                source_id=source_id,
                stream_type=data.get("stream_type"),
                sequence=data.get("sequence"),
                metadata={"domain": domain},
                ingested_at=time.time(),
            )
            
            # Procesar
            handler = DataPointHandler(get_db(), get_broker())
            result = handler.process(datapoint, domain=domain)
            
            # Enviar ACK
            await manager.send_ack(
                source_id,
                sequence=datapoint.sequence,
                status="ok" if result.success else "error"
            )
    
    except WebSocketDisconnect:
        manager.disconnect(source_id)
        logger.info(f"[WS] Disconnected: source={source_id}")
```

**Ventajas WebSocket:**
- Latencia ultra-baja (< 10ms)
- Conexión persistente (no overhead de HTTP)
- ACK inmediato por cada punto
- Ideal para high-frequency data (finanzas, IoT industrial)

---

### 3.5 TRANSPORTE 4 — CSV Upload (NUEVO)

**Endpoint propuesto:** `POST /ingest/csv`

**Flujo:**
1. Cliente sube archivo CSV con multipart/form-data
2. Servidor infiere `stream_id` y `timestamp` desde columnas
3. Cada fila se convierte en DataPoint
4. Procesamiento async con job ID para tracking

**Implementación propuesta:**

```python
from fastapi import UploadFile, File, BackgroundTasks
import pandas as pd
import uuid


class CSVIngestConfig(BaseModel):
    """Configuración para ingesta de CSV."""
    
    domain: str = "iot"
    source_id: Optional[str] = None
    
    # Mapeo de columnas
    timestamp_column: str = "timestamp"
    value_columns: List[str] = Field(default_factory=list)  # Si vacío, todas las numéricas
    
    # Formato de timestamp
    timestamp_format: str = "unix"  # unix | iso8601 | custom
    timestamp_unit: str = "s"  # s | ms | us
    
    # Opciones
    skip_rows: int = 0
    max_rows: Optional[int] = None


@router.post("/ingest/csv")
async def ingest_csv(
    file: UploadFile = File(...),
    config: CSVIngestConfig = Depends(),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """Endpoint para ingesta de CSV.
    
    Soporta múltiples formatos:
    - CSV con timestamp + múltiples columnas de valores
    - CSV con timestamp + single value
    - CSV sin timestamp (usa row index)
    """
    
    # Generar job ID
    job_id = str(uuid.uuid4())
    
    # Leer CSV
    df = pd.read_csv(file.file, skiprows=config.skip_rows, nrows=config.max_rows)
    
    # Validar columnas
    if config.timestamp_column not in df.columns:
        raise HTTPException(400, f"Column '{config.timestamp_column}' not found")
    
    # Determinar columnas de valores
    value_columns = config.value_columns
    if not value_columns:
        # Auto-detectar columnas numéricas
        value_columns = df.select_dtypes(include=['number']).columns.tolist()
        value_columns.remove(config.timestamp_column)
    
    # Procesar en background
    background_tasks.add_task(
        process_csv_async,
        df=df,
        config=config,
        value_columns=value_columns,
        job_id=job_id,
        db=db,
    )
    
    return {
        "job_id": job_id,
        "status": "processing",
        "rows": len(df),
        "streams": len(value_columns),
    }


async def process_csv_async(
    df: pd.DataFrame,
    config: CSVIngestConfig,
    value_columns: List[str],
    job_id: str,
    db: Session,
):
    """Procesa CSV de forma asíncrona."""
    
    datapoints = []
    
    for idx, row in df.iterrows():
        # Parsear timestamp
        ts_value = row[config.timestamp_column]
        if config.timestamp_format == "unix":
            timestamp = float(ts_value)
            if config.timestamp_unit == "ms":
                timestamp /= 1000
            elif config.timestamp_unit == "us":
                timestamp /= 1000000
        elif config.timestamp_format == "iso8601":
            timestamp = pd.to_datetime(ts_value).timestamp()
        
        # Crear DataPoint por cada columna de valor
        for col in value_columns:
            value = float(row[col])
            
            datapoint = DataPoint(
                stream_id=f"{config.source_id}:{col}" if config.source_id else col,
                value=value,
                timestamp=timestamp,
                source_id=config.source_id,
                stream_type=col,
                metadata={
                    "domain": config.domain,
                    "csv_job_id": job_id,
                    "csv_row": idx,
                },
                ingested_at=time.time(),
            )
            
            datapoints.append(datapoint)
    
    # Procesar en batches
    batch_size = 1000
    for i in range(0, len(datapoints), batch_size):
        batch = datapoints[i:i+batch_size]
        handler = DataPointBatchHandler(db, get_broker())
        handler.ingest(batch, domain=config.domain)
    
    logger.info(f"[CSV] Job {job_id} completed: {len(datapoints)} datapoints")
```

**Ejemplo CSV:**

```csv
timestamp,temperature,humidity,pressure
1708900000.0,23.5,65.2,1013.25
1708900001.0,23.6,65.1,1013.24
1708900002.0,23.7,65.0,1013.23
```

Se convierte en 3 streams:
- `source:temperature`
- `source:humidity`
- `source:pressure`

---

## 4. CAMBIOS POR ARCHIVO

### 4.1 Archivos a MANTENER (sin cambios)

| Archivo | Justificación |
|---------|---------------|
| `common/config.py` | Configuración de BD es agnóstica |
| `ingest_api/auth/` | Autenticación es agnóstica |
| `ingest_api/rate_limiter.py` | Rate limiting es agnóstico |
| `ingest_api/metrics/` | Métricas son agnósticas |
| `ingest_api/debug.py` | Debug es agnóstico |

---

### 4.2 Archivos a REFACTORIZAR

| Archivo | Cambios | Justificación |
|---------|---------|---------------|
| `ingest_api/schemas.py` | Agregar `DataPointBatchIn`, `DataPointIn` | Mantener DTOs IoT legacy + nuevos agnósticos |
| `ingest_api/classification/reading_classifier.py` | Renombrar a `datapoint_classifier.py`, refactor interno | Abstraer lógica de clasificación |
| `ingest_api/classification/models.py` | Agregar `DataPointClass`, `ClassifiedDataPoint` | Mantener modelos IoT legacy + nuevos agnósticos |
| `ingest_api/classification/thresholds.py` | Crear `constraints_manager.py` paralelo | Abstraer queries SQL a interface agnóstica |
| `ingest_api/pipelines/router.py` | Crear `datapoint_router.py` paralelo | Mantener router IoT legacy + nuevo agnóstico |
| `ingest_api/mqtt/receiver.py` | Agregar soporte multi-domain topics | Extender sin romper topic IoT actual |
| `ingest_api/mqtt/processor.py` | Agregar `DataPointProcessor` paralelo | Mantener procesador IoT legacy + nuevo agnóstico |
| `ingest_api/endpoints/packet_ingest.py` | Mantener + agregar `/ingest/datapoints` | Coexistencia de endpoints |
| `ingest_api/core/domain/contracts.py` | Agregar `DataPoint`, `StreamConfig` | Mantener contratos IoT legacy + nuevos agnósticos |
| `ingest_api/core/domain/broker_interface.py` | Extender `Reading` → `DataPoint` | Backward compatible |

---

### 4.3 Archivos NUEVOS a crear

| Archivo | Propósito |
|---------|-----------|
| `ingest_api/core/domain/datapoint.py` | Contrato `DataPoint` universal |
| `ingest_api/core/domain/stream_config.py` | Configuración de streams |
| `ingest_api/classification/datapoint_classifier.py` | Clasificador agnóstico |
| `ingest_api/classification/constraints_manager.py` | Gestor de constraints agnóstico |
| `ingest_api/pipelines/datapoint_router.py` | Router agnóstico |
| `ingest_api/pipelines/datapoint_handler.py` | Handler agnóstico |
| `ingest_api/endpoints/datapoint_ingest.py` | Endpoint HTTP agnóstico |
| `ingest_api/endpoints/csv_ingest.py` | Endpoint CSV upload |
| `ingest_api/websocket/stream_ingest.py` | Endpoint WebSocket |
| `ingest_api/adapters/iot_adapter.py` | Adapter IoT → DataPoint |
| `ingest_api/adapters/infrastructure_adapter.py` | Adapter infra → DataPoint |
| `ingest_api/adapters/finance_adapter.py` | Adapter finanzas → DataPoint |

---

### 4.4 Archivos a DEPRECAR (futuro)

| Archivo | Cuándo deprecar | Criterio |
|---------|-----------------|----------|
| `ingest_api/schemas.py` (DTOs IoT) | Fase 4 (6+ meses) | Cuando 90% del tráfico use API agnóstica |
| `ingest_api/classification/reading_classifier.py` | Fase 4 (6+ meses) | Cuando todos los sensores migren a constraints |
| `ingest_api/pipelines/router.py` (IoT) | Fase 4 (6+ meses) | Cuando router agnóstico esté probado en prod |

---

## 5. ORDEN DE IMPLEMENTACIÓN

### FASE 1 — Contratos y Adapters (Semana 1-2)

**Objetivo:** Crear contratos agnósticos sin romper nada existente.

**Tareas:**
1. ✅ Crear `ingest_api/core/domain/datapoint.py` con `DataPoint` universal
2. ✅ Crear `ingest_api/core/domain/stream_config.py` con `ValueConstraints`, `RateConstraints`, `StreamConfig`
3. ✅ Crear `ingest_api/adapters/iot_adapter.py` con `DataPoint.from_iot_reading()`
4. ✅ Agregar tests unitarios para contratos
5. ✅ Documentar contratos en README

**Dependencias:** Ninguna  
**Riesgo:** Bajo (no toca código existente)

---

### FASE 2 — Clasificador Agnóstico (Semana 3-4)

**Objetivo:** Implementar clasificador agnóstico paralelo al IoT.

**Tareas:**
1. ✅ Crear `ingest_api/classification/datapoint_classifier.py`
2. ✅ Crear `ingest_api/classification/constraints_manager.py` (interface)
3. ✅ Implementar `SqlConstraintsManager` que lee de tablas IoT actuales
4. ✅ Agregar tests de clasificación con DataPoints
5. ✅ Validar que clasificador agnóstico produce mismos resultados que IoT

**Dependencias:** FASE 1  
**Riesgo:** Medio (lógica de negocio crítica)

---

### FASE 3 — Endpoint HTTP Agnóstico (Semana 5-6)

**Objetivo:** Agregar endpoint `/ingest/datapoints` sin romper `/ingest/packets`.

**Tareas:**
1. ✅ Crear `ingest_api/endpoints/datapoint_ingest.py`
2. ✅ Implementar `POST /ingest/datapoints` con `DataPointBatchIn`
3. ✅ Crear `DataPointBatchHandler` que usa clasificador agnóstico
4. ✅ Agregar tests de integración HTTP
5. ✅ Documentar API en OpenAPI/Swagger
6. ✅ Feature flag `ENABLE_AGNOSTIC_API=true`

**Dependencias:** FASE 1, FASE 2  
**Riesgo:** Medio (nuevo endpoint en producción)

---

### FASE 4 — MQTT Multi-Domain (Semana 7-8)

**Objetivo:** Extender MQTT para soportar topics multi-dominio.

**Tareas:**
1. ✅ Refactorizar `mqtt/receiver.py` para suscribirse a `{domain}/+/+`
2. ✅ Agregar parser de topic agnóstico
3. ✅ Mantener soporte para topic IoT legacy `iot/sensors/+/readings`
4. ✅ Agregar tests de MQTT con topics multi-dominio
5. ✅ Feature flag `ENABLE_MQTT_MULTI_DOMAIN=true`

**Dependencias:** FASE 1, FASE 2  
**Riesgo:** Alto (MQTT es crítico en producción)

---

### FASE 5 — WebSocket (Semana 9-10)

**Objetivo:** Implementar endpoint WebSocket para streaming.

**Tareas:**
1. ✅ Crear `ingest_api/websocket/stream_ingest.py`
2. ✅ Implementar `ws://host/ingest/stream`
3. ✅ Agregar `WebSocketManager` para gestionar conexiones
4. ✅ Agregar tests de WebSocket
5. ✅ Documentar protocolo WebSocket
6. ✅ Feature flag `ENABLE_WEBSOCKET_INGEST=true`

**Dependencias:** FASE 1, FASE 2  
**Riesgo:** Medio (nuevo transporte)

---

### FASE 6 — CSV Upload (Semana 11-12)

**Objetivo:** Implementar endpoint de CSV upload.

**Tareas:**
1. ✅ Crear `ingest_api/endpoints/csv_ingest.py`
2. ✅ Implementar `POST /ingest/csv` con multipart/form-data
3. ✅ Agregar procesamiento async con job tracking
4. ✅ Agregar tests de CSV con múltiples formatos
5. ✅ Documentar formato CSV esperado
6. ✅ Feature flag `ENABLE_CSV_INGEST=true`

**Dependencias:** FASE 1, FASE 2  
**Riesgo:** Bajo (procesamiento batch)

---

### FASE 7 — Migración Gradual (Mes 4-6)

**Objetivo:** Migrar sensores IoT existentes a API agnóstica.

**Tareas:**
1. ✅ Crear script de migración de `alert_thresholds` → `stream_constraints`
2. ✅ Migrar 10% de sensores a API agnóstica (canary)
3. ✅ Monitorear métricas de latencia, errores, throughput
4. ✅ Migrar 50% de sensores
5. ✅ Migrar 100% de sensores
6. ✅ Deprecar endpoints IoT legacy (DeprecationWarning)

**Dependencias:** FASE 1-6  
**Riesgo:** Alto (migración en producción)

---

### FASE 8 — Nuevos Dominios (Mes 7+)

**Objetivo:** Onboarding de nuevos dominios (infra, finanzas, salud).

**Tareas:**
1. ✅ Crear adapters específicos por dominio
2. ✅ Configurar constraints por dominio
3. ✅ Documentar onboarding de nuevos dominios
4. ✅ Crear templates de configuración

**Dependencias:** FASE 7  
**Riesgo:** Bajo (dominios nuevos, no afectan IoT)

---

## 6. RIESGOS Y MITIGACIONES

### RIESGO 1 — Regresión en Pipeline IoT Actual

**Probabilidad:** Alta  
**Impacto:** Crítico  
**Descripción:** Cambios en clasificador o router pueden romper flujo IoT existente.

**Mitigación:**
1. **Coexistencia total:** Mantener código IoT legacy intacto durante FASE 1-6
2. **Feature flags:** Activar API agnóstica solo en entornos de test inicialmente
3. **Tests de regresión:** Suite completa de tests IoT debe pasar al 100% en cada fase
4. **Canary deployment:** Migrar 10% de sensores primero, monitorear 1 semana
5. **Rollback plan:** Script de rollback automático si error rate > 1%

**Criterio de éxito:** 0 regresiones en pipeline IoT durante FASE 1-6.

---

### RIESGO 2 — Performance Degradation

**Probabilidad:** Media  
**Impacto:** Alto  
**Descripción:** Capa de abstracción puede agregar latencia.

**Mitigación:**
1. **Benchmarking:** Medir latencia p50/p95/p99 antes y después de cada fase
2. **Target:** Latencia agnóstica ≤ latencia IoT + 5ms
3. **Optimización:** Cache de constraints en memoria (LRU, TTL 60s)
4. **Profiling:** Identificar bottlenecks con py-spy/cProfile
5. **Async processing:** WebSocket y CSV usan procesamiento async

**Criterio de éxito:** Latencia p95 < 50ms para HTTP, < 10ms para WebSocket.

---

### RIESGO 3 — Complejidad de Migración de Datos

**Probabilidad:** Media  
**Impacto:** Alto  
**Descripción:** Migrar `alert_thresholds` → `stream_constraints` puede ser complejo.

**Mitigación:**
1. **Dual-write:** Escribir en ambas tablas durante FASE 7
2. **Script de migración:** Automatizar conversión de thresholds → constraints
3. **Validación:** Comparar resultados de clasificador IoT vs agnóstico
4. **Rollback:** Mantener tablas IoT legacy hasta FASE 8
5. **Feature flag:** `USE_AGNOSTIC_CONSTRAINTS=false` por defecto

**Criterio de éxito:** 100% de sensores migrados sin cambios en clasificación.

---

## 7. ESTRATEGIA DE COMPATIBILIDAD

### 7.1 Coexistencia de APIs

**Durante FASE 1-6:**
- Endpoint IoT: `POST /ingest/packets` (legacy, mantener)
- Endpoint agnóstico: `POST /ingest/datapoints` (nuevo, feature flag)
- Ambos endpoints funcionan en paralelo
- Feature flag `ENABLE_AGNOSTIC_API` controla activación

**Durante FASE 7:**
- Endpoint IoT: `POST /ingest/packets` (deprecated, DeprecationWarning)
- Endpoint agnóstico: `POST /ingest/datapoints` (recomendado)
- Migración gradual de clientes

**Durante FASE 8:**
- Endpoint IoT: `POST /ingest/packets` (eliminado)
- Endpoint agnóstico: `POST /ingest/datapoints` (único)

---

### 7.2 Adapter IoT → DataPoint

**Estrategia:** Crear adapter que traduce automáticamente lecturas IoT a DataPoints.

```python
class IoTToDataPointAdapter:
    """Adapter que traduce lecturas IoT a DataPoints.
    
    Permite que código IoT legacy funcione sin cambios.
    """
    
    @staticmethod
    def adapt_reading(
        sensor_id: int,
        value: float,
        timestamp: float,
        device_uuid: Optional[str] = None,
        sensor_type: Optional[str] = None,
    ) -> DataPoint:
        """Convierte lectura IoT a DataPoint."""
        return DataPoint.from_iot_reading(
            sensor_id=sensor_id,
            value=value,
            timestamp=timestamp,
            device_uuid=device_uuid,
            sensor_type=sensor_type,
        )
    
    @staticmethod
    def adapt_classified_reading(
        classified: ClassifiedReading,
    ) -> ClassifiedDataPoint:
        """Convierte ClassifiedReading IoT a ClassifiedDataPoint."""
        classification_map = {
            ReadingClass.ALERT: DataPointClass.ALERT,
            ReadingClass.WARNING: DataPointClass.WARNING,
            ReadingClass.ML_PREDICTION: DataPointClass.NORMAL,
        }
        
        return ClassifiedDataPoint(
            stream_id=str(classified.sensor_id),
            value=classified.value,
            timestamp=classified.device_timestamp.timestamp(),
            classification=classification_map[classified.classification],
            reason=classified.reason,
            metadata={"domain": "iot"},
        )
```

**Uso en código existente:**

```python
# ANTES (código IoT legacy)
reading = SensorReadingIn(sensor_id=123, value=23.5, timestamp=datetime.now())
classified = classifier.classify(reading.sensor_id, reading.value, ...)

# DESPUÉS (con adapter, sin cambios en lógica)
reading = SensorReadingIn(sensor_id=123, value=23.5, timestamp=datetime.now())
datapoint = IoTToDataPointAdapter.adapt_reading(
    sensor_id=reading.sensor_id,
    value=reading.value,
    timestamp=reading.timestamp.timestamp(),
)
classified_dp = datapoint_classifier.classify(datapoint)
```

---

### 7.3 Feature Flags

**Flags propuestos:**

```python
# .env
ENABLE_AGNOSTIC_API=false          # Activar endpoint /ingest/datapoints
ENABLE_MQTT_MULTI_DOMAIN=false     # Activar topics multi-dominio
ENABLE_WEBSOCKET_INGEST=false      # Activar WebSocket
ENABLE_CSV_INGEST=false            # Activar CSV upload
USE_AGNOSTIC_CONSTRAINTS=false     # Usar stream_constraints en vez de alert_thresholds
```

**Implementación:**

```python
from functools import lru_cache
import os


@lru_cache(maxsize=1)
def get_feature_flags() -> dict:
    """Carga feature flags desde .env."""
    return {
        "agnostic_api": os.getenv("ENABLE_AGNOSTIC_API", "false").lower() == "true",
        "mqtt_multi_domain": os.getenv("ENABLE_MQTT_MULTI_DOMAIN", "false").lower() == "true",
        "websocket": os.getenv("ENABLE_WEBSOCKET_INGEST", "false").lower() == "true",
        "csv": os.getenv("ENABLE_CSV_INGEST", "false").lower() == "true",
        "agnostic_constraints": os.getenv("USE_AGNOSTIC_CONSTRAINTS", "false").lower() == "true",
    }
```

---

## 8. CRITERIOS DE ÉXITO

### 8.1 Criterios Técnicos

| Criterio | Target | Medición |
|----------|--------|----------|
| **Latencia HTTP p95** | < 50ms | Prometheus metrics |
| **Latencia WebSocket p95** | < 10ms | Prometheus metrics |
| **Throughput** | ≥ 10,000 datapoints/s | Load testing |
| **Error rate** | < 0.1% | Prometheus metrics |
| **Test coverage** | ≥ 90% | pytest-cov |
| **Regresiones IoT** | 0 | Test suite IoT |

---

### 8.2 Criterios de Negocio

| Criterio | Target | Medición |
|----------|--------|----------|
| **Migración IoT** | 100% sensores | Dashboard |
| **Nuevos dominios** | ≥ 2 (infra + finanzas) | Onboarding docs |
| **Downtime** | 0 durante migración | Uptime monitoring |
| **Rollbacks** | 0 en producción | Deployment logs |

---

## 9. CONCLUSIÓN

### 9.1 Resumen Ejecutivo

Esta propuesta transforma `iot_ingest_services` de un servicio acoplado a IoT en un **servicio de ingesta universal multi-transporte** que soporta:

- ✅ **Múltiples dominios:** IoT, infraestructura, finanzas, salud
- ✅ **Múltiples transportes:** HTTP REST, MQTT, WebSocket, CSV
- ✅ **Contratos agnósticos:** `DataPoint`, `StreamConfig`, `ValueConstraints`
- ✅ **Migración sin downtime:** Coexistencia de APIs legacy + agnósticas
- ✅ **Extensibilidad:** Agregar nuevos dominios sin modificar core

---

### 9.2 Próximos Pasos

1. **Revisión de propuesta:** Discutir con equipo arquitectónico
2. **Aprobación de fases:** Aprobar FASE 1-2 para comenzar implementación
3. **Creación de épicas:** Crear épicas en Jira para cada fase
4. **Asignación de recursos:** Asignar 2 devs para FASE 1-2 (4 semanas)

---

### 9.3 Preguntas Abiertas

1. ¿Qué dominios priorizar después de IoT? (infra vs finanzas vs salud)
2. ¿Migrar tablas SQL a esquema agnóstico o mantener dual-write?
3. ¿Usar Kafka en vez de Redis Streams para publicación?
4. ¿Implementar gRPC como transporte adicional?

---

**FIN DE PROPUESTA**
