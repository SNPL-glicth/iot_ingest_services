# ingest_api/pipelines

Pipeline de procesamiento de lecturas clasificadas por propósito.

## Estructura

```
pipelines/
├── router.py              # ReadingRouter — clasifica y enruta cada lectura
├── router_sp.py           # execute_sp_with_retry, get_sensor_type_cached
├── router_models.py       # PipelineType enum, SP_RETRY_CONFIG
├── sensor_resolver.py     # resolve_sensor_id (device_uuid+sensor_uuid → int, cache)
│
├── contracts/
│   └── unified_reading.py # UnifiedReading — contrato único entre todos los pipelines
│
├── handlers/              # Handlers de alto nivel (usados por endpoints)
│   ├── single.py          # SingleReadingHandler
│   └── batch.py           # BatchReadingHandler
│
├── alerts/                # Pipeline ALERT — violaciones de rango físico
│   ├── alert_ingest.py    # AlertIngestPipeline (clase principal)
│   ├── alert_rules.py     # AlertRules.accepts(), .get_severity()
│   ├── alert_persistence.py  # Persiste en dbo.alerts + dbo.sensor_readings
│   ├── alert_repository.py   # Queries SQL de alertas
│   └── notification_service.py
│
├── warnings/              # Pipeline WARNING — delta spikes
│   ├── warning_ingest.py  # WarningIngestPipeline
│   ├── warning_rules.py   # WarningRules.accepts()
│   └── warning_persistence.py  # Persiste en dbo.ml_events (DELTA_SPIKE)
│
├── predictions/           # Pipeline PREDICTION — datos limpios para ML
│   ├── prediction_ingest.py   # PredictionIngestPipeline
│   ├── prediction_rules.py    # PredictionRules.accepts()
│   └── prediction_dispatch.py # Dispatch al broker ML (IReadingBroker)
│
├── shared/                # Utilidades compartidas entre pipelines
│   ├── validation.py      # is_suspicious_zero_reading, log_suspicious_reading
│   ├── guards.py          # guard_reading — validaciones defensivas
│   ├── delta_utils.py     # compute_delta_spike (lógica pura, sin estado)
│   ├── physical_ranges.py # PhysicalRanges por tipo de sensor
│   ├── threshold_queries.py
│   └── suspicious_readings.py
│
└── resilience/            # Componentes de resiliencia
    ├── factory.py         # get_resilience_components() → (dedup, dlq)
    ├── circuit_breaker.py # CircuitBreaker (CLOSED/OPEN/HALF_OPEN)
    ├── circuit_breaker_config.py
    ├── circuit_breaker_utils.py
    ├── deduplication.py   # MessageDeduplicator (Redis SET NX, TTL 60s)
    ├── dead_letter.py     # DeadLetterQueue (Redis List)
    ├── dlq_consumer.py    # DLQConsumer — reintenta mensajes fallidos
    ├── dlq_models.py
    ├── dlq_operations.py
    ├── dlq_startup.py
    └── retry.py           # retry_with_backoff
```

---

## Flujo de enrutamiento

```
ReadingRouter.route(sensor_id, value, timestamp)
  │
  ├── guard_reading()              ← rechaza NaN, infinitos, sensor_id inválido
  ├── is_suspicious_zero_reading() ← loguea pero no rechaza
  ├── MessageDeduplicator.is_duplicate()  ← Redis SET NX
  │
  └── execute_sp_with_retry()     ← SP sp_insert_reading_and_check_threshold
        │
        └── Resultado del SP determina pipeline:
              ├── violación física  → AlertIngestPipeline
              ├── delta spike       → WarningIngestPipeline
              └── limpio            → PredictionIngestPipeline
```

---

## Los tres pipelines

### ALERT — Violaciones de rango físico

**Condición:** `value < physical_min OR value > physical_max`

```python
AlertRules.accepts(sensor_id, value, physical_range)
  → (True, range, reason) si viola rango físico
  → (False, ...) si no aplica
```

**Persiste:**
- `dbo.sensor_readings` — lectura que rompe el umbral
- `dbo.alerts` — alerta activa (cierra la anterior si existe)
- `dbo.alert_notifications` — notificación push

**Reglas estrictas:**
- Severity siempre `critical` — no se puede degradar
- Solo 1 alerta activa por sensor
- **NUNCA** envía al broker ML

---

### WARNING — Delta spikes

**Condición:** cambio brusco de valor en tiempo corto (`|Δvalue/Δt| > threshold`)

```python
WarningRules.accepts(sensor_id, value, prev_value, delta_t)
  → (True, reason) si hay delta spike
```

**Persiste:**
- `dbo.ml_events` con `event_type=DELTA_SPIKE`

**Reglas estrictas:**
- Solo 1 advertencia activa por sensor
- **NUNCA** envía al broker ML

---

### PREDICTION — Datos limpios para ML

**Condición:** sin violación física, sin delta spike

```python
PredictionRules.accepts(classified_reading)
  → True si ReadingClass.ML_PREDICTION
```

**Persiste:**
- `dbo.sensor_readings_latest` — última lectura válida

**Dispatch:**
- `broker.publish(reading)` → ML online (`SimpleMlOnlineProcessor`)

---

## Resiliencia

### Deduplicación

```python
dedup = MessageDeduplicator(redis_client, ttl_seconds=60)
if dedup.is_duplicate(sensor_id, timestamp):
    return  # descarta silenciosamente
```

Usa Redis `SET NX EX 60`. Si Redis no está disponible, se deshabilita automáticamente.

### Dead Letter Queue

Mensajes que fallan tras todos los reintentos van a la DLQ (Redis List).
`DLQConsumer` los reintenta periódicamente con backoff.

```bash
# Ver estado DLQ
GET /resilience/health
```

### Circuit Breaker

Protege el SP de SQL Server. Tras `CB_FAILURE_THRESHOLD` fallos consecutivos
pasa a estado OPEN (rechaza llamadas) y prueba recuperación en HALF_OPEN.

---

## sensor_resolver.py

Convierte `(device_uuid, sensor_uuid)` → `sensor_id: int` con cache en memoria:

```python
sensor_id = resolve_sensor_id(db, device_uuid, sensor_uuid)
# Cache TTL: SENSOR_CACHE_TTL segundos (default 300)
# Valida que sensor_uuid pertenece al device_uuid
```

---

## contracts/unified_reading.py

`UnifiedReading` es el contrato único que fluye entre todos los pipelines:

```python
@dataclass
class UnifiedReading:
    sensor_id: int
    value: float
    device_timestamp: datetime
    received_at: datetime
    reading_class: ReadingClass      # ALERT | WARNING | ML_PREDICTION
    physical_range: PhysicalRange
    delta_info: Optional[DeltaInfo]
    audit_id: str                    # UUID para trazabilidad
```
