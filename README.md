# iot_ingest_services

Microservicio Python de **ingesta universal multi-dominio** y jobs batch auxiliares.

**Arquitectura Agnóstica:** Soporta múltiples dominios (IoT, infrastructure, finance, health) con un núcleo universal. IoT funciona como un adapter más, manteniendo 100% compatibilidad con el sistema legacy.

Recibe datos vía HTTP, MQTT, WebSocket o CSV, los clasifica según configuración por stream, persiste en SQL Server (IoT) o PostgreSQL (otros dominios) y publica al broker ML. Los jobs batch ejecutan predicciones baseline y enriquecen anomalías con explicaciones.

---

## Estructura del proyecto

```
iot_ingest_services/
├── common/                        # Configuración y conexión BD compartida
│   ├── config.py                  # Settings (DB_HOST, DB_PORT, DB_USER…)
│   └── db.py                      # get_engine() singleton SQLAlchemy
│
├── ingest_api/                    # FastAPI — punto de entrada HTTP
│   ├── main.py                    # Wiring: routers + startup/shutdown (IoT + Universal)
│   ├── schemas.py                 # Pydantic models (IoT legacy + Universal)
│   ├── batch_inserter.py          # BatchInserter async (buffer + flush)
│   ├── rate_limiter.py            # Rate limiting por IP y dispositivo
│   ├── device_auth.py             # Validación de device_uuid + sensor_uuid
│   ├── debug.py                   # Helpers de debug (solo dev)
│   │
│   ├── endpoints/                 # Rutas HTTP
│   │   ├── health.py              # GET /health, /health/postgres
│   │   ├── packet_ingest.py       # POST /ingest/packets  ← IoT recomendado
│   │   ├── single_ingest.py       # POST /ingest/readings  (IoT legacy)
│   │   ├── batch_ingest.py        # POST /ingest/readings/bulk  (IoT legacy)
│   │   ├── sensor_status.py       # GET /sensors/{id}/status
│   │   ├── diagnostics.py         # GET /diagnostics
│   │   └── resilience_health.py   # GET /resilience/health
│   │
│   ├── auth/                      # Autenticación y Autorización
│   │   ├── api_key.py             # X-API-Key global (legacy/dev)
│   │   ├── device_key.py          # X-Device-Key por dispositivo (IoT)
│   │   ├── authorization.py       # Roles: ADMIN, SOURCE_WRITER, READ_ONLY (ISO 27001)
│   │   └── api_key_validator.py   # Validación contra PostgreSQL con SHA-256
│   │
│   ├── broker/                    # Publicación de lecturas al ML
│   │   ├── factory.py             # get_broker() — crea broker según config
│   │   ├── throttled.py           # ThrottledReadingBroker (rate limit por sensor)
│   │   └── ml_broker_adapter.py   # Adapter ML → IReadingBroker
│   │
│   ├── classification/            # Clasificación de lecturas ANTES de persistir
│   │   ├── reading_classifier.py  # ReadingClassifier → ALERT/WARNING/ML_PREDICTION
│   │   ├── state_manager.py       # SensorStateManager (estado operacional)
│   │   ├── state_repository.py    # Persistencia de estado en BD
│   │   ├── state_models.py        # SensorOperationalState enum
│   │   ├── thresholds.py          # ThresholdManager (carga umbrales)
│   │   ├── delta_detector.py      # DeltaDetector (spikes de velocidad)
│   │   ├── consecutive_tracker.py # ConsecutiveTracker (N lecturas seguidas)
│   │   └── models.py              # ReadingClass, ClassifiedReading, PhysicalRange
│   │
│   ├── pipelines/                 # Pipelines de procesamiento por propósito
│   │   ├── router.py              # ReadingRouter — clasifica y enruta
│   │   ├── router_sp.py           # execute_sp_with_retry, get_sensor_type_cached
│   │   ├── router_models.py       # PipelineType enum, SP_RETRY_CONFIG
│   │   ├── sensor_resolver.py     # resolve_sensor_id (UUID → int con cache)
│   │   ├── contracts/
│   │   │   └── unified_reading.py # UnifiedReading — contrato único entre pipelines
│   │   ├── handlers/
│   │   │   ├── single.py          # SingleReadingHandler
│   │   │   └── batch.py           # BatchReadingHandler
│   │   ├── alerts/
│   │   │   ├── alert_ingest.py    # AlertIngestPipeline
│   │   │   ├── alert_rules.py     # Reglas: solo violaciones de rango físico
│   │   │   ├── alert_persistence.py # Persiste en dbo.alerts
│   │   │   ├── alert_repository.py  # Queries SQL de alertas
│   │   │   └── notification_service.py
│   │   ├── warnings/
│   │   │   ├── warning_ingest.py  # WarningIngestPipeline
│   │   │   ├── warning_rules.py   # Reglas: solo delta spikes
│   │   │   └── warning_persistence.py # Persiste en dbo.ml_events (DELTA_SPIKE)
│   │   ├── predictions/
│   │   │   ├── prediction_ingest.py   # PredictionIngestPipeline
│   │   │   ├── prediction_rules.py    # Reglas: solo datos limpios
│   │   │   └── prediction_dispatch.py # Dispatch al broker ML
│   │   ├── shared/
│   │   │   ├── validation.py      # is_suspicious_zero_reading, log_suspicious_reading
│   │   │   ├── guards.py          # guard_reading (validaciones defensivas)
│   │   │   ├── delta_utils.py     # Detección de delta spikes (lógica pura)
│   │   │   ├── physical_ranges.py # Rangos físicos por tipo de sensor
│   │   │   ├── threshold_queries.py
│   │   │   └── suspicious_readings.py
│   │   └── resilience/
│   │       ├── factory.py         # get_resilience_components() — crea dedup + DLQ
│   │       ├── circuit_breaker.py # CircuitBreaker (CLOSED/OPEN/HALF_OPEN)
│   │       ├── circuit_breaker_config.py
│   │       ├── circuit_breaker_utils.py
│   │       ├── deduplication.py   # MessageDeduplicator (Redis SET NX, TTL 60s)
│   │       ├── dead_letter.py     # DeadLetterQueue (Redis List)
│   │       ├── dlq_consumer.py    # DLQConsumer — reintenta mensajes fallidos
│   │       ├── dlq_models.py
│   │       ├── dlq_operations.py
│   │       ├── dlq_startup.py
│   │       └── retry.py           # retry_with_backoff
│   │
│   ├── mqtt/                      # Receptor MQTT (canal principal de ingesta)
│   │   ├── simple_receiver.py     # SimpleReceiver — paho-mqtt → SP directo
│   │   ├── receiver.py            # MQTTReceiver (modular, FF_MQTT_MODULAR_RECEIVER)
│   │   ├── receiver_singleton.py  # Singleton del receptor
│   │   ├── receiver_connections.py
│   │   ├── receiver_stats.py
│   │   ├── processor.py           # MQTTProcessor — procesa mensaje MQTT
│   │   ├── async_processor.py     # AsyncMQTTProcessor
│   │   ├── message_handler.py     # handle_message()
│   │   ├── validators.py          # Validación de payload MQTT
│   │   ├── connections.py         # Gestión de conexiones MQTT
│   │   └── backpressure.py        # Backpressure (cola + semáforo)
│   │
│   ├── core/                      # NÚCLEO UNIVERSAL AGNÓSTICO
│   │   ├── receiver.py            # start/stop_modular_receiver (IoT)
│   │   ├── domain/                # Contratos universales multi-dominio
│   │   │   ├── data_point.py      # DataPoint (reemplaza Reading como universal)
│   │   │   ├── stream_config.py   # StreamConfig + ValueConstraints
│   │   │   ├── classification.py  # DataPointClass + ClassificationResult
│   │   │   ├── series_id.py       # SeriesIdMapper (universal ↔ IoT)
│   │   │   ├── reading.py         # Reading dataclass (IoT legacy)
│   │   │   ├── contracts.py       # Contratos de dominio (IoT)
│   │   │   └── broker_interface.py # IReadingBroker ABC
│   │   ├── classification/        # Clasificador universal
│   │   │   ├── universal_classifier.py  # UniversalClassifier (thread-safe)
│   │   │   └── config_provider.py       # StreamConfigProvider + defaults
│   │   ├── adapters/
│   │   │   └── mqtt_adapter.py    # MQTTReadingAdapter (IoT)
│   │   ├── pipeline/
│   │   │   ├── processor.py       # ReadingProcessor (IoT)
│   │   │   └── sp_executor.py     # SPExecutor (IoT)
│   │   ├── transport/
│   │   │   ├── mqtt_client.py     # MQTTClient (IoT)
│   │   │   └── message_handler.py
│   │   ├── validation/
│   │   │   ├── payload_validator.py
│   │   │   └── reading_validator.py
│   │   ├── redis/
│   │   │   ├── connection.py      # RedisConnection singleton
│   │   │   └── publisher.py       # RedisStreamPublisher (XADD readings:validated)
│   │   └── monitoring/
│   │       ├── health.py          # HealthChecker
│   │       └── stats.py           # StatsCollector
│   │
│   ├── adapters/                  # ADAPTERS DE DOMINIO
│   │   └── iot/
│   │       ├── adapter.py         # IoTAdapter: Reading ↔ DataPoint bridge
│   │       └── __init__.py
│   │
│   ├── transports/                # TRANSPORTES UNIVERSALES (plugins)
│   │   ├── base.py                # IngestTransport ABC
│   │   ├── http/
│   │   │   ├── transport.py       # HTTPTransport
│   │   │   ├── schemas.py         # DataPacketIn, DataIngestResult
│   │   │   └── endpoints.py       # POST /ingest/data (universal)
│   │   ├── mqtt/
│   │   │   ├── transport.py       # MQTTTransport (universal)
│   │   │   └── receiver.py        # UniversalMQTTReceiver (FF_MQTT_UNIVERSAL)
│   │   ├── websocket/
│   │   │   └── handler.py         # WebSocket ingestion
│   │   └── csv/
│   │       ├── processor.py       # CSVProcessor
│   │       └── endpoints.py       # POST /ingest/csv
│   │
│   ├── infrastructure/            # INFRAESTRUCTURA UNIVERSAL
│   │   ├── persistence/
│   │   │   ├── postgres.py        # PostgreSQLStorage (dominios no-IoT)
│   │   │   ├── domain_storage_router.py  # Router: IoT→SQL Server, otros→PostgreSQL
│   │   │   └── migrations/
│   │   │       ├── postgres_001.sql      # Schema PostgreSQL
│   │   │       ├── 001_audit_log.sql     # Audit trail table
│   │   │       └── 002_api_keys_roles.sql # API keys con roles
│   │   └── audit/
│   │       └── audit_logger.py    # AuditLogger (ISO 27001 A.12.4)
│   │
│   ├── metrics/                   # Métricas de ingesta
│   │   ├── service.py             # MetricsService
│   │   ├── aggregator.py          # MetricsAggregator
│   │   ├── models.py              # IngestionMetrics dataclass
│   │   ├── sensor_stats.py        # SensorStats
│   │   └── timing_stats.py        # TimingStats
│   │
│   └── queries/                   # Consultas BD auxiliares
│       └── sensor_status.py       # get_sensor_status()
│
└── jobs/                          # Jobs batch independientes
    ├── ml_batch_runner.py         # Facade → jobs/batch/runner.py
    ├── ai_explainer_runner.py     # Enriquece anomalías con explicaciones AI
    └── batch/
        ├── runner.py              # Orquestador del ciclo batch
        ├── config.py              # RunnerConfig (env vars)
        ├── db_queries.py          # SQL helpers (lecturas, predicciones)
        ├── prediction.py          # Lógica de predicción + umbrales
        ├── enterprise.py          # BatchEnterpriseContainer (bridge ML)
        ├── threshold_events.py    # Eventos de umbral
        └── retry.py               # Retry con backoff
```

---

## Endpoints HTTP

| Método | Ruta | Auth | Descripción |
|---|---|---|---|
| `GET` | `/health` | — | Health check |
| `GET` | `/mqtt/health` | — | Estado del receptor MQTT |
| `GET` | `/mqtt/stats` | — | Estadísticas MQTT |
| `GET` | `/sensors/{id}/status` | API Key | Estado operacional del sensor |
| `GET` | `/diagnostics` | API Key | Diagnóstico interno |
| `GET` | `/resilience/health` | — | Estado circuit breaker + DLQ |
| `POST` | `/ingest/packets` | Device Key | **IoT recomendado** — paquete por device_uuid |
| `POST` | `/ingest/readings` | API Key | IoT legacy — lectura única por sensor_id |
| `POST` | `/ingest/readings/bulk` | API Key | IoT legacy — lote por sensor_id |
| `POST` | `/ingest/data` | API Key (roles) | **Universal** — ingesta multi-dominio (no IoT) |
| `POST` | `/ingest/csv` | API Key | **Universal** — ingesta batch CSV |
| `GET` | `/health/postgres` | — | Health check PostgreSQL |

### Autenticación y Autorización

| Header | Modo | Cuándo usar | Dominios |
|---|---|---|---|
| `X-Device-Key` | Por dispositivo | `/ingest/packets` — producción | IoT |
| `X-API-Key` | Global | Endpoints legacy, desarrollo | IoT |
| `X-API-Key` | Con roles (PostgreSQL) | `/ingest/data`, `/ingest/csv` | Universal (no IoT) |

**Roles de autorización (ISO 27001 A.9.2.3):**
- `ADMIN`: Acceso completo a todos los source_id y dominios
- `SOURCE_WRITER`: Solo puede escribir en su source_id asignado
- `READ_ONLY`: Sin permisos de escritura

API keys universales se validan contra tabla `api_keys` en PostgreSQL con hash SHA-256.

Controlado por `DEVICE_AUTH_ENABLED=1` (IoT) en `.env`.

---

## Flujo de ingesta IoT HTTP (`POST /ingest/packets`)

```
POST /ingest/packets
  │
  ├── Rate limiting (por IP + dispositivo)
  ├── Auth: X-Device-Key → valida device_uuid
  ├── resolve_sensor_id(device_uuid, sensor_uuid) → sensor_id  [cache TTL]
  │
  └── Por cada lectura en el paquete:
        │
        ├── ReadingClassifier.classify()
        │     ├── ThresholdManager → carga rangos físicos
        │     ├── DeltaDetector → detecta spike de velocidad
        │     └── → ReadingClass: ALERT | WARNING | ML_PREDICTION
        │
        ├── MessageDeduplicator.is_duplicate()  [Redis SET NX, TTL 60s]
        │
        └── ReadingRouter.route()
              ├── ALERT      → AlertIngestPipeline
              │                 → dbo.sensor_readings + dbo.alerts + dbo.alert_notifications
              │                 → NO envía al broker ML
              ├── WARNING    → WarningIngestPipeline
              │                 → dbo.ml_events (DELTA_SPIKE)
              │                 → NO envía al broker ML
              └── ML_PREDICTION → PredictionIngestPipeline
                                  → dbo.sensor_readings_latest
                                  → broker.publish(reading)  → ML online
```

---

## Flujo de ingesta Universal HTTP (`POST /ingest/data`)

```
POST /ingest/data
  │
  ├── Autenticación: X-API-Key → valida contra PostgreSQL (SHA-256)
  ├── Autorización: verifica source_id ownership + domain permitido
  ├── Rechaza domain='iot' (debe usar /ingest/packets)
  │
  └── Por cada DataPoint en el paquete:
        │
        ├── HTTPTransport.parse_message() → DataPoint
        ├── SeriesIdMapper.build_series_id(domain, source, stream)
        │
        ├── UniversalClassifier.classify()
        │     ├── StreamConfigProvider → carga constraints por stream
        │     ├── Verifica: physical_range, operational_range, warning_zone
        │     ├── Detecta: delta excesivo (absoluto/relativo)
        │     └── → DataPointClass: NORMAL | WARNING | CRITICAL | ANOMALY
        │
        └── DomainStorageRouter.insert()
              ├── domain='iot' → NotImplementedError (usar pipeline IoT)
              └── otros dominios → PostgreSQL (data_points table)
                                 → AuditLogger (ingestion_audit_log)
```

---

## Flujo MQTT IoT (`FF_MQTT_INGEST_ENABLED=true`)

```
Dispositivo/Simulador
  → MQTT topic: iot/sensors/{id}/readings
  → SimpleReceiver (paho-mqtt)
      ├── parse_json + validate
      ├── MessageDeduplicator [Redis SET NX]
      └── SP sp_insert_reading_and_check_threshold
            ├── INSERT dbo.sensor_readings
            ├── Evalúa umbrales → dbo.alerts, dbo.alert_notifications
            ├── Detecta delta spike → dbo.ml_events
            └── Redis XADD readings:validated  (opcional, para ML)
```

**Dos receptores MQTT independientes:**

1. **MQTT IoT** (`FF_MQTT_INGEST_ENABLED=true`):
   - Topics: `iot/sensors/{id}/readings`
   - Implementaciones:
     - `FF_MQTT_MODULAR_RECEIVER=false` → `mqtt/simple_receiver.py` (paho → SP directo)
     - `FF_MQTT_MODULAR_RECEIVER=true` → `core/receiver.py` (arquitectura modular)

2. **MQTT Universal** (`FF_MQTT_UNIVERSAL=true`):
   - Topics: `{domain}/{source}/{stream}/data`
   - Implementación: `transports/mqtt/receiver.py`
   - Pipeline: MQTTTransport → UniversalClassifier → PostgreSQL
   - Rechaza domain='iot' automáticamente
   - Incluye deduplicación (5 min TTL)

---

## Clasificación de datos

### Clasificación IoT (ReadingClassifier)

Cada lectura IoT se clasifica **antes** de persistir en uno de tres flujos:

| Clase | Condición | Persiste en | Envía a ML |
|---|---|---|---|
| `ALERT` | Viola rango físico del sensor | `dbo.alerts` + `dbo.sensor_readings` | ❌ No |
| `WARNING` | Delta spike detectado | `dbo.ml_events` (DELTA_SPIKE) | ❌ No |
| `ML_PREDICTION` | Dato limpio | `dbo.sensor_readings_latest` | ✅ Sí |

Regla: **solo 1 alerta/advertencia activa por sensor** — se cierra la anterior antes de crear una nueva.

### Clasificación Universal (UniversalClassifier)

Cada DataPoint universal se clasifica según StreamConfig:

| Clase | Condición | Persiste | Alerta |
|---|---|---|---|
| `NORMAL` | Dentro de todos los rangos | ✅ Sí | ❌ No |
| `WARNING_VIOLATION` | Fuera de rango operacional o zona warning | ✅ Sí | ⚠️ Configurable |
| `CRITICAL_VIOLATION` | Fuera de rango físico o zona crítica | ✅ Sí | ✅ Sí |
| `ANOMALY_DETECTED` | Delta excesivo (absoluto/relativo) | ✅ Sí | ⚠️ Configurable |

**Thread-safe:** UniversalClassifier usa locks para acceso concurrente a valores previos.

---

## Resiliencia

| Componente | Implementación | Config |
|---|---|---|
| **Deduplicación** | Redis SET NX, TTL 60s | `REDIS_URL`, `REDIS_ENABLED` |
| **Dead Letter Queue** | Redis List | `DLQ_MAX_SIZE=1000` |
| **Circuit Breaker** | CLOSED/OPEN/HALF_OPEN | `CB_FAILURE_THRESHOLD=5` |
| **Retry** | Backoff exponencial | `SP_MAX_RETRIES=3` |
| **Rate Limiting** | Ventana deslizante en memoria | `RATE_LIMIT_*` env vars |

Si Redis no está disponible, deduplicación y DLQ se deshabilitan automáticamente (degradación graceful).

---

## Jobs batch

### `jobs/ml_batch_runner.py` (facade → `jobs/batch/`)

Ciclo batch de predicciones ML:

```
jobs/batch/runner.py
  → db_queries.list_active_sensors()
  → Por cada sensor:
      → db_queries.load_recent_readings(sensor_id, window=50)
      → prediction.compute_prediction(values)   ← Taylor / Baseline
      → prediction.check_threshold_events()
      → db_queries.save_prediction()
      → threshold_events.emit_if_needed()
```

Ejecutar: `python -m jobs.ml_batch_runner`

### `jobs/ai_explainer_runner.py`

Enriquece predicciones anómalas con explicaciones del servicio AI:

```
SELECT predictions WHERE is_anomaly=1 AND anomaly_score >= threshold AND explanation IS NULL
  → POST AI_EXPLAINER_URL/explain/anomaly
  → UPDATE predictions SET explanation = {...}
```

Ejecutar: `python -m jobs.ai_explainer_runner`

Si `ai-explainer` está caído: omite esa predicción y continúa (no bloquea).

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `DB_HOST` | `localhost` | Host SQL Server |
| `DB_PORT` | `1434` | Puerto SQL Server |
| `DB_USER` | `sa` | Usuario BD |
| `DB_PASSWORD` | — | Contraseña BD |
| `DB_NAME` | `iot_monitoring_system` | Nombre BD |
| `ODBC_DRIVER` | `ODBC Driver 17 for SQL Server` | Driver ODBC |
| `FF_MQTT_INGEST_ENABLED` | `false` | Habilita receptor MQTT IoT |
| `FF_MQTT_MODULAR_RECEIVER` | `false` | Usa `core/receiver.py` en vez de `mqtt/simple_receiver.py` |
| `FF_MQTT_UNIVERSAL` | `false` | Habilita receptor MQTT universal (dominios no-IoT) |
| `DEVICE_AUTH_ENABLED` | `0` | Habilita autenticación por dispositivo (IoT) |
| `POSTGRES_URL` | — | URL PostgreSQL para ingesta universal (ej: `postgresql://user:pass@host:5432/db`) |
| `REDIS_URL` | `redis://localhost:6379/0` | URL Redis (dedup + DLQ) |
| `REDIS_ENABLED` | `true` | Habilita Redis |
| `BATCH_BUFFER_SIZE` | `100` | Tamaño buffer BatchInserter |
| `BATCH_FLUSH_INTERVAL` | `5.0` | Segundos entre flushes |
| `BATCH_MAX_SIZE` | `500` | Máximo lecturas por flush |
| `AI_EXPLAINER_URL` | `http://localhost:8003` | URL del servicio AI Explainer |

---

## Comunicación con otros servicios

| Servicio | Dirección | Detalle |
|---|---|---|
| **SQL Server** (`iot_database`) | Lee/Escribe (IoT) | `sensor_readings`, `sensor_readings_latest`, `alerts`, `alert_notifications`, `ml_events` |
| **PostgreSQL** | Lee/Escribe (Universal) | `data_points`, `stream_configs`, `value_constraints`, `api_keys`, `ingestion_audit_log` |
| **ML** (`iot_machine_learning`) | Import directo | `SensorProcessor`, `BatchEnterpriseContainer`, broker |
| **Redis** | Escribe | Deduplicación (SET NX) + DLQ (List) + stream `readings:validated` (XADD) |
| **AI Explainer** (`ai-explainer`) | HTTP POST | `/explain/anomaly` — solo desde `ai_explainer_runner.py` |
| **Backend** (`iot_monitor_backend`) | Indirecto | Consume `alerts`, `predictions`, `ml_events` vía BD |
| **Simulación** (`iot_simulation`) | HTTP POST | Envía a `/ingest/packets` |

---

## Ejecución

```bash
# Instalar dependencias
pip install -r requirements.txt

# Iniciar API
uvicorn ingest_api.main:app --reload --port 8001

# Job batch ML
python -m jobs.ml_batch_runner

# Job AI explainer
python -m jobs.ai_explainer_runner
```

---

## Qué NO hace este módulo

- No implementa backend de usuarios/roles (`iot_monitor_backend`).
- No aplica migraciones SQL (`iot_database`).
- No garantiza entrega en tiempo real end-to-end (broker in-memory o Redis, no distribuido multi-proceso).
- No ejecuta ML avanzado (Taylor, cognitivo) — eso es `iot_machine_learning`.
