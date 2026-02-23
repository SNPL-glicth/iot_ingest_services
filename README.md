# iot_ingest_services

Microservicio Python de **ingesta de lecturas IoT** y jobs batch auxiliares.

Recibe lecturas de sensores (HTTP o MQTT), las clasifica, persiste en SQL Server y publica al broker ML. Los jobs batch ejecutan predicciones baseline y enriquecen anomalías con explicaciones.

---

## Estructura del proyecto

```
iot_ingest_services/
├── common/                        # Configuración y conexión BD compartida
│   ├── config.py                  # Settings (DB_HOST, DB_PORT, DB_USER…)
│   └── db.py                      # get_engine() singleton SQLAlchemy
│
├── ingest_api/                    # FastAPI — punto de entrada HTTP
│   ├── main.py                    # Wiring: routers + startup/shutdown
│   ├── schemas.py                 # Pydantic models de entrada/salida
│   ├── batch_inserter.py          # BatchInserter async (buffer + flush)
│   ├── rate_limiter.py            # Rate limiting por IP y dispositivo
│   ├── device_auth.py             # Validación de device_uuid + sensor_uuid
│   ├── debug.py                   # Helpers de debug (solo dev)
│   │
│   ├── endpoints/                 # Rutas HTTP
│   │   ├── health.py              # GET /health
│   │   ├── packet_ingest.py       # POST /ingest/packets  ← recomendado
│   │   ├── single_ingest.py       # POST /ingest/readings  (legacy)
│   │   ├── batch_ingest.py        # POST /ingest/readings/bulk  (legacy)
│   │   ├── sensor_status.py       # GET /sensors/{id}/status
│   │   ├── diagnostics.py         # GET /diagnostics
│   │   └── resilience_health.py   # GET /resilience/health
│   │
│   ├── auth/                      # Autenticación
│   │   ├── api_key.py             # X-API-Key global (legacy/dev)
│   │   └── device_key.py          # X-Device-Key por dispositivo (recomendado)
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
│   ├── core/                      # Arquitectura modular (FF_MQTT_MODULAR_RECEIVER)
│   │   ├── receiver.py            # start/stop_modular_receiver
│   │   ├── domain/
│   │   │   ├── reading.py         # Reading dataclass
│   │   │   ├── contracts.py       # Contratos de dominio
│   │   │   └── broker_interface.py # IReadingBroker ABC
│   │   ├── adapters/
│   │   │   └── mqtt_adapter.py    # MQTTReadingAdapter
│   │   ├── pipeline/
│   │   │   ├── processor.py       # ReadingProcessor
│   │   │   └── sp_executor.py     # SPExecutor
│   │   ├── transport/
│   │   │   ├── mqtt_client.py     # MQTTClient
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
| `POST` | `/ingest/packets` | Device Key | **Recomendado** — paquete por device_uuid |
| `POST` | `/ingest/readings` | API Key | Legacy — lectura única por sensor_id |
| `POST` | `/ingest/readings/bulk` | API Key | Legacy — lote por sensor_id |

### Autenticación

| Header | Modo | Cuándo usar |
|---|---|---|
| `X-Device-Key` | Por dispositivo | `/ingest/packets` — producción |
| `X-API-Key` | Global | Endpoints legacy, desarrollo |

Controlado por `DEVICE_AUTH_ENABLED=1` en `.env`.

---

## Flujo de ingesta HTTP (`POST /ingest/packets`)

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

## Flujo MQTT (`FF_MQTT_INGEST_ENABLED=true`)

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

Dos implementaciones seleccionables por feature flag:
- `FF_MQTT_MODULAR_RECEIVER=false` → `mqtt/simple_receiver.py` (paho → SP directo)
- `FF_MQTT_MODULAR_RECEIVER=true` → `core/receiver.py` (arquitectura modular)

---

## Clasificación de lecturas

Cada lectura se clasifica **antes** de persistir en uno de tres flujos:

| Clase | Condición | Persiste en | Envía a ML |
|---|---|---|---|
| `ALERT` | Viola rango físico del sensor | `dbo.alerts` + `dbo.sensor_readings` | ❌ No |
| `WARNING` | Delta spike detectado | `dbo.ml_events` (DELTA_SPIKE) | ❌ No |
| `ML_PREDICTION` | Dato limpio | `dbo.sensor_readings_latest` | ✅ Sí |

Regla: **solo 1 alerta/advertencia activa por sensor** — se cierra la anterior antes de crear una nueva.

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
| `FF_MQTT_INGEST_ENABLED` | `false` | Habilita receptor MQTT |
| `FF_MQTT_MODULAR_RECEIVER` | `false` | Usa `core/receiver.py` en vez de `mqtt/simple_receiver.py` |
| `DEVICE_AUTH_ENABLED` | `0` | Habilita autenticación por dispositivo |
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
| **SQL Server** (`iot_database`) | Lee/Escribe | `sensor_readings`, `sensor_readings_latest`, `alerts`, `alert_notifications`, `ml_events` |
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
