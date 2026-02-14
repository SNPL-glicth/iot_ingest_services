# AUDITORÍA: Ingesta + Adaptadores ML — Performance bajo Carga

**Fecha:** 2026-02-13  
**Alcance:** Flujo MQTT→Persistencia, Adaptadores ML, Stream Consumer  
**Objetivo:** Evaluar capacidad para 10 sensores (~36,000 lecturas/hora)  
**Método:** Análisis estático de código + métricas temporales insertadas

---

## 1. FLUJO COMPLETO MQTT → PERSISTENCIA

### 1.1 Diagrama de flujo por lectura

```
paho callback (_on_message)           ← thread del network loop de paho
  └─ handle_message()                 ← síncrono en el mismo thread
       ├─ parse_json()                ← orjson si disponible (~0.01ms)
       ├─ validate_mqtt_reading()     ← Pydantic BaseModel (~0.1-0.5ms)
       │    ├─ @validator timestamp   ← fromisoformat + time.time() [PARSE 1]
       │    └─ @root_validator        ← fromisoformat + .timestamp() [PARSE 2]
       ├─ get_deduplicator()          ← singleton, Redis SET NX (~0.1ms)
       └─ processor.process()
            ├─ _parse_timestamp()     ← fromisoformat [PARSE 3]
            ├─ _execute_sp()          ← engine.begin() + SP (~5-50ms)
            └─ _publish_to_ml()       ← Redis XADD (~0.1ms)
```

### 1.2 Hallazgos

#### H-ING-1: Timestamp parseado 3 veces por lectura [MEDIA]
- **Archivo:** `validators.py:67-80` (validator), `validators.py:82-94` (root_validator), `processor.py:64-69` (_parse_timestamp)
- **Impacto:** `datetime.fromisoformat()` se ejecuta 3 veces por mensaje. El root_validator cachea el resultado, pero processor.py lo ignora y re-parsea.
- **Costo:** ~0.15ms × 3 = ~0.45ms por lectura. A 36K lecturas = ~16s desperdiciados/hora.
- **Corrección:** Usar `payload.timestamp_datetime` en processor.py en vez de re-parsear.

#### H-ING-2: Bloqueo síncrono total en thread de paho [ALTA]
- **Archivo:** `receiver.py:147-150`, `message_handler.py:38-80`, `processor.py:28-62`
- **Impacto:** Todo el pipeline (validación + dedup + SP + Redis) se ejecuta en el callback `_on_message` del network loop de paho-mqtt. Mientras se ejecuta el SP (~5-50ms), **no se procesan otros mensajes MQTT**.
- **Cálculo bajo carga:** 10 sensores × 1 lectura/s = 10 msg/s. Si SP toma 20ms promedio, se consumen 200ms/s del thread. **Capacidad máxima teórica: ~50 msg/s** antes de que el backlog crezca.
- **Riesgo a 36K/hora (10/s):** BAJO si SP < 30ms. MEDIO si SP > 50ms (latencia acumulada).

#### H-ING-3: `engine.begin()` crea transacción nueva por cada lectura [ALTA]
- **Archivo:** `processor.py:73-86`
- **Impacto:** Cada lectura abre una conexión del pool, ejecuta el SP, y cierra la transacción. No hay batching real en el path MQTT→SP.
- **Costo:** Overhead de transacción SQL Server (~2-5ms) × 36K = 72-180s/hora solo en overhead transaccional.
- **Nota:** El `BatchInserter` existe (`batch_inserter.py`) pero **NO se usa en el path MQTT→SP**. El MQTT path usa el SP directamente (que hace INSERT + threshold check + alerts en una sola llamada).

#### H-ING-4: Dos clases RedisConnection con semántica diferente [BAJA]
- **Archivos:** `receiver_connections.py:46-99` (stream=`readings:raw`, maxlen=10000) vs `connections.py:69-129` (stream=`readings:validated`, maxlen=10000)
- **Impacto:** `processor.py` importa de `connections.py` (stream `readings:validated`), pero `receiver.py` instancia `receiver_connections.RedisConnection` (stream `readings:raw`). El procesador recibe la instancia de `receiver.py`, así que publica a `readings:raw` correctamente. Pero la confusión de imports es un riesgo de regresión.

#### H-ING-5: Pydantic v1 validators en hot path [BAJA]
- **Archivo:** `validators.py:22-169`
- **Impacto:** Usa `@validator` y `@root_validator` (Pydantic v1 API). Cada lectura crea un `MQTTReadingPayload` con validación completa. Pydantic v1 es ~3-5x más lento que v2 para validación.
- **Costo estimado:** ~0.3-0.5ms por lectura. A 36K = ~10-18s/hora.

#### H-ING-6: `import time` dentro de función hot path [BAJA]
- **Archivo:** `message_handler.py:42`
- **Impacto:** `import time` se ejecuta en cada llamada a `handle_message()`. Python cachea imports, pero el lookup en `sys.modules` tiene costo (~0.001ms). Trivial pero innecesario.

#### H-ING-7: Deduplicador genera msg_id con string formatting [BAJA]
- **Archivo:** `deduplication.py:119-120`
- **Impacto:** `f"{sensor_id}:{timestamp:.6f}:{value:.6f}"` genera un string de ~30 chars por lectura. Luego Redis SET NX con TTL. Costo total ~0.1ms. Aceptable.

#### H-ING-8: Redis Stream maxlen=10000 sin approximate [MEDIA]
- **Archivo:** `receiver_connections.py:95` — `xadd(maxlen=10000)` sin `approximate=True`
- **Impacto:** Sin `approximate`, Redis hace XTRIM exacto en cada XADD, que es O(N) en el peor caso. Con `approximate`, Redis solo trima cuando supera ~maxlen*1.5.
- **Nota:** `connections.py:123` SÍ usa `approximate=True`. Inconsistencia.

### 1.3 Complejidad temporal por lectura

| Operación | Complejidad | Tiempo estimado |
|-----------|-------------|-----------------|
| JSON parse (orjson) | O(n) payload | ~0.01ms |
| Pydantic validation | O(1) | ~0.3-0.5ms |
| Timestamp parse (×3) | O(1) | ~0.45ms |
| Dedup Redis SET NX | O(1) | ~0.1ms |
| SP execution | O(1) SQL | ~5-50ms |
| Redis XADD | O(1) amortized | ~0.1ms |
| **Total por lectura** | | **~6-51ms** |

### 1.4 Uso de memoria por lectura

| Objeto | Tamaño estimado |
|--------|-----------------|
| `MQTTReadingPayload` (Pydantic) | ~800-1200 bytes |
| `dict` data para Redis | ~200 bytes |
| SP params dict | ~100 bytes |
| **Total por lectura en vuelo** | **~1.1-1.5 KB** |

Con 10 msg/s, máximo ~15 KB en vuelo simultáneo. **Sin riesgo de memoria.**

---

## 2. ADAPTADORES ML

### 2.1 Flujo batch runner (producción)

```
run_once()
  └─ engine.begin()                    ← UNA transacción para TODOS los sensores
       └─ for sensor_id in active_sensors:
            ├─ ensure_watermark()      ← INSERT IF NOT EXISTS
            ├─ get_last_reading_id()   ← SELECT watermark
            ├─ get_sensor_max_reading_id() ← SELECT MAX(id) [con retry deadlock]
            ├─ load_recent_values()    ← SELECT TOP(window) ORDER BY ts DESC
            ├─ predict_moving_average() o predict_enterprise()
            ├─ get_or_create_active_model_id() ← SELECT/INSERT ml_models
            ├─ get_device_id_for_sensor() ← SELECT sensors
            ├─ insert_prediction()     ← INSERT predictions
            ├─ eval_pred_threshold()   ← SELECT thresholds + INSERT ml_events
            └─ update_watermark()      ← UPDATE watermarks
```

### 2.2 Hallazgos

#### H-ML-1: 6-8 queries SQL por sensor por ciclo batch [ALTA]
- **Archivos:** `db_queries.py`, `prediction.py`, `threshold_events.py`
- **Impacto:** Para 10 sensores: 60-80 queries por ciclo. Cada query es un roundtrip a SQL Server (~1-5ms). Total: ~60-400ms por ciclo.
- **Riesgo a 36K:** El batch runner corre cada 60s. Con 10 sensores y ~40ms/sensor, un ciclo toma ~400ms. **Holgura amplia.**
- **Mejora potencial:** Combinar `ensure_watermark` + `get_last_reading_id` en una sola query. Combinar `get_or_create_active_model_id` + `get_device_id_for_sensor`.

#### H-ML-2: `load_recent_values()` descarta timestamps [MEDIA]
- **Archivo:** `db_queries.py:92-104`
- **Impacto:** Carga `SELECT TOP(window) [value]` — solo valores, sin timestamps. El enterprise path luego llama `storage.load_sensor_window()` que carga OTRA VEZ con timestamps. **Doble carga de datos para enterprise sensors.**
- **Costo:** Una query extra por sensor enterprise (~2-5ms).

#### H-ML-3: Enterprise adapter recrea `BatchEnterpriseContainer` por stream consumer [MEDIA]
- **Archivo:** `stream_consumer.py:39-51`
- **Impacto:** El stream consumer crea un `BatchEnterpriseContainer` en `_init_use_case()`. Este container crea una conexión SQLAlchemy singleton. Si el consumer se reinicia, la conexión se pierde sin `close()`.
- **Riesgo:** Connection leak si el consumer crashea y se reinicia repetidamente.

#### H-ML-4: Enterprise prediction hace doble carga de datos [ALTA]
- **Archivos:** `runner.py:71` (load_recent_values), `enterprise_prediction.py:113` → `use_case.execute()` → `storage.load_sensor_window()`
- **Flujo:** El batch runner carga `values = load_recent_values(conn, sensor_id, window)` para decidir si hay datos suficientes. Luego, si usa enterprise, el adapter llama `use_case.execute(sensor_id, window_size)` que internamente llama `storage.load_sensor_window(sensor_id, limit=window_size)` — **cargando los mismos datos otra vez desde SQL Server.**
- **Costo:** Query duplicada × sensores enterprise. ~2-5ms × N sensores.

#### H-ML-5: `_safe_float` importa `math` en cada llamada [BAJA]
- **Archivo:** `sqlserver_storage.py:29-38`
- **Impacto:** `import math` dentro de `_safe_float()` y `_is_valid_sensor_value()`. Se llama por cada reading en la ventana. Python cachea el import, pero el lookup tiene costo.
- **Costo:** ~0.001ms × 500 readings = ~0.5ms. Trivial.

#### H-ML-6: `BaselineConfig` se recrea por sensor [BAJA]
- **Archivo:** `runner.py:80,84,93`
- **Impacto:** `BaselineConfig(window=cfg.window)` es un frozen dataclass creado por cada sensor. Costo: ~0.001ms. Trivial, pero podría ser una constante fuera del loop.

#### H-ML-7: `get_engine()` se llama en cada `run_once()` [MEDIA]
- **Archivo:** `runner.py:40`, `common/db.py:24-45`
- **Impacto:** `get_engine()` llama `create_engine()` + `SELECT 1` test query **en cada ciclo del batch runner** (cada 60s). Debería ser un singleton.
- **Costo:** ~10-50ms por ciclo desperdiciados en crear engine + test connection.

#### H-ML-8: Stream consumer no usa los readings que acumula [MEDIA]
- **Archivo:** `stream_consumer.py:128-139`
- **Impacto:** El `SlidingWindowStore` acumula readings del stream, pero `_predict()` llama `adapter.predict(sensor_id, window_size)` que internamente carga datos de SQL Server via `storage.load_sensor_window()`. **Los readings acumulados en memoria se ignoran.**
- **Consecuencia:** La sliding window es inútil actualmente — solo sirve como trigger counter. Los datos reales vienen de SQL.

---

## 3. STREAM CONSUMER

### 3.1 Hallazgos específicos

#### H-SC-1: `xack` por mensaje individual [MEDIA]
- **Archivo:** `stream_consumer.py:107,110`
- **Impacto:** Cada mensaje se ACK individualmente. Redis soporta batch ACK: `xack(stream, group, id1, id2, ...)`. Con 50 msgs/batch, son 50 roundtrips vs 1.
- **Costo:** ~0.05ms × 50 = ~2.5ms desperdiciados por batch.

#### H-SC-2: `_parse_reading` define función `d()` en cada llamada [BAJA]
- **Archivo:** `stream_consumer.py:117`
- **Impacto:** `def d(v)` se define como closure en cada invocación de `_parse_reading`. Costo: ~0.001ms. Trivial.

---

## 4. RESUMEN DE RIESGOS BAJO 36,000 LECTURAS/HORA

### 4.1 Escenario: 10 sensores, 1 lectura/segundo cada uno

| Métrica | Valor |
|---------|-------|
| Lecturas/hora | 36,000 |
| Lecturas/segundo | 10 |
| Tiempo disponible por lectura | 100ms |
| Tiempo estimado por lectura (SP path) | 6-51ms |
| **Utilización del thread paho** | **6-51%** |

### 4.2 Matriz de riesgo

| ID | Hallazgo | Severidad | Riesgo @36K | Prioridad |
|----|----------|-----------|-------------|-----------|
| H-ING-2 | Bloqueo síncrono en paho thread | ALTA | MEDIO | **ALTA** |
| H-ING-3 | Transacción nueva por lectura (SP path) | ALTA | BAJO | MEDIA |
| H-ML-4 | Doble carga de datos enterprise | ALTA | MEDIO | **ALTA** |
| H-ML-8 | Sliding window no se usa realmente | MEDIA | BAJO | **ALTA** (deuda técnica) |
| H-ML-7 | get_engine() recrea engine cada ciclo | MEDIA | BAJO | MEDIA |
| H-ML-1 | 6-8 queries por sensor batch | ALTA | BAJO | MEDIA |
| H-ML-2 | load_recent_values descarta timestamps | MEDIA | BAJO | MEDIA |
| H-ING-1 | Timestamp parseado 3 veces | MEDIA | BAJO | MEDIA |
| H-ING-8 | Redis XADD sin approximate | MEDIA | BAJO | MEDIA |
| H-ML-3 | Connection leak en stream consumer | MEDIA | BAJO | MEDIA |
| H-SC-1 | xack individual vs batch | MEDIA | BAJO | BAJA |
| H-ING-4 | Dos RedisConnection clases | BAJA | BAJO | BAJA |
| H-ING-5 | Pydantic v1 en hot path | BAJA | BAJO | BAJA |
| H-ING-6 | import time en función | BAJA | BAJO | BAJA |
| H-ML-5 | import math en _safe_float | BAJA | BAJO | BAJA |
| H-ML-6 | BaselineConfig recreada | BAJA | BAJO | BAJA |

### 4.3 Evaluación global

| Aspecto | Estado | Notas |
|---------|--------|-------|
| **¿Soporta 10 sensores?** | ✅ SÍ | Holgura amplia en todos los paths |
| **¿Soporta 50 sensores?** | ⚠️ PROBABLE | H-ING-2 se vuelve crítico (~50% thread) |
| **¿Soporta 100 sensores?** | ❌ NO | Thread paho saturado, necesita async/workers |
| **Memoria** | ✅ OK | ~15KB en vuelo, sin acumulación |
| **Latencia P99** | ⚠️ | SP puede tomar 50ms+ bajo contención |
| **Batching real** | ❌ NO | MQTT path es 1-a-1, no hay pipeline batch |

---

## 5. PUNTOS DE MEJORA PRIORIZADOS

### Prioridad ALTA (resolver antes de escalar a >20 sensores)

1. **Desacoplar paho callback del SP** — Mover `processor.process()` a un thread pool o queue interna para no bloquear el network loop de paho.
2. **Eliminar doble carga enterprise** — El batch runner ya carga `values`; pasar esos datos al enterprise adapter en vez de que recargue de SQL.
3. **Definir rol del SlidingWindowStore** — O se usa para alimentar ML directamente (sin SQL), o se elimina y se usa solo como trigger counter explícito.

### Prioridad MEDIA (optimización incremental)

4. **Singleton para `get_engine()`** — Cachear el engine, no recrear cada ciclo.
5. **Eliminar triple parse de timestamp** — Usar `payload.timestamp_datetime` en processor.
6. **Consolidar queries batch** — Combinar watermark + last_reading_id en una query.
7. **Redis XADD approximate** — Agregar `approximate=True` en `receiver_connections.py`.
8. **Batch xack** — Acumular message IDs y ACK en una sola llamada.

### Prioridad BAJA (cleanup)

9. Unificar las dos clases `RedisConnection`.
10. Migrar a Pydantic v2 validators.
11. Mover `import time` fuera de `handle_message`.
12. Mover `import math` fuera de `_safe_float`.

---

## 6. MÉTRICAS TEMPORALES INSERTADAS

Se insertaron métricas `[AUDIT_METRICS]` en 3 archivos para medición en runtime:

| Archivo | Métrica | Tag |
|---------|---------|-----|
| `processor.py` | SP time, Redis time, payload size | `[AUDIT_METRICS] process` |
| `runner.py` | Cycle time, per-sensor time, max sensor | `[AUDIT_METRICS] batch` |
| `stream_consumer.py` | Batch time, msg count, window sizes | `[AUDIT_METRICS] stream` |
| `stream_consumer.py` | Prediction time per sensor | `[AUDIT_METRICS] predict` |

**Para buscar:** `grep -r "AUDIT_METRICS" iot_ingest_services/ iot_machine_learning/`  
**Para eliminar:** Buscar `[AUDIT_METRICS]` y eliminar los bloques marcados como "Temporary — remove after audit".

---

## 7. CONCLUSIÓN

El sistema **soporta 10 sensores (36K lecturas/hora) con holgura**. El cuello de botella principal es el bloqueo síncrono del thread de paho-mqtt durante la ejecución del SP. A 10 msg/s con SP de 20ms promedio, se usa ~20% del thread — aceptable.

Los riesgos reales aparecen al escalar a **>30 sensores**, donde el thread de paho se satura y la doble carga de datos enterprise se vuelve costosa.

La deuda técnica más importante es que el `SlidingWindowStore` del stream consumer acumula datos que luego se ignoran — el enterprise adapter recarga todo de SQL Server. Esto debe resolverse antes de que el stream consumer tenga un rol real en producción.
