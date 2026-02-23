# jobs/

Jobs batch independientes del servicio de ingesta.

Se ejecutan manualmente o por cron externo — no están integrados al scheduler de `ingest_api`.

## Estructura

```
jobs/
├── ml_batch_runner.py         # Facade → jobs/batch/ (backward compat)
├── ai_explainer_runner.py     # Enriquece anomalías con explicaciones AI
└── batch/
    ├── runner.py              # run_once() — orquestador del ciclo batch
    ├── config.py              # RunnerConfig (env vars)
    ├── db_queries.py          # SQL helpers: list_active_sensors, load_readings, save_prediction
    ├── prediction.py          # compute_prediction() + check_threshold_events()
    ├── enterprise.py          # BatchEnterpriseContainer (bridge a iot_machine_learning)
    ├── threshold_events.py    # emit_threshold_event_if_needed()
    └── retry.py               # retry_with_backoff
```

---

## `ml_batch_runner.py` — Predicciones batch

Ciclo batch que genera predicciones ML para todos los sensores activos.

### Flujo

```
run_once()
  ├── db_queries.list_active_sensors()   → List[int]  (sensor_ids activos)
  └── Por cada sensor:
        ├── db_queries.load_recent_readings(sensor_id, window=50)
        │     → List[float] (últimas N lecturas)
        ├── enterprise.BatchEnterpriseContainer.predict(sensor_id, values)
        │     → PredictionResult (Taylor / Baseline según feature flags)
        ├── db_queries.save_prediction(sensor_id, result)
        │     → INSERT dbo.predictions
        └── threshold_events.emit_if_needed(sensor_id, result)
              → INSERT dbo.ml_events si predicción viola umbral del usuario
```

### Regla de umbral

Si la predicción cae dentro del rango WARNING configurado por el usuario → **no** genera evento ML.
Solo genera evento si la predicción viola el rango y no hay un evento activo reciente.

### Ejecución

```bash
python -m jobs.ml_batch_runner

# O directamente:
python -m jobs.batch.runner
```

### Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `ML_BATCH_WINDOW_SIZE` | `50` | Lecturas históricas por sensor |
| `ML_BATCH_HORIZON` | `1` | Pasos a predecir |
| `ML_ENGINE` | `baseline` | Motor: `baseline`, `taylor`, `cognitive` |
| `ML_BATCH_RETRY_MAX` | `3` | Reintentos por sensor en caso de error |
| `ML_BATCH_RETRY_DELAY` | `1.0` | Segundos entre reintentos |

---

## `ai_explainer_runner.py` — Explicaciones AI

Enriquece predicciones anómalas con explicaciones generadas por el servicio `ai-explainer`.

### Flujo

```
ai_explainer_runner.main()
  ├── SELECT FROM dbo.predictions
  │     WHERE is_anomaly = 1
  │       AND anomaly_score >= AI_EXPLAINER_THRESHOLD
  │       AND explanation IS NULL
  │     LIMIT AI_EXPLAINER_BATCH_SIZE
  │
  └── Por cada predicción:
        ├── POST {AI_EXPLAINER_URL}/explain/anomaly
        │     Body: { sensor_id, value, anomaly_score, timestamp, context }
        ├── Si respuesta OK:
        │     UPDATE dbo.predictions SET explanation = {json}
        └── Si error (timeout, 5xx, caído):
              log warning + continúa con la siguiente
```

### Comportamiento ante fallos

- Si `ai-explainer` está caído: omite esa predicción, continúa con las demás.
- No bloquea ni reintenta indefinidamente — la predicción queda con `explanation=NULL` hasta el próximo ciclo.
- Idempotente: solo procesa predicciones con `explanation IS NULL`.

### Ejecución

```bash
python -m jobs.ai_explainer_runner
```

### Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `AI_EXPLAINER_URL` | `http://localhost:8003` | URL del servicio AI Explainer |
| `AI_EXPLAINER_THRESHOLD` | `0.7` | Score mínimo para solicitar explicación |
| `AI_EXPLAINER_BATCH_SIZE` | `50` | Predicciones por ciclo |
| `AI_EXPLAINER_TIMEOUT` | `10` | Timeout HTTP en segundos |

---

## `batch/enterprise.py` — Bridge a iot_machine_learning

`BatchEnterpriseContainer` conecta el runner batch con los motores ML de `iot_machine_learning`:

```python
container = BatchEnterpriseContainer.from_env()
result = container.predict(sensor_id=42, values=[20.1, 20.3, 20.5])
# result.predicted_value, result.engine_name, result.confidence
```

Selecciona el motor según `ML_ENGINE`:
- `baseline` → media móvil (sin dependencias externas)
- `taylor` → `TaylorPredictionEngine` (requiere datos de entrenamiento)
- `cognitive` → `MetaCognitiveOrchestrator` (requiere modelos entrenados)

---

## Notas operacionales

- Los jobs **no comparten estado** con `ingest_api` — cada ejecución es independiente.
- La conexión BD se obtiene de `common/db.py` (`get_engine()` singleton).
- `list_active_sensors()` retorna lista (no generador) para evitar el error `'This Connection is closed'` con conexiones lazy.
- Ambos jobs son seguros para ejecutar en paralelo (no hay locks compartidos).
