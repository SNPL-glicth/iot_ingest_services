# iot_ingest_services
Microservicios Python (FastAPI) para:
- Ingesta de lecturas de sensores (llama al SP `sp_insert_reading_and_check_threshold`).
- Machine Learning (MVP) para generar predicciones y guardarlas en la tabla `predictions`.

## Estado actual (diciembre 2025)
- `ingest_api` (puerto sugerido: `8001`): recibe lecturas por `device_uuid` + `sensor_uuid` (batch) y hace 1 commit por paquete.
- `ml_service` (puerto sugerido: `8002`): expone `POST /ml/predict` (predicción puntual) sobre lecturas históricas.
- Paquete ML centralizado: `iot_ingest_services/ml` (baseline de media móvil + metadatos de modelo).
- Estas predicciones se consumen en Flutter en la sección **Predicciones (ML)**.
- **ML online** (`ml_stream_runner`):
  - Consume un stream abstracto de lecturas de alta frecuencia (`ReadingBroker` / `InMemoryReadingBroker`).
  - Mantiene buffers deslizantes por sensor (ventanas 1s/5s/10s) y calcula agregados (avg/min/max/trend).
  - A partir de esos agregados deriva `severity` (NORMAL/WARN/CRITICAL), explicación humana y `recommended_action`.
  - Solo cuando cambia el estado, inserta filas en `dbo.ml_events` y notificaciones en `dbo.alert_notifications`.

### Relación con Alertas / Advertencias (ML)
- Alertas operacionales (umbral) se generan en la BD vía SP/trigger y se exponen por Nest.
- Advertencias (ML) / eventos ML se generan en `ml_events` (online/batch) y se exponen vía Nest para dashboards.
- Notificaciones de ML (para la campanita en Flutter) se materializan en `alert_notifications`.

## Reutilizar credenciales de Nest
Por defecto, los servicios cargan el archivo `iot_monitor_backend/.env` (en la raíz del repo). Así no duplicas credenciales.

Si quieres usar otro archivo:
- `IOT_ENV_FILE=/ruta/al/.env`

Variables esperadas:
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
Opcional:
- `ODBC_DRIVER` (por ejemplo: `ODBC Driver 17 for SQL Server` o `ODBC Driver 18 for SQL Server`)
- `INGEST_API_KEY` (si está definida, la API exige el header `X-API-Key` en endpoints `/ingest/*`)

## Instalar dependencias
```bash
pip install -r iot_ingest_services/requirements.txt
```

## Preparar la BD para ingesta en lote (recomendado)
Aplica el parche SQL (no borra la BD) que crea los SP bulk y mejora el trigger para inserts en lote:

- Archivo: `iot_ingest_services/sql/patch_bulk_ingest.sql`

Si tu BD ya tenía el SP bulk anterior y quieres agregar trazabilidad (columna `sensor_readings.device_timestamp`), aplica:
- Archivo: `iot_ingest_services/sql/patch_device_timestamp.sql`

Ejemplo con `sqlcmd` (ajusta host/puerto y credenciales):
```bash
sqlcmd -S localhost,1434 -U sa -P "<TU_PASSWORD>" -d iot_monitoring_system -i iot_ingest_services/sql/patch_device_timestamp.sql
```

## Ejecutar ingest_api
```bash
uvicorn iot_ingest_services.ingest_api.main:app --reload --port 8001
```

Endpoints:
- `GET /health`
- `POST /ingest/packets` (recomendado: UUID + batch)
- `POST /ingest/readings` (legacy: `sensor_id`)
- `POST /ingest/readings/bulk` (legacy: `sensor_id`)

Ejemplo (recomendado, paquete por dispositivo):
```bash
curl -X POST http://localhost:8001/ingest/packets \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <TU_API_KEY>" \
  -d '{
    "device_uuid": "00000000-0000-0000-0000-000000000001",
    "readings": [
      {"sensor_uuid": "00000000-0000-0000-0000-000000000101", "value": 23.5},
      {"sensor_uuid": "00000000-0000-0000-0000-000000000102", "value": 48.1}
    ]
  }'
```

El servicio resuelve `sensor_uuid -> sensor_id` (validando que el sensor pertenezca al dispositivo) y hace 1 commit por paquete.
El cache TTL para el mapeo se controla con `SENSOR_MAP_TTL_SECONDS` (default: 300).

Si envías `ts` en el paquete, el backend lo guarda como `device_timestamp` en `sensor_readings` (si aplicaste el parche `patch_device_timestamp.sql`).

Ejemplo (legacy):
```bash
curl -X POST http://localhost:8001/ingest/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor_id": 1, "value": 23.5}'
```

## Ejecutar ml_service

### Linux / macOS (bash)
```bash
# opcional (dev): crea/activa un modelo baseline si no existe uno activo
export AUTO_CREATE_MODEL=1

python -m uvicorn iot_ingest_services.ml_service.main:app --reload --host 0.0.0.0 --port 8002
```

### Windows (PowerShell)
```powershell
# opcional (dev): crea/activa un modelo baseline si no existe uno activo
$env:AUTO_CREATE_MODEL = "1"

py -m uvicorn iot_ingest_services.ml_service.main:app --reload --host 0.0.0.0 --port 8002
```

Endpoints:
- `GET /health`
- `POST /ml/predict`

Ejemplo (predicción puntual):
```bash
curl -X POST http://localhost:8002/ml/predict \
  -H "Content-Type: application/json" \
  -d '{"sensor_id": 1, "horizon_minutes": 5, "window": 20}'
```

## Ejecutar ML Batch Runner (sklearn + IsolationForest)

Este runner ejecuta el pipeline ML sobre todos los sensores configurados, insertando filas en `dbo.predictions` y, si aplica, eventos en `dbo.ml_events`.

### Linux / macOS (bash)
```bash
# Una sola vez (útil para pruebas)
python -m iot_ingest_services.ml_service.runners.ml_batch_runner --once

# En bucle cada 60 segundos
python -m iot_ingest_services.ml_service.runners.ml_batch_runner --interval-seconds 60
```

### Windows (PowerShell)
```powershell
# Una sola vez (útil para pruebas)
py -m iot_ingest_services.ml_service.runners.ml_batch_runner --once

# En bucle cada 60 segundos
py -m iot_ingest_services.ml_service.runners.ml_batch_runner --interval-seconds 60
```

## Flujo de integración
- Sensores -> `ingest_api` -> SQL Server (SP + triggers) -> Nest (`/monitoring/*`) -> Flutter.
- ML (servicio HTTP puntual) -> `ml_service` -> SQL Server (`predictions`) -> Nest (`GET /monitoring/predictions`) -> Flutter.
- ML (batch/sklearn) -> `ml_batch_runner` -> `dbo.predictions` + `dbo.ml_events` -> Nest (`/monitoring/*`) -> Flutter.
- ML online (streaming interno) -> `ml_stream_runner` + `ReadingBroker` -> `dbo.ml_events` + `dbo.alert_notifications` -> Nest (`/notifications/*`, `/monitoring/*`, `/intelligence/*`) -> Flutter.
