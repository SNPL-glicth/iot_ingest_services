# Transportes Universales

Plugins de transporte para ingesta multi-dominio. Cada transporte implementa la interfaz `IngestTransport` y es agnóstico al dominio.

---

## Arquitectura

```
IngestTransport (ABC)
  ├── start() → bool
  ├── stop() → None
  ├── parse_message(raw) → Iterator[DataPoint]
  └── properties: name, stats
```

Todos los transportes:
- Parsean mensajes a `DataPoint` universal
- Rechazan `domain='iot'` (debe usar pipeline IoT legacy)
- Son stateless (no conocen el dominio específico)
- Pueden ejecutarse en paralelo sin interferencia

---

## Transportes Disponibles

### 1. HTTP Transport (`http/`)

**Endpoint:** `POST /ingest/data`

**Autenticación:** X-API-Key con roles (PostgreSQL)

**Payload:**
```json
{
  "domain": "infrastructure",
  "source_id": "web-01",
  "data_points": [
    {
      "stream_id": "cpu",
      "value": 45.2,
      "timestamp": "2026-02-25T22:00:00Z",
      "metadata": {"unit": "percent"}
    }
  ]
}
```

**Características:**
- Batch ingestion (múltiples DataPoints por request)
- Validación de autorización por source_id y domain
- Rechaza domain='iot' con HTTP 400
- Retorna clasificaciones por DataPoint

**Archivos:**
- `transport.py` - HTTPTransport parser
- `schemas.py` - DataPacketIn, DataPointIn, DataIngestResult
- `endpoints.py` - FastAPI routes

---

### 2. MQTT Transport (`mqtt/`)

**Topics:** `{domain}/{source_id}/{stream_id}/data`

**Feature Flag:** `FF_MQTT_UNIVERSAL=true`

**Payload (JSON):**
```json
{
  "value": 45.2,
  "timestamp": "2026-02-25T22:00:00Z",
  "metadata": {"unit": "percent"}
}
```

**Características:**
- Parsea topic para extraer domain, source_id, stream_id
- Rechaza topics `iot/*` automáticamente
- Deduplicación con TTL 5 minutos
- Pipeline completo: parse → classify → persist → audit
- Cliente MQTT separado del IoT (sin interferencia)

**Archivos:**
- `transport.py` - MQTTTransport parser
- `receiver.py` - UniversalMQTTReceiver (singleton)

**Configuración:**
```bash
FF_MQTT_UNIVERSAL=true
MQTT_BROKER_HOST=localhost
MQTT_BROKER_PORT=1883
```

---

### 3. WebSocket Transport (`websocket/`)

**Endpoint:** `ws://host:port/ingest/stream`

**Autenticación:** Token en handshake

**Características:**
- Streaming en tiempo real
- Sesiones persistentes por cliente
- Control de backpressure con queue
- Clasificación y persistencia por mensaje

**Archivos:**
- `handler.py` - WebSocket connection handler

---

### 4. CSV Transport (`csv/`)

**Endpoint:** `POST /ingest/csv`

**Autenticación:** X-API-Key con roles

**Características:**
- Procesamiento chunked (no carga todo en memoria)
- Soporta archivos grandes
- Validación de columnas requeridas: domain, source_id, stream_id, value, timestamp
- Procesamiento asíncrono con background tasks

**Archivos:**
- `processor.py` - CSVProcessor (pandas chunking)
- `endpoints.py` - FastAPI upload endpoint

**Formato CSV:**
```csv
domain,source_id,stream_id,value,timestamp
infrastructure,web-01,cpu,45.2,2026-02-25T22:00:00Z
infrastructure,web-01,memory,78.5,2026-02-25T22:00:00Z
```

---

## Flujo Común de Transportes

```
Transport.parse_message(raw)
  ↓
DataPoint(series_id, value, timestamp, domain, ...)
  ↓
UniversalClassifier.classify(dp, config)
  ↓
ClassificationResult(classification, reason, should_persist)
  ↓
DomainStorageRouter.insert(dp)
  ↓
PostgreSQL (data_points table)
  +
AuditLogger (ingestion_audit_log)
```

---

## Agregar Nuevo Transporte

1. **Crear clase que implemente `IngestTransport`:**

```python
from ..base import IngestTransport
from ...core.domain.data_point import DataPoint

class MyTransport(IngestTransport):
    @property
    def name(self) -> str:
        return "my_transport"
    
    def start(self) -> bool:
        # Inicializar recursos
        return True
    
    def stop(self) -> None:
        # Cleanup
        pass
    
    def parse_message(self, raw_message: Any) -> Iterator[DataPoint]:
        # Parsear a DataPoint
        # Rechazar domain='iot'
        yield DataPoint(...)
```

2. **Crear endpoint (si aplica):**

```python
from fastapi import APIRouter
from ...auth.api_key_validator import verify_api_key

router = APIRouter()

@router.post("/ingest/my_transport")
async def ingest_my_transport(
    payload: MyPayload,
    api_key_info: ApiKeyInfo = Depends(verify_api_key),
):
    # Validar autorización
    # Parsear con MyTransport
    # Clasificar y persistir
    pass
```

3. **Registrar en `main.py`:**

```python
from .transports.my_transport import router as my_transport_router

app.include_router(my_transport_router)
```

---

## Seguridad (ISO 27001)

Todos los transportes universales implementan:

- **A.9.2.3 - Control de acceso:** Roles ADMIN, SOURCE_WRITER, READ_ONLY
- **A.9.4.2 - Log-on seguro:** API keys hasheados SHA-256
- **A.12.4 - Audit trail:** Todas las ingestas registradas con metadata
- **Validación de ownership:** source_id debe coincidir con api_key
- **Validación de domain:** domain debe estar en allowed_domains

---

## Métricas

Cada transporte expone:
- `messages_processed` - Total procesados exitosamente
- `messages_rejected` - Total rechazados (validación/clasificación)
- `errors` - Total errores de parsing

Accesibles vía `transport.stats` property.

---

## Testing

```bash
# HTTP
curl -X POST http://localhost:8001/ingest/data \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_key_here" \
  -d '{
    "domain": "infrastructure",
    "source_id": "web-01",
    "data_points": [{"stream_id": "cpu", "value": 45.2}]
  }'

# MQTT (con mosquitto_pub)
mosquitto_pub -h localhost -p 1883 \
  -t "infrastructure/web-01/cpu/data" \
  -m '{"value": 45.2, "timestamp": "2026-02-25T22:00:00Z"}'

# CSV
curl -X POST http://localhost:8001/ingest/csv \
  -H "X-API-Key: your_key_here" \
  -F "file=@data.csv" \
  -F "domain=infrastructure"
```

---

## Notas Importantes

- **IoT NO usa estos transportes:** IoT tiene su propio pipeline (`/ingest/packets`)
- **Thread-safe:** UniversalClassifier usa locks para concurrencia
- **Deduplicación:** Solo MQTT universal tiene dedup (5 min TTL)
- **Feature Flags:** MQTT universal requiere `FF_MQTT_UNIVERSAL=true`
- **PostgreSQL requerido:** Todos persisten en PostgreSQL (no SQL Server)
