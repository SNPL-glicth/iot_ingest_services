# ingest_api/mqtt

Receptor MQTT para ingesta directa desde dispositivos/simuladores.

Canal principal cuando `FF_MQTT_INGEST_ENABLED=true`.

## Estructura

```
mqtt/
├── simple_receiver.py     # SimpleReceiver — implementación por defecto
├── receiver.py            # MQTTReceiver — implementación modular
├── receiver_singleton.py  # Singleton del receptor activo
├── receiver_connections.py
├── receiver_stats.py      # ReceiverStats — contadores de mensajes
├── processor.py           # MQTTProcessor — procesa un mensaje MQTT
├── async_processor.py     # AsyncMQTTProcessor — versión async
├── message_handler.py     # handle_message() — punto de entrada por mensaje
├── validators.py          # Validación de payload MQTT (Pydantic)
├── connections.py         # Gestión de conexiones paho-mqtt
└── backpressure.py        # BackpressureController (cola + semáforo)
```

---

## Dos implementaciones

### `simple_receiver.py` — Por defecto (`FF_MQTT_MODULAR_RECEIVER=false`)

Flujo directo: paho callback → SP SQL Server.

```
paho _on_message()
  └── handle_message()
        ├── parse_json (orjson si disponible)
        ├── validate_mqtt_reading()   ← Pydantic
        ├── MessageDeduplicator       ← Redis SET NX
        └── processor.process()
              ├── _execute_sp()       ← sp_insert_reading_and_check_threshold
              └── _publish_to_ml()   ← Redis XADD readings:validated
```

Activar:
```bash
FF_MQTT_INGEST_ENABLED=true
FF_MQTT_MODULAR_RECEIVER=false   # default
```

### `receiver.py` — Modular (`FF_MQTT_MODULAR_RECEIVER=true`)

Arquitectura limpia con capas separadas (transport → domain → pipeline).
Usa `core/` internamente.

```
MQTTReceiver
  └── core/transport/mqtt_client.py
        └── core/adapters/mqtt_adapter.py
              └── core/pipeline/processor.py
                    ├── core/pipeline/sp_executor.py
                    └── core/redis/publisher.py
```

Activar:
```bash
FF_MQTT_INGEST_ENABLED=true
FF_MQTT_MODULAR_RECEIVER=true
```

---

## Topic MQTT

```
iot/sensors/{sensor_id}/readings
```

### Payload esperado

```json
{
  "sensor_id": 42,
  "value": 23.5,
  "timestamp": "2026-02-22T14:30:00Z",
  "device_uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## Backpressure

`BackpressureController` limita la cantidad de mensajes procesados en paralelo:

```python
controller = BackpressureController(
    max_queue_size=1000,    # MQTT_BACKPRESSURE_MAX_QUEUE
    max_concurrent=10,      # MQTT_BACKPRESSURE_MAX_CONCURRENT
)
```

Si la cola está llena, los mensajes nuevos se descartan con log de warning.

---

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `FF_MQTT_INGEST_ENABLED` | `false` | Habilita el receptor MQTT |
| `FF_MQTT_MODULAR_RECEIVER` | `false` | Usa `receiver.py` en vez de `simple_receiver.py` |
| `MQTT_BROKER_HOST` | `localhost` | Host del broker MQTT |
| `MQTT_BROKER_PORT` | `1883` | Puerto MQTT |
| `MQTT_CLIENT_ID` | `iot_ingest` | Client ID paho |
| `MQTT_USERNAME` | — | Usuario MQTT (opcional) |
| `MQTT_PASSWORD` | — | Contraseña MQTT (opcional) |
| `MQTT_BACKPRESSURE_MAX_QUEUE` | `1000` | Tamaño máximo de cola |
| `MQTT_BACKPRESSURE_MAX_CONCURRENT` | `10` | Procesamiento concurrente máximo |

---

## Health check

```bash
GET /mqtt/health   # Estado del receptor activo
GET /mqtt/stats    # Contadores: messages_received, processed, errors, duplicates
```

---

## Notas de rendimiento

- Timestamp parseado en `validators.py` (Pydantic) — evitar parseos adicionales en processor
- Deduplicación Redis: ~0.1ms por lectura
- SP SQL Server: 5–50ms por lectura (dominante)
- Redis XADD (publish ML): ~0.1ms
- Capacidad estimada: ~36,000 lecturas/hora con 10 sensores activos
