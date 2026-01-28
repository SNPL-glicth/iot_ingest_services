# iot_ingest_services
 
 ## Qué hace esta parte del sistema
 
 Este proyecto contiene microservicios y jobs Python orientados a **ingesta** de lecturas de sensores y a tareas batch auxiliares.
 
 En el estado actual, aquí viven:
 
 - `ingest_api` (FastAPI): recibe lecturas (idealmente por `device_uuid` + `sensor_uuid`) y las persiste en SQL Server.
 - `jobs/`:
   - `ml_batch_runner.py`: job baseline (media móvil) que inserta predicciones en `dbo.predictions`.
   - `ai_explainer_runner.py`: job que enriquece predicciones anómalas con explicaciones vía `ai-explainer`, escribiéndolas en `predictions.explanation`.
 
 **Importante:** la lógica de ML “principal” está en `iot_machine_learning/`. Este repo de ingesta reutiliza ese código (por import) para baseline, broker y utilidades.
 
 ## Qué problema resuelve
 
 - Proveer un punto único de entrada (HTTP) para lecturas, con:
   - Rate limiting.
   - Validación mínima.
   - Mapeo `device_uuid + sensor_uuid -> sensor_id` con cache.
 - Persistir lecturas en SQL Server, disparando las reglas de umbrales/alertas que existen en BD.
 - Publicar lecturas “limpias” a un broker (en memoria en el estado actual) para que el ML online pueda suscribirse.
 - Ejecutar jobs batch que complementan el pipeline (baseline y explicaciones).
 
 ## Cómo funciona internamente (flujo real)
 
 ### `ingest_api` (FastAPI)
 
 - Entry point: `iot_ingest_services/ingest_api/main.py`.
 - Endpoints principales:
   - `GET /health`
   - `POST /ingest/packets` (recomendado)
   - `POST /ingest/readings` (legacy por `sensor_id`)
   - `POST /ingest/readings/bulk` (legacy por `sensor_id`)
 - Flujo real de `POST /ingest/packets`:
   - Rate limiting por IP y/o dispositivo.
   - Autenticación:
     - Preferida: `X-Device-Key` (por dispositivo) cuando está habilitada.
     - Alternativa: `X-API-Key` (global) para endpoints legacy y/o modo dev.
   - Resuelve `sensor_uuid` a `sensor_id` validando pertenencia del sensor al dispositivo.
   - Clasifica y enruta cada lectura a través de `ReadingRouter` (arquitectura de “pipelines”).
   - Inserta en BD y hace `commit` por paquete.
   - Publica lecturas al `ReadingBroker` (implementación actual: `InMemoryReadingBroker` envuelto en throttling).
 
 ### Jobs batch (`iot_ingest_services/jobs`)
 
 - `ml_batch_runner.py`:
   - Lee lecturas recientes de `dbo.sensor_readings`.
   - Calcula predicción baseline (media móvil) usando `iot_machine_learning.ml.baseline`.
   - Inserta filas en `dbo.predictions`.
   - Puede crear eventos en `dbo.ml_events` cuando la predicción viola umbrales, respetando la regla: si cae dentro del rango WARNING del usuario, no genera evento.
 - `ai_explainer_runner.py`:
   - No ejecuta ML.
   - Selecciona predicciones con `is_anomaly=1`, `anomaly_score >= threshold` y `explanation` vacío.
   - Llama a `ai-explainer` (`AI_EXPLAINER_URL`, default `http://localhost:8003`) y guarda el JSON en `dbo.predictions.explanation`.
 
 ## Cómo se comunica con las otras partes
 
 - **SQL Server (`iot_database`)**:
   - Inserta/lee `sensor_readings`.
   - Escribe `predictions`, `ml_models` (si aplica), `ml_events` y `alert_notifications` (según pipeline).
 - **ML (`iot_machine_learning`)**:
   - Se importa directamente para baseline/runner/broker.
 - **Backend (`iot_monitor_backend`)**:
   - Consume `readings/latest`, `alerts`, `predictions`, `ml_events` y notificaciones desde la BD.
 - **Dashboard (`iot_monito_dashboard`)**:
   - Consume al backend; no consume `ingest_api` directamente.
 - **Simulación (`iot_simulation`)**:
   - Envía lecturas a `POST /ingest/packets` para simular hardware.
 
 ## Ventajas del enfoque actual
 
 - Separación clara: ingesta no intenta “hacer UI” ni gestionar usuarios.
 - Persistencia + reglas de umbral cerca del dato (BD) y pipeline ML acoplado por tablas.
 - Broker desacoplado por interfaz (`ReadingBroker`), aunque la implementación actual sea en memoria.
 
 ## Desventajas o limitaciones actuales
 
 - El broker in-memory no permite distribuir consumo online entre procesos.
 - Hay múltiples modos de autenticación (device-key y api-key) que aumentan complejidad operativa.
 - Los jobs batch no están integrados a un scheduler en este repo (se ejecutan manualmente o por cron externo).
 
 ## Decisiones técnicas tomadas y por qué
 
 - **FastAPI** como gateway HTTP para lecturas: simple y rápido de integrar.
 - **Cache de sensor mapping** (`device_uuid/sensor_uuid -> sensor_id`): evita consultas por cada reading.
 - **Jobs batch separados**: el pipeline de ingesta no depende de servicios externos como Ollama.
 
 ## Qué NO hace esta parte
 
 - No implementa el backend de usuarios/roles (eso vive en `iot_monitor_backend`).
 - No aplica migraciones SQL.
 - No garantiza entrega de eventos “en tiempo real” (la UI se actualiza por polling vía backend).
 
 ## Preguntas tipo debate o entrevista
 
 ### ¿Por qué no escribir directamente a un bus/cola en lugar de BD?
 
 En el estado actual no hay un broker distribuido implementado. El contrato real entre servicios se basa en **tablas SQL** (`sensor_readings`, `predictions`, `ml_events`, `alert_notifications`).
 
 ### ¿Qué pasa si `ai-explainer` está caído?
 
 No bloquea la ingesta: `ai_explainer_runner.py` es un job separado y maneja errores por predicción, simplemente omite y continúa.



ingest_api/
├── main.py                 # 65 líneas (solo wiring)
│
├── endpoints/              # Capa HTTP
│   ├── health.py
│   ├── sensor_status.py
│   ├── single_ingest.py
│   ├── batch_ingest.py
│   └── packet_ingest.py
│
├── auth/                   # Autenticación
│   ├── api_key.py
│   └── device_key.py
│
├── broker/                 # Publicación a ML
│   ├── throttled.py
│   └── factory.py
│
├── queries/                # Consultas BD
│   └── sensor_status.py
│
├── classification/         # ← NUEVO (Fase 4)
│   ├── classifier.py       # ReadingClassifier
│   └── sensor_state.py     # SensorStateManager
│
├── ingest/                 # Core de ingesta
│   ├── router.py
│   ├── sensor_resolver.py
│   ├── handlers/
│   │   ├── single.py
│   │   └── batch.py
│   ├── alerts/
│   ├── warnings/
│   ├── predictions/
│   └── common/
│       └── delta_utils.py  # Lógica de delta spike
│
├── debug.py
├── batch_inserter.py
├── rate_limiter.py
├── device_auth.py
└── schemas.py