# Refactorización del Servicio de Ingesta

## Resumen

El servicio de ingesta ha sido refactorizado para clasificar los datos **ANTES** de persistir o enviar a otros servicios, dividiéndolos en 3 flujos lógicos separados por propósito.

## Problema Anterior

- Todas las lecturas entraban igual
- Se guardaban masivamente (>100k lecturas)
- Alertas, advertencias y ML consumían los mismos datos
- Esto generaba:
  - Saturación de la base de datos
  - Deadlocks en DB
  - Predicciones incorrectas (valores críticos → severity = low)
  - Alertas duplicadas por sensor

## Nueva Arquitectura

### 1. Clasificación por Propósito (`classification.py`)

Cada lectura se clasifica en uno de 3 flujos:

#### A. ALERTAS (hard rules) - `ReadingClass.ALERT`
- **Se activan solo si**: el valor viola el rango físico del sensor
- **No se guardan todas las lecturas**
- **Solo se guarda**:
  - Última lectura válida (en `sensor_readings_latest`)
  - Lectura que rompe el umbral (en `sensor_readings` + `alerts`)
- **Genera**: 1 alerta activa por sensor (no acumulable)
- **Severity siempre**: `critical`

#### B. ADVERTENCIAS / VARIACIONES (delta/spike) - `ReadingClass.WARNING`
- **Detecta cambios rápidos**:
  - Delta absoluto: `|current - last|`
  - Delta relativo: `|delta| / |last|`
  - Slope absoluto: `|delta| / dt` (unidades/segundo)
  - Slope relativo: `(|delta|/|last|) / dt` (1/segundo)
- **Ejemplo válido**:
  ```
  0.1s → 45
  0.2s → 46  ← DELTA_SPIKE detectado
  0.3s → 45
  ```
- **Solo guarda eventos**, no el stream completo
- **Máximo**: 1 advertencia activa por sensor

#### C. DATOS PARA ML / PREDICCIÓN - `ReadingClass.ML_PREDICTION`
- **Solo entran lecturas**:
  - Sin violación física
  - Sin delta spike fuerte
- **Se conservan decimales**
- **El ML NO recibe datos crudos masivos**
- **Se trabajan ventanas cortas en memoria** + agregados (1m, 5m, 1h)

### 2. Flujos de Ingesta Separados (`ingest_flows.py`)

#### `ingest_alert()`
- Actualiza `sensor_readings_latest`
- Guarda la lectura que rompe el umbral en `sensor_readings`
- Cierra alertas activas previas del mismo sensor
- Crea nueva alerta activa con severity=`critical`

#### `ingest_warning()`
- Actualiza `sensor_readings_latest`
- Guarda el evento de delta spike en `sensor_readings`
- Cierra advertencias activas previas del mismo sensor
- Crea nuevo evento ML de tipo `DELTA_SPIKE` en `ml_events`

#### `ingest_prediction()`
- Actualiza `sensor_readings_latest`
- **NO guarda todas las lecturas** en `sensor_readings`
- La lectura se publica en el broker ML para procesamiento en memoria
- Reduce carga en BD y evita deadlocks

### 3. Integración en `main.py`

Los endpoints existentes (`/ingest/readings`, `/ingest/readings/bulk`, `/ingest/packets`) ahora:

1. Clasifican cada lectura usando `ReadingClassifier`
2. Enrutan al flujo correspondiente (`ingest_alert`, `ingest_warning`, `ingest_prediction`)
3. Solo publican en el broker ML si la lectura es `ML_PREDICTION` (datos limpios)

## Reglas Críticas (NO Negociables)

1. **Nunca puede existir**: valor fuera de rango físico con severity = low
2. **Si hay violación física**: ML NO puede bajar severidad
3. **Si hay DELTA_SPIKE reciente**: severity ≥ warning
4. **Una alerta/advertencia/predicción por sensor**, no acumulable

## Persistencia Optimizada

- **Ingesta NO guarda todas las lecturas**
- **Guardar solo**:
  - Último valor por sensor (`sensor_readings_latest`)
  - Eventos relevantes (`alerts`, `ml_events`)
- **Evitar**: locks largos y transacciones pesadas

## Beneficios

1. **Reducción de carga**: Solo se guardan lecturas relevantes
2. **Eliminación de deadlocks**: Menos transacciones pesadas
3. **Severidades correctas**: Valores críticos siempre tienen severity=critical
4. **ML más preciso**: Solo recibe datos limpios, sin violaciones físicas ni spikes
5. **Alertas no duplicadas**: 1 alerta activa por sensor

## Compatibilidad

- **Endpoints existentes**: No se modifican (backward compatible)
- **Backend**: No se rompe la arquitectura existente
- **Stored procedures**: Se mantienen para compatibilidad, pero la lógica principal está en Python

## Archivos Modificados

- `ingest_api/main.py`: Refactorizado para usar clasificación
- `ingest_api/classification.py`: Nuevo módulo de clasificación
- `ingest_api/ingest_flows.py`: Nuevo módulo con flujos separados

## Próximos Pasos (Opcional)

- Agregar métricas de clasificación (cuántas lecturas van a cada flujo)
- Implementar agregados periódicos para ML (1m, 5m, 1h)
- Optimizar cache de rangos físicos y umbrales de delta

