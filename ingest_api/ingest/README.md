# Arquitectura de Ingesta por Propósito

## Estructura

```
ingest/
├── common/                    # Utilidades compartidas
│   ├── validation.py         # Validaciones defensivas por pipeline
│   ├── physical_ranges.py    # Manejo de rangos físicos
│   └── delta_utils.py         # Detección de delta spikes
│
├── alerts/                    # Pipeline de ALERTAS
│   ├── alert_ingest.py        # Clase principal del pipeline
│   ├── alert_rules.py         # Reglas de negocio
│   └── alert_persistence.py   # Lógica de persistencia
│
├── warnings/                  # Pipeline de WARNINGS
│   ├── warning_ingest.py      # Clase principal del pipeline
│   ├── warning_rules.py       # Reglas de negocio
│   └── warning_persistence.py # Lógica de persistencia
│
├── predictions/               # Pipeline de PREDICCIONES
│   ├── prediction_ingest.py  # Clase principal del pipeline
│   ├── prediction_rules.py    # Reglas de negocio
│   └── prediction_dispatch.py  # Dispatch al broker ML
│
└── router.py                  # Router central de clasificación
```

## Principios de Diseño

### 1. Separación por Propósito
Cada pipeline es **independiente** y **aislado**:
- Define qué datos acepta
- Tiene sus propias reglas de validación
- Implementa su propia lógica de persistencia/dispatch
- Rechaza defensivamente datos que no le corresponden

### 2. Validación Defensiva
Cada pipeline **rechaza** datos que no cumplen sus criterios:
- `AlertIngestPipeline`: Solo acepta violaciones de rango físico
- `WarningIngestPipeline`: Solo acepta delta spikes detectados
- `PredictionIngestPipeline`: Solo acepta datos limpios

### 3. Router Central
El `ReadingRouter` clasifica **UNA VEZ** y enruta a **exactamente UN pipeline**:
- Orden de evaluación estricto: ALERT → WARNING → PREDICTION
- Sin estado compartido entre sensores
- Sin filtrado implícito, solo enrutamiento explícito

## Pipelines

### ALERT Pipeline
**Propósito**: Valores fuera de rango físico

**Reglas**:
- ✅ Acepta solo violaciones de rango físico
- ✅ Severity siempre = `critical` (nunca puede ser downgradeado)
- ✅ Persiste: última lectura válida + lectura que rompe umbral
- ❌ **NUNCA** envía datos al ML

**Archivos**:
- `alert_rules.py`: Define qué acepta y reglas de negocio
- `alert_persistence.py`: Persistencia en `alerts` y `sensor_readings`
- `alert_ingest.py`: Clase principal con validación defensiva

### WARNING Pipeline
**Propósito**: Delta spikes o cambios bruscos

**Reglas**:
- ✅ Acepta solo lecturas con delta spike detectado
- ✅ Persiste: evento de delta spike en `ml_events`
- ✅ Solo 1 advertencia activa por sensor
- ❌ **NUNCA** envía datos al ML

**Archivos**:
- `warning_rules.py`: Define qué acepta y reglas de negocio
- `warning_persistence.py`: Persistencia en `ml_events` con código `DELTA_SPIKE`
- `warning_ingest.py`: Clase principal con validación defensiva

### PREDICTION Pipeline
**Propósito**: Datos limpios para ML

**Reglas**:
- ✅ Acepta solo datos limpios (sin violación física, sin delta spike)
- ✅ Conserva decimales completos
- ✅ NO persiste todas las lecturas masivamente
- ✅ Actualiza `sensor_readings_latest`
- ✅ **SIEMPRE** envía datos limpios al broker ML

**Archivos**:
- `prediction_rules.py`: Define qué acepta y reglas de negocio
- `prediction_dispatch.py`: Dispatch al broker ML
- `prediction_ingest.py`: Clase principal con validación defensiva

## Flujo de Datos

```
Lectura → ReadingRouter.classify_and_route()
              ↓
    ┌─────────┼─────────┐
    ↓         ↓         ↓
  ALERT   WARNING  PREDICTION
    ↓         ↓         ↓
  Rechaza  Rechaza   Rechaza
  si no    si no     si no
  viola    tiene     está
  rango    spike     limpio
    ↓         ↓         ↓
  Persiste Persiste  Dispatch
  alerta   warning   a ML
```

## Validación Defensiva

Cada pipeline implementa validación defensiva:

```python
# Ejemplo: AlertIngestPipeline
def ingest(self, sensor_id, value, ...):
    should_accept, physical_range, reason = AlertRules.accepts(...)
    
    if not should_accept:
        raise ValueError(f"Alert pipeline rechazó datos: {reason}")
    
    # Solo procesa si pasa la validación
    persist_alert(...)
```

## Reglas Estrictas

1. **ALERT y WARNING NO envían datos al ML**
   - Implementado con `assert not AlertRules.should_forward_to_ml()`
   - Implementado con `assert not WarningRules.should_forward_to_ml()`

2. **PREDICTION solo recibe datos limpios**
   - Validación defensiva rechaza violaciones físicas
   - Validación defensiva rechaza delta spikes

3. **Solo 1 alerta/advertencia activa por sensor**
   - Implementado en `alert_persistence.py` y `warning_persistence.py`
   - Cierra alertas/advertencias previas antes de crear nuevas

4. **No guardar todas las lecturas masivamente**
   - ALERT: Solo guarda lectura que rompe umbral
   - WARNING: Solo guarda evento de delta spike
   - PREDICTION: Solo actualiza `sensor_readings_latest`, NO persiste lectura completa

5. **No downgradear severity CRITICAL**
   - `AlertRules.get_severity()` siempre retorna `"critical"`
   - No hay código path que pueda cambiar esto

## Compatibilidad

- ✅ Endpoints públicos sin cambios
- ✅ Respuestas idénticas (`IngestResult`, `PacketIngestResult`)
- ✅ Sin breaking changes en la API
- ✅ Mismo comportamiento desde el exterior

## Uso

```python
from ingest.router import ReadingRouter

router = ReadingRouter(db, broker)
pipeline_type = router.classify_and_route(
    sensor_id=1,
    value=45.5,
    device_timestamp=datetime.now(),
)
# Retorna: PipelineType.ALERT, WARNING, o PREDICTION
```

