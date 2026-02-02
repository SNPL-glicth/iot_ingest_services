# Auditoría: Warm-up y Consistencia de Severidades

**Fecha:** 2026-02-02  
**Autor:** Cascade  
**Estado:** Correcciones implementadas

---

## Problemas Identificados

### 1. Primeras lecturas disparan delta_spike (sin warm-up)

**Síntoma:** Las primeras lecturas de un sensor nuevo disparan eventos Delta Spike inmediatamente.

**Causa raíz:** El SP `sp_insert_reading_and_check_threshold` no verificaba si el sensor tenía suficientes lecturas históricas antes de evaluar delta spikes.

**Ubicación del problema:**
- `database/scripts/fix_delta_spike_cooldown.sql` líneas 274-439
- El SP solo verificaba `cooldown_sec` y `min_delta_abs`, pero NO `min_readings`

### 2. Inconsistencia ALERT vs WARNING

**Síntoma:** Se publica ALERT en notificaciones pero se guarda WARNING en historial (o viceversa).

**Causa raíz:** El SP usaba diferentes variables para determinar la severidad en `alerts` vs `alert_notifications`.

**Ubicación del problema:**
- SP líneas 227-239: La notificación usaba `@v_final_severity` pero el título usaba lógica separada
- SP líneas 423-436: Para delta spike, `event_type` y `severity` de notificación podían diferir

### 3. ML responde instantáneamente sin ventana temporal

**Síntoma:** El sistema ML evalúa delta spikes con solo 1 lectura previa.

**Causa raíz:** No había validación de `min_readings` en el detector de delta spike.

**Ubicación del problema:**
- `classification/delta_detector.py`: No verifica cantidad de lecturas históricas
- SP: Solo verificaba existencia de `@v_prev_value`, no cantidad de lecturas

### 4. Falta de baseline histórico obligatorio

**Síntoma:** Sensores nuevos generan eventos sin contexto suficiente.

**Causa raíz:** No existía concepto de "warm-up" en el SP. El `state_manager.py` lo implementaba pero el SP lo ignoraba.

---

## Arquitectura del Problema

```
┌─────────────────────────────────────────────────────────────────┐
│                     FLUJO ACTUAL (ROTO)                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  MQTT → router.py → SP → alerts + alert_notifications           │
│                      ↓                                          │
│                   ml_events (delta spike)                       │
│                                                                 │
│  PROBLEMA: SP no respeta warm-up del state_manager              │
│  PROBLEMA: Severidades inconsistentes entre tablas              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     FLUJO CORREGIDO                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  MQTT → router.py → SP (con warm-up) → alerts + notifications   │
│                      ↓                                          │
│                   ml_events (con min_readings)                  │
│                                                                 │
│  1. SP verifica operational_state antes de evaluar umbrales     │
│  2. SP incrementa valid_readings_count en cada lectura          │
│  3. SP transiciona INITIALIZING → NORMAL después de N lecturas  │
│  4. Delta spike requiere min_readings lecturas recientes        │
│  5. Severidades SIEMPRE consistentes entre tablas               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Correcciones Implementadas

### Migración SQL: `fix_warmup_and_severity.sql`

**Cambios en schema:**
```sql
-- Columnas agregadas a sensors
ALTER TABLE sensors ADD valid_readings_count INT NOT NULL DEFAULT 0;
ALTER TABLE sensors ADD min_readings_for_normal INT NOT NULL DEFAULT 10;
ALTER TABLE sensors ADD operational_state VARCHAR(20) NOT NULL DEFAULT 'INITIALIZING';
ALTER TABLE sensors ADD state_changed_at DATETIME2 NULL;

-- Columna agregada a delta_thresholds
ALTER TABLE delta_thresholds ADD min_readings INT NOT NULL DEFAULT 5;
```

**Cambios en SP:**

1. **Warm-up obligatorio:**
   ```sql
   IF @v_operational_state = 'INITIALIZING'
   BEGIN
       SET @v_valid_readings_count = @v_valid_readings_count + 1;
       UPDATE sensors SET valid_readings_count = @v_valid_readings_count WHERE id = @p_sensor_id;
       
       IF @v_valid_readings_count >= @v_min_readings_for_normal
           -- Transicionar a NORMAL
       ELSE
           RETURN; -- Salir sin evaluar umbrales
   END
   ```

2. **Delta spike con min_readings:**
   ```sql
   IF @v_recent_readings_count < @v_d_min_readings
   BEGIN
       SET @v_delta_trigger = 0; -- No disparar
   END
   ```

3. **Severidades consistentes:**
   ```sql
   -- alerts y alert_notifications usan @v_final_severity
   -- ml_events y alert_notifications usan @v_delta_severity
   ```

---

## Reglas de Dominio Finales

### Estados Operacionales del Sensor

| Estado | Puede generar eventos | Descripción |
|--------|----------------------|-------------|
| INITIALIZING | ❌ NO | Warm-up, acumulando lecturas |
| NORMAL | ✅ SÍ | Operando normalmente |
| WARNING | ✅ SÍ | Delta spike activo |
| ALERT | ✅ SÍ | Umbral crítico violado |
| STALE | ❌ NO | Sin lecturas recientes |

### Transiciones de Estado

```
INITIALIZING ──(N lecturas)──→ NORMAL
NORMAL ──(warning threshold)──→ WARNING
NORMAL ──(critical threshold)──→ ALERT
WARNING ──(critical threshold)──→ ALERT
WARNING ──(valor normal)──→ NORMAL
ALERT ──(valor normal)──→ NORMAL
NORMAL ──(sin lecturas 2h)──→ STALE
STALE ──(nueva lectura)──→ INITIALIZING
```

### Prioridad de Evaluación

1. **CRITICAL** (prioridad absoluta)
2. **WARNING** (solo si no hay CRITICAL)
3. **DELTA SPIKE** (solo si no hay CRITICAL ni WARNING de umbral)
4. **ML_PREDICTION** (dato limpio)

---

## Validación

### Script de Test: `test_warmup_validation.sql`

Ejecutar para validar:
```sql
-- En SSMS o Azure Data Studio
:r C:\Users\SOPORTE\Desktop\flutter2\database\scripts\fix_warmup_and_severity.sql
:r C:\Users\SOPORTE\Desktop\flutter2\database\scripts\test_warmup_validation.sql
```

**Resultados esperados:**
- Lecturas 1-10: NO alertas, NO notificaciones, NO eventos ML
- Lectura 11+: SÍ alertas si valor fuera de rango
- Severidades consistentes entre `alerts.severity` y `alert_notifications.severity`

---

## Archivos Modificados

| Archivo | Cambio |
|---------|--------|
| `database/scripts/fix_warmup_and_severity.sql` | **NUEVO** - Migración con correcciones |
| `database/scripts/test_warmup_validation.sql` | **NUEVO** - Script de validación |
| `classification/state_manager.py` | Ya implementaba warm-up (sin cambios) |
| `classification/state_repository.py` | Ya implementaba warm-up (sin cambios) |

---

## Próximos Pasos

1. **Ejecutar migración** en BD de desarrollo:
   ```powershell
   sqlcmd -S localhost -d iot_db -i database/scripts/fix_warmup_and_severity.sql
   ```

2. **Ejecutar test de validación**:
   ```powershell
   sqlcmd -S localhost -d iot_db -i database/scripts/test_warmup_validation.sql
   ```

3. **Reiniciar sensores existentes** (opcional):
   ```sql
   UPDATE sensors SET operational_state = 'INITIALIZING', valid_readings_count = 0;
   ```

4. **Monitorear logs** para verificar:
   - `reason: "warmup"` durante primeras lecturas
   - `reason: "insufficient_data"` para delta spikes tempranos
   - Severidades consistentes en notificaciones
