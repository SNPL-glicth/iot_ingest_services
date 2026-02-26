# Autenticación y Autorización

Sistema de autenticación dual: IoT legacy + Universal con roles.

---

## Arquitectura

```
┌─────────────────────────────────────────────────┐
│              Autenticación                      │
├─────────────────────────────────────────────────┤
│  IoT Legacy          │  Universal (ISO 27001)   │
│  ─────────────       │  ─────────────────────   │
│  X-Device-Key        │  X-API-Key (con roles)   │
│  X-API-Key (global)  │  PostgreSQL validation   │
│  SQL Server          │  SHA-256 hashing         │
└─────────────────────────────────────────────────┘
```

---

## Autenticación IoT (Legacy)

### 1. Device Key (`device_key.py`)

**Header:** `X-Device-Key`

**Uso:** `POST /ingest/packets` (producción)

**Validación:**
- Verifica que device_uuid existe en SQL Server
- Verifica que sensor_uuid pertenece al device_uuid
- Cache con TTL para performance

**Activación:**
```bash
DEVICE_AUTH_ENABLED=1
```

### 2. API Key Global (`api_key.py`)

**Header:** `X-API-Key`

**Uso:** Endpoints legacy, desarrollo

**Validación:**
- Acepta cualquier key > 10 caracteres (trivial)
- Solo para desarrollo/testing
- **NO usar en producción**

---

## Autenticación Universal (ISO 27001)

### Arquitectura de Roles

**Archivo:** `authorization.py`

```python
class Role(str, Enum):
    ADMIN = "admin"           # Acceso completo
    SOURCE_WRITER = "source_writer"  # Solo su source_id
    READ_ONLY = "read_only"   # Sin escritura

@dataclass
class ApiKeyInfo:
    key_id: str
    key_prefix: str
    role: Role
    allowed_source_id: Optional[str]
    allowed_domains: List[str]
    is_active: bool
```

### Validación de API Keys

**Archivo:** `api_key_validator.py`

**Header:** `X-API-Key`

**Flujo:**
```
1. Recibir X-API-Key en header
   ↓
2. Hashear con SHA-256
   ↓
3. SELECT FROM api_keys WHERE key_hash = :hash
   ↓
4. Verificar is_active = TRUE
   ↓
5. UPDATE last_used_at = NOW()
   ↓
6. Retornar ApiKeyInfo con role y permisos
```

**Seguridad:**
- Keys nunca almacenados en plaintext (solo SHA-256)
- Intentos fallidos logueados con WARNING
- Connection string nunca expuesto en logs
- Errores genéricos al cliente (no detalles)

---

## Autorización por Roles

### ADMIN

**Permisos:**
- ✅ Puede escribir en cualquier source_id
- ✅ Puede escribir en cualquier domain
- ✅ allowed_source_id = NULL
- ✅ allowed_domains = []

**Uso:**
- Administración del sistema
- Migración de datos
- Testing multi-dominio

### SOURCE_WRITER

**Permisos:**
- ✅ Solo puede escribir en su source_id asignado
- ✅ Solo puede escribir en dominios permitidos
- ❌ allowed_source_id = NOT NULL (requerido)
- ❌ allowed_domains = array específico

**Uso:**
- Aplicaciones de producción
- Servicios específicos (web-01, db-01, etc.)
- Aislamiento de datos

### READ_ONLY

**Permisos:**
- ❌ Sin permisos de escritura
- ✅ Solo consultas (futuro)

**Uso:**
- Dashboards
- Reportes
- Auditoría

---

## Verificación de Acceso

**Función:** `verify_source_access(api_key_info, source_id, domain)`

**Validaciones:**

1. **Source ID Ownership:**
```python
if role == SOURCE_WRITER:
    if api_key_info.allowed_source_id != source_id:
        raise HTTPException(403, "Not authorized for this source_id")
```

2. **Domain Permissions:**
```python
if role == SOURCE_WRITER:
    if domain not in api_key_info.allowed_domains:
        raise HTTPException(403, "Not authorized for this domain")
```

**Logging:**
- Todos los intentos fallidos logueados con key_id
- No se loguea el API key en plaintext
- Solo key_prefix (primeros 8 chars) para identificación

---

## Base de Datos

### Tabla `api_keys`

**Migración:** `infrastructure/persistence/migrations/002_api_keys_roles.sql`

```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash VARCHAR(64) NOT NULL UNIQUE,  -- SHA-256
    key_prefix VARCHAR(8) NOT NULL,         -- Primeros 8 chars
    role VARCHAR(20) NOT NULL DEFAULT 'source_writer',
    allowed_source_id VARCHAR(255),         -- NULL = ADMIN
    allowed_domains TEXT[] DEFAULT '{}',    -- {} = ADMIN
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    created_by VARCHAR(255)
);
```

**Constraints:**
- `role` debe ser: admin, source_writer, read_only
- `SOURCE_WRITER` debe tener `allowed_source_id NOT NULL`
- Índice en `key_hash` para performance

---

## Generar API Keys

### Script Python

```python
import secrets
import hashlib

# 1. Generar key seguro
api_key = secrets.token_urlsafe(32)
print(f"API Key (guardar seguro): {api_key}")

# 2. Hashear para DB
key_hash = hashlib.sha256(api_key.encode()).hexdigest()
key_prefix = api_key[:8]

print(f"Hash (insertar en DB): {key_hash}")
print(f"Prefix (para logs): {key_prefix}")
```

### Insertar en PostgreSQL

```sql
-- SOURCE_WRITER para web-01 (solo infrastructure)
INSERT INTO api_keys (key_hash, key_prefix, role, allowed_source_id, allowed_domains, created_by)
VALUES (
    'abc123...hash',
    'Xy8zQ...',
    'source_writer',
    'web-01',
    ARRAY['infrastructure'],
    'admin_user'
);

-- ADMIN (acceso completo)
INSERT INTO api_keys (key_hash, key_prefix, role, allowed_source_id, allowed_domains, created_by)
VALUES (
    'def456...hash',
    'Zw9xR...',
    'admin',
    NULL,
    '{}',
    'admin_user'
);
```

---

## Uso en Endpoints

### Dependency Injection

```python
from fastapi import Depends
from .api_key_validator import verify_api_key, verify_source_access
from .authorization import ApiKeyInfo

@router.post("/ingest/data")
async def ingest_data(
    packet: DataPacketIn,
    api_key_info: ApiKeyInfo = Depends(verify_api_key),
):
    # Verificar autorización
    verify_source_access(api_key_info, packet.source_id, packet.domain)
    
    # Procesar ingesta
    ...
```

### Manejo de Errores

**401 Unauthorized:**
- API key no existe
- API key inactivo (is_active=FALSE)
- Hash no coincide

**403 Forbidden:**
- source_id no permitido
- domain no permitido
- Rol READ_ONLY intentando escribir

**503 Service Unavailable:**
- PostgreSQL no disponible
- Error de conexión

---

## Audit Trail (ISO 27001 A.12.4)

Todos los eventos de autenticación se registran:

**Tabla:** `ingestion_audit_log`

**Campos:**
- `api_key_id` - UUID del key usado
- `source_id` - Source ID del dato
- `domain` - Dominio del dato
- `transport` - http, mqtt, websocket, csv
- `status` - accepted, rejected
- `error_message` - Si fue rechazado
- `timestamp` - Timestamp del evento

**Retención:** Configurable (default: 90 días)

---

## Testing

### Crear API Key de Test

```bash
# Ejecutar migración
psql -U nico -d universal_ingest -f infrastructure/persistence/migrations/002_api_keys_roles.sql

# Generar key
python -c "
import secrets, hashlib
key = secrets.token_urlsafe(32)
print('Key:', key)
print('Hash:', hashlib.sha256(key.encode()).hexdigest())
"

# Insertar en DB (reemplazar hash)
psql -U nico -d universal_ingest -c "
INSERT INTO api_keys (key_hash, key_prefix, role, allowed_source_id, allowed_domains)
VALUES ('HASH_AQUI', 'PREFIX', 'source_writer', 'test-01', ARRAY['infrastructure']);
"
```

### Probar Endpoint

```bash
# Con key válido
curl -X POST http://localhost:8001/ingest/data \
  -H "Content-Type: application/json" \
  -H "X-API-Key: TU_KEY_AQUI" \
  -d '{
    "domain": "infrastructure",
    "source_id": "test-01",
    "data_points": [{"stream_id": "cpu", "value": 45.2}]
  }'

# Esperado: 200 OK

# Con source_id no permitido
curl -X POST http://localhost:8001/ingest/data \
  -H "X-API-Key: TU_KEY_AQUI" \
  -d '{
    "domain": "infrastructure",
    "source_id": "other-01",
    "data_points": [...]
  }'

# Esperado: 403 Forbidden
```

---

## Migración desde Legacy

### Fase 1: Coexistencia
- IoT usa X-Device-Key (sin cambios)
- Universal usa X-API-Key con roles
- Ambos sistemas funcionan en paralelo

### Fase 2: Migración Gradual
- Crear API keys para servicios existentes
- Migrar endpoints uno por uno
- Monitorear audit logs

### Fase 3: Deprecación
- Marcar X-API-Key global como deprecated
- Forzar uso de roles en nuevos servicios

### Fase 4: Cleanup
- Remover X-API-Key global (solo desarrollo)
- Solo X-Device-Key (IoT) y X-API-Key con roles (Universal)

---

## Cumplimiento ISO 27001

| Control | Implementación | Archivo |
|---------|----------------|---------|
| **A.9.2.3** - Gestión de privilegios | Roles ADMIN, SOURCE_WRITER, READ_ONLY | `authorization.py` |
| **A.9.4.2** - Log-on seguro | SHA-256 hashing, no plaintext | `api_key_validator.py` |
| **A.12.4.1** - Registro de eventos | Audit trail completo | `infrastructure/audit/audit_logger.py` |
| **A.12.4.3** - Logs de administrador | Intentos fallidos logueados | `api_key_validator.py:89` |

---

## Troubleshooting

**Error: "PostgreSQL not configured"**
- Verificar `POSTGRES_URL` en `.env`
- Ejecutar migraciones

**Error: "Invalid API key"**
- Verificar que el hash coincide en DB
- Verificar `is_active = TRUE`
- Revisar logs para key_prefix

**Error: "Not authorized for source_id"**
- Verificar `allowed_source_id` en tabla api_keys
- Verificar que role != READ_ONLY

**Error: "Not authorized for domain"**
- Verificar `allowed_domains` array en tabla api_keys
- Para ADMIN, debe ser array vacío `{}`
