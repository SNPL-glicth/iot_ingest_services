-- Migration 002: API Keys with Role-Based Access Control (ISO 27001)
-- Creates api_keys table for authentication and authorization

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- API Keys table with role-based access control
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash VARCHAR(64) NOT NULL UNIQUE,  -- SHA-256 del key real (nunca plaintext)
    key_prefix VARCHAR(8) NOT NULL,         -- Primeros 8 chars para identificación en logs
    role VARCHAR(20) NOT NULL DEFAULT 'source_writer',
    allowed_source_id VARCHAR(255),         -- NULL = cualquiera (solo ADMIN)
    allowed_domains TEXT[] DEFAULT '{}',    -- {} = todos (solo ADMIN)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    created_by VARCHAR(255),
    
    -- Constraints
    CONSTRAINT chk_role CHECK (role IN ('admin', 'source_writer', 'read_only')),
    CONSTRAINT chk_admin_permissions CHECK (
        -- ADMIN puede tener allowed_source_id NULL y allowed_domains vacío
        -- SOURCE_WRITER debe tener allowed_source_id NOT NULL
        (role = 'admin') OR 
        (role = 'source_writer' AND allowed_source_id IS NOT NULL) OR
        (role = 'read_only')
    )
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_api_keys_hash 
    ON api_keys (key_hash) WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_api_keys_source 
    ON api_keys (allowed_source_id) WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_api_keys_active 
    ON api_keys (is_active, last_used_at DESC);

-- Comentarios para documentación (ISO 27001 A.9.2.3)
COMMENT ON TABLE api_keys IS 'API keys for universal ingestion with role-based access control (ISO 27001 A.9.2.3)';
COMMENT ON COLUMN api_keys.key_hash IS 'SHA-256 hash of API key (never store plaintext)';
COMMENT ON COLUMN api_keys.key_prefix IS 'First 8 characters of key for identification in logs';
COMMENT ON COLUMN api_keys.role IS 'Role: admin (full access), source_writer (limited to source_id), read_only (no write)';
COMMENT ON COLUMN api_keys.allowed_source_id IS 'Source ID this key can write to (NULL = any, admin only)';
COMMENT ON COLUMN api_keys.allowed_domains IS 'Domains this key can write to (empty array = all, admin only)';
COMMENT ON COLUMN api_keys.last_used_at IS 'Last time this key was used for authentication';

-- Ejemplo de inserción de API key ADMIN (para testing)
-- Key real: "admin-test-key-12345678901234567890"
-- Hash SHA-256: calculado con hashlib.sha256(b"admin-test-key-12345678901234567890").hexdigest()
INSERT INTO api_keys (key_hash, key_prefix, role, allowed_source_id, allowed_domains, created_by)
VALUES (
    'e8c8c0c8f5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5',  -- Placeholder hash
    'admin-te',
    'admin',
    NULL,  -- Admin puede escribir en cualquier source_id
    '{}',  -- Admin puede escribir en cualquier domain
    'migration_002'
)
ON CONFLICT (key_hash) DO NOTHING;

-- IMPORTANTE: En producción, generar keys reales con:
-- import secrets; secrets.token_urlsafe(32)
-- Y hashear con: hashlib.sha256(key.encode()).hexdigest()
