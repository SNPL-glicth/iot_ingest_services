-- Migration 001: Audit Log Table for ISO 27001 Compliance
-- Creates ingestion_audit_log table for traceability

CREATE TABLE IF NOT EXISTS ingestion_audit_log (
    id BIGSERIAL PRIMARY KEY,
    
    -- Qué (What)
    series_id VARCHAR(500) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    value DOUBLE PRECISION,
    classification VARCHAR(50),
    
    -- Cómo (How)
    transport VARCHAR(20) NOT NULL,  -- http, mqtt, websocket, csv
    
    -- Quién (Who)
    api_key_hash VARCHAR(64),  -- SHA256 hash
    client_ip INET,
    
    -- Cuándo (When)
    data_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Resultado (Result)
    status VARCHAR(20) NOT NULL,  -- accepted, rejected, failed
    error_message TEXT,
    
    -- Metadata
    metadata JSONB
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_audit_series 
    ON ingestion_audit_log(series_id, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_source 
    ON ingestion_audit_log(source_id, domain, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_api_key 
    ON ingestion_audit_log(api_key_hash, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_timestamp 
    ON ingestion_audit_log(ingested_at DESC);

-- Comentarios para documentación
COMMENT ON TABLE ingestion_audit_log IS 'Audit trail for data ingestion (ISO 27001 A.12.4)';
COMMENT ON COLUMN ingestion_audit_log.api_key_hash IS 'SHA256 hash of API key (never store plaintext)';
COMMENT ON COLUMN ingestion_audit_log.data_timestamp IS 'Timestamp of the data point itself';
COMMENT ON COLUMN ingestion_audit_log.ingested_at IS 'Timestamp when data was ingested';
