-- PostgreSQL schema for multi-domain universal ingestion
-- This schema supports infrastructure, finance, health, and other non-IoT domains
-- IoT domain continues to use SQL Server

-- Enable TimescaleDB extension for time-series optimization
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Stream configurations table
-- Stores metadata and settings for each data stream
CREATE TABLE IF NOT EXISTS stream_configs (
    id BIGSERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL DEFAULT 'generic',
    display_name VARCHAR(255),
    unit VARCHAR(50),
    enable_ml_prediction BOOLEAN DEFAULT TRUE,
    enable_alerting BOOLEAN DEFAULT TRUE,
    domain_metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stream_id, source_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_stream_configs_domain 
ON stream_configs (domain);

CREATE INDEX IF NOT EXISTS idx_stream_configs_source 
ON stream_configs (source_id, domain);

-- Value constraints table
-- Defines validation rules for each stream
CREATE TABLE IF NOT EXISTS value_constraints (
    id BIGSERIAL PRIMARY KEY,
    stream_config_id BIGINT REFERENCES stream_configs(id) ON DELETE CASCADE,
    physical_min DOUBLE PRECISION,
    physical_max DOUBLE PRECISION,
    operational_min DOUBLE PRECISION,
    operational_max DOUBLE PRECISION,
    warning_min DOUBLE PRECISION,
    warning_max DOUBLE PRECISION,
    critical_min DOUBLE PRECISION,
    critical_max DOUBLE PRECISION,
    max_abs_delta DOUBLE PRECISION,
    max_rel_delta DOUBLE PRECISION,
    noise_abs_threshold DOUBLE PRECISION,
    z_score_threshold DOUBLE PRECISION DEFAULT 3.0,
    consecutive_violations_required INT DEFAULT 1,
    cooldown_seconds INT DEFAULT 300,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (stream_config_id)
);

-- Data points table (hypertable for time-series data)
-- Stores all ingested data points from non-IoT domains
CREATE TABLE IF NOT EXISTS data_points (
    id BIGSERIAL,
    stream_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    classification VARCHAR(50) DEFAULT 'normal',
    sequence BIGINT,
    domain_metadata JSONB DEFAULT '{}',
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable for efficient time-series queries
SELECT create_hypertable('data_points', 'timestamp', 
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_dp_stream_time 
ON data_points (stream_id, source_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_dp_domain_time 
ON data_points (domain, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_dp_classification 
ON data_points (classification) 
WHERE classification != 'normal';

CREATE INDEX IF NOT EXISTS idx_dp_source 
ON data_points (source_id, domain, timestamp DESC);

-- Stream alerts table
-- Tracks active and resolved alerts for all streams
CREATE TABLE IF NOT EXISTS stream_alerts (
    id BIGSERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    threshold_violated VARCHAR(100),
    message TEXT,
    triggered_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    resolution_reason VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_alerts_active 
ON stream_alerts (stream_id, source_id, is_active, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_domain 
ON stream_alerts (domain, is_active, triggered_at DESC);

-- CSV import jobs table
-- Tracks bulk import operations from CSV files
CREATE TABLE IF NOT EXISTS csv_import_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(255),
    domain VARCHAR(50),
    filename VARCHAR(500),
    status VARCHAR(20) DEFAULT 'pending',
    total_rows INT DEFAULT 0,
    processed_rows INT DEFAULT 0,
    inserted_rows INT DEFAULT 0,
    rejected_rows INT DEFAULT 0,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_csv_jobs_status 
ON csv_import_jobs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_csv_jobs_domain 
ON csv_import_jobs (domain, created_at DESC);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for stream_configs
DROP TRIGGER IF EXISTS update_stream_configs_updated_at ON stream_configs;
CREATE TRIGGER update_stream_configs_updated_at
    BEFORE UPDATE ON stream_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for value_constraints
DROP TRIGGER IF EXISTS update_value_constraints_updated_at ON value_constraints;
CREATE TRIGGER update_value_constraints_updated_at
    BEFORE UPDATE ON value_constraints
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
