
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS bronze.service_requests_raw (
    raw_record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_city TEXT NOT NULL,
    source_request_id TEXT NOT NULL,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_source_request
    ON bronze.service_requests_raw (source_city, source_request_id);

CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at
    ON bronze.service_requests_raw (ingested_at);

CREATE TABLE IF NOT EXISTS silver.service_requests (
    source_city TEXT NOT NULL,
    source_request_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    agency TEXT,
    agency_name TEXT,
    category TEXT NOT NULL,
    description TEXT,
    status TEXT,
    area TEXT,
    postal_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_city, source_request_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_created_at
    ON silver.service_requests (created_at);

CREATE INDEX IF NOT EXISTS idx_silver_city_category
    ON silver.service_requests (source_city, category);

CREATE INDEX IF NOT EXISTS idx_silver_city_status
    ON silver.service_requests (source_city, status);

CREATE TABLE IF NOT EXISTS ops.ingestion_checkpoints (
    source_city TEXT NOT NULL,
    checkpoint_name TEXT NOT NULL,
    checkpoint_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_city, checkpoint_name)
);
