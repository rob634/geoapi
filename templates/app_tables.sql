-- Enhanced operations table
CREATE TABLE IF NOT EXISTS hosting.operations (
    operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    operation_type VARCHAR(50) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    parameters JSONB,
    operation_status VARCHAR(50) NOT NULL DEFAULT 'queued',
    priority INTEGER DEFAULT 5,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    queued_at TIMESTAMP,
    processing_started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result JSONB,
    error_details JSONB,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    logs JSONB,
    published_services JSONB, -- Track ArcGIS services created
    CONSTRAINT unique_request UNIQUE (request_id, operation_type)
);

-- Index for efficient queries
CREATE INDEX IF NOT EXISTS idx_operations_status ON hosting.operations(operation_status);
CREATE INDEX IF NOT EXISTS idx_operations_type_status ON hosting.operations(operation_type, operation_status);