-- CDR Migration System PostgreSQL Schema
-- This schema supports the CDR migration system with progress tracking,
-- document buffering, and collection management

-- Enable UUID extension for unique identifiers
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Migration Progress Table
-- Tracks the overall progress of migration for each date
CREATE TABLE IF NOT EXISTS migration_progress (
    date_str VARCHAR(8) PRIMARY KEY,              -- YYYYMMDD format
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, extracting, extracted, inserting, completed, failed
    extraction_id UUID,                           -- Links to extraction tasks
    source_records INTEGER DEFAULT 0,             -- Total records found in source
    extracted_records INTEGER DEFAULT 0,          -- Records successfully extracted
    target_collections JSONB DEFAULT '{}',        -- Map of target collections and record counts
    started_at TIMESTAMP DEFAULT NOW(),           -- Processing start time
    updated_at TIMESTAMP DEFAULT NOW(),           -- Last progress update
    completed_at TIMESTAMP,                       -- Processing completion time
    version INTEGER DEFAULT 1,                    -- Optimistic locking for atomic updates
    error_count INTEGER DEFAULT 0,                -- Number of processing errors
    error_message TEXT,                           -- Latest error details

    CONSTRAINT valid_status CHECK (status IN ('pending', 'extracting', 'extracted', 'inserting', 'completed', 'failed'))
);


-- Collection Management Table
-- Tracks target collection creation and statistics
CREATE TABLE IF NOT EXISTS collection_management (
    collection_name VARCHAR(20) PRIMARY KEY,      -- e.g., cdr2020, cdr2021
    first_created_at TIMESTAMP DEFAULT NOW(),     -- When collection was first encountered
    indexed_at TIMESTAMP,                         -- When indexes were created
    record_count BIGINT DEFAULT 0,                -- Running record count
    last_updated TIMESTAMP DEFAULT NOW(),         -- Last time collection was updated

    CONSTRAINT valid_collection_name CHECK (collection_name ~ '^cdr[0-9]{4}$')
);

-- Indexes for performance optimization

-- Migration Progress indexes
CREATE INDEX IF NOT EXISTS idx_migration_progress_status ON migration_progress(status);
CREATE INDEX IF NOT EXISTS idx_migration_progress_started_at ON migration_progress(started_at);


-- Collection Management indexes
CREATE INDEX IF NOT EXISTS idx_collection_management_last_updated ON collection_management(last_updated);

-- Triggers for automatic timestamp updates

-- Update migration_progress.updated_at on any change
CREATE OR REPLACE FUNCTION update_migration_progress_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_migration_progress_timestamp
    BEFORE UPDATE ON migration_progress
    FOR EACH ROW
    EXECUTE FUNCTION update_migration_progress_timestamp();

-- Update collection_management.last_updated on any change
CREATE OR REPLACE FUNCTION update_collection_management_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_collection_management_timestamp
    BEFORE UPDATE ON collection_management
    FOR EACH ROW
    EXECUTE FUNCTION update_collection_management_timestamp();

-- Utility functions for common operations

-- Function to get migration statistics
CREATE OR REPLACE FUNCTION get_migration_statistics(
    start_date VARCHAR(8) DEFAULT NULL,
    end_date VARCHAR(8) DEFAULT NULL
)
RETURNS TABLE (
    total_dates BIGINT,
    completed_dates BIGINT,
    failed_dates BIGINT,
    total_source_records BIGINT,
    total_extracted_records BIGINT,
    completion_rate DECIMAL(5,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) as total_dates,
        COUNT(*) FILTER (WHERE status = 'completed') as completed_dates,
        COUNT(*) FILTER (WHERE status = 'failed') as failed_dates,
        COALESCE(SUM(source_records), 0) as total_source_records,
        COALESCE(SUM(extracted_records), 0) as total_extracted_records,
        CASE
            WHEN COUNT(*) > 0 THEN
                ROUND((COUNT(*) FILTER (WHERE status = 'completed')::DECIMAL / COUNT(*)) * 100, 2)
            ELSE 0
        END as completion_rate
    FROM migration_progress
    WHERE (start_date IS NULL OR date_str >= start_date)
    AND (end_date IS NULL OR date_str <= end_date);
END;
$$ LANGUAGE plpgsql;


-- Views for monitoring and reporting

-- View for migration overview
CREATE OR REPLACE VIEW migration_overview AS
SELECT
    date_str,
    status,
    source_records,
    extracted_records,
    CASE
        WHEN source_records > 0 THEN
            ROUND((extracted_records::DECIMAL / source_records) * 100, 2)
        ELSE 0
    END as extraction_rate,
    target_collections,
    error_count,
    EXTRACT(EPOCH FROM (COALESCE(completed_at, updated_at) - started_at)) / 60 as duration_minutes,
    started_at,
    completed_at
FROM migration_progress
ORDER BY date_str DESC;
