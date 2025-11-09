-- Schema version tracking
-- Tracks schema evolution over time for CDC tables

CREATE TABLE IF NOT EXISTS schema_versions (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    schema JSONB NOT NULL,
    changes JSONB,
    change_type VARCHAR(50),
    applied_at TIMESTAMP DEFAULT NOW(),
    applied_by VARCHAR(255),
    rollback_sql TEXT,
    
    -- One version per table per version number
    UNIQUE(table_name, version)
);

-- Indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_schema_versions_table ON schema_versions(table_name);
CREATE INDEX IF NOT EXISTS idx_schema_versions_latest ON schema_versions(table_name, version DESC);
CREATE INDEX IF NOT EXISTS idx_schema_versions_applied_at ON schema_versions(applied_at);

-- Comments for documentation
COMMENT ON TABLE schema_versions IS 'Tracks schema evolution history for CDC tables';
COMMENT ON COLUMN schema_versions.schema IS 'Full schema definition at this version';
COMMENT ON COLUMN schema_versions.changes IS 'List of changes from previous version';
COMMENT ON COLUMN schema_versions.change_type IS 'Overall change classification: SAFE, WARNING, or BREAKING';
COMMENT ON COLUMN schema_versions.rollback_sql IS 'SQL statements to rollback this schema change';

