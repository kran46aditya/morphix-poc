-- Volume routing metadata
ALTER TABLE jobs 
ADD COLUMN IF NOT EXISTS estimated_daily_volume BIGINT,
ADD COLUMN IF NOT EXISTS force_sink_type VARCHAR(20);

-- Quality scores
CREATE TABLE IF NOT EXISTS quality_scores (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    overall_score FLOAT NOT NULL,
    rule_results JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Cost tracking
CREATE TABLE IF NOT EXISTS cost_tracking (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,
    embedding_cost FLOAT DEFAULT 0,
    storage_cost FLOAT DEFAULT 0,
    total_cost FLOAT DEFAULT 0,
    embeddings_generated INT DEFAULT 0,
    cache_hits INT DEFAULT 0,
    savings_from_cache FLOAT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Schema evolution tracking
CREATE TABLE IF NOT EXISTS schema_versions (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    schema JSONB NOT NULL,
    changes JSONB,
    breaking_changes BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(job_id, version)
);

-- Embedding cache metadata (Redis stores actual embeddings)
CREATE TABLE IF NOT EXISTS embedding_metadata (
    text_hash VARCHAR(64) PRIMARY KEY,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_accessed TIMESTAMP DEFAULT NOW(),
    access_count INT DEFAULT 1,
    dimension INT DEFAULT 384
);

-- Add resume token to job_executions (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'job_executions') THEN
        ALTER TABLE job_executions 
        ADD COLUMN IF NOT EXISTS resume_token JSONB;
    END IF;
END $$;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_quality_scores_job ON quality_scores(job_id);
CREATE INDEX IF NOT EXISTS idx_cost_tracking_job ON cost_tracking(job_id);
CREATE INDEX IF NOT EXISTS idx_schema_versions_job ON schema_versions(job_id);
CREATE INDEX IF NOT EXISTS idx_embedding_metadata_last_accessed ON embedding_metadata(last_accessed);

