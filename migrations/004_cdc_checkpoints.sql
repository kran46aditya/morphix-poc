-- CDC Checkpoints Table
-- Stores resume tokens for crash recovery

CREATE TABLE IF NOT EXISTS cdc_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    collection VARCHAR(255) NOT NULL,
    resume_token JSONB NOT NULL,
    last_event_time TIMESTAMP,
    records_processed BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- One checkpoint per job per collection
    UNIQUE(job_id, collection)
);

-- Indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_cdc_checkpoints_job_id ON cdc_checkpoints(job_id);
CREATE INDEX IF NOT EXISTS idx_cdc_checkpoints_collection ON cdc_checkpoints(collection);
CREATE INDEX IF NOT EXISTS idx_cdc_checkpoints_updated_at ON cdc_checkpoints(updated_at);

-- Function to update updated_at automatically
CREATE OR REPLACE FUNCTION update_cdc_checkpoint_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the function
DROP TRIGGER IF EXISTS trigger_update_cdc_checkpoint_timestamp ON cdc_checkpoints;
CREATE TRIGGER trigger_update_cdc_checkpoint_timestamp
    BEFORE UPDATE ON cdc_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION update_cdc_checkpoint_timestamp();

-- Comments for documentation
COMMENT ON TABLE cdc_checkpoints IS 'Stores MongoDB CDC resume tokens for crash recovery';
COMMENT ON COLUMN cdc_checkpoints.resume_token IS 'MongoDB changestream resume token (opaque BSON)';
COMMENT ON COLUMN cdc_checkpoints.records_processed IS 'Total records processed since CDC start';

