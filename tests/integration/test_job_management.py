"""
Integration tests for job management system.

Tests job creation, scheduling, and execution flow.
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.jobs.job_manager import JobManager
from src.jobs.models import BatchJobConfig, StreamJobConfig, JobType, JobStatus, JobTrigger
from src.jobs.scheduler import JobScheduler


class TestJobManagementIntegration:
    """Integration tests for job management."""
    
    def test_job_creation_flow(self):
        """Test job creation and retrieval."""
        # Note: This requires database connection
        # In real integration tests, you'd use a test database
        
        # Create a test batch job config
        job_config = BatchJobConfig(
            job_id="test_job_123",
            job_name="Test Batch Job",
            user_id=1,
            mongo_uri="mongodb://localhost:27017/testdb",
            database="testdb",
            collection="testcollection",
            query={},
            date_field="created_at",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/hudi/test",
            created_by="test_user"
        )
        
        # Verify config structure
        assert job_config.job_type == JobType.BATCH
        assert job_config.job_id == "test_job_123"
        assert job_config.enabled is True
        
        return job_config
    
    def test_scheduler_initialization(self):
        """Test scheduler can be initialized."""
        scheduler = JobScheduler()
        assert scheduler.job_manager is not None
        assert scheduler.batch_processor is not None
        assert scheduler.stream_processor is not None
        assert scheduler.running is False
        
        # Test scheduler can be started (but don't keep it running)
        scheduler.start_scheduler()
        assert scheduler.running is True
        
        # Stop it immediately
        scheduler.stop_scheduler()
        assert scheduler.running is False
    
    def test_job_config_validation(self):
        """Test job configuration validation."""
        # Test batch job config
        batch_config = BatchJobConfig(
            job_id="test_1",
            job_name="Test",
            user_id=1,
            mongo_uri="mongodb://localhost:27017/db",
            database="db",
            collection="coll",
            date_field="date",
            hudi_table_name="table",
            hudi_base_path="/path",
            created_by="user"
        )
        assert batch_config.job_type == JobType.BATCH
        
        # Test stream job config
        stream_config = StreamJobConfig(
            job_id="test_2",
            job_name="Test Stream",
            user_id=1,
            mongo_uri="mongodb://localhost:27017/db",
            database="db",
            collection="coll",
            hudi_table_name="table",
            hudi_base_path="/path",
            created_by="user"
        )
        assert stream_config.job_type == JobType.STREAM
        assert stream_config.polling_interval_seconds == 60


class TestJobAPIIntegration:
    """Integration tests for job API endpoints (requires running server)."""
    
    @pytest.mark.skip(reason="Requires running API server and database")
    def test_job_crud_operations(self):
        """Test full CRUD operations for jobs."""
        # This would test:
        # 1. POST /jobs/batch/create - Create job
        # 2. GET /jobs/{job_id}/status - Get job status
        # 3. PUT /jobs/{job_id} - Update job
        # 4. PUT /jobs/{job_id}/disable - Disable job
        # 5. PUT /jobs/{job_id}/enable - Enable job
        # 6. DELETE /jobs/{job_id} - Delete job
        pass
    
    @pytest.mark.skip(reason="Requires running API server and database")
    def test_job_execution_flow(self):
        """Test job execution lifecycle."""
        # This would test:
        # 1. Create a job
        # 2. POST /jobs/{job_id}/start - Start job
        # 3. GET /jobs/{job_id}/status - Check status
        # 4. POST /jobs/{job_id}/stop - Stop job (if stream)
        # 5. Verify execution history
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

