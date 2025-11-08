"""
Unit tests for cost tracking functionality.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.monitoring.cost_tracker import CostTracker, CostBreakdown
from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger


class TestCostTracker:
    """Test cases for CostTracker class."""
    
    def test_initialization(self):
        """Test tracker initialization."""
        tracker = CostTracker()
        assert tracker is not None
        # Check that constants exist
        assert hasattr(CostTracker, 'CPU_HOUR_COST')
        assert hasattr(CostTracker, 'STORAGE_GB_MONTH')
        assert CostTracker.CPU_HOUR_COST >= 0
        assert CostTracker.STORAGE_GB_MONTH >= 0
    
    def test_calculate_execution_cost_basic(self):
        """Test basic execution cost calculation."""
        tracker = CostTracker()
        
        # Mock job execution
        class MockJobExecution:
            pass
        
        job_exec = MockJobExecution()
        
        cost = tracker.calculate_execution_cost(
            job_execution=job_exec,
            embeddings_generated=1000,
            cache_hits=500,
            storage_gb=1.0
        )
        
        assert isinstance(cost, CostBreakdown)
        assert cost.embedding_compute_cost >= 0
        assert cost.storage_cost >= 0
        assert cost.savings_from_cache >= 0
        assert cost.total_cost >= 0
    
    def test_calculate_execution_cost_with_cache(self):
        """Test cost calculation with cache savings."""
        tracker = CostTracker()
        
        class MockJobExecution:
            pass
        
        job_exec = MockJobExecution()
        
        # 10K embeddings, 7K cached (70% cache hit rate)
        cost = tracker.calculate_execution_cost(
            job_execution=job_exec,
            embeddings_generated=10000,
            cache_hits=7000,
            storage_gb=5.0
        )
        
        assert cost.embedding_compute_cost > 0
        assert cost.savings_from_cache > 0
        assert cost.total_cost > 0
        # Savings should reduce total cost
        assert cost.savings_from_cache > 0
    
    def test_calculate_execution_cost_no_cache(self):
        """Test cost calculation without cache."""
        tracker = CostTracker()
        
        class MockJobExecution:
            pass
        
        job_exec = MockJobExecution()
        
        cost = tracker.calculate_execution_cost(
            job_execution=job_exec,
            embeddings_generated=1000,
            cache_hits=0,
            storage_gb=1.0
        )
        
        assert cost.savings_from_cache == 0
        assert cost.embedding_compute_cost > 0
    
    def test_estimate_monthly_cost(self):
        """Test monthly cost estimation."""
        tracker = CostTracker()
        
        job_config = BatchJobConfig(
            job_id="test_cost",
            job_name="Test Job",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at",
            estimated_daily_volume=100000
        )
        
        estimate = tracker.estimate_monthly_cost(job_config)
        
        assert isinstance(estimate, dict)
        assert 'embeddings' in estimate
        assert 'storage' in estimate
        assert 'total' in estimate
        assert 'records_per_dollar' in estimate
        assert estimate['total'] > 0
        assert estimate['records_per_dollar'] > 0
    
    def test_estimate_monthly_cost_high_volume(self):
        """Test monthly cost estimation for high volume."""
        tracker = CostTracker()
        
        job_config = BatchJobConfig(
            job_id="test_high_volume",
            job_name="High Volume Job",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at",
            estimated_daily_volume=10_000_000
        )
        
        estimate = tracker.estimate_monthly_cost(job_config)
        
        assert estimate['total'] > 0
        # High volume should have higher costs
        assert estimate['storage'] > 0
    
    def test_estimate_monthly_cost_low_volume(self):
        """Test monthly cost estimation for low volume."""
        tracker = CostTracker()
        
        job_config = BatchJobConfig(
            job_id="test_low_volume",
            job_name="Low Volume Job",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at",
            estimated_daily_volume=1000
        )
        
        estimate = tracker.estimate_monthly_cost(job_config)
        
        assert estimate['total'] > 0
        # Low volume should have lower costs
        assert estimate['storage'] >= 0
    
    def test_cost_breakdown_structure(self):
        """Test CostBreakdown model structure."""
        cost = CostBreakdown(
            embedding_compute_cost=0.01,
            storage_cost=0.05,
            vector_db_cost=0.02,
            warehouse_cost=0.01,
            total_cost=0.09,
            savings_from_cache=0.005
        )
        
        assert cost.embedding_compute_cost == 0.01
        assert cost.storage_cost == 0.05
        assert cost.vector_db_cost == 0.02
        assert cost.warehouse_cost == 0.01
        assert cost.savings_from_cache == 0.005
        assert cost.total_cost == 0.09
    
    def test_cost_tracker_constants(self):
        """Test that cost constants are defined."""
        tracker = CostTracker()
        
        # Check constants exist
        assert hasattr(CostTracker, 'CPU_HOUR_COST')
        assert hasattr(CostTracker, 'GPU_HOUR_COST')
        assert hasattr(CostTracker, 'STORAGE_GB_MONTH')
        assert hasattr(CostTracker, 'VECTOR_DB_GB_MONTH')
        
        assert CostTracker.CPU_HOUR_COST >= 0
        assert CostTracker.GPU_HOUR_COST >= 0
        assert CostTracker.STORAGE_GB_MONTH >= 0
        assert CostTracker.VECTOR_DB_GB_MONTH >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

