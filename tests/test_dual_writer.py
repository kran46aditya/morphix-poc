"""
Unit tests for dual destination writer (Vector DB + Warehouse).
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger


class TestDualDestinationWriter:
    """Test cases for DualDestinationWriter class."""
    
    @pytest.mark.skip(reason="Requires vector DB and warehouse connections")
    def test_initialization(self):
        """Test writer initialization."""
        from src.destinations.dual_writer import DualDestinationWriter
        
        writer = DualDestinationWriter()
        assert writer is not None
    
    @pytest.mark.skip(reason="Requires vector DB and warehouse connections")
    def test_write_dual(self):
        """Test dual destination write."""
        from src.destinations.dual_writer import DualDestinationWriter
        
        writer = DualDestinationWriter()
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'text': ['First text', 'Second text', 'Third text']
        })
        
        job_config = BatchJobConfig(
            job_id="test_dual",
            job_name="Test Dual Write",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at"
        )
        
        result = writer.write_dual(
            df=df,
            job_config=job_config,
            text_column="text"
        )
        
        assert result is not None
        assert hasattr(result, 'vector_db_success')
        assert hasattr(result, 'warehouse_success')
    
    @patch('src.destinations.dual_writer.LocalEmbedder')
    @patch('src.destinations.dual_writer.HudiWriter')
    def test_write_dual_with_mocks(self, mock_hudi, mock_embedder):
        """Test dual write with mocked dependencies."""
        from src.destinations.dual_writer import DualDestinationWriter
        
        # Setup mocks
        mock_embedder_instance = MagicMock()
        mock_embedder.return_value = mock_embedder_instance
        mock_embedder_instance.embed_batch.return_value = [
            np.array([0.1, 0.2, 0.3]),
            np.array([0.4, 0.5, 0.6])
        ]
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.return_value = MagicMock(rows_written=2)
        
        writer = DualDestinationWriter()
        df = pd.DataFrame({
            'id': [1, 2],
            'text': ['First', 'Second']
        })
        
        job_config = BatchJobConfig(
            job_id="test",
            job_name="Test",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at"
        )
        
        result = writer.write_dual(
            df=df,
            job_config=job_config,
            text_column="text"
        )
        
        assert result is not None
        mock_embedder_instance.embed_batch.assert_called_once()
    
    @patch('src.destinations.dual_writer.LocalEmbedder')
    @patch('src.destinations.dual_writer.HudiWriter')
    def test_write_dual_vector_db_failure(self, mock_hudi, mock_embedder):
        """Test dual write when vector DB fails."""
        from src.destinations.dual_writer import DualDestinationWriter
        
        # Setup mocks - vector DB fails
        mock_embedder_instance = MagicMock()
        mock_embedder.return_value = mock_embedder_instance
        mock_embedder_instance.embed_batch.side_effect = Exception("Vector DB error")
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.return_value = MagicMock(rows_written=2)
        
        writer = DualDestinationWriter()
        df = pd.DataFrame({
            'id': [1, 2],
            'text': ['First', 'Second']
        })
        
        job_config = BatchJobConfig(
            job_id="test",
            job_name="Test",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at"
        )
        
        result = writer.write_dual(
            df=df,
            job_config=job_config,
            text_column="text"
        )
        
        assert result is not None
        assert result.vector_db_success is False
        assert result.warehouse_success is True  # Warehouse should still succeed
    
    @patch('src.destinations.dual_writer.LocalEmbedder')
    @patch('src.destinations.dual_writer.HudiWriter')
    def test_write_dual_warehouse_failure(self, mock_hudi, mock_embedder):
        """Test dual write when warehouse fails."""
        from src.destinations.dual_writer import DualDestinationWriter
        
        # Setup mocks - warehouse fails
        mock_embedder_instance = MagicMock()
        mock_embedder.return_value = mock_embedder_instance
        mock_embedder_instance.embed_batch.return_value = [
            np.array([0.1, 0.2, 0.3])
        ]
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.side_effect = Exception("Warehouse error")
        
        writer = DualDestinationWriter()
        df = pd.DataFrame({
            'id': [1],
            'text': ['First']
        })
        
        job_config = BatchJobConfig(
            job_id="test",
            job_name="Test",
            job_type="batch",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test",
            collection="test",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/test",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL),
            created_by="test",
            date_field="created_at"
        )
        
        result = writer.write_dual(
            df=df,
            job_config=job_config,
            text_column="text"
        )
        
        assert result is not None
        assert result.vector_db_success is True
        assert result.warehouse_success is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

