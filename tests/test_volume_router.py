"""
Unit tests for volume-based routing functionality.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.volume_router import VolumeRouter, VOLUME_THRESHOLD
from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger


class TestVolumeRouter:
    """Test cases for VolumeRouter class."""
    
    def test_initialization_default_threshold(self):
        """Test router initialization with default threshold."""
        router = VolumeRouter()
        assert router.threshold == VOLUME_THRESHOLD
        assert router.threshold == 10_000_000
    
    def test_initialization_custom_threshold(self):
        """Test router initialization with custom threshold."""
        router = VolumeRouter(threshold=5_000_000)
        assert router.threshold == 5_000_000
    
    def test_determine_sink_high_volume(self):
        """Test routing high volume jobs to Hudi."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_high",
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
            estimated_daily_volume=15_000_000
        )
        
        result = router.determine_sink(config)
        assert result == "hudi"
    
    def test_determine_sink_low_volume(self):
        """Test routing low volume jobs to Iceberg."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_low",
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
            estimated_daily_volume=5_000_000
        )
        
        result = router.determine_sink(config)
        assert result == "iceberg"
    
    def test_determine_sink_at_threshold(self):
        """Test routing at threshold boundary."""
        router = VolumeRouter()
        # Exactly at threshold should go to iceberg (< threshold)
        config = BatchJobConfig(
            job_id="test_threshold",
            job_name="Threshold Job",
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
            estimated_daily_volume=VOLUME_THRESHOLD
        )
        
        result = router.determine_sink(config)
        assert result == "iceberg"  # <= threshold goes to iceberg
    
    def test_determine_sink_above_threshold(self):
        """Test routing just above threshold."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_above",
            job_name="Above Threshold Job",
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
            estimated_daily_volume=VOLUME_THRESHOLD + 1
        )
        
        result = router.determine_sink(config)
        assert result == "hudi"
    
    def test_determine_sink_no_volume(self):
        """Test routing when volume is not specified (defaults to Iceberg)."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_no_volume",
            job_name="No Volume Job",
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
            estimated_daily_volume=None
        )
        
        result = router.determine_sink(config)
        assert result == "iceberg"
    
    def test_determine_sink_force_hudi(self):
        """Test manual override to force Hudi."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_force",
            job_name="Force Hudi Job",
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
            estimated_daily_volume=1_000_000,  # Low volume
            force_sink_type="hudi"
        )
        
        result = router.determine_sink(config)
        assert result == "hudi"
    
    def test_determine_sink_force_iceberg(self):
        """Test manual override to force Iceberg."""
        router = VolumeRouter()
        config = BatchJobConfig(
            job_id="test_force_iceberg",
            job_name="Force Iceberg Job",
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
            estimated_daily_volume=20_000_000,  # High volume
            force_sink_type="iceberg"
        )
        
        result = router.determine_sink(config)
        assert result == "iceberg"
    
    @patch('src.hudi_writer.HudiWriter')
    def test_get_writer_instance_hudi(self, mock_hudi_writer):
        """Test getting HudiWriter instance."""
        router = VolumeRouter()
        mock_instance = MagicMock()
        mock_hudi_writer.return_value = mock_instance
        
        writer = router.get_writer_instance("hudi")
        
        assert writer == mock_instance
        mock_hudi_writer.assert_called_once()
    
    @patch('src.lake.iceberg_writer.IcebergWriter')
    def test_get_writer_instance_iceberg(self, mock_iceberg_writer):
        """Test getting IcebergWriter instance."""
        router = VolumeRouter()
        mock_instance = MagicMock()
        mock_iceberg_writer.return_value = mock_instance
        
        writer = router.get_writer_instance("iceberg")
        
        assert writer == mock_instance
        mock_iceberg_writer.assert_called_once()
    
    def test_get_writer_instance_invalid(self):
        """Test getting writer with invalid sink type."""
        router = VolumeRouter()
        
        with pytest.raises(ValueError, match="Unknown sink type"):
            router.get_writer_instance("invalid")
    
    @patch('src.hudi_writer.HudiWriter')
    def test_get_writer_instance_hudi_import_error(self, mock_hudi_writer):
        """Test HudiWriter import error handling."""
        router = VolumeRouter()
        mock_hudi_writer.side_effect = ImportError("Hudi not available")
        
        with pytest.raises(ImportError, match="HudiWriter not available"):
            router.get_writer_instance("hudi")
    
    @patch('src.lake.iceberg_writer.IcebergWriter')
    def test_get_writer_instance_iceberg_import_error(self, mock_iceberg_writer):
        """Test IcebergWriter import error handling."""
        router = VolumeRouter()
        mock_iceberg_writer.side_effect = ImportError("Iceberg not available")
        
        with pytest.raises(ImportError, match="IcebergWriter not available"):
            router.get_writer_instance("iceberg")
    
    def test_custom_threshold_routing(self):
        """Test routing with custom threshold."""
        router = VolumeRouter(threshold=5_000_000)
        
        # Below custom threshold
        config_low = BatchJobConfig(
            job_id="test_custom_low",
            job_name="Custom Low",
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
            estimated_daily_volume=3_000_000
        )
        assert router.determine_sink(config_low) == "iceberg"
        
        # Above custom threshold
        config_high = BatchJobConfig(
            job_id="test_custom_high",
            job_name="Custom High",
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
            estimated_daily_volume=7_000_000
        )
        assert router.determine_sink(config_high) == "hudi"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

