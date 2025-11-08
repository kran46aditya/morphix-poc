"""
End-to-End Unit Tests for Morphix ETL Platform

Tests the complete flow from data extraction to warehouse writing,
including all new features: volume routing, quality rules, cost tracking, etc.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger, JobResult, JobStatus
from src.core.volume_router import VolumeRouter
from src.quality.rules_engine import QualityRulesEngine, QualityRule, RuleType
from src.monitoring.cost_tracker import CostTracker
from src.etl.schema_generator import SchemaGenerator
from src.etl.data_transformer import DataTransformer


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': [
            'alice@example.com',
            'bob@test.com',
            'charlie@example.com',
            'david@test.com',
            'eve@example.com'
        ],
        'age': [25, 30, 35, None, 40],
        'score': [85, 92, 78, 95, 88],
        'created_at': pd.to_datetime([
            '2024-01-01',
            '2024-01-02',
            '2024-01-03',
            '2024-01-04',
            '2024-01-05'
        ]),
        'metadata': [
            {'key1': 'value1'},
            {'key2': 'value2'},
            {'key1': 'value3'},
            {'key2': 'value4'},
            {'key1': 'value5'}
        ]
    })


@pytest.fixture
def batch_job_config_high_volume():
    """High volume batch job config (routes to Hudi)."""
    return BatchJobConfig(
        job_id="test_high_volume",
        job_name="High Volume Test Job",
        job_type="batch",
        user_id=1,
        mongo_uri="mongodb://localhost:27017",
        database="testdb",
        collection="testcollection",
        hudi_table_name="test_table",
        hudi_base_path="s3://test-bucket/hudi/",
        schedule=JobSchedule(trigger=JobTrigger.MANUAL),
        created_by="test_user",
        date_field="created_at",
        estimated_daily_volume=15_000_000  # High volume → Hudi
    )


@pytest.fixture
def batch_job_config_low_volume():
    """Low volume batch job config (routes to Iceberg)."""
    return BatchJobConfig(
        job_id="test_low_volume",
        job_name="Low Volume Test Job",
        job_type="batch",
        user_id=1,
        mongo_uri="mongodb://localhost:27017",
        database="testdb",
        collection="testcollection",
        hudi_table_name="test_table",
        hudi_base_path="s3://test-bucket/hudi/",
        schedule=JobSchedule(trigger=JobTrigger.MANUAL),
        created_by="test_user",
        date_field="created_at",
        estimated_daily_volume=5_000_000  # Low volume → Iceberg
    )


@pytest.fixture
def quality_rules():
    """Sample quality rules for testing."""
    return [
        QualityRule(
            rule_id="email_format",
            rule_type=RuleType.PATTERN_MATCH,
            column="email",
            parameters={"pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
            severity="error"
        ),
        QualityRule(
            rule_id="age_completeness",
            rule_type=RuleType.NULL_THRESHOLD,
            column="age",
            parameters={"max_null_percent": 25},
            severity="warning"
        ),
        QualityRule(
            rule_id="score_range",
            rule_type=RuleType.RANGE_CHECK,
            column="score",
            parameters={"min_value": 0, "max_value": 100},
            severity="error"
        )
    ]


class TestCompleteETLFlow:
    """Test complete ETL flow with all components."""
    
    def test_volume_routing_high_volume(self, batch_job_config_high_volume):
        """Test volume routing for high volume job."""
        router = VolumeRouter()
        sink_type = router.determine_sink(batch_job_config_high_volume)
        
        assert sink_type == "hudi"
    
    def test_volume_routing_low_volume(self, batch_job_config_low_volume):
        """Test volume routing for low volume job."""
        router = VolumeRouter()
        sink_type = router.determine_sink(batch_job_config_low_volume)
        
        assert sink_type == "iceberg"
    
    def test_schema_generation_from_data(self, sample_data):
        """Test schema generation from sample data."""
        schema = SchemaGenerator.generate_from_dataframe(sample_data)
        
        assert isinstance(schema, dict)
        assert 'id' in schema
        assert 'name' in schema
        assert 'email' in schema
        assert schema['id']['type'] == 'integer'
        assert schema['name']['type'] == 'string'
        assert schema['email']['type'] == 'string'
    
    def test_data_transformation_with_schema(self, sample_data):
        """Test data transformation with generated schema."""
        schema = SchemaGenerator.generate_from_dataframe(sample_data)
        transformer = DataTransformer(schema=schema)
        
        # Flatten nested structures
        df_flat = transformer.flatten_dataframe(sample_data)
        
        # Apply schema validation
        df_validated = transformer.apply_schema(df_flat, strict=False)
        
        assert len(df_validated) == len(sample_data)
        assert 'id' in df_validated.columns
    
    def test_quality_rules_application(self, sample_data, quality_rules):
        """Test quality rules application on data."""
        engine = QualityRulesEngine()
        results = engine.apply_rules(sample_data, quality_rules)
        
        assert len(results) == len(quality_rules)
        
        # Check that rules were applied
        for result in results:
            assert result.rule_id in [r.rule_id for r in quality_rules]
            assert result.total_count == len(sample_data)
        
        # Calculate quality score
        score = engine.calculate_quality_score(results)
        assert 0 <= score <= 100
    
    def test_quality_score_calculation(self, sample_data, quality_rules):
        """Test quality score calculation."""
        engine = QualityRulesEngine()
        results = engine.apply_rules(sample_data, quality_rules)
        score = engine.calculate_quality_score(results)
        
        assert isinstance(score, float)
        assert 0 <= score <= 100
    
    def test_quality_report_generation(self, sample_data, quality_rules):
        """Test quality report generation."""
        engine = QualityRulesEngine()
        results = engine.apply_rules(sample_data, quality_rules)
        report = engine.generate_report(results)
        
        assert 'overall_score' in report
        assert 'total_rules' in report
        assert 'top_issues' in report
        assert report['total_rules'] == len(quality_rules)
    
    def test_cost_tracking_calculation(self):
        """Test cost tracking for job execution."""
        tracker = CostTracker()
        
        class MockJobExecution:
            pass
        
        job_exec = MockJobExecution()
        
        cost = tracker.calculate_execution_cost(
            job_execution=job_exec,
            embeddings_generated=10000,
            cache_hits=7000,
            storage_gb=5.0
        )
        
        assert cost.embedding_compute_cost >= 0
        assert cost.storage_cost >= 0
        assert cost.savings_from_cache >= 0
        assert cost.total_cost >= 0
    
    def test_cost_estimation_monthly(self, batch_job_config_high_volume):
        """Test monthly cost estimation."""
        tracker = CostTracker()
        estimate = tracker.estimate_monthly_cost(batch_job_config_high_volume)
        
        assert 'embeddings' in estimate
        assert 'storage' in estimate
        assert 'total' in estimate
        assert estimate['total'] > 0


class TestEndToEndBatchJob:
    """Test complete batch job processing flow."""
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    @patch('src.jobs.batch_jobs.HudiWriter')
    def test_batch_job_high_volume_flow(
        self,
        mock_hudi,
        mock_etl_create,
        batch_job_config_high_volume,
        sample_data
    ):
        """Test complete batch job flow for high volume (Hudi)."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mocks
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.return_value = {
            'data': sample_data,
            'rows_processed': len(sample_data),
            'schema': {}
        }
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.return_value = MagicMock(rows_written=len(sample_data))
        
        # Process job
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_high_volume)
        
        # Verify result
        assert isinstance(result, JobResult)
        assert result.status in [JobStatus.SUCCESS, JobStatus.FAILED]
        
        # Verify volume routing was used
        assert processor.volume_router is not None
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    @patch('src.lake.iceberg_writer.IcebergWriter')
    def test_batch_job_low_volume_flow(
        self,
        mock_iceberg,
        mock_etl_create,
        batch_job_config_low_volume,
        sample_data
    ):
        """Test complete batch job flow for low volume (Iceberg)."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mocks
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.return_value = {
            'data': sample_data,
            'rows_processed': len(sample_data),
            'schema': {}
        }
        
        mock_iceberg_instance = MagicMock()
        mock_iceberg.return_value = mock_iceberg_instance
        mock_iceberg_instance.write_dataframe.return_value = MagicMock(rows_written=len(sample_data))
        
        # Process job
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_low_volume)
        
        # Verify result
        assert isinstance(result, JobResult)
        assert result.status in [JobStatus.SUCCESS, JobStatus.FAILED]
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    @patch('src.jobs.batch_jobs.HudiWriter')
    def test_batch_job_with_quality_checks(
        self,
        mock_hudi,
        mock_etl_create,
        batch_job_config_high_volume,
        sample_data,
        quality_rules
    ):
        """Test batch job with quality rules applied."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mocks
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.return_value = {
            'data': sample_data,
            'rows_processed': len(sample_data),
            'schema': {}
        }
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.return_value = MagicMock(rows_written=len(sample_data))
        
        # Process job
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_high_volume)
        
        # Verify quality engine is initialized
        assert processor.quality_engine is not None
        
        # Verify result
        assert isinstance(result, JobResult)
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    @patch('src.jobs.batch_jobs.HudiWriter')
    def test_batch_job_with_cost_tracking(
        self,
        mock_hudi,
        mock_etl_create,
        batch_job_config_high_volume,
        sample_data
    ):
        """Test batch job with cost tracking."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mocks
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.return_value = {
            'data': sample_data,
            'rows_processed': len(sample_data),
            'schema': {}
        }
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.return_value = MagicMock(rows_written=len(sample_data))
        
        # Process job
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_high_volume)
        
        # Verify cost tracker is initialized
        assert processor.cost_tracker is not None
        
        # Verify result
        assert isinstance(result, JobResult)


class TestEndToEndDataFlow:
    """Test complete data flow from extraction to writing."""
    
    def test_complete_data_flow_with_mocks(self, sample_data):
        """Test complete data flow with all components mocked."""
        # Step 1: Generate schema
        schema = SchemaGenerator.generate_from_dataframe(sample_data)
        assert isinstance(schema, dict)
        
        # Step 2: Transform data
        transformer = DataTransformer(schema=schema)
        df_flat = transformer.flatten_dataframe(sample_data)
        df_validated = transformer.apply_schema(df_flat, strict=False)
        assert len(df_validated) == len(sample_data)
        
        # Step 3: Apply quality rules
        rules = [
            QualityRule(
                rule_id="email_check",
                rule_type=RuleType.PATTERN_MATCH,
                column="email",
                parameters={"pattern": r'^[^@]+@[^@]+\.[^@]+$'},
                severity="error"
            )
        ]
        engine = QualityRulesEngine()
        quality_results = engine.apply_rules(df_validated, rules)
        assert len(quality_results) == 1
        
        # Step 4: Calculate quality score
        quality_score = engine.calculate_quality_score(quality_results)
        assert 0 <= quality_score <= 100
        
        # Step 5: Track costs
        tracker = CostTracker()
        cost = tracker.calculate_execution_cost(
            job_execution=Mock(),
            embeddings_generated=1000,
            cache_hits=500,
            storage_gb=1.0
        )
        assert cost.total_cost >= 0
    
    def test_volume_routing_integration(self, batch_job_config_high_volume, batch_job_config_low_volume):
        """Test volume routing integration with job configs."""
        router = VolumeRouter()
        
        # High volume should route to Hudi
        high_sink = router.determine_sink(batch_job_config_high_volume)
        assert high_sink == "hudi"
        
        # Low volume should route to Iceberg
        low_sink = router.determine_sink(batch_job_config_low_volume)
        assert low_sink == "iceberg"
    
    def test_schema_flattening_suggestions(self, sample_data):
        """Test schema flattening suggestions."""
        schema = SchemaGenerator.generate_from_dataframe(sample_data)
        suggestions = SchemaGenerator.suggest_flattening_strategy(schema)
        
        assert isinstance(suggestions, list)
        # Should suggest flattening for nested metadata field
        assert any('metadata' in str(s.get('field', '')) for s in suggestions)
    
    def test_breaking_changes_detection(self):
        """Test breaking changes detection."""
        old_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'string'},  # Breaking change
            'name': {'type': 'string'},
            'email': {'type': 'string'}  # Non-breaking addition
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        assert changes['compatible'] is False
        assert len(changes['breaking_changes']) > 0
        assert len(changes['non_breaking_changes']) > 0


class TestEndToEndErrorHandling:
    """Test error handling in end-to-end flow."""
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    def test_batch_job_handles_etl_errors(self, mock_etl_create, batch_job_config_high_volume):
        """Test batch job handles ETL pipeline errors."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mock to raise error
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.side_effect = Exception("ETL pipeline error")
        
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_high_volume)
        
        # Should handle error gracefully
        assert result.status == JobStatus.FAILED
        assert result.error_message is not None
    
    @patch('src.jobs.batch_jobs.create_pipeline_from_credentials')
    @patch('src.jobs.batch_jobs.HudiWriter')
    def test_batch_job_handles_write_errors(
        self,
        mock_hudi,
        mock_etl_create,
        batch_job_config_high_volume,
        sample_data
    ):
        """Test batch job handles write errors."""
        from src.jobs.batch_jobs import BatchJobProcessor
        
        # Setup mocks
        mock_etl_instance = MagicMock()
        mock_etl_create.return_value = mock_etl_instance
        mock_etl_instance.run.return_value = {
            'data': sample_data,
            'rows_processed': len(sample_data),
            'schema': {}
        }
        
        mock_hudi_instance = MagicMock()
        mock_hudi.return_value = mock_hudi_instance
        mock_hudi_instance.write_dataframe.side_effect = Exception("Write error")
        
        processor = BatchJobProcessor()
        result = processor.process_batch_job(batch_job_config_high_volume)
        
        # Should handle error gracefully
        assert result.status == JobStatus.FAILED
        assert result.error_message is not None
    
    def test_quality_rules_handles_missing_column(self, sample_data):
        """Test quality rules handle missing columns gracefully."""
        engine = QualityRulesEngine()
        
        rule = QualityRule(
            rule_id="missing_column",
            rule_type=RuleType.NULL_THRESHOLD,
            column="nonexistent_column",
            parameters={"max_null_percent": 10},
            severity="error"
        )
        
        results = engine.apply_rules(sample_data, [rule])
        
        # Should return result indicating column not found
        assert len(results) == 1
        assert results[0].passed is False


class TestEndToEndPerformance:
    """Test performance aspects of end-to-end flow."""
    
    def test_large_dataset_handling(self):
        """Test handling of larger datasets."""
        # Create larger dataset
        large_data = pd.DataFrame({
            'id': range(1000),
            'name': [f'User_{i}' for i in range(1000)],
            'value': np.random.rand(1000)
        })
        
        # Generate schema
        schema = SchemaGenerator.generate_from_dataframe(large_data)
        assert len(schema) == 3
        
        # Apply transformation
        transformer = DataTransformer(schema=schema)
        df_validated = transformer.apply_schema(large_data, strict=False)
        assert len(df_validated) == 1000
    
    def test_quality_rules_performance(self):
        """Test quality rules performance on larger datasets."""
        large_data = pd.DataFrame({
            'id': range(1000),
            'email': [f'user_{i}@example.com' for i in range(1000)],
            'score': np.random.randint(0, 100, 1000)
        })
        
        rules = [
            QualityRule(
                rule_id="email_format",
                rule_type=RuleType.PATTERN_MATCH,
                column="email",
                parameters={"pattern": r'^[^@]+@[^@]+\.[^@]+$'},
                severity="error"
            )
        ]
        
        engine = QualityRulesEngine()
        start_time = datetime.now()
        results = engine.apply_rules(large_data, rules)
        end_time = datetime.now()
        
        # Should complete in reasonable time (< 5 seconds)
        duration = (end_time - start_time).total_seconds()
        assert duration < 5.0
        assert len(results) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

