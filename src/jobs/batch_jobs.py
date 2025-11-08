"""
Batch job processor for handling scheduled ETL jobs.

Enhanced batch processor with volume routing and dual writes.
"""

import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import logging

from .models import BatchJobConfig, JobResult, JobStatus
from ..etl import ETLPipeline, create_pipeline_from_credentials
from ..hudi_writer import HudiWriter, HudiWriteConfig, HudiTableConfig
from ..core.volume_router import VolumeRouter
# Lazy imports to avoid dependency issues
# from ..embeddings.local_embedder import LocalEmbedder
from ..monitoring.cost_tracker import CostTracker
from ..quality.rules_engine import QualityRulesEngine
# from ..destinations.dual_writer import DualDestinationWriter

logger = logging.getLogger(__name__)


class BatchJobProcessor:
    """Enhanced batch processor with volume routing and dual writes."""
    
    def __init__(self):
        """Initialize batch job processor."""
        self.volume_router = VolumeRouter()
        self.embedder = None  # Lazy initialization (import when needed)
        self.cost_tracker = CostTracker()
        self.quality_engine = QualityRulesEngine()
        self.warehouse_writer = None
    
    def process_batch_job(self, job_config: BatchJobConfig) -> JobResult:
        """Process a batch job.
        
        Args:
            job_config: Batch job configuration
            
        Returns:
            Job execution result
        """
        execution_id = f"batch_{job_config.job_id}_{int(time.time())}"
        start_time = datetime.utcnow()
        
        try:
            # Determine sink type (Hudi vs Iceberg)
            sink_type = self.volume_router.determine_sink(job_config)
            self.warehouse_writer = self.volume_router.get_writer_instance(sink_type)
            logger.info(f"Using {sink_type} writer for job {job_config.job_id}")
            
            # Calculate date range for batch processing
            end_date = job_config.end_date or datetime.utcnow()
            start_date = job_config.start_date or (end_date - timedelta(days=job_config.lookback_days))
            
            # Build MongoDB query with date range
            mongo_query = job_config.query.copy()
            mongo_query[job_config.date_field] = {
                "$gte": start_date,
                "$lte": end_date
            }
            
            # Create ETL pipeline
            pipeline = create_pipeline_from_credentials(
                username=job_config.mongo_uri.split("://")[1].split(":")[0],
                password=job_config.mongo_uri.split(":")[2].split("@")[0],
                host=job_config.mongo_uri.split("@")[1].split(":")[0],
                port=int(job_config.mongo_uri.split(":")[-1].split("/")[0]),
                database=job_config.database,
                collection=job_config.collection,
                schema=job_config.schema
            )
            
            # Process data in batches
            total_records_processed = 0
            total_records_written = 0
            total_files_created = 0
            total_bytes_processed = 0
            
            # Calculate batch size based on data volume
            batch_size = min(job_config.batch_size, 100000)  # Cap at 100k records
            
            # Process data
            if job_config.use_spark:
                # Use Spark for large datasets
                spark_df, spark_session = pipeline.run_pipeline(
                    query=mongo_query,
                    limit=job_config.limit,
                    use_spark=True,
                    flatten=job_config.flatten_data,
                    clean=job_config.clean_data,
                    apply_schema=job_config.apply_schema
                )
                
                # Convert to pandas for Hudi writing (simplified approach)
                pandas_df = spark_df.toPandas()
                total_records_processed = len(pandas_df)
                
                # Write to Hudi
                write_result = self._write_to_hudi(pandas_df, job_config)
                total_records_written = write_result.records_written
                total_files_created = write_result.files_written
                total_bytes_processed = write_result.bytes_written
                
                spark_session.stop()
                
            else:
                # Use pandas for smaller datasets
                pandas_df = pipeline.run_pipeline(
                    query=mongo_query,
                    limit=job_config.limit,
                    use_spark=False,
                    flatten=job_config.flatten_data,
                    clean=job_config.clean_data,
                    apply_schema=job_config.apply_schema
                )
                
                total_records_processed = len(pandas_df)
                
                # Write to Hudi
                write_result = self._write_to_hudi(pandas_df, job_config)
                total_records_written = write_result.records_written
                total_files_created = write_result.files_written
                total_bytes_processed = write_result.bytes_written
            
            # Calculate execution time
            end_time = datetime.utcnow()
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Calculate performance metrics
            records_per_second = total_records_processed / duration_seconds if duration_seconds > 0 else 0
            
            return JobResult(
                job_id=job_config.job_id,
                execution_id=execution_id,
                status=JobStatus.SUCCESS,
                started_at=start_time,
                completed_at=end_time,
                duration_seconds=duration_seconds,
                records_processed=total_records_processed,
                records_written=total_records_written,
                files_created=total_files_created,
                bytes_processed=total_bytes_processed,
                records_per_second=records_per_second
            )
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration_seconds = (end_time - start_time).total_seconds()
            
            return JobResult(
                job_id=job_config.job_id,
                execution_id=execution_id,
                status=JobStatus.FAILED,
                started_at=start_time,
                completed_at=end_time,
                duration_seconds=duration_seconds,
                error_message=str(e),
                error_details={"exception_type": type(e).__name__}
            )
        
        finally:
            if self.warehouse_writer and hasattr(self.warehouse_writer, 'close'):
                self.warehouse_writer.close()
    
    def _write_to_warehouse(self, df: pd.DataFrame, job_config: BatchJobConfig) -> Any:
        """Write DataFrame to warehouse (Hudi or Iceberg).
        
        Args:
            df: DataFrame to write
            job_config: Job configuration
            
        Returns:
            Write result
        """
        if not self.warehouse_writer:
            raise ValueError("Warehouse writer not initialized")
        
        # Use appropriate writer based on type
        if isinstance(self.warehouse_writer, HudiWriter):
            # Hudi write
            table_config = HudiTableConfig(
                table_name=job_config.hudi_table_name,
                database=job_config.hudi_database,
                base_path=job_config.hudi_base_path,
                schema=job_config.schema or {},
                partition_field=job_config.partition_field
            )
            
            write_config = HudiWriteConfig(
                table_name=job_config.hudi_table_name,
                record_key_field="id",
                partition_field=job_config.partition_field,
                precombine_field="updated_at"
            )
            
            return self.warehouse_writer.write_dataframe(df, write_config, table_config)
        else:
            # Iceberg write
            return self.warehouse_writer.write_dataframe(
                df,
                table_name=job_config.hudi_table_name,
                mode="append"
            )
    
    def validate_batch_job(self, job_config: BatchJobConfig) -> Dict[str, Any]:
        """Validate batch job configuration.
        
        Args:
            job_config: Batch job configuration
            
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        # Validate required fields
        if not job_config.date_field:
            errors.append("Date field is required for batch jobs")
        
        if not job_config.hudi_table_name:
            errors.append("Hudi table name is required")
        
        if not job_config.hudi_base_path:
            errors.append("Hudi base path is required")
        
        # Validate date range
        if job_config.start_date and job_config.end_date:
            if job_config.start_date >= job_config.end_date:
                errors.append("Start date must be before end date")
        
        # Validate batch size
        if job_config.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if job_config.batch_size > 1000000:
            warnings.append("Large batch size may cause memory issues")
        
        # Validate MongoDB connection
        try:
            # Test MongoDB connection
            pipeline = create_pipeline_from_credentials(
                username=job_config.mongo_uri.split("://")[1].split(":")[0],
                password=job_config.mongo_uri.split(":")[2].split("@")[0],
                host=job_config.mongo_uri.split("@")[1].split(":")[0],
                port=int(job_config.mongo_uri.split(":")[-1].split("/")[0]),
                database=job_config.database,
                collection=job_config.collection
            )
            
            # Test read
            test_df = pipeline.mongo_reader.read_to_pandas(limit=1)
            if test_df.empty:
                warnings.append("MongoDB query returned no data")
                
        except Exception as e:
            errors.append(f"MongoDB connection failed: {str(e)}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
