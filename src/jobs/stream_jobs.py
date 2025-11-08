"""
Stream job processor for handling real-time ETL jobs.

Real-time processing using MongoDB change streams.
NOT polling - use native change streams for sub-second latency.
"""

import time
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import pymongo
from pymongo.errors import PyMongoError
import logging

from .models import StreamJobConfig, JobResult, JobStatus
from ..etl import ETLPipeline, create_pipeline_from_credentials
from ..hudi_writer import HudiWriter, HudiWriteConfig, HudiTableConfig
from ..core.volume_router import VolumeRouter
from ..quality.rules_engine import QualityRulesEngine

logger = logging.getLogger(__name__)


class StreamJobProcessor:
    """Processor for stream ETL jobs."""
    
    def __init__(self):
        """Initialize stream job processor."""
        self.hudi_writer = None
        self.running_jobs = {}
        self.stop_events = {}
    
    def start_stream_job(self, job_config: StreamJobConfig) -> str:
        """Start a stream job.
        
        Args:
            job_config: Stream job configuration
            
        Returns:
            Execution ID
        """
        execution_id = f"stream_{job_config.job_id}_{int(time.time())}"
        
        # Create stop event for this job
        stop_event = threading.Event()
        self.stop_events[execution_id] = stop_event
        
        # Start processing thread
        thread = threading.Thread(
            target=self._process_stream_job,
            args=(job_config, execution_id, stop_event)
        )
        thread.daemon = True
        thread.start()
        
        # Store running job info
        self.running_jobs[execution_id] = {
            "job_config": job_config,
            "thread": thread,
            "started_at": datetime.utcnow(),
            "status": JobStatus.RUNNING
        }
        
        return execution_id
    
    def stop_stream_job(self, execution_id: str) -> bool:
        """Stop a stream job.
        
        Args:
            execution_id: Execution ID
            
        Returns:
            True if stopped successfully
        """
        if execution_id in self.stop_events:
            self.stop_events[execution_id].set()
            
            if execution_id in self.running_jobs:
                self.running_jobs[execution_id]["status"] = JobStatus.CANCELLED
            
            return True
        
        return False
    
    def get_stream_job_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get stream job status.
        
        Args:
            execution_id: Execution ID
            
        Returns:
            Job status information
        """
        if execution_id not in self.running_jobs:
            return None
        
        job_info = self.running_jobs[execution_id]
        return {
            "execution_id": execution_id,
            "status": job_info["status"].value,
            "started_at": job_info["started_at"],
            "is_running": job_info["thread"].is_alive()
        }
    
    def _process_stream_job(self, job_config: StreamJobConfig, execution_id: str, stop_event: threading.Event):
        """Process stream job using MongoDB change streams.
        
        Args:
            job_config: Stream job configuration
            execution_id: Execution ID
            stop_event: Stop event for graceful shutdown
        """
        try:
            # Initialize components
            volume_router = VolumeRouter()
            sink_type = volume_router.determine_sink(job_config)
            warehouse_writer = volume_router.get_writer_instance(sink_type)
            
            # Connect to MongoDB
            client = pymongo.MongoClient(job_config.mongo_uri)
            db = client[job_config.database]
            collection = db[job_config.collection]
            
            # Start change stream
            self.start_change_stream(
                collection,
                job_config,
                execution_id,
                stop_event,
                warehouse_writer
            )
            
            # Job completed
            self.running_jobs[execution_id]["status"] = JobStatus.SUCCESS
            
        except Exception as e:
            logger.error(f"Fatal error in stream job {execution_id}: {str(e)}")
            self.running_jobs[execution_id]["status"] = JobStatus.FAILED
        
        finally:
            if hasattr(self, 'hudi_writer') and self.hudi_writer:
                self.hudi_writer.close()
    
    def start_change_stream(
        self,
        collection,
        job_config: StreamJobConfig,
        execution_id: str,
        stop_event: threading.Event,
        warehouse_writer
    ):
        """
        Open MongoDB change stream.
        
        Implementation:
        1. Connect to MongoDB with change stream support
        2. Open change stream on specified collection
        3. Start from resume_token if provided (for crash recovery)
        4. Process changes in micro-batches (100 docs)
        5. Save resume token every 1000 docs to PostgreSQL
        6. Handle: insert, update, delete, replace operations
        
        Change stream options:
        - full_document='updateLookup' (get full doc on update)
        - batch_size=100
        - max_await_time_ms=1000
        """
        try:
            # Build change stream pipeline
            pipeline = self._build_change_stream_pipeline(job_config)
            
            # Resume token if available
            resume_token = job_config.resume_token
            
            # Open change stream
            with collection.watch(
                pipeline=pipeline,
                full_document='updateLookup',
                resume_after=resume_token,
                batch_size=100,
                max_await_time_ms=1000
            ) as stream:
                batch = []
                batch_count = 0
                
                for change in stream:
                    if stop_event.is_set():
                        break
                    
                    # Process change event
                    processed = self._process_change_event(change, job_config)
                    if processed:
                        batch.append(processed)
                    
                    # Write batch when it reaches size
                    if len(batch) >= job_config.batch_size:
                        self._write_batch(batch, job_config, warehouse_writer)
                        batch_count += len(batch)
                        batch = []
                        
                        # Save resume token periodically
                        if batch_count % 1000 == 0:
                            self._save_resume_token(execution_id, stream.resume_token)
                
                # Write remaining batch
                if batch:
                    self._write_batch(batch, job_config, warehouse_writer)
                    self._save_resume_token(execution_id, stream.resume_token)
                    
        except PyMongoError as e:
            logger.error(f"MongoDB error in change stream: {e}")
            # Handle connection errors, save state, retry
            raise
        except Exception as e:
            logger.error(f"Error in change stream: {e}")
            raise
    
    def _build_change_stream_pipeline(self, job_config: StreamJobConfig) -> list:
        """Build MongoDB change stream pipeline.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Change stream pipeline
        """
        pipeline = []
        
        # Add query filter if provided
        if job_config.query:
            pipeline.append({"$match": job_config.query})
        
        return pipeline
    
    def _process_change_event(
        self, 
        change: dict, 
        job_config: StreamJobConfig
    ) -> Optional[dict]:
        """
        Transform single change event.
        
        Handle operation types:
        - insert: process full document
        - update: get updated fields + full doc
        - replace: process replacement doc
        - delete: mark for deletion in destinations
        
        Args:
            change: Change stream event
            job_config: Job configuration
            
        Returns:
            Processed record or None
        """
        operation = change.get('operationType')
        
        if operation == 'insert':
            doc = change.get('fullDocument')
            if doc:
                return self._transform_document(doc, job_config)
        
        elif operation in ['update', 'replace']:
            doc = change.get('fullDocument')  # updateLookup gives full doc
            if doc:
                return self._transform_document(doc, job_config)
        
        elif operation == 'delete':
            return {
                '_id': change.get('documentKey', {}).get('_id'),
                '_deleted': True
            }
        
        return None
    
    def _transform_document(self, doc: dict, job_config: StreamJobConfig) -> dict:
        """Transform document according to job config.
        
        Args:
            doc: MongoDB document
            job_config: Job configuration
            
        Returns:
            Transformed document
        """
        # Apply transformations if needed
        # This is a placeholder - actual transformation would use ETL pipeline
        return doc
    
    def _write_batch(
        self,
        batch: list,
        job_config: StreamJobConfig,
        warehouse_writer
    ):
        """Write batch to warehouse.
        
        Args:
            batch: List of processed records
            job_config: Job configuration
            warehouse_writer: Warehouse writer instance
        """
        if not batch:
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(batch)
            
            # Write to warehouse
            if hasattr(warehouse_writer, 'write_dataframe'):
                result = warehouse_writer.write_dataframe(
                    df,
                    table_name=job_config.hudi_table_name
                )
                logger.info(f"Wrote {len(batch)} records to warehouse")
            else:
                logger.error("Warehouse writer does not support write_dataframe")
                
        except Exception as e:
            logger.error(f"Error writing batch: {e}")
    
    def _save_resume_token(self, execution_id: str, token: dict):
        """Save resume token to PostgreSQL for crash recovery.
        
        Args:
            execution_id: Execution ID
            token: Resume token from change stream
        """
        try:
            from ..postgres.credentials import _get_conn
            import json
            
            conn = _get_conn()
            cursor = conn.cursor()
            
            # Store in job_executions table (would need to be created)
            # This is a placeholder - actual implementation would update job_executions table
            logger.debug(f"Saving resume token for execution {execution_id}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"Failed to save resume token: {e}")
    
    def _process_stream_batch(self, pipeline: ETLPipeline, job_config: StreamJobConfig) -> Dict[str, Any]:
        """Process a single batch in stream job.
        
        Args:
            pipeline: ETL pipeline
            job_config: Job configuration
            
        Returns:
            Processing result
        """
        try:
            # Build query for recent data
            mongo_query = job_config.query.copy()
            if job_config.real_time_processing:
                # Query for recent data based on polling interval
                recent_time = datetime.utcnow() - timedelta(seconds=job_config.polling_interval_seconds)
                mongo_query["updated_at"] = {"$gte": recent_time}
            
            # Process data
            if job_config.use_spark:
                # Use Spark for large datasets
                spark_df, spark_session = pipeline.run_pipeline(
                    query=mongo_query,
                    limit=job_config.batch_size,
                    use_spark=True,
                    flatten=job_config.flatten_data,
                    clean=job_config.clean_data,
                    apply_schema=job_config.apply_schema
                )
                
                # Convert to pandas for Hudi writing
                pandas_df = spark_df.toPandas()
                records_processed = len(pandas_df)
                
                # Write to Hudi
                if records_processed > 0:
                    write_result = self._write_to_hudi(pandas_df, job_config)
                    records_written = write_result.records_written
                else:
                    records_written = 0
                
                spark_session.stop()
                
            else:
                # Use pandas for smaller datasets
                pandas_df = pipeline.run_pipeline(
                    query=mongo_query,
                    limit=job_config.batch_size,
                    use_spark=False,
                    flatten=job_config.flatten_data,
                    clean=job_config.clean_data,
                    apply_schema=job_config.apply_schema
                )
                
                records_processed = len(pandas_df)
                
                # Write to Hudi
                if records_processed > 0:
                    write_result = self._write_to_hudi(pandas_df, job_config)
                    records_written = write_result.records_written
                else:
                    records_written = 0
            
            return {
                "records_processed": records_processed,
                "records_written": records_written,
                "success": True
            }
            
        except Exception as e:
            return {
                "records_processed": 0,
                "records_written": 0,
                "success": False,
                "error": str(e)
            }
    
    def _write_to_hudi(self, df: pd.DataFrame, job_config: StreamJobConfig) -> Any:
        """Write DataFrame to Hudi table.
        
        Args:
            df: DataFrame to write
            job_config: Job configuration
            
        Returns:
            Write result
        """
        # Create Hudi table configuration
        table_config = HudiTableConfig(
            table_name=job_config.hudi_table_name,
            database=job_config.hudi_database,
            base_path=job_config.hudi_base_path,
            schema=job_config.schema or {},
            partition_field=job_config.partition_field
        )
        
        # Create Hudi write configuration
        write_config = HudiWriteConfig(
            table_name=job_config.hudi_table_name,
            record_key_field="id",
            partition_field=job_config.partition_field,
            precombine_field="updated_at"
        )
        
        # Write DataFrame
        return self.hudi_writer.write_dataframe(df, write_config, table_config)
    
    def validate_stream_job(self, job_config: StreamJobConfig) -> Dict[str, Any]:
        """Validate stream job configuration.
        
        Args:
            job_config: Stream job configuration
            
        Returns:
            Validation result
        """
        errors = []
        warnings = []
        
        # Validate required fields
        if not job_config.hudi_table_name:
            errors.append("Hudi table name is required")
        
        if not job_config.hudi_base_path:
            errors.append("Hudi base path is required")
        
        # Validate polling interval
        if job_config.polling_interval_seconds <= 0:
            errors.append("Polling interval must be positive")
        
        if job_config.polling_interval_seconds < 10:
            warnings.append("Very short polling interval may cause performance issues")
        
        # Validate batch size
        if job_config.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if job_config.batch_size > 100000:
            warnings.append("Large batch size may cause memory issues in stream processing")
        
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
    
    def cleanup_completed_jobs(self):
        """Clean up completed stream jobs."""
        completed_jobs = []
        
        for execution_id, job_info in self.running_jobs.items():
            if not job_info["thread"].is_alive():
                completed_jobs.append(execution_id)
        
        for execution_id in completed_jobs:
            del self.running_jobs[execution_id]
            if execution_id in self.stop_events:
                del self.stop_events[execution_id]
