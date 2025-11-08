"""
Job management models and configurations.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from enum import Enum
from croniter import croniter


class JobStatus(str, Enum):
    """Job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class JobType(str, Enum):
    """Job type enumeration."""
    BATCH = "batch"
    STREAM = "stream"


class JobTrigger(str, Enum):
    """Job trigger types."""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    API = "api"


class JobSchedule(BaseModel):
    """Job schedule configuration."""
    trigger: JobTrigger = Field(..., description="Trigger type")
    cron_expression: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    start_time: Optional[datetime] = Field(None, description="Start time for scheduled jobs")
    end_time: Optional[datetime] = Field(None, description="End time for scheduled jobs")
    timezone: str = Field(default="UTC", description="Timezone for scheduling")
    max_runs: Optional[int] = Field(None, description="Maximum number of runs")
    retry_count: int = Field(default=3, description="Number of retries on failure")
    retry_delay_seconds: int = Field(default=300, description="Delay between retries in seconds")


class JobConfig(BaseModel):
    """Base job configuration."""
    job_id: str = Field(..., description="Unique job identifier")
    job_name: str = Field(..., description="Human-readable job name")
    job_type: JobType = Field(..., description="Job type")
    user_id: int = Field(..., description="User who created the job")
    
    # MongoDB configuration
    mongo_uri: str = Field(..., description="MongoDB connection URI")
    database: str = Field(..., description="MongoDB database name")
    collection: str = Field(..., description="MongoDB collection name")
    query: Dict[str, Any] = Field(default_factory=dict, description="MongoDB query filter")
    sort: Optional[Dict[str, int]] = Field(None, description="MongoDB sort order")
    limit: Optional[int] = Field(None, description="MongoDB document limit")
    
    # Schema configuration
    schema: Optional[Dict[str, Any]] = Field(None, description="Data transformation schema")
    schema_file_path: Optional[str] = Field(None, description="Path to schema file")
    
    # Hudi configuration
    hudi_table_name: str = Field(..., description="Target Hudi table name")
    hudi_database: str = Field(default="default", description="Target Hudi database")
    hudi_base_path: str = Field(..., description="Hudi table base path")
    partition_field: Optional[str] = Field(None, description="Hudi partition field")
    
    # Job settings
    schedule: JobSchedule = Field(..., description="Job schedule configuration")
    enabled: bool = Field(default=True, description="Whether job is enabled")
    description: Optional[str] = Field(None, description="Job description")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Job creation time")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update time")
    created_by: str = Field(..., description="User who created the job")


class BatchJobConfig(JobConfig):
    """Batch job specific configuration."""
    job_type: JobType = Field(default=JobType.BATCH, description="Job type")
    
    # Batch specific settings
    date_field: str = Field(..., description="Date field for batch processing")
    batch_size: int = Field(default=10000, description="Batch size for processing")
    parallel_execution: bool = Field(default=True, description="Enable parallel execution")
    max_parallel_jobs: int = Field(default=5, description="Maximum parallel batch jobs")
    
    # Time range configuration
    start_date: Optional[datetime] = Field(None, description="Start date for batch processing")
    end_date: Optional[datetime] = Field(None, description="End date for batch processing")
    lookback_days: int = Field(default=1, description="Days to look back for batch processing")
    
    # Data processing options
    flatten_data: bool = Field(default=True, description="Flatten nested data")
    clean_data: bool = Field(default=True, description="Clean data during processing")
    apply_schema: bool = Field(default=True, description="Apply schema validation")
    use_spark: bool = Field(default=False, description="Use Spark for large datasets")


class StreamJobConfig(JobConfig):
    """Stream job specific configuration."""
    job_type: JobType = Field(default=JobType.STREAM, description="Job type")
    
    # Stream specific settings
    polling_interval_seconds: int = Field(default=60, description="Polling interval in seconds")
    batch_size: int = Field(default=1000, description="Batch size for stream processing")
    checkpoint_interval: int = Field(default=1000, description="Checkpoint interval")
    
    # Stream processing options
    real_time_processing: bool = Field(default=True, description="Enable real-time processing")
    watermark_delay_seconds: int = Field(default=10, description="Watermark delay in seconds")
    max_late_data_days: int = Field(default=1, description="Maximum late data days")
    
    # Data processing options
    flatten_data: bool = Field(default=True, description="Flatten nested data")
    clean_data: bool = Field(default=True, description="Clean data during processing")
    apply_schema: bool = Field(default=True, description="Apply schema validation")


class JobResult(BaseModel):
    """Job execution result."""
    job_id: str = Field(..., description="Job identifier")
    execution_id: str = Field(..., description="Execution identifier")
    status: JobStatus = Field(..., description="Execution status")
    
    # Execution details
    started_at: datetime = Field(..., description="Execution start time")
    completed_at: Optional[datetime] = Field(None, description="Execution completion time")
    duration_seconds: Optional[float] = Field(None, description="Execution duration in seconds")
    
    # Processing statistics
    records_processed: int = Field(default=0, description="Number of records processed")
    records_written: int = Field(default=0, description="Number of records written")
    files_created: int = Field(default=0, description="Number of files created")
    bytes_processed: int = Field(default=0, description="Bytes processed")
    
    # Error information
    error_message: Optional[str] = Field(None, description="Error message if failed")
    error_details: Optional[Dict[str, Any]] = Field(None, description="Detailed error information")
    
    # Performance metrics
    records_per_second: Optional[float] = Field(None, description="Processing rate")
    memory_usage_mb: Optional[float] = Field(None, description="Memory usage in MB")
    cpu_usage_percent: Optional[float] = Field(None, description="CPU usage percentage")
    
    # Metadata
    logs: List[str] = Field(default_factory=list, description="Execution logs")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class JobExecution(BaseModel):
    """Job execution record."""
    execution_id: str = Field(..., description="Execution identifier")
    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Execution status")
    
    # Execution details
    started_at: datetime = Field(..., description="Execution start time")
    completed_at: Optional[datetime] = Field(None, description="Execution completion time")
    triggered_by: JobTrigger = Field(..., description="What triggered the execution")
    
    # Configuration snapshot
    job_config: JobConfig = Field(..., description="Job configuration at execution time")
    
    # Result
    result: Optional[JobResult] = Field(None, description="Execution result")
    
    # Retry information
    retry_count: int = Field(default=0, description="Number of retries")
    max_retries: int = Field(default=3, description="Maximum retries allowed")
    
    # Worker information
    worker_id: Optional[str] = Field(None, description="Worker that executed the job")
    worker_host: Optional[str] = Field(None, description="Worker hostname")


class JobMetrics(BaseModel):
    """Job performance metrics."""
    job_id: str = Field(..., description="Job identifier")
    time_window: str = Field(..., description="Time window for metrics")
    
    # Execution counts
    total_executions: int = Field(default=0, description="Total executions")
    successful_executions: int = Field(default=0, description="Successful executions")
    failed_executions: int = Field(default=0, description="Failed executions")
    cancelled_executions: int = Field(default=0, description="Cancelled executions")
    
    # Performance metrics
    average_duration_seconds: float = Field(default=0.0, description="Average execution duration")
    min_duration_seconds: float = Field(default=0.0, description="Minimum execution duration")
    max_duration_seconds: float = Field(default=0.0, description="Maximum execution duration")
    
    # Data metrics
    total_records_processed: int = Field(default=0, description="Total records processed")
    total_records_written: int = Field(default=0, description="Total records written")
    average_records_per_second: float = Field(default=0.0, description="Average processing rate")
    
    # Error metrics
    error_rate: float = Field(default=0.0, description="Error rate percentage")
    common_errors: List[Dict[str, Any]] = Field(default_factory=list, description="Common error types")
    
    # Resource metrics
    average_memory_usage_mb: float = Field(default=0.0, description="Average memory usage")
    average_cpu_usage_percent: float = Field(default=0.0, description="Average CPU usage")
    
    # Timestamps
    first_execution: Optional[datetime] = Field(None, description="First execution time")
    last_execution: Optional[datetime] = Field(None, description="Last execution time")
    last_successful_execution: Optional[datetime] = Field(None, description="Last successful execution time")


class JobAlert(BaseModel):
    """Job alert configuration."""
    alert_id: str = Field(..., description="Alert identifier")
    job_id: str = Field(..., description="Job identifier")
    
    # Alert conditions
    alert_on_failure: bool = Field(default=True, description="Alert on job failure")
    alert_on_success: bool = Field(default=False, description="Alert on job success")
    alert_on_duration_exceeded: bool = Field(default=False, description="Alert if duration exceeds threshold")
    duration_threshold_seconds: Optional[int] = Field(None, description="Duration threshold in seconds")
    
    # Alert settings
    enabled: bool = Field(default=True, description="Whether alert is enabled")
    notification_channels: List[str] = Field(default_factory=list, description="Notification channels")
    alert_message_template: Optional[str] = Field(None, description="Custom alert message template")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Alert creation time")
    created_by: str = Field(..., description="User who created the alert")


class JobDependency(BaseModel):
    """Job dependency configuration."""
    job_id: str = Field(..., description="Job identifier")
    depends_on_job_id: str = Field(..., description="Job this job depends on")
    dependency_type: str = Field(default="success", description="Dependency type (success, completion, etc.)")
    delay_seconds: int = Field(default=0, description="Delay after dependency completion")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Dependency creation time")
    created_by: str = Field(..., description="User who created the dependency")
