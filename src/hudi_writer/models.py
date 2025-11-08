"""
Hudi data models and configurations.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class HudiTableType(str, Enum):
    """Hudi table types."""
    COPY_ON_WRITE = "COPY_ON_WRITE"
    MERGE_ON_READ = "MERGE_ON_READ"


class HudiOperationType(str, Enum):
    """Hudi operation types."""
    UPSERT = "upsert"
    INSERT = "insert"
    BULK_INSERT = "bulk_insert"
    DELETE = "delete"


class HudiRecordKeyType(str, Enum):
    """Hudi record key types."""
    SIMPLE = "simple"
    COMPLEX = "complex"


class HudiTableConfig(BaseModel):
    """Configuration for Hudi table creation."""
    table_name: str = Field(..., description="Name of the Hudi table")
    database: str = Field(default="default", description="Database name")
    base_path: str = Field(..., description="Base path for the table")
    table_type: HudiTableType = Field(default=HudiTableType.COPY_ON_WRITE, description="Table type")
    
    # Record key configuration
    record_key_field: str = Field(default="id", description="Record key field")
    partition_field: Optional[str] = Field(None, description="Partition field")
    precombine_field: str = Field(default="updated_at", description="Precombine field")
    
    # Schema configuration
    schema: Dict[str, Any] = Field(..., description="Table schema")
    
    # Table properties
    hoodie_table_name: Optional[str] = Field(None, description="Hoodie table name")
    hoodie_database_name: Optional[str] = Field(None, description="Hoodie database name")
    
    # Write configuration
    write_operation: HudiOperationType = Field(default=HudiOperationType.UPSERT, description="Write operation")
    insert_shuffle_parallelism: int = Field(default=200, description="Insert shuffle parallelism")
    upsert_shuffle_parallelism: int = Field(default=200, description="Upsert shuffle parallelism")
    
    # Compaction configuration
    hoodie_compaction_inline: bool = Field(default=True, description="Enable inline compaction")
    hoodie_compaction_schedule_enabled: bool = Field(default=True, description="Enable compaction scheduling")
    hoodie_compaction_trigger_strategy: str = Field(default="num_commits", description="Compaction trigger strategy")
    hoodie_compaction_delta_commits: int = Field(default=5, description="Compaction delta commits")
    
    # Clean configuration
    hoodie_cleaner_policy: str = Field(default="KEEP_LATEST_COMMITS", description="Cleaner policy")
    hoodie_cleaner_commits_retained: int = Field(default=10, description="Cleaner commits retained")
    
    # Index configuration
    hoodie_index_type: str = Field(default="BLOOM", description="Index type")
    hoodie_bloom_index_parallelism: int = Field(default=400, description="Bloom index parallelism")
    
    # Metadata configuration
    hoodie_metadata_enable: bool = Field(default=True, description="Enable metadata")
    hoodie_metadata_index_columns: str = Field(default="", description="Metadata index columns")
    
    # Additional properties
    additional_properties: Dict[str, str] = Field(default_factory=dict, description="Additional Hudi properties")


class HudiWriteConfig(BaseModel):
    """Configuration for Hudi write operations."""
    table_name: str = Field(..., description="Target table name")
    operation: HudiOperationType = Field(default=HudiOperationType.UPSERT, description="Write operation")
    
    # Write options
    record_key_field: str = Field(default="id", description="Record key field")
    partition_field: Optional[str] = Field(None, description="Partition field")
    precombine_field: str = Field(default="updated_at", description="Precombine field")
    
    # Performance tuning
    insert_shuffle_parallelism: int = Field(default=200, description="Insert shuffle parallelism")
    upsert_shuffle_parallelism: int = Field(default=200, description="Upsert shuffle parallelism")
    
    # Batch configuration
    batch_size: int = Field(default=100000, description="Batch size for writes")
    max_file_size: int = Field(default=120 * 1024 * 1024, description="Max file size in bytes")
    
    # Commit configuration
    hoodie_commit_time: Optional[str] = Field(None, description="Custom commit time")
    hoodie_async_clean: bool = Field(default=True, description="Enable async clean")
    
    # Additional options
    additional_options: Dict[str, str] = Field(default_factory=dict, description="Additional write options")


class HudiTableInfo(BaseModel):
    """Information about a Hudi table."""
    table_name: str = Field(..., description="Table name")
    database: str = Field(..., description="Database name")
    base_path: str = Field(..., description="Base path")
    table_type: HudiTableType = Field(..., description="Table type")
    
    # Schema information
    schema: Dict[str, Any] = Field(..., description="Table schema")
    partition_fields: List[str] = Field(default_factory=list, description="Partition fields")
    
    # Table statistics
    total_files: int = Field(default=0, description="Total number of files")
    total_records: int = Field(default=0, description="Total number of records")
    total_size_bytes: int = Field(default=0, description="Total size in bytes")
    
    # Metadata
    created_at: Optional[datetime] = Field(None, description="Table creation time")
    last_commit_time: Optional[datetime] = Field(None, description="Last commit time")
    last_commit_operation: Optional[str] = Field(None, description="Last commit operation")
    
    # Configuration
    record_key_field: str = Field(..., description="Record key field")
    precombine_field: str = Field(..., description="Precombine field")
    
    # Status
    is_active: bool = Field(default=True, description="Table active status")


class HudiWriteResult(BaseModel):
    """Result of a Hudi write operation."""
    table_name: str = Field(..., description="Target table name")
    operation: HudiOperationType = Field(..., description="Write operation")
    
    # Write statistics
    records_written: int = Field(..., description="Number of records written")
    files_written: int = Field(..., description="Number of files written")
    bytes_written: int = Field(..., description="Bytes written")
    
    # Commit information
    commit_time: str = Field(..., description="Commit time")
    commit_id: str = Field(..., description="Commit ID")
    
    # Performance metrics
    write_duration_ms: int = Field(..., description="Write duration in milliseconds")
    records_per_second: float = Field(..., description="Records per second")
    
    # Status
    success: bool = Field(..., description="Write success status")
    error_message: Optional[str] = Field(None, description="Error message if failed")


class HudiQueryConfig(BaseModel):
    """Configuration for Hudi queries."""
    table_name: str = Field(..., description="Table name")
    database: str = Field(default="default", description="Database name")
    
    # Query options
    snapshot_query: bool = Field(default=True, description="Use snapshot query")
    incremental_query: bool = Field(default=False, description="Use incremental query")
    point_in_time_query: bool = Field(default=False, description="Use point-in-time query")
    
    # Time range for incremental queries
    start_time: Optional[str] = Field(None, description="Start time for incremental query")
    end_time: Optional[str] = Field(None, description="End time for incremental query")
    
    # Partition pruning
    partition_filters: Dict[str, Any] = Field(default_factory=dict, description="Partition filters")
    
    # Additional options
    additional_options: Dict[str, str] = Field(default_factory=dict, description="Additional query options")


class HudiCompactionConfig(BaseModel):
    """Configuration for Hudi compaction."""
    table_name: str = Field(..., description="Table name")
    database: str = Field(default="default", description="Database name")
    
    # Compaction options
    compaction_type: str = Field(default="compaction", description="Compaction type")
    max_memory_per_partition_merge: int = Field(default=100 * 1024 * 1024, description="Max memory per partition merge")
    compaction_parallelism: int = Field(default=200, description="Compaction parallelism")
    
    # Trigger options
    trigger_strategy: str = Field(default="num_commits", description="Trigger strategy")
    delta_commits: int = Field(default=5, description="Delta commits")
    
    # Additional options
    additional_options: Dict[str, str] = Field(default_factory=dict, description="Additional compaction options")


class HudiCleanConfig(BaseModel):
    """Configuration for Hudi cleaning."""
    table_name: str = Field(..., description="Table name")
    database: str = Field(default="default", description="Database name")
    
    # Clean options
    clean_type: str = Field(default="clean", description="Clean type")
    clean_policy: str = Field(default="KEEP_LATEST_COMMITS", description="Clean policy")
    commits_retained: int = Field(default=10, description="Commits retained")
    
    # Additional options
    additional_options: Dict[str, str] = Field(default_factory=dict, description="Additional clean options")
