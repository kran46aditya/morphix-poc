"""
Hudi writer for writing DataFrames to Hudi tables.
"""

import os
import time
from typing import Optional, Dict, Any, Union
from datetime import datetime
import pandas as pd

# Optional Spark imports - will be imported when needed
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    StructType = None
    StructField = None
    StringType = None
    LongType = None
    TimestampType = None

from .models import HudiTableConfig, HudiWriteConfig, HudiWriteResult, HudiTableType, HudiOperationType


class HudiWriter:
    """Writer for Hudi tables with support for various operations."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Hudi writer.
        
        Args:
            spark_session: Optional Spark session. If None, creates a new one.
        """
        self.spark = spark_session or self._create_spark_session()
        self._setup_hudi_config()
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Hudi configuration."""
        return SparkSession.builder \
            .appName("HudiWriter") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hudi") \
            .config("spark.kryo.registrator", "org.apache.spark.sql.hudi.HoodieSparkKryoRegistrar") \
            .getOrCreate()
    
    def _setup_hudi_config(self):
        """Setup Hudi configuration."""
        # Set Hudi JAR path if available
        hudi_jar_path = os.getenv("HUDI_JAR_PATH")
        if hudi_jar_path:
            self.spark.conf.set("spark.jars", hudi_jar_path)
        
        # Set default Hudi properties
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    def create_table(self, config: HudiTableConfig) -> bool:
        """Create a Hudi table.
        
        Args:
            config: Hudi table configuration
            
        Returns:
            True if table created successfully
        """
        try:
            # Create empty DataFrame with schema
            schema = self._convert_schema_to_spark(config.schema)
            empty_df = self.spark.createDataFrame([], schema)
            
            # Set Hudi options
            hudi_options = self._build_hudi_options(config)
            
            # Write empty DataFrame to create table
            empty_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(config.base_path)
            
            return True
            
        except Exception as e:
            print(f"Error creating Hudi table: {str(e)}")
            return False
    
    def write_dataframe(
        self, 
        df: Union[pd.DataFrame, DataFrame], 
        config: HudiWriteConfig,
        table_config: Optional[HudiTableConfig] = None
    ) -> HudiWriteResult:
        """Write DataFrame to Hudi table.
        
        Args:
            df: DataFrame to write (pandas or Spark)
            config: Write configuration
            table_config: Table configuration (if creating new table)
            
        Returns:
            Write result with statistics
        """
        start_time = time.time()
        
        try:
            # Convert pandas DataFrame to Spark if needed
            if isinstance(df, pd.DataFrame):
                spark_df = self.spark.createDataFrame(df)
            else:
                spark_df = df
            
            # Get table path
            table_path = self._get_table_path(config.table_name, table_config)
            
            # Build Hudi options
            hudi_options = self._build_write_options(config, table_config)
            
            # Write DataFrame
            write_result = spark_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(table_path)
            
            # Calculate statistics
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)
            
            # Get write statistics
            records_written = spark_df.count()
            files_written = self._count_files_in_path(table_path)
            bytes_written = self._get_path_size(table_path)
            
            # Generate commit information
            commit_time = datetime.now().strftime("%Y%m%d%H%M%S")
            commit_id = f"{commit_time}_000"
            
            return HudiWriteResult(
                table_name=config.table_name,
                operation=config.operation,
                records_written=records_written,
                files_written=files_written,
                bytes_written=bytes_written,
                commit_time=commit_time,
                commit_id=commit_id,
                write_duration_ms=duration_ms,
                records_per_second=records_written / (duration_ms / 1000) if duration_ms > 0 else 0,
                success=True
            )
            
        except Exception as e:
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)
            
            return HudiWriteResult(
                table_name=config.table_name,
                operation=config.operation,
                records_written=0,
                files_written=0,
                bytes_written=0,
                commit_time="",
                commit_id="",
                write_duration_ms=duration_ms,
                records_per_second=0,
                success=False,
                error_message=str(e)
            )
    
    def upsert_dataframe(
        self, 
        df: Union[pd.DataFrame, DataFrame], 
        config: HudiWriteConfig,
        table_config: Optional[HudiTableConfig] = None
    ) -> HudiWriteResult:
        """Upsert DataFrame to Hudi table.
        
        Args:
            df: DataFrame to upsert
            config: Write configuration
            table_config: Table configuration
            
        Returns:
            Write result with statistics
        """
        # Set operation to upsert
        config.operation = HudiOperationType.UPSERT
        return self.write_dataframe(df, config, table_config)
    
    def insert_dataframe(
        self, 
        df: Union[pd.DataFrame, DataFrame], 
        config: HudiWriteConfig,
        table_config: Optional[HudiTableConfig] = None
    ) -> HudiWriteResult:
        """Insert DataFrame to Hudi table.
        
        Args:
            df: DataFrame to insert
            config: Write configuration
            table_config: Table configuration
            
        Returns:
            Write result with statistics
        """
        # Set operation to insert
        config.operation = HudiOperationType.INSERT
        return self.write_dataframe(df, config, table_config)
    
    def delete_records(
        self, 
        df: Union[pd.DataFrame, DataFrame], 
        config: HudiWriteConfig,
        table_config: Optional[HudiTableConfig] = None
    ) -> HudiWriteResult:
        """Delete records from Hudi table.
        
        Args:
            df: DataFrame with records to delete (must contain record keys)
            config: Write configuration
            table_config: Table configuration
            
        Returns:
            Write result with statistics
        """
        # Set operation to delete
        config.operation = HudiOperationType.DELETE
        return self.write_dataframe(df, config, table_config)
    
    def _convert_schema_to_spark(self, schema: Dict[str, Any]) -> StructType:
        """Convert schema dictionary to Spark StructType.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            Spark StructType
        """
        fields = []
        for field_name, field_config in schema.items():
            field_type = field_config.get('type', 'string')
            nullable = field_config.get('nullable', True)
            
            # Convert type to Spark type
            if field_type == 'string':
                spark_type = StringType()
            elif field_type in ['int', 'integer']:
                spark_type = LongType()
            elif field_type == 'datetime':
                spark_type = TimestampType()
            else:
                spark_type = StringType()  # Default to string
            
            fields.append(StructField(field_name, spark_type, nullable))
        
        return StructType(fields)
    
    def _build_hudi_options(self, config: HudiTableConfig) -> Dict[str, str]:
        """Build Hudi options for table creation.
        
        Args:
            config: Table configuration
            
        Returns:
            Hudi options dictionary
        """
        options = {
            "hoodie.table.name": config.hoodie_table_name or config.table_name,
            "hoodie.database.name": config.hoodie_database_name or config.database,
            "hoodie.table.type": config.table_type.value,
            "hoodie.datasource.write.recordkey.field": config.record_key_field,
            "hoodie.datasource.write.precombine.field": config.precombine_field,
            "hoodie.datasource.write.operation": config.write_operation.value,
            "hoodie.datasource.write.insert.shuffle.parallelism": str(config.insert_shuffle_parallelism),
            "hoodie.datasource.write.upsert.shuffle.parallelism": str(config.upsert_shuffle_parallelism),
            "hoodie.compact.inline": str(config.hoodie_compaction_inline).lower(),
            "hoodie.compact.schedule.enabled": str(config.hoodie_compaction_schedule_enabled).lower(),
            "hoodie.compact.trigger.strategy": config.hoodie_compaction_trigger_strategy,
            "hoodie.compact.delta_commits": str(config.hoodie_compaction_delta_commits),
            "hoodie.cleaner.policy": config.hoodie_cleaner_policy,
            "hoodie.cleaner.commits.retained": str(config.hoodie_cleaner_commits_retained),
            "hoodie.index.type": config.hoodie_index_type,
            "hoodie.bloom.index.parallelism": str(config.hoodie_bloom_index_parallelism),
            "hoodie.metadata.enable": str(config.hoodie_metadata_enable).lower(),
            "hoodie.metadata.index.column": config.hoodie_metadata_index_columns
        }
        
        # Add partition field if specified
        if config.partition_field:
            options["hoodie.datasource.write.partitionpath.field"] = config.partition_field
        
        # Add additional properties
        options.update(config.additional_properties)
        
        return options
    
    def _build_write_options(self, config: HudiWriteConfig, table_config: Optional[HudiTableConfig]) -> Dict[str, str]:
        """Build Hudi options for write operations.
        
        Args:
            config: Write configuration
            table_config: Table configuration
            
        Returns:
            Hudi options dictionary
        """
        options = {
            "hoodie.datasource.write.operation": config.operation.value,
            "hoodie.datasource.write.recordkey.field": config.record_key_field,
            "hoodie.datasource.write.precombine.field": config.precombine_field,
            "hoodie.datasource.write.insert.shuffle.parallelism": str(config.insert_shuffle_parallelism),
            "hoodie.datasource.write.upsert.shuffle.parallelism": str(config.upsert_shuffle_parallelism),
            "hoodie.parquet.max.file.size": str(config.max_file_size),
            "hoodie.cleaner.async.enabled": str(config.hoodie_async_clean).lower()
        }
        
        # Add partition field if specified
        if config.partition_field:
            options["hoodie.datasource.write.partitionpath.field"] = config.partition_field
        
        # Add custom commit time if specified
        if config.hoodie_commit_time:
            options["hoodie.datasource.write.hoodie.recordkey.field"] = config.hoodie_commit_time
        
        # Add additional options
        options.update(config.additional_options)
        
        return options
    
    def _get_table_path(self, table_name: str, table_config: Optional[HudiTableConfig]) -> str:
        """Get table path for writing.
        
        Args:
            table_name: Table name
            table_config: Table configuration
            
        Returns:
            Table path
        """
        if table_config:
            return table_config.base_path
        else:
            # Default path structure
            base_path = os.getenv("HUDI_BASE_PATH", "/tmp/hudi")
            return f"{base_path}/{table_name}"
    
    def _count_files_in_path(self, path: str) -> int:
        """Count files in a path.
        
        Args:
            path: Path to count files
            
        Returns:
            Number of files
        """
        try:
            # Use Spark to count files
            files = self.spark.sparkContext.wholeTextFiles(path).collect()
            return len(files)
        except Exception:
            return 0
    
    def _get_path_size(self, path: str) -> int:
        """Get total size of files in a path.
        
        Args:
            path: Path to get size
            
        Returns:
            Total size in bytes
        """
        try:
            # Use Spark to get file sizes
            files = self.spark.sparkContext.wholeTextFiles(path)
            total_size = files.map(lambda x: len(x[1])).sum()
            return int(total_size)
        except Exception:
            return 0
    
    def close(self):
        """Close Spark session."""
        if self.spark:
            self.spark.stop()
