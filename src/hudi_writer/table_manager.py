"""
Hudi table management for creating, updating, and managing Hudi tables.
"""

import os
from typing import Optional, List, Dict, Any
from datetime import datetime

# Optional Spark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, max as spark_max, count as spark_count, sum as spark_sum
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None

from .models import HudiTableConfig, HudiTableInfo, HudiTableType
from .writer import HudiWriter


class HudiTableManager:
    """Manager for Hudi table operations."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Hudi table manager.
        
        Args:
            spark_session: Optional Spark session. If None, creates a new one.
        """
        self.spark = spark_session or self._create_spark_session()
        self.writer = HudiWriter(self.spark)
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Hudi configuration."""
        return SparkSession.builder \
            .appName("HudiTableManager") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hudi") \
            .config("spark.kryo.registrator", "org.apache.spark.sql.hudi.HoodieSparkKryoRegistrar") \
            .getOrCreate()
    
    def create_table(self, config: HudiTableConfig) -> bool:
        """Create a new Hudi table.
        
        Args:
            config: Table configuration
            
        Returns:
            True if table created successfully
        """
        try:
            # Check if table already exists
            if self.table_exists(config.table_name, config.database):
                print(f"Table {config.database}.{config.table_name} already exists")
                return False
            
            # Create table using writer
            success = self.writer.create_table(config)
            
            if success:
                print(f"Table {config.database}.{config.table_name} created successfully")
            
            return success
            
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False
    
    def table_exists(self, table_name: str, database: str = "default") -> bool:
        """Check if a Hudi table exists.
        
        Args:
            table_name: Table name
            database: Database name
            
        Returns:
            True if table exists
        """
        try:
            # Try to read table metadata
            self.spark.sql(f"DESCRIBE {database}.{table_name}")
            return True
        except Exception:
            return False
    
    def get_table_info(self, table_name: str, database: str = "default") -> Optional[HudiTableInfo]:
        """Get information about a Hudi table.
        
        Args:
            table_name: Table name
            database: Database name
            
        Returns:
            Table information or None if not found
        """
        try:
            if not self.table_exists(table_name, database):
                return None
            
            # Get table metadata
            table_metadata = self.spark.sql(f"DESCRIBE {database}.{table_name}").collect()
            
            # Extract schema
            schema = {}
            partition_fields = []
            record_key_field = "id"
            precombine_field = "updated_at"
            
            for row in table_metadata:
                col_name = row["col_name"]
                col_type = row["data_type"]
                
                if col_name.startswith("# "):
                    continue
                elif col_name == "":
                    continue
                elif col_name.startswith("Partition Information"):
                    break
                else:
                    schema[col_name] = {"type": self._convert_spark_type_to_schema_type(col_type)}
            
            # Get table statistics
            try:
                df = self.spark.table(f"{database}.{table_name}")
                total_records = df.count()
                total_files = self._count_files_in_table(database, table_name)
                total_size = self._get_table_size(database, table_name)
            except Exception:
                total_records = 0
                total_files = 0
                total_size = 0
            
            # Get table properties
            try:
                properties = self.spark.sql(f"SHOW TBLPROPERTIES {database}.{table_name}").collect()
                table_type = HudiTableType.COPY_ON_WRITE  # Default
                
                for prop in properties:
                    if prop["key"] == "hoodie.table.type":
                        table_type = HudiTableType(prop["value"])
                    elif prop["key"] == "hoodie.datasource.write.recordkey.field":
                        record_key_field = prop["value"]
                    elif prop["key"] == "hoodie.datasource.write.precombine.field":
                        precombine_field = prop["value"]
            except Exception:
                pass
            
            return HudiTableInfo(
                table_name=table_name,
                database=database,
                base_path=self._get_table_base_path(database, table_name),
                table_type=table_type,
                schema=schema,
                partition_fields=partition_fields,
                total_files=total_files,
                total_records=total_records,
                total_size_bytes=total_size,
                record_key_field=record_key_field,
                precombine_field=precombine_field
            )
            
        except Exception as e:
            print(f"Error getting table info: {str(e)}")
            return None
    
    def list_tables(self, database: str = "default") -> List[str]:
        """List all tables in a database.
        
        Args:
            database: Database name
            
        Returns:
            List of table names
        """
        try:
            tables = self.spark.sql(f"SHOW TABLES IN {database}").collect()
            return [row["tableName"] for row in tables]
        except Exception as e:
            print(f"Error listing tables: {str(e)}")
            return []
    
    def drop_table(self, table_name: str, database: str = "default") -> bool:
        """Drop a Hudi table.
        
        Args:
            table_name: Table name
            database: Database name
            
        Returns:
            True if table dropped successfully
        """
        try:
            if not self.table_exists(table_name, database):
                print(f"Table {database}.{table_name} does not exist")
                return False
            
            # Drop table
            self.spark.sql(f"DROP TABLE {database}.{table_name}")
            
            # Also drop the underlying files if they exist
            table_path = self._get_table_base_path(database, table_name)
            if os.path.exists(table_path):
                import shutil
                shutil.rmtree(table_path)
            
            print(f"Table {database}.{table_name} dropped successfully")
            return True
            
        except Exception as e:
            print(f"Error dropping table: {str(e)}")
            return False
    
    def update_table_schema(self, table_name: str, new_schema: Dict[str, Any], database: str = "default") -> bool:
        """Update table schema (schema evolution).
        
        Args:
            table_name: Table name
            new_schema: New schema definition
            database: Database name
            
        Returns:
            True if schema updated successfully
        """
        try:
            if not self.table_exists(table_name, database):
                print(f"Table {database}.{table_name} does not exist")
                return False
            
            # Hudi supports schema evolution automatically
            # We just need to write data with the new schema
            print(f"Schema evolution is automatic in Hudi. New schema will be applied on next write.")
            return True
            
        except Exception as e:
            print(f"Error updating table schema: {str(e)}")
            return False
    
    def optimize_table(self, table_name: str, database: str = "default") -> bool:
        """Optimize a Hudi table (run compaction).
        
        Args:
            table_name: Table name
            database: Database name
            
        Returns:
            True if optimization completed successfully
        """
        try:
            if not self.table_exists(table_name, database):
                print(f"Table {database}.{table_name} does not exist")
                return False
            
            # Run compaction
            self.spark.sql(f"CALL run_compaction(table => '{database}.{table_name}')")
            
            print(f"Table {database}.{table_name} optimized successfully")
            return True
            
        except Exception as e:
            print(f"Error optimizing table: {str(e)}")
            return False
    
    def clean_table(self, table_name: str, database: str = "default") -> bool:
        """Clean old files from a Hudi table.
        
        Args:
            table_name: Table name
            database: Database name
            
        Returns:
            True if cleaning completed successfully
        """
        try:
            if not self.table_exists(table_name, database):
                print(f"Table {database}.{table_name} does not exist")
                return False
            
            # Run clean
            self.spark.sql(f"CALL run_clean(table => '{database}.{table_name}')")
            
            print(f"Table {database}.{table_name} cleaned successfully")
            return True
            
        except Exception as e:
            print(f"Error cleaning table: {str(e)}")
            return False
    
    def _convert_spark_type_to_schema_type(self, spark_type: str) -> str:
        """Convert Spark type to schema type.
        
        Args:
            spark_type: Spark data type
            
        Returns:
            Schema type
        """
        type_mapping = {
            "string": "string",
            "int": "integer",
            "bigint": "integer",
            "long": "integer",
            "double": "float",
            "float": "float",
            "boolean": "boolean",
            "timestamp": "datetime",
            "date": "datetime"
        }
        
        # Extract base type (remove precision/scale info)
        base_type = spark_type.split("(")[0].lower()
        return type_mapping.get(base_type, "string")
    
    def _count_files_in_table(self, database: str, table_name: str) -> int:
        """Count files in a Hudi table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Number of files
        """
        try:
            # Get table path
            table_path = self._get_table_base_path(database, table_name)
            
            # Count files using Spark
            files = self.spark.sparkContext.wholeTextFiles(table_path).collect()
            return len(files)
        except Exception:
            return 0
    
    def _get_table_size(self, database: str, table_name: str) -> int:
        """Get total size of a Hudi table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Total size in bytes
        """
        try:
            # Get table path
            table_path = self._get_table_base_path(database, table_name)
            
            # Get file sizes using Spark
            files = self.spark.sparkContext.wholeTextFiles(table_path)
            total_size = files.map(lambda x: len(x[1])).sum()
            return int(total_size)
        except Exception:
            return 0
    
    def _get_table_base_path(self, database: str, table_name: str) -> str:
        """Get base path for a Hudi table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Base path
        """
        # Try to get from table properties
        try:
            properties = self.spark.sql(f"SHOW TBLPROPERTIES {database}.{table_name}").collect()
            for prop in properties:
                if prop["key"] == "location":
                    return prop["value"]
        except Exception:
            pass
        
        # Default path structure
        base_path = os.getenv("HUDI_BASE_PATH", "/tmp/hudi")
        return f"{base_path}/{database}/{table_name}"
    
    def close(self):
        """Close Spark session."""
        if self.spark:
            self.spark.stop()
        if self.writer:
            self.writer.close()
