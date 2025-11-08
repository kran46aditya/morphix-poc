"""
Integration helper for connecting Module 1 (MongoDB Reader) and Module 2 (Data Transformer)
"""

from typing import Dict, Any, Optional, Union, Tuple
import pandas as pd

from .mongo_api_reader import MongoDataReader
from .data_transformer import DataTransformer


class ETLPipeline:
    """Integrated ETL pipeline combining Module 1 and Module 2."""
    
    def __init__(self, mongo_reader: MongoDataReader, transformer: DataTransformer):
        """Initialize the ETL pipeline.
        
        Args:
            mongo_reader: Configured MongoDataReader instance
            transformer: Configured DataTransformer instance
        """
        self.mongo_reader = mongo_reader
        self.transformer = transformer
        self.execution_log = []
        
    def run_pipeline(
        self,
        query: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        use_spark: bool = False,
        flatten: bool = True,
        clean: bool = True,
        apply_schema: bool = True
    ) -> Union[pd.DataFrame, Tuple[Any, Any]]:
        """Run the complete ETL pipeline from MongoDB to transformed DataFrame.
        
        Args:
            query: MongoDB query filter
            limit: Maximum number of documents to read
            use_spark: Whether to use Spark DataFrames
            flatten: Whether to flatten nested structures
            clean: Whether to apply data cleaning
            apply_schema: Whether to apply schema transformations
            
        Returns:
            Transformed DataFrame (pandas or Spark)
        """
        self.execution_log = []
        
        try:
            # Step 1: Read data from MongoDB
            self.execution_log.append("Starting data read from MongoDB...")
            
            if use_spark:
                raw_data, spark_session = self.mongo_reader.read_to_spark(query=query, limit=limit)
                self.execution_log.append(f"Read Spark DataFrame with {raw_data.count()} rows")
                
                # For Spark, convert to pandas for transformation, then back to Spark
                pandas_data = raw_data.toPandas()
                self.execution_log.append("Converted Spark DataFrame to pandas for transformation")
                
                # Step 2: Transform data
                transformed_data = self.transformer.transform(
                    pandas_data, flatten=flatten, clean=clean, apply_schema=apply_schema
                )
                self.execution_log.append("Applied transformations to pandas DataFrame")
                
                # Convert back to Spark
                final_spark_df = spark_session.createDataFrame(transformed_data)
                self.execution_log.append("Converted back to Spark DataFrame")
                
                return final_spark_df, spark_session
                
            else:
                raw_data = self.mongo_reader.read_to_pandas(query=query, limit=limit or 1000)
                self.execution_log.append(f"Read pandas DataFrame with {len(raw_data)} rows")
                
                # Step 2: Transform data
                transformed_data = self.transformer.transform(
                    raw_data, flatten=flatten, clean=clean, apply_schema=apply_schema
                )
                self.execution_log.append("Applied transformations to pandas DataFrame")
                
                return transformed_data
                
        except Exception as e:
            error_msg = f"Pipeline execution failed: {str(e)}"
            self.execution_log.append(error_msg)
            raise RuntimeError(error_msg) from e
    
    def get_execution_log(self) -> list:
        """Get the execution log from the last pipeline run."""
        return self.execution_log.copy()
    
    def validate_pipeline_config(self) -> Dict[str, Any]:
        """Validate the pipeline configuration."""
        validation_result = {
            'mongo_reader': {
                'configured': self.mongo_reader is not None,
                'mongo_uri': getattr(self.mongo_reader, 'mongo_uri', None),
                'database': getattr(self.mongo_reader, 'database', None),
                'collection': getattr(self.mongo_reader, 'collection', None)
            },
            'transformer': {
                'configured': self.transformer is not None,
                'has_schema': bool(self.transformer.schema) if self.transformer else False,
                'custom_transformations': len(self.transformer.transformations) if self.transformer else 0
            },
            'ready': True
        }
        
        # Check if required components are present
        if not self.mongo_reader:
            validation_result['ready'] = False
            validation_result['errors'] = validation_result.get('errors', [])
            validation_result['errors'].append('MongoDataReader not configured')
            
        if not self.transformer:
            validation_result['ready'] = False
            validation_result['errors'] = validation_result.get('errors', [])
            validation_result['errors'].append('DataTransformer not configured')
            
        return validation_result


def create_pipeline_from_credentials(
    username: str,
    password: str,
    host: str,
    port: int,
    database: str,
    collection: str,
    schema: Optional[Dict[str, Any]] = None
) -> ETLPipeline:
    """Create an ETL pipeline from MongoDB credentials.
    
    Args:
        username: MongoDB username
        password: MongoDB password
        host: MongoDB host
        port: MongoDB port
        database: Database name
        collection: Collection name
        schema: Optional schema for transformation
        
    Returns:
        Configured ETLPipeline instance
    """
    from .mongo_api_reader import create_reader_from_credentials
    
    mongo_reader = create_reader_from_credentials(
        username, password, host, port, database, collection
    )
    transformer = DataTransformer(schema=schema)
    
    return ETLPipeline(mongo_reader, transformer)


def create_pipeline_from_uri(
    mongo_uri: str,
    database: str,
    collection: str,
    schema: Optional[Dict[str, Any]] = None
) -> ETLPipeline:
    """Create an ETL pipeline from MongoDB URI.
    
    Args:
        mongo_uri: MongoDB connection URI
        database: Database name
        collection: Collection name
        schema: Optional schema for transformation
        
    Returns:
        Configured ETLPipeline instance
    """
    from .mongo_api_reader import create_reader_from_connection_info
    
    mongo_reader = create_reader_from_connection_info(mongo_uri, database, collection)
    transformer = DataTransformer(schema=schema)
    
    return ETLPipeline(mongo_reader, transformer)


def create_pipeline_from_api(
    api_url: str = "http://localhost:8000/mongo/read",
    user_id: Optional[str] = None,
    schema: Optional[Dict[str, Any]] = None
) -> 'APIPipeline':
    """Create an ETL pipeline that reads data via API.
    
    Args:
        api_url: API endpoint URL
        user_id: User ID for stored credentials lookup
        schema: Optional schema for transformation
        
    Returns:
        Configured APIPipeline instance
    """
    return APIPipeline(api_url=api_url, user_id=user_id, schema=schema)


class APIPipeline:
    """ETL pipeline that reads data via API instead of direct database connection."""
    
    def __init__(self, api_url: str, user_id: Optional[str] = None, schema: Optional[Dict[str, Any]] = None):
        """Initialize API-based ETL pipeline.
        
        Args:
            api_url: API endpoint URL
            user_id: User ID for stored credentials
            schema: Optional schema for transformation
        """
        self.api_url = api_url
        self.user_id = user_id
        self.transformer = DataTransformer(schema=schema)
        self.execution_log = []
        
    def run_pipeline(
        self,
        payload: Optional[Dict[str, Any]] = None,
        flatten: bool = True,
        clean: bool = True,
        apply_schema: bool = True
    ) -> pd.DataFrame:
        """Run the ETL pipeline using API data source.
        
        Args:
            payload: API request payload (overrides user_id if provided)
            flatten: Whether to flatten nested structures  
            clean: Whether to apply data cleaning
            apply_schema: Whether to apply schema transformations
            
        Returns:
            Transformed pandas DataFrame
        """
        from .mongo_api_reader import read_via_api
        
        self.execution_log = []
        
        try:
            # Prepare API payload
            if payload is None:
                if self.user_id:
                    payload = {"user_id": self.user_id}
                else:
                    raise ValueError("Either payload or user_id must be provided")
                    
            # Step 1: Read data via API
            self.execution_log.append(f"Reading data from API: {self.api_url}")
            raw_data = read_via_api(api_url=self.api_url, payload=payload)
            self.execution_log.append(f"Read {len(raw_data)} rows from API")
            
            # Step 2: Transform data
            transformed_data = self.transformer.transform(
                raw_data, flatten=flatten, clean=clean, apply_schema=apply_schema
            )
            self.execution_log.append("Applied transformations to DataFrame")
            
            return transformed_data
            
        except Exception as e:
            error_msg = f"API pipeline execution failed: {str(e)}"
            self.execution_log.append(error_msg)
            raise RuntimeError(error_msg) from e
    
    def get_execution_log(self) -> list:
        """Get the execution log from the last pipeline run."""
        return self.execution_log.copy()


# Convenience functions for common schema patterns

def create_user_profile_schema() -> Dict[str, Any]:
    """Create a schema for typical user profile data."""
    return {
        '_id': {'type': 'str', 'required': True},
        'user_name': {'type': 'str', 'required': True, 'nullable': False},
        'user_email': {'type': 'str', 'required': True, 'pattern': r'^[^@]+@[^@]+\.[^@]+$'},
        'user_profile_age': {'type': 'int', 'min_value': 0, 'max_value': 150},
        'user_profile_location_city': {'type': 'str'},
        'user_profile_location_state': {'type': 'str'},
        'user_profile_preferences': {'type': 'str'},  # comma-separated
        'created_at': {'type': 'datetime'},
        'updated_at': {'type': 'datetime', 'nullable': True}
    }


def create_order_schema() -> Dict[str, Any]:
    """Create a schema for order/transaction data."""
    return {
        '_id': {'type': 'str', 'required': True},
        'order_id': {'type': 'str', 'required': True},
        'customer_id': {'type': 'str', 'required': True},
        'order_total': {'type': 'float', 'min_value': 0},
        'order_status': {'type': 'category', 'categories': ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']},
        'order_date': {'type': 'datetime', 'required': True},
        'items_count': {'type': 'int', 'min_value': 0},
        'shipping_address_city': {'type': 'str'},
        'shipping_address_state': {'type': 'str'},
        'shipping_address_zip': {'type': 'str', 'pattern': r'^\d{5}(-\d{4})?$'}
    }


def create_event_schema() -> Dict[str, Any]:
    """Create a schema for event/analytics data."""
    return {
        '_id': {'type': 'str', 'required': True},
        'event_type': {'type': 'str', 'required': True},
        'user_id': {'type': 'str', 'required': True},
        'timestamp': {'type': 'datetime', 'required': True},
        'properties_page': {'type': 'str'},
        'properties_source': {'type': 'str'},
        'properties_campaign': {'type': 'str'},
        'device_type': {'type': 'category', 'categories': ['desktop', 'mobile', 'tablet']},
        'session_id': {'type': 'str'}
    }