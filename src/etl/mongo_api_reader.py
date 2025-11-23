"""
ETL Module 1: MongoDB Data Reader

This module handles reading data from MongoDB and converting it into DataFrames.
It supports both direct connection and API-based reading.
"""

from typing import Optional, Dict, Any, Union, Tuple
import json
import pandas as pd
import time
import hashlib
from ..mongodb import connection as mongo_conn
from ..utils.logging import get_logger, CorrelationContext


class MongoDataReader:
    """First ETL module that reads MongoDB data and returns appropriate DataFrame."""
    
    def __init__(self, mongo_uri: str, database: str, collection: str):
        """Initialize the MongoDB data reader.
        
        Args:
            mongo_uri: MongoDB connection URI
            database: Database name
            collection: Collection name
        """
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection = collection
        self.logger = get_logger(__name__)
    
    def _compute_schema_fingerprint(self, df: pd.DataFrame) -> str:
        """Compute schema fingerprint from DataFrame.
        
        Args:
            df: DataFrame to fingerprint
            
        Returns:
            SHA256 hash of sorted keys and basic types
        """
        if df.empty:
            return hashlib.sha256(b"").hexdigest()
        
        # Create deterministic schema representation
        schema_parts = []
        for col in sorted(df.columns):
            dtype = str(df[col].dtype)
            # Normalize dtype to basic types
            if 'int' in dtype:
                dtype = 'integer'
            elif 'float' in dtype:
                dtype = 'float'
            elif 'bool' in dtype:
                dtype = 'boolean'
            elif 'datetime' in dtype:
                dtype = 'datetime'
            else:
                dtype = 'string'
            schema_parts.append(f"{col}:{dtype}")
        
        schema_str = "|".join(schema_parts)
        return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
    
    def read_to_pandas(self, query: Optional[Dict[str, Any]] = None, limit: int = 1000) -> pd.DataFrame:
        """Read MongoDB data into a pandas DataFrame using pymongo.
        
        Args:
            query: MongoDB query filter
            limit: Maximum number of documents to read
            
        Returns:
            pandas DataFrame containing the data
        """
        start_time = time.time()
        error_occurred = False
        error_message = None
        record_count = 0
        
        try:
            docs = mongo_conn.read_with_pymongo(
                mongo_uri=self.mongo_uri,
                database=self.database,
                collection=self.collection,
                query=query or {},
                limit=limit
            )
            
            if not docs:
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(docs)
                record_count = len(df)
            
            # Compute schema fingerprint
            schema_fingerprint = self._compute_schema_fingerprint(df)
            
            # Calculate latency
            latency_ms = (time.time() - start_time) * 1000
            
            # Emit success event
            self.logger.info(
                "MongoDB read completed",
                extra={
                    'event_type': 'mongo_read_success',
                    'database': self.database,
                    'collection': self.collection,
                    'query': query,
                    'limit': limit,
                    'record_count': record_count,
                    'schema_fingerprint': schema_fingerprint,
                    'latency_ms': round(latency_ms, 2),
                    'columns': list(df.columns) if not df.empty else []
                }
            )
            
            return df
            
        except Exception as e:
            error_occurred = True
            error_message = str(e)
            latency_ms = (time.time() - start_time) * 1000
            
            # Emit error event
            self.logger.error(
                f"MongoDB read failed: {error_message}",
                exc_info=True,
                extra={
                    'event_type': 'mongo_read_error',
                    'database': self.database,
                    'collection': self.collection,
                    'query': query,
                    'limit': limit,
                    'error_message': error_message,
                    'latency_ms': round(latency_ms, 2)
                }
            )
            
            raise
    
    def read_to_spark(self, query: Optional[Dict[str, Any]] = None, limit: Optional[int] = None):
        """Read MongoDB data into a Spark DataFrame.
        
        Args:
            query: MongoDB query filter
            limit: Maximum number of documents to read (optional)
            
        Returns:
            Tuple of (Spark DataFrame, SparkSession)
        """
        start_time = time.time()
        error_occurred = False
        error_message = None
        
        try:
            from pyspark.sql import SparkSession
            
            # Create SparkSession with MongoDB connector
            spark = (
                SparkSession.builder
                .appName("mongo-etl-reader")
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
                .config("spark.mongodb.input.uri", self.mongo_uri)
                .config("spark.mongodb.output.uri", self.mongo_uri)
                .getOrCreate()
            )
            
            # Build reader
            reader = (
                spark.read.format("mongo")
                .option("uri", self.mongo_uri)
                .option("database", self.database) 
                .option("collection", self.collection)
            )
            
            # Add query filter if provided
            if query:
                reader = reader.option("pipeline", json.dumps([{"$match": query}]))
            
            df = reader.load()
            
            # Apply limit if specified
            if limit:
                df = df.limit(limit)
            
            # Get record count and schema info
            record_count = df.count()
            schema_fingerprint = self._compute_schema_fingerprint_from_spark(df)
            latency_ms = (time.time() - start_time) * 1000
            
            # Emit success event
            self.logger.info(
                "MongoDB Spark read completed",
                extra={
                    'event_type': 'mongo_spark_read_success',
                    'database': self.database,
                    'collection': self.collection,
                    'query': query,
                    'limit': limit,
                    'record_count': record_count,
                    'schema_fingerprint': schema_fingerprint,
                    'latency_ms': round(latency_ms, 2)
                }
            )
            
            return df, spark
            
        except Exception as e:
            error_occurred = True
            error_message = str(e)
            latency_ms = (time.time() - start_time) * 1000
            
            # Emit error event
            self.logger.error(
                f"MongoDB Spark read failed: {error_message}",
                exc_info=True,
                extra={
                    'event_type': 'mongo_spark_read_error',
                    'database': self.database,
                    'collection': self.collection,
                    'query': query,
                    'limit': limit,
                    'error_message': error_message,
                    'latency_ms': round(latency_ms, 2)
                }
            )
            
            raise
    
    def _compute_schema_fingerprint_from_spark(self, df) -> str:
        """Compute schema fingerprint from Spark DataFrame.
        
        Args:
            df: Spark DataFrame to fingerprint
            
        Returns:
            SHA256 hash of sorted keys and basic types
        """
        try:
            schema_parts = []
            for field in sorted(df.schema.fields, key=lambda f: f.name):
                dtype = str(field.dataType)
                # Normalize to basic types
                if 'IntegerType' in dtype or 'LongType' in dtype:
                    dtype = 'integer'
                elif 'FloatType' in dtype or 'DoubleType' in dtype:
                    dtype = 'float'
                elif 'BooleanType' in dtype:
                    dtype = 'boolean'
                elif 'TimestampType' in dtype or 'DateType' in dtype:
                    dtype = 'datetime'
                else:
                    dtype = 'string'
                schema_parts.append(f"{field.name}:{dtype}")
            
            schema_str = "|".join(schema_parts)
            return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
        except Exception:
            return hashlib.sha256(b"unknown").hexdigest()
    
    def read(self, use_spark: bool = False, query: Optional[Dict[str, Any]] = None, limit: Optional[int] = None) -> Union[pd.DataFrame, Tuple[Any, Any]]:
        """Read data using either pandas or Spark based on preference.
        
        Args:
            use_spark: If True, use Spark DataFrame; if False, use pandas DataFrame
            query: MongoDB query filter
            limit: Maximum number of documents to read
            
        Returns:
            pandas DataFrame if use_spark=False, or (Spark DataFrame, SparkSession) if use_spark=True
        """
        if use_spark:
            return self.read_to_spark(query=query, limit=limit)
        else:
            return self.read_to_pandas(query=query, limit=limit or 1000)


def create_reader_from_connection_info(mongo_uri: str, database: str, collection: str) -> MongoDataReader:
    """Factory function to create a MongoDataReader from connection information.
    
    Args:
        mongo_uri: MongoDB connection URI
        database: Database name  
        collection: Collection name
        
    Returns:
        MongoDataReader instance
    """
    return MongoDataReader(mongo_uri, database, collection)


def create_reader_from_credentials(username: str, password: str, host: str, port: int, database: str, collection: str) -> MongoDataReader:
    """Factory function to create a MongoDataReader from individual credentials.
    
    Args:
        username: MongoDB username
        password: MongoDB password
        host: MongoDB host
        port: MongoDB port
        database: Database name
        collection: Collection name
        
    Returns:
        MongoDataReader instance
    """
    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
    return MongoDataReader(mongo_uri, database, collection)


def read_via_api(api_url: str = "http://localhost:8000/mongo/read", payload: Optional[Dict[str, Any]] = None, timeout: int = 30) -> pd.DataFrame:
    """Read MongoDB data via the API endpoint and return as pandas DataFrame.
    
    Args:
        api_url: URL of the MongoDB read API endpoint
        payload: Request payload for the API
        timeout: Request timeout in seconds
        
    Returns:
        pandas DataFrame containing the data
    """
    import requests
    
    payload = payload or {}
    
    try:
        resp = requests.post(api_url, json=payload, timeout=timeout)
        resp.raise_for_status()
        
        body = resp.json()
        if body.get("status") != "ok":
            raise RuntimeError(f"API returned error: {body}")
        
        data = body.get("data", [])
        return pd.DataFrame(data)
        
    except requests.RequestException as e:
        raise RuntimeError(f"API request failed: {e}")
    except (KeyError, ValueError) as e:
        raise RuntimeError(f"Invalid API response: {e}")
