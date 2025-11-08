"""
ETL Module 1: MongoDB Data Reader

This module handles reading data from MongoDB and converting it into DataFrames.
It supports both direct connection and API-based reading.
"""

from typing import Optional, Dict, Any, Union, Tuple
import json
import pandas as pd
from ..mongodb import connection as mongo_conn


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
    
    def read_to_pandas(self, query: Optional[Dict[str, Any]] = None, limit: int = 1000) -> pd.DataFrame:
        """Read MongoDB data into a pandas DataFrame using pymongo.
        
        Args:
            query: MongoDB query filter
            limit: Maximum number of documents to read
            
        Returns:
            pandas DataFrame containing the data
        """
        docs = mongo_conn.read_with_pymongo(
            mongo_uri=self.mongo_uri,
            database=self.database,
            collection=self.collection,
            query=query or {},
            limit=limit
        )
        
        if not docs:
            return pd.DataFrame()
        
        return pd.DataFrame(docs)
    
    def read_to_spark(self, query: Optional[Dict[str, Any]] = None, limit: Optional[int] = None):
        """Read MongoDB data into a Spark DataFrame.
        
        Args:
            query: MongoDB query filter
            limit: Maximum number of documents to read (optional)
            
        Returns:
            Tuple of (Spark DataFrame, SparkSession)
        """
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
            
        return df, spark
    
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
