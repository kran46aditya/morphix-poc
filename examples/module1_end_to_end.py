#!/usr/bin/env python3
"""
End-to-End Example: Module 1 - MongoDB Data Reader

This script demonstrates the complete usage of the first ETL module,
which reads data from MongoDB and converts it to DataFrames.

Usage examples:
1. Direct connection with credentials
2. Connection via MongoDB URI 
3. Reading via API endpoint
4. Both pandas and Spark DataFrame outputs
"""

import sys
import os
sys.path.append('src')

from etl import MongoDataReader, create_reader_from_connection_info, create_reader_from_credentials, read_via_api


def example_1_direct_credentials():
    """Example 1: Create reader using individual MongoDB credentials."""
    print("=== Example 1: Direct Credentials ===")
    
    # Create reader using individual credentials
    reader = create_reader_from_credentials(
        username="myuser",
        password="mypassword", 
        host="localhost",
        port=27017,
        database="sample_db",
        collection="users"
    )
    
    print(f"Created reader for: {reader.database}.{reader.collection}")
    print(f"MongoDB URI: {reader.mongo_uri}")
    
    # Note: This would actually connect to MongoDB if it was running
    # For demo purposes, we'll just show the setup
    try:
        # This would read data into pandas DataFrame
        # df = reader.read_to_pandas(query={"status": "active"}, limit=100)
        # print(f"Would read data into pandas DataFrame with {len(df)} rows")
        print("Reader configured successfully for pandas DataFrame output")
    except Exception as e:
        print(f"Connection would fail (expected in demo): {e}")


def example_2_connection_uri():
    """Example 2: Create reader using MongoDB URI."""
    print("\n=== Example 2: MongoDB URI ===")
    
    # Create reader using full MongoDB URI
    reader = create_reader_from_connection_info(
        mongo_uri="mongodb://myuser:mypassword@localhost:27017/sample_db?authSource=admin",
        database="sample_db",
        collection="products"
    )
    
    print(f"Created reader for: {reader.database}.{reader.collection}")
    
    try:
        # This would read data into Spark DataFrame
        # df, spark_session = reader.read_to_spark(query={"price": {"$gte": 100}})
        # print(f"Would read data into Spark DataFrame")
        # spark_session.stop()  # Clean up Spark session
        print("Reader configured successfully for Spark DataFrame output")
    except Exception as e:
        print(f"Spark connection would fail (expected in demo): {e}")


def example_3_unified_read_method():
    """Example 3: Using the unified read method."""
    print("\n=== Example 3: Unified Read Method ===")
    
    reader = MongoDataReader(
        mongo_uri="mongodb://localhost:27017",
        database="analytics",
        collection="events"
    )
    
    print("Configured reader with unified read method")
    
    # Example of how to choose between pandas and Spark
    use_spark_for_large_data = False  # This could be based on data size or user preference
    
    if use_spark_for_large_data:
        print("Would use Spark for large dataset processing")
        # result = reader.read(use_spark=True, query={"event_type": "click"}, limit=10000)
        # df, spark_session = result
    else:
        print("Would use pandas for smaller dataset processing") 
        # df = reader.read(use_spark=False, query={"event_type": "click"}, limit=1000)


def example_4_api_based_reading():
    """Example 4: Reading data via API endpoint."""
    print("\n=== Example 4: API-Based Reading ===")
    
    # This assumes the FastAPI server is running on localhost:8000
    api_payload = {
        "user_id": "analytics_user",  # Use stored credentials
        # OR provide direct connection info:
        # "mongo_uri": "mongodb://localhost:27017",
        # "database": "sample_db", 
        # "collection": "users",
        # "query": {"status": "active"}
    }
    
    try:
        # This would call the API and return pandas DataFrame
        # df = read_via_api(
        #     api_url="http://localhost:8000/mongo/read",
        #     payload=api_payload,
        #     timeout=30
        # )
        # print(f"Would read {len(df)} documents via API")
        print("API-based reading configured successfully")
    except Exception as e:
        print(f"API call would fail (expected when server not running): {e}")


def example_5_error_handling():
    """Example 5: Proper error handling patterns."""
    print("\n=== Example 5: Error Handling ===")
    
    reader = MongoDataReader(
        mongo_uri="mongodb://invalid:27017", 
        database="test_db",
        collection="test_collection"
    )
    
    try:
        # df = reader.read_to_pandas(limit=10)
        print("Would handle connection errors gracefully")
    except Exception as e:
        print(f"Proper error handling for: {type(e).__name__}")
    
    # API error handling example
    try:
        # df = read_via_api(
        #     api_url="http://nonexistent-server:8000/mongo/read",
        #     payload={"user_id": "test"}
        # )
        print("Would handle API errors gracefully")
    except Exception as e:
        print(f"Proper error handling for API calls: {type(e).__name__}")


def main():
    """Run all examples to demonstrate Module 1 capabilities."""
    print("MongoDB Data Reader - Module 1 End-to-End Examples")
    print("=" * 55)
    
    example_1_direct_credentials()
    example_2_connection_uri() 
    example_3_unified_read_method()
    example_4_api_based_reading()
    example_5_error_handling()
    
    print("\n" + "=" * 55)
    print("Examples completed successfully!")
    print("\nTo run with real data:")
    print("1. Start MongoDB server")
    print("2. Start FastAPI server: uvicorn src.api.mongo_api:app --reload --port 8000")
    print("3. Update connection details in examples")
    print("4. Uncomment the actual data reading lines")


if __name__ == "__main__":
    main()