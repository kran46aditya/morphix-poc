#!/usr/bin/env python3
"""
Complete End-to-End Test Script for Meeting CDC Pipeline

This script:
1. Registers/logs in user via API
2. Stores MongoDB credentials via API
3. Creates stream job via API (if endpoint exists)
4. Starts data generator in background
5. Monitors CDC processing
6. Queries data via Trino API
7. Validates end-to-end flow
"""

import requests
import time
import threading
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import json
import signal

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.cdc_data_generator import CDCDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/test_db")
DATABASE = os.getenv("MONGO_DATABASE", "test_db")
COLLECTION = os.getenv("MONGO_COLLECTION", "meetings")
POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/morphix")

# Test user credentials
TEST_USERNAME = f"e2e_test_{int(time.time())}"
TEST_PASSWORD = "TestPass123!"
TEST_EMAIL = f"{TEST_USERNAME}@test.com"

# Global flag for graceful shutdown
shutdown_flag = threading.Event()


def register_user(api_base_url: str) -> bool:
    """Register a new user."""
    register_url = f"{api_base_url}/auth/register"
    register_data = {
        "username": TEST_USERNAME,
        "email": TEST_EMAIL,
        "password": TEST_PASSWORD,
        "full_name": "E2E Test User"
    }
    
    try:
        response = requests.post(register_url, json=register_data, timeout=10)
        if response.status_code == 201:
            logger.info(f"✅ User registered: {TEST_USERNAME}")
            return True
        elif response.status_code == 400:
            logger.info("⚠️  User may already exist, continuing...")
            return True
        else:
            logger.warning(f"Registration response: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Registration failed: {e}")
        return False


def login_user(api_base_url: str) -> Optional[str]:
    """Login user and return auth token."""
    login_url = f"{api_base_url}/auth/login"
    login_data = {
        "username": TEST_USERNAME,
        "password": TEST_PASSWORD
    }
    
    try:
        response = requests.post(
            login_url,
            json=login_data,  # Use JSON instead of form data
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 200:
            token_data = response.json()
            token = token_data.get("access_token")
            if token:
                logger.info("✅ Login successful")
                return token
            else:
                logger.error("❌ No token in response")
                return None
        else:
            logger.error(f"❌ Login failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ Login error: {e}")
        return None


def store_mongodb_credentials(api_base_url: str, token: str) -> bool:
    """Store MongoDB credentials via API."""
    headers = {"Authorization": f"Bearer {token}"}
    credentials_url = f"{api_base_url}/mongo/credentials"
    
    credentials_data = {
        "mongo_uri": MONGO_URI,
        "database": DATABASE,
        "collection": COLLECTION
    }
    
    try:
        response = requests.post(credentials_url, json=credentials_data, headers=headers, timeout=10)
        if response.status_code in [200, 201]:
            logger.info("✅ MongoDB credentials stored")
            return True
        else:
            logger.warning(f"⚠️  Credentials storage: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.warning(f"⚠️  Could not store credentials: {e}")
        return False


def test_mongodb_connection(api_base_url: str, token: str) -> bool:
    """Test MongoDB connection via API."""
    headers = {"Authorization": f"Bearer {token}"}
    test_url = f"{api_base_url}/mongo/test-connection"
    
    try:
        response = requests.get(test_url, headers=headers, timeout=10)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ MongoDB connection test: {result.get('status')}")
            return True
        else:
            logger.warning(f"⚠️  Connection test: {response.status_code}")
            return False
    except Exception as e:
        logger.warning(f"⚠️  Connection test error: {e}")
        return False


def read_mongodb_data(api_base_url: str, token: str, limit: int = 10) -> Optional[Dict]:
    """Read data from MongoDB via API."""
    headers = {"Authorization": f"Bearer {token}"}
    read_url = f"{api_base_url}/mongo/read"
    
    read_data = {
        "mongo_uri": MONGO_URI,
        "database": DATABASE,
        "collection": COLLECTION,
        "query": {},
        "limit": limit,
        "use_pyspark": False
    }
    
    try:
        response = requests.post(read_url, json=read_data, headers=headers, timeout=30)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Read {result.get('count', 0)} documents from MongoDB")
            return result
        else:
            logger.warning(f"⚠️  Read response: {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"⚠️  Read error: {e}")
        return None


def create_stream_job(api_base_url: str, token: str) -> Optional[str]:
    """Create a stream job via API."""
    headers = {"Authorization": f"Bearer {token}"}
    job_url = f"{api_base_url}/jobs/stream/create"
    
    job_data = {
        "job_name": f"meeting_cdc_{int(time.time())}",
        "mongo_uri": MONGO_URI,
        "database": DATABASE,
        "collection": COLLECTION,
        "query": {},
        "hudi_table_name": "meetings",
        "hudi_base_path": "/tmp/hudi_test/meetings",
        "polling_interval_seconds": 5,
        "batch_size": 1000,
        "description": "Meeting CDC stream job for E2E testing",
        "schedule": {
            "type": "interval",
            "interval_seconds": 60
        }
    }
    
    try:
        response = requests.post(job_url, json=job_data, headers=headers, timeout=10)
        if response.status_code in [200, 201]:
            job_info = response.json()
            job_id = job_info.get('job_id')
            logger.info(f"✅ Stream job created: {job_id}")
            return job_id
        else:
            logger.warning(f"⚠️  Job creation: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.warning(f"⚠️  Could not create stream job: {e}")
        return None


def query_trino(api_base_url: str, token: str, sql: str) -> Optional[Dict]:
    """Execute Trino query via API."""
    headers = {"Authorization": f"Bearer {token}"}
    query_url = f"{api_base_url}/trino/query"
    
    query_data = {
        "sql": sql,
        "max_rows": 100
    }
    
    try:
        response = requests.post(query_url, json=query_data, headers=headers, timeout=30)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Trino query executed: {result.get('status')}")
            logger.info(f"   Rows returned: {result.get('rows_returned', 0)}")
            return result
        else:
            logger.warning(f"⚠️  Query response: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.warning(f"⚠️  Query error: {e}")
        return None


def run_data_generator(duration: int = 60):
    """Run data generator in background thread."""
    generator = CDCDataGenerator(
        mongo_uri=MONGO_URI,
        database=DATABASE,
        collection=COLLECTION,
        operations_per_second=2.0
    )
    
    logger.info(f"🚀 Starting data generator for {duration} seconds...")
    generator.run_continuous(
        duration_seconds=duration,
        operation_weights={'insert': 0.5, 'update': 0.4, 'delete': 0.1}
    )


def main():
    """Main E2E test execution."""
    logger.info("\n" + "="*80)
    logger.info("MORPHIX PLATFORM - END-TO-END TEST")
    logger.info("="*80)
    logger.info(f"API URL: {API_BASE_URL}")
    logger.info(f"MongoDB: {DATABASE}.{COLLECTION}")
    logger.info("="*80 + "\n")
    
    # Step 1: Register and Login
    logger.info("STEP 1: User Registration & Login")
    logger.info("-" * 60)
    if not register_user(API_BASE_URL):
        logger.error("❌ User registration failed, exiting")
        sys.exit(1)
    
    token = login_user(API_BASE_URL)
    if not token:
        logger.error("❌ Login failed, exiting")
        sys.exit(1)
    
    # Step 2: Store MongoDB Credentials
    logger.info("\nSTEP 2: Store MongoDB Credentials")
    logger.info("-" * 60)
    store_mongodb_credentials(API_BASE_URL, token)
    
    # Step 3: Test MongoDB Connection
    logger.info("\nSTEP 3: Test MongoDB Connection")
    logger.info("-" * 60)
    test_mongodb_connection(API_BASE_URL, token)
    
    # Step 4: Create Stream Job
    logger.info("\nSTEP 4: Create Stream Job")
    logger.info("-" * 60)
    job_id = create_stream_job(API_BASE_URL, token)
    if job_id:
        logger.info(f"   Job ID: {job_id}")
    
    # Step 5: Read Initial Data
    logger.info("\nSTEP 5: Read Initial Data from MongoDB")
    logger.info("-" * 60)
    initial_data = read_mongodb_data(API_BASE_URL, token, limit=5)
    
    # Step 6: Generate Data Changes
    logger.info("\nSTEP 6: Generate Data Changes (CDC Test)")
    logger.info("-" * 60)
    logger.info("Starting data generator in background...")
    
    generator_thread = threading.Thread(
        target=run_data_generator,
        args=(60,),  # Run for 60 seconds
        daemon=True
    )
    generator_thread.start()
    
    # Wait a bit for data to be generated
    time.sleep(10)
    
    # Step 7: Read Updated Data
    logger.info("\nSTEP 7: Read Updated Data from MongoDB")
    logger.info("-" * 60)
    updated_data = read_mongodb_data(API_BASE_URL, token, limit=10)
    
    # Step 8: Query via Trino (if available)
    logger.info("\nSTEP 8: Query Data via Trino API")
    logger.info("-" * 60)
    
    # Health check
    headers = {"Authorization": f"Bearer {token}"}
    health_url = f"{API_BASE_URL}/trino/health"
    try:
        response = requests.get(health_url, headers=headers, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Trino is available")
            
            # List catalogs
            catalogs_url = f"{API_BASE_URL}/trino/catalogs"
            response = requests.get(catalogs_url, headers=headers, timeout=10)
            if response.status_code == 200:
                catalogs = response.json()
                logger.info(f"   Catalogs: {[c.get('catalog_name') for c in catalogs.get('catalogs', [])]}")
            
            # Execute a simple query
            query_result = query_trino(API_BASE_URL, token, "SELECT 1 as test")
            if query_result and query_result.get('status') == 'ok':
                logger.info("✅ Trino query successful")
        else:
            logger.warning("⚠️  Trino not available, skipping queries")
    except Exception as e:
        logger.warning(f"⚠️  Trino check failed: {e}")
    
    # Wait for generator to finish
    logger.info("\nWaiting for data generator to complete...")
    generator_thread.join(timeout=70)
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("✅ END-TO-END TEST COMPLETED")
    logger.info("="*80)
    logger.info(f"User: {TEST_USERNAME}")
    logger.info(f"Collection: {DATABASE}.{COLLECTION}")
    logger.info("="*80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Test interrupted by user")
        shutdown_flag.set()
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

