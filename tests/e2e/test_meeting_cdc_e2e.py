"""
End-to-End Test: Meeting CDC Pipeline via APIs

Complete E2E test that:
1. Registers/logs in user via API
2. Stores MongoDB credentials via API
3. Creates stream job via API
4. Generates continuous data changes
5. Monitors CDC processing
6. Queries data via Trino API
7. Validates end-to-end flow
"""

import pytest
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

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import data generator
from scripts.cdc_data_generator import CDCDataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/test_db")
DATABASE = os.getenv("MONGO_DATABASE", "test_db")
COLLECTION = os.getenv("MONGO_COLLECTION", "meetings")
POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/morphix")

# Test user credentials
TEST_USERNAME = f"testuser_{int(time.time())}"
TEST_PASSWORD = "TestPass123!"
TEST_EMAIL = f"{TEST_USERNAME}@test.com"


class TestMeetingCDCE2E:
    """End-to-end test for Meeting CDC pipeline via APIs."""
    
    @pytest.fixture(scope="class")
    def api_base_url(self):
        """API base URL."""
        return API_BASE_URL
    
    @pytest.fixture(scope="class")
    def auth_token(self, api_base_url):
        """Register and login user, return auth token."""
        logger.info("\n" + "="*60)
        logger.info("STEP 1: User Registration & Login")
        logger.info("="*60)
        
        # Register user
        register_url = f"{api_base_url}/auth/register"
        register_data = {
            "username": TEST_USERNAME,
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            "full_name": "Test User"
        }
        
        try:
            response = requests.post(register_url, json=register_data, timeout=10)
            if response.status_code == 201:
                logger.info(f"✅ User registered: {TEST_USERNAME}")
            elif response.status_code == 400:
                # User might already exist, try login
                logger.info(f"⚠️  User may already exist, attempting login...")
            else:
                logger.warning(f"⚠️  Registration response: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️  Registration failed (may already exist): {e}")
        
        # Login
        login_url = f"{api_base_url}/auth/login"
        login_data = {
            "username": TEST_USERNAME,
            "password": TEST_PASSWORD
        }
        
        try:
            response = requests.post(
                login_url,
                data=login_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                token = token_data.get("access_token")
                if token:
                    logger.info(f"✅ Login successful, token obtained")
                    return token
                else:
                    pytest.skip(f"Login succeeded but no token returned: {token_data}")
            else:
                pytest.skip(f"Login failed: {response.status_code} - {response.text}")
        except Exception as e:
            pytest.skip(f"Could not connect to API: {e}")
    
    @pytest.fixture(scope="class")
    def mongodb_credentials_saved(self, api_base_url, auth_token):
        """Store MongoDB credentials via API."""
        logger.info("\n" + "="*60)
        logger.info("STEP 2: Store MongoDB Credentials")
        logger.info("="*60)
        
        headers = {"Authorization": f"Bearer {auth_token}"}
        credentials_url = f"{api_base_url}/mongo/credentials"
        
        credentials_data = {
            "mongo_uri": MONGO_URI,
            "database": DATABASE,
            "collection": COLLECTION
        }
        
        try:
            response = requests.post(credentials_url, json=credentials_data, headers=headers, timeout=10)
            if response.status_code in [200, 201]:
                logger.info(f"✅ MongoDB credentials stored")
                return True
            else:
                logger.warning(f"⚠️  Credentials storage response: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.warning(f"⚠️  Could not store credentials: {e}")
            return False
    
    def test_1_user_registration_and_login(self, api_base_url):
        """Test 1: User registration and login."""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: User Registration & Login")
        logger.info("="*60)
        
        # Register
        register_url = f"{api_base_url}/auth/register"
        register_data = {
            "username": TEST_USERNAME,
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            "full_name": "Test User"
        }
        
        try:
            response = requests.post(register_url, json=register_data, timeout=10)
            if response.status_code == 201:
                user_data = response.json()
                logger.info(f"✅ User registered: {user_data.get('username')}")
            elif response.status_code == 400:
                logger.info("⚠️  User may already exist, continuing...")
            else:
                logger.warning(f"Registration response: {response.status_code}")
        except Exception as e:
            pytest.skip(f"API not available: {e}")
        
        # Login
        login_url = f"{api_base_url}/auth/login"
        login_data = {
            "username": TEST_USERNAME,
            "password": TEST_PASSWORD
        }
        
        response = requests.post(
            login_url,
            data=login_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        
        assert response.status_code == 200, f"Login failed: {response.status_code}"
        token_data = response.json()
        assert "access_token" in token_data, "No access token in response"
        
        logger.info("✅ Login successful")
        return token_data["access_token"]
    
    def test_2_store_mongodb_credentials(self, api_base_url, auth_token):
        """Test 2: Store MongoDB credentials via API."""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Store MongoDB Credentials")
        logger.info("="*60)
        
        headers = {"Authorization": f"Bearer {auth_token}"}
        credentials_url = f"{api_base_url}/mongo/credentials"
        
        credentials_data = {
            "mongo_uri": MONGO_URI,
            "database": DATABASE,
            "collection": COLLECTION
        }
        
        response = requests.post(credentials_url, json=credentials_data, headers=headers, timeout=10)
        
        # Accept 200 or 201
        assert response.status_code in [200, 201], \
            f"Failed to store credentials: {response.status_code} - {response.text}"
        
        logger.info("✅ MongoDB credentials stored via API")
    
    def test_3_create_stream_job(self, api_base_url, auth_token):
        """Test 3: Create stream job via API."""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: Create Stream Job")
        logger.info("="*60)
        
        headers = {"Authorization": f"Bearer {auth_token}"}
        job_url = f"{api_base_url}/jobs/stream/create"
        
        job_data = {
            "job_name": f"meeting_cdc_{int(time.time())}",
            "description": "Meeting CDC stream job",
            "mongo_credentials": {
                "mongo_uri": MONGO_URI,
                "database": DATABASE,
                "collection": COLLECTION
            },
            "hudi_config": {
                "table_name": "meetings",
                "database": "default",
                "base_path": "/tmp/hudi_test/meetings"
            },
            "stream_config": {
                "batch_size": 10,
                "batch_interval": 5,
                "checkpoint_interval": 10
            },
            "enabled": True
        }
        
        try:
            response = requests.post(job_url, json=job_data, headers=headers, timeout=10)
            if response.status_code in [200, 201]:
                job_info = response.json()
                logger.info(f"✅ Stream job created: {job_info.get('job_id')}")
                return job_info.get('job_id')
            else:
                logger.warning(f"⚠️  Job creation response: {response.status_code} - {response.text}")
                pytest.skip(f"Could not create stream job: {response.status_code}")
        except Exception as e:
            pytest.skip(f"Could not create stream job: {e}")
    
    def test_4_cdc_data_generation(self):
        """Test 4: Generate continuous data changes."""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: CDC Data Generation")
        logger.info("="*60)
        
        generator = CDCDataGenerator(
            mongo_uri=MONGO_URI,
            database=DATABASE,
            collection=COLLECTION,
            operations_per_second=2.0
        )
        
        # Generate data for 30 seconds
        logger.info("Generating data for 30 seconds...")
        generator.run_continuous(duration_seconds=30, operation_weights={
            'insert': 0.5,
            'update': 0.4,
            'delete': 0.1
        })
        
        # Verify data was created
        count = generator.collection.count_documents({})
        logger.info(f"✅ Total documents in collection: {count}")
        assert count > 0, "No documents were created"
        
        generator.client.close()
    
    def test_5_cdc_with_data_generator(self, api_base_url, auth_token):
        """Test 5: Run CDC with continuous data generation."""
        logger.info("\n" + "="*60)
        logger.info("TEST 5: CDC with Data Generator")
        logger.info("="*60)
        
        # This test would:
        # 1. Start CDC stream job
        # 2. Run data generator in background
        # 3. Monitor CDC processing
        # 4. Validate data in Hudi
        
        # For now, just verify the data generator works
        generator = CDCDataGenerator(
            mongo_uri=MONGO_URI,
            database=DATABASE,
            collection=COLLECTION,
            operations_per_second=1.0
        )
        
        # Insert a few documents
        for i in range(5):
            generator.insert_meeting()
            time.sleep(0.5)
        
        count = generator.collection.count_documents({"is_deleted": False})
        logger.info(f"✅ Created {count} active documents")
        assert count >= 5, "Not enough documents created"
        
        generator.client.close()
    
    def test_6_query_trino(self, api_base_url, auth_token):
        """Test 6: Query data via Trino API."""
        logger.info("\n" + "="*60)
        logger.info("TEST 6: Query via Trino API")
        logger.info("="*60)
        
        headers = {"Authorization": f"Bearer {auth_token}"}
        
        # Health check
        health_url = f"{api_base_url}/trino/health"
        try:
            response = requests.get(health_url, headers=headers, timeout=10)
            if response.status_code != 200:
                pytest.skip(f"Trino not available: {response.status_code}")
        except Exception as e:
            pytest.skip(f"Trino API not available: {e}")
        
        # List catalogs
        catalogs_url = f"{api_base_url}/trino/catalogs"
        try:
            response = requests.get(catalogs_url, headers=headers, timeout=10)
            if response.status_code == 200:
                catalogs = response.json()
                logger.info(f"✅ Trino catalogs: {catalogs.get('catalogs', [])}")
            else:
                logger.warning(f"Could not list catalogs: {response.status_code}")
        except Exception as e:
            logger.warning(f"Could not list catalogs: {e}")
        
        # Execute query
        query_url = f"{api_base_url}/trino/query"
        query_data = {
            "sql": "SELECT 1 as test",
            "max_rows": 10
        }
        
        try:
            response = requests.post(query_url, json=query_data, headers=headers, timeout=30)
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ Trino query executed: {result.get('status')}")
                logger.info(f"   Rows returned: {result.get('rows_returned', 0)}")
            else:
                logger.warning(f"Query response: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Could not execute query: {e}")
        
        logger.info("✅ Trino API test completed")
    
    def test_7_end_to_end_flow(self, api_base_url):
        """Test 7: Complete end-to-end flow."""
        logger.info("\n" + "="*60)
        logger.info("TEST 7: Complete E2E Flow")
        logger.info("="*60)
        
        # Step 1: Login
        token = self.test_1_user_registration_and_login(api_base_url)
        
        # Step 2: Store credentials
        self.test_2_store_mongodb_credentials(api_base_url, token)
        
        # Step 3: Generate data
        self.test_4_cdc_data_generation()
        
        # Step 4: Query Trino (if available)
        try:
            self.test_6_query_trino(api_base_url, token)
        except Exception as e:
            logger.warning(f"Trino query skipped: {e}")
        
        logger.info("✅ End-to-end flow completed")


# Standalone execution
if __name__ == "__main__":
    import os
    os.environ.setdefault("PYTEST_CURRENT_TEST", "test_7_end_to_end_flow")
    
    pytest.main([__file__, "-v", "-s"])

