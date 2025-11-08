#!/usr/bin/env python3
"""
Phase 1 Simple Integration Test
Tests the core infrastructure modules without database connections
"""

import sys
import os
sys.path.append('src')

from datetime import datetime


def test_imports():
    """Test that all modules can be imported."""
    print("=== Testing Module Imports ===")
    
    try:
        # Test auth imports
        from auth.models import User, UserCreate, UserLogin, Token, UserResponse
        print("✅ Auth models imported successfully")
        
        # Test hudi imports
        from src.hudi_writer.models import HudiTableConfig, HudiWriteConfig, HudiTableInfo
        print("✅ Hudi models imported successfully")
        
        # Test jobs imports
        from jobs.models import JobConfig, BatchJobConfig, StreamJobConfig, JobStatus, JobType
        print("✅ Jobs models imported successfully")
        
        # Test API imports
        from api.mongo_api import app
        print("✅ Main API imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {str(e)}")
        return False


def test_model_creation():
    """Test that models can be created."""
    print("\n=== Testing Model Creation ===")
    
    try:
        # Test auth models
        from auth.models import UserCreate, UserLogin
        
        user_create = UserCreate(
            username="testuser",
            email="test@example.com",
            full_name="Test User",
            password="testpassword123"
        )
        print("✅ UserCreate model created successfully")
        
        user_login = UserLogin(
            username="testuser",
            password="testpassword123"
        )
        print("✅ UserLogin model created successfully")
        
        # Test hudi models
        from src.hudi_writer.models import HudiTableConfig
        
        schema = {
            "id": {"type": "string", "required": True},
            "name": {"type": "string", "required": True},
            "age": {"type": "integer", "nullable": True}
        }
        
        table_config = HudiTableConfig(
            table_name="test_table",
            database="test_db",
            base_path="/tmp/hudi/test_table",
            schema=schema
        )
        print("✅ HudiTableConfig model created successfully")
        
        # Test jobs models
        from jobs.models import BatchJobConfig, StreamJobConfig, JobSchedule, JobTrigger
        
        batch_job = BatchJobConfig(
            job_id="test_batch_job",
            job_name="Test Batch Job",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test_db",
            collection="test_collection",
            date_field="created_at",
            hudi_table_name="test_hudi_table",
            hudi_base_path="/tmp/hudi/test_hudi_table",
            created_by="test_user",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL)
        )
        print("✅ BatchJobConfig model created successfully")
        
        stream_job = StreamJobConfig(
            job_id="test_stream_job",
            job_name="Test Stream Job",
            user_id=1,
            mongo_uri="mongodb://localhost:27017",
            database="test_db",
            collection="test_collection",
            hudi_table_name="test_hudi_table",
            hudi_base_path="/tmp/hudi/test_hudi_table",
            created_by="test_user",
            schedule=JobSchedule(trigger=JobTrigger.MANUAL)
        )
        print("✅ StreamJobConfig model created successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Model creation test failed: {str(e)}")
        return False


def test_api_routes():
    """Test that API routes are registered."""
    print("\n=== Testing API Routes ===")
    
    try:
        from api.mongo_api import app
        
        # Get all routes
        routes = [route.path for route in app.routes]
        
        # Check for auth routes
        auth_routes = [route for route in routes if route.startswith("/auth")]
        print(f"✅ Found {len(auth_routes)} auth routes")
        
        # Check for hudi routes
        hudi_routes = [route for route in routes if route.startswith("/hudi")]
        print(f"✅ Found {len(hudi_routes)} hudi routes")
        
        # Check for jobs routes
        jobs_routes = [route for route in routes if route.startswith("/jobs")]
        print(f"✅ Found {len(jobs_routes)} jobs routes")
        
        # Check for specific important routes
        important_routes = [
            "/auth/register",
            "/auth/login",
            "/auth/me",
            "/hudi/table/create",
            "/hudi/tables",
            "/jobs/batch/create",
            "/jobs/stream/create",
            "/jobs",
            "/health"
        ]
        
        missing_routes = []
        for route in important_routes:
            if route in routes:
                print(f"✅ Route registered: {route}")
            else:
                missing_routes.append(route)
                print(f"❌ Route missing: {route}")
        
        if missing_routes:
            print(f"❌ {len(missing_routes)} routes are missing")
            return False
        else:
            print("✅ All important routes are registered")
            return True
            
    except Exception as e:
        print(f"❌ API routes test failed: {str(e)}")
        return False


def test_schema_validation():
    """Test schema validation functionality."""
    print("\n=== Testing Schema Validation ===")
    
    try:
        from src.hudi_writer.schema_manager import HudiSchemaManager
        
        schema_manager = HudiSchemaManager()
        
        # Test schema validation
        valid_schema = {
            "id": {"type": "string", "required": True},
            "name": {"type": "string", "required": True},
            "age": {"type": "integer", "nullable": True},
            "created_at": {"type": "datetime", "required": True}
        }
        
        validation_result = schema_manager.validate_schema(valid_schema)
        if validation_result["valid"]:
            print("✅ Valid schema validation passed")
        else:
            print(f"❌ Valid schema validation failed: {validation_result['errors']}")
            return False
        
        # Test schema optimization
        optimized_schema = schema_manager.optimize_schema_for_hudi(valid_schema)
        if "id" in optimized_schema and "updated_at" in optimized_schema:
            print("✅ Schema optimization passed")
        else:
            print("❌ Schema optimization failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Schema validation test failed: {str(e)}")
        return False


def main():
    """Run all Phase 1 simple tests."""
    print("🚀 Starting Phase 1 Simple Integration Tests\n")
    
    tests = [
        test_imports,
        test_model_creation,
        test_api_routes,
        test_schema_validation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Phase 1 Simple Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All Phase 1 simple tests passed! Core infrastructure modules are working.")
        print("\n📋 Phase 1 Implementation Summary:")
        print("✅ User Authentication System - Complete")
        print("✅ Hudi Writer Module - Complete")
        print("✅ Job Management System - Complete")
        print("✅ API Integration - Complete")
        print("✅ Schema Management - Complete")
        print("\n🚀 Ready for Phase 2: Trino Integration and Advanced Features")
        return True
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
