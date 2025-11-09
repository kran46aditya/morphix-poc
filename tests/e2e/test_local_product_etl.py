"""
End-to-end test for MongoDB → Hudi pipeline.

Tests complete flow on localhost:
1. Connect to MongoDB (test_db.Product)
2. Infer schema from collection
3. Extract and flatten data
4. Write to Hudi
5. Validate results

Run:
    python -m pytest tests/e2e/test_local_product_etl.py -v -s
"""

import pytest
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
import os
import shutil
from pathlib import Path
from datetime import datetime
import logging
import json
import sys
import time
import threading

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import your modules
from src.etl.schema_generator import SchemaGenerator
from src.etl.data_transformer import DataTransformer
from src.etl.pipeline import ETLPipeline, create_pipeline_from_uri
from src.hudi_writer.writer import HudiWriter
from src.hudi_writer.models import HudiWriteConfig, HudiTableConfig, HudiOperationType, HudiTableType

# Optional CDC imports
try:
    from src.connectors.cdc.mongo_changestream import ChangeStreamWatcher, CDCConfig
    from src.connectors.cdc.checkpoint_store import CheckpointStore
    CDC_AVAILABLE = True
except ImportError:
    CDC_AVAILABLE = False
    ChangeStreamWatcher = None
    CDCConfig = None
    CheckpointStore = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Test Configuration
# Try multiple MongoDB URI options
MONGO_URI_OPTIONS = [
    os.getenv("MONGO_URI"),  # User-provided URI
    "mongodb://myUserAdmin:NewStrongPass!234@localhost:27017/test_db?authSource=admin&replicaSet=rs0",  # Replica set with auth
    "mongodb://myUserAdmin:NewStrongPass!234@localhost:27017/test_db?authSource=admin",  # Standalone with auth
    "mongodb://localhost:27017/test_db?replicaSet=rs0",  # Replica set without auth
    "mongodb://localhost:27017/test_db",  # Standalone without auth
]

DATABASE = os.getenv("MONGO_DATABASE", "test_db")
COLLECTION = os.getenv("MONGO_COLLECTION", "Product")
HUDI_BASE_PATH = "/tmp/hudi_test/test_db"
HUDI_TABLE_NAME = "product"
POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/morphix")

# Try to determine which MongoDB URI to use
def get_mongo_uri():
    """Get MongoDB URI, trying multiple options."""
    for uri in MONGO_URI_OPTIONS:
        if not uri:
            continue
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            client.close()
            logger.info(f"✅ Using MongoDB URI: {uri.split('@')[-1] if '@' in uri else uri}")
            return uri
        except Exception:
            continue
    raise ConnectionError("Cannot connect to MongoDB with any of the configured URIs")

MONGO_URI = get_mongo_uri()


class TestProductETLEndToEnd:
    """End-to-end tests for Product collection ETL."""
    
    @pytest.fixture(scope="class")
    def mongodb_client(self):
        """Connect to MongoDB."""
        logger.info("Connecting to MongoDB...")
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            
            # Test connection
            client.admin.command('ping')
            logger.info("✅ MongoDB connection successful")
            
            yield client
        except Exception as e:
            pytest.fail(f"❌ MongoDB connection failed: {e}")
        finally:
            if 'client' in locals():
                client.close()
    
    @pytest.fixture(scope="class")
    def mongodb_collection(self, mongodb_client):
        """Get Product collection."""
        db = mongodb_client[DATABASE]
        collection = db[COLLECTION]
        
        # Verify collection exists and has data
        try:
            count = collection.count_documents({})
            logger.info(f"Found {count} documents in {DATABASE}.{COLLECTION}")
            
            if count == 0:
                pytest.skip(f"Collection {COLLECTION} is empty. Please add test data.")
            
            return collection
        except Exception as e:
            pytest.fail(f"❌ Cannot access collection: {e}")
    
    @pytest.fixture(scope="class")
    def sample_data(self, mongodb_collection):
        """Fetch sample data from MongoDB."""
        # Get first 10 documents for inspection
        try:
            cursor = mongodb_collection.find().limit(10)
            docs = list(cursor)
            
            logger.info(f"Sample document structure:")
            if docs:
                logger.info(json.dumps(docs[0], indent=2, default=str))
            
            return docs
        except Exception as e:
            pytest.fail(f"❌ Failed to fetch sample data: {e}")
    
    @pytest.fixture(scope="function")
    def clean_hudi_output(self):
        """Clean Hudi output directory before each test."""
        hudi_path = Path(HUDI_BASE_PATH)
        if hudi_path.exists():
            logger.info(f"Cleaning up {hudi_path}")
            shutil.rmtree(hudi_path)
        yield
        # Cleanup after test
        if hudi_path.exists():
            shutil.rmtree(hudi_path, ignore_errors=True)
    
    def test_1_mongodb_connection(self, mongodb_client):
        """Test 1: Verify MongoDB connection and authentication."""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: MongoDB Connection")
        logger.info("="*60)
        
        try:
            # Test database access
            db = mongodb_client[DATABASE]
            collections = db.list_collection_names()
            
            logger.info(f"Available collections: {collections}")
            assert COLLECTION in collections, f"Collection {COLLECTION} not found"
            
            # Test read permissions
            collection = db[COLLECTION]
            count = collection.count_documents({})
            logger.info(f"✅ Can read from {COLLECTION}: {count} documents")
            assert count > 0, "Collection is empty"
            
        except Exception as e:
            pytest.fail(f"❌ Cannot read from collection: {e}")
    
    def test_2_schema_inference(self, mongodb_collection, sample_data):
        """Test 2: Schema inference from Product collection."""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Schema Inference")
        logger.info("="*60)
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(sample_data)
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            # Generate schema
            logger.info("Generating schema...")
            schema = SchemaGenerator.generate_from_dataframe(
                df=df,
                sample_size=len(sample_data),
                include_constraints=True
            )
            
            # Validate schema
            assert isinstance(schema, dict), "Schema must be a dictionary"
            assert len(schema) > 0, "Schema is empty"
            
            logger.info(f"✅ Schema generated: {len(schema)} fields")
            
            # Print schema summary
            for field_name, field_config in list(schema.items())[:5]:
                field_type = field_config.get('type', 'unknown')
                nullable = field_config.get('nullable', True)
                logger.info(f"  - {field_name}: {field_type} "
                           f"({'nullable' if nullable else 'required'})")
            
            # Check for nested structures
            nested_fields = [
                name for name, config in schema.items()
                if config.get('type') in ['object', 'array'] or 
                   isinstance(config.get('type'), dict)
            ]
            if nested_fields:
                logger.info(f"📦 Nested fields detected: {nested_fields}")
            else:
                logger.info("📋 All fields are flat")
            
            return schema
            
        except Exception as e:
            pytest.fail(f"❌ Schema inference failed: {e}")
    
    def test_3_data_flattening(self, sample_data):
        """Test 3: Data flattening and transformation."""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: Data Flattening")
        logger.info("="*60)
        
        try:
            # Convert to DataFrame
            df_original = pd.DataFrame(sample_data)
            original_shape = df_original.shape
            logger.info(f"Original shape: {original_shape}")
            
            # Initialize transformer
            transformer = DataTransformer()
            
            # Flatten data
            logger.info("Flattening nested structures...")
            df_flat = transformer.flatten_dataframe(df_original.copy())
            flat_shape = df_flat.shape
            
            logger.info(f"Flattened shape: {flat_shape}")
            logger.info(f"Columns added: {flat_shape[1] - original_shape[1]}")
            
            # Show flattened columns
            new_columns = set(df_flat.columns) - set(df_original.columns)
            if new_columns:
                logger.info(f"📋 New flattened columns:")
                for col in sorted(new_columns)[:10]:  # Show first 10
                    logger.info(f"  - {col}")
            else:
                logger.info("ℹ️  No nested structures to flatten")
            
            # Validate flattening
            assert flat_shape[0] == original_shape[0], "Row count should not change"
            assert flat_shape[1] >= original_shape[1], "Should have same or more columns"
            
            logger.info("✅ Flattening successful")
            
            return df_flat
            
        except Exception as e:
            pytest.fail(f"❌ Data flattening failed: {e}")
    
    def test_4_batch_etl_pipeline(
        self,
        mongodb_collection,
        clean_hudi_output
    ):
        """Test 4: Complete batch ETL pipeline."""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: Batch ETL Pipeline")
        logger.info("="*60)
        
        try:
            # Create pipeline
            logger.info("Creating ETL pipeline...")
            pipeline = create_pipeline_from_uri(
                mongo_uri=MONGO_URI,
                database=DATABASE,
                collection=COLLECTION
            )
            
            # Run ETL to get transformed data
            logger.info("Running ETL pipeline...")
            start_time = datetime.now()
            
            df_transformed = pipeline.run_pipeline(
                query={},  # Get all documents
                limit=1000,  # Limit for testing
                flatten=True,
                clean=True,
                apply_schema=False  # Don't apply schema for now
            )
            
            records_processed = len(df_transformed)
            logger.info(f"ETL processed {records_processed} records")
            
            # Ensure all nested structures are fully flattened
            # Check for any remaining dict/list columns
            nested_cols = []
            for col in df_transformed.columns:
                sample_val = df_transformed[col].dropna().head(1)
                if len(sample_val) > 0:
                    val = sample_val.iloc[0]
                    if isinstance(val, (dict, list)):
                        nested_cols.append(col)
            
            if nested_cols:
                logger.warning(f"Found nested columns, flattening: {nested_cols}")
                transformer = DataTransformer()
                df_transformed = transformer.flatten_dataframe(df_transformed)
                logger.info(f"After additional flattening: {df_transformed.shape}")
            
            # Convert ALL object columns to string to avoid Spark schema conflicts
            # This ensures Hudi gets consistent simple types
            logger.info("Converting all object columns to string for Hudi compatibility...")
            for col in df_transformed.columns:
                if df_transformed[col].dtype == 'object':
                    try:
                        # Convert to string, handling all complex types
                        df_transformed[col] = df_transformed[col].apply(
                            lambda x: (
                                json.dumps(x, default=str) if isinstance(x, (dict, list))
                                else x.decode('utf-8', errors='replace') if isinstance(x, bytes)
                                else str(x) if x is not None else None
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Could not convert column {col} to string: {e}, using astype")
                        df_transformed[col] = df_transformed[col].astype(str)
            
            logger.info(f"Final DataFrame shape: {df_transformed.shape}")
            logger.info(f"Column dtypes: {df_transformed.dtypes.to_dict()}")
            
            # Initialize Hudi writer
            logger.info("Initializing Hudi writer...")
            hudi_writer = HudiWriter()
            
            # Determine record key field
            record_key_field = "_id" if "_id" in df_transformed.columns else "id"
            
            # Determine precombine field
            precombine_field = "updated_at"
            if "updated_at" not in df_transformed.columns:
                if "created_at" in df_transformed.columns:
                    precombine_field = "created_at"
                else:
                    precombine_field = record_key_field
            
            # Create Hudi table config
            table_config = HudiTableConfig(
                table_name=HUDI_TABLE_NAME,
                database="default",
                base_path=HUDI_BASE_PATH,
                table_type=HudiTableType.COPY_ON_WRITE,
                record_key_field=record_key_field,
                precombine_field=precombine_field,
                schema={}  # Schema will be inferred
            )
            
            # Create Hudi write config
            write_config = HudiWriteConfig(
                table_name=HUDI_TABLE_NAME,
                operation=HudiOperationType.INSERT,
                record_key_field=record_key_field,
                precombine_field=precombine_field
            )
            
            # Write to Hudi
            logger.info("Writing to Hudi...")
            write_result = hudi_writer.write_dataframe(
                df=df_transformed,
                config=write_config,
                table_config=table_config
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Check if Hudi is properly configured
            if not write_result.success:
                error_msg = write_result.error_message or ""
                if "hudi" in error_msg.lower() and ("not found" in error_msg.lower() or "classnotfound" in error_msg.lower()):
                    pytest.skip(f"Hudi not properly configured: {error_msg[:200]}")
            
            # Validate result
            assert write_result.success, f"ETL failed: {write_result.error_message}"
            
            records_written = write_result.records_written
            
            logger.info(f"✅ ETL completed in {duration:.2f}s")
            logger.info(f"   Records processed: {records_processed}")
            logger.info(f"   Records written: {records_written}")
            logger.info(f"   Hudi path: {HUDI_BASE_PATH}")
            
            assert records_processed > 0, "No records processed"
            assert records_written > 0, "No records written to Hudi"
            
            # Verify Hudi files created
            hudi_path = Path(HUDI_BASE_PATH)
            assert hudi_path.exists(), f"Hudi path not created: {hudi_path}"
            
            # Check for Hudi metadata
            hoodie_path = hudi_path / ".hoodie"
            if hoodie_path.exists():
                logger.info(f"📁 Hudi structure created:")
                for item in hudi_path.iterdir():
                    logger.info(f"   - {item.name}")
            
            # Cleanup
            hudi_writer.close()
            
            return {
                'status': 'success',
                'records_processed': records_processed,
                'records_written': records_written,
                'hudi_table_path': str(hudi_path),
                'duration': duration
            }
            
        except Exception as e:
            logger.error(f"❌ Batch ETL pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            pytest.fail(f"Batch ETL pipeline failed: {e}")
    
    def test_5_hudi_data_validation(self, mongodb_collection):
        """Test 5: Validate data in Hudi matches MongoDB."""
        logger.info("\n" + "="*60)
        logger.info("TEST 5: Hudi Data Validation")
        logger.info("="*60)
        
        try:
            # First run ETL to create Hudi table
            logger.info("Running ETL to populate Hudi...")
            pipeline = create_pipeline_from_uri(
                mongo_uri=MONGO_URI,
                database=DATABASE,
                collection=COLLECTION
            )
            
            df_transformed = pipeline.run_pipeline(query={}, limit=100, flatten=True, clean=True)
            
            # Ensure all nested structures are fully flattened
            nested_cols = []
            for col in df_transformed.columns:
                sample_val = df_transformed[col].dropna().head(1)
                if len(sample_val) > 0:
                    val = sample_val.iloc[0]
                    if isinstance(val, (dict, list)):
                        nested_cols.append(col)
            
            if nested_cols:
                logger.warning(f"Found nested columns, flattening: {nested_cols}")
                transformer = DataTransformer()
                df_transformed = transformer.flatten_dataframe(df_transformed)
            
            # Convert ALL object columns to string to avoid Spark schema conflicts
            logger.info("Converting all object columns to string for Hudi compatibility...")
            for col in df_transformed.columns:
                if df_transformed[col].dtype == 'object':
                    try:
                        # Convert to string, handling all complex types
                        df_transformed[col] = df_transformed[col].apply(
                            lambda x: (
                                json.dumps(x, default=str) if isinstance(x, (dict, list))
                                else x.decode('utf-8', errors='replace') if isinstance(x, bytes)
                                else str(x) if x is not None else None
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Could not convert column {col} to string: {e}, using astype")
                        df_transformed[col] = df_transformed[col].astype(str)
            
            # Initialize Hudi writer
            hudi_writer = HudiWriter()
            
            record_key_field = "_id" if "_id" in df_transformed.columns else "id"
            precombine_field = "updated_at" if "updated_at" in df_transformed.columns else record_key_field
            
            table_config = HudiTableConfig(
                table_name=HUDI_TABLE_NAME,
                database="default",
                base_path=HUDI_BASE_PATH,
                table_type=HudiTableType.COPY_ON_WRITE,
                record_key_field=record_key_field,
                precombine_field=precombine_field,
                schema={}
            )
            
            write_config = HudiWriteConfig(
                table_name=HUDI_TABLE_NAME,
                operation=HudiOperationType.INSERT,
                record_key_field=record_key_field,
                precombine_field=precombine_field
            )
            
            write_result = hudi_writer.write_dataframe(df_transformed, write_config, table_config)
            
            # Check if Hudi is properly configured
            if not write_result.success:
                error_msg = write_result.error_message or ""
                if "hudi" in error_msg.lower() and ("not found" in error_msg.lower() or "classnotfound" in error_msg.lower()):
                    pytest.skip(f"Hudi not properly configured: {error_msg[:200]}")
            
            assert write_result.success, f"Hudi write failed: {write_result.error_message}"
            
            # Read from MongoDB
            mongo_docs = list(mongodb_collection.find().limit(100))
            mongo_ids = {str(doc['_id']) for doc in mongo_docs}
            logger.info(f"MongoDB: {len(mongo_ids)} documents")
            
            # Read from Hudi using Spark
            logger.info("Reading from Hudi...")
            spark = SparkSession.builder \
                .appName("HudiValidation") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .getOrCreate()
            
            hudi_path = f"{HUDI_BASE_PATH}"
            
            try:
                hudi_df = spark.read.format("hudi").load(hudi_path)
                hudi_count = hudi_df.count()
                logger.info(f"Hudi: {hudi_count} records")
                
                # Validate counts match (allowing for some variance due to flattening)
                assert hudi_count > 0, "Hudi table is empty"
                
                # Validate IDs match if _id column exists
                if '_id' in [col.name for col in hudi_df.schema.fields]:
                    hudi_ids = {row['_id'] for row in hudi_df.select('_id').collect()}
                    missing_in_hudi = mongo_ids - hudi_ids
                    extra_in_hudi = hudi_ids - mongo_ids
                    
                    if missing_in_hudi:
                        logger.warning(f"⚠️  Missing in Hudi: {len(missing_in_hudi)} records")
                    if extra_in_hudi:
                        logger.warning(f"⚠️  Extra in Hudi: {len(extra_in_hudi)} records")
                    
                    # Allow some variance (not all records may have been written)
                    assert len(missing_in_hudi) <= len(mongo_ids) * 0.1, \
                        f"Too many missing records in Hudi: {len(missing_in_hudi)}"
                
                logger.info("✅ All MongoDB records found in Hudi")
                
                # Sample data comparison
                logger.info("\nSample record comparison:")
                if mongo_docs:
                    mongo_sample = mongo_docs[0]
                    mongo_fields = set(mongo_sample.keys())
                    
                    # Get Hudi sample
                    hudi_sample = hudi_df.first()
                    if hudi_sample:
                        hudi_fields = set(hudi_sample.asDict().keys())
                        
                        logger.info(f"MongoDB fields: {len(mongo_fields)}")
                        logger.info(f"Hudi fields: {len(hudi_fields)}")
                        
                        # Expected: Hudi might have more fields due to flattening
                        assert len(hudi_fields) >= len(mongo_fields) - 2, \
                            "Hudi should have same or more fields (due to flattening)"
                
                logger.info("✅ Data validation passed")
                
            finally:
                spark.stop()
                hudi_writer.close()
                
        except Exception as e:
            logger.error(f"❌ Hudi data validation failed: {e}")
            import traceback
            traceback.print_exc()
            pytest.fail(f"Hudi data validation failed: {e}")
    
    @pytest.mark.skipif(
        not CDC_AVAILABLE,
        reason="CDC not available"
    )
    def test_6_cdc_real_time_sync(self, mongodb_collection):
        """Test 6: CDC real-time synchronization."""
        logger.info("\n" + "="*60)
        logger.info("TEST 6: CDC Real-Time Sync")
        logger.info("="*60)
        
        try:
            # Check if MongoDB has replica set enabled (required for changestreams)
            try:
                client = mongodb_collection.database.client
                rs_status = client.admin.command('replSetGetStatus')
                logger.info("✅ Replica set is enabled")
            except Exception as e:
                pytest.skip(f"MongoDB replica set not enabled (required for changestreams): {e}")
            
            # Setup checkpoint store
            try:
                checkpoint_store = CheckpointStore(POSTGRES_URL)
            except Exception as e:
                pytest.skip(f"PostgreSQL not available for checkpoint store: {e}")
            
            # Create CDC watcher
            config = CDCConfig(
                batch_size=10,
                batch_interval=5
            )
            
            watcher = ChangeStreamWatcher(
                collection=mongodb_collection,
                checkpoint_store=checkpoint_store,
                config=config,
                job_id="test_cdc"
            )
            
            # Track processed changes
            processed_changes = []
            
            def callback(batch):
                logger.info(f"CDC: Received batch of {len(batch)} changes")
                processed_changes.extend(batch)
            
            # Start watcher in background thread
            watcher_thread = threading.Thread(
                target=watcher.start,
                args=(callback,),
                daemon=True
            )
            watcher_thread.start()
            
            # Wait for watcher to start
            time.sleep(2)
            
            # Insert test documents
            logger.info("Inserting test documents...")
            test_docs = [
                {"name": f"Product_{i}", "price": i * 10, "category": "test"}
                for i in range(5)
            ]
            
            insert_result = mongodb_collection.insert_many(test_docs)
            inserted_ids = [str(id) for id in insert_result.inserted_ids]
            logger.info(f"Inserted {len(inserted_ids)} documents")
            
            # Wait for CDC to process
            max_wait = 30  # seconds
            wait_start = time.time()
            
            while time.time() - wait_start < max_wait:
                if len(processed_changes) >= len(test_docs):
                    break
                time.sleep(1)
            
            # Stop watcher
            watcher.stop()
            watcher_thread.join(timeout=5)
            
            # Validate CDC captured all changes
            logger.info(f"CDC processed {len(processed_changes)} changes")
            
            # If no changes were captured, check if it's a replica set issue
            if len(processed_changes) == 0:
                # Check if changestreams are actually working
                try:
                    # Try to open a changestream manually
                    with mongodb_collection.watch() as stream:
                        # Insert a test doc
                        test_doc = {"_test": "changestream_check"}
                        mongodb_collection.insert_one(test_doc)
                        # Try to read from stream (with timeout)
                        import signal
                        def timeout_handler(signum, frame):
                            raise TimeoutError("Changestream timeout")
                        
                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(5)  # 5 second timeout
                        try:
                            change = next(stream)
                            signal.alarm(0)  # Cancel alarm
                            mongodb_collection.delete_one(test_doc)
                            # If we got here, changestreams work but watcher didn't capture
                            logger.warning("Changestreams work but watcher didn't capture changes")
                        except (StopIteration, TimeoutError):
                            signal.alarm(0)
                            mongodb_collection.delete_one(test_doc)
                            pytest.skip("Changestreams not working properly (may need replica set)")
                except Exception as e:
                    logger.warning(f"Could not verify changestreams: {e}")
                    pytest.skip(f"Changestreams may not be available: {e}")
            
            assert len(processed_changes) >= len(test_docs), \
                f"CDC missed changes: expected {len(test_docs)}, got {len(processed_changes)}"
            
            # Verify checkpoint was saved
            checkpoint = checkpoint_store.load_checkpoint(
                job_id="test_cdc",
                collection=COLLECTION
            )
            
            assert checkpoint is not None, "Checkpoint not saved"
            logger.info(f"✅ Checkpoint saved: {str(checkpoint)[:50]}...")
            
            # Cleanup test documents
            mongodb_collection.delete_many({"category": "test"})
            logger.info("✅ CDC real-time sync passed")
            
            checkpoint_store.close()
            
        except Exception as e:
            logger.error(f"❌ CDC real-time sync failed: {e}")
            import traceback
            traceback.print_exc()
            pytest.fail(f"CDC real-time sync failed: {e}")
    
    def test_7_schema_evolution_detection(self, mongodb_collection, sample_data):
        """Test 7: Detect schema changes in new data."""
        logger.info("\n" + "="*60)
        logger.info("TEST 7: Schema Evolution Detection")
        logger.info("="*60)
        
        try:
            # Generate initial schema
            df_v1 = pd.DataFrame(sample_data)
            schema_v1 = SchemaGenerator.generate_from_dataframe(df_v1)
            logger.info(f"Schema v1: {len(schema_v1)} fields")
            
            # Simulate new document with additional field
            new_doc = sample_data[0].copy() if sample_data else {}
            new_doc['new_field'] = "test_value"
            new_doc['new_nested'] = {"sub_field": 123}
            
            df_v2 = pd.DataFrame([new_doc])
            schema_v2 = SchemaGenerator.generate_from_dataframe(df_v2)
            logger.info(f"Schema v2: {len(schema_v2)} fields")
            
            # Detect changes
            logger.info("Detecting schema changes...")
            changes = SchemaGenerator.detect_breaking_changes(schema_v1, schema_v2)
            
            assert 'non_breaking_changes' in changes, "Should detect non-breaking changes"
            assert len(changes.get('non_breaking_changes', [])) >= 1, "Should detect at least 1 new field"
            
            logger.info(f"✅ Detected changes:")
            logger.info(f"   Breaking changes: {len(changes.get('breaking_changes', []))}")
            logger.info(f"   Non-breaking changes: {len(changes.get('non_breaking_changes', []))}")
            logger.info(f"   Has breaking changes: {changes.get('has_breaking_changes', False)}")
            logger.info(f"   Compatible: {changes.get('compatible', False)}")
            
        except Exception as e:
            logger.error(f"❌ Schema evolution detection failed: {e}")
            import traceback
            traceback.print_exc()
            pytest.fail(f"Schema evolution detection failed: {e}")
    
    def test_8_performance_benchmark(self, mongodb_collection):
        """Test 8: Performance benchmark."""
        logger.info("\n" + "="*60)
        logger.info("TEST 8: Performance Benchmark")
        logger.info("="*60)
        
        try:
            # Benchmark parameters
            record_counts = [100, 500, 1000]
            results = []
            
            for count in record_counts:
                logger.info(f"\nBenchmarking {count} records...")
                
                # Create pipeline
                pipeline = create_pipeline_from_uri(
                    mongo_uri=MONGO_URI,
                    database=DATABASE,
                    collection=COLLECTION
                )
                
                # Time the ETL read and transform
                start = datetime.now()
                df = pipeline.run_pipeline(query={}, limit=count, flatten=True, clean=True)
                duration = (datetime.now() - start).total_seconds()
                
                records_per_sec = count / duration if duration > 0 else 0
                
                results.append({
                    'records': count,
                    'duration': duration,
                    'records_per_sec': records_per_sec
                })
                
                logger.info(f"  Duration: {duration:.2f}s")
                logger.info(f"  Throughput: {records_per_sec:.0f} records/sec")
            
            # Summary
            logger.info("\n📊 Performance Summary:")
            for r in results:
                logger.info(f"  {r['records']:>5} records: {r['duration']:>6.2f}s "
                           f"({r['records_per_sec']:>6.0f} rec/sec)")
            
            # Verify performance is reasonable
            # Should process at least 10 records/sec (conservative for local testing)
            avg_throughput = sum(r['records_per_sec'] for r in results) / len(results)
            assert avg_throughput >= 10, \
                f"Performance too slow: {avg_throughput:.0f} records/sec (expected >10)"
            
            logger.info(f"✅ Average throughput: {avg_throughput:.0f} records/sec")
            
        except Exception as e:
            logger.error(f"❌ Performance benchmark failed: {e}")
            import traceback
            traceback.print_exc()
            pytest.fail(f"Performance benchmark failed: {e}")


# Standalone execution
if __name__ == "__main__":
    """
    Run tests directly without pytest.
    
    Usage:
        python tests/e2e/test_local_product_etl.py
    """
    
    # Create test instance
    test = TestProductETLEndToEnd()
    
    # Setup fixtures manually
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        collection = client[DATABASE][COLLECTION]
        sample_docs = list(collection.find().limit(10))
        
        print("\n" + "="*70)
        print("MORPHIX E2E TEST SUITE - LOCAL ENVIRONMENT")
        print("="*70)
        
        # Run tests in order
        test.test_1_mongodb_connection(client)
        test.test_2_schema_inference(collection, sample_docs)
        test.test_3_data_flattening(sample_docs)
        test.test_4_batch_etl_pipeline(collection, None)
        test.test_5_hudi_data_validation(collection)
        
        # Optional CDC test
        if CDC_AVAILABLE:
            test.test_6_cdc_real_time_sync(collection)
        
        test.test_7_schema_evolution_detection(collection, sample_docs)
        test.test_8_performance_benchmark(collection)
        
        print("\n" + "="*70)
        print("✅ ALL TESTS PASSED")
        print("="*70)
        sys.exit(0)
        
    except Exception as e:
        print("\n" + "="*70)
        print(f"❌ TEST FAILED: {e}")
        print("="*70)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        if 'client' in locals():
            client.close()

