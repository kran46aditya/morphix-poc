"""Unit tests for CDC components."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
from typing import Dict, Any, List
import time
import threading

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from src.connectors.cdc.mongo_changestream import (
    ChangeStreamWatcher, CDCConfig, CDCError, ResumeTokenError, CheckpointError
)
from src.connectors.cdc.checkpoint_store import CheckpointStore, CDCCheckpoint


class TestCDCConfig:
    """Test CDCConfig."""
    
    def test_valid_config(self):
        """Test valid configuration."""
        config = CDCConfig(batch_size=100, batch_interval=5)
        assert config.batch_size == 100
        assert config.batch_interval == 5
    
    def test_invalid_batch_size(self):
        """Test invalid batch_size raises ValueError."""
        with pytest.raises(ValueError, match="batch_size must be positive"):
            CDCConfig(batch_size=0)
    
    def test_invalid_batch_interval(self):
        """Test invalid batch_interval raises ValueError."""
        with pytest.raises(ValueError, match="batch_interval must be positive"):
            CDCConfig(batch_interval=0)
    
    def test_invalid_max_retries(self):
        """Test invalid max_retries raises ValueError."""
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            CDCConfig(max_retries=-1)


class TestChangeStreamWatcher:
    """Test ChangeStreamWatcher."""
    
    @pytest.fixture
    def mock_collection(self):
        """Mock MongoDB collection."""
        collection = Mock()
        collection.name = "test_collection"
        return collection
    
    @pytest.fixture
    def mock_checkpoint_store(self):
        """Mock checkpoint store."""
        store = Mock(spec=CheckpointStore)
        store.load_checkpoint.return_value = None
        store.save_checkpoint = Mock()
        return store
    
    @pytest.fixture
    def watcher(self, mock_collection, mock_checkpoint_store):
        """Create watcher instance."""
        config = CDCConfig(batch_size=100, batch_interval=5)
        return ChangeStreamWatcher(
            collection=mock_collection,
            checkpoint_store=mock_checkpoint_store,
            config=config,
            job_id="test_job"
        )
    
    def test_init_validates_collection(self, mock_checkpoint_store):
        """Test initialization validates collection type."""
        config = CDCConfig()
        with pytest.raises(TypeError, match="collection must be a PyMongo Collection"):
            ChangeStreamWatcher(
                collection="not_a_collection",
                checkpoint_store=mock_checkpoint_store,
                config=config,
                job_id="test_job"
            )
    
    def test_init_validates_checkpoint_store(self, mock_collection):
        """Test initialization validates checkpoint store."""
        config = CDCConfig()
        with pytest.raises(TypeError, match="checkpoint_store must be a CheckpointStore"):
            ChangeStreamWatcher(
                collection=mock_collection,
                checkpoint_store="not_a_store",
                config=config,
                job_id="test_job"
            )
    
    def test_resume_token_loaded_on_start(self, watcher, mock_checkpoint_store):
        """Test resume token is loaded from checkpoint store."""
        resume_token = {"_data": "test_token"}
        mock_checkpoint_store.load_checkpoint.return_value = resume_token
        
        # Mock the changestream to exit immediately
        mock_stream = MagicMock()
        mock_stream.__iter__ = Mock(return_value=iter([]))
        mock_stream.resume_token = resume_token
        
        watcher.collection.watch = Mock(return_value=mock_stream)
        
        callback = Mock()
        try:
            watcher.start(callback=callback)
        except StopIteration:
            pass
        
        mock_checkpoint_store.load_checkpoint.assert_called_once_with(
            "test_job",
            "test_collection"
        )
    
    def test_buffer_flushes_on_size_threshold(self, watcher, mock_checkpoint_store):
        """Test buffer flushes when size threshold reached."""
        # Create a mock changestream that yields enough changes
        changes = [{"operationType": "insert", "fullDocument": {"_id": i}} for i in range(101)]
        mock_stream = MagicMock()
        mock_stream.__iter__ = Mock(return_value=iter(changes))
        mock_stream.resume_token = None
        
        watcher.collection.watch = Mock(return_value=mock_stream)
        
        callback = Mock()
        
        # Start in a thread and stop it quickly
        def run_watcher():
            try:
                watcher.start(callback=callback)
            except Exception:
                pass
        
        thread = threading.Thread(target=run_watcher)
        thread.start()
        time.sleep(0.1)
        watcher.stop()
        thread.join(timeout=1)
        
        # Verify callback was called (at least once when buffer reaches threshold)
        assert callback.called
    
    def test_checkpoint_saved_after_successful_batch(
        self, watcher, mock_checkpoint_store
    ):
        """Test checkpoint is saved after callback succeeds."""
        resume_token = {"_data": "test_token"}
        
        # Mock changestream
        changes = [{"operationType": "insert", "fullDocument": {"_id": 1}}]
        mock_stream = MagicMock()
        mock_stream.__iter__ = Mock(return_value=iter(changes))
        mock_stream.resume_token = resume_token
        
        watcher.collection.watch = Mock(return_value=mock_stream)
        
        callback = Mock()
        
        # Start and stop quickly
        def run_watcher():
            try:
                watcher.start(callback=callback)
            except Exception:
                pass
        
        thread = threading.Thread(target=run_watcher)
        thread.start()
        time.sleep(0.1)
        watcher.stop()
        thread.join(timeout=1)
        
        # Verify checkpoint was saved
        assert mock_checkpoint_store.save_checkpoint.called
    
    def test_error_recovery_with_exponential_backoff(self, watcher):
        """Test exponential backoff on transient errors."""
        from pymongo.errors import ConnectionFailure
        
        # Mock collection.watch to raise ConnectionFailure
        watcher.collection.watch = Mock(side_effect=ConnectionFailure("Connection failed"))
        
        callback = Mock()
        
        # Mock time.sleep to track delays
        delays = []
        original_sleep = time.sleep
        def mock_sleep(seconds):
            delays.append(seconds)
            if len(delays) >= 3:  # Stop after 3 attempts
                watcher.stop()
        
        with patch('time.sleep', side_effect=mock_sleep):
            try:
                watcher.start(callback=callback)
            except Exception:
                pass
        
        # Verify exponential backoff: 2^1=2, 2^2=4, 2^3=8
        assert len(delays) >= 2
        assert delays[0] == 2  # First retry: 2^1
        assert delays[1] == 4  # Second retry: 2^2
    
    def test_graceful_shutdown(self, watcher, mock_checkpoint_store):
        """Test graceful shutdown flushes buffer and saves checkpoint."""
        resume_token = {"_data": "test_token"}
        
        # Create a changestream that yields one change
        changes = [{"operationType": "insert", "fullDocument": {"_id": 1}}]
        mock_stream = MagicMock()
        mock_stream.__iter__ = Mock(return_value=iter(changes))
        mock_stream.resume_token = resume_token
        
        watcher.collection.watch = Mock(return_value=mock_stream)
        
        callback = Mock()
        
        # Start in thread
        def run_watcher():
            try:
                watcher.start(callback=callback)
            except Exception:
                pass
        
        thread = threading.Thread(target=run_watcher)
        thread.start()
        time.sleep(0.1)
        
        # Stop gracefully
        watcher.stop()
        thread.join(timeout=1)
        
        # Verify checkpoint was saved
        assert mock_checkpoint_store.save_checkpoint.called
    
    def test_corrupted_resume_token_handling(
        self, watcher, mock_checkpoint_store
    ):
        """Test handling of corrupted resume token."""
        # Return invalid token
        mock_checkpoint_store.load_checkpoint.return_value = {"invalid": "token"}
        
        # Mock changestream
        mock_stream = MagicMock()
        mock_stream.__iter__ = Mock(return_value=iter([]))
        mock_stream.resume_token = None
        
        watcher.collection.watch = Mock(return_value=mock_stream)
        
        callback = Mock()
        
        try:
            watcher.start(callback=callback)
        except StopIteration:
            pass
        
        # Should still start (just without resume token)
        watcher.collection.watch.assert_called()


class TestCheckpointStore:
    """Test CheckpointStore."""
    
    @pytest.fixture
    def mock_database_url(self):
        """Mock database URL."""
        return "postgresql://user:pass@localhost/testdb"
    
    @pytest.fixture
    def store(self, mock_database_url):
        """Create checkpoint store with mocked database."""
        with patch('src.connectors.cdc.checkpoint_store.create_engine') as mock_engine:
            with patch('src.connectors.cdc.checkpoint_store.Base.metadata.create_all'):
                mock_session = Mock()
                mock_session.query.return_value.filter_by.return_value.first.return_value = None
                mock_session.query.return_value.filter_by.return_value.with_for_update.return_value.first.return_value = None
                
                mock_session_factory = Mock(return_value=mock_session)
                
                mock_engine_instance = Mock()
                mock_engine_instance.connect.return_value.__enter__ = Mock(return_value=Mock())
                mock_engine_instance.connect.return_value.__exit__ = Mock(return_value=None)
                mock_engine.return_value = mock_engine_instance
                
                with patch('src.connectors.cdc.checkpoint_store.sessionmaker', return_value=mock_session_factory):
                    store = CheckpointStore(mock_database_url)
                    store.SessionLocal = mock_session_factory
                    return store
    
    def test_save_checkpoint_creates_new(self, store):
        """Test saving checkpoint creates new record."""
        resume_token = {"_data": "test_token"}
        
        # Mock session
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.with_for_update.return_value.first.return_value = None
        mock_session.begin.return_value.__enter__ = Mock(return_value=None)
        mock_session.begin.return_value.__exit__ = Mock(return_value=None)
        
        store.SessionLocal = Mock(return_value=mock_session)
        
        store.save_checkpoint(
            job_id="test_job",
            collection="test_collection",
            resume_token=resume_token
        )
        
        # Verify session.add was called (new checkpoint created)
        assert mock_session.add.called
    
    def test_save_checkpoint_updates_existing(self, store):
        """Test saving checkpoint updates existing record."""
        resume_token = {"_data": "test_token"}
        
        # Mock existing checkpoint
        existing_checkpoint = Mock(spec=CDCCheckpoint)
        existing_checkpoint.job_id = "test_job"
        existing_checkpoint.collection = "test_collection"
        
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.with_for_update.return_value.first.return_value = existing_checkpoint
        mock_session.begin.return_value.__enter__ = Mock(return_value=None)
        mock_session.begin.return_value.__exit__ = Mock(return_value=None)
        
        store.SessionLocal = Mock(return_value=mock_session)
        
        store.save_checkpoint(
            job_id="test_job",
            collection="test_collection",
            resume_token=resume_token
        )
        
        # Verify checkpoint was updated
        assert existing_checkpoint.resume_token == resume_token
    
    def test_load_checkpoint_returns_none_if_not_exists(self, store):
        """Test loading non-existent checkpoint returns None."""
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        
        store.SessionLocal = Mock(return_value=mock_session)
        
        result = store.load_checkpoint("test_job", "test_collection")
        
        assert result is None
    
    def test_load_checkpoint_returns_token(self, store):
        """Test loading checkpoint returns resume token."""
        resume_token = {"_data": "test_token"}
        
        mock_checkpoint = Mock()
        mock_checkpoint.resume_token = resume_token
        mock_checkpoint.records_processed = 100
        
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_checkpoint
        
        store.SessionLocal = Mock(return_value=mock_session)
        
        result = store.load_checkpoint("test_job", "test_collection")
        
        assert result == resume_token
    
    def test_delete_checkpoint(self, store):
        """Test deleting checkpoint."""
        mock_checkpoint = Mock()
        
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_checkpoint
        mock_session.begin.return_value.__enter__ = Mock(return_value=None)
        mock_session.begin.return_value.__exit__ = Mock(return_value=None)
        
        store.SessionLocal = Mock(return_value=mock_session)
        
        store.delete_checkpoint("test_job", "test_collection")
        
        # Verify delete was called
        assert mock_session.delete.called
    
    def test_validate_resume_token(self, store):
        """Test resume token validation."""
        # Valid token
        assert store._validate_resume_token({"_data": "test"}) is True
        assert store._validate_resume_token({"token": "value"}) is True
        
        # Invalid tokens
        assert store._validate_resume_token({}) is False
        assert store._validate_resume_token("not_a_dict") is False


class TestStreamJobProcessor:
    """Test StreamJobProcessor integration."""
    
    @pytest.fixture
    def mock_job_config(self):
        """Mock stream job config."""
        from src.jobs.models import StreamJobConfig
        return StreamJobConfig(
            job_id="test_job",
            job_name="Test Job",
            mongo_uri="mongodb://localhost:27017",
            database="testdb",
            collection="test_collection",
            hudi_table_name="test_table",
            hudi_base_path="/tmp/hudi",
            user_id=1,
            created_by="test_user",
            schedule=Mock()
        )
    
    def test_process_batch_converts_to_dataframe(self, mock_job_config):
        """Test batch is converted to DataFrame."""
        from src.jobs.stream_jobs import StreamJobProcessor
        import pandas as pd
        
        processor = StreamJobProcessor()
        
        batch = [
            {
                "operationType": "insert",
                "fullDocument": {"_id": 1, "name": "Alice"}
            }
        ]
        
        # Mock Hudi writer
        processor.hudi_writer = Mock()
        processor.hudi_writer.upsert_dataframe.return_value = Mock(records_written=1)
        
        processor._process_batch(batch, mock_job_config)
        
        # Verify Hudi writer was called
        assert processor.hudi_writer.upsert_dataframe.called
    
    def test_process_batch_handles_deletes(self, mock_job_config):
        """Test delete operations are handled correctly."""
        from src.jobs.stream_jobs import StreamJobProcessor
        
        processor = StreamJobProcessor()
        
        batch = [
            {
                "operationType": "delete",
                "documentKey": {"_id": 1},
                "clusterTime": datetime.utcnow()
            }
        ]
        
        # Mock Hudi writer
        processor.hudi_writer = Mock()
        processor.hudi_writer.upsert_dataframe.return_value = Mock(records_written=1)
        
        processor._process_batch(batch, mock_job_config)
        
        # Verify Hudi writer was called
        assert processor.hudi_writer.upsert_dataframe.called
        
        # Verify the call included _deleted flag
        call_args = processor.hudi_writer.upsert_dataframe.call_args
        df = call_args[0][0]  # First positional argument
        assert "_deleted" in df.columns
        assert df["_deleted"].iloc[0] is True
    
    def test_process_batch_writes_to_hudi(self, mock_job_config):
        """Test batch is written to Hudi."""
        from src.jobs.stream_jobs import StreamJobProcessor
        
        processor = StreamJobProcessor()
        
        batch = [
            {
                "operationType": "insert",
                "fullDocument": {"_id": 1, "name": "Alice", "age": 30}
            },
            {
                "operationType": "update",
                "fullDocument": {"_id": 2, "name": "Bob", "age": 25}
            }
        ]
        
        # Mock Hudi writer
        processor.hudi_writer = Mock()
        processor.hudi_writer.upsert_dataframe.return_value = Mock(records_written=2)
        
        processor._process_batch(batch, mock_job_config)
        
        # Verify Hudi writer was called with DataFrame
        assert processor.hudi_writer.upsert_dataframe.called
        call_args = processor.hudi_writer.upsert_dataframe.call_args
        df = call_args[0][0]
        assert len(df) == 2

