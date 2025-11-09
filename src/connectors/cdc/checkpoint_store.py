"""
PostgreSQL-backed checkpoint store for CDC resume tokens.

Thread-safe, ACID-compliant storage for resume tokens.
"""

from sqlalchemy import create_engine, Column, String, DateTime, JSON, BigInteger, UniqueConstraint, Index, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging
import json
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Import CheckpointError from mongo_changestream
from .mongo_changestream import CheckpointError

logger = logging.getLogger(__name__)

Base = declarative_base()


class CDCCheckpoint(Base):
    """
    CDC checkpoint model.
    
    Stores:
    - job_id: Unique job identifier
    - collection: MongoDB collection name
    - resume_token: MongoDB resume token (JSONB)
    - last_event_time: Timestamp of last processed event
    - records_processed: Total records processed
    - created_at: First checkpoint time
    - updated_at: Last update time
    """
    __tablename__ = "cdc_checkpoints"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(String(255), nullable=False, index=True)
    collection = Column(String(255), nullable=False, index=True)
    resume_token = Column(JSON, nullable=False)
    last_event_time = Column(DateTime, nullable=True)
    records_processed = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('job_id', 'collection', name='uq_cdc_checkpoints_job_collection'),
        Index('idx_cdc_checkpoints_updated_at', 'updated_at'),
    )


try:
    from prometheus_client import Counter
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

if PROMETHEUS_AVAILABLE:
    checkpoint_saves_total = Counter(
        'morphix_cdc_checkpoint_saves_total',
        'Total checkpoint saves',
        ['status']
    )
    
    checkpoint_loads_total = Counter(
        'morphix_cdc_checkpoint_loads_total',
        'Total checkpoint loads',
        ['status']
    )
else:
    class DummyMetric:
        def inc(self, *args, **kwargs):
            pass
    checkpoint_saves_total = DummyMetric()
    checkpoint_loads_total = DummyMetric()


class CheckpointStore:
    """
    Thread-safe checkpoint store.
    
    Features:
    - ACID transactions (PostgreSQL)
    - Automatic retry on transient failures
    - Connection pooling
    - Metrics instrumentation
    
    Thread Safety: YES (SQLAlchemy session per thread)
    
    Example:
        >>> store = CheckpointStore(database_url)
        >>> store.save_checkpoint(job_id, collection, resume_token)
        >>> token = store.load_checkpoint(job_id, collection)
    """
    
    def __init__(self, database_url: str):
        """
        Initialize checkpoint store.
        
        Args:
            database_url: PostgreSQL connection URL
            
        Raises:
            CheckpointError: If database connection fails
        """
        try:
            # Create SQLAlchemy engine with connection pooling
            self.engine = create_engine(
                database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
            
            # Create tables if not exist
            Base.metadata.create_all(self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("CheckpointStore initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize CheckpointStore: {e}")
            raise CheckpointError(f"Database connection failed: {e}") from e
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OperationalError, SQLAlchemyError))
    )
    def save_checkpoint(
        self,
        job_id: str,
        collection: str,
        resume_token: Dict[str, Any],
        last_event_time: Optional[datetime] = None,
        records_processed: int = 0
    ) -> None:
        """
        Save checkpoint (upsert).
        
        If checkpoint exists: update
        If checkpoint doesn't exist: insert
        
        Args:
            job_id: Job identifier
            collection: MongoDB collection name
            resume_token: MongoDB resume token
            last_event_time: Timestamp of last processed event
            records_processed: Total records processed so far
            
        Raises:
            CheckpointError: If save fails after retries
            
        Note: Uses transaction. Atomic operation.
        """
        session: Optional[Session] = None
        try:
            # Validate resume token
            if not self._validate_resume_token(resume_token):
                raise CheckpointError("Invalid resume token structure")
            
            session = self.SessionLocal()
            
            # Start transaction with row-level locking
            with session.begin():
                # Try to find existing checkpoint
                checkpoint = session.query(CDCCheckpoint).filter_by(
                    job_id=job_id,
                    collection=collection
                ).with_for_update().first()
                
                if checkpoint:
                    # Update existing checkpoint
                    checkpoint.resume_token = resume_token
                    checkpoint.records_processed = records_processed
                    checkpoint.updated_at = datetime.utcnow()
                    if last_event_time:
                        checkpoint.last_event_time = last_event_time
                else:
                    # Create new checkpoint
                    checkpoint = CDCCheckpoint(
                        job_id=job_id,
                        collection=collection,
                        resume_token=resume_token,
                        last_event_time=last_event_time,
                        records_processed=records_processed
                    )
                    session.add(checkpoint)
                
                # Commit happens automatically (context manager)
            
            if PROMETHEUS_AVAILABLE:
                checkpoint_saves_total.labels(status='success').inc()
            
            logger.debug(
                f"Saved checkpoint for job {job_id}, collection {collection}",
                extra={
                    "job_id": job_id,
                    "collection": collection,
                    "records_processed": records_processed
                }
            )
            
        except IntegrityError as e:
            if session:
                session.rollback()
            logger.error(
                f"Integrity error saving checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            if PROMETHEUS_AVAILABLE:
                checkpoint_saves_total.labels(status='error').inc()
            raise CheckpointError(f"Integrity error: {e}") from e
            
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            logger.error(
                f"Database error saving checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            if PROMETHEUS_AVAILABLE:
                checkpoint_saves_total.labels(status='error').inc()
            raise CheckpointError(f"Database error: {e}") from e
            
        except Exception as e:
            if session:
                session.rollback()
            logger.error(
                f"Unexpected error saving checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            if PROMETHEUS_AVAILABLE:
                checkpoint_saves_total.labels(status='error').inc()
            raise CheckpointError(f"Unexpected error: {e}") from e
            
        finally:
            if session:
                session.close()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OperationalError, SQLAlchemyError))
    )
    def load_checkpoint(
        self,
        job_id: str,
        collection: str
    ) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint for job+collection.
        
        Args:
            job_id: Job identifier
            collection: MongoDB collection name
            
        Returns:
            Resume token dict if exists, None otherwise
            
        Raises:
            CheckpointError: If load fails after retries
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            checkpoint = session.query(CDCCheckpoint).filter_by(
                job_id=job_id,
                collection=collection
            ).first()
            
            if not checkpoint:
                if PROMETHEUS_AVAILABLE:
                    checkpoint_loads_total.labels(status='not_found').inc()
                logger.debug(
                    f"No checkpoint found for job {job_id}, collection {collection}",
                    extra={"job_id": job_id, "collection": collection}
                )
                return None
            
            # Validate resume token
            resume_token = checkpoint.resume_token
            if not self._validate_resume_token(resume_token):
                logger.warning(
                    f"Invalid resume token structure in checkpoint",
                    extra={"job_id": job_id, "collection": collection}
                )
                if PROMETHEUS_AVAILABLE:
                    checkpoint_loads_total.labels(status='invalid').inc()
                return None
            
            if PROMETHEUS_AVAILABLE:
                checkpoint_loads_total.labels(status='success').inc()
            
            logger.debug(
                f"Loaded checkpoint for job {job_id}, collection {collection}",
                extra={
                    "job_id": job_id,
                    "collection": collection,
                    "records_processed": checkpoint.records_processed
                }
            )
            
            return resume_token
            
        except SQLAlchemyError as e:
            logger.error(
                f"Database error loading checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            if PROMETHEUS_AVAILABLE:
                checkpoint_loads_total.labels(status='error').inc()
            raise CheckpointError(f"Database error: {e}") from e
            
        except Exception as e:
            logger.error(
                f"Unexpected error loading checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            if PROMETHEUS_AVAILABLE:
                checkpoint_loads_total.labels(status='error').inc()
            raise CheckpointError(f"Unexpected error: {e}") from e
            
        finally:
            if session:
                session.close()
    
    def delete_checkpoint(
        self,
        job_id: str,
        collection: str
    ) -> None:
        """
        Delete checkpoint (used for job cleanup).
        
        Args:
            job_id: Job identifier
            collection: MongoDB collection name
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            with session.begin():
                checkpoint = session.query(CDCCheckpoint).filter_by(
                    job_id=job_id,
                    collection=collection
                ).first()
                
                if checkpoint:
                    session.delete(checkpoint)
                    logger.info(
                        f"Deleted checkpoint for job {job_id}, collection {collection}",
                        extra={"job_id": job_id, "collection": collection}
                    )
                else:
                    logger.debug(
                        f"No checkpoint to delete for job {job_id}, collection {collection}",
                        extra={"job_id": job_id, "collection": collection}
                    )
            
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            logger.error(
                f"Database error deleting checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            raise CheckpointError(f"Database error: {e}") from e
            
        except Exception as e:
            if session:
                session.rollback()
            logger.error(
                f"Unexpected error deleting checkpoint: {e}",
                extra={"job_id": job_id, "collection": collection}
            )
            raise CheckpointError(f"Unexpected error: {e}") from e
            
        finally:
            if session:
                session.close()
    
    def get_all_checkpoints(self) -> List[CDCCheckpoint]:
        """
        Get all checkpoints (for admin dashboard).
        
        Returns:
            List of all checkpoints
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            checkpoints = session.query(CDCCheckpoint).all()
            return checkpoints
            
        except SQLAlchemyError as e:
            logger.error(f"Database error loading all checkpoints: {e}")
            raise CheckpointError(f"Database error: {e}") from e
            
        except Exception as e:
            logger.error(f"Unexpected error loading all checkpoints: {e}")
            raise CheckpointError(f"Unexpected error: {e}") from e
            
        finally:
            if session:
                session.close()
    
    def _validate_resume_token(self, token: Dict[str, Any]) -> bool:
        """Validate resume token structure."""
        if not isinstance(token, dict):
            return False
        
        # MongoDB resume tokens have _data field or are valid dicts
        # Accept any non-empty dict as valid
        if len(token) == 0:
            return False
        
        return True
    
    def close(self) -> None:
        """Close database connections."""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("CheckpointStore connections closed")

