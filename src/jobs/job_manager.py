"""
Job manager for creating, scheduling, and monitoring ETL jobs.
"""

import os
import uuid
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

from .models import (
    JobConfig, BatchJobConfig, StreamJobConfig, JobStatus, JobType,
    JobResult, JobExecution, JobMetrics, JobAlert, JobDependency
)
from ..utils.logging import get_logger
from enum import Enum

# Database setup - use centralized configuration
try:
    import sys
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from config.settings import get_settings
    _settings = get_settings()
    DATABASE_URL = _settings.database.connection_url
except ImportError:
    # Fallback for backwards compatibility
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/morphix")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class JobDB(Base):
    """Job database model."""
    __tablename__ = "jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(255), unique=True, index=True, nullable=False)
    job_name = Column(String(255), nullable=False)
    job_type = Column(String(50), nullable=False)
    user_id = Column(Integer, nullable=False)
    
    # Configuration (stored as JSON)
    config = Column(JSON, nullable=False)
    
    # Status
    status = Column(String(50), default=JobStatus.PENDING.value, nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    
    # Metadata
    description = Column(Text, nullable=True)
    created_by = Column(String(255), nullable=False)


class JobExecutionDB(Base):
    """Job execution database model."""
    __tablename__ = "job_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    execution_id = Column(String(255), unique=True, index=True, nullable=False)
    job_id = Column(String(255), nullable=False, index=True)
    status = Column(String(50), nullable=False)
    
    # Execution details
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    triggered_by = Column(String(50), nullable=False)
    
    # Configuration snapshot
    job_config = Column(JSON, nullable=False)
    
    # Result
    result = Column(JSON, nullable=True)
    
    # Retry information
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    
    # Worker information
    worker_id = Column(String(255), nullable=True)
    worker_host = Column(String(255), nullable=True)


# Create tables (lazy initialization)
def _create_tables():
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Could not create database tables: {e}")
        print("Tables will be created when database is available")


# Job state machine states
class JobRunState(str, Enum):
    """Job run state enumeration."""
    RECEIVED = "RECEIVED"
    VALIDATED = "VALIDATED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"


class JobManager:
    """Manager for job operations."""
    
    # Metadata directory for job runs (configurable via METADATA_BASE env var)
    METADATA_BASE = Path(os.getenv("METADATA_BASE", "/metadata"))
    
    def __init__(self):
        """Initialize job manager."""
        self.db = SessionLocal()
        self.logger = get_logger(__name__)
        # Try to create tables on first use
        try:
            _create_tables()
        except Exception:
            pass  # Tables will be created when database is available
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
    
    def create_job(self, job_config: JobConfig) -> str:
        """Create a new job.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Job ID
        """
        try:
            # Generate job ID if not provided
            if not job_config.job_id:
                job_config.job_id = str(uuid.uuid4())
            
            # Create job record
            db_job = JobDB(
                job_id=job_config.job_id,
                job_name=job_config.job_name,
                job_type=job_config.job_type.value,
                user_id=job_config.user_id,
                config=job_config.dict(),
                status=JobStatus.PENDING.value,
                enabled=job_config.enabled,
                description=job_config.description,
                created_by=job_config.created_by
            )
            
            self.db.add(db_job)
            self.db.commit()
            
            return job_config.job_id
            
        except IntegrityError:
            self.db.rollback()
            raise ValueError("Job with this ID already exists")
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Failed to create job: {str(e)}")
    
    def get_job(self, job_id: str) -> Optional[JobConfig]:
        """Get job by ID.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job configuration or None
        """
        db_job = self.db.query(JobDB).filter(JobDB.job_id == job_id).first()
        if not db_job:
            return None
        
        return self._db_job_to_job_config(db_job)
    
    def list_jobs(self, user_id: Optional[int] = None, job_type: Optional[JobType] = None) -> List[JobConfig]:
        """List jobs.
        
        Args:
            user_id: Filter by user ID
            job_type: Filter by job type
            
        Returns:
            List of job configurations
        """
        query = self.db.query(JobDB)
        
        if user_id:
            query = query.filter(JobDB.user_id == user_id)
        
        if job_type:
            query = query.filter(JobDB.job_type == job_type.value)
        
        db_jobs = query.all()
        return [self._db_job_to_job_config(db_job) for db_job in db_jobs]
    
    def update_job(self, job_id: str, job_config: JobConfig) -> bool:
        """Update job configuration.
        
        Args:
            job_id: Job identifier
            job_config: Updated job configuration
            
        Returns:
            True if updated successfully
        """
        try:
            db_job = self.db.query(JobDB).filter(JobDB.job_id == job_id).first()
            if not db_job:
                return False
            
            # Update job configuration
            db_job.job_name = job_config.job_name
            db_job.job_type = job_config.job_type.value
            db_job.config = job_config.dict()
            db_job.enabled = job_config.enabled
            db_job.description = job_config.description
            db_job.updated_at = datetime.utcnow()
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Failed to update job: {str(e)}")
    
    def delete_job(self, job_id: str) -> bool:
        """Delete job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if deleted successfully
        """
        try:
            db_job = self.db.query(JobDB).filter(JobDB.job_id == job_id).first()
            if not db_job:
                return False
            
            # Delete job executions first
            self.db.query(JobExecutionDB).filter(JobExecutionDB.job_id == job_id).delete()
            
            # Delete job
            self.db.delete(db_job)
            self.db.commit()
            
            return True
            
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Failed to delete job: {str(e)}")
    
    def enable_job(self, job_id: str) -> bool:
        """Enable job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if enabled successfully
        """
        return self._set_job_enabled(job_id, True)
    
    def disable_job(self, job_id: str) -> bool:
        """Disable job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if disabled successfully
        """
        return self._set_job_enabled(job_id, False)
    
    def _set_job_enabled(self, job_id: str, enabled: bool) -> bool:
        """Set job enabled status.
        
        Args:
            job_id: Job identifier
            enabled: Enabled status
            
        Returns:
            True if updated successfully
        """
        try:
            db_job = self.db.query(JobDB).filter(JobDB.job_id == job_id).first()
            if not db_job:
                return False
            
            db_job.enabled = enabled
            db_job.updated_at = datetime.utcnow()
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Failed to update job status: {str(e)}")
    
    def start_job(self, job_id: str, triggered_by: str = "manual") -> Optional[str]:
        """Start job execution with state machine.
        
        Args:
            job_id: Job identifier
            triggered_by: What triggered the execution
            
        Returns:
            Execution ID or None
        """
        try:
            # Get job configuration
            job_config = self.get_job(job_id)
            if not job_config:
                return None
            
            if not job_config.enabled:
                raise ValueError("Job is disabled")
            
            # Generate execution ID
            execution_id = str(uuid.uuid4())
            start_ts = datetime.utcnow()
            
            # Initialize job run state: RECEIVED
            self._persist_job_run(
                execution_id=execution_id,
                job_id=job_id,
                state=JobRunState.RECEIVED,
                start_ts=start_ts,
                triggered_by=triggered_by,
                job_config=job_config
            )
            
            # Transition to VALIDATED (validation happens in before_run)
            # For now, we'll transition to RUNNING directly
            # In Sprint 2, validation will happen in before_run()
            
            # Create execution record
            db_execution = JobExecutionDB(
                execution_id=execution_id,
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                started_at=start_ts,
                triggered_by=triggered_by,
                job_config=job_config.dict()
            )
            
            self.db.add(db_execution)
            
            # Update job last run
            db_job = self.db.query(JobDB).filter(JobDB.job_id == job_id).first()
            if db_job:
                db_job.last_run = start_ts
                db_job.status = JobStatus.RUNNING.value
            
            # Transition to RUNNING
            self._update_job_run_state(execution_id, JobRunState.RUNNING)
            
            self.db.commit()
            
            return execution_id
            
        except Exception as e:
            self.db.rollback()
            # Update state to FAILED
            try:
                self._update_job_run_state(execution_id, JobRunState.FAILED, error_summary=str(e))
            except:
                pass
            raise RuntimeError(f"Failed to start job: {str(e)}")
    
    def before_run(self, execution_id: str, job_config: JobConfig, sample_df=None) -> bool:
        """Validate job before running (called before actual execution).
        
        Runs GX suite if sample data is provided.
        
        Args:
            execution_id: Execution identifier
            job_config: Job configuration
            sample_df: Optional sample DataFrame for validation
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Transition to VALIDATED state
            self._update_job_run_state(execution_id, JobRunState.VALIDATED)
            
            # Run GX suite if sample data is provided
            if sample_df is not None:
                try:
                    from ..quality.gx_builder import generate_suite
                    from ..quality.gx_runner import run_suite
                    
                    # Generate suite from sample
                    suite = generate_suite(sample_df, suite_name=f"{job_config.collection}_suite")
                    
                    if suite:
                        # Run suite
                        result = run_suite(
                            suite, 
                            sample_df, 
                            collection=job_config.collection,
                            save_results=True
                        )
                        
                        if not result.get("passed", False):
                            # Validation failed
                            failed_count = len(result.get("failed_expectations", []))
                            error_summary = f"GX validation failed: {failed_count} expectations failed"
                            
                            self.logger.error(
                                error_summary,
                                extra={
                                    'event_type': 'gx_validation_failed',
                                    'execution_id': execution_id,
                                    'job_id': job_config.job_id,
                                    'collection': job_config.collection,
                                    'failed_count': failed_count
                                }
                            )
                            
                            self._update_job_run_state(
                                execution_id, 
                                JobRunState.VALIDATION_FAILED, 
                                error_summary=error_summary
                            )
                            return False
                        else:
                            self.logger.info(
                                "GX validation passed",
                                extra={
                                    'event_type': 'gx_validation_passed',
                                    'execution_id': execution_id,
                                    'job_id': job_config.job_id,
                                    'collection': job_config.collection
                                }
                            )
                except ImportError:
                    # GX not available, skip validation
                    self.logger.warning(
                        "Great Expectations not available, skipping validation",
                        extra={
                            'event_type': 'gx_validation_skipped',
                            'execution_id': execution_id,
                            'job_id': job_config.job_id
                        }
                    )
                except Exception as e:
                    # GX validation error
                    error_summary = f"GX validation error: {str(e)}"
                    self.logger.error(
                        error_summary,
                        exc_info=True,
                        extra={
                            'event_type': 'gx_validation_error',
                            'execution_id': execution_id,
                            'job_id': job_config.job_id,
                            'error': str(e)
                        }
                    )
                    self._update_job_run_state(
                        execution_id, 
                        JobRunState.VALIDATION_FAILED, 
                        error_summary=error_summary
                    )
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Validation failed for execution {execution_id}: {e}",
                exc_info=True,
                extra={
                    'event_type': 'job_validation_error',
                    'execution_id': execution_id,
                    'job_id': job_config.job_id,
                    'error': str(e)
                }
            )
            self._update_job_run_state(execution_id, JobRunState.VALIDATION_FAILED, error_summary=str(e))
            return False
    
    def _persist_job_run(self, execution_id: str, job_id: str, state: str, 
                        start_ts: datetime, triggered_by: str, 
                        job_config: JobConfig, end_ts: Optional[datetime] = None,
                        duration_ms: Optional[float] = None, retry_count: int = 0,
                        error_summary: Optional[str] = None):
        """Persist job run information to job_run.json file.
        
        Args:
            execution_id: Execution identifier
            job_id: Job identifier
            state: Current job run state
            start_ts: Start timestamp
            triggered_by: What triggered the execution
            job_config: Job configuration
            end_ts: End timestamp (optional)
            duration_ms: Duration in milliseconds (optional)
            retry_count: Number of retries
            error_summary: Error summary if failed (optional)
        """
        try:
            # Create metadata directory structure
            metadata_dir = self.METADATA_BASE / "job_runs" / job_id
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            # Create job run document
            job_run = {
                "execution_id": execution_id,
                "job_id": job_id,
                "state": state,
                "start_ts": start_ts.isoformat() + "Z",
                "end_ts": end_ts.isoformat() + "Z" if end_ts else None,
                "duration_ms": duration_ms,
                "retry_count": retry_count,
                "triggered_by": triggered_by,
                "error_summary": error_summary,
                "job_name": job_config.job_name,
                "job_type": job_config.job_type.value,
                "collection": job_config.collection,
                "database": job_config.database
            }
            
            # Write to file with deterministic filename
            job_run_file = metadata_dir / f"{execution_id}.json"
            with open(job_run_file, 'w') as f:
                json.dump(job_run, f, indent=2, default=str)
            
            self.logger.info(
                f"Job run persisted to {job_run_file}",
                extra={
                    'event_type': 'job_run_persisted',
                    'execution_id': execution_id,
                    'job_id': job_id,
                    'state': state,
                    'job_run_file': str(job_run_file)
                }
            )
            
        except Exception as e:
            self.logger.warning(
                f"Failed to persist job run: {e}",
                exc_info=True,
                extra={
                    'event_type': 'job_run_persist_error',
                    'execution_id': execution_id,
                    'job_id': job_id,
                    'error': str(e)
                }
            )
    
    def _update_job_run_state(self, execution_id: str, state: str, 
                              error_summary: Optional[str] = None):
        """Update job run state and persist to file.
        
        Args:
            execution_id: Execution identifier
            state: New state
            error_summary: Error summary if failed (optional)
        """
        try:
            # Find existing job run file
            metadata_dir = self.METADATA_BASE / "job_runs"
            job_run_file = None
            
            # Search for the execution file
            for job_dir in metadata_dir.iterdir():
                if job_dir.is_dir():
                    potential_file = job_dir / f"{execution_id}.json"
                    if potential_file.exists():
                        job_run_file = potential_file
                        break
            
            if job_run_file and job_run_file.exists():
                # Read existing job run
                with open(job_run_file, 'r') as f:
                    job_run = json.load(f)
                
                # Update state
                job_run["state"] = state
                
                # Update end_ts and duration if finishing
                if state in [JobRunState.FINISHED, JobRunState.FAILED, JobRunState.VALIDATION_FAILED]:
                    if job_run.get("end_ts") is None:
                        end_ts = datetime.utcnow()
                        start_ts = datetime.fromisoformat(job_run["start_ts"].replace("Z", "+00:00"))
                        duration_ms = (end_ts - start_ts).total_seconds() * 1000
                        job_run["end_ts"] = end_ts.isoformat() + "Z"
                        job_run["duration_ms"] = round(duration_ms, 2)
                
                # Update error summary if provided
                if error_summary:
                    job_run["error_summary"] = error_summary
                
                # Write back
                with open(job_run_file, 'w') as f:
                    json.dump(job_run, f, indent=2, default=str)
                
                self.logger.info(
                    f"Job run state updated to {state}",
                    extra={
                        'event_type': 'job_run_state_updated',
                        'execution_id': execution_id,
                        'state': state,
                        'job_run_file': str(job_run_file)
                    }
                )
            else:
                self.logger.warning(
                    f"Job run file not found for execution {execution_id}",
                    extra={
                        'event_type': 'job_run_file_not_found',
                        'execution_id': execution_id
                    }
                )
                
        except Exception as e:
            self.logger.warning(
                f"Failed to update job run state: {e}",
                exc_info=True,
                extra={
                    'event_type': 'job_run_state_update_error',
                    'execution_id': execution_id,
                    'state': state,
                    'error': str(e)
                }
            )
    
    def complete_job(self, execution_id: str, result: JobResult) -> bool:
        """Complete job execution.
        
        Args:
            execution_id: Execution identifier
            result: Job result
            
        Returns:
            True if completed successfully
        """
        try:
            db_execution = self.db.query(JobExecutionDB).filter(
                JobExecutionDB.execution_id == execution_id
            ).first()
            
            if not db_execution:
                return False
            
            # Determine final state
            if result.status == JobStatus.SUCCESS:
                final_state = JobRunState.FINISHED
            elif result.status == JobStatus.FAILED:
                final_state = JobRunState.FAILED
            else:
                final_state = JobRunState.FINISHED  # Default
            
            # Update job run state
            error_summary = result.error_message
            if result.error_details:
                error_summary = f"{error_summary or ''} | {json.dumps(result.error_details)}"
            
            self._update_job_run_state(
                execution_id, 
                final_state,
                error_summary=error_summary
            )
            
            # Update execution
            db_execution.status = result.status.value
            db_execution.completed_at = datetime.utcnow()
            db_execution.result = result.dict()
            
            # Update job status
            db_job = self.db.query(JobDB).filter(JobDB.job_id == db_execution.job_id).first()
            if db_job:
                db_job.status = result.status.value
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            raise RuntimeError(f"Failed to complete job: {str(e)}")
    
    def get_job_executions(self, job_id: str, limit: int = 100) -> List[JobExecution]:
        """Get job executions.
        
        Args:
            job_id: Job identifier
            limit: Maximum number of executions
            
        Returns:
            List of job executions
        """
        db_executions = self.db.query(JobExecutionDB).filter(
            JobExecutionDB.job_id == job_id
        ).order_by(JobExecutionDB.started_at.desc()).limit(limit).all()
        
        return [self._db_execution_to_job_execution(db_execution) for db_execution in db_executions]
    
    def get_job_metrics(self, job_id: str, days: int = 30) -> Optional[JobMetrics]:
        """Get job metrics.
        
        Args:
            job_id: Job identifier
            days: Number of days to look back
            
        Returns:
            Job metrics or None
        """
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            
            # Get executions in time window
            db_executions = self.db.query(JobExecutionDB).filter(
                JobExecutionDB.job_id == job_id,
                JobExecutionDB.started_at >= start_date
            ).all()
            
            if not db_executions:
                return None
            
            # Calculate metrics
            total_executions = len(db_executions)
            successful_executions = len([e for e in db_executions if e.status == JobStatus.SUCCESS.value])
            failed_executions = len([e for e in db_executions if e.status == JobStatus.FAILED.value])
            cancelled_executions = len([e for e in db_executions if e.status == JobStatus.CANCELLED.value])
            
            # Calculate durations
            durations = []
            for execution in db_executions:
                if execution.completed_at:
                    duration = (execution.completed_at - execution.started_at).total_seconds()
                    durations.append(duration)
            
            avg_duration = sum(durations) / len(durations) if durations else 0.0
            min_duration = min(durations) if durations else 0.0
            max_duration = max(durations) if durations else 0.0
            
            # Calculate data metrics
            total_records_processed = 0
            total_records_written = 0
            
            for execution in db_executions:
                if execution.result:
                    result_data = execution.result
                    total_records_processed += result_data.get('records_processed', 0)
                    total_records_written += result_data.get('records_written', 0)
            
            # Calculate error rate
            error_rate = (failed_executions / total_executions * 100) if total_executions > 0 else 0.0
            
            # Get timestamps
            first_execution = min(db_executions, key=lambda x: x.started_at).started_at
            last_execution = max(db_executions, key=lambda x: x.started_at).started_at
            
            last_successful_execution = None
            for execution in sorted(db_executions, key=lambda x: x.started_at, reverse=True):
                if execution.status == JobStatus.SUCCESS.value:
                    last_successful_execution = execution.started_at
                    break
            
            return JobMetrics(
                job_id=job_id,
                time_window=f"{days} days",
                total_executions=total_executions,
                successful_executions=successful_executions,
                failed_executions=failed_executions,
                cancelled_executions=cancelled_executions,
                average_duration_seconds=avg_duration,
                min_duration_seconds=min_duration,
                max_duration_seconds=max_duration,
                total_records_processed=total_records_processed,
                total_records_written=total_records_written,
                average_records_per_second=total_records_processed / (avg_duration) if avg_duration > 0 else 0.0,
                error_rate=error_rate,
                first_execution=first_execution,
                last_execution=last_execution,
                last_successful_execution=last_successful_execution
            )
            
        except Exception as e:
            print(f"Error calculating job metrics: {str(e)}")
            return None
    
    def _db_job_to_job_config(self, db_job: JobDB) -> JobConfig:
        """Convert database job to JobConfig."""
        config_data = db_job.config
        
        # Create appropriate job config based on type
        if db_job.job_type == JobType.BATCH.value:
            return BatchJobConfig(**config_data)
        elif db_job.job_type == JobType.STREAM.value:
            return StreamJobConfig(**config_data)
        else:
            return JobConfig(**config_data)
    
    def run(self, job_id: str, backfill: bool = False, 
            timestamp_field: Optional[str] = None,
            change_token: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Run job with optional backfill mode.
        
        Args:
            job_id: Job identifier
            backfill: If True, perform full scan. If False, use incremental mode
            timestamp_field: Field name for timestamp-based incremental reads (optional)
            change_token: Change token for incremental reads (optional)
            
        Returns:
            Execution ID or None
        """
        try:
            job_config = self.get_job(job_id)
            if not job_config:
                return None
            
            # Start job execution
            execution_id = self.start_job(job_id, triggered_by="backfill" if backfill else "incremental")
            if not execution_id:
                return None
            
            # Log backfill mode
            if backfill:
                self.logger.info(
                    f"Running job {job_id} in backfill mode (full scan)",
                    extra={
                        'event_type': 'job_backfill_started',
                        'job_id': job_id,
                        'execution_id': execution_id
                    }
                )
            else:
                self.logger.info(
                    f"Running job {job_id} in incremental mode",
                    extra={
                        'event_type': 'job_incremental_started',
                        'job_id': job_id,
                        'execution_id': execution_id,
                        'timestamp_field': timestamp_field,
                        'has_change_token': change_token is not None
                    }
                )
            
            # The actual job execution would happen here
            # This is a placeholder - actual implementation would call the appropriate processor
            # based on job type (batch vs stream)
            
            return execution_id
            
        except Exception as e:
            self.logger.error(
                f"Failed to run job: {e}",
                exc_info=True,
                extra={
                    'event_type': 'job_run_error',
                    'job_id': job_id,
                    'backfill': backfill,
                    'error': str(e)
                }
            )
            return None
    
    def _db_execution_to_job_execution(self, db_execution: JobExecutionDB) -> JobExecution:
        """Convert database execution to JobExecution."""
        # Reconstruct job config
        job_config = self._db_job_to_job_config(JobDB(
            job_id=db_execution.job_id,
            job_name="",  # Not needed for execution
            job_type="",  # Not needed for execution
            user_id=0,    # Not needed for execution
            config=db_execution.job_config,
            status="",    # Not needed for execution
            enabled=True, # Not needed for execution
            created_at=datetime.utcnow(),  # Not needed for execution
            updated_at=datetime.utcnow(),  # Not needed for execution
            created_by=""  # Not needed for execution
        ))
        
        # Reconstruct result
        result = None
        if db_execution.result:
            result = JobResult(**db_execution.result)
        
        return JobExecution(
            execution_id=db_execution.execution_id,
            job_id=db_execution.job_id,
            status=JobStatus(db_execution.status),
            started_at=db_execution.started_at,
            completed_at=db_execution.completed_at,
            triggered_by=db_execution.triggered_by,
            job_config=job_config,
            result=result,
            retry_count=db_execution.retry_count,
            max_retries=db_execution.max_retries,
            worker_id=db_execution.worker_id,
            worker_host=db_execution.worker_host
        )
