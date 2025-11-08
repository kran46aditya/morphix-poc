"""
Job scheduler for managing scheduled ETL jobs.
"""

import time
import threading
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from croniter import croniter
import schedule

from .models import JobConfig, BatchJobConfig, StreamJobConfig, JobStatus, JobTrigger
from .job_manager import JobManager
from .batch_jobs import BatchJobProcessor
from .stream_jobs import StreamJobProcessor


class JobScheduler:
    """Scheduler for managing job execution."""
    
    def __init__(self):
        """Initialize job scheduler."""
        self.job_manager = JobManager()
        self.batch_processor = BatchJobProcessor()
        self.stream_processor = StreamJobProcessor()
        self.scheduler_thread = None
        self.running = False
        self.scheduled_jobs = {}
    
    def start_scheduler(self):
        """Start the job scheduler."""
        if self.running:
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        print("Job scheduler started")
    
    def stop_scheduler(self):
        """Stop the job scheduler."""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        print("Job scheduler stopped")
    
    def schedule_job(self, job_config: JobConfig) -> bool:
        """Schedule a job for execution.
        
        Args:
            job_config: Job configuration
            
        Returns:
            True if scheduled successfully
        """
        try:
            if job_config.schedule.trigger == JobTrigger.SCHEDULED:
                if not job_config.schedule.cron_expression:
                    raise ValueError("Cron expression is required for scheduled jobs")
                
                # Validate cron expression
                try:
                    croniter(job_config.schedule.cron_expression)
                except Exception as e:
                    raise ValueError(f"Invalid cron expression: {str(e)}")
                
                # Schedule job
                self.scheduled_jobs[job_config.job_id] = {
                    "job_config": job_config,
                    "next_run": self._calculate_next_run(job_config),
                    "last_run": None,
                    "run_count": 0
                }
                
                print(f"Job {job_config.job_id} scheduled with cron: {job_config.schedule.cron_expression}")
                
            elif job_config.schedule.trigger == JobTrigger.MANUAL:
                # Manual jobs don't need scheduling
                pass
            
            return True
            
        except Exception as e:
            print(f"Error scheduling job {job_config.job_id}: {str(e)}")
            return False
    
    def unschedule_job(self, job_id: str) -> bool:
        """Unschedule a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if unscheduled successfully
        """
        if job_id in self.scheduled_jobs:
            del self.scheduled_jobs[job_id]
            print(f"Job {job_id} unscheduled")
            return True
        
        return False
    
    def trigger_job(self, job_id: str, triggered_by: str = "manual") -> Optional[str]:
        """Manually trigger a job.
        
        Args:
            job_id: Job identifier
            triggered_by: What triggered the execution
            
        Returns:
            Execution ID or None
        """
        try:
            # Get job configuration
            job_config = self.job_manager.get_job(job_id)
            if not job_config:
                print(f"Job {job_id} not found")
                return None
            
            if not job_config.enabled:
                print(f"Job {job_id} is disabled")
                return None
            
            # Start job execution
            execution_id = self.job_manager.start_job(job_id, triggered_by)
            if not execution_id:
                print(f"Failed to start job {job_id}")
                return None
            
            # Process job based on type
            if job_config.job_type == "batch":
                result = self.batch_processor.process_batch_job(job_config)
            elif job_config.job_type == "stream":
                result = self.stream_processor.start_stream_job(job_config)
                # For stream jobs, we don't have a result immediately
                result = JobResult(
                    job_id=job_id,
                    execution_id=execution_id,
                    status=JobStatus.RUNNING,
                    started_at=datetime.utcnow()
                )
            else:
                print(f"Unknown job type: {job_config.job_type}")
                return None
            
            # Complete job execution
            self.job_manager.complete_job(execution_id, result)
            
            print(f"Job {job_id} executed successfully")
            return execution_id
            
        except Exception as e:
            print(f"Error executing job {job_id}: {str(e)}")
            return None
    
    def get_scheduled_jobs(self) -> List[Dict[str, Any]]:
        """Get list of scheduled jobs.
        
        Returns:
            List of scheduled job information
        """
        jobs = []
        
        for job_id, job_info in self.scheduled_jobs.items():
            jobs.append({
                "job_id": job_id,
                "job_name": job_info["job_config"].job_name,
                "job_type": job_info["job_config"].job_type,
                "next_run": job_info["next_run"],
                "last_run": job_info["last_run"],
                "run_count": job_info["run_count"],
                "enabled": job_info["job_config"].enabled
            })
        
        return jobs
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job status information
        """
        if job_id in self.scheduled_jobs:
            job_info = self.scheduled_jobs[job_id]
            return {
                "job_id": job_id,
                "job_name": job_info["job_config"].job_name,
                "job_type": job_info["job_config"].job_type,
                "next_run": job_info["next_run"],
                "last_run": job_info["last_run"],
                "run_count": job_info["run_count"],
                "enabled": job_info["job_config"].enabled,
                "status": "scheduled"
            }
        
        # Check if it's a running stream job
        stream_status = self.stream_processor.get_stream_job_status(job_id)
        if stream_status:
            return {
                "job_id": job_id,
                "status": stream_status["status"],
                "started_at": stream_status["started_at"],
                "is_running": stream_status["is_running"]
            }
        
        return None
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.running:
            try:
                current_time = datetime.utcnow()
                
                # Check for jobs that need to run
                for job_id, job_info in self.scheduled_jobs.items():
                    if not job_info["job_config"].enabled:
                        continue
                    
                    # Check if it's time to run
                    if job_info["next_run"] and current_time >= job_info["next_run"]:
                        try:
                            # Trigger job
                            execution_id = self.trigger_job(job_id, "scheduled")
                            
                            if execution_id:
                                # Update job info
                                job_info["last_run"] = current_time
                                job_info["run_count"] += 1
                                
                                # Calculate next run time
                                job_info["next_run"] = self._calculate_next_run(job_info["job_config"])
                                
                                # Check if we've reached max runs
                                if (job_info["job_config"].schedule.max_runs and 
                                    job_info["run_count"] >= job_info["job_config"].schedule.max_runs):
                                    job_info["job_config"].enabled = False
                                    print(f"Job {job_id} reached max runs, disabling")
                            
                        except Exception as e:
                            print(f"Error executing scheduled job {job_id}: {str(e)}")
                
                # Clean up completed stream jobs
                self.stream_processor.cleanup_completed_jobs()
                
                # Sleep for 1 minute before next check
                time.sleep(60)
                
            except Exception as e:
                print(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)
    
    def _calculate_next_run(self, job_config: JobConfig) -> Optional[datetime]:
        """Calculate next run time for a job.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Next run time or None
        """
        if job_config.schedule.trigger != JobTrigger.SCHEDULED:
            return None
        
        if not job_config.schedule.cron_expression:
            return None
        
        try:
            # Calculate next run time
            cron = croniter(job_config.schedule.cron_expression, datetime.utcnow())
            next_run = cron.get_next(datetime)
            
            # Check if we're within the allowed time range
            if job_config.schedule.start_time and next_run < job_config.schedule.start_time:
                next_run = job_config.schedule.start_time
            
            if job_config.schedule.end_time and next_run > job_config.schedule.end_time:
                return None  # Job has ended
            
            return next_run
            
        except Exception as e:
            print(f"Error calculating next run time for job {job_config.job_id}: {str(e)}")
            return None
    
    def load_jobs_from_database(self):
        """Load scheduled jobs from database."""
        try:
            with JobManager() as job_manager:
                jobs = job_manager.list_jobs()
                
                for job_config in jobs:
                    if job_config.schedule.trigger == JobTrigger.SCHEDULED:
                        self.schedule_job(job_config)
                
                print(f"Loaded {len(self.scheduled_jobs)} scheduled jobs from database")
                
        except Exception as e:
            print(f"Error loading jobs from database: {str(e)}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status.
        
        Returns:
            Scheduler status information
        """
        return {
            "running": self.running,
            "scheduled_jobs": len(self.scheduled_jobs),
            "running_stream_jobs": len(self.stream_processor.running_jobs),
            "uptime": datetime.utcnow() - (self.scheduler_thread.start_time if self.scheduler_thread else datetime.utcnow())
        }
