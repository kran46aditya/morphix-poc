"""
Infrastructure cost tracking and estimation.
"""

from datetime import datetime, timedelta
from typing import Dict, Any
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


class CostBreakdown(BaseModel):
    """Cost breakdown by category."""
    embedding_compute_cost: float
    storage_cost: float
    vector_db_cost: float
    warehouse_cost: float
    total_cost: float
    savings_from_cache: float


class CostTracker:
    """Track and estimate costs."""
    
    # Pricing constants (update based on your infrastructure)
    GPU_HOUR_COST = 0.50  # vast.ai H100 instance
    CPU_HOUR_COST = 0.05  # Standard VM
    STORAGE_GB_MONTH = 0.023  # S3 standard
    VECTOR_DB_GB_MONTH = 0.50  # Pinecone estimate
    
    def calculate_execution_cost(
        self,
        job_execution,
        embeddings_generated: int,
        cache_hits: int,
        storage_gb: float
    ) -> CostBreakdown:
        """
        Calculate actual costs for job execution.
        
        Components:
        1. Embedding compute:
           - If cache hit: $0
           - If cache miss: (time_to_embed * GPU_HOUR_COST)
           - Estimate: 1000 embeddings/sec on CPU, 10K/sec on GPU
        
        2. Storage:
           - Hudi/Iceberg files: GB * STORAGE_GB_MONTH / 30 / 24
           - Vector index: vectors * 384 * 4 bytes / 1e9 * VECTOR_DB_GB_MONTH
        
        3. Savings:
           - cache_hits * cost_per_embedding
        
        Args:
            job_execution: Job execution object
            embeddings_generated: Total embeddings generated
            cache_hits: Number of cache hits
            storage_gb: Storage used in GB
            
        Returns:
            CostBreakdown with cost details
        """
        # Calculate embedding cost
        embeddings_to_generate = embeddings_generated - cache_hits
        embedding_time_hours = embeddings_to_generate / (1000 * 3600)  # CPU rate
        embedding_cost = embedding_time_hours * self.CPU_HOUR_COST
        
        # Calculate savings
        cost_per_embedding = embedding_cost / max(embeddings_to_generate, 1)
        savings = cache_hits * cost_per_embedding
        
        # Calculate storage (per hour)
        storage_cost = storage_gb * self.STORAGE_GB_MONTH / 30 / 24
        
        return CostBreakdown(
            embedding_compute_cost=embedding_cost,
            storage_cost=storage_cost,
            vector_db_cost=0,  # Estimated separately
            warehouse_cost=0,  # Estimated separately
            total_cost=embedding_cost + storage_cost,
            savings_from_cache=savings
        )
    
    def estimate_monthly_cost(
        self,
        job_config
    ) -> Dict[str, float]:
        """
        Project monthly costs based on job configuration.
        
        Inputs:
        - estimated_daily_volume
        - Text field sizes
        - Cache hit rate estimate (default 70%)
        
        Returns monthly cost projection.
        
        Args:
            job_config: Job configuration
            
        Returns:
            Dictionary with monthly cost estimates
        """
        daily_volume = getattr(job_config, 'estimated_daily_volume', 10000)
        cache_hit_rate = 0.70  # Assume 70% cache hits after warmup
        
        # Calculate embeddings needed
        daily_new_embeddings = daily_volume * (1 - cache_hit_rate)
        monthly_embeddings = daily_new_embeddings * 30
        
        # Calculate monthly embedding cost
        embedding_time_hours = monthly_embeddings / (1000 * 3600)
        monthly_embedding_cost = embedding_time_hours * self.CPU_HOUR_COST
        
        # Estimate storage (assume 1KB per record)
        monthly_storage_gb = daily_volume * 30 * 0.001
        monthly_storage_cost = monthly_storage_gb * self.STORAGE_GB_MONTH
        
        return {
            'embeddings': monthly_embedding_cost,
            'storage': monthly_storage_cost,
            'total': monthly_embedding_cost + monthly_storage_cost,
            'records_per_dollar': daily_volume * 30 / max(monthly_embedding_cost + monthly_storage_cost, 0.01)
        }

