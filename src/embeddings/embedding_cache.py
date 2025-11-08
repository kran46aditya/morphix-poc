"""Cache management for embeddings."""

from datetime import datetime, timedelta
import redis
import logging

logger = logging.getLogger(__name__)


class EmbeddingCache:
    """Manage Redis cache for embeddings."""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        """Initialize cache manager.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
        """
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=False
            )
            self.redis_client.ping()
            self.enabled = True
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self.redis_client = None
            self.enabled = False
    
    def get_hit_rate(self) -> float:
        """Calculate cache hit rate from metrics.
        
        Note: This would require tracking hits/misses separately.
        This is a placeholder implementation.
        
        Returns:
            Cache hit rate (0.0 to 1.0)
        """
        # Would need to track hits/misses in a separate key
        # For now, return 0.0 as placeholder
        return 0.0
    
    def cleanup_expired(self):
        """Remove expired entries (TTL handled by Redis automatically).
        
        This is a no-op since Redis handles TTL automatically.
        """
        if not self.enabled:
            return
        
        # Redis automatically removes expired keys
        # This method is kept for potential future manual cleanup logic
        pass
    
    def get_size_mb(self) -> float:
        """Calculate total cache size.
        
        Returns:
            Cache size in MB
        """
        if not self.enabled:
            return 0.0
        
        try:
            info = self.redis_client.info('memory')
            used_memory = info.get('used_memory', 0)
            return used_memory / (1024 * 1024)  # Convert to MB
        except Exception as e:
            logger.error(f"Error getting cache size: {e}")
            return 0.0

