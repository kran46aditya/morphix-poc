"""
Local embedding generation using sentence-transformers.

Zero API costs, Redis caching for deduplication.
"""

from sentence_transformers import SentenceTransformer
import hashlib
import numpy as np
import redis
from typing import List, Optional, Tuple
import pickle
import logging

logger = logging.getLogger(__name__)


class LocalEmbedder:
    """Generate embeddings locally with caching."""
    
    def __init__(
        self, 
        model_name: str = "all-MiniLM-L6-v2",
        cache_ttl_days: int = 7,
        device: str = "cpu",
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0
    ):
        """
        Load sentence-transformers model.
        Connect to Redis for caching.
        Model outputs 384-dim embeddings.
        
        Args:
            model_name: Sentence transformer model name
            cache_ttl_days: Cache TTL in days
            device: Device to use ("cpu" or "cuda")
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
        """
        logger.info(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name, device=device)
        self.cache_ttl = cache_ttl_days * 86400  # Convert to seconds
        
        try:
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                db=redis_db,
                decode_responses=False
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Connected to Redis for embedding cache")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}. Caching disabled.")
            self.redis_client = None
    
    def _hash_text(self, text: str) -> str:
        """SHA-256 hash for cache key.
        
        Args:
            text: Text to hash
            
        Returns:
            Hex digest of hash
        """
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
    def embed_batch(
        self, 
        texts: List[str], 
        use_cache: bool = True
    ) -> np.ndarray:
        """
        Generate embeddings for text batch.
        
        Steps:
        1. Hash each text
        2. Check Redis cache (MGET all hashes)
        3. For cache misses: generate embeddings in batches of 32
        4. Store new embeddings in Redis with TTL
        5. Return combined array (cached + new)
        
        Args:
            texts: List of texts to embed
            use_cache: Whether to use cache
            
        Returns:
            np.ndarray of shape (len(texts), 384)
        """
        if not texts:
            return np.array([])
        
        if not use_cache or not self.redis_client:
            # No caching, generate all embeddings
            return self.model.encode(texts, convert_to_numpy=True, batch_size=32)
        
        # Hash all texts
        hashes = [self._hash_text(text) for text in texts]
        
        # Check cache
        cache_keys = [f"embedding:{h}" for h in hashes]
        cached_embeddings = self.redis_client.mget(cache_keys)
        
        # Separate cache hits and misses
        embeddings_list = []
        texts_to_embed = []
        indices_to_embed = []
        
        for idx, (text, cached) in enumerate(zip(texts, cached_embeddings)):
            if cached is not None:
                # Cache hit
                try:
                    embedding = pickle.loads(cached)
                    embeddings_list.append((idx, embedding))
                except Exception as e:
                    logger.warning(f"Failed to unpickle cached embedding: {e}")
                    texts_to_embed.append(text)
                    indices_to_embed.append(idx)
            else:
                # Cache miss
                texts_to_embed.append(text)
                indices_to_embed.append(idx)
        
        # Generate embeddings for cache misses
        if texts_to_embed:
            new_embeddings = self.model.encode(
                texts_to_embed, 
                convert_to_numpy=True, 
                batch_size=32
            )
            
            # Store in cache
            pipeline = self.redis_client.pipeline()
            for text, embedding, idx in zip(texts_to_embed, new_embeddings, indices_to_embed):
                hash_key = hashes[indices_to_embed.index(idx)]
                cache_key = f"embedding:{hash_key}"
                pipeline.setex(
                    cache_key,
                    self.cache_ttl,
                    pickle.dumps(embedding)
                )
                embeddings_list.append((indices_to_embed[indices_to_embed.index(idx)], embedding))
            
            pipeline.execute()
        
        # Reconstruct in original order
        embeddings_list.sort(key=lambda x: x[0])
        result = np.array([emb for _, emb in embeddings_list])
        
        return result
    
    def embed_with_deduplication(
        self, 
        texts: List[str]
    ) -> Tuple[np.ndarray, List[bool]]:
        """
        Deduplicate texts before embedding.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            Tuple of (embeddings, was_cached) where was_cached indicates cache hits
        """
        if not texts:
            return np.array([]), []
        
        # Deduplicate
        unique_texts = []
        text_to_index = {}
        indices = []
        
        for idx, text in enumerate(texts):
            if text not in text_to_index:
                text_to_index[text] = len(unique_texts)
                unique_texts.append(text)
            indices.append(text_to_index[text])
        
        # Embed unique texts
        unique_embeddings = self.embed_batch(unique_texts, use_cache=True)
        
        # Map back to original order
        embeddings = unique_embeddings[indices]
        
        # Check which were cached (simplified - assume cache hit if in unique set)
        was_cached = [False] * len(texts)  # Would need to track this during embed_batch
        
        return embeddings, was_cached
    
    def get_cache_stats(self) -> dict:
        """Return cache hit rate, total embeddings cached.
        
        Returns:
            Dictionary with cache statistics
        """
        if not self.redis_client:
            return {
                "enabled": False,
                "total_cached": 0,
                "hit_rate": 0.0
            }
        
        try:
            # Count keys with embedding prefix
            keys = self.redis_client.keys("embedding:*")
            total_cached = len(keys)
            
            # Note: Hit rate would need to be tracked separately
            # This is a simplified version
            return {
                "enabled": True,
                "total_cached": total_cached,
                "hit_rate": 0.0  # Would need tracking mechanism
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {
                "enabled": False,
                "total_cached": 0,
                "hit_rate": 0.0
            }
    
    def clear_cache(self):
        """Clear all cached embeddings."""
        if self.redis_client:
            try:
                keys = self.redis_client.keys("embedding:*")
                if keys:
                    self.redis_client.delete(*keys)
                logger.info(f"Cleared {len(keys)} cached embeddings")
            except Exception as e:
                logger.error(f"Error clearing cache: {e}")

