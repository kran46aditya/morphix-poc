"""
Unit tests for local embedding service and cache.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestLocalEmbedder:
    """Test cases for LocalEmbedder class."""
    
    @pytest.mark.skip(reason="Requires sentence-transformers and Redis")
    def test_initialization(self):
        """Test embedder initialization."""
        from src.embeddings.local_embedder import LocalEmbedder
        
        embedder = LocalEmbedder()
        assert embedder is not None
        assert embedder.model_name is not None
    
    @pytest.mark.skip(reason="Requires sentence-transformers and Redis")
    def test_embed_single_text(self):
        """Test embedding a single text."""
        from src.embeddings.local_embedder import LocalEmbedder
        
        embedder = LocalEmbedder()
        text = "This is a test sentence"
        embedding = embedder.embed_batch([text])
        
        assert embedding is not None
        assert len(embedding) == 1
        assert len(embedding[0]) > 0
    
    @pytest.mark.skip(reason="Requires sentence-transformers and Redis")
    def test_embed_batch(self):
        """Test embedding a batch of texts."""
        from src.embeddings.local_embedder import LocalEmbedder
        
        embedder = LocalEmbedder()
        texts = ["First sentence", "Second sentence", "Third sentence"]
        embeddings = embedder.embed_batch(texts)
        
        assert len(embeddings) == 3
        assert all(len(emb) > 0 for emb in embeddings)
    
    @pytest.mark.skip(reason="Requires sentence-transformers and Redis")
    def test_cache_hit(self):
        """Test that cached embeddings are returned."""
        from src.embeddings.local_embedder import LocalEmbedder
        
        embedder = LocalEmbedder()
        text = "Test for cache"
        
        # First call - should compute
        embedding1 = embedder.embed_batch([text])[0]
        
        # Second call - should use cache
        embedding2 = embedder.embed_batch([text])[0]
        
        # Embeddings should be identical
        np.testing.assert_array_almost_equal(embedding1, embedding2)
    
    @patch('src.embeddings.local_embedder.redis.Redis')
    @patch('src.embeddings.local_embedder.SentenceTransformer')
    def test_embed_with_mock(self, mock_model, mock_redis):
        """Test embedding with mocked dependencies."""
        from src.embeddings.local_embedder import LocalEmbedder
        
        # Setup mocks
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.get.return_value = None  # Cache miss
        
        mock_model_instance = MagicMock()
        mock_model.return_value = mock_model_instance
        mock_model_instance.encode.return_value = np.array([[0.1, 0.2, 0.3]])
        
        embedder = LocalEmbedder()
        result = embedder.embed_batch(["test"])
        
        assert len(result) == 1
        mock_model_instance.encode.assert_called_once()
    
    @patch('src.embeddings.local_embedder.redis.Redis')
    def test_cache_retrieval(self, mock_redis):
        """Test cache retrieval."""
        from src.embeddings.local_embedder import LocalEmbedder
        import pickle
        
        # Setup mock Redis with cached value
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        
        cached_embedding = np.array([0.1, 0.2, 0.3])
        mock_redis_instance.get.return_value = pickle.dumps(cached_embedding)
        
        embedder = LocalEmbedder()
        result = embedder.embed_batch(["cached text"])
        
        assert len(result) == 1
        np.testing.assert_array_almost_equal(result[0], cached_embedding)
        mock_redis_instance.get.assert_called()


class TestEmbeddingCache:
    """Test cases for EmbeddingCache class."""
    
    @pytest.mark.skip(reason="Requires Redis connection")
    def test_cache_initialization(self):
        """Test cache initialization."""
        from src.embeddings.embedding_cache import EmbeddingCache
        
        cache = EmbeddingCache()
        assert cache is not None
    
    @pytest.mark.skip(reason="Requires Redis connection")
    def test_get_cache_hit_rate(self):
        """Test cache hit rate calculation."""
        from src.embeddings.embedding_cache import EmbeddingCache
        
        cache = EmbeddingCache()
        hit_rate = cache.get_cache_hit_rate()
        
        assert 0 <= hit_rate <= 1
    
    @pytest.mark.skip(reason="Requires Redis connection")
    def test_cleanup_old_entries(self):
        """Test cleanup of old cache entries."""
        from src.embeddings.embedding_cache import EmbeddingCache
        
        cache = EmbeddingCache()
        cleaned = cache.cleanup_old_entries(days=30)
        
        assert isinstance(cleaned, int)
        assert cleaned >= 0
    
    @patch('src.embeddings.embedding_cache.redis.Redis')
    def test_cache_stats_with_mock(self, mock_redis):
        """Test cache statistics with mocked Redis."""
        from src.embeddings.embedding_cache import EmbeddingCache
        
        # Setup mock
        mock_redis_instance = MagicMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.info.return_value = {'used_memory': 1024 * 1024}  # 1MB
        
        cache = EmbeddingCache()
        size = cache.get_cache_size_mb()
        
        assert size >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

