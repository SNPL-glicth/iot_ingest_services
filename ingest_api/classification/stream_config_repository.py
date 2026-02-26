"""Repository for loading stream configurations from PostgreSQL.

Provides cached access to stream configurations and value constraints
for universal data classification.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.domain.stream_config import StreamConfig, ValueConstraints

logger = logging.getLogger(__name__)


class StreamConfigRepository:
    """Loads and caches StreamConfig from PostgreSQL.
    
    Uses LRU cache with TTL to minimize database queries while
    ensuring fresh configuration data.
    """
    
    def __init__(
        self,
        postgres_engine: Optional[Engine],
        max_cache: int = 10000,
        ttl: int = 300,
    ):
        """Initialize the repository.
        
        Args:
            postgres_engine: PostgreSQL engine (None if not configured)
            max_cache: Maximum number of configs to cache
            ttl: Time-to-live for cache entries in seconds
        """
        self._engine = postgres_engine
        self._cache: OrderedDict[str, StreamConfig] = OrderedDict()
        self._cache_times: Dict[str, float] = {}
        self._max = max_cache
        self._ttl = ttl
    
    def get_config(
        self,
        stream_id: str,
        source_id: str,
        domain: str,
    ) -> Optional[StreamConfig]:
        """Get stream configuration from cache or database.
        
        Args:
            stream_id: Stream identifier
            source_id: Source identifier
            domain: Domain identifier
            
        Returns:
            StreamConfig if found, None otherwise
        """
        if self._engine is None:
            return None
        
        key = f"{domain}:{source_id}:{stream_id}"
        
        # Check cache
        if key in self._cache:
            if time.time() - self._cache_times[key] < self._ttl:
                # Move to end (LRU)
                self._cache.move_to_end(key)
                return self._cache[key]
            else:
                # Expired - remove
                del self._cache[key]
                del self._cache_times[key]
        
        # Cache miss - load from database
        config = self._load_from_db(stream_id, source_id, domain)
        
        if config:
            self._set_cache(key, config)
        
        return config
    
    def _load_from_db(
        self,
        stream_id: str,
        source_id: str,
        domain: str,
    ) -> Optional[StreamConfig]:
        """Load stream configuration from PostgreSQL.
        
        Args:
            stream_id: Stream identifier
            source_id: Source identifier
            domain: Domain identifier
            
        Returns:
            StreamConfig if found, None otherwise
        """
        try:
            with self._engine.connect() as conn:
                # Load stream config
                result = conn.execute(
                    text("""
                        SELECT 
                            id, stream_id, source_id, domain, display_name, unit,
                            enable_ml_prediction, enable_alerting, domain_metadata
                        FROM stream_configs
                        WHERE stream_id = :stream_id 
                          AND source_id = :source_id 
                          AND domain = :domain
                    """),
                    {
                        "stream_id": stream_id,
                        "source_id": source_id,
                        "domain": domain,
                    },
                ).fetchone()
                
                if not result:
                    return None
                
                config_id = result[0]
                
                # Load constraints
                constraints_result = conn.execute(
                    text("""
                        SELECT 
                            physical_min, physical_max,
                            operational_min, operational_max,
                            warning_min, warning_max,
                            critical_min, critical_max,
                            max_abs_delta, max_rel_delta,
                            noise_abs_threshold, z_score_threshold,
                            consecutive_violations_required, cooldown_seconds
                        FROM value_constraints
                        WHERE stream_config_id = :config_id
                    """),
                    {"config_id": config_id},
                ).fetchone()
                
                constraints = None
                if constraints_result:
                    constraints = ValueConstraints(
                        physical_min=constraints_result[0],
                        physical_max=constraints_result[1],
                        operational_min=constraints_result[2],
                        operational_max=constraints_result[3],
                        warning_min=constraints_result[4],
                        warning_max=constraints_result[5],
                        critical_min=constraints_result[6],
                        critical_max=constraints_result[7],
                        max_abs_delta=constraints_result[8],
                        max_rel_delta=constraints_result[9],
                        noise_abs_threshold=constraints_result[10],
                        z_score_threshold=constraints_result[11] or 3.0,
                        consecutive_violations_required=constraints_result[12] or 1,
                        cooldown_seconds=constraints_result[13] or 300,
                    )
                
                # Parse domain_metadata JSON
                import json
                domain_metadata = {}
                if result[8]:
                    try:
                        domain_metadata = json.loads(result[8])
                    except json.JSONDecodeError:
                        logger.warning(
                            "Failed to parse domain_metadata for stream %s",
                            stream_id,
                        )
                
                return StreamConfig(
                    stream_id=result[1],
                    source_id=result[2],
                    domain=result[3],
                    stream_type="continuous",  # Default
                    constraints=constraints,
                    enable_ml_prediction=result[6],
                    enable_alerting=result[7],
                    domain_metadata=domain_metadata,
                )
                
        except Exception as e:
            logger.exception(
                "Failed to load stream config for %s:%s:%s - %s",
                domain, source_id, stream_id, e,
            )
            return None
    
    def _set_cache(self, key: str, config: StreamConfig) -> None:
        """Add config to cache with LRU eviction.
        
        Args:
            key: Cache key
            config: Stream configuration
        """
        # Evict oldest if at capacity
        if len(self._cache) >= self._max:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
            del self._cache_times[oldest_key]
        
        self._cache[key] = config
        self._cache_times[key] = time.time()
    
    def get_default_config(self, domain: str) -> StreamConfig:
        """Get default configuration for a domain.
        
        Used when no specific configuration exists for a stream.
        
        Args:
            domain: Domain identifier
            
        Returns:
            Default StreamConfig for the domain
        """
        # Domain-specific defaults
        defaults = {
            "infrastructure": StreamConfig(
                stream_id="*",
                source_id="*",
                domain="infrastructure",
                stream_type="continuous",
                enable_ml_prediction=True,
                enable_alerting=True,
            ),
            "finance": StreamConfig(
                stream_id="*",
                source_id="*",
                domain="finance",
                stream_type="continuous",
                enable_ml_prediction=True,
                enable_alerting=True,
            ),
            "health": StreamConfig(
                stream_id="*",
                source_id="*",
                domain="health",
                stream_type="continuous",
                enable_ml_prediction=True,
                enable_alerting=True,
            ),
        }
        
        return defaults.get(
            domain,
            StreamConfig(
                stream_id="*",
                source_id="*",
                domain=domain,
                stream_type="continuous",
                enable_ml_prediction=False,
                enable_alerting=False,
            ),
        )
    
    def clear_cache(self) -> None:
        """Clear the entire cache."""
        self._cache.clear()
        self._cache_times.clear()
        logger.info("Stream config cache cleared")
