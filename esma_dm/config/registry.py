"""
Configuration registry for ESMA Data Manager components.

This module provides specialized configuration classes that eliminate
hardcoded defaults and provide mode-specific settings.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pathlib import Path

# Import constants for defaults
from esma_dm.utils.constants import (
    DEFAULT_DATE_FROM,
    DEFAULT_REQUEST_LIMIT,
    MAX_REQUEST_LIMIT,
    DATABASE_MODES
)


@dataclass
class FIRDSConfig:
    """Centralized FIRDS configuration."""
    
    # Date range defaults
    default_date_from: str = DEFAULT_DATE_FROM
    default_date_to: Optional[str] = field(default_factory=lambda: datetime.today().strftime('%Y-%m-%d'))
    
    # Request limits
    default_limit: int = DEFAULT_REQUEST_LIMIT
    max_limit: int = MAX_REQUEST_LIMIT
    
    # Caching behavior
    cache_ttl_hours: int = 24
    cache_enabled: bool = True
    
    # Processing configuration
    batch_size: int = 10000
    chunk_size: int = 50000
    
    # Mode-specific settings
    mode: str = DATABASE_MODES['CURRENT']
    
    @classmethod
    def for_mode(cls, mode: str) -> 'FIRDSConfig':
        """
        Get mode-specific FIRDS configuration.
        
        Args:
            mode: Database mode ('current' or 'history')
            
        Returns:
            FIRDSConfig instance with mode-specific settings
        """
        if mode == 'current':
            # For current mode, only look at recent files (last 2 months)
            recent_date = (datetime.today() - timedelta(days=60)).strftime('%Y-%m-%d')
            return cls(
                default_date_from=recent_date,
                cache_enabled=True,
                cache_ttl_hours=24,
                mode=mode
            )
        elif mode == 'history':
            # For history mode, use full date range
            return cls(
                default_date_from=DEFAULT_DATE_FROM,
                cache_enabled=False,  # Always fresh data for history
                cache_ttl_hours=0,
                batch_size=5000,     # Smaller batches for history processing
                mode=mode
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    def get_date_range(self, date_from: Optional[str] = None, date_to: Optional[str] = None) -> tuple[str, str]:
        """
        Get validated date range with defaults.
        
        Args:
            date_from: Start date override
            date_to: End date override
            
        Returns:
            Tuple of (date_from, date_to)
        """
        return (
            date_from or self.default_date_from,
            date_to or self.default_date_to
        )
    
    def validate_limit(self, limit: int) -> int:
        """
        Validate and bound request limit.
        
        Args:
            limit: Requested limit
            
        Returns:
            Bounded limit within valid range
        """
        return min(max(limit, 1), self.max_limit)


@dataclass 
class FITRSConfig:
    """Centralized FITRS configuration."""
    
    # Date range defaults (FITRS data available from 2017)
    default_date_from: str = "2017-01-01"
    default_date_to: Optional[str] = field(default_factory=lambda: datetime.today().strftime('%Y-%m-%d'))
    
    # Request limits (FITRS allows larger batches)
    default_limit: int = 10000
    max_limit: int = 50000
    
    # Processing configuration
    batch_size: int = 25000
    cache_enabled: bool = True
    cache_ttl_hours: int = 12  # Shorter TTL for more frequent updates
    
    def get_date_range(self, date_from: Optional[str] = None, date_to: Optional[str] = None) -> tuple[str, str]:
        """
        Get validated date range with defaults.
        
        Args:
            date_from: Start date override
            date_to: End date override
            
        Returns:
            Tuple of (date_from, date_to)
        """
        return (
            date_from or self.default_date_from,
            date_to or self.default_date_to
        )


@dataclass
class DatabaseConfig:
    """Database-specific configuration settings."""
    
    # Connection settings
    connection_timeout: int = 30
    query_timeout: int = 300
    
    # Performance settings
    memory_limit: str = "2GB"
    max_threads: int = 4
    
    # Bulk operations
    bulk_insert_batch_size: int = 100000
    vacuum_threshold: int = 1000000
    
    # Mode-specific settings
    enable_versioning: bool = False
    enable_constraints: bool = True
    
    @classmethod
    def for_mode(cls, mode: str) -> 'DatabaseConfig':
        """Get mode-specific database configuration."""
        if mode == 'current':
            return cls(
                enable_versioning=False,
                enable_constraints=True,
                bulk_insert_batch_size=100000
            )
        elif mode == 'history':
            return cls(
                enable_versioning=True,
                enable_constraints=False,  # Less constraints for faster bulk ops
                bulk_insert_batch_size=50000
            )
        else:
            raise ValueError(f"Unknown mode: {mode}")


# Factory functions for easy access
def get_firds_config(mode: str = 'current') -> FIRDSConfig:
    """Get FIRDS configuration for specified mode."""
    return FIRDSConfig.for_mode(mode)


def get_fitrs_config() -> FITRSConfig:
    """Get FITRS configuration."""
    return FITRSConfig()


def get_database_config(mode: str = 'current') -> DatabaseConfig:
    """Get database configuration for specified mode."""
    return DatabaseConfig.for_mode(mode)