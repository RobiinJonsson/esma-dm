"""
Configuration management for ESMA Data Manager.

This module provides centralized configuration classes for different components
and operational modes, eliminating hardcoded defaults throughout the codebase.
"""

from .base import Config, default_config
from .registry import (
    FIRDSConfig,
    FITRSConfig,
    DatabaseConfig,
    get_firds_config,
    get_fitrs_config,
    get_database_config
)

__all__ = [
    # Base configuration
    'Config',
    'default_config',
    # Specialized configurations
    'FIRDSConfig',
    'FITRSConfig', 
    'DatabaseConfig',
    # Factory functions
    'get_firds_config',
    'get_fitrs_config',
    'get_database_config'
]