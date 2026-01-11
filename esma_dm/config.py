"""
Configuration management for ESMA Data Manager
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# Import constants
from esma_dm.utils.constants import (
    FIRDS_SOLR_URL,
    FITRS_SOLR_URL,
    SSR_SOLR_URL,
    BENCHMARKS_SOLR_URL,
    DEFAULT_DATE_FROM,
    DEFAULT_REQUEST_LIMIT,
    DATABASE_MODES
)


def _get_project_root() -> Path:
    """Get the project root directory (where this package is installed)."""
    # Start from this file's location and go up to find the project root
    # esma_dm/config.py -> esma_dm/ -> project_root/
    return Path(__file__).parent.parent


@dataclass
class Config:
    """
    Configuration class for ESMA Data Manager.
    
    Attributes:
        downloads_path: Directory where downloaded files will be cached
        cache_enabled: Whether to use cached files (default: True)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        temp_dir: Temporary directory for file operations
        mode: Database mode ('current' or 'history')
        
    ESMA Register URLs:
        FIRDS_BASE_URL: FIRDS Solr endpoint
        FITRS_BASE_URL: FITRS Solr endpoint
        SSR_BASE_URL: SSR Solr endpoint
        BENCHMARKS_BASE_URL: Benchmarks Solr endpoint
    """
    
    downloads_path: Path = field(default_factory=lambda: _get_project_root() / "downloads" / "data")
    cache_enabled: bool = True
    log_level: str = "INFO"
    temp_dir: Optional[Path] = None
    mode: str = DATABASE_MODES['CURRENT']
    
    # ESMA Register URLs (from constants)
    FIRDS_BASE_URL: str = FIRDS_SOLR_URL
    FITRS_BASE_URL: str = FITRS_SOLR_URL
    SSR_BASE_URL: str = SSR_SOLR_URL
    BENCHMARKS_BASE_URL: str = BENCHMARKS_SOLR_URL
    
    # Default request parameters
    FIRDS_DATE_FROM: str = DEFAULT_DATE_FROM
    FIRDS_REQUEST_LIMIT: int = DEFAULT_REQUEST_LIMIT
    
    def __post_init__(self):
        """Ensure paths exist and are properly configured."""
        # Validate mode
        if self.mode not in DATABASE_MODES.values():
            raise ValueError(
                f"Invalid mode '{self.mode}'. Must be one of: {list(DATABASE_MODES.values())}"
            )
        
        # Ensure downloads path exists
        if not self.downloads_path.exists():
            self.downloads_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for different data types
        (self.downloads_path / "firds").mkdir(exist_ok=True)
        (self.downloads_path / "fitrs").mkdir(exist_ok=True)
        (self.downloads_path / "benchmarks").mkdir(exist_ok=True)
        (self.downloads_path / "ssr").mkdir(exist_ok=True)
        
        # Set temp_dir if not provided
        if self.temp_dir is None:
            self.temp_dir = self.downloads_path / "temp"
        
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def get_database_path(self, data_type: str = 'firds', mode: Optional[str] = None) -> Path:
        """
        Get the database file path for a specific data type and mode.
        
        Args:
            data_type: Type of data ('firds', 'fitrs', etc.)
            mode: Database mode ('current' or 'history'), defaults to config mode
        
        Returns:
            Path to database file
        
        Example:
            >>> config = Config()
            >>> config.get_database_path('firds', 'current')
            PosixPath('.../downloads/data/firds/firds_current.duckdb')
        """
        mode = mode or self.mode
        if mode not in DATABASE_MODES.values():
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {list(DATABASE_MODES.values())}")
        
        db_name = f"{data_type}_{mode}.duckdb" if data_type == 'firds' else f"{data_type}.db"
        return self.downloads_path / data_type / db_name
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Create configuration from environment variables.
        
        Environment variables:
            ESMA_DM_DOWNLOADS_PATH: Custom downloads directory
            ESMA_DM_CACHE_ENABLED: Enable/disable caching (true/false)
            ESMA_DM_LOG_LEVEL: Logging level
            ESMA_DM_MODE: Database mode ('current' or 'history')
        
        Returns:
            Config instance with environment-based settings
        """
        downloads_path = os.getenv("ESMA_DM_DOWNLOADS_PATH")
        cache_enabled = os.getenv("ESMA_DM_CACHE_ENABLED", "true").lower() == "true"
        log_level = os.getenv("ESMA_DM_LOG_LEVEL", "INFO")
        mode = os.getenv("ESMA_DM_MODE", DATABASE_MODES['CURRENT'])
        
        kwargs = {
            "cache_enabled": cache_enabled,
            "log_level": log_level,
            "mode": mode,
        }
        
        if downloads_path:
            kwargs["downloads_path"] = Path(downloads_path)
        
        return cls(**kwargs)


# Global default configuration instance
default_config = Config()
