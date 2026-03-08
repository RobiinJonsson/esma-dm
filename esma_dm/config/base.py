"""
Configuration management for ESMA Data Manager - Base configuration class.
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


def _get_downloads_dir() -> Path:
    """Get the downloads directory for cached CSV files."""
    # Find the project root (where setup.py is located)
    current_dir = Path(__file__).parent
    project_root = current_dir
    
    # Go up until we find setup.py or reach the root
    while project_root.parent != project_root:
        if (project_root / "setup.py").exists():
            break
        project_root = project_root.parent
    
    # Use downloads/data directory in the project root
    downloads_dir = project_root / "downloads" / "data"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    return downloads_dir


def _get_database_dir() -> Path:
    """Get the database directory within the package storage."""
    # Path(__file__) is esma_dm/config/base.py — go up two levels to reach esma_dm/
    package_dir = Path(__file__).parent.parent  # esma_dm/
    db_dir = package_dir / "storage" / "duckdb" / "database"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir


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
    
    downloads_path: Path = field(default_factory=lambda: _get_downloads_dir())
    database_path: Path = field(default_factory=lambda: _get_database_dir())
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
        
        # Create subdirectories for different data types in downloads
        (self.downloads_path / "firds").mkdir(exist_ok=True)
        (self.downloads_path / "fitrs").mkdir(exist_ok=True)
        (self.downloads_path / "dvcap").mkdir(exist_ok=True)
        (self.downloads_path / "benchmarks").mkdir(exist_ok=True)
        (self.downloads_path / "ssr").mkdir(exist_ok=True)
        (self.downloads_path / "cache").mkdir(exist_ok=True)
        
        # Ensure database path exists
        if not self.database_path.exists():
            self.database_path.mkdir(parents=True, exist_ok=True)
        
        # Set temp_dir if not provided
        if self.temp_dir is None:
            self.temp_dir = self.downloads_path / "temp"
        
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def get_database_path(self, mode: Optional[str] = None) -> Path:
        """
        Get the unified database file path for the given mode.

        Both FIRDS and FITRS data are stored in the same database file.
        The file is named after the mode so current and history data
        remain in separate files.

        Args:
            mode: Database mode ('current' or 'history'), defaults to config mode

        Returns:
            Path to database file

        Example:
            >>> config = Config()
            >>> config.get_database_path('current')
            PosixPath('.../storage/duckdb/database/esma_current.duckdb')
        """
        mode = mode or self.mode
        if mode not in DATABASE_MODES.values():
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {list(DATABASE_MODES.values())}")
        return self.database_path / f"esma_{mode}.duckdb"
    
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
