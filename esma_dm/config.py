"""
Configuration management for ESMA Data Manager
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


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
    """
    
    downloads_path: Path = field(default_factory=lambda: _get_project_root() / "downloads" / "data")
    cache_enabled: bool = True
    log_level: str = "INFO"
    temp_dir: Optional[Path] = None
    
    def __post_init__(self):
        """Ensure paths exist and are properly configured."""
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
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Create configuration from environment variables.
        
        Environment variables:
            ESMA_DM_DOWNLOADS_PATH: Custom downloads directory
            ESMA_DM_CACHE_ENABLED: Enable/disable caching (true/false)
            ESMA_DM_LOG_LEVEL: Logging level
        
        Returns:
            Config instance with environment-based settings
        """
        downloads_path = os.getenv("ESMA_DM_DOWNLOADS_PATH")
        cache_enabled = os.getenv("ESMA_DM_CACHE_ENABLED", "true").lower() == "true"
        log_level = os.getenv("ESMA_DM_LOG_LEVEL", "INFO")
        
        kwargs = {
            "cache_enabled": cache_enabled,
            "log_level": log_level,
        }
        
        if downloads_path:
            kwargs["downloads_path"] = Path(downloads_path)
        
        return cls(**kwargs)


# Global default configuration instance
default_config = Config()
