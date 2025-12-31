"""
Unit tests for configuration
"""
import pytest
from pathlib import Path
from esma_dm import Config


class TestConfig:
    
    def test_default_config(self):
        """Test default configuration."""
        config = Config()
        
        assert config.downloads_path.exists()
        assert config.cache_enabled is True
        assert config.log_level == "INFO"
        assert config.temp_dir is not None
    
    def test_custom_config(self):
        """Test custom configuration."""
        custom_path = Path.home() / "custom_esma_data"
        
        config = Config(
            downloads_path=custom_path,
            cache_enabled=False,
            log_level="DEBUG"
        )
        
        assert config.downloads_path == custom_path
        assert config.cache_enabled is False
        assert config.log_level == "DEBUG"
    
    def test_subdirectories_created(self):
        """Test that subdirectories are created."""
        config = Config()
        
        assert (config.downloads_path / "firds").exists()
        assert (config.downloads_path / "fitrs").exists()
        assert (config.downloads_path / "benchmarks").exists()
        assert (config.downloads_path / "ssr").exists()
    
    def test_from_env(self, monkeypatch):
        """Test configuration from environment variables."""
        monkeypatch.setenv("ESMA_DM_CACHE_ENABLED", "false")
        monkeypatch.setenv("ESMA_DM_LOG_LEVEL", "DEBUG")
        
        config = Config.from_env()
        
        assert config.cache_enabled is False
        assert config.log_level == "DEBUG"
