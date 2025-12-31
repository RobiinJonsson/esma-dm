"""
Unit tests for utilities
"""
import pytest
from esma_dm.utils import Utils


class TestUtils:
    
    def test_hash(self):
        """Test hash generation."""
        hash1 = Utils._hash("test_string")
        hash2 = Utils._hash("test_string")
        hash3 = Utils._hash("different_string")
        
        assert hash1 == hash2
        assert hash1 != hash3
        assert len(hash1) == 32  # MD5 hash length
    
    def test_extract_file_name_from_url(self):
        """Test file name extraction from URL."""
        url = "https://example.com/path/to/file_name.zip"
        result = Utils.extract_file_name_from_url(url)
        
        assert result == "file_name"
    
    def test_extract_file_name_complex_url(self):
        """Test file name extraction from complex URL."""
        url = "https://registers.esma.europa.eu/files/FULINS_E_20240101_1of1.zip"
        result = Utils.extract_file_name_from_url(url)
        
        assert result == "FULINS_E_20240101_1of1"
    
    def test_set_logger(self):
        """Test logger creation."""
        logger = Utils.set_logger("test_logger")
        
        assert logger is not None
        assert logger.name == "test_logger"
        assert len(logger.handlers) > 0
