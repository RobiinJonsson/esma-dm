"""
Unit tests for FIRDS client
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch
from esma_dm import FIRDSClient


class TestFIRDSClient:
    
    def test_client_initialization(self):
        """Test FIRDS client initialization."""
        client = FIRDSClient(date_from='2024-01-01', date_to='2024-12-31')
        
        assert client.date_from == '2024-01-01'
        assert client.date_to == '2024-12-31'
        assert client.limit == 10000
    
    def test_invalid_asset_type(self):
        """Test that invalid asset type raises ValueError."""
        client = FIRDSClient()
        
        with pytest.raises(ValueError):
            client.get_latest_full_files(asset_type='X')
    
    @patch('esma_dm.firds.requests.get')
    def test_get_file_list_success(self, mock_get):
        """Test successful file list retrieval."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """<?xml version="1.0" encoding="UTF-8"?>
        <response>
            <result>
                <doc>
                    <str name="file_name">FULINS_E_20240101_1of1.zip</str>
                    <str name="file_type">Full</str>
                </doc>
            </result>
        </response>"""
        mock_get.return_value = mock_response
        
        client = FIRDSClient()
        result = client.get_file_list()
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
    
    @patch('esma_dm.firds.requests.get')
    def test_get_file_list_failure(self, mock_get):
        """Test file list retrieval failure."""
        # Mock failed response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        
        client = FIRDSClient()
        
        with pytest.raises(Exception):
            client.get_file_list()
