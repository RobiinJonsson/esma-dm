"""
Unit tests for QueryBuilder utility.
"""
import pytest
from esma_dm.utils import QueryBuilder, QueryMode


class TestQueryBuilder:
    """Test cases for QueryBuilder utility."""
    
    def test_init_current_mode(self):
        """Test QueryBuilder initialization in current mode."""
        qb = QueryBuilder('current')
        assert qb.mode == 'current'
    
    def test_init_history_mode(self):
        """Test QueryBuilder initialization in history mode."""
        qb = QueryBuilder('history')
        assert qb.mode == 'history'
    
    def test_get_instrument_by_isin_current(self):
        """Test ISIN query generation in current mode."""
        qb = QueryBuilder('current')
        query = qb.get_instrument_by_isin('US0378331005')
        
        assert 'SELECT' in query
        assert 'isin, cfi_code, full_name, issuer, source_file, indexed_at, asset_type' in query
        assert 'FROM instruments' in query
        assert 'WHERE isin = ?' in query
        assert 'latest_record_flag' not in query  # Not in current mode
    
    def test_get_instrument_by_isin_history(self):
        """Test ISIN query generation in history mode."""
        qb = QueryBuilder('history')
        query = qb.get_instrument_by_isin('US0378331005')
        
        assert 'SELECT' in query
        assert 'version_number' in query
        assert 'valid_from_date' in query
        assert 'latest_record_flag = true' in query
    
    def test_get_instrument_history_requires_history_mode(self):
        """Test that history queries require history mode."""
        qb = QueryBuilder('current')
        
        with pytest.raises(ValueError, match="History queries only available in history mode"):
            qb.get_instrument_history('US0378331005')
    
    def test_get_instrument_history_success(self):
        """Test successful history query generation."""
        qb = QueryBuilder('history')
        query = qb.get_instrument_history('US0378331005')
        
        assert 'ORDER BY version_number DESC' in query
        assert 'valid_from_date' in query
        assert 'record_type' in query
    
    def test_get_asset_specific_details_valid_type(self):
        """Test asset-specific query for valid asset type."""
        qb = QueryBuilder()
        query = qb.get_asset_specific_details('E', 'US0378331005')
        
        assert 'FROM equity_instruments' in query
        assert 'WHERE isin = ?' in query
    
    def test_get_asset_specific_details_invalid_type(self):
        """Test asset-specific query for invalid asset type."""
        qb = QueryBuilder()
        
        with pytest.raises(ValueError, match="Unknown asset type: X"):
            qb.get_asset_specific_details('X', 'US0378331005')
    
    def test_search_instruments(self):
        """Test search query generation."""
        qb = QueryBuilder()
        query = qb.search_instruments(20)
        
        assert 'SELECT isin, cfi_code, full_name, issuer' in query
        assert 'WHERE full_name ILIKE ? OR isin ILIKE ?' in query
        assert 'ORDER BY' in query
        assert 'LIMIT 20' in query
    
    def test_get_instruments_by_cfi_category(self):
        """Test CFI category query generation."""
        qb = QueryBuilder()
        query = qb.get_instruments_by_cfi_category(50)
        
        assert 'WHERE cfi_code LIKE ?' in query
        assert 'ORDER BY full_name' in query
        assert 'LIMIT 50' in query
    
    def test_get_stats_by_asset_type(self):
        """Test asset type statistics query."""
        qb = QueryBuilder()
        query = qb.get_stats_by_asset_type()
        
        assert 'LEFT(cfi_code, 1) as asset_type' in query
        assert 'COUNT(*) as count' in query
        assert 'GROUP BY LEFT(cfi_code, 1)' in query
        assert 'ORDER BY count DESC' in query
    
    def test_bulk_insert_instruments(self):
        """Test bulk insert query generation."""
        qb = QueryBuilder()
        columns = ['isin', 'cfi_code', 'full_name']
        query = qb.bulk_insert_instruments(columns)
        
        assert 'INSERT INTO instruments (isin, cfi_code, full_name)' in query
        assert 'VALUES (?, ?, ?)' in query
    
    def test_bulk_insert_asset_table(self):
        """Test asset table bulk insert."""
        qb = QueryBuilder()
        columns = ['isin', 'shares_outstanding']
        query = qb.bulk_insert_asset_table('E', columns)
        
        assert 'INSERT INTO equity_instruments' in query
        assert 'VALUES (?, ?)' in query
    
    def test_bulk_insert_asset_table_invalid_type(self):
        """Test asset table insert with invalid type."""
        qb = QueryBuilder()
        
        with pytest.raises(ValueError, match="Unknown asset type: X"):
            qb.bulk_insert_asset_table('X', ['isin'])
    
    def test_create_index(self):
        """Test index creation query."""
        qb = QueryBuilder()
        query = qb.create_index('instruments', 'isin')
        
        assert 'CREATE INDEX IF NOT EXISTS idx_instruments_isin' in query
        assert 'ON instruments (isin)' in query
    
    def test_create_index_custom_name(self):
        """Test index creation with custom name."""
        qb = QueryBuilder()
        query = qb.create_index('instruments', 'cfi_code', 'custom_idx')
        
        assert 'CREATE INDEX IF NOT EXISTS custom_idx' in query
    
    def test_upsert_instrument(self):
        """Test upsert query generation."""
        qb = QueryBuilder()
        columns = ['isin', 'cfi_code']
        query = qb.upsert_instrument(columns)
        
        assert 'INSERT OR REPLACE INTO instruments' in query
        assert 'VALUES (?, ?)' in query
    
    def test_get_latest_version(self):
        """Test latest version query."""
        qb = QueryBuilder()
        query = qb.get_latest_version('US0378331005')
        
        assert 'SELECT MAX(version_number)' in query
        assert 'WHERE isin = ?' in query
    
    def test_update_previous_versions(self):
        """Test previous version update query."""
        qb = QueryBuilder()
        query = qb.update_previous_versions('US0378331005')
        
        assert 'UPDATE instruments' in query
        assert 'SET latest_record_flag = false' in query
        assert 'WHERE isin = ? AND latest_record_flag = true' in query
    
    def test_get_instruments_by_date_range_current(self):
        """Test date range query in current mode."""
        qb = QueryBuilder('current')
        query = qb.get_instruments_by_date_range('2024-01-01', '2024-01-31')
        
        assert 'DATE(indexed_at)' in query
        assert 'ORDER BY indexed_at DESC' in query
    
    def test_get_instruments_by_date_range_history(self):
        """Test date range query in history mode."""
        qb = QueryBuilder('history')
        query = qb.get_instruments_by_date_range('2024-01-01', '2024-01-31')
        
        assert 'valid_from_date' in query
        assert 'ORDER BY valid_from_date DESC' in query
    
    def test_format_search_params(self):
        """Test search parameter formatting."""
        params = QueryBuilder.format_search_params('APPLE')
        expected = ['%APPLE%', '%APPLE%', 'APPLE', 'APPLE%', 'APPLE%']
        assert params == expected
    
    def test_get_asset_type_from_cfi_valid(self):
        """Test CFI asset type extraction for valid codes."""
        assert QueryBuilder.get_asset_type_from_cfi('ESVUFR') == 'E'
        assert QueryBuilder.get_asset_type_from_cfi('DBFTFR') == 'D'
        assert QueryBuilder.get_asset_type_from_cfi('OPASPS') == 'O'
    
    def test_get_asset_type_from_cfi_invalid(self):
        """Test CFI asset type extraction for invalid codes."""
        assert QueryBuilder.get_asset_type_from_cfi('') is None
        assert QueryBuilder.get_asset_type_from_cfi(None) is None
    
    def test_validate_asset_type_valid(self):
        """Test asset type validation for valid types."""
        assert QueryBuilder.validate_asset_type('E') is True
        assert QueryBuilder.validate_asset_type('D') is True
        assert QueryBuilder.validate_asset_type('O') is True
        assert QueryBuilder.validate_asset_type('S') is True
    
    def test_validate_asset_type_invalid(self):
        """Test asset type validation for invalid types."""
        assert QueryBuilder.validate_asset_type('X') is False
        assert QueryBuilder.validate_asset_type('Z') is False
        assert QueryBuilder.validate_asset_type('') is False
    
    def test_asset_type_tables_mapping(self):
        """Test asset type to table name mapping."""
        assert 'E' in QueryBuilder.ASSET_TYPE_TABLES
        assert QueryBuilder.ASSET_TYPE_TABLES['E'] == 'equity_instruments'
        assert QueryBuilder.ASSET_TYPE_TABLES['D'] == 'debt_instruments'
        assert QueryBuilder.ASSET_TYPE_TABLES['O'] == 'option_instruments'