"""
Quick tests for FIRDS RTS 23 enhancements
"""

def test_imports():
    """Test that all new types can be imported."""
    print("Testing imports...")
    from esma_dm import (
        FIRDSClient,
        FIRDSFile,
        FileType,
        AssetType,
        CommodityBaseProduct,
        OptionType,
        ExerciseStyle,
        DeliveryType,
        BondSeniority
    )
    print("✓ All imports successful")


def test_enums():
    """Test enum values and membership."""
    print("\nTesting enums...")
    from esma_dm import AssetType, CommodityBaseProduct, OptionType
    
    # Test AssetType
    assert AssetType.EQUITY.value == "E"
    assert AssetType.DEBT.value == "D"
    assert len([t for t in AssetType]) == 10
    print("✓ AssetType enum working")
    
    # Test CommodityBaseProduct
    assert CommodityBaseProduct.NRGY.value == "NRGY"
    assert CommodityBaseProduct.METL.value == "METL"
    print("✓ CommodityBaseProduct enum working")
    
    # Test OptionType
    assert OptionType.CALL.value == "CALL"
    assert OptionType.PUT.value == "PUT"
    print("✓ OptionType enum working")


def test_validation_utilities():
    """Test ISIN, LEI, CFI validation."""
    print("\nTesting validation utilities...")
    from esma_dm import FIRDSClient
    
    # Test ISIN validation
    assert FIRDSClient.validate_isin('US0378331005') == True
    assert FIRDSClient.validate_isin('GB00B1YW4409') == True
    assert FIRDSClient.validate_isin('INVALID') == False
    assert FIRDSClient.validate_isin('US037833') == False
    print("✓ ISIN validation working")
    
    # Test LEI validation
    assert FIRDSClient.validate_lei('549300VALTPVHYSYMH70') == True
    assert FIRDSClient.validate_lei('INVALID') == False
    assert FIRDSClient.validate_lei('12345') == False
    print("✓ LEI validation working")
    
    # Test CFI validation
    assert FIRDSClient.validate_cfi('ESVUFR') == True
    assert FIRDSClient.validate_cfi('DBXXXX') == True
    assert FIRDSClient.validate_cfi('FXXXXX') == True
    assert FIRDSClient.validate_cfi('INVALID') == False
    assert FIRDSClient.validate_cfi('ZXXXXX') == False  # Invalid first char
    print("✓ CFI validation working")


def test_firds_file_dataclass():
    """Test FIRDSFile dataclass."""
    print("\nTesting FIRDSFile dataclass...")
    from esma_dm import FIRDSFile
    import pandas as pd
    
    # Create test row
    row = pd.Series({
        'file_name': 'FULINS_E_20241231_1of2.zip',
        'file_type': 'Full',
        'publication_date': '2024-12-31',
        'download_link': 'https://example.com/file.zip'
    })
    
    # Create FIRDSFile from row
    file = FIRDSFile.from_row(row)
    
    assert file.file_name == 'FULINS_E_20241231_1of2.zip'
    assert file.file_type == 'Full'
    assert file.asset_type == 'E'
    assert file.date_extracted == '20241231'
    assert file.part_number == 1
    assert file.total_parts == 2
    print("✓ FIRDSFile dataclass working")


def test_client_new_methods():
    """Test new client methods exist and have correct signatures."""
    print("\nTesting client method signatures...")
    from esma_dm import FIRDSClient
    import inspect
    
    # Check new methods exist
    assert hasattr(FIRDSClient, 'get_files_metadata')
    assert hasattr(FIRDSClient, 'get_delta_files')
    assert hasattr(FIRDSClient, 'validate_isin')
    assert hasattr(FIRDSClient, 'validate_lei')
    assert hasattr(FIRDSClient, 'validate_cfi')
    print("✓ All new methods exist")
    
    # Check method signatures
    sig = inspect.signature(FIRDSClient.get_file_list)
    params = list(sig.parameters.keys())
    assert 'file_type' in params
    assert 'asset_type' in params
    print("✓ get_file_list has new parameters")
    
    sig = inspect.signature(FIRDSClient.get_delta_files)
    params = list(sig.parameters.keys())
    assert 'asset_type' in params
    assert 'date_from' in params
    assert 'date_to' in params
    print("✓ get_delta_files has correct signature")


def main():
    """Run all tests."""
    print("=" * 60)
    print("FIRDS RTS 23 Enhancements - Quick Tests")
    print("=" * 60)
    
    try:
        test_imports()
        test_enums()
        test_validation_utilities()
        test_firds_file_dataclass()
        test_client_new_methods()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
