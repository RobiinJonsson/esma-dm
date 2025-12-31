"""
Advanced FIRDS Client Usage Examples

Demonstrates new features based on RTS 23 specifications:
- File metadata with FIRDSFile dataclass
- Enhanced filtering by file type (FULINS/DLTINS)
- Delta file retrieval
- Validation utilities for ISIN, LEI, CFI codes
- Usage of new enums for classifications
"""

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


def example_file_metadata():
    """Example: Get structured file metadata."""
    print("=" * 80)
    print("Example 1: Structured File Metadata")
    print("=" * 80)
    
    firds = FIRDSClient(date_from='2024-01-01')
    
    # Get metadata for FULINS equity files
    files = firds.get_files_metadata(file_type='FULINS', asset_type='E')
    
    print(f"\nFound {len(files)} FULINS equity files")
    print("\nLatest 5 files:")
    for f in files[-5:]:
        print(f"  {f.file_name}")
        print(f"    Type: {f.file_type}")
        print(f"    Asset Type: {f.asset_type}")
        print(f"    Date: {f.date_extracted}")
        print(f"    Part: {f.part_number}/{f.total_parts}")
        print(f"    Published: {f.publication_date}")
        print()


def example_filter_by_file_type():
    """Example: Filter files by FULINS or DLTINS."""
    print("=" * 80)
    print("Example 2: Filter by File Type")
    print("=" * 80)
    
    firds = FIRDSClient(date_from='2024-12-01')
    
    # Get only FULINS (full snapshot) files
    fulins_files = firds.get_file_list(file_type='FULINS')
    print(f"\nFULINS files: {len(fulins_files)}")
    
    # Get only DLTINS (delta/incremental) files
    dltins_files = firds.get_file_list(file_type='DLTINS')
    print(f"DLTINS files: {len(dltins_files)}")
    
    # Get DLTINS files for specific asset type
    equity_deltas = firds.get_file_list(file_type='DLTINS', asset_type='E')
    print(f"Equity DLTINS files: {len(equity_deltas)}")


def example_delta_files():
    """Example: Retrieve delta files for tracking changes."""
    print("=" * 80)
    print("Example 3: Delta Files (Incremental Changes)")
    print("=" * 80)
    
    firds = FIRDSClient()
    
    # Get delta files for a date range
    print("\nRetrieving delta files for equities...")
    changes = firds.get_delta_files(
        asset_type='E',
        date_from='2024-12-01',
        date_to='2024-12-31'
    )
    
    if not changes.empty:
        print(f"Retrieved {len(changes)} delta records")
        
        # Analyze record types
        if 'RecrdTp' in changes.columns:
            print("\nRecord types:")
            print(changes['RecrdTp'].value_counts())
    else:
        print("No delta records found for the specified period")


def example_validation_utilities():
    """Example: Validate ISIN, LEI, and CFI codes."""
    print("=" * 80)
    print("Example 4: Validation Utilities")
    print("=" * 80)
    
    # Test ISIN validation
    test_isins = [
        'US0378331005',  # Apple - Valid
        'GB00B1YW4409',  # Sage Group - Valid
        'INVALID123',     # Invalid
        'US037833',       # Too short
    ]
    
    print("\nISIN Validation:")
    for isin in test_isins:
        is_valid = FIRDSClient.validate_isin(isin)
        print(f"  {isin}: {'✓ Valid' if is_valid else '✗ Invalid'}")
    
    # Test LEI validation
    test_leis = [
        '549300VALTPVHYSYMH70',  # Valid
        'INVALID',                # Invalid
    ]
    
    print("\nLEI Validation:")
    for lei in test_leis:
        is_valid = FIRDSClient.validate_lei(lei)
        print(f"  {lei}: {'✓ Valid' if is_valid else '✗ Invalid'}")
    
    # Test CFI validation
    test_cfis = [
        'ESVUFR',    # Equity - Valid
        'DBXXXX',    # Debt - Valid
        'FXXXXX',    # Future - Valid
        'INVALID',   # Invalid length
        'ZXXXXX',    # Invalid first char
    ]
    
    print("\nCFI Validation:")
    for cfi in test_cfis:
        is_valid = FIRDSClient.validate_cfi(cfi)
        print(f"  {cfi}: {'✓ Valid' if is_valid else '✗ Invalid'}")


def example_enum_usage():
    """Example: Using enums for type-safe classifications."""
    print("=" * 80)
    print("Example 5: Enum Classifications")
    print("=" * 80)
    
    # Asset types
    print("\nAsset Types (ISO 10962 CFI):")
    for asset_type in AssetType:
        print(f"  {asset_type.value}: {asset_type.name}")
    
    # Commodity base products
    print("\nCommodity Base Products:")
    for commodity in CommodityBaseProduct:
        print(f"  {commodity.value}: {commodity.name}")
    
    # Option types
    print("\nOption Types:")
    for opt_type in OptionType:
        print(f"  {opt_type.value}: {opt_type.name}")
    
    # Exercise styles
    print("\nExercise Styles:")
    for style in ExerciseStyle:
        print(f"  {style.value}: {style.name}")
    
    # Delivery types
    print("\nDelivery Types:")
    for delivery in DeliveryType:
        print(f"  {delivery.value}: {delivery.name}")
    
    # Bond seniority
    print("\nBond Seniority:")
    for seniority in BondSeniority:
        print(f"  {seniority.value}: {seniority.name}")


def example_enum_validation():
    """Example: Validate values against enums."""
    print("=" * 80)
    print("Example 6: Enum Validation")
    print("=" * 80)
    
    # Validate asset type
    asset_codes = ['E', 'D', 'F', 'X']  # X is invalid
    
    print("\nValidating Asset Type Codes:")
    for code in asset_codes:
        try:
            asset_type = AssetType(code)
            print(f"  {code}: ✓ Valid ({asset_type.name})")
        except ValueError:
            valid_codes = [t.value for t in AssetType]
            print(f"  {code}: ✗ Invalid (valid codes: {', '.join(valid_codes)})")
    
    # Validate commodity base product
    commodities = ['NRGY', 'METL', 'INVALID']
    
    print("\nValidating Commodity Base Products:")
    for commodity in commodities:
        try:
            base_product = CommodityBaseProduct(commodity)
            print(f"  {commodity}: ✓ Valid ({base_product.name})")
        except ValueError:
            print(f"  {commodity}: ✗ Invalid")


def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("FIRDS Advanced Usage Examples")
    print("Based on RTS 23 Specifications")
    print("=" * 80 + "\n")
    
    try:
        example_file_metadata()
        print("\n")
        
        example_filter_by_file_type()
        print("\n")
        
        example_delta_files()
        print("\n")
        
        example_validation_utilities()
        print("\n")
        
        example_enum_usage()
        print("\n")
        
        example_enum_validation()
        print("\n")
        
        print("=" * 80)
        print("All examples completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
