"""
Field Coverage Analysis Tool

Compares reference data from database against raw FIRDS CSV data
to verify all fields are being collected correctly.
"""
import pandas as pd
from pathlib import Path
import esma_dm as edm


def analyze_field_coverage(isin: str, verbose: bool = True):
    """
    Analyze field coverage for an instrument by comparing database
    reference data against raw CSV data.
    
    Args:
        isin: Instrument ISIN to analyze
        verbose: Print detailed output
    
    Returns:
        Dictionary with analysis results
    """
    if verbose:
        print(f"\n{'=' * 80}")
        print(f"Field Coverage Analysis for: {isin}")
        print('=' * 80)
    
    # 1. Get reference data from database
    ref_data = edm.reference(isin)
    
    if ref_data is None:
        print(f"ERROR: ISIN {isin} not found in database")
        return None
    
    if verbose:
        print(f"\n1. Database Reference Data:")
        print(f"   Instrument Type: {ref_data.get('instrument_type')}")
        print(f"   CFI Code: {ref_data.get('cfi_code')}")
        print(f"   Source File: {ref_data.get('source_file')}")
        print(f"   Short Name: {ref_data.get('short_name')}")
    
    # 2. Find and read raw CSV data
    source_file = ref_data.get('source_file')
    if not source_file:
        print("ERROR: No source file information")
        return None
    
    csv_path = Path('downloads/data/firds') / source_file
    
    if not csv_path.exists():
        print(f"ERROR: Source file not found: {csv_path}")
        return None
    
    if verbose:
        print(f"\n2. Reading Raw CSV Data from: {source_file}")
    
    # Read the specific row
    df = pd.read_csv(csv_path, low_memory=False)
    raw_record = df[df['Id'] == isin]
    
    if len(raw_record) == 0:
        print(f"ERROR: ISIN {isin} not found in CSV file")
        return None
    
    raw_record = raw_record.iloc[0]
    
    if verbose:
        print(f"   Found record in CSV with {len(df.columns)} total columns")
        print(f"   CSV has {len(raw_record[raw_record.notna()])} non-null fields for this ISIN")
    
    # 3. Analyze field mapping
    if verbose:
        print(f"\n3. Field Mapping Analysis:")
        print(f"   {'Column Name':<60} {'CSV Value':<30} {'DB Field':<30}")
        print(f"   {'-' * 120}")
    
    # Map common fields
    field_mapping = {
        'Id': 'isin',
        'RefData_FinInstrmGnlAttrbts_FullNm': 'full_name',
        'RefData_FinInstrmGnlAttrbts_ShrtNm': 'short_name',
        'RefData_FinInstrmGnlAttrbts_ClssfctnTp': 'cfi_code',
        'RefData_FinInstrmGnlAttrbts_NtnlCcy': 'currency',
        'RefData_Issr': 'issuer',
        'RefData_TradgVnRltdAttrbts_Id': 'trading_venue_id',
        'RefData_TradgVnRltdAttrbts_FrstTradDt': 'first_trade_date',
        'RefData_TradgVnRltdAttrbts_TermntnDt': 'termination_date',
        'RefData_DerivInstrmAttrbts_XpryDt': 'expiry_date',
        'RefData_DerivInstrmAttrbts_PricMltplr': 'price_multiplier',
        'RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN': 'underlying_isin',
        'RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_LEI': 'underlying_lei',
        'RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN': 'underlying_index_isin',
        'RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm': 'underlying_index_name',
        'RefData_DerivInstrmAttrbts_UndrlygInstrm_Bskt_ISIN': 'underlying_basket_isin',
        'RefData_DerivInstrmAttrbts_DlvryTp': 'delivery_type',
        'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Nm': 'interest_rate_reference_name',
        'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_OthrNtnlCcy': 'fx_other_notional_currency',
    }
    
    mapped_fields = 0
    unmapped_fields = 0
    missing_in_csv = 0
    null_in_csv = 0
    
    # Check all CSV columns
    csv_columns_with_data = []
    for col in df.columns:
        if pd.notna(raw_record[col]) and raw_record[col] != '':
            csv_columns_with_data.append(col)
    
    # Compare mapped fields
    for csv_col, db_field in field_mapping.items():
        if csv_col in raw_record.index:
            csv_value = raw_record[csv_col]
            db_value = ref_data.get(db_field)
            
            # Format values for display
            csv_val_str = str(csv_value)[:28] if pd.notna(csv_value) else 'NULL'
            db_val_str = str(db_value)[:28] if db_value is not None else 'NULL'
            
            # Check if mapped correctly
            if pd.notna(csv_value) and csv_value != '':
                if db_value is not None and str(db_value) != 'nan':
                    status = '✓'
                    mapped_fields += 1
                else:
                    status = '✗ MISSING'
                    unmapped_fields += 1
            else:
                status = '- (null in CSV)'
                null_in_csv += 1
            
            if verbose:
                print(f"   {csv_col:<60} {csv_val_str:<30} {status}")
        else:
            missing_in_csv += 1
    
    # Find unmapped CSV columns with data
    unmapped_csv_cols = []
    for col in csv_columns_with_data:
        if col not in field_mapping:
            unmapped_csv_cols.append(col)
    
    if verbose and unmapped_csv_cols:
        print(f"\n4. CSV Columns with Data NOT in Field Mapping:")
        for col in unmapped_csv_cols[:20]:  # Show first 20
            print(f"   {col}: {str(raw_record[col])[:60]}")
        if len(unmapped_csv_cols) > 20:
            print(f"   ... and {len(unmapped_csv_cols) - 20} more")
    
    # Summary
    if verbose:
        print(f"\n5. Summary:")
        print(f"   Total mapped fields checked: {len(field_mapping)}")
        print(f"   ✓ Successfully mapped: {mapped_fields}")
        print(f"   ✗ Missing in DB (but present in CSV): {unmapped_fields}")
        print(f"   - Null in CSV: {null_in_csv}")
        print(f"   CSV columns with data: {len(csv_columns_with_data)}")
        print(f"   Unmapped CSV columns: {len(unmapped_csv_cols)}")
        
        # Calculate coverage percentage
        if mapped_fields + unmapped_fields > 0:
            coverage = (mapped_fields / (mapped_fields + unmapped_fields)) * 100
            print(f"\n   Field Coverage: {coverage:.1f}%")
    
    return {
        'isin': isin,
        'instrument_type': ref_data.get('instrument_type'),
        'mapped_fields': mapped_fields,
        'unmapped_fields': unmapped_fields,
        'null_in_csv': null_in_csv,
        'csv_columns_with_data': len(csv_columns_with_data),
        'unmapped_csv_columns': unmapped_csv_cols,
        'coverage_percent': (mapped_fields / (mapped_fields + unmapped_fields) * 100) if (mapped_fields + unmapped_fields) > 0 else 0
    }


def test_multiple_types():
    """Test field coverage across different instrument types."""
    
    print("\n" + "=" * 80)
    print("FIELD COVERAGE ANALYSIS - All Instrument Types")
    print("=" * 80)
    
    # Get samples from all asset types
    asset_types = {
        'E': ('equity', 'Equities'),
        'D': ('debt', 'Debt Instruments'),
        'C': ('civ', 'Collective Investment Vehicles'),
        'F': ('futures', 'Futures'),
        'O': ('options', 'Options'),
        'S': ('swap', 'Swaps'),
        'T': ('referential', 'Referential Instruments'),
        'R': ('rights', 'Entitlements/Rights'),
        'I': ('spot', 'Spot'),
        'J': ('forward', 'Forwards'),
    }
    
    test_instruments = {}
    
    print("\nCollecting sample instruments from each asset type...")
    for cfi_char, (attr_name, display_name) in asset_types.items():
        try:
            asset_api = getattr(edm.reference, attr_name)
            count = asset_api.count()
            
            if count > 0:
                # Get one sample
                sample = asset_api.sample(1)
                if not sample.empty:
                    isin = sample.iloc[0]['isin']
                    test_instruments[cfi_char] = isin
                    print(f"  {cfi_char} - {display_name}: {isin} ({count:,} instruments)")
                else:
                    print(f"  {cfi_char} - {display_name}: No data")
            else:
                print(f"  {cfi_char} - {display_name}: No data")
        except Exception as e:
            print(f"  {cfi_char} - {display_name}: Error - {e}")
    
    # Analyze each instrument
    results = {}
    
    for inst_type, isin in test_instruments.items():
        result = analyze_field_coverage(isin, verbose=True)
        if result:
            results[inst_type] = result
    
    # Overall summary
    print("\n" + "=" * 80)
    print("OVERALL SUMMARY BY INSTRUMENT TYPE")
    print("=" * 80)
    print(f"{'Type':<6} {'Name':<30} {'ISIN':<15} {'Mapped':<8} {'Missing':<8} {'Coverage':<10}")
    print("-" * 80)
    
    for inst_type, result in results.items():
        type_name = asset_types[inst_type][1]
        print(f"{inst_type:<6} {type_name:<30} {result['isin']:<15} {result['mapped_fields']:<8} "
              f"{result['unmapped_fields']:<8} {result['coverage_percent']:.1f}%")
    
    # Calculate average coverage
    if results:
        avg_coverage = sum(r['coverage_percent'] for r in results.values()) / len(results)
        total_mapped = sum(r['mapped_fields'] for r in results.values())
        total_missing = sum(r['unmapped_fields'] for r in results.values())
        
        print("-" * 80)
        print(f"{'AVERAGE':<6} {'':<30} {'':<15} {total_mapped:<8} {total_missing:<8} {avg_coverage:.1f}%")
    
    return results


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        # Analyze specific ISIN
        isin = sys.argv[1]
        analyze_field_coverage(isin, verbose=True)
    else:
        # Test multiple types
        test_multiple_types()
