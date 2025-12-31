"""
Basic FITRS Usage Example

This example demonstrates how to use the FITRSClient to retrieve
transparency data from ESMA.
"""
from esma_dm import FITRSClient


def main():
    print("=" * 60)
    print("ESMA Data Manager - FITRS Example")
    print("=" * 60)
    print()
    
    # Initialize FITRS client
    print("1. Initializing FITRS client...")
    fitrs = FITRSClient(date_from='2024-01-01')
    print("   ✓ Client initialized")
    print()
    
    # Get list of available files
    print("2. Fetching available transparency files...")
    files = fitrs.get_file_list()
    print(f"   ✓ Found {len(files)} files")
    
    # Show equity vs non-equity breakdown
    equity_files = files[files['instrument_type'] == 'Equity Instruments']
    non_equity_files = files[files['instrument_type'] == 'Non-Equity Instruments']
    print(f"   - Equity files: {len(equity_files)}")
    print(f"   - Non-Equity files: {len(non_equity_files)}")
    print()
    
    # Get latest equity transparency data
    print("3. Downloading latest equity transparency data...")
    print("   (This may take a few minutes on first run)")
    eq_transparency = fitrs.get_latest_full_files(
        asset_type='E',
        instrument_type='equity'
    )
    print(f"   ✓ Retrieved {len(eq_transparency)} transparency records")
    
    if not eq_transparency.empty:
        print(f"   Columns: {', '.join(eq_transparency.columns[:10])}...")
        print(f"   Sample data:\n{eq_transparency.head()}")
    print()
    
    # Get DVCAP data
    print("4. Fetching DVCAP (Double Volume Cap) data...")
    try:
        dvcap = fitrs.get_dvcap_latest()
        print(f"   ✓ Retrieved {len(dvcap)} DVCAP records")
        if not dvcap.empty:
            print(f"   Sample:\n{dvcap.head()}")
    except Exception as e:
        print(f"   ℹ Could not fetch DVCAP: {e}")
    print()
    
    # Get transparency for specific ISINs
    print("5. Fetching transparency for specific ISINs...")
    target_isins = ['GB00B1YW4409']  # Sage Group
    
    transparency = fitrs.get_instruments(target_isins, instrument_type='equity')
    
    if not transparency.empty:
        print(f"   ✓ Found {len(transparency)} transparency records")
        print(f"   Details:\n{transparency.to_string()}")
    else:
        print("   ℹ No transparency records found")
    print()
    
    print("=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
