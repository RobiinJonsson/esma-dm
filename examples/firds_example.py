"""
Basic FIRDS Usage Example

This example demonstrates how to use the FIRDSClient to retrieve
financial instrument reference data from ESMA.
"""
from esma_dm import FIRDSClient
import pandas as pd


def main():
    print("=" * 60)
    print("ESMA Data Manager - FIRDS Example")
    print("=" * 60)
    print()
    
    # Initialize FIRDS client
    print("1. Initializing FIRDS client...")
    firds = FIRDSClient(date_from='2024-01-01')
    print("   ✓ Client initialized")
    print()
    
    # Get list of available files
    print("2. Fetching available files...")
    files = firds.get_file_list()
    print(f"   ✓ Found {len(files)} files")
    print(f"   Latest files:\n{files[['file_name', 'publication_date']].head()}")
    print()
    
    # Get latest equity instruments
    print("3. Downloading latest equity reference data...")
    print("   (This may take a few minutes on first run)")
    equities = firds.get_latest_full_files(asset_type='E')
    print(f"   ✓ Retrieved {len(equities)} equity instruments")
    
    if not equities.empty:
        print(f"   Columns: {', '.join(equities.columns[:10])}...")
        print(f"   Sample data:\n{equities.head()}")
    print()
    
    # Get specific instruments by ISIN
    print("4. Fetching specific instruments by ISIN...")
    target_isins = [
        'GB00B1YW4409',  # Sage Group
        'US0378331005',  # Apple Inc (if available in EU dataset)
    ]
    
    instruments = firds.get_instruments(target_isins, asset_type='E')
    
    if not instruments.empty:
        print(f"   ✓ Found {len(instruments)} instruments")
        print(f"   Details:\n{instruments[['Id', 'FullNm']].to_string()}")
    else:
        print("   ℹ No instruments found (they may not be in latest dataset)")
    print()
    
    # Try different asset types
    print("5. Exploring different asset types...")
    asset_types = ['E', 'D', 'C']
    for at in asset_types:
        try:
            df = firds.get_latest_full_files(asset_type=at)
            print(f"   {at}: {len(df)} instruments")
        except Exception as e:
            print(f"   {at}: Error - {e}")
    print()
    
    print("=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
