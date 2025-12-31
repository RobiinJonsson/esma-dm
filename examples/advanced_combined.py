"""
Advanced Usage Example - Combining Multiple Data Sources

This example shows how to combine FIRDS and FITRS data to get
a complete view of instruments with both reference data and transparency.
"""
from esma_dm import FIRDSClient, FITRSClient, SSRClient
import pandas as pd


def main():
    print("=" * 60)
    print("ESMA Data Manager - Advanced Combined Example")
    print("=" * 60)
    print()
    
    # Initialize all clients
    print("1. Initializing clients...")
    firds = FIRDSClient(date_from='2024-01-01')
    fitrs = FITRSClient(date_from='2024-01-01')
    ssr = SSRClient()
    print("   ✓ All clients initialized")
    print()
    
    # Define target ISINs
    target_isins = [
        'GB00B1YW4409',  # Example ISIN
    ]
    
    print(f"2. Fetching complete data for {len(target_isins)} ISINs...")
    print()
    
    # Get reference data from FIRDS
    print("   a) Fetching reference data (FIRDS)...")
    reference_data = firds.get_instruments(target_isins, asset_type='E')
    print(f"      ✓ Found {len(reference_data)} reference records")
    
    # Get transparency data from FITRS
    print("   b) Fetching transparency data (FITRS)...")
    transparency_data = fitrs.get_instruments(target_isins, instrument_type='equity')
    print(f"      ✓ Found {len(transparency_data)} transparency records")
    
    # Check SSR status
    print("   c) Checking SSR exemption status...")
    ssr_data = ssr.get_exempted_shares(today_only=True)
    is_exempted = ssr_data[ssr_data['shs_isin'].isin(target_isins)]
    print(f"      ✓ {len(is_exempted)} ISINs are SSR exempted")
    print()
    
    # Combine the data
    print("3. Combining datasets...")
    
    if not reference_data.empty and not transparency_data.empty:
        # Merge on ISIN (Id column)
        combined = pd.merge(
            reference_data,
            transparency_data,
            on='Id',
            how='outer',
            suffixes=('_ref', '_trans')
        )
        
        # Add SSR status
        combined['is_ssr_exempted'] = combined['Id'].isin(is_exempted['shs_isin'])
        
        print(f"   ✓ Combined dataset has {len(combined)} records")
        print(f"   ✓ Combined dataset has {len(combined.columns)} columns")
        print()
        
        # Display key information
        print("4. Key Information Summary:")
        print("=" * 60)
        
        for _, row in combined.iterrows():
            print(f"\nISIN: {row['Id']}")
            
            # Reference data
            if 'FullNm' in row and pd.notna(row['FullNm']):
                print(f"  Name: {row['FullNm']}")
            
            # Transparency metrics
            if 'AvrgDalyTrnvr' in row and pd.notna(row['AvrgDalyTrnvr']):
                print(f"  Avg Daily Turnover: {row['AvrgDalyTrnvr']}")
            if 'AvrgDalyNbOfTxs' in row and pd.notna(row['AvrgDalyNbOfTxs']):
                print(f"  Avg Daily Transactions: {row['AvrgDalyNbOfTxs']}")
            
            # SSR status
            print(f"  SSR Exempted: {row['is_ssr_exempted']}")
        
        print("\n" + "=" * 60)
        
        # Save combined data
        output_file = "combined_esma_data.csv"
        combined.to_csv(output_file, index=False)
        print(f"\n5. Combined data saved to: {output_file}")
    else:
        print("   ℹ Not enough data to combine")
    
    print()
    print("=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
