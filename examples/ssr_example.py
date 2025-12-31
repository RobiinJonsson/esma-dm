"""
SSR (Short Selling Regulation) Example

This example demonstrates how to use the SSRClient to retrieve
exempted shares data.
"""
from esma_dm import SSRClient


def main():
    print("=" * 60)
    print("ESMA Data Manager - SSR Example")
    print("=" * 60)
    print()
    
    # Initialize SSR client
    print("1. Initializing SSR client...")
    ssr = SSRClient()
    print("   ✓ Client initialized")
    print()
    
    # Get currently active exemptions
    print("2. Fetching currently active SSR exemptions...")
    active_exemptions = ssr.get_exempted_shares(today_only=True)
    print(f"   ✓ Found {len(active_exemptions)} active exemptions")
    
    if not active_exemptions.empty:
        # Show breakdown by country
        country_counts = active_exemptions['shs_countryCode'].value_counts()
        print(f"\n   Exemptions by country:")
        for country, count in country_counts.head(10).items():
            print(f"   - {country}: {count}")
        
        print(f"\n   Sample data:\n{active_exemptions.head()}")
    print()
    
    # Get exemptions for specific country
    print("3. Fetching UK exemptions...")
    uk_exemptions = ssr.get_exempted_shares_by_country('GB')
    print(f"   ✓ Found {len(uk_exemptions)} UK exemptions")
    
    if not uk_exemptions.empty:
        print(f"   Sample UK ISINs:")
        for isin in uk_exemptions['shs_isin'].head(5):
            print(f"   - {isin}")
    print()
    
    # Get all exemptions (including expired)
    print("4. Fetching all exemptions (including expired)...")
    all_exemptions = ssr.get_exempted_shares(today_only=False)
    print(f"   ✓ Found {len(all_exemptions)} total exemptions")
    print(f"   Active: {len(active_exemptions)}")
    print(f"   Expired/Inactive: {len(all_exemptions) - len(active_exemptions)}")
    print()
    
    # Try multiple countries
    print("5. Comparing exemptions across countries...")
    countries = ['GB', 'DE', 'FR', 'IT', 'ES']
    
    for country in countries:
        try:
            df = ssr.get_exempted_shares_by_country(country)
            print(f"   {country}: {len(df)} exemptions")
        except Exception as e:
            print(f"   {country}: Error - {e}")
    print()
    
    print("=" * 60)
    print("Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
