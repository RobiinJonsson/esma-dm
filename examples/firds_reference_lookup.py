"""
Simple example demonstrating the reference() method for single ISIN lookup.
"""

from esma_dm import FIRDSClient


def main():
    """Demonstrate single ISIN reference data lookup."""
    
    # Initialize client
    firds = FIRDSClient()
    
    # Example ISINs
    test_isins = [
        'US0378331005',  # Apple Inc
        'GB00B1YW4409',  # Sage Group
        'INVALID123',     # Invalid ISIN
    ]
    
    print("FIRDS Reference Data Lookup")
    print("=" * 60)
    
    for isin in test_isins:
        print(f"\nLooking up: {isin}")
        print("-" * 60)
        
        try:
            # Get reference data
            ref = firds.reference(isin)
            
            if ref is not None:
                # Display key fields
                print(f"ISIN: {ref.get('Id', 'N/A')}")
                print(f"Full Name: {ref.get('FullNm', 'N/A')}")
                print(f"CFI Code: {ref.get('ClssfctnTp', 'N/A')}")
                print(f"Currency: {ref.get('NtnlCcy', 'N/A')}")
                
                # Additional fields if available
                if 'CmmdtyDerivInd' in ref:
                    print(f"Commodity Derivative: {ref['CmmdtyDerivInd']}")
                
                if 'TradgVnRltdAttrbts_Id' in ref:
                    print(f"Trading Venue: {ref['TradgVnRltdAttrbts_Id']}")
            else:
                print("No reference data found")
                
        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
