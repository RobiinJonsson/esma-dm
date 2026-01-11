"""
Example: CFI Classification of Instruments

Demonstrates how to use the CFI (Classification of Financial Instruments) 
module to decode and classify instruments according to ISO 10962 standard.
"""

from pathlib import Path
from esma_dm.storage.duckdb_store import DuckDBStorage
from esma_dm.models.utils import CFI, Category

def demonstrate_cfi_classification():
    """Demonstrate CFI classification features."""
    
    # Initialize storage
    cache_dir = Path('downloads')
    db_path = str(cache_dir / 'data' / 'firds' / 'firds_complete.db')
    storage = DuckDBStorage(cache_dir, db_path)
    
    # Check if database has data
    try:
        count = storage.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        if count == 0:
            print("\nDatabase is empty. Please run:")
            print("  1. python examples/00_initialize_database.py")
            print("  2. Download and index data")
            return
    except:
        print("\nDatabase not initialized. Please run:")
        print("  python examples/00_initialize_database.py")
        return
    
    print("="*80)
    print("CFI CLASSIFICATION DEMONSTRATION")
    print("="*80)
    print()
    
    # 1. Get sample instruments from each category
    print("Sample Instruments by CFI Category:")
    print("-"*80)
    
    categories = {
        'E': 'Equities',
        'D': 'Debt',
        'C': 'Collective Investment Vehicles',
        'F': 'Futures',
        'O': 'Options',
        'S': 'Swaps',
        'H': 'Non-standardized Derivatives',
        'R': 'Entitlements (Rights)',
        'I': 'Spot',
        'J': 'Forwards'
    }
    
    for category_code, category_name in categories.items():
        # Get one instrument from this category
        results = storage.con.execute(f"""
            SELECT isin, full_name, cfi_code
            FROM instruments
            WHERE cfi_code LIKE '{category_code}%'
            LIMIT 1
        """).fetchall()
        
        if results:
            isin, name, cfi_code = results[0]
            print(f"\n{category_name} ({category_code}):")
            print(f"  ISIN: {isin}")
            print(f"  Name: {name[:60]}...")
            print(f"  CFI: {cfi_code}")
            
            # Decode the CFI
            try:
                cfi = CFI(cfi_code)
                print(f"  Category: {cfi.category_description}")
                print(f"  Group: {cfi.group_description}")
                
                # Show attribute details
                attrs = cfi.describe()
                if 'decoded_attributes' in attrs:
                    print(f"  Attributes:")
                    for key, value in list(attrs['decoded_attributes'].items())[:3]:
                        print(f"    - {key}: {value}")
            except Exception as e:
                print(f"  Error decoding: {e}")
    
    print("\n" + "="*80)
    print("Detailed Classification Examples")
    print("="*80)
    
    # 2. Show detailed classification for specific instruments
    test_isins = []
    
    # Get one ISIN from each major category
    for cat in ['E', 'D', 'F', 'O']:
        result = storage.con.execute(f"""
            SELECT isin FROM instruments 
            WHERE cfi_code LIKE '{cat}%' 
            LIMIT 1
        """).fetchone()
        if result:
            test_isins.append(result[0])
    
    for isin in test_isins:
        print(f"\n{'-'*80}")
        classification = storage.classify_instrument(isin)
        
        if classification:
            print(f"ISIN: {classification['isin']}")
            print(f"Name: {classification['name'][:60]}...")
            print(f"CFI Code: {classification['cfi_code']}")
            print(f"Instrument Type: {classification['instrument_type']}")
            
            if isinstance(classification.get('classification'), dict):
                cls = classification['classification']
                print(f"\nFull Classification:")
                print(f"  Category: {cls.get('category_description')}")
                print(f"  Group: {cls.get('group_description')}")
                
                if 'decoded_attributes' in cls:
                    print(f"  Decoded Attributes:")
                    for key, value in cls['decoded_attributes'].items():
                        print(f"    - {key}: {value}")
    
    # 3. Search and classify
    print("\n" + "="*80)
    print("Search with CFI Classification")
    print("="*80)
    
    search_results = storage.search_instruments("BOND", limit=5)
    print(f"\nSearch results for 'BOND':")
    for inst in search_results:
        print(f"\n  {inst['isin']} - {inst['full_name'][:50]}...")
        print(f"  CFI: {inst.get('cfi_code')}")
        if 'category_description' in inst:
            print(f"  Category: {inst['category_description']}")
            print(f"  Group: {inst['group_description']}")
    
    # 4. Get instruments by CFI category
    print("\n" + "="*80)
    print("Instruments by CFI Category: Equities (E)")
    print("="*80)
    
    equities = storage.get_instruments_by_cfi_category('E', limit=10)
    print(f"\nFound {len(equities)} equities:")
    for eq in equities[:5]:  # Show first 5
        print(f"\n  {eq['isin']}")
        print(f"  Name: {eq['name'][:50]}...")
        print(f"  CFI: {eq['cfi_code']}")
        print(f"  Category: {eq['category']}")
        print(f"  Group: {eq['group']}")
    
    # 5. CFI Statistics
    print("\n" + "="*80)
    print("CFI Category Statistics")
    print("="*80)
    
    stats = storage.con.execute("""
        SELECT 
            SUBSTRING(cfi_code, 1, 1) as category,
            COUNT(*) as count
        FROM instruments
        WHERE cfi_code IS NOT NULL
        GROUP BY category
        ORDER BY count DESC
    """).fetchall()
    
    print(f"\n{'Category':<15} {'Code':<10} {'Count':>15}")
    print("-"*40)
    for cat_code, count in stats:
        try:
            cat = Category(cat_code)
            cat_name = categories.get(cat_code, 'Unknown')
            print(f"{cat_name:<15} {cat_code:<10} {count:>15,}")
        except:
            print(f"{'Unknown':<15} {cat_code:<10} {count:>15,}")
    
    storage.close()
    print("\n" + "="*80)
    print("Classification demonstration complete!")
    print("="*80)

if __name__ == '__main__':
    demonstrate_cfi_classification()
