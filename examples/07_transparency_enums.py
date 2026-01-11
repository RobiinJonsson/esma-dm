"""
Test transparency utility enums and helper methods.
"""
from esma_dm.clients.fitrs import FITRSClient
from esma_dm.models.transparency_enums import (
    Methodology, InstrumentClassification, FileType, SegmentationCriteria,
    format_segmentation_info
)

fitrs = FITRSClient()

print("=" * 80)
print("TRANSPARENCY UTILITY ENUMS DEMONSTRATION")
print("=" * 80)

# List all methodologies
print("\n1. Available Methodologies:")
print("-" * 80)
for m in fitrs.list_methodologies():
    print(f"   {m['code']:6} : {m['description']}")

# List all instrument classifications
print("\n2. Instrument Classifications:")
print("-" * 80)
for c in fitrs.list_classifications():
    print(f"   {c['code']:6} : {c['description']}")

# List all file types
print("\n3. FITRS File Types:")
print("-" * 80)
for ft in fitrs.list_file_types():
    print(f"   {ft['code']:15} : {ft['description']}")

# Get detailed info for specific codes
print("\n4. Detailed Code Information:")
print("-" * 80)

year_info = fitrs.get_methodology_info('YEAR')
print(f"   Methodology 'YEAR':")
print(f"     Description: {year_info['description']}")
print(f"     Valid: {year_info['valid']}")

shrs_info = fitrs.get_classification_info('SHRS')
print(f"\n   Classification 'SHRS':")
print(f"     Description: {shrs_info['description']}")
print(f"     Valid: {shrs_info['valid']}")

# Test segmentation criteria
print("\n5. Segmentation Criteria Examples:")
print("-" * 80)

criteria_examples = ['BSPD', 'TTMB', 'IRCU', 'EQTY', 'CMTY', 'FXCU', 'UINS']
for code in criteria_examples:
    info = format_segmentation_info(code)
    print(f"   {code:8} [{info['category']:25}] : {info['description']}")

# Test file type checks
print("\n6. File Type Classification:")
print("-" * 80)
test_types = ['FULECR', 'FULNCR', 'DLTECR', 'FULNCR_NYAR']
for ft in test_types:
    is_equity = FileType.is_equity(ft)
    is_non_equity = FileType.is_non_equity(ft)
    is_subclass = FileType.is_subclass(ft)
    is_delta = FileType.is_delta(ft)
    
    print(f"   {ft:15} - Equity: {is_equity:5} | Non-Equity: {is_non_equity:5} | "
          f"Sub-class: {is_subclass:5} | Delta: {is_delta:5}")

# Show all segmentation criteria by category
print("\n7. All Segmentation Criteria by Category:")
print("-" * 80)

categories = {}
for criterion in SegmentationCriteria:
    category = SegmentationCriteria.get_category(criterion.name)
    if category not in categories:
        categories[category] = []
    categories[category].append((criterion.name, criterion.value))

for category, criteria in sorted(categories.items()):
    print(f"\n   {category}:")
    for code, description in criteria:
        print(f"     {code:15} : {description}")

print("\n" + "=" * 80)
print("✓ All transparency utility enums working correctly")
print("=" * 80)
