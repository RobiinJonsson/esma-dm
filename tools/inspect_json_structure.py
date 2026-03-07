"""Inspect raw JSON structure for attributes."""
import json, pprint

with open('docs/ISO 10962_CFI.json', encoding='utf-8') as f:
    data = json.load(f)

cat_map = {c['code']: c for c in data['categories']}

# Check R category structure
r = cat_map['R']
print('Top-level keys:', list(r.keys()))
print()
for g in r.get('groups', []):
    print(f'Group {g["code"]} keys:', list(g.keys()))
    print(pprint.pformat(g, width=120)[:600])
    print()
    break  # just the first group

# Check L category (financing — has real attrs per the dump)
print('=== L category ===')
l = cat_map['L']
for g in l.get('groups', []):
    print(f'Group {g["code"]} keys:', list(g.keys()))
    print(pprint.pformat(g, width=120)[:1000])
    print()
    break
