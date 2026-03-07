"""Dump attribute values for R/I/J/K/L/T/M categories from ISO 10962 JSON."""
import json

with open('docs/ISO 10962_CFI.json', encoding='utf-8') as f:
    data = json.load(f)

cat_map = {c['code']: c for c in data['categories']}

for cat_code in ('R', 'I', 'J', 'K', 'L', 'T', 'M'):
    cat = cat_map[cat_code]
    print(f'=== CATEGORY {cat_code}: {cat["name"]} ===')
    for g in cat.get('groups', []):
        print(f'  GROUP {g["code"]}: {g["name"]}')
        for i, a in enumerate(g.get('attributes', []), 1):
            print(f'    attr{i}: {a["name"]}')
            for v in a.get('values', []):
                desc = v.get('description', '').replace('\n', ' ').strip()[:100]
                print(f'      {v["code"]}: {desc}')
        print()
    print()
