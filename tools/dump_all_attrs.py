"""Dump all attribute values for all categories from ISO 10962 JSON."""
import json

with open('docs/ISO 10962_CFI.json', encoding='utf-8') as f:
    data = json.load(f)

cat_map = {c['code']: c for c in data['categories']}


def dump_group(cat_code, group_code=None):
    cat = cat_map[cat_code]
    print(f'=== CATEGORY {cat_code}: {cat["name"]} ===')
    for g in cat.get('groups', []):
        if group_code and g['code'] != group_code:
            continue
        print(f'  GROUP {g["code"]}: {g["name"]}')
        for i in range(1, 5):
            attr = g.get(f'attribute{i}', {})
            if not attr:
                continue
            attr_name = attr.get('attributeName', '')
            values = attr.get('attributeValues', [])
            print(f'    attr{i}: {attr_name}')
            for v in values:
                desc = v.get('description', '').replace('\n', ' ').strip()[:100]
                print(f'      {v["code"]}: {desc}')
        print()


for cat_code in ('E', 'D', 'C', 'R', 'O', 'F', 'S', 'H', 'I', 'J', 'K', 'L', 'T', 'M'):
    dump_group(cat_code)
    print()
