import json

with open('docs/ISO 10962_CFI.json', encoding='utf-8') as f:
    data = json.load(f)

for cat in data['categories']:
    print(f"CATEGORY {cat['code']}: {cat['name']}")
    for g in cat.get('groups', []):
        print(f"  {g['code']}: {g['name']}")
        for i in range(1, 5):
            attr = g.get(f'attribute{i}')
            if attr:
                print(f"    attr{i}: {attr['attributeName']}")
                for v in attr.get('attributeValues', []):
                    print(f"      {v['code']}: {v['description']}")
