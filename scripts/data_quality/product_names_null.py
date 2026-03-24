import csv

total = 0
empty_rows = []

with open('data/processed/product_names.csv') as f:

    for row in csv.DictReader(f):
        total += 1

        if not row['product_name']:
            empty_rows.append(row)

empty = len(empty_rows)

print(f'Total: {total:,}')
print(f'Empty name: {empty:,} ({empty/total*100:.2f}%)')

print('\nSample empty rows:')

for r in empty_rows[:5]:
    print(r['product_id'], '|', r['url'][:80])
