import csv
total, null = 0, 0

with open('data/exports/ip_locations.csv') as f:
    for row in csv.DictReader(f):
        total += 1
        if not row['country']:
            null += 1

print(f'Total: {total:,}')
print(f'Null country: {null:,} ({null/total*100:.2f}%)')
