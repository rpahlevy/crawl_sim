import sys
import glob
import csv

randomize = False
limit = 0
output_name = 'results.csv'

for arg in sys.argv:
    if '--random' == arg:
        randomize = True
        continue

    if '--limit' in arg:
        limit = int(arg.replace('--limit=', ''))
        continue

    if '--output' in arg:
        output_name = arg.replace('--output=', '')

results = {}
csv_counter = 0
print('Loading results csv...')
for result_csv in glob.glob('result/*.csv'):
    with open(result_csv, 'r', encoding='utf8') as f:
        csv_counter += 1

        rows = [{k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)]

        for row in rows:
            id = row['status_id']
            if id not in results:
                results[id] = row
                continue

            prev = results[id]
            if row['similarity'] > prev['similarity']:
                results[id]['similarity'] = row['similarity']
                results[id]['source_url'] = row['source_url']

print('Loaded {} results csv.'.format(csv_counter))

results_arr = []
for id in results:
    results_arr.append(results[id])
print('With {} data.'.format(len(results_arr)))

if randomize:
    print('Using randomize')
    import random
    random.shuffle(results_arr)

if limit <= 0:
    print('Limit unspecified, saving all results')
    limit = len(results_arr)

if len(results_arr) > 1:
    with open(output_name, 'w', encoding='utf8', newline='') as f:
        output = csv.writer(f)
        output.writerow(results_arr[0].keys())

        for index, row in enumerate(results_arr):
            if index >= limit:
                break
            output.writerow(row.values())

print('Done processing {} result to {}'.format(limit, output_name))