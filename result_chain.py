import glob
import csv
import jsonlines

output_name = 'results.csv'
file_datasets = 'HTA_noduplicates.json'
chained_name = 'chained_results.csv'

print('Loading results csv...')
with open(output_name, 'r', encoding='utf8') as f:
    results_arr = [{k: v for k, v in row.items()}
        for row in csv.DictReader(f, skipinitialspace=True)]

print('Loading datasets...')
with jsonlines.open(file_datasets) as f:
    for index, row in enumerate(results_arr):
        print('Chaining {}'.format(index))
        for idx, data in enumerate(f):
            if row['status_id'] == data['id']:
                row['status_data'] = data['text']
                break

print('Saving chained result...')
with open(chained_name, 'w', encoding='utf8', newline='') as f:
    output = csv.writer(f)
    output.writerow(results_arr[0].keys())

    for index, row in enumerate(results_arr):
        output.writerow(row.values())