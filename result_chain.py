import glob
import csv
import jsonlines

# output_name = 'results.csv'
output_name = 'result/results.jl'
file_datasets = 'HTA_noduplicates.json'
chained_name = 'chained_results.csv'

# print('Loading results csv...')
# with open(output_name, 'r', encoding='utf8') as f:
#     results_arr = [{k: v for k, v in row.items()}
#         for row in csv.DictReader(f, skipinitialspace=True)]
print('Loading results jsonlist...')
with jsonlines.open(output_name) as f:
    results_arr = [{k: v for k, v in row.items()}
        for row in f]
print(len(results_arr))

print('Loading datasets...')
with jsonlines.open(file_datasets) as f:
    datasets = []
    for index, data in enumerate(f):
        if (index >= 3000):
            break
        datasets.append({
            'status_id': data['id'],
            'status_data': data['text'],
        })

    for index, row in enumerate(results_arr):
        print('Chaining {} : {}'.format(index, row['status_id']))
        for idx, data in enumerate(datasets):
            # print('-- Found {} : {}'.format(idx, data['status_id']))
            if (idx >= 3000):
                break
            if int(row['status_id']) == int(data['status_id']):
                row['status_data'] = data['status_data']
                break
        if (idx >= 3000):
            break

print('Saving chained result...')
with open(chained_name, 'w', encoding='utf8', newline='') as f:
    output = csv.writer(f)
    output.writerow(results_arr[0].keys())

    for index, row in enumerate(results_arr):
        output.writerow(row.values())