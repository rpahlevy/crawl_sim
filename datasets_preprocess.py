import csv
import sys
import json

import spacy
print('Load en_core_web_lg')
nlp = spacy.load("en_core_web_lg")

def process_text(text):                                                 
    doc = nlp(text.lower())
    result = []
    for token in doc:
        if token.text in nlp.Defaults.stop_words:
            continue
        if token.is_punct:
            continue
        if token.lemma == '-PRON-':
            continue
        if 'rt' == token.lemma_:
            continue
        if '@' in token.lemma_:
            continue
        # if 'http' in token.lemma_:
        #     continue
        result.append(token.lemma_
            # .replace('#', '')
            .replace('w/', 'with'))
    result = nlp(" ".join(result))
    return result

file_datasets = 'source/datasets'
if len(sys.argv) >= 2:
    total = sys.argv[1]
    if total != '50' and total != '100':
        sys.exit('Use 50, 100 or empty to use ALL : %s' % total)

    file_datasets += ('-' + total)

file_datasets += '.csv'

print('Load datasets')
datasets = []
with open(file_datasets, 'r', encoding='utf8') as f:
    datasets = [{k: v for k, v in row.items()}
        for row in csv.DictReader(f, skipinitialspace=True)]

processed = []
for data in datasets:
    processed.append({
        'status_id': data['status_id'],
        'status_data': process_text(data['status_data']),
        'status_timestamp': data['status_timestamp'],
    })

if len(processed) > 1:
    with open('source/preprocessed.csv', 'w', encoding='utf8', newline='') as f:
        output = csv.writer(f)
        output.writerow(processed[0].keys())
        for row in processed:
            output.writerow(row.values())


sys.exit('Preprocessed : %d data' % len(datasets))