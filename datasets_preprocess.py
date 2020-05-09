import csv
import sys
import jsonlines

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

def append_processed(processed, with_header):
    mode = 'w' if with_header else 'a'
    with open('source/preprocessed.csv', mode, encoding='utf8', newline='') as f:
        output = csv.writer(f)
        if with_header:
            output.writerow(processed[0].keys())
        for row in processed:
            output.writerow(row.values())
        print('-- added {} data'.format(len(processed)))

# file_datasets = 'source/datasets'
file_datasets = 'HTA_noduplicates.json'
total = 100
if len(sys.argv) >= 2:
    total = int(sys.argv[1])

print('Load datasets')
processed = []
with jsonlines.open(file_datasets) as f:
    for index, data in enumerate(f):
        if index >= total:
            break

        processed.append({
            'status_id': data['id'],
            'status_data': process_text(data['text']),
            'status_timestamp': data['topsy']['timestamp'],
        })

        if index % 1000 == 999:
            print('processed {} data'.format(index + 1))
            append_processed(processed, index == 999)
            processed = []


if len(processed) > 1:
    print('processed {} data'.format(index))
    append_processed(processed, False)


sys.exit('Preprocessed : %d data' % index)