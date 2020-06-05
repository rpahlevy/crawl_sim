import csv
import sys
import html
import jsonlines

import spacy
print('Load en_core_web_lg')
nlp = spacy.load("en_core_web_lg")

import re

from operator import itemgetter

def process_text(text):
    text = html.unescape(text)
    text = re.sub('[^a-zA-Z?:@[w_]+http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+(?:(?:\d+,?)+(?:\.?\d+)?)]', 
                ' ', text)
    doc = nlp(text)
    result = []
    for token in doc:
        if token.text in nlp.Defaults.stop_words:
            continue
    #     if token.is_punct:
    #         continue
    #     if token.lemma == '-PRON-':
    #         continue
    #     if 'rt' == token.lemma_:
    #         continue
    #     if '@' in token.lemma_:
    #         continue
        if 'http' not in token.text:
            result.append(token.text.lower())
        else:
            result.append(token.text)
            # .replace('#', '')
            # .replace('w/', 'with'))
    result = nlp(" ".join(result).replace('# ', '#').replace('& amp;', '&'))
    return result

def append_processed(processed, with_header):
    mode = 'w' if with_header else 'a'
    with open('source/preprocessed.csv', mode, encoding='utf8', newline='') as f:
        output = csv.writer(f)
        if with_header:
            output.writerow(processed[0].keys())
        for row in processed:
            output.writerow(row.values())
        # print('-- added {} data'.format(len(processed)))

# file_datasets = 'source/datasets'
file_datasets = '/content/HTA_noduplicates.json'
total = 100
if len(sys.argv) >= 2:
    total = int(sys.argv[1])

print('Get last row')
try:
    start_row = sum(1 for line in open('source/preprocessed.csv')) - 1
except:
    start_row = 0

print('Load datasets')
datasets = []
processed = []
with jsonlines.open(file_datasets) as f:
    # for index, data in enumerate(f):
    #     datasets.append(data)
    #     datasets = sorted(datasets, key=itemgetter('retweet_count'), reverse=True)
    #     if len(datasets) > total:
    #         datasets.pop()
    #     print('read {} data => {}'.format(index + 1, len(datasets)))

    for index, data in enumerate(f):
        if index < start_row:
            continue

        if index >= total:
            break

        processed.append({
            'status_id': data['id'],
            'status_data': process_text(data['text']),
            'status_timestamp': data['topsy']['timestamp'],
        })

        if index % 1000 == 999:
            print('read {} data'.format(index + 1))
            append_processed(processed, index == 999)
            processed = []


if len(processed) > 1:
    print('processed {} data'.format(index))
    append_processed(processed, index <= 999)


sys.exit('Preprocessed : %d data' % index)