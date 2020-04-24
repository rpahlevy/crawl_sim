import csv

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

import spacy
print('Load en_core_web_lg')
nlp = spacy.load("en_core_web_lg")

from datetime import datetime
import json

import sys

class SingleSpider(CrawlSpider):
    name = "single"

    # file_websites = 'source/websites.txt'
    # ubah dengan datasets-50 / datasets-100 / datasets / preprocessed
    file_datasets = 'source/preprocessed.csv'
    file_sim_cache = 'cache/similarity.csv'

    start_urls = []
    allowed_domains = []

    # print('Load website list')
    # load start_urls dari file websites
    # with open(file_websites, 'r') as f:
    #     start_urls = f.read().strip().split('\n')

    # parse start_urls untuk mendapatkan domain -> masukkan ke allowed_domains
    for arg in sys.argv:
        if ('=' not in arg):
            continue

        arg = arg.split('=')
        if arg[0] == 'website':
            urls = arg[1].split(';')
            for url in urls:
                url = url.strip()
                start_urls.append(url)
                domain = url.split('/')[2]#.replace('www.', '')
                allowed_domains.append(domain)

    if len(start_urls) == 0:
        sys.exit('Masukkan URL dengan perintah -a website=, contoh "-a website=https://who.int/"')

    # print(allowed_domains)
    # sys.exit()
    # variabel untuk menyimpan cache nlp
    cache_nlp = {}

    # variabel untuk menyimpan cache similarity
    print('Load cache similarity')
    cache_sim = {}
    try:
        s = open(file_sim_cache, 'r')

        sim = s.read().strip().split('\n')
        for row in sim:
            sim_arr = row.split(';')
            if len(sim_arr) < 3:
                continue

            if sim_arr[0] not in cache_sim:
                cache_sim[sim_arr[0]] = {}
            cache_sim[sim_arr[0]][sim_arr[1]] = sim_arr[2]

        s.close()
    except FileNotFoundError:
        print('No cache similarity')

    # threshold untuk menentukan tweet kredibel / tidak
    trust_threshold = 0.90

    # load datasets
    print('Load datasets')
    datasets = []
    with open(file_datasets, 'r', encoding='utf8') as f:
        datasets = [{k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)]

    # penanda untuk simpan setiap x waktu
    last_save = datetime.now()

    # simpan setiap x, dalam satuan detik
    save_each = 600

    rules = [Rule(LinkExtractor(allow='/'), callback='parse_url', follow=True)]

    def process_text(self, text, cache):
        key = text
        if cache:
            if key in self.cache_nlp:
                return self.cache_nlp[key]
        
        doc = nlp(text.lower())
        result = []
        for token in doc:
            if token.text in nlp.Defaults.stop_words:
                continue
            if token.is_punct:
                continue
            if token.lemma_ == '-PRON-':
                continue
            result.append(token.lemma_)

        result = nlp(" ".join(result))

        # cache result
        if cache:
            self.cache_nlp[key] = result

        return result

    def parse_url(self, response):
        url = response.url

        # get domain
        domain = url.split('/')[2]#.replace('www.', '')

        # skip if not in allowed_domains
        if domain not in self.allowed_domains:
            return

        # get content
        body = response.xpath('normalize-space(//body)').extract_first()

        if body is None:
            return

        body = self.process_text(body, False)
        if (body.vector_norm):
            for dataset in self.datasets:
                # cek apakah sudah mencapai similarity yg diinginkan
                if 'similarity' in dataset and dataset['similarity'] >= self.trust_threshold:
                    continue

                word = dataset['status_data']

                if word in self.cache_sim and url in self.cache_sim[word]:
                    similarity = float(self.cache_sim[word][url])
                else:
                    # karena sudah di preprocess lsg panggil nlp
                    word = nlp(word)
                    if (word.vector_norm):
                        similarity = word.similarity(body)
                    else:
                        similarity = 0
                    
                    # simpan ke cache program
                    if word not in self.cache_sim:
                        self.cache_sim[word] = {}
                    self.cache_sim[word][url] = similarity

                    # simpan ke cache file
                    with open('cache/similarity.csv', 'a') as f:
                        f.write(f'{word};{url};{similarity}\n')

                if 'result' not in dataset:
                    dataset['result'] = {}

                if 'similarity' not in dataset:
                    dataset['similarity'] = 0
                    dataset['source_url'] = ''

                if domain not in dataset['result']:
                    dataset['result'][domain] = {
                        'similarity': 0,
                        'source_url': ''
                    }

                max_sim = dataset['result'][domain]['similarity']
                max_sim_all = dataset['similarity']

                if similarity > max_sim:
                    dataset['result'][domain]['similarity'] = similarity
                    dataset['result'][domain]['source_url'] = url

                if similarity > max_sim_all:
                    dataset['similarity'] = similarity
                    dataset['source_url'] = url

        # cek apakah sudah waktunya dump
        now = datetime.now()
        diff = now - self.last_save
        if (diff.seconds > self.save_each):
            self.last_save = now
            self.dump()

    def dump(self):
        results = {}
        for dataset in self.datasets:
            for domain in dataset['result']:
                if domain not in results:
                    results[domain] = []

                results[domain].append({
                    'status_id': dataset['status_id'],
                    'status_data': dataset['status_data'],
                    'status_timestamp': dataset['status_timestamp'],
                    'similarity': dataset['result'][domain]['similarity'],
                    'source_url': dataset['result'][domain]['source_url'],
                })

        for domain in results:
            file_result = 'result/%s.csv' % domain
            result = results[domain]
            with open(file_result, 'w', encoding='utf8', newline='') as f:
                output = csv.writer(f)
                output.writerow(result[0].keys())
                for row in result:
                    output.writerow(row.values())

    def closed(self, reason):
        self.dump()