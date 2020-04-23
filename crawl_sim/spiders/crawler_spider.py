import csv

import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

import spacy
print('Load en_core_web_lg')
nlp = spacy.load("en_core_web_lg")

from datetime import datetime
import json

class CrawlerSpider(CrawlSpider):
    name = "crawler"

    file_websites = 'source/websites.txt'
    # ubah dengan datasets-50 / datasets-100 / datasets
    file_datasets = 'source/datasets-50.csv'
    file_sim_cache = 'cache/similarity.csv'

    start_urls = []
    allowed_domains = []

    print('Load website list')
    # load start_urls dari file websites
    with open(file_websites, 'r') as f:
        start_urls = f.read().strip().split('\n')

    # parse start_urls untuk mendapatkan domain -> masukkan ke allowed_domains
    for website in start_urls:
        domain = website.split('/')[2]#.replace('www.', '')
        allowed_domains.append(domain)

    # variabel untuk menyimpan cache nlp
    cache_nlp = {}

    # variabel untuk menyimpan cache similarity
    cache_sim = {}
    
    # threshold untuk menentukan tweet kredibel / tidak
    trust_threshold = 0.90
    
    # datasets
    datasets = []
    
    # penanda untuk simpan setiap x waktu
    last_save = datetime.now()

    # simpan setiap x, dalam satuan detik
    save_each = 600

    rules = [Rule(LinkExtractor(allow='/'), callback='parse_url', follow=True)]

    def start_requests(self):
        # self.log('Load cache similarity')
        # try:
        #     s = open(self.file_sim_cache, 'r')

        #     sim = s.read().strip().split('\n')
        #     for row in sim:
        #         sim_arr = row.split(';')
        #         if len(sim_arr) < 3:
        #             continue

        #         if sim_arr[0] not in self.cache_sim:
        #             self.cache_sim[sim_arr[0]] = {}
        #         self.cache_sim[sim_arr[0]][sim_arr[1]] = sim_arr[2]

        #     s.close()
        # except FileNotFoundError:
        #     print('No cache similarity')

        # load datasets
        self.log('Load datasets')
        with open(self.file_datasets, 'r', encoding='utf8') as f:
            self.datasets = [{k: v for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]

        # start request
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

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
                    similarity = self.cache_sim[word][url]
                else:
                    word = self.process_text(word, True)
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
        # hitung total similarity
        # for dataset in self.datasets:
        #     total_similarity = 0
        #     count_similarity = 0
        #     for domain in dataset['result']:
        #         result = dataset['result'][domain]
        #         total_similarity += result['similarity']
        #         count_similarity += 1

        #     dataset['similarity'] = total_similarity / count_similarity

        # get timestamp untuk namafile
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime("%Y%m%d_%H%M%S.%f")
        file_result = 'result/datasets-%s.json' % timestampStr
        file_rank = 'result/rank-%s.json' % timestampStr

        # simpan ke json
        with open(file_result, 'w') as f:
            json.dump(self.datasets, f)

        # sort by similarity lalu simpan ke json
        sorted_datasets = sorted(self.datasets, key=lambda i: i['similarity'], reverse=True)
        with open(file_rank, 'w') as f:
            json.dump(sorted_datasets, f)

    def closed(self, reason):
        self.dump()

