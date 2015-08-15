#!/usr/bin/python

import argparse
import json
import logging
import random
import requests
import time

logging.basicConfig(filename='perf.log',
        format='%(asctime)s %(levelname)s: perf: %(message)s',
        level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class client:
    def __init__(self, seed):
        random.seed(seed)

        self.max = 10000000
        self.sample = 'abcdefghijklmnopqrstuvwxyz0123456789.'

        self.indexes = []
        self.num_indexes = 3000
        self.gen_indexes(self.num_indexes)

        self.max_indexes_per_message = 300

    def rand_string(self, ln):
        return ''.join(random.sample(self.sample, ln))

    def msg_id(self):
        return self.rand_string(10) + '@' + self.rand_string(20)

    def gen_indexes(self, num):
        for i in range(num):
            self.indexes.append(self.rand_string(random.randint(5, 20)))

    def get_indexes(self):
        return random.sample(self.indexes, self.max_indexes_per_message)

    def format_elliptics(self):
        raw = {}
        raw['id'] = self.msg_id()
        raw['bucket'] = ""
        raw['key'] = ""

        msg = {}
        msg['indexes'] = self.get_indexes()
        msg['ids'] = [raw]
        return json.dumps(msg, encoding='utf8', ensure_ascii=False)

    def send(self, url, data):
        headers = {}
        timeout = 10

        #print data

        r = requests.post(url, data=data.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not update indexes: url: %s, status: %d" % (url, r.status_code))

    def generate_and_send(self, url):
        data = self.format_elliptics()
        self.send(url, data)

    def test(self, url):
        num = 0
        while True:
            start = time.time()
            self.generate_and_send(url)
            end = time.time()

            num += 1
            logging.info("%d %d/%d %f", num, self.max_indexes_per_message, self.num_indexes, (end - start) * 1000.0)
            print time.ctime(), num, self.max_indexes_per_message, self.num_indexes, (end - start) * 1000.0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Performance tester arguments')
    parser.add_argument('--url', dest='url', action='store', required=True,
            help='Remote URL to send index request')

    args = parser.parse_args()
    c = client(1)
    c.test(args.url)
