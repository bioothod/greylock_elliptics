#!/usr/bin/python

import argparse
import json
import requests

class search_client():
    def __init__(self, url):
        self.url = url

    def search(self, indexes, page_num, page_start):
        s = {}
        p = {}
        p['num'] = page_num
        p['start'] = page_start
        s['paging'] = p
        s['indexes'] = indexes

        js = json.dumps(s, encoding='utf8', ensure_ascii=False)
        print js

        headers = {}
        timeout = len(indexes) / 100 + 30

        r = requests.post(self.url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not search for indexes: url: %s, status: %d" % (self.url, r.status_code))

        res = json.loads(r.text)

        print "completed: %s" % res["completed"]
        print "paging: num: %d, start: '%s'" % (res["paging"]["num"], res["paging"]["start"])
        for k in res["ids"]:
            print "bucket: '%s', key: '%s', id: '%s'" % (k["bucket"], k["key"], k["id"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Simple search client arguments')
    parser.add_argument('--url', dest='url', action='store', required=True,
            help='Remote URL to send index request')
    parser.add_argument('--search', dest='search', action='store', required=True,
            help='Text to search (documents containing every token will be returned)')
    parser.add_argument('--page-num', dest='page_num', action='store', default=100,
            help='Maximum number of documents for given search request.')
    parser.add_argument('--page-start', dest='page_start', action='store', default='',
            help='Start token for the second and higher search result pages ' +
                 '(this token is returned by server and should be set for the next request).')

    args = parser.parse_args()

    c = search_client(args.url)
    c.search(page_num = args.page_num, page_start = args.page_start, indexes = args.search.split())
