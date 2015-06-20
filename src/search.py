#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import requests
import sys

from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint

from chardet.universaldetector import UniversalDetector

class searcher:
    def __init__(self):
        self.detector = UniversalDetector()

    def detect_encoding(self, text):
        self.detector.reset()
        self.detector.feed(text)
        self.detector.close()

        return self.detector.result['encoding']

    def recode(self, text):
        enc = self.detect_encoding(text)

        if enc == 'binary':
            return u''

        return unicode(text.decode(enc))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Elliptics indexes searching client.')
    parser.add_argument('--idx', dest='idx', action='append', required=True,
            help='Search for document IDs which contain all requested indexes.')
    parser.add_argument('--indexes-server', dest='iserver', action='store', required=True,
            help='Indexes server, for example: http://example.com')
    parser.add_argument('--page-num', dest='page_num', action='store', default=100,
            help='Maximum number of documents for this request')
    parser.add_argument('--page-start', dest='page_start', action='store', default='',
            help='Start token for the second and higher search result pages ' +
                 '(this token is returned by server and should be set for the next request)')

    args = parser.parse_args()

    search = searcher()

    s = {}

    p = {}
    p["num"] = args.page_num
    p["start"] = args.page_start
    s["paging"] = p
    s["indexes"] = args.idx

    # this will be a unicode string
    js = json.dumps(s, encoding='utf8', ensure_ascii=False)

    url = args.iserver + '/search'
    headers = {}
    timeout = 3 * len(args.idx)

    r = requests.post(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
    if r.status_code != requests.codes.ok:
        print "Could not search for indexes:", r.status_code
        exit(-1)

    res = json.loads(r.text)

    print "completed: %s" % res["completed"]
    print "paging: num: %d, start: '%s'" % (res["paging"]["num"], res["paging"]["start"])
    for k in res["ids"]:
        print "bucket: '%s', key: '%s', id: '%s'" % (k["bucket"], k["key"], k["id"])

    #print js.decode('unicode_internal').encode('utf8')


