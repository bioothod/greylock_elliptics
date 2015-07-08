#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import re
import requests
import sys

from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint

from email.parser import Parser
from email.utils import parseaddr

class indexes_client_parser(HTMLParser):
    def __init__(self, id, bucket, key):
        HTMLParser.__init__(self)

        # if this ID lives in elliptics, bucket and key can be used to read the data
        self.id = id
        self.bucket = bucket
        self.key = key

        self.words = set()
        self.normalized_words = set()

        self.encoding = ''
        self.url = re.compile('(\w+)(\.\w+)+(:\d+)?(/+\w+)+')
        self.host = re.compile('(\w+)(\.\w+)+(:\d+)?')
        self.mail = re.compile('(\w+)([\.!\-_\+]\w+)*@(\w+)([\.!\-_\+]\w+)*')

    def set_encoding(self, enc):
        self.encoding = enc

    def set_encoding_from_email(self, msg):
        enc = msg.get_content_charset()
        if not enc:
            charset = msg.get_charset()
            if charset:
                enc = charset.input_codec

        if enc:
            self.set_encoding(enc)
        else:
            self.set_encoding('')

    def recode(self, text):
        enc = self.encoding

        if enc == 'binary':
            return u''

        if enc == 'unknown-8bit':
            return text

        if enc == '':
            return text

        #print text.decode(enc), enc
        return unicode(text.decode(enc))

    def parse_regexps(self, decoded):
        for m in self.host.finditer(decoded):
            s = m.group(0)
            self.words.add(s)
            #print "host: %s" % s.decode('unicode_internal').encode('utf8')
        for m in self.url.finditer(decoded):
            s = m.group(0)
            self.words.add(s)
            #print "url: %s" % s.decode('unicode_internal').encode('utf8')
        for m in self.mail.finditer(decoded):
            s = m.group(0)
            self.words.add(s)
            #print "mail: %s" % s.decode('unicode_internal').encode('utf8')

    # tags may contain meaningful data too
    def handle_starttag(self, tag, attrs):
        for a in attrs:
            self.handle_data(a)

        self.handle_data(tag)

    def handle_endtag(self, tag):
        pass
    def handle_data(self, data):
        decoded = self.recode(data).lower()
        if len(decoded) != 0:
            self.words.add(decoded)
            self.parse_regexps(decoded)

    def feed_email(self, reader):
        p = Parser()
        msg = p.parse(reader)

        from email.header import decode_header
        def parse_header(h):
            if not h or len(h) == 0:
                return []

            ret = []
            for x in decode_header(h):
                if not x[1]:
                    ret.append(x[0])
                    #print x[0]
                else:
                    #print x[0].decode(x[1]).encode('utf8')
                    ret.append(x[0].decode(x[1]))

            return ret

        def get_id(ret):
            for r in ret:
                addr = parseaddr(r)
                if len(addr[1]) != 0:
                    # sanity check to find non-local addresses, i.e. not 'username', but something like 'name@host.domain')
                    m = re.match("([^@|\s]+@[^@]+\.[^@|\s]+)", addr[1])
                    if m:
                        return m.group(1)
            return None

        def feed_header(ret):
            for r in ret:
                self.words.add(r)

        feed_header(parse_header(msg['Subject']))
        feed_header(parse_header(msg['Cc']))
        feed_header(parse_header(msg['Bcc']))
        feed_header(parse_header(msg['From']))

        h = parse_header(msg['To'])
        feed_header(h)

        if not self.id or len(self.id) == 0:
            self.id = get_id(h)
            if not self.id or len(self.id) == 0:
                raise NameError("Could not detect ID in email (there is no 'To' header) and "
                        "no ID has been provided via command line, exiting")

        def feed_check_multipart(msg):
            if not msg.is_multipart():
                self.set_encoding_from_email(msg)
                self.feed(msg.get_payload(decode=True))
            else:
                # these are multipart parts as email.Message objects
                for m in msg.get_payload():
                    feed_check_multipart(m)

        feed_check_multipart(msg)

    def normalize(self, normalize_url):
        raw = {}
        raw['text'] = '. '.join(self.words)

        # this will be a unicode string
        js = json.dumps(raw, encoding='utf8', ensure_ascii=False)

        headers = {}
        timeout = len(js) / 1000 + 10

        #print js.decode('unicode_internal').encode('utf8')
        r = requests.post(normalize_url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not normalize text: url: %s, status: %d" % (normalize_url, r.status_code))

        ret = r.json()
        for k, v in ret['keys'].items():
            self.normalized_words.add(v)
            print "%s -> %s" % (k, v)

    def index(self, index_url):
        raw = {}
        raw["id"] = self.id
        raw["bucket"] = self.bucket
        raw["key"] = self.key

        msg = {}
        msg["ids"] = [raw]
        msg["indexes"] = []

        for w in self.normalized_words:
            msg["indexes"].append(w)

        # this will be a unicode string
        js = json.dumps(msg, encoding='utf8', ensure_ascii=False)
        #print js.decode('unicode_internal').encode('utf8')

        headers = {}
        timeout = len(self.normalized_words) / 50 + 10

        #print js.decode('unicode_internal').encode('utf8')
        r = requests.post(index_url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not update indexes: url: %s, status: %d" % (index_url, r.status_code))

        print "Index has been successfully updated for ID %s" % raw["id"]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Elliptics indexing client.')
    parser.add_argument('--file', dest='file', action='store', required=True, type=argparse.FileType('r'),
            help='Input file to parse and index')

    parser.add_argument('--id', dest='id', action='store',
            help='ID of the document used in indexing')
    parser.add_argument('--bucket', dest='bucket', action='store', default="",
            help='Bucket (if stored in elliptics) of the document used in indexing')
    parser.add_argument('--key', dest='key', action='store', default="",
            help='Key (if stored in elliptics) of the document used in indexing')

    parser.add_argument('--email', dest='email', action='store_true', default=False,
            help='Provided document is an email and should be parsed accordingly')

    parser.add_argument('--normalize-url', dest='normalize_url', action='store', required=True,
            help='URL used to normalize data, for example: http://example.com/normalize')
    parser.add_argument('--index-url', dest='index_url', action='store', required=True,
            help='URL used to index data, for example: http://example.com/index')


    args = parser.parse_args()

    if not args.email and not args.id:
        print("You must specify ID: it will be generated either from email (requires --email option), "
                "or it must be provided via --id option")
        exit(-1)

    iparser = indexes_client_parser(args.id, args.bucket, args.key)

    if args.email:
        iparser.feed_email(args.file)
    else:
        iparser.feed(args.file.read())

    print "id: %s" % (iparser.id)
    iparser.normalize(args.normalize_url)
    iparser.index(args.index_url)

