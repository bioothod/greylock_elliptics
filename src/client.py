#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import re
import random
import requests
import sys

from HTMLParser import HTMLParser

from chardet.universaldetector import UniversalDetector

from email.parser import Parser
from email.utils import parseaddr, parsedate_tz, mktime_tz

class content_parser(HTMLParser):
    def __init__(self, mailbox, id):
        HTMLParser.__init__(self)

        self.detector = UniversalDetector()

        # if this ID lives in elliptics
        self.id = id

        # all indexes are related to given mailbox
        # if it is None, 'To' address is used
        self.set_mailbox(mailbox)

        self.words = set()
        self.attrs = set()

        self.encoding = ''
        self.url = re.compile('(\w+)(\.\w+)+(:\d+)?(/+\w+)+')
        self.host = re.compile('(\w+)(\.\w+)+(:\d+)?')
        self.mail = re.compile('(\w+)([\.!\-_\+]\w+)*@(\w+)([\.!\-_\+]\w+)*')

    def set_mailbox(self, mailbox):
        self.mailbox = mailbox

    def detect_encoding(self, text):
        self.detector.reset()
        self.detector.feed(text)
        self.detector.close()

        return self.detector.result['encoding']

    def set_encoding(self, enc):
        self.encoding = enc

    def set_encoding_from_email(self, msg):
        enc = msg.get_content_charset()
        if not enc:
            charset = msg.get_charset()
            if charset:
                enc = charset.input_codec

        self.set_encoding(enc)

    def recode(self, text):
        enc = self.encoding

        if not enc:
            enc = self.detect_encoding(text)

        if enc == 'binary':
            return u''

        if not enc or enc == 'unknown-8bit':
            return unicode(text, errors='ignore')

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

        def get_mail_addr(ret):
            for r in ret:
                addr = parseaddr(r)
                if len(addr[1]) != 0:
                    # sanity check to find non-local addresses, i.e. not 'username', but something like 'name@host.domain')
                    m = re.match("([^@|\s]+@[^@]+\.[^@|\s]+)", addr[1])
                    if m:
                        return m.group(1)
            return None

        def feed_mail_addr(ret, prefix):
            for r in ret:
                self.words.add(r)
            addr = get_mail_addr(ret)
            if addr:
                self.attrs.add(prefix + ':' + addr)


        for r in parse_header(msg['Subject']):
            self.words.add(r)

        feed_mail_addr(parse_header(msg['Cc']), 'to')
        feed_mail_addr(parse_header(msg['Bcc']), 'to')
        feed_mail_addr(parse_header(msg['From']), 'from')

        to_header = parse_header(msg['To'])
        feed_mail_addr(to_header, 'to')

        # this address will be used to modify every index,
        # i.e. this scripts only updates indexes which belong to given mailbox
        if not self.mailbox:
            self.set_mailbox(get_mail_addr(to_header))
            if not self.mailbox:
                raise NameError("No mailbox name has been provided: there is no 'To' header and nothing was provided via command line, exiting")

        if not self.id or len(self.id) == 0:
            msg_id = msg['Message-Id']
            if not msg_id:
                raise NameError("There is no 'Message-Id' header and no ID has been specified via command line, exiting")

            # @get_mail_addr() performs sanity check on its arguments
            self.id = get_mail_addr([msg_id])
            if not self.id or len(self.id) == 0:
                raise NameError("Could not detect ID in 'Message-Id' header and "
                        "no ID has been provided via command line, exiting")

            t = mktime_tz(parsedate_tz(msg['Date']))
            self.id = str(t) + '.' + self.id

        def feed_check_multipart(msg):
            if not msg.is_multipart():
                self.set_encoding_from_email(msg)
                self.feed(msg.get_payload(decode=True))
            else:
                # these are multipart parts as email.Message objects
                for m in msg.get_payload():
                    feed_check_multipart(m)

        feed_check_multipart(msg)


class search_machine():
    def __init__(self, mailbox):
        if not mailbox:
            raise RuntimeError("Search machine must be initialized with valid mailbox, it can not be NULL")

        self.mailbox = mailbox
        self.mailbox_hash = hash(mailbox) & 0xffffffff

    def normalize(self, url, words):
        raw = {}
        raw['text'] = ' '.join(words)

        #print raw['text']
        # this will be a unicode string
        js = json.dumps(raw, encoding='utf8', ensure_ascii=False)

        headers = {}
        timeout = len(js) / 1000 + 10

        #print js.decode('unicode_internal').encode('utf8')
        r = requests.post(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not normalize text: url: %s, status: %d" % (url, r.status_code))

        return r.json()['keys'].values()

    def get_url(self, urls):
        idx = self.mailbox_hash % len(urls)
        return urls[idx]

    def text_attrs_to_dict(self, text, attrs):
        idx = {}
        for a in attrs:
            spl = a.split(':', 1)
            if len(spl) >= 2:
                idx[spl[0]] = spl[1]
        idx["text"] = text

        return idx

    def index(self, url, text, attrs, id, bucket, key):
        doc = {}
        doc["id"] = id
        doc["bucket"] = bucket
        doc["key"] = key
        doc["index"] = self.text_attrs_to_dict(text, attrs)

        docs = {}
        docs["docs"] = [doc]
        docs["mailbox"] = self.mailbox


        # this will be a unicode string
        js = json.dumps(docs, encoding='utf8', ensure_ascii=False)
        print js

        headers = {}
        timeout = len(words) / 50 + 10

        r = requests.post(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not update indexes: url: %s, status: %d" % (url, r.status_code))

        print "All indexes for document '%s' have been successfully updated" % (id)

    def index_multiple_urls(self, urls, text, attrs, id, bucket, key):
        url = self.get_url(urls)
        return self.index(url, text, attrs, id, bucket, key)

    def search(self, url, text, attrs, paging_start, paging_num):
        p = {}
        p["num"] = paging_num
        p["start"] = paging_start

        s = {}
        s["paging"] = p
        s["text"] = text
        s["mailbox"] = self.mailbox
        s["query"] = self.text_attrs_to_dict(text, attrs)

        # this will be a unicode string
        js = json.dumps(s, encoding='utf8', ensure_ascii=False)
        print js

        headers = {}
        timeout = len(words) / 100 + 10

        r = requests.post(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            raise RuntimeError("Could not search for indexes: url: %s, status: %d" % (url, r.status_code))

        res = json.loads(r.text)

        print "completed: %s" % res["completed"]
        print "paging: num: %d, start: '%s'" % (res["paging"]["num"], res["paging"]["start"])
        for k in res["ids"]:
            print "bucket: '%s', key: '%s', id: '%s'" % (k["bucket"], k["key"], k["id"])

    def search_multiple_urls(self, urls, text, attrs, paging_start, paging_num):
        url = self.get_url(urls)
        return self.search(url, text, attrs, paging_start, paging_num)


if __name__ == '__main__':
    generic_parser = argparse.ArgumentParser(description='Generic arguments.', add_help=False)
    generic_parser.add_argument('--file', dest='file', action='store', type=argparse.FileType('r'),
            help='Input file to parse and index.')
    generic_parser.add_argument('--mailbox', dest='mailbox', action='store',
            help='All indexes are updated/searched in given mailbox only. ' +
                'If not provided, \'To\' header is used for indexation. ' + 
                'This option is required for search request.')


    direct_parser = argparse.ArgumentParser(description='Arguments, which must be specified if Consul autodiscovery is not used.', add_help=False)
    direct_parser.add_argument('--normalize-url', dest='normalize_urls', action='append',
            help='URL used to normalize data, for example: http://example.com/normalize. Can be specified multiple times.')
    direct_parser.add_argument('--index-url', dest='index_urls', action='append',
            help='URL used to index data, for example: http://example.com/index. Can be specified multiple times.')
    direct_parser.add_argument('--search-url', dest='search_urls', action='append',
            help='URL used to search for data, for example: http://example.com/search. Can be specified multiple times.')


    index_parser = argparse.ArgumentParser(description='Index arguments.', add_help=False)
    index_parser.add_argument('--id', dest='id', action='store',
            help='ID of the document used in indexing.')
    index_parser.add_argument('--bucket', dest='bucket', action='store', default="",
            help='Bucket (if stored in elliptics) of the document used in indexing.')
    index_parser.add_argument('--key', dest='key', action='store', default="",
            help='Key (if stored in elliptics) of the document used in indexing.')
    index_parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
            help='Do not normalize and index data, just parse and print processing messages (if enabled).')
    index_parser.add_argument('--email', dest='email', action='store_true', default=False,
            help='Provided document is an email and should be parsed accordingly.')


    search_parser = argparse.ArgumentParser(description='Search arguments.', add_help=False)
    search_parser.add_argument('--search', dest='search', action='store',
            help='Text to search (documents containg every token will be returned), '
                'use \'attr:to:address@host.name\' to search for attributes (To,Cc,Bcc and From headers results in \'to\' and \'from\' attributes).')
    search_parser.add_argument('--page-num', dest='page_num', action='store', default=100,
            help='Maximum number of documents for given search request.')
    search_parser.add_argument('--page-start', dest='page_start', action='store', default='',
            help='Start token for the second and higher search result pages ' +
                 '(this token is returned by server and should be set for the next request).')

    parser = argparse.ArgumentParser(description='Elliptics indexing client.',
            parents=[generic_parser, direct_parser, index_parser, search_parser])


    args = parser.parse_args()

    if not args.search:
        if not args.email and not args.id:
            print("You must specify ID: it will be generated either from email (requires --email option), "
                    "or it must be provided via --id option")
            exit(-1)

        p = content_parser(args.mailbox, args.id)
        if args.email:
            p.feed_email(args.file)
        else:
            p.feed(args.file.read())

        print "id: %s, mailbox: %s" % (p.id, p.mailbox)

        if args.dry_run:
            exit(0)

        words = p.words

        sm = search_machine(p.mailbox)
        if args.normalize_urls and len(args.normalize_urls) != 0:
            norm_url = random.choice(args.normalize_urls)
            words = sm.normalize(norm_url, words)

        sm.index_multiple_urls(args.index_urls, ' '.join(words), p.attrs, p.id, args.bucket, args.key)
    else:
        if not args.mailbox:
            print("You must specify mailbox name to search in")
            exit(-1)

        if args.dry_run:
            exit(0)

        attrs = []
        words = []
        for a in args.search.split():
            if a.startswith('attr:'):
                aname = a[5:]
                if len(aname) > 0:
                    attrs.append(aname)
            else:
                words.append(a)

        sm = search_machine(args.mailbox)

        if args.normalize_urls and len(args.normalize_urls) != 0:
            norm_url = random.choice(args.normalize_urls)
            words = sm.normalize(norm_url, words)

        sm.search_multiple_urls(args.search_urls, ' '.join(words), attrs, args.page_start, args.page_num)
