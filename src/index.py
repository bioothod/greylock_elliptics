#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import re
import requests
import sys

from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint

from chardet.universaldetector import UniversalDetector

from email.parser import Parser
from email.utils import parseaddr

class indexes_client_parser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)

        self.words = set()
        self.detector = UniversalDetector()
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

    def detect_encoding(self, text):
        self.detector.reset()
        self.detector.feed(text)
        self.detector.close()

        return self.detector.result['encoding']

    def recode(self, text):
        enc = self.encoding
        if enc == '':
            enc = self.detect_encoding(text)

        if enc == 'binary':
            return u''

        if enc == 'unknown-8bit':
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
        rep = '`-=][\';/.,~!@#$%^&*()+}{\":?><\\|'
        rep_char = ' '

        decoded = self.recode(data).lower()
        
        cleared = ''
        for x in decoded:
            if x in rep:
                cleared += rep_char
            else:
                cleared += x

        for w in cleared.split():
            if len(w) > 0:
                #print "%s" % w.decode('unicode_internal').encode('utf8')
                self.words.add(w)

        self.parse_regexps(decoded)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Elliptics indexing client.')
    parser.add_argument('--file', dest='file', action='store', required=True, type=argparse.FileType('r'),
            help='Input file to parse and index')
    parser.add_argument('--id', dest='id', action='store',
            help='ID of the document used in indexing')
    parser.add_argument('--email', dest='email', action='store_true', default=False,
            help='Provided document is an email and should be parsed accordingly')
    parser.add_argument('--indexes-server', dest='iserver', action='store', required=True,
            help='Indexes server, for example: http://example.com')
    parser.add_argument('--bucket', dest='bucket', action='store', default="",
            help='Bucket (if stored in elliptics) of the document used in indexing')
    parser.add_argument('--key', dest='key', action='store', default="",
            help='Key (if stored in elliptics) of the document used in indexing')

    args = parser.parse_args()

    iparser = indexes_client_parser()

    if not args.email and not args.id:
        print("You must specify ID: it will be generated either from email (requires --email option), "
                "or it must be provided via --id option")
        exit(-1)

    id = ''
    if args.id:
        id = args.id

    if args.email:
        p = Parser()
        msg = p.parse(args.file)

        from email.header import decode_header
        def parse_header(h):
            if not h or len(h) == 0:
                return []

            ret = []
            for x in decode_header(h):
                if not x[1]:
                    ret.append(x[0].encode('utf8'))
                    print x[0]
                else:
                    print x[0].decode(x[1]).encode('utf8')
                    ret.append(x[0].decode(x[1]).encode('utf8'))

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
            print ret
            for r in ret:
                iparser.feed(r)

        feed_header(parse_header(msg['Subject']))
        feed_header(parse_header(msg['Cc']))
        feed_header(parse_header(msg['Bcc']))
        feed_header(parse_header(msg['From']))

        h = parse_header(msg['To'])
        feed_header(h)

        if len(id) == 0:
            id = get_id(h)
            if not id or len(id) == 0:
                print("Could not detect ID in email (there is no 'To' header) and no ID has been provided via command line, exiting")
                exit(-1)


        def feed_check_multipart(msg):
            if not msg.is_multipart():
                iparser.set_encoding_from_email(msg)
                iparser.feed(msg.get_payload(decode=True))
            else:
                # these are multipart parts as email.Message objects
                for m in msg.get_payload():
                    feed_check_multipart(m)

        feed_check_multipart(msg)
    else:
        iparser.feed(args.file.read())

    print "Tokens: %d, id: %s" % (len(iparser.words), id)
    for i in range(1000):
        raw = {}
        raw["id"] = "%s.%d" % (id, i)
        raw["bucket"] = args.bucket
        raw["key"] = args.key

        msg = {}
        msg["ids"] = [raw]
        msg["indexes"] = []

        for w in iparser.words:
            msg["indexes"].append(w)

        # this will be a unicode string
        js = json.dumps(msg, encoding='utf8', ensure_ascii=False)

        url = args.iserver + '/index'
        headers = {}
        timeout = 3 * len(iparser.words)

        #print js.decode('unicode_internal').encode('utf8')
        r = requests.post(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=timeout)
        if r.status_code != requests.codes.ok:
            print "Could not update indexes:", r.status_code
            exit(-1)

        print "Index has been successfully updated for ID %s" % id["id"]
    #print js.decode('unicode_internal').encode('utf8')

