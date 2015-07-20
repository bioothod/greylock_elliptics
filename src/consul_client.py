#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import re
import random
import requests
import sys

import consul
from client import search_machine, content_parser

logging.basicConfig(filename1='/var/log/consul.log',
        format='%(asctime)s %(levelname)s: consul: %(message)s',
        level=logging.INFO)
logging.getLogger().setLevel(logging.DEBUG)

class service:
    def __init__(self, id, name, addr, port):
        self.id = id
        self.name = name
        self.addr = addr
        self.port = port

    def __repr__(self):
        return '[id: %s, name: %s, addr: %s:%s]' % (self.id, self.name, self.addr, self.port)

    def url(self):
        return 'http://' + self.addr + ':' + str(self.port) + '/' + self.name

class consul_discovery(consul.consul):
    def __init__(self, urls, timeout = 10):
        consul.consul.__init__(self, urls, timeout)

        self.consul_urls = urls
        self.timeout = timeout

        # cache critical services on startup
        self.critical = self.get_critical_services()

    def get_random_consul_url(self):
        url = random.choice(self.consul_urls) + '/v1/catalog/service'
        return url

    def service_from_json(self, e):
        addr = e.get('ServiceAddress', e.get('Address'))
        port = e.get('ServicePort')
        sname = e.get('ServiceName')
        id = e.get('ServiceID', sname)

        return service(id, sname, addr, port)

    def get_critical_services(self):
        url = random.choice(self.consul_urls) + '/v1/health/state/critical'

        r = requests.get(url, timeout=self.timeout)
        if r.status_code != requests.codes.ok:
            logging.error("id: %s: could not read info about critical services: message: %s, status: %d", id, r.text, r.status_code)
            return None

        critical = []
        js = r.json()
        for e in js:
            s = self.service_from_json(e)
            if s:
                critical.append(s)

        logging.info("get_critical_services: critical: %s", critical)
        return critical

    def check_critical(self, s, crit):
        for c in crit:
            if s.id == c.id:
                return True
            if s.addr == c.addr and s.port == c.port:
                return True

        return False

    def read(self, url):
        headers = {}
        r = requests.get(url, headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok:
            logging.error("id: %s: could not continue lock: message: %s, status: %d", id, r.text, r.status_code)
            return False

        logging.info("%s: reply: %s", url, r.text)
        services = []
        for e in r.json():
            s = self.service_from_json(e)
            if s:
                if not self.check_critical(s, self.critical):
                    services.append(s)

        return services

    def get_index_services(self):
        url = self.get_random_consul_url() + '/index'
        return self.read(url)

    def get_normalize_services(self):
        url = self.get_random_consul_url() + '/normalize'
        return self.read(url)

    def get_search_services(self):
        # search service is the same as index
        url = self.get_random_consul_url() + '/search'
        return self.read(url)

if __name__ == '__main__':
    generic_parser = argparse.ArgumentParser(description='Generic arguments.', add_help=False)
    generic_parser.add_argument('--file', dest='file', action='store', type=argparse.FileType('r'),
            help='Input file to parse and index.')
    generic_parser.add_argument('--mailbox', dest='mailbox', action='store',
            help='All indexes are updated/searched in given mailbox only. ' +
                'If not provided, \'To\' header is used for indexation. ' + 
                'This option is required for search request.')


    consul_parser = argparse.ArgumentParser(description='Consul elliptics client arguments', add_help=False)
    consul_parser.add_argument('--consul-url', dest='consul_urls', action='append', required=True,
            help='URL used to lock/unlock and autodiscover normalize/index/search services via Consul service. Can be specified multiple times. ' +
                'If specified, direct normalize/index/search URLs will not be used '
                'and Consul autodiscovery will be used to find out needed services instead. ' +
                'Sharding will not be used too in favour of distributed locks implemented over Consul key-value API.')


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

    parser = argparse.ArgumentParser(description='Elliptics indexing client (Consul autodiscovery and locking).',
            parents=[generic_parser, consul_parser, index_parser, search_parser])

    args = parser.parse_args()

    c = consul_discovery(args.consul_urls)

    if not args.search:
        if not args.email and not args.id:
            logging.error("You must specify ID: it will be generated either from email (requires --email option), "
                    "or it must be provided via --id option")
            exit(-1)

        p = content_parser(args.mailbox, args.id)
        if args.email:
            p.feed_email(args.file)
        else:
            p.feed(args.file.read())

        logging.info("id: %s, mailbox: %s", p.id, p.mailbox)

        if args.dry_run:
            exit(0)

        if not p.mailbox:
            logging.error("Parser has failed to find mailbox in data, it has to be specified in command line.")
            exit(-1)

        sm = search_machine(p.mailbox)

        normalize_services = c.get_normalize_services()
        if len(normalize_services) == 0:
            logging.error("There are no normalize services available, exiting.")
            exit(-1)

        url = random.choice(normalize_services).url()
        words = sm.normalize(url, p.words)

        index_services = c.get_index_services()
        if len(index_services) == 0:
            logging.error("There are no index services available, exiting.")
            exit(-1)

        index_url = random.choice(index_services).url()

        c.lock(p.mailbox)
        try:
            sm.index(index_url, words, p.attrs, p.id, args.bucket, args.key)
        except Exception as e:
            logging.error("index exception: url: %s, id: %s: %s", index_url, p.id, e)

        c.unlock(p.mailbox)
    else:
        if not args.mailbox:
            logging.error("You must specify mailbox name to search in")
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

        normalize_services = c.get_normalize_services()
        if len(normalize_services) == 0:
            logging.error("There are no normalize services available, exiting.")
            exit(-1)

        url = random.choice(normalize_services).url()
        words = sm.normalize(url, words)

        search_services = c.get_search_services()
        if len(search_services) == 0:
            logging.error("There are no search services available, exiting.")
            exit(-1)

        search_url = random.choice(search_services).url()

        c.lock(args.mailbox)
        try:
            sm.search(search_url, words, attrs, args.page_start, args.page_num)
        except Exception as e:
            logging.error("search exception: url: %s: %s", search_url, e)

        c.unlock(args.mailbox)
