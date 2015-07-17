#!/usr/bin/python
# -*- coding: utf-8 -*-

import base64
import json
import logging
import random
import requests
import sys
import time

logging.basicConfig(filename1='/var/log/consul.log',
        format='%(asctime)s %(levelname)s: consul: %(message)s',
        level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class consul():
    def __init__(self, consul_urls, timeout = 10):
        self.urls = consul_urls
        self.cas = 0
        self.timeout = timeout

    def get_url(self):
        return random.choice(self.urls) + '/v1/kv'

    def message(self, id):
        msg = {}
        msg['id'] = id
        msg['start_time'] = time.time()
        msg['timeout'] = self.timeout

        js = json.dumps(msg, encoding='utf8', ensure_ascii=False)
        return js

    def decode_reply(self, js):
        logging.info("decoding reply: %s", js)
        for e in js:
            value = e.get('Value')
            if value:
                return json.loads(base64.b64decode(value))

        return None

    def get_lock_info(self, id):
        js = self.message(id)
        url = "%s/%s" % (self.get_url(), id)
        headers = {}

        r = requests.get(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok:
            logging.error("id: %s: could not read lock info: message: %s, status: %d", id, r.text, r.status_code)
            return None

        return r.json()

    def get_cas(self, id):
        cas = 0

        info = self.get_lock_info(id)
        if info and len(info) > 0:
            cas = info[0].get('ModifyIndex', 0)

        return cas

    def can_break_lock_set_cas(self, id):
        old = self.get_lock_info(id)
        if old and len(old) > 0:
            old_value = self.decode_reply(old)
            # if there is no value in reply (its a weird lock, not ours), just break the lock
            if old_value:
                old_timeout = old_value.get('timeout', self.timeout)
                old_start_time = old_value.get('start_time')

                # check that we are allowed to break the lock
                if old_start_time:
                    # not allowed to break the lock if its timeout hasn't yet expired
                    if int(old_start_time) + old_timeout >= time.time():
                        logging.info("id: %s: could not broke lock: timeout hasn't yet expired: old_start_time: %d, old_timeout: %d, current_time: %d",
                                id, old_start_time, old_timeout, time.time())
                        return False

                    logging.info("id: %s: time to break lock: timeout has expired: old_start_time: %d, old_timeout: %d, current_time: %d",
                                id, old_start_time, old_timeout, time.time())

            # since we are about to break the lock, get this key's modification index
            self.cas = old[0].get('ModifyIndex', 0)

        return True

    def break_lock(self, id, forced = False):
        if not forced:
            can = self.can_break_lock_set_cas(id)
            if not can:
                return False
        else:
            self.cas = self.get_cas(id)

        url = "%s/%s?cas=%d" % (self.get_url(), id, self.cas)
        js = self.message(id)
        headers = {}

        r = requests.put(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok:
            logging.error("id: %s: could not break lock: message: %s, status: %d", id, r.text, r.status_code)
            return False

        self.cas = self.get_cas(id)
        logging.info("id: %s: broke lock: cas: %d", id, self.cas)
        return r.text == 'true'

    def lock(self, id):
        if self.cas != 0:
            logging.error("id: %s: could not get lock: already locked, cas: %d", id, self.cas)
            return False

        js = self.message(id)

        # only allow to write this lock data if there is no other lock already being held
        self.cas = 0

        url = "%s/%s?cas=%d" % (self.get_url(), id, self.cas)
        headers = {}

        r = requests.put(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok or r.text != 'true':
            logging.error("id: %s: could not get lock: message: %s, status: %d", id, r.text, r.status_code)
            return False

        self.cas = self.get_cas(id)
        logging.info("id: %s: locked: cas: %d", id, self.cas)

        return True

    def continue_lock(self, id):
        if self.cas == 0:
            self.cas = self.get_cas(id)

        js = self.message(id)
        url = "%s/%s?cas=%d" % (self.get_url(), id, self.cas)
        headers = {}

        r = requests.put(url, data=js.decode('unicode_internal').encode('utf8'), headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok or r.text != 'true':
            logging.error("id: %s: could not continue lock: message: %s, status: %d", id, r.text, r.status_code)
            return False

        self.cas = self.get_cas(id)
        logging.info("id: %s: continued: cas: %d", id, self.cas)

        return True

    def unlock(self, id):
        url = "%s/%s?cas=%d" % (self.get_url(), id, self.cas)
        headers = {}

        r = requests.delete(url, headers=headers, timeout=self.timeout)
        if r.status_code != requests.codes.ok:
            logging.error("id: %s: could not unlock: message: %s, status: %d", id, r.text, r.status_code)
            return False

        logging.info("id: %s: unlocked: cas: %d", id, self.cas)

        if r.text == 'true':
            self.cas = 0
            return True

        return False

if __name__ == '__main__':
    urls = []

    if len(sys.argv) > 1:
        urls += sys.argv[1:]
    else:
        urls.append('http://localhost:8500')

    timeout = 5
    l = consul(urls, timeout)

    id = 'some-lock'

    logging.info("id: %s: going to lock", id)
    locked = l.lock(id)
    logging.info("id: %s: successfully locked: %s", id, locked)

    if not locked:
        logging.info("id: %s: going to break lock", id)
        broken = l.break_lock(id, forced = True)
        logging.info("id: %s: successfully broken: %s", id, broken)
        assert broken == True


    logging.info("id: %s: going to lock already locked lock, this should fail", id)
    locked = l.lock(id)
    logging.info("id: %s: successfully locked: %s", id, locked)
    assert locked == False

    logging.info("id: %s: going to unlock already locked lock", id)
    unlocked = l.unlock(id)
    logging.info("id: %s: successfully unlocked: %s", id, unlocked)
    assert unlocked == True

    logging.info("id: %s: going to lock", id)
    locked = l.lock(id)
    logging.info("id: %s: successfully locked: %s", id, locked)
    assert locked == True

    logging.info("id: %s: going to break lock", id)
    broken = l.break_lock(id, forced = True)
    logging.info("id: %s: successfully broken: %s", id, broken)
    assert broken == True

    logging.info("id: %s: going to unlock already locked lock", id)
    unlocked = l.unlock(id)
    logging.info("id: %s: successfully unlocked: %s", id, unlocked)
    assert unlocked == True
