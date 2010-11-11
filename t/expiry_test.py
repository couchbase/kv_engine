#!/usr/bin/env python

import sys
import time
import exceptions

sys.path.append('../management')
import mc_bin_client

EXPIRY = 2
VERBOSE = False

def log(*s):
    if VERBOSE:
        print ' '.join(s)

def store(mc, kprefix):
    k = kprefix + '.set'
    log(">> ", k)
    mc.set(k, EXPIRY, 0, k)
    k = kprefix + '.add'
    log("++ ", k)
    mc.add(k, EXPIRY, 0, k)

def check(mc, kprefix):
    for suffix in ['.set', '.add']:
        try:
            k = kprefix + suffix
            log("<< ", k)
            mc.get(k)
            raise exceptions.Exception("Expected to fail to get " + k)
        except mc_bin_client.MemcachedError:
            pass

if __name__ == '__main__':
    mc = mc_bin_client.MemcachedClient()

    if '-v' in sys.argv:
        VERBOSE = True

    store(mc, 'a1')
    store(mc, 'a2')
    log("(sleep)")
    time.sleep(EXPIRY + 1)
    check(mc, 'a2')
    store(mc, 'a1')
    store(mc, 'a2')
