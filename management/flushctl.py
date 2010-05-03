#!/usr/bin/env python
"""
Flush control for ep-engine.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import string
import random
import struct
import exceptions
import time

import mc_bin_client

def usage():
    print >> sys.stderr, "Usage: %s host:port start|stop" % sys.argv[0]
    print >> sys.stderr, "  or   %s host:port set param value" % sys.argv[0]
    print >> sys.stderr, ""
    print >> sys.stderr, " Available params:"
    print >> sys.stderr, "    min_data_age  - minimum data age before flushing data"
    print >> sys.stderr, "    queue_age_cap - maximum queue age before flushing data"
    print >> sys.stderr, "    max_txn_size - maximum number of items in a flusher transaction"
    exit(1)

if __name__ == '__main__':
    try:
        hp, cmd = sys.argv[1:3]
        host, port = hp.split(':')
        port = int(port)
    except ValueError:
        usage()

    mc = mc_bin_client.MemcachedClient(host, port)
    f = {'stop': mc.stop_persistence,
         'start': mc.start_persistence,
         'set': mc.set_flush_param}.get(cmd)

    if not f:
        usage()

    print "Issuing %s command" % cmd
    f(*sys.argv[3:])

    if cmd == 'stop':
        stopped = False
        while not stopped:
            time.sleep(0.5)
            stats = mc.stats()
            if stats['ep_flusher_state'] == 'paused':
                stopped = True

