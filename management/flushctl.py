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

import mc_bin_client

def usage():
    print >> sys.stderr, "Usage: %s host:port start|stop" % sys.argv[0]
    exit(1)

if __name__ == '__main__':
    try:
        hp, cmd = sys.argv[1:]
        host, port = hp.split(':')
        port = int(port)
    except ValueError:
        usage()

    mc = mc_bin_client.MemcachedClient(host, port)
    f = {'stop': mc.stop_persistence, 'start': mc.start_persistence}.get(cmd)

    if not f:
        usage()

    print "Issuing %s command" % cmd
    f()

