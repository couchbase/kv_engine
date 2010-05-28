#!/usr/bin/env python
"""
Tap control for ep-engine.

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
    print >> sys.stderr, "Usage: %s host:port start|stop|status for start/stop replication" % sys.argv[0]
    print >> sys.stderr, "  or   %s host:port set param value" % sys.argv[0]
    print >> sys.stderr, ""
    print >> sys.stderr, " Available params:"
    print >> sys.stderr, "    tap_peer  - set the replication master"
    sys.exit(1)

if __name__ == '__main__':
    try:
        hp, cmd = sys.argv[1:3]
        host, port = hp.split(':')
        port = int(port)
    except ValueError:
        usage()

    mc = mc_bin_client.MemcachedClient(host, port)
    f = {'stop': mc.stop_replication,
         'start': mc.start_replication,
         'set': mc.set_tap_param,
         'status':mc.stats}.get(cmd)

    if not f:
        usage()

    print "Issuing %s command" % cmd
    if cmd != 'status':
        try:
            f(*sys.argv[3:])
        except mc_bin_client.MemcachedError, e:
            print "ERROR: %s" % e.msg


    stats = mc.stats("tap")

    print
    print "Current replication stats: "
    print "ep_replication_peer  : %s" % stats['ep_replication_peer']
    print "ep_replication_state : %s" % stats['ep_replication_state']
    print "ep_replication_status: %s" % stats['ep_replication_status']
    print

    if int(stats['ep_tap_count']) > 0:
        print "Replication clients:"
        for t in [ t for t in stats if ':qlen' in t ]:
            k = t.split(':')
            print "%s:%s - backlog: %s" %( k[1], k[2], stats[t])
        print
