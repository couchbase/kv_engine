#!/usr/bin/env python

import sys

import mc_bin_client

def usage():
    print >> sys.stderr, "Usage: %s host:port list" % sys.argv[0]
    print >> sys.stderr, "  or   %s host:port set [vbid] [vbstate]" % sys.argv[0]
    sys.exit(1)

def listvb(mc):
    vbs = mc.stats('vbucket')
    for (vb, state) in sorted(list(vbs.items())):
        print "vbucket", vb[3:], state

if __name__ == '__main__':

    try:
        hp, cmd = sys.argv[1:3]
        host, port = hp.split(':')
        port = int(port)
    except ValueError:
        usage()

    mc = mc_bin_client.MemcachedClient(host, port)
    f = {'list': lambda: listvb(mc),
         'set': mc.set_vbucket_state}.get(cmd)

    if not f:
        usage()

    print "Issuing %s command" % cmd
    f(*sys.argv[3:])
