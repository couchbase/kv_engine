#!/usr/bin/env python
"""
Example script for tap.py.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""

import os
import sys
import string
import asyncore
import signal
import getopt

import mc_bin_server
import mc_bin_client

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT

import memcacheConstants

import tap

def usage(err=0):
    print >> sys.stderr, """
Usage: %s [-u bucket_user [-p bucket_password]] host:port [... hostN:portN]

Example:
  %s -u user_profiles -p secret9876 membase-01:11210 membase-02:11210
""" % (os.path.basename(sys.argv[0]),
       os.path.basename(sys.argv[0]))
    sys.exit(err)

def parse_args(args):
    user = None
    pswd = None

    try:
        opts, args = getopt.getopt(args, 'hu:p:', ['help'])
    except getopt.GetoptError, e:
        usage("ERROR: " + e.msg)

    for (o, a) in opts:
        if o == '--help' or o == '-h':
            usage()
        elif o == '-u':
            user = a
        elif o == '-p':
            pswd = a
        else:
            usage("ERROR: unknown option - " + o)

    if not args or len(args) < 1:
        usage("ERROR: missing at least one host:port to TAP")

    return user, pswd, args

def signal_handler(signal, frame):
    print 'Tap stream terminated by user'
    sys.exit(0)

def buildGoodSet(goodChars=string.printable, badChar='?'):
    """Build a translation table that turns all characters not in goodChars
    to badChar"""
    allChars=string.maketrans("", "")
    badchars=string.translate(allChars, allChars, goodChars)
    rv=string.maketrans(badchars, badChar * len(badchars))
    return rv

# Build a translation table that includes only characters
transt=buildGoodSet()

def abbrev(v, maxlen=30):
    if len(v) > maxlen:
        return v[:maxlen] + "..."
    else:
        return v

def keyprint(v):
    return string.translate(abbrev(v), transt)

def mainLoop(serverList, cb, opts={}, user=None, pswd=None):
    """Run the given callback for each tap message from any of the
    upstream servers.

    loops until all connections drop
    """
    signal.signal(signal.SIGINT, signal_handler)

    connections = (tap.TapDescriptor(a) for a in serverList)
    tap.TapClient(connections, cb, opts=opts, user=user, pswd=pswd)
    asyncore.loop()

if __name__ == '__main__':
    user, pswd, args = parse_args(sys.argv[1:])

    def cb(identifier, cmd, extra, key, vb, val, cas):
        print "%s: ``%s'' (vb:%d) -> ``%s'' (%d bytes from %s)" % (
            memcacheConstants.COMMAND_NAMES[cmd],
            key, vb, keyprint(val), len(val), identifier)

    # This is an example opts parameter to do future-only tap:
    opts = {memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff}
    # If you omit it, or supply a past time_t value for backfill, it
    # will get all data.
    opts = {}

    mainLoop(args, cb, opts, user, pswd)
