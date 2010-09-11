#!/usr/bin/env python
"""
tap protocol client.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
import string
import random
import struct
import asyncore
import exceptions

import mc_bin_server

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
import memcacheConstants

class TapConnection(mc_bin_server.MemcachedBinaryChannel):

    def __init__(self, server, port, callback, clientId=None, opts={}):
        mc_bin_server.MemcachedBinaryChannel.__init__(self, None, None,
                                                      self._createTapCall(clientId,
                                                                          opts))
        self.callback = callback
        self.identifier = (server, port)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((server, port))

    def _createTapCall(self, key=None, opts={}):
        # Client identifier
        if not key:
            key = "".join(random.sample(string.letters, 16))
        dtype=0
        opaque=0
        cas=0

        extraHeader, val = self._encodeOpts(opts)

        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
                        memcacheConstants.CMD_TAP_CONNECT,
                        len(key), len(extraHeader), dtype, 0,
                        len(key) + len(extraHeader) + len(val),
                        opaque, cas)
        return msg + extraHeader + key + val

    def _encodeOpts(self, opts):
        header = 0
        val = []
        for op in sorted(opts.keys()):
            header |= op
            if op in memcacheConstants.TAP_FLAG_TYPES:
                val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                       opts[op]))
            elif op == memcacheConstants.TAP_FLAG_LIST_VBUCKETS:
                val.append(self._encodeVBucketList(opts[op]))
            else:
                val.append(opts[op])
        return struct.pack(">I", header), ''.join(val)

    def _encodeVBucketList(self, vbl):
        l = list(vbl) # in case it's a generator
        vals = [struct.pack("!H", len(l))]
        for v in vbl:
            vals.append(struct.pack("!H", v))
        return ''.join(vals)

    def processCommand(self, cmd, klen, vb, extralen, cas, data):
        extra = data[0:extralen]
        key = data[extralen:(extralen+klen)]
        val = data[(extralen+klen):]
        self.callback(self.identifier, cmd, extra, key, vb, val, cas)

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

class TapClient(object):

    def __init__(self, servers, callback, opts={}):
        for t in servers:
            tc = TapConnection(t.host, t.port, callback, t.id, opts)

def buildGoodSet(goodChars=string.printable, badChar='?'):
    """Build a translation table that turns all characters not in goodChars
    to badChar"""
    allChars=string.maketrans("", "")
    badchars=string.translate(allChars, allChars, goodChars)
    rv=string.maketrans(badchars, badChar * len(badchars))
    return rv

class TapDescriptor(object):
    port = 11211
    id = None

    def __init__(self, s):
        self.host = s
        if ':' in s:
            self.host, self.port = s.split(':', 1)
            self.port = int(self.port)

        if '@' in self.host:
            self.id, self.host = self.host.split('@', 1)

# Build a translation table that includes only characters
transt=buildGoodSet()

def abbrev(v, maxlen=30):
    if len(v) > maxlen:
        return v[:maxlen] + "..."
    else:
        return v

def keyprint(v):
    return string.translate(abbrev(v), transt)

def mainLoop(serverList, cb, opts={}):
    """Run the given callback for each tap message from any of the
    upstream servers.

    loops until all connections drop
    """

    connections = (TapDescriptor(a) for a in serverList)
    TapClient(connections, cb, opts=opts)
    asyncore.loop()

if __name__ == '__main__':
    def cb(identifier, cmd, extra, key, vb, val, cas):
        print "%s: ``%s'' (vb:%d) -> ``%s'' (%d bytes from %s)" % (
            memcacheConstants.COMMAND_NAMES[cmd],
            key, vb, keyprint(val), len(val), identifier)

    # This is an example opts parameter to do future-only tap:
    opts = {memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff}
    # If you omit it, or supply a past time_t value for backfill, it
    # will get all data.
    opts = {}

    mainLoop(sys.argv[1:], cb, opts)
