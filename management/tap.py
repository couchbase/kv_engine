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
                        len(key), len(extraHeader), dtype,
                        len(key) + len(extraHeader) + len(val),
                        opaque, cas)
        return msg + extraHeader + key + val

    def _encodeOpts(self, opts):
        header = 0
        val = []
        for op in sorted(opts.keys()):
            header |= op
            if memcacheConstants.TAP_FLAG_TYPES[op]:
                val.append(struct.pack(memcacheConstants.TAP_FLAG_TYPES[op],
                                       opts[op]))
        return struct.pack(">I", header), ''.join(val)

    def processCommand(self, cmd, klen, extralen, cas, data):
        extra = data[0:extralen]
        key = data[extralen:(extralen+klen)]
        val = data[(extralen+klen):]
        self.callback(self.identifier, cmd, extra, key, val, cas)

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

class TapClient(object):

    def __init__(self, servers, callback, clientId=None, opts={}):
        for (h,p) in servers:
            tc = TapConnection(h, p, callback, clientId, opts)

def abbrev(v, maxlen=30):
    if len(v) > maxlen:
        return v[:maxlen] + "..."
    else:
        return v

if __name__ == '__main__':
    connections = (a.split(':') for a in sys.argv[1:])
    def cb(identifier, cmd, extra, key, val, cas):
        print "%s: ``%s'' -> ``%s'' (%d bytes from %s)" % (
            memcacheConstants.COMMAND_NAMES[cmd],
            key, abbrev(val), len(val), identifier)

    # This is an example opts parameter to do future-only tap:
    opts = {memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff}
    # If you omit it, or supply a past time_t value for backfill, it
    # will get all data.
    opts = {}

    TapClient(((h, int(p)) for (h,p) in connections), cb, opts=opts)
    asyncore.loop()
