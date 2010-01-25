#!/usr/bin/env python
"""
tap protocol client.

Copyright (c) 2010  Dustin Sallings <dustin@spy.net>
"""

import sys
import socket
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

    def __init__(self, server, port, callback):
        mc_bin_server.MemcachedBinaryChannel.__init__(self, None, None,
                                                   self._createTapCall())
        self.callback = callback
        self.identifier = (server, port)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((server, port))

    def _createTapCall(self, key="", extraHeader="", val=""):
        dtype=0
        opaque=0
        cas=0
        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
                        memcacheConstants.CMD_TAP_CONNECT,
                        len(key), len(extraHeader), dtype,
                        len(key) + len(extraHeader) + len(val),
                        opaque, cas)
        return msg + extraHeader + key + val

    def processCommand(self, cmd, klen, cas, data):
        extralen = memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0)
        extra = data[0:extralen]
        key = data[extralen:(extralen+klen)]
        val = data[(extralen+klen):]
        self.callback(self.identifier, cmd, extra, key, val, cas)

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

class TapClient(object):

    def __init__(self, servers, callback):
        for (h,p) in servers:
            tc = TapConnection(h, p, callback)

if __name__ == '__main__':
    connections = (a.split(':') for a in sys.argv[1:])
    def cb(identifier, cmd, extra, key, val, cas):
        print "%s: ``%s'' -> ``%s'' (%d bytes from %s)" % (
            memcacheConstants.COMMAND_NAMES[cmd],
            key, val, len(val), identifier)
    TapClient(((h, int(p)) for (h,p) in connections), cb)
    asyncore.loop()
