#!/usr/bin/env python
"""
A memcached test server.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import asyncore
import random
import string
import socket
import struct
import time
import hmac
import heapq

import memcacheConstants

from memcacheConstants import MIN_RECV_PACKET, REQ_PKT_FMT, RES_PKT_FMT
from memcacheConstants import INCRDECR_RES_FMT
from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE, EXTRA_HDR_FMTS

VERSION="1.0"

class BaseBackend(object):
    """Higher-level backend (processes commands and stuff)."""

    # Command IDs to method names.  This is used to build a dispatch dict on
    # the fly.
    CMDS={
        memcacheConstants.CMD_GET: 'handle_get',
        memcacheConstants.CMD_GETQ: 'handle_getq',
        memcacheConstants.CMD_SET: 'handle_set',
        memcacheConstants.CMD_ADD: 'handle_add',
        memcacheConstants.CMD_REPLACE: 'handle_replace',
        memcacheConstants.CMD_DELETE: 'handle_delete',
        memcacheConstants.CMD_INCR: 'handle_incr',
        memcacheConstants.CMD_DECR: 'handle_decr',
        memcacheConstants.CMD_QUIT: 'handle_quit',
        memcacheConstants.CMD_FLUSH: 'handle_flush',
        memcacheConstants.CMD_NOOP: 'handle_noop',
        memcacheConstants.CMD_VERSION: 'handle_version',
        memcacheConstants.CMD_APPEND: 'handle_append',
        memcacheConstants.CMD_PREPEND: 'handle_prepend',
        memcacheConstants.CMD_SASL_LIST_MECHS: 'handle_sasl_mechs',
        memcacheConstants.CMD_SASL_AUTH: 'handle_sasl_auth',
        memcacheConstants.CMD_SASL_STEP: 'handle_sasl_step',
        }

    def __init__(self):
        self.handlers={}
        self.sched=[]

        for id, method in self.CMDS.iteritems():
            self.handlers[id]=getattr(self, method, self.handle_unknown)

    def _splitKeys(self, fmt, keylen, data):
        """Split the given data into the headers as specified in the given
        format, the key, and the data.

        Return (hdrTuple, key, data)"""
        hdrSize=struct.calcsize(fmt)
        assert hdrSize <= len(data), "Data too short for " + fmt + ': ' + `data`
        hdr=struct.unpack(fmt, data[:hdrSize])
        assert len(data) >= hdrSize + keylen
        key=data[hdrSize:keylen+hdrSize]
        assert len(key) == keylen, "len(%s) == %d, expected %d" \
            % (key, len(key), keylen)
        val=data[keylen+hdrSize:]
        return hdr, key, val

    def _error(self, which, msg):
        return which, 0, msg

    def processCommand(self, cmd, keylen, vb, cas, data):
        """Entry point for command processing.  Lower level protocol
        implementations deliver values here."""

        now=time.time()
        while self.sched and self.sched[0][0] <= now:
            print "Running delayed job."
            heapq.heappop(self.sched)[1]()

        hdrs, key, val=self._splitKeys(EXTRA_HDR_FMTS.get(cmd, ''),
            keylen, data)

        return self.handlers.get(cmd, self.handle_unknown)(cmd, hdrs, key,
            cas, val)

    def handle_noop(self, cmd, hdrs, key, cas, data):
        """Handle a noop"""
        print "Noop"
        return 0, 0, ''

    def handle_unknown(self, cmd, hdrs, key, cas, data):
        """invoked for any unknown command."""
        return self._error(memcacheConstants.ERR_UNKNOWN_CMD,
            "The command %d is unknown" % cmd)

class DictBackend(BaseBackend):
    """Sample backend implementation with a non-expiring dict."""

    def __init__(self):
        super(DictBackend, self).__init__()
        self.storage={}
        self.held_keys={}
        self.challenge = ''.join(random.sample(string.ascii_letters
                                               + string.digits, 32))

    def __lookup(self, key):
        rv=self.storage.get(key, None)
        if rv:
            now=time.time()
            if now >= rv[1]:
                print key, "expired"
                del self.storage[key]
                rv=None
        else:
            print "Miss looking up", key
        return rv

    def handle_get(self, cmd, hdrs, key, cas, data):
        val=self.__lookup(key)
        if val:
            rv = 0, id(val), struct.pack(
                memcacheConstants.GET_RES_FMT, val[0]) + str(val[2])
        else:
            rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        return rv

    def handle_set(self, cmd, hdrs, key, cas, data):
        print "Handling a set with", hdrs
        val=self.__lookup(key)
        exp, flags=hdrs
        def f(val):
            return self.__handle_unconditional_set(cmd, hdrs, key, data)
        return self._withCAS(key, cas, f)

    def handle_getq(self, cmd, hdrs, key, cas, data):
        rv=self.handle_get(cmd, hdrs, key, cas, data)
        if rv[0] == memcacheConstants.ERR_NOT_FOUND:
            print "Swallowing miss"
            rv = None
        return rv

    def __handle_unconditional_set(self, cmd, hdrs, key, data):
        exp=hdrs[1]
        # If it's going to expire soon, tell it to wait a while.
        if exp == 0:
            exp=float(2 ** 31)
        self.storage[key]=(hdrs[0], time.time() + exp, data)
        print "Stored", self.storage[key], "in", key
        if key in self.held_keys:
            del self.held_keys[key]
        return 0, id(self.storage[key]), ''

    def __mutation(self, cmd, hdrs, key, data, multiplier):
        amount, initial, expiration=hdrs
        rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        val=self.storage.get(key, None)
        print "Mutating %s, hdrs=%s, val=%s %s" % (key, `hdrs`, `val`,
            multiplier)
        if val:
            val = (val[0], val[1], max(0, long(val[2]) + (multiplier * amount)))
            self.storage[key]=val
            rv=0, id(val), str(val[2])
        else:
            if expiration != memcacheConstants.INCRDECR_SPECIAL:
                self.storage[key]=(0, time.time() + expiration, initial)
                rv=0, id(self.storage[key]), str(initial)
        if rv[0] == 0:
            rv = rv[0], rv[1], struct.pack(
                memcacheConstants.INCRDECR_RES_FMT, long(rv[2]))
        print "Returning", rv
        return rv

    def handle_incr(self, cmd, hdrs, key, cas, data):
        return self.__mutation(cmd, hdrs, key, data, 1)

    def handle_decr(self, cmd, hdrs, key, cas, data):
        return self.__mutation(cmd, hdrs, key, data, -1)

    def __has_hold(self, key):
        rv=False
        now=time.time()
        print "Looking for hold of", key, "in", self.held_keys, "as of", now
        if key in self.held_keys:
            if time.time() > self.held_keys[key]:
                del self.held_keys[key]
            else:
                rv=True
        return rv

    def handle_add(self, cmd, hdrs, key, cas, data):
        rv=self._error(memcacheConstants.ERR_EXISTS, 'Data exists for key')
        if key not in self.storage and not self.__has_hold(key):
            rv=self.__handle_unconditional_set(cmd, hdrs, key, data)
        return rv

    def handle_replace(self, cmd, hdrs, key, cas, data):
        rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        if key in self.storage and not self.__has_hold(key):
            rv=self.__handle_unconditional_set(cmd, hdrs, key, data)
        return rv

    def handle_flush(self, cmd, hdrs, key, cas, data):
        timebomb_delay=hdrs[0]
        def f():
            self.storage.clear()
            self.held_keys.clear()
            print "Flushed"
        if timebomb_delay:
            heapq.heappush(self.sched, (time.time() + timebomb_delay, f))
        else:
            f()
        return 0, 0, ''

    def handle_delete(self, cmd, hdrs, key, cas, data):
        def f(val):
            rv=self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
            if val:
                del self.storage[key]
                rv = 0, 0, ''
            print "Deleted", key, hdrs[0]
            if hdrs[0] > 0:
                self.held_keys[key] = time.time() + hdrs[0]
            return rv
        return self._withCAS(key, cas, f)

    def handle_version(self, cmd, hdrs, key, cas, data):
        return 0, 0, "Python test memcached server %s" % VERSION

    def _withCAS(self, key, cas, f):
        val=self.storage.get(key, None)
        if cas == 0 or (val and cas == id(val)):
            rv=f(val)
        elif val:
            rv = self._error(memcacheConstants.ERR_EXISTS, 'Exists')
        else:
            rv = self._error(memcacheConstants.ERR_NOT_FOUND, 'Not found')
        return rv

    def handle_prepend(self, cmd, hdrs, key, cas, data):
        def f(val):
            self.storage[key]=(val[0], val[1], data + val[2])
            return 0, id(self.storage[key]), ''
        return self._withCAS(key, cas, f)

    def handle_append(self, cmd, hdrs, key, cas, data):
        def f(val):
            self.storage[key]=(val[0], val[1], val[2] + data)
            return 0, id(self.storage[key]), ''
        return self._withCAS(key, cas, f)

    def handle_sasl_mechs(self, cmd, hdrs, key, cas, data):
        return 0, 0, 'PLAIN CRAM-MD5'

    def handle_sasl_step(self, cmd, hdrs, key, cas, data):
        assert key == 'CRAM-MD5'

        u, resp = data.split(' ', 1)
        expected = hmac.HMAC('testpass', self.challenge).hexdigest()

        if u == 'testuser' and resp == expected:
            print "Successful CRAM-MD5 auth."
            return 0, 0, 'OK'
        else:
            print "Errored a CRAM-MD5 auth."
            return self._error(memcacheConstants.ERR_AUTH, 'Auth error.')

    def _handle_sasl_auth_plain(self, data):
        foruser, user, passwd = data.split("\0")
        if user == 'testuser' and passwd == 'testpass':
            print "Successful plain auth"
            return 0, 0, "OK"
        else:
            print "Bad username/password:  %s/%s" % (user, passwd)
            return self._error(memcacheConstants.ERR_AUTH, 'Auth error.')

    def _handle_sasl_auth_cram_md5(self, data):
        assert data == ''
        print "Issuing %s as a CRAM-MD5 challenge." % self.challenge
        return memcacheConstants.ERR_AUTH_CONTINUE, 0, self.challenge

    def handle_sasl_auth(self, cmd, hdrs, key, cas, data):
        mech = key

        if mech == 'PLAIN':
            return self._handle_sasl_auth_plain(data)
        elif mech == 'CRAM-MD5':
            return self._handle_sasl_auth_cram_md5(data)
        else:
            print "Unhandled auth type:  %s" % mech
            return self._error(memcacheConstants.ERR_AUTH, 'Auth error.')

class MemcachedBinaryChannel(asyncore.dispatcher):
    """A channel implementing the binary protocol for memcached."""

    # Receive buffer size
    BUFFER_SIZE = 4096

    def __init__(self, channel, backend, wbuf=""):
        asyncore.dispatcher.__init__(self, channel)
        self.log_info("New bin connection from %s" % str(self.addr))
        self.backend=backend
        self.wbuf=wbuf
        self.rbuf=""

    def __hasEnoughBytes(self):
        rv=False
        if len(self.rbuf) >= MIN_RECV_PACKET:
            magic, cmd, keylen, extralen, datatype, vb, remaining, opaque, cas=\
                struct.unpack(REQ_PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            rv = len(self.rbuf) - MIN_RECV_PACKET >= remaining
        return rv

    def processCommand(self, cmd, keylen, vb, cas, data):
        return self.backend.processCommand(cmd, keylen, vb, cas, data)

    def handle_read(self):
        self.rbuf += self.recv(self.BUFFER_SIZE)
        while self.__hasEnoughBytes():
            magic, cmd, keylen, extralen, datatype, vb, remaining, opaque, cas=\
                struct.unpack(REQ_PKT_FMT, self.rbuf[:MIN_RECV_PACKET])
            assert magic == REQ_MAGIC_BYTE
            assert keylen <= remaining, "Keylen is too big: %d > %d" \
                % (keylen, remaining)
            assert extralen == memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0), \
                "Extralen is too large for cmd 0x%x: %d" % (cmd, extralen)
            # Grab the data section of this request
            data=self.rbuf[MIN_RECV_PACKET:MIN_RECV_PACKET+remaining]
            assert len(data) == remaining
            # Remove this request from the read buffer
            self.rbuf=self.rbuf[MIN_RECV_PACKET+remaining:]
            # Process the command
            cmdVal = self.processCommand(cmd, keylen, vb, extralen, cas, data)
            # Queue the response to the client if applicable.
            if cmdVal:
                try:
                    status, cas, response = cmdVal
                except ValueError:
                    print "Got", cmdVal
                    raise
                dtype=0
                extralen=memcacheConstants.EXTRA_HDR_SIZES.get(cmd, 0)
                self.wbuf += struct.pack(RES_PKT_FMT,
                    RES_MAGIC_BYTE, cmd, keylen,
                    extralen, dtype, status,
                    len(response), opaque, cas) + response

    def writable(self):
        return self.wbuf

    def handle_write(self):
        sent = self.send(self.wbuf)
        self.wbuf = self.wbuf[sent:]

    def handle_close(self):
        self.log_info("Disconnected from %s" % str(self.addr))
        self.close()

class MemcachedServer(asyncore.dispatcher):
    """A memcached server."""
    def __init__(self, backend, handler, port=11211):
        asyncore.dispatcher.__init__(self)

        self.handler=handler
        self.backend=backend

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(("", port))
        self.listen(5)
        self.log_info("Listening on %d" % port)

    def handle_accept(self):
        channel, addr = self.accept()
        self.handler(channel, self.backend)

if __name__ == '__main__':
    port = 11211
    import sys
    if sys.argv > 1:
        port = int(sys.argv[1])
    server = MemcachedServer(DictBackend(), MemcachedBinaryChannel, port=port)
    asyncore.loop()
