#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import sys
import time
import hmac
import socket
import random
import struct
import exceptions

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
import memcacheConstants

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg='Memcached error #' + `status`
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status=status
        self.msg=msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)

class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211):
        self.host = host
        self.port = port
        self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect_ex((host, port))
        self.r=random.Random()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        dtype=0
        msg=struct.pack(REQ_PKT_FMT, REQ_MAGIC_BYTE,
            cmd, len(key), len(extraHeader), dtype, self.vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.send(msg + extraHeader + key + val)

    def _handleKeyedResponse(self, myopaque):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            data = self.s.recv(MIN_RECV_PACKET - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            response += data
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self.s.recv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            rv += data
            remaining -= len(data)

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        assert myopaque is None or opaque == myopaque, \
            "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode != 0:
            raise MemcachedError(errcode,  rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0):
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val=self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0):
        """Increment or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0):
        """Decrement or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp)

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val)

    def __parseGet(self, data, klen=0):
        flags=struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '')
        return self.__parseGet(parts)

    def getl(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '')
        return self.__parseGet(parts, len(key))

    def cas(self, key, exp, flags, oldVal, val):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val)

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(memcacheConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(memcacheConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        print "setting flush param:", key, val
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val)

    def stop_replication(self):
        return self._doCmd(memcacheConstants.CMD_STOP_REPLICATION, '', '')

    def start_replication(self):
        return self._doCmd(memcacheConstants.CMD_START_REPLICATION, '', '')

    def set_tap_param(self, key, val):
        print "setting tap param:", key, val
        return self._doCmd(memcacheConstants.CMD_SET_TAP_PARAM, key, val)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(memcacheConstants.VB_SET_PKT_FMT,
                            memcacheConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE, '',
                           state)

    def get_vbucket_state(self, vbucket):
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE,
                           str(vbucket), '')

    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key):
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '')

    def getMulti(self, keys):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued=dict(enumerate(keys))
        terminal=len(opaqued)+10
        # Send all of the keys in quiet
        for k,v in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k)

        self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv={}
        done=False
        while not done:
            opaque, cas, data=self._handleSingleResponse(None)
            if opaque != terminal:
                rv[opaqued[opaque]]=self.__parseGet((opaque, cas, data))
            else:
                done=True

        return rv

    def stats(self, sub=''):
        """Get stats."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0):
        """Delete the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def persistence_sync(self, keys):
        keyspec_list = []
        payload = struct.pack(">H", len(keys))

        for keyspec in keys:
            key = ""
            vbucketid = 0
            if isinstance(keyspec, tuple):
                (key, vbucketid) = keyspec
            else:
                key = keyspec
            payload += struct.pack(">H", len(key))
            payload += key
            payload += struct.pack(">H", vbucketid)
            keyspec_list.append( (key, vbucketid) )

        print "sending sync command for keys:", keyspec_list
        return self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
