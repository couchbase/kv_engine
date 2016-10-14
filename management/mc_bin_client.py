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
import select

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
from memcacheConstants import TOUCH_PKT_FMT, GAT_PKT_FMT, GETL_PKT_FMT
from memcacheConstants import COMPACT_DB_PKT_FMT
import memcacheConstants

class TimeoutError(exceptions.Exception):
    def __init__(self, time):
        exceptions.Exception.__init__(self, "Operation timed out")
        self.time = time

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        str = 'Error: Operation timed out (%d seconds)\n' % self.time
        str += 'Please check list of arguments (e.g., IP address, port number) '
        str += 'passed or the connectivity to a server to be connected'
        return str

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

    def __init__(self, host='127.0.0.1', port=11211, family=socket.AF_INET):
        self.host = host
        self.port = port
        self.s=socket.socket(family, socket.SOCK_STREAM)
        if hasattr(socket, 'AF_UNIX') and family == socket.AF_UNIX:
            self.s.connect_ex(host)
        else:
            self.s.connect_ex((host, port))
        self.s.setblocking(0)
        self.r=random.Random()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE):
        msg=struct.pack(fmt, magic,
            cmd, len(key), len(extraHeader), dtype, vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.send(msg + extraHeader + key + val)

    def _socketRecv(self, amount):
        ready = select.select([self.s], [], [], 30)
        if ready[0]:
            return self.s.recv(amount)
        raise TimeoutError(30)

    def _recvMsg(self):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            data = self._socketRecv(MIN_RECV_PACKET - len(response))
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            response += data
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas=\
            struct.unpack(RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            data = self._socketRecv(remaining)
            if data == '':
                raise exceptions.EOFError("Got empty data (remote died?).")
            rv += data
            remaining -= len(data)

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, rv

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, rv = self._recvMsg()
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

    def _doMetaCmd(self, cmd, key, value, cas, exp, flags, seqno, remote_cas):
        extra = struct.pack('>IIQQ', flags, exp, seqno, remote_cas)
        return self._doCmd(cmd, key, value, extra, cas)

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def setWithMeta(self, key, value, exp, flags, seqno, remote_cas):
        """Set a value and its meta data in the memcached server."""
        return self._doMetaCmd(memcacheConstants.CMD_SET_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def addWithMeta(self, key, value, exp, flags, seqno, remote_cas):
        return self._doMetaCmd(memcacheConstants.CMD_ADD_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val)

    def observe(self, key, vbucket):
        """Observe a key for persistence and replication."""
        value = struct.pack('>HH', vbucket, len(key)) + key
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_OBSERVE, '', value)
        rep_time = (cas & 0xFFFFFFFF)
        persist_time =  (cas >> 32) & 0xFFFFFFFF
        persisted = struct.unpack('>B', data[4+len(key)])[0]
        return opaque, rep_time, persist_time, persisted

    def __parseGet(self, data, klen=0):
        flags=struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '')
        return self.__parseGet(parts)

    def getMeta(self, key):
        """Get the metadata for a given key within the memcached server."""
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_GET_META, key, '')
        deleted = struct.unpack('>I', data[0:4])[0]
        flags = struct.unpack('>I', data[4:8])[0]
        exp = struct.unpack('>I', data[8:12])[0]
        seqno = struct.unpack('>Q', data[12:20])[0]
        return (deleted, flags, exp, seqno, cas)

    def getl(self, key, exp=15):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
            struct.pack(memcacheConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(parts)

    def cas(self, key, exp, flags, oldVal, val):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val)

    def touch(self, key, exp):
        """Touch a key in the memcached server."""
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
            struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key, exp):
        """Get the value for a given key and touch it within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GAT, key, '',
            struct.pack(memcacheConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(parts)

    def getr(self, key):
        """Get the value for a given key in a replica vbucket within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET_REPLICA, key, '')
        return self.__parseGet(parts, len(key))

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def verbose(self, level):
        """Set the verbosity level."""
        return self._doCmd(memcacheConstants.CMD_VERBOSE, '', '',
                           extraHeader=struct.pack(">I", level))

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

    def set_param(self, vbucket, key, val, type):
        print "setting param:", key, val
        self.vbucketId = vbucket
        type = struct.pack(memcacheConstants.SET_PARAM_FMT, type)
        return self._doCmd(memcacheConstants.CMD_SET_PARAM, key, val, type)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(memcacheConstants.VB_SET_PKT_FMT,
                            memcacheConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE, '', '',
                           state)

    def compact_db(self, vbucket, purgeBeforeTs, purgeBeforeSeq, dropDeletes):
        assert isinstance(vbucket, int)
        assert isinstance(purgeBeforeTs, long)
        assert isinstance(purgeBeforeSeq, long)
        assert isinstance(dropDeletes, int)
        self.vbucketId = vbucket
        compact = struct.pack(memcacheConstants.COMPACT_DB_PKT_FMT,
                            purgeBeforeTs, purgeBeforeSeq, dropDeletes)
        return self._doCmd(memcacheConstants.CMD_COMPACT_DB, '', '',
                           compact)

    def get_vbucket_state(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE, '', '')

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

    def setMulti(self, exp, flags, items):
        """Multi-set (using setq).

        Give me (key, value) pairs."""

        # If this is a dict, convert it to a pair generator
        if hasattr(items, 'iteritems'):
            items = items.iteritems()

        opaqued=dict(enumerate(items))
        terminal=len(opaqued)+10
        extra=struct.pack(SET_PKT_FMT, flags, exp)

        # Send all of the keys in quiet
        for opaque,kv in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_SETQ, kv[0], kv[1], opaque, extra)

        self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        failed = []
        done=False
        while not done:
            try:
                opaque, cas, data = self._handleSingleResponse(None)
                done = opaque == terminal
            except MemcachedError, e:
                failed.append(e)

        return failed

    def delMulti(self, items):
        """Multi-delete (using delq).

        Give me a collection of keys."""

        opaqued = dict(enumerate(items))
        terminal = len(opaqued)+10
        extra = ''

        # Send all of the keys in quiet
        for opaque, k in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_DELETEQ, k, '', opaque, extra)

        self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        failed = []
        done=False
        while not done:
            try:
                opaque, cas, data = self._handleSingleResponse(None)
                done = opaque == terminal
            except MemcachedError, e:
                failed.append(e)

        return failed

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

    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(memcacheConstants.CMD_DEREGISTER_TAP_CLIENT, tap_name, '', '', 0)

    def reset_replication_chain(self):
        """Reset the replication chain."""
        return self._doCmd(memcacheConstants.CMD_RESET_REPLICATION_CHAIN, '', '', '', 0)

    def list_buckets(self):
        """Get the name of all buckets."""
        opaque, cas, data = self._doCmd(
            memcacheConstants.CMD_LIST_BUCKETS, '', '', '', 0)
        return data.strip().split(' ')
