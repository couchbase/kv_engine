#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import array
import exceptions
import hmac
import json
import random
import re
import select
import socket
import struct
import sys
import time

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, DEL_PKT_FMT, INCRDECR_RES_FMT
from memcacheConstants import TOUCH_PKT_FMT, GAT_PKT_FMT, GETL_PKT_FMT
from memcacheConstants import COMPACT_DB_PKT_FMT
import memcacheConstants

def parse_address(addr):
    """Parse a host string with optional port number into a
    (host, port, family) triple."""

    # Sane defaults
    family = socket.AF_UNSPEC
    port = 11210
    # Is this IPv6?
    if addr.startswith('['):
        matches = re.match(r'^\[([^\]]+)\](:(\d+))?$', addr)
        family = socket.AF_INET6
    else:
        matches = re.match(r'^([^:]+)(:(\d+))?$', addr)
    if matches:
        # The host is the first group
        host = matches.group(1)
        # Optional port is the 3rd group
        if matches.group(3):
            port = int(matches.group(3))
    else:
        raise Exception("Invalid format for host string: '{}'".format(addr))

    return host, port, family

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


# This metaclass causes any instantiation of MemcachedError to transparently
# construct the appropriate subclass if the error code is known.
class MemcachedErrorMetaclass(type):
    _dispatch = {}
    def __new__(meta, name, bases, dct):
        cls = (super(MemcachedErrorMetaclass, meta)
               .__new__(meta, name, bases, dct))
        if 'ERRCODE' in dct:
            meta._dispatch[dct['ERRCODE']] = cls
        return cls

    def __call__(cls, *args, **kwargs):
        err = None
        if 'status' in kwargs:
            err = kwargs['status']
        elif len(args):
            err = args[0]

        if err in cls._dispatch:
            cls = cls._dispatch[err]

        return (super(MemcachedErrorMetaclass, cls)
                .__call__(*args, **kwargs))

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    __metaclass__ = MemcachedErrorMetaclass

    def __init__(self, status, msg):
        supermsg='Memcached error #' + repr(status)
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status=status
        self.msg=msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)

class ErrorKeyEnoent(MemcachedError): ERRCODE = 0x1
class ErrorKeyEexists(MemcachedError): ERRCODE = 0x2
class ErrorE2big(MemcachedError): ERRCODE = 0x3
class ErrorEinval(MemcachedError): ERRCODE = 0x4
class ErrorNotStored(MemcachedError): ERRCODE = 0x5
class ErrorDeltaBadval(MemcachedError): ERRCODE = 0x6
class ErrorNotMyVbucket(MemcachedError): ERRCODE = 0x7
class ErrorNoBucket(MemcachedError): ERRCODE = 0x8
class ErrorLocked(MemcachedError): ERRCODE = 0x9
class ErrorAuthStale(MemcachedError): ERRCODE = 0x1f
class ErrorAuthError(MemcachedError): ERRCODE = 0x20
class ErrorAuthContinue(MemcachedError): ERRCODE = 0x21
class ErrorErange(MemcachedError): ERRCODE = 0x22
class ErrorRollback(MemcachedError): ERRCODE = 0x23
class ErrorEaccess(MemcachedError): ERRCODE = 0x24
class ErrorNotInitialized(MemcachedError): ERRCODE = 0x25
class ErrorUnknownCommand(MemcachedError): ERRCODE = 0x81
class ErrorEnomem(MemcachedError): ERRCODE = 0x82
class ErrorNotSupported(MemcachedError): ERRCODE = 0x83
class ErrorEinternal(MemcachedError): ERRCODE = 0x84
class ErrorEbusy(MemcachedError): ERRCODE = 0x85
class ErrorEtmpfail(MemcachedError): ERRCODE = 0x86
class ErrorXattrEinval(MemcachedError): ERRCODE = 0x87
class ErrorUnknownCollection(MemcachedError): ERRCODE = 0x88
class ErrorNoCollectionManifest(MemcachedError): ERRCODE = 0x89
class ErrorSubdocPathEnoent(MemcachedError): ERRCODE = 0xc0
class ErrorSubdocPathMismatch(MemcachedError): ERRCODE = 0xc1
class ErrorSubdocPathEinval(MemcachedError): ERRCODE = 0xc2
class ErrorSubdocPathE2big(MemcachedError): ERRCODE = 0xc3
class ErrorSubdocPathE2deep(MemcachedError): ERRCODE = 0xc4
class ErrorSubdocValueCantinsert(MemcachedError): ERRCODE = 0xc5
class ErrorSubdocDocNotjson(MemcachedError): ERRCODE = 0xc6
class ErrorSubdocNumErange(MemcachedError): ERRCODE = 0xc7
class ErrorSubdocDeltaEinval(MemcachedError): ERRCODE = 0xc8
class ErrorSubdocPathEexists(MemcachedError): ERRCODE = 0xc9
class ErrorSubdocValueEtoodeep(MemcachedError): ERRCODE = 0xca
class ErrorSubdocInvalidCombo(MemcachedError): ERRCODE = 0xcb
class ErrorSubdocMultiPathFailure(MemcachedError): ERRCODE = 0xcc
class ErrorSubdocSuccessDeleted(MemcachedError): ERRCODE = 0xcd
class ErrorSubdocXattrInvalidFlagCombo(MemcachedError): ERRCODE = 0xce
class ErrorSubdocXattrInvalidKeyCombo(MemcachedError): ERRCODE = 0xcf
class ErrorSubdocXattrUnknownMacro(MemcachedError): ERRCODE = 0xd0

class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211, family=socket.AF_UNSPEC):
        self.host = host
        self.port = port

        # Iterate all addresses for the given family; using the first
        # one we can successfully connect to. If none succeed raise
        # the last attempted as an exception.
        for info in socket.getaddrinfo(self.host, self.port, family,
                                       socket.SOCK_STREAM):
            _family, socktype, proto, _, sockaddr = info
            try:
                sock = socket.socket(_family, socktype, proto)
                sock.settimeout(10)
                sock.connect(sockaddr)
                self.s = sock
                break
            except socket.error as sock_error:
                # If we get here socket objects will be close()d via
                # garbage collection.
                pass
        else:
            # Didn't break from the loop, re-raise the last error
            raise sock_error

        self.s.setblocking(0)
        self.r=random.Random()
        self.req_features = set()
        self.features = set()
        self.error_map = None
        self.error_map_version = 1
        self.collection_map = {}

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0, collection=None):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId, collection=collection)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE, collection=None):
        if collection:
            key = self._encodeCollectionId(key, collection)

        msg=struct.pack(fmt, magic,
            cmd, len(key), len(extraHeader), dtype, vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        self.s.sendall(msg + extraHeader + key + val)

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
            if self.error_map is None:
                msg = rv
            else:
                err = self.error_map['errors'].get(errcode, rv)
                msg = "{name} : {desc} : {rv}".format(rv=rv, **err)

            raise MemcachedError(errcode,  msg)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0, collection=None):
        """Send a command and await its response."""
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas, collection)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val, collection):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas, collection)

    def _cat(self, cmd, key, cas, val, collection):
        return self._doCmd(cmd, key, val, '', cas, collection)

    def hello(self, name):
        resp = self._doCmd(memcacheConstants.CMD_HELLO, name,
                           struct.pack('>' + 'H' * len(self.req_features),
                                       *self.req_features))
        supported = resp[2]
        for i in range(0, len(supported), struct.calcsize(">H")):
            self.features.update(
                struct.unpack_from(">H", supported, i))

        if self.is_xerror_supported():
            self.error_map = self.get_error_map()

        return resp

    def append(self, key, value, cas=0, collection=None):
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value, collection)

    def prepend(self, key, value, cas=0, collection=None):
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value, collection)

    def __incrdecr(self, cmd, key, amt, init, exp, collection):
        something, cas, val=self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp),
            collection=collection)
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0, collection=None):
        """Increment or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp, collection)

    def decr(self, key, amt=1, init=0, exp=0, collection=None):
        """Decrement or create the named counter."""
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp, collection)

    def _doMetaCmd(self, cmd, key, value, cas, exp, flags, seqno, remote_cas, collection):
        extra = struct.pack('>IIQQ', flags, exp, seqno, remote_cas)
        return self._doCmd(cmd, key, value, extra, cas, collection)

    def set(self, key, exp, flags, val, collection=None):
        """Set a value in the memcached server."""
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val, collection)

    def setWithMeta(self, key, value, exp, flags, seqno, remote_cas, collection=None):
        """Set a value and its meta data in the memcached server."""
        return self._doMetaCmd(memcacheConstants.CMD_SET_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas, collection)

    def delWithMeta(self, key, exp, flags, seqno, remote_cas, collection=None):
        return self._doMetaCmd(memcacheConstants.CMD_DELETE_WITH_META,
                               key, '', 0, exp, flags, seqno, remote_cas, collection)

    def add(self, key, exp, flags, val, collection=None):
        """Add a value in the memcached server iff it doesn't already exist."""
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val, collection)

    def addWithMeta(self, key, value, exp, flags, seqno, remote_cas, collection=None):
        return self._doMetaCmd(memcacheConstants.CMD_ADD_WITH_META,
                               key, value, 0, exp, flags, seqno, remote_cas, collection)

    def replace(self, key, exp, flags, val, collection=None):
        """Replace a value in the memcached server iff it already exists."""
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val, collection)

    def observe(self, key, vbucket, collection=None):
        """Observe a key for persistence and replication."""
        value = struct.pack('>HH', vbucket, len(key)) + key
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_OBSERVE, '', value, collection=collection)
        rep_time = (cas & 0xFFFFFFFF)
        persist_time =  (cas >> 32) & 0xFFFFFFFF
        persisted = struct.unpack('>B', data[4+len(key)])[0]
        return opaque, rep_time, persist_time, persisted

    def __parseGet(self, data, klen=0):
        flags=struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, collection=None):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET, key, '', collection=collection)
        return self.__parseGet(parts)

    def getMeta(self, key, collection=None):
        """Get the metadata for a given key within the memcached server."""
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_GET_META, key, '', collection=collection)
        deleted = struct.unpack('>I', data[0:4])[0]
        flags = struct.unpack('>I', data[4:8])[0]
        exp = struct.unpack('>I', data[8:12])[0]
        seqno = struct.unpack('>Q', data[12:20])[0]
        return (deleted, flags, exp, seqno, cas)

    def getl(self, key, exp=15, collection=None):
        """Get the value for a given key within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
            struct.pack(memcacheConstants.GETL_PKT_FMT, exp), collection=collection)
        return self.__parseGet(parts)

    def cas(self, key, exp, flags, oldVal, val, collection=None):
        """CAS in a new value for the given key and comparison value."""
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val, collection)

    def touch(self, key, exp, collection=None):
        """Touch a key in the memcached server."""
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
            struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp), collection=collection)

    def gat(self, key, exp, collection=None):
        """Get the value for a given key and touch it within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GAT, key, '',
            struct.pack(memcacheConstants.GAT_PKT_FMT, exp), collection=collection)
        return self.__parseGet(parts)

    def getr(self, key, collection=None):
        """Get the value for a given key in a replica vbucket within the memcached server."""
        parts=self._doCmd(memcacheConstants.CMD_GET_REPLICA, key, '', collection=collection)
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

    def evict_key(self, key, collection=None):
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '', collection=collection)

    def getMulti(self, keys, collection=None):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued=dict(enumerate(keys))
        terminal=len(opaqued)+10
        # Send all of the keys in quiet
        for k,v in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k, collection=collection)

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

    def setMulti(self, exp, flags, items, collection=None):
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
            self._sendCmd(memcacheConstants.CMD_SETQ, kv[0], kv[1], opaque, extra, collection=collection)

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

    def delMulti(self, items, collection=None):
        """Multi-delete (using delq).

        Give me a collection of keys."""

        opaqued = dict(enumerate(items))
        terminal = len(opaqued)+10
        extra = ''

        # Send all of the keys in quiet
        for opaque, k in opaqued.iteritems():
            self._sendCmd(memcacheConstants.CMD_DELETEQ, k, '', opaque, extra, collection=collection)

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

    def get_random_key(self):
        opaque=self.r.randint(0, 2**32)
        self._sendCmd(memcacheConstants.CMD_GET_RANDOM_KEY, '', '', opaque)
        cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
        rv = {}
        if klen:
            rv[data[4:klen+4]] = data[klen:]
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, collection=None):
        """Delete the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas, collection=collection)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def list_buckets(self):
        """Get the name of all buckets."""
        opaque, cas, data = self._doCmd(
            memcacheConstants.CMD_LIST_BUCKETS, '', '', '', 0)
        return data.strip().split(' ')

    def get_error_map(self):
        _, _, errmap = self._doCmd(memcacheConstants.CMD_GET_ERROR_MAP, '',
                    struct.pack("!H", self.error_map_version))

        errmap = json.loads(errmap)

        d = {}

        for k,v in errmap['errors'].iteritems():
            d[int(k, 16)] = v

        errmap['errors'] = d
        return errmap

    def set_collections(self, manifest):
        rv = self._doCmd(memcacheConstants.CMD_COLLECTIONS_SET_MANIFEST,
                         '',
                         manifest)

        # Load up the collection map from the JSON
        self._update_collection_map(manifest)

    def get_collections(self, update_map=False):
        rv = self._doCmd(memcacheConstants.CMD_COLLECTIONS_GET_MANIFEST,
                         '',
                         '')

        if update_map:
            self._update_collection_map(rv[2])
        return rv

    def enable_xerror(self):
        self.req_features.add(memcacheConstants.FEATURE_XERROR)

    def enable_collections(self):
        self.req_features.add(memcacheConstants.FEATURE_COLLECTIONS)

    def is_xerror_supported(self):
        return memcacheConstants.FEATURE_XERROR in self.features

    def is_collections_supported(self):
        return memcacheConstants.FEATURE_COLLECTIONS in self.features

    # Collections on the wire uses a varint encoding for the collection-ID
    # A simple unsigned_leb128 encoded is used:
    #    https://en.wikipedia.org/wiki/LEB128
    # @return a string with the binary encoding
    def _encodeCollectionId(self, key, collection):
        if not self.is_collections_supported():
                raise exceptions.RuntimeError("Collections are not enabled")

        if type(collection) == str:
            try:
                collection = self.collection_map[collection]
            except KeyError, e:
                print("Error: cannot map collection \"{}\" to an ID".format(collection))
                raise e

        output = array.array('B', [0])
        while collection > 0:
            byte = collection & 0xFF
            collection >>= 7
            # collection has more bits?
            if collection > 0:
                # Set the 'continue' bit of this byte
                byte |= 0x80
                output[-1] = byte
                output.append(0)
            else:
                output[-1] = byte
        return output.tostring() + key

    def _update_collection_map(self, manifest):
        self.collection_map = {}
        parsed = json.loads(manifest)
        for e in parsed['collections']:
            self.collection_map[e[u'name']] = int(e[u'uid'])