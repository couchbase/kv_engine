#!/usr/bin/env python3
"""

Portions Copyright (c) 2009-Present Couchbase
Portions Copyright (c) 2007  Dustin Sallings <dustin@spy.net>

Use of this software is governed by the Apache License, Version 2.0, included
in the file licenses/APL2.txt.

"""

import struct

# Command constants
CMD_GET = 0
CMD_SET = 1
CMD_ADD = 2
CMD_REPLACE = 3
CMD_DELETE = 4
CMD_INCR = 5
CMD_DECR = 6
CMD_QUIT = 7
CMD_FLUSH = 8
CMD_GETQ = 9
CMD_NOOP = 10
CMD_VERSION = 11
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f
CMD_STAT = 0x10
CMD_SETQ = 0x11
CMD_DELETEQ = 0x14
CMD_VERBOSE = 0x1b
CMD_TOUCH = 0x1c
CMD_GAT = 0x1d
CMD_HELLO = 0x1f
CMD_GET_REPLICA = 0x83
CMD_OBSERVE = 0x92

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Bucket extension
CMD_CREATE_BUCKET = 0x85
CMD_DELETE_BUCKET = 0x86
CMD_LIST_BUCKETS = 0x87
CMD_EXPAND_BUCKET = 0x88
CMD_SELECT_BUCKET = 0x89

CMD_STOP_PERSISTENCE = 0x80
CMD_START_PERSISTENCE = 0x81
CMD_SET_PARAM = 0x82

CMD_EVICT_KEY = 0x93

CMD_GET_META = 0xa0
CMD_GETQ_META = 0xa1

CMD_SET_WITH_META = 0xa2
CMD_SETQ_WITH_META = 0xa3

CMD_ADD_WITH_META = 0xa4
CMD_ADDQ_WITH_META = 0xa5

CMD_DELETE_WITH_META = 0xa8
CMD_DELETEQ_WITH_META = 0xa9

# vbucket stuff
CMD_SET_VBUCKET_STATE = 0x3d
CMD_GET_VBUCKET_STATE = 0x3e
CMD_DELETE_VBUCKET = 0x3f

CMD_GET_LOCKED = 0x94
CMD_COMPACT_DB = 0xb3
CMD_GET_RANDOM_KEY = 0xb6

# Collections
CMD_COLLECTIONS_SET_MANIFEST = 0xb9
CMD_COLLECTIONS_GET_MANIFEST = 0xba
CMD_COLLECTIONS_GET_ID = 0xbb
CMD_COLLECTIONS_GET_SCOPE_ID = 0xbc

CMD_PREPARE_SNAPSHOT = 0xe0
CMD_RELEASE_SNAPSHOT = 0xe1
CMD_DOWNLOAD_SNAPSHOT = 0xe2
CMD_GET_FILE_FRAGMENT = 0xe3

CMD_GET_ERROR_MAP = 0xfe

# event IDs for the SYNC command responses
CMD_SYNC_EVENT_PERSISTED = 1
CMD_SYNC_EVENT_MODIFED = 2
CMD_SYNC_EVENT_DELETED = 3
CMD_SYNC_EVENT_REPLICATED = 4
CMD_SYNC_INVALID_KEY = 5
CMD_SYNC_INVALID_CAS = 6

VB_STATE_ACTIVE = 1
VB_STATE_REPLICA = 2
VB_STATE_PENDING = 3
VB_STATE_DEAD = 4
VB_STATE_NAMES = {'active': VB_STATE_ACTIVE,
                  'replica': VB_STATE_REPLICA,
                  'pending': VB_STATE_PENDING,
                  'dead': VB_STATE_DEAD}

# Parameter types of CMD_SET_PARAM command.
ENGINE_PARAM_FLUSH = 1
ENGINE_PARAM_REPLICATION = 2
ENGINE_PARAM_CHECKPOINT = 3
ENGINE_PARAM_DCP = 4
ENGINE_PARAM_VBUCKET = 5


COMMAND_NAMES = dict(((globals()[k], k)
                     for k in globals() if k.startswith("CMD_")))

# Enableable features
FEATURE_DATATYPE = 0x01
FEATURE_TLS = 0x2
FEATURE_TCPNODELAY = 0x03
FEATURE_MUTATION_SEQNO = 0x04
FEATURE_TCPDELAY = 0x05
FEATURE_XATTR = 0x06
FEATURE_XERROR = 0x07
FEATURE_SELECT_BUCKET = 0x08
FEATURE_JSON = 0x0b
FEATURE_TRACING = 0x0f
FEATURE_COLLECTIONS = 0x12

# Flags, expiration
SET_PKT_FMT = ">II"

# flags
GET_RES_FMT = ">I"

# How long until the deletion takes effect.
DEL_PKT_FMT = ""

# amount, initial value, expiration
INCRDECR_PKT_FMT = ">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL = 0xffffffff
INCRDECR_RES_FMT = ">Q"

# Time bomb
FLUSH_PKT_FMT = ">I"

# Touch commands
# expiration
TOUCH_PKT_FMT = ">I"
GAT_PKT_FMT = ">I"
GETL_PKT_FMT = ">I"

# set param command
SET_PARAM_FMT = ">I"

# 2 bit integer.  :/
VB_SET_PKT_FMT = ">I"

# 8b: purge_before_ts, purge_before_seq, 1b: drop_deletes, spare1, 2b: spare2
COMPACT_DB_PKT_FMT = ">QQBxxxxxxx"

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
ALT_REQ_MAGIC_BYTE = 0x08
RES_MAGIC_BYTE = 0x81
ALT_RES_MAGIC_BYTE = 0x18

# magic, opcode, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
REQ_PKT_FMT = ">BBHBBHIIQ"
# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT = ">BBHBBHIIQ"
# magic, opcode, frame_extra_len, keylen, extralen, datatype, status, bodylen, opaque, cas
ALT_RES_PKT_FMT = ">BBBBBBHIIQ"
# magic, opcode, frame_extra_len, keylen, extralen, datatype, vbucket, bodylen, opaque, cas
ALT_REQ_PKT_FMT = ">BBBBBBHIIQ"

# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS = {
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
    CMD_SET_VBUCKET_STATE: VB_SET_PKT_FMT,
    CMD_COMPACT_DB: COMPACT_DB_PKT_FMT
}

EXTRA_HDR_SIZES = dict(
    [(k, struct.calcsize(v)) for (k, v) in EXTRA_HDR_FMTS.items()])

# Kept for backwards compatibility with existing mc_bin_client users.

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
ERR_AUTH = 0x20

# These error codes match protocol_binary.h naming.

ERR_SUCCESS = 0x00
ERR_KEY_ENOENT = 0x01
ERR_KEY_EEXISTS = 0x02
ERR_E2BIG = 0x03
ERR_EINVAL = 0x04
ERR_NOT_STORED = 0x05
ERR_DELTA_BADVAL = 0x06
ERR_NOT_MY_VBUCKET = 0x07
ERR_AUTH_ERROR = 0x20
ERR_AUTH_CONTINUE = 0x21
ERR_ERANGE = 0x22
ERR_UNKNOWN_COMMAND = 0x81
ERR_ENOMEM = 0x82
ERR_NOT_SUPPORTED = 0x83
ERR_EINTERNAL = 0x84
ERR_EBUSY = 0x85
ERR_ETMPFAIL = 0x86

META_REVID = 0x01

DURABILITY_LEVEL_MAJORITY = 0x1

# SetWithMeta options
FORCE_WITH_META_OP = 0x1
FORCE_ACCEPT_WITH_META_OPS = 0x2
REGENERATE_CAS = 0x4
SKIP_CONFLICT_RESOLUTION_FLAG = 0x08
IS_EXPIRATION = 0x10

DTYPE_RAW = 0
DTYPE_JSON = 1
DTYPE_SNAPPY = 2
DTYPE_XATTR = 4
