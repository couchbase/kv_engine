#!/usr/bin/env python
"""

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
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
CMD_STAT = 0x10
CMD_APPEND = 0x0e
CMD_PREPEND = 0x0f

# SASL stuff
CMD_SASL_LIST_MECHS = 0x20
CMD_SASL_AUTH = 0x21
CMD_SASL_STEP = 0x22

# Bucket extension
CMD_CREATE_BUCKET = 0x25
CMD_DELETE_BUCKET = 0x26
CMD_LIST_BUCKETS = 0x27
CMD_EXPAND_BUCKET = 0x28

CMD_STOP_PERSISTENCE = 0x80
CMD_START_PERSISTENCE = 0x81
CMD_SET_FLUSH_PARAM = 0x82

CMD_START_REPLICATION = 0x90
CMD_STOP_REPLICATION = 0x91
CMD_SET_TAP_PARAM = 0x92


# Replication
CMD_TAP_CONNECT = 0x40
CMD_TAP_MUTATION = 0x41
CMD_TAP_DELETE = 0x42
CMD_TAP_FLUSH = 0x43
CMD_TAP_OPAQUE = 0x44

COMMAND_NAMES = dict(((globals()[k], k) for k in globals() if k.startswith("CMD_")))

# TAP flags
TAP_FLAG_BACKFILL = 0x01
TAP_FLAG_DUMP = 0x02

TAP_FLAG_TYPES = {TAP_FLAG_BACKFILL: ">Q",
                  TAP_FLAG_DUMP: ""}

# Flags, expiration
SET_PKT_FMT=">II"

# flags
GET_RES_FMT=">I"

# How long until the deletion takes effect.
DEL_PKT_FMT=""

## TAP stuff
# eng-specific length, flags, ttl, [res, res, res]; item flags, exp
TAP_MUTATION_PKT_FMT = "HHbxxxII"
TAP_GENERAL_PKT_FMT = "HHbxxx"

# amount, initial value, expiration
INCRDECR_PKT_FMT=">QQI"
# Special incr expiration that means do not store
INCRDECR_SPECIAL=0xffffffff
INCRDECR_RES_FMT=">Q"

# Time bomb
FLUSH_PKT_FMT=">I"

MAGIC_BYTE = 0x80
REQ_MAGIC_BYTE = 0x80
RES_MAGIC_BYTE = 0x81

# magic, opcode, keylen, extralen, datatype, [reserved], bodylen, opaque, cas
REQ_PKT_FMT=">BBHBBxxIIQ"
# magic, opcode, keylen, extralen, datatype, status, bodylen, opaque, cas
RES_PKT_FMT=">BBHBBHIIQ"
# min recv packet size
MIN_RECV_PACKET = struct.calcsize(REQ_PKT_FMT)
# The header sizes don't deviate
assert struct.calcsize(REQ_PKT_FMT) == struct.calcsize(RES_PKT_FMT)

EXTRA_HDR_FMTS={
    CMD_SET: SET_PKT_FMT,
    CMD_ADD: SET_PKT_FMT,
    CMD_REPLACE: SET_PKT_FMT,
    CMD_INCR: INCRDECR_PKT_FMT,
    CMD_DECR: INCRDECR_PKT_FMT,
    CMD_DELETE: DEL_PKT_FMT,
    CMD_FLUSH: FLUSH_PKT_FMT,
    CMD_TAP_MUTATION: TAP_MUTATION_PKT_FMT,
    CMD_TAP_DELETE: TAP_GENERAL_PKT_FMT,
    CMD_TAP_FLUSH: TAP_GENERAL_PKT_FMT,
    CMD_TAP_OPAQUE: TAP_GENERAL_PKT_FMT

}

EXTRA_HDR_SIZES=dict(
    [(k, struct.calcsize(v)) for (k,v) in EXTRA_HDR_FMTS.items()])

ERR_UNKNOWN_CMD = 0x81
ERR_NOT_FOUND = 0x1
ERR_EXISTS = 0x2
ERR_AUTH = 0x20
ERR_AUTH_CONTINUE = 0x21
