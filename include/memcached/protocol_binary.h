/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (c) <2008>, Sun Microsystems, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the  nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY SUN MICROSYSTEMS, INC. ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL SUN MICROSYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * Summary: Constants used by to implement the binary protocol.
 *
 * Copy: See Copyright for the status of this software.
 *
 * Author: Trond Norbye <trond.norbye@sun.com>
 */
#pragma once

#include "config.h"

#include <memcached/vbucket.h>
#include <platform/socket.h>
#include <gsl/gsl>

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <cstdint>
#include <stdexcept>
#include <string>

/**
 * \addtogroup Protocol
 * @{
 */

/**
 * This file contains definitions of the constants and packet formats
 * defined in the binary specification. Please note that you _MUST_ remember
 * to convert each multibyte field to / from network byte order to / from
 * host order.
 */

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/feature.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>

// For backward compatibility with old sources

// Magic
const uint8_t PROTOCOL_BINARY_REQ = uint8_t(cb::mcbp::Magic::ClientRequest);
const uint8_t PROTOCOL_BINARY_RES = uint8_t(cb::mcbp::Magic::ClientResponse);

/**
 * Definition of the header structure for a request packet.
 * See section 2
 */
union protocol_binary_request_header {
    cb::mcbp::Request request;
    uint8_t bytes[24];
};

/**
 * Definition of the header structure for a response packet.
 * See section 2
 */
union protocol_binary_response_header {
    cb::mcbp::Response response;
    uint8_t bytes[24];
};

/**
 * Definition of a request-packet containing no extras
 */
typedef union {
    struct {
        protocol_binary_request_header header;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header)];
} protocol_binary_request_no_extras;

/**
 * Definition of a response-packet containing no extras
 */
typedef union {
    struct {
        protocol_binary_response_header header;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header)];
} protocol_binary_response_no_extras;

/**
 * Definition of the packet used by the get, getq, getk and getkq command.
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_get;
typedef protocol_binary_request_no_extras protocol_binary_request_getq;
typedef protocol_binary_request_no_extras protocol_binary_request_getk;
typedef protocol_binary_request_no_extras protocol_binary_request_getkq;

/**
 * Definition of the packet returned from a successful get, getq, getk and
 * getkq.
 * See section 4
 */
typedef union {
    struct {
        protocol_binary_response_header header;
        struct {
            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header) + 4];
} protocol_binary_response_get;

typedef protocol_binary_response_get protocol_binary_response_getq;
typedef protocol_binary_response_get protocol_binary_response_getk;
typedef protocol_binary_response_get protocol_binary_response_getkq;

/**
 * Definition of the packet used by the delete command
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_delete;

/**
 * Definition of the packet returned by the delete command
 * See section 4
 *
 * extlen should be either zero, or 16 if the client has enabled the
 * MUTATION_SEQNO feature, with the following format:
 *
 *   Header:           (0-23): <protocol_binary_response_header>
 *   Extras:
 *     Vbucket UUID   (24-31): 0x0000000000003039
 *     Seqno          (32-39): 0x000000000000002D
 */
typedef protocol_binary_response_no_extras protocol_binary_response_delete;

/**
 * Definition of the packet used by the flush command
 * See section 4
 * Please note that the expiration field is optional, so remember to see
 * check the header.bodysize to see if it is present.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            /*
             * Specifying a non-null expiration time is no longer
             * supported
             */
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_flush;

/**
 * Definition of the packet returned by the flush command
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_flush;

/**
 * Definition of the packet used by set, add and replace
 * See section 4
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t flags;
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_set;
typedef protocol_binary_request_set protocol_binary_request_add;
typedef protocol_binary_request_set protocol_binary_request_replace;

/**
 * Definition of the packet returned by set, add and replace
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_set;
typedef protocol_binary_response_no_extras protocol_binary_response_add;
typedef protocol_binary_response_no_extras protocol_binary_response_replace;

/**
 * Definition of the noop packet
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_noop;

/**
 * Definition of the packet returned by the noop command
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_noop;

/**
 * Definition of the structure used by the increment and decrement
 * command.
 * See section 4
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t delta;
            uint64_t initial;
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 20];
} protocol_binary_request_incr;
typedef protocol_binary_request_incr protocol_binary_request_decr;

/**
 * Definition of the response from an incr or decr command
 * command.
 *
 * The result of the incr/decr is a uint64_t placed at header + extlen.
 *
 * extlen should be either zero, or 16 if the client has enabled the
 * MUTATION_SEQNO feature, with the following format:
 *
 *   Header:           (0-23): <protocol_binary_response_header>
 *   Extras:
 *     Vbucket UUID   (24-31): 0x0000000000003039
 *     Seqno          (32-39): 0x000000000000002D
 *   Value:           (40-47): ....
 *
 */
typedef protocol_binary_response_no_extras protocol_binary_response_incr;
typedef protocol_binary_response_no_extras protocol_binary_response_decr;

/**
 * Definition of the quit
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_quit;

/**
 * Definition of the packet returned by the quit command
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_quit;

/**
 * Definition of the packet used by append and prepend command
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_append;
typedef protocol_binary_request_no_extras protocol_binary_request_prepend;

/**
 * Definition of the packet returned from a successful append or prepend
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_append;
typedef protocol_binary_response_no_extras protocol_binary_response_prepend;

/**
 * Definition of the packet used by the version command
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_version;

/**
 * Definition of the packet returned from a successful version command
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_version;

/**
 * Definition of the packet used by the stats command.
 * See section 4
 */
typedef protocol_binary_request_no_extras protocol_binary_request_stats;

/**
 * Definition of the packet returned from a successful stats command
 * See section 4
 */
typedef protocol_binary_response_no_extras protocol_binary_response_stats;

/**
 * Definition of the packet used by the verbosity command
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t level;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_verbosity;

/**
 * Definition of the packet returned from the verbosity command
 */
typedef protocol_binary_response_no_extras protocol_binary_response_verbosity;

/**
 * Definition of the packet used by the touch command.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_touch;

/**
 * Definition of the packet returned from the touch command
 */
typedef protocol_binary_response_no_extras protocol_binary_response_touch;

/**
 * Definition of the packet used by the GAT(Q) command.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_gat;

typedef protocol_binary_request_gat protocol_binary_request_gatq;

/**
 * Definition of the packet returned from the GAT(Q)
 */
typedef protocol_binary_response_get protocol_binary_response_gat;
typedef protocol_binary_response_get protocol_binary_response_gatq;


/**
 * Definitions for extended (flexible) metadata
 *
 * @1: Flex Code to identify the number of extended metadata fields
 * @2: Size of the Flex Code, set to 1 byte
 * @3: Current size of extended metadata
 */
typedef enum {
    FLEX_META_CODE = 0x01,
    FLEX_DATA_OFFSET = 1,
    EXT_META_LEN = 1
} protocol_binary_flexmeta;

/**
 * Definitions of sub-document path flags (this is a bitmap)
 */
typedef enum : uint8_t {
    /** No flags set */
    SUBDOC_FLAG_NONE = 0x0,

    /** (Mutation) Should non-existent intermediate paths be created? */
    SUBDOC_FLAG_MKDIR_P = 0x01,

    /**
     * 0x02 is unused
     */

    /**
     * If set, the path refers to an Extended Attribute (XATTR).
     * If clear, the path refers to a path inside the document body.
     */
    SUBDOC_FLAG_XATTR_PATH = 0x04,

    /**
     * 0x08 is unused
     */

    /**
     * Expand macro values inside extended attributes. The request is
     * invalid if this flag is set without SUBDOC_FLAG_XATTR_PATH being
     * set.
     */
    SUBDOC_FLAG_EXPAND_MACROS = 0x10,

} protocol_binary_subdoc_flag;

namespace mcbp {
namespace subdoc {
/**
 * Definitions of sub-document doc flags (this is a bitmap).
 */

enum class doc_flag : uint8_t {
    None = 0x0,

    /**
     * (Mutation) Create the document if it does not exist. Implies
     * SUBDOC_FLAG_MKDIR_P and Set (upsert) mutation semantics. Not valid
     * with Add.
     */
        Mkdoc = 0x1,

    /**
     * (Mutation) Add the document only if it does not exist. Implies
     * SUBDOC_FLAG_MKDIR_P. Not valid with Mkdoc.
     */
        Add = 0x02,

    /**
     * Allow access to XATTRs for deleted documents (instead of
     * returning KEY_ENOENT).
     */
        AccessDeleted = 0x04,

};
} // namespace subdoc
} // namespace mcbp




/**
 * Definition of the packet used by SUBDOCUMENT single-path commands.
 *
 * The path, which is always required, is in the Body, after the Key.
 *
 *   Header:                        24 @0: <protocol_binary_request_header>
 *   Extras:
 *     Sub-document pathlen          2 @24: <variable>
 *     Sub-document flags            1 @26: <protocol_binary_subdoc_flag>
 *     Expiry                        4 @27: (Optional) Mutations only. The
 *                                          ttl
 *     Sub-document doc flags        1 @27: (Optional) @31 if expiry is
 *                                          set. Note these are the
 *                                          subdocument doc flags not the
 *                                          flag section in the document.
 *   Body:
 *     Key                      keylen @27: <variable>
 *     Path                    pathlen @27+keylen: <variable>
 *     Value to insert/replace
 *               vallen-keylen-pathlen @27+keylen+pathlen: [variable]
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint16_t pathlen; // Length in bytes of the sub-doc path.
            uint8_t subdoc_flags; // See protocol_binary_subdoc_flag
            /* uint32_t expiry     (optional for mutations only - present
                                    if extlen == 7 or extlen == 8) */
            /* uint8_t doc_flags   (optional - present if extlen == 4 or
                                    extlen == 8)  Note these are the
                                    subdocument doc flags not the flag
                                    \section in the document. */
        } extras;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 3];
} protocol_binary_request_subdocument;

/** Definition of the packet used by SUBDOCUMENT responses.
 */
typedef union {
    struct {
        protocol_binary_response_header header;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header)];
} protocol_binary_response_subdocument;

/**
 * Definition of the request packets used by SUBDOCUMENT multi-path commands.
 *
 * Multi-path sub-document commands differ from single-path in that they
 * encode a series of multiple paths to operate on (from a single key).
 * There are two multi-path commands - MULTI_LOOKUP and MULTI_MUTATION.
 * - MULTI_LOOKUP consists of variable number of subdoc lookup commands
 *                (SUBDOC_GET or SUBDOC_EXISTS).
 * - MULTI_MUTATION consists of a variable number of subdoc mutation
 *                  commands (i.e. all subdoc commands apart from
 *                  SUBDOC_{GET,EXISTS}).
 *
 * Each path to be operated on is specified by an Operation Spec, which are
 * contained in the body. This defines the opcode, path, and value
 * (for mutations).
 *
 * A maximum of MULTI_MAX_PATHS paths (operations) can be encoded in a
 * single multi-path command.
 *
 *  SUBDOC_MULTI_LOOKUP:
 *    Header:                24 @0:  <protocol_binary_request_header>
 *    Extras:            0 or 1 @24: (optional) doc_flags. Note these are
 *                                   the subdocument doc flags not the flag
 *                                   section in the document.
 *    Body:         <variable>  @24:
 *        Key            keylen @24: <variable>
 *        1..MULTI_MAX_PATHS [Lookup Operation Spec]
 *
 *        Lookup Operation Spec:
 *                            1 @0 : Opcode
 *                            1 @1 : Flags
 *                            2 @2 : Path Length
 *                      pathlen @4 : Path
 */
static const int PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS = 16;

typedef struct {
    cb::mcbp::ClientOpcode opcode;
    uint8_t flags;
    uint16_t pathlen;
    /* uint8_t path[pathlen] */
} protocol_binary_subdoc_multi_lookup_spec;

typedef protocol_binary_request_no_extras
        protocol_binary_request_subdocument_multi_lookup;

/*
 *
 * SUBDOC_MULTI_MUTATION
 *    Header:                24 @0:  <protocol_binary_request_header>
 *    Extras:            0 OR 4 @24: (optional) expiration
 *                       0 OR 1 @24: (optional) doc_flags. Note these are
 *                                   the subdocument doc flags not the
 *                                   flag section in the document.
 *    Body:           variable  @24 + extlen:
 *        Key            keylen @24: <variable>
 *        1..MULTI_MAX_PATHS [Mutation Operation Spec]
 *
 *        Mutation Operation Spec:
 *                            1 @0         : Opcode
 *                            1 @1         : Flags
 *                            2 @2         : Path Length
 *                            4 @4         : Value Length
 *                      pathlen @8         : Path
 *                       vallen @8+pathlen : Value
 */
typedef struct {
    cb::mcbp::ClientOpcode opcode;
    uint8_t flags;
    uint16_t pathlen;
    uint32_t valuelen;
    /* uint8_t path[pathlen] */
    /* uint8_t value[valuelen]  */
} protocol_binary_subdoc_multi_mutation_spec;

typedef protocol_binary_request_no_extras
        protocol_binary_request_subdocument_multi_mutation;

/**
 * Definition of the response packets used by SUBDOCUMENT multi-path
 * commands.
 *
 * SUBDOC_MULTI_LOOKUP - Body consists of a series of lookup_result structs,
 *                       one per lookup_spec in the request.
 *
 * Lookup Result:
 *                            2 @0 : status
 *                            4 @2 : resultlen
 *                    resultlen @6 : result
 */
typedef struct {
    protocol_binary_request_header header;
    /* Variable-length 1..PROTOCOL_BINARY_SUBDOC_MULTI_MAX_PATHS */
    protocol_binary_subdoc_multi_lookup_spec body[1];
} protocol_binary_response_subdoc_multi_lookup;

/**
 * SUBDOC_MULTI_MUTATION response
 *
 * Extras is either 0 or 16 if MUTATION_SEQNO is enabled.
 *
 * Body consists of a variable number of subdoc_multi_mutation_result_spec
 * structs:
 *
 * On success (header.status == SUCCESS), zero or more result specs, one for
 * each multi_mutation_spec which wishes to return a value.
 *
 * Mutation Result (success):
 *   [0..N] of:
 *                   1 @0 : index - Index of multi_mutation spec this result
 *                          corresponds to.
 *                   2 @1 : status - Status of the mutation (should always
 *                          be SUCCESS for successful multi-mutation
 *                          requests).
 *                   4 @3 : resultlen - Result value length
 *           resultlen @7 : Value payload
 *

 * On one of more of the mutation specs failing, there is exactly one
 * result spec, specifying the index and status code of the first failing
 * mutation spec.
 *
 * Mutation Result (failure):
 *   1 of:
 *                   1 @0 : index - Index of multi_mutation spec this result
 *                          corresponds to.
 *                   2 @1 : status - Status of the mutation (should always be
 *                          !SUCCESS for failures).
 *
 * (Note: On failure the multi_mutation_result_spec only includes the
 *        first two fields).
 */
typedef union {
    struct {
        protocol_binary_response_header header;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header)];
} protocol_binary_response_subdoc_multi_mutation;

/**
 * Definition of a request for a range operation.
 * See http://code.google.com/p/memcached/wiki/RangeOps
 *
 * These types are used for range operations and exist within
 * this header for use in other projects.  Range operations are
 * not expected to be implemented in the memcached server itself.
 */
typedef union {
    struct {
        protocol_binary_response_header header;
        struct {
            uint16_t size;
            uint8_t reserved;
            uint8_t flags;
            uint32_t max_results;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_rangeop;

typedef protocol_binary_request_rangeop protocol_binary_request_rget;
typedef protocol_binary_request_rangeop protocol_binary_request_rset;
typedef protocol_binary_request_rangeop protocol_binary_request_rsetq;
typedef protocol_binary_request_rangeop protocol_binary_request_rappend;
typedef protocol_binary_request_rangeop protocol_binary_request_rappendq;
typedef protocol_binary_request_rangeop protocol_binary_request_rprepend;
typedef protocol_binary_request_rangeop protocol_binary_request_rprependq;
typedef protocol_binary_request_rangeop protocol_binary_request_rdelete;
typedef protocol_binary_request_rangeop protocol_binary_request_rdeleteq;
typedef protocol_binary_request_rangeop protocol_binary_request_rincr;
typedef protocol_binary_request_rangeop protocol_binary_request_rincrq;
typedef protocol_binary_request_rangeop protocol_binary_request_rdecr;
typedef protocol_binary_request_rangeop protocol_binary_request_rdecrq;

/**
 * Definition of tap commands - Note: TAP removed in 5.0
 * See To be written
 *
 */

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            /**
             * flags is a bitmask used to set properties for the
             * the connection. Please In order to be forward compatible
             * you should set all undefined bits to 0.
             *
             * If the bit require extra userdata, it will be stored
             * in the user-data field of the body (passed to the engine
             * as enginespeciffic). That means that when you parse the
             * flags and the engine-specific data, you have to work your
             * way from bit 0 and upwards to find the correct offset for
             * the data.
             *
             */
            uint32_t flags;

/**
 * Backfill age
 *
 * By using this flag you can limit the amount of data being
 * transmitted. If you don't specify a backfill age, the
 * server will transmit everything it contains.
 *
 * The first 8 bytes in the engine specific data contains
 * the oldest entry (from epoc) you're interested in.
 * Specifying a time in the future (for the server you are
 * connecting to), will cause it to start streaming current
 * changes.
 */
#define TAP_CONNECT_FLAG_BACKFILL 0x01
/**
 * Dump will cause the server to send the data stored on the
 * server, but disconnect when the keys stored in the server
 * are transmitted.
 */
#define TAP_CONNECT_FLAG_DUMP 0x02
/**
 * The body contains a list of 16 bits words in network byte
 * order specifying the vbucket ids to monitor. The first 16
 * bit word contains the number of buckets. The number of 0
 * means "all buckets"
 */
#define TAP_CONNECT_FLAG_LIST_VBUCKETS 0x04
/**
 * The responsibility of the vbuckets is to be transferred
 * over to the caller when all items are transferred.
 */
#define TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS 0x08
/**
 * The tap consumer supports ack'ing of tap messages
 */
#define TAP_CONNECT_SUPPORT_ACK 0x10
/**
 * The tap consumer would prefer to just get the keys
 * back. If the engine supports this it will set
 * the TAP_FLAG_NO_VALUE flag in each of the
 * tap packets returned.
 */
#define TAP_CONNECT_REQUEST_KEYS_ONLY 0x20
/**
 * The body contains a list of (vbucket_id, last_checkpoint_id)
 * pairs. This provides the checkpoint support in TAP streams.
 * The last checkpoint id represents the last checkpoint that
 * was successfully persisted.
 */
#define TAP_CONNECT_CHECKPOINT 0x40
/**
 * The tap consumer is a registered tap client, which means that
 * the tap server will maintain its checkpoint cursor permanently.
 */
#define TAP_CONNECT_REGISTERED_CLIENT 0x80

/**
 * The initial TAP implementation convert flags to/from network
 * byte order, but the values isn't stored in host local order
 * causing them to change if you mix platforms..
 */
#define TAP_CONNECT_TAP_FIX_FLAG_BYTEORDER 0x100

        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_tap_connect;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            struct {
                uint16_t enginespecific_length;
/*
 * The flag section support the following flags
 */
/**
 * Request that the consumer send a response packet
 * for this packet. The opaque field must be preserved
 * in the response.
 */
#define TAP_FLAG_ACK 0x01
/**
 * The value for the key is not included in the packet
 */
#define TAP_FLAG_NO_VALUE 0x02
/**
 * The flags are in network byte order
 */
#define TAP_FLAG_NETWORK_BYTE_ORDER 0x04

                uint16_t flags;
                uint8_t ttl;
                uint8_t res1;
                uint8_t res2;
                uint8_t res3;
            } tap;
            struct {
                uint32_t flags;
                uint32_t expiration;
            } item;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 16];
} protocol_binary_request_tap_mutation;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            struct {
                uint16_t enginespecific_length;
                /**
                 * See the definition of the flags for
                 * protocol_binary_request_tap_mutation for a description
                 * of the available flags.
                 */
                uint16_t flags;
                uint8_t ttl;
                uint8_t res1;
                uint8_t res2;
                uint8_t res3;
            } tap;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_tap_no_extras;

typedef protocol_binary_request_tap_no_extras
        protocol_binary_request_tap_delete;
typedef protocol_binary_request_tap_no_extras protocol_binary_request_tap_flush;

/**
 * TAP OPAQUE command list
 */
#define TAP_OPAQUE_ENABLE_AUTO_NACK 0
#define TAP_OPAQUE_INITIAL_VBUCKET_STREAM 1
#define TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC 2
#define TAP_OPAQUE_OPEN_CHECKPOINT 3
#define TAP_OPAQUE_COMPLETE_VB_FILTER_CHANGE 4
#define TAP_OPAQUE_CLOSE_TAP_STREAM 7
#define TAP_OPAQUE_CLOSE_BACKFILL 8

typedef protocol_binary_request_tap_no_extras
        protocol_binary_request_tap_opaque;
typedef protocol_binary_request_tap_no_extras
        protocol_binary_request_tap_vbucket_set;

/**
 * Definition of the packet used by the scrub.
 */
typedef protocol_binary_request_no_extras protocol_binary_request_scrub;

/**
 * Definition of the packet returned from scrub.
 */
typedef protocol_binary_response_no_extras protocol_binary_response_scrub;

/**
 * Definition of the packet used by set vbucket
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            vbucket_state_t state;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) +
                  sizeof(vbucket_state_t)];
} protocol_binary_request_set_vbucket;
/**
 * Definition of the packet returned from set vbucket
 */
typedef protocol_binary_response_no_extras protocol_binary_response_set_vbucket;
/**
 * Definition of the packet used by del vbucket
 */
typedef protocol_binary_request_no_extras protocol_binary_request_del_vbucket;
/**
 * Definition of the packet returned from del vbucket
 */
typedef protocol_binary_response_no_extras protocol_binary_response_del_vbucket;

/**
 * Definition of the packet used by get vbucket
 */
typedef protocol_binary_request_no_extras protocol_binary_request_get_vbucket;

/**
 * Definition of the packet returned from get vbucket
 */
typedef union {
    struct {
        protocol_binary_response_header header;
        struct {
            vbucket_state_t state;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header) +
                  sizeof(vbucket_state_t)];
} protocol_binary_response_get_vbucket;

/**
 * The HELLO command is used by the client and the server to agree
 * upon the set of features the other end supports. It is initiated
 * by the client by sending its agent string and the list of features
 * it would like to use. The server will then reply with the list
 * of the requested features it supports.
 *
 * ex:
 * Client ->  HELLO [myclient 2.0] datatype, tls
 * Server ->  HELLO SUCCESS datatype
 *
 * In this example the server responds that it allows the client to
 * use the datatype extension, but not the tls extension.
 */

/**
 * Definition of the packet requested by hello cmd.
 * Key: This is a client-specific identifier (not really used by
 *      the server, except for logging the HELLO and may therefore
 *      be used to identify the client at a later time)
 * Body: Contains all features supported by client. Each feature is
 *       specified as an uint16_t in network byte order.
 */
typedef protocol_binary_request_no_extras protocol_binary_request_hello;

/**
 * Definition of the packet returned by hello cmd.
 * Body: Contains all features requested by the client that the
 *       server agrees to ssupport. Each feature is
 *       specified as an uint16_t in network byte order.
 */
typedef protocol_binary_response_no_extras protocol_binary_response_hello;

/**
 * The SET_CTRL_TOKEN command will be used by ns_server and ns_server alone
 * to set the session cas token in memcached which will be used to
 * recognize the particular instance on ns_server. The previous token will
 * be passed in the cas section of the request header for the CAS operation,
 * and the new token will be part of ext (8B).
 *
 * The response to this request will include the cas as it were set,
 * and a SUCCESS as status, or a KEY_EEXISTS with the existing token in
 * memcached if the CAS operation were to fail.
 */

/**
 * Definition of the request packet for SET_CTRL_TOKEN.
 * Body: new session_cas_token of uint64_t type.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t new_cas;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_set_ctrl_token;

/**
 * Definition of the response packet for SET_CTRL_TOKEN
 */
typedef protocol_binary_response_no_extras
        protocol_binary_response_set_ctrl_token;

/**
 * The GET_CTRL_TOKEN command will be used by ns_server to fetch the current
 * session cas token held in memcached.
 *
 * The response to this request will include the token currently held in
 * memcached in the cas field of the header.
 */

/**
 * Definition of the request packet for GET_CTRL_TOKEN.
 */
typedef protocol_binary_request_no_extras
        protocol_binary_request_get_ctrl_token;

/**
 * Definition of the response packet for GET_CTRL_TOKEN
 */
typedef protocol_binary_response_no_extras
        protocol_binary_response_get_ctrl_token;

/* DCP related stuff */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t seqno;
/*
 * The following flags are defined
 */
#define DCP_OPEN_PRODUCER 1
#define DCP_OPEN_NOTIFIER 2

/**
 * Indicate that the server include the documents' XATTRs
 * within mutation and deletion bodies.
 */
#define DCP_OPEN_INCLUDE_XATTRS 4

/**
 * Indicate that the server should strip off the values (note,
 * if you add INCLUDE_XATTR those will be present)
 */
#define DCP_OPEN_NO_VALUE 8

#define DCP_OPEN_UNUSED 16

/**
 * Request that DCP delete message include the time the a delete was persisted.
 * This only applies to deletes being backfilled from storage, in-memory deletes
 * will have a delete time of 0
 */
#define DCP_OPEN_INCLUDE_DELETE_TIMES 32

            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_dcp_open;

typedef protocol_binary_response_no_extras protocol_binary_response_dcp_open;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
/*
 * The following flags are defined
 */
#define DCP_ADD_STREAM_FLAG_TAKEOVER 1
#define DCP_ADD_STREAM_FLAG_DISKONLY 2
#define DCP_ADD_STREAM_FLAG_LATEST 4
/**
 * This flag is not used anymore, and should NOT be
 * set. It is replaced by DCP_OPEN_NO_VALUE.
 */
#define DCP_ADD_STREAM_FLAG_NO_VALUE 8
/**
 * Indicate the server to add stream only if the vbucket
 * is active.
 * If the vbucket is not active, the stream request fails with
 * error ENGINE_NOT_MY_VBUCKET
 */
#define DCP_ADD_STREAM_ACTIVE_VB_ONLY 16
/**
 * Indicate the server to check for vb_uuid match even at start_seqno 0 before
 * adding the stream successfully.
 * If the flag is set and there is a vb_uuid mismatch at start_seqno 0, then
 * the server returns ENGINE_ROLLBACK error.
 */
#define DCP_ADD_STREAM_STRICT_VBUUID 32
            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_dcp_add_stream;

typedef union {
    struct {
        protocol_binary_response_header header;
        struct {
            uint32_t opaque;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_response_header) + 4];
} protocol_binary_response_dcp_add_stream;

typedef protocol_binary_request_no_extras
        protocol_binary_request_dcp_close_stream;
typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_close_stream;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t flags;
            uint32_t reserved;
            uint64_t start_seqno;
            uint64_t end_seqno;
            uint64_t vbucket_uuid;
            uint64_t snap_start_seqno;
            uint64_t snap_end_seqno;
        } body;
        /* Group ID is specified in the key */
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 48];
} protocol_binary_request_dcp_stream_req;

typedef union {
    struct {
        protocol_binary_response_header header;
    } message;
    /*
    ** In case of cb::mcbp::Status::Rollback the body contains
    ** the rollback sequence number (uint64_t)
    */
    uint8_t bytes[sizeof(protocol_binary_request_header)];
} protocol_binary_response_dcp_stream_req;

typedef protocol_binary_request_no_extras
        protocol_binary_request_dcp_get_failover_log;

/* The body of the message contains UUID/SEQNO pairs */
typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_get_failover_log;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            /**
             * All flags set to 0 == OK,
             * 1: state changed
             */
            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_dcp_stream_end;
typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_stream_end;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t start_seqno;
            uint64_t end_seqno;
            uint32_t flags;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 20];
} protocol_binary_request_dcp_snapshot_marker;

typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_snapshot_marker;

union protocol_binary_request_dcp_mutation {
    protocol_binary_request_dcp_mutation(uint32_t opaque,
                                         Vbid vbucket,
                                         uint64_t cas,
                                         uint16_t keyLen,
                                         uint32_t valueLen,
                                         protocol_binary_datatype_t datatype,
                                         uint64_t bySeqno,
                                         uint64_t revSeqno,
                                         uint32_t flags,
                                         uint32_t expiration,
                                         uint32_t lockTime,
                                         uint16_t nmeta,
                                         uint8_t nru) {
        auto& req = message.header.request;

        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
        req.setExtlen(gsl::narrow<uint8_t>(getExtrasLength()));
        req.setKeylen(keyLen);
        req.setBodylen(gsl::narrow<uint8_t>(getExtrasLength()) + keyLen +
                       nmeta + valueLen);
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));

        auto& body = message.body;
        body.by_seqno = htonll(bySeqno);
        body.rev_seqno = htonll(revSeqno);
        body.flags = flags;
        body.expiration = htonl(expiration);
        body.lock_time = htonl(lockTime);
        body.nmeta = htons(nmeta);
        body.nru = nru;
    }

    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t by_seqno;
            uint64_t rev_seqno;
            uint32_t flags;
            uint32_t expiration;
            uint32_t lock_time;
            uint16_t nmeta;
            uint8_t nru;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 31];

    static uint8_t getExtrasLength() {
        return (2 * sizeof(uint64_t)) + (3 * sizeof(uint32_t)) +
               sizeof(uint16_t) + sizeof(uint8_t);
    }

    static size_t getHeaderLength() {
        return sizeof(protocol_binary_request_header) + getExtrasLength();
    }
};

union protocol_binary_request_dcp_deletion {
    static constexpr size_t extlen = 18;
    protocol_binary_request_dcp_deletion(uint32_t opaque,
                                         Vbid vbucket,
                                         uint64_t cas,
                                         uint16_t keyLen,
                                         uint32_t valueLen,
                                         protocol_binary_datatype_t datatype,
                                         uint64_t bySeqno,
                                         uint64_t revSeqno,
                                         uint16_t nmeta) {
        auto& req = message.header.request;
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        req.setExtlen(extlen);
        req.setKeylen(keyLen);
        req.setBodylen(
                gsl::narrow<uint32_t>(extlen + keyLen + nmeta + valueLen));
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));

        auto& body = message.body;
        body.by_seqno = htonll(bySeqno);
        body.rev_seqno = htonll(revSeqno);
        body.nmeta = htons(nmeta);
    }
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t by_seqno;
            uint64_t rev_seqno;
            uint16_t nmeta;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + extlen];
};

union protocol_binary_request_dcp_deletion_v2 {
    static constexpr size_t extlen = 21;
    protocol_binary_request_dcp_deletion_v2(uint32_t opaque,
                                            Vbid vbucket,
                                            uint64_t cas,
                                            uint16_t keyLen,
                                            uint32_t valueLen,
                                            protocol_binary_datatype_t datatype,
                                            uint64_t bySeqno,
                                            uint64_t revSeqno,
                                            uint32_t deleteTime,
                                            uint8_t collectionLen) {
        auto& req = message.header.request;
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        req.setExtlen(extlen);
        req.setKeylen(keyLen);
        req.setBodylen(extlen + keyLen + valueLen);
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));

        auto& body = message.body;
        body.by_seqno = htonll(bySeqno);
        body.rev_seqno = htonll(revSeqno);
        body.delete_time = htonl(deleteTime);
        body.collection_len = collectionLen;
    }

    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t by_seqno;
            uint64_t rev_seqno;
            uint32_t delete_time;
            uint8_t collection_len;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + extlen];
};

union protocol_binary_request_dcp_expiration {
    static const size_t extlen = (2 * sizeof(uint64_t)) + sizeof(uint32_t);
    protocol_binary_request_dcp_expiration(uint32_t opaque,
                                           Vbid vbucket,
                                           uint64_t cas,
                                           uint16_t keyLen,
                                           uint32_t valueLen,
                                           protocol_binary_datatype_t datatype,
                                           uint64_t bySeqno,
                                           uint64_t revSeqno,
                                           uint32_t deleteTime) {
        auto& req = message.header.request;
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
        req.setExtlen(extlen);
        req.setKeylen(keyLen);
        req.setBodylen(extlen + keyLen + valueLen);
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));

        auto& body = message.body;
        body.by_seqno = htonll(bySeqno);
        body.rev_seqno = htonll(revSeqno);
        body.delete_time = htonl(deleteTime);
    }

    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t by_seqno;
            uint64_t rev_seqno;
            uint32_t delete_time;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + extlen];

    static size_t getExtrasLength() {
        return extlen;
    }

    /**
     * Retrieve the size of a DCP expiration header - all the non variable
     * data of the packet. The size of a collectionAware DCP stream expiration
     * differs to that of legacy DCP streams.
     */
    static size_t getHeaderLength() {
        return sizeof(protocol_binary_request_header) + getExtrasLength();
    }
};

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            /**
             * 0x01 - Active
             * 0x02 - Replica
             * 0x03 - Pending
             * 0x04 - Dead
             */
            uint8_t state;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 1];
} protocol_binary_request_dcp_set_vbucket_state;
typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_set_vbucket_state;

typedef protocol_binary_request_no_extras protocol_binary_request_dcp_noop;
typedef protocol_binary_response_no_extras protocol_binary_response_dcp_noop;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t buffer_bytes;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_dcp_buffer_acknowledgement;
typedef protocol_binary_response_no_extras
        protocol_binary_response_dcp_buffer_acknowledgement;

typedef protocol_binary_request_no_extras protocol_binary_request_dcp_control;
typedef protocol_binary_response_no_extras protocol_binary_response_dcp_control;

/**
 * Events that the system may send
 */
namespace mcbp {
namespace systemevent {

enum class id : uint32_t {
    CreateCollection = 0,
    DeleteCollection = 1,
    FlushCollection = 2,
    CreateScope = 3,
    DropScope = 4
};

enum class version : uint8_t { version0 = 0, version1 = 1 };

/**
 * Validate that the uint32_t represents a valid systemevent::id
 */
static inline bool validate_id(uint32_t event) {
    switch (id(event)) {
        case id::CreateCollection:
        case id::DeleteCollection:
        case id::FlushCollection:
        case id::CreateScope:
        case id::DropScope:
            return true;
    }
    return false;
}

/**
 * Validate that the uint8_t represents a valid systemevent::version
 */
static inline bool validate_version(uint8_t event) {
    switch (version(event)) {
    case version::version0:
    case version::version1:
        return true;
    }
    return false;
}
}
}

/**
 * Format for a DCP_SYSTEM_EVENT packet. Encodes a sequence number for the
 * event and the event's identifier.
 */
union protocol_binary_request_dcp_system_event {
    protocol_binary_request_dcp_system_event(
            uint32_t opaque,
            Vbid vbucket,
            uint16_t keyLen,
            size_t valueLen,
            mcbp::systemevent::id event,
            uint64_t bySeqno,
            mcbp::systemevent::version version) {
        auto& req = message.header.request;
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpSystemEvent);
        req.setExtlen(getExtrasLength());
        req.setKeylen(keyLen);
        req.setBodylen(
                gsl::narrow<uint32_t>(getExtrasLength() + keyLen + valueLen));
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setDatatype(cb::mcbp::Datatype::Raw);
        message.body.event = htonl(uint32_t(event));
        message.body.by_seqno = htonll(bySeqno);
        message.body.version = uint8_t(version);
    }
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t by_seqno;
            uint32_t event;
            uint8_t version;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 13];

    /**
     * @returns the extlen value that a system_event packet should encode.
     */
    static inline uint8_t getExtrasLength() {
        return sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint8_t);
    }
};

/**
 * IOCTL_GET command message to get/set control parameters.
 */
typedef protocol_binary_request_no_extras protocol_binary_request_ioctl_get;
typedef protocol_binary_request_no_extras protocol_binary_request_ioctl_set;

typedef protocol_binary_request_no_extras
        protocol_binary_request_config_validate;
typedef protocol_binary_request_no_extras protocol_binary_request_config_reload;

typedef protocol_binary_request_no_extras protocol_binary_request_ssl_refresh;
typedef protocol_binary_response_no_extras protocol_binary_response_ssl_refresh;

/**
 * Request command timings for a bucket from memcached. Privileged
 * connections may specify the name of the bucket in the "key" field,
 * or the aggregated timings for the entire server by using the
 * special name <code>/all/</code>.
 *
 * The returned payload is a json document of the following format:
 *    { "us" : [ x, x, x, x, ... ],
 *      "ms" : [ y, y, y, ...],
 *      "500ms" : [ z, z, z, ...],
 *      "wayout" : nnn
 *    }
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint8_t opcode;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 1];
} protocol_binary_request_get_cmd_timer;

typedef protocol_binary_response_no_extras
        protocol_binary_response_get_cmd_timer;

typedef protocol_binary_request_no_extras protocol_binary_request_create_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_delete_bucket;
typedef protocol_binary_request_no_extras protocol_binary_request_list_buckets;
typedef protocol_binary_request_no_extras protocol_binary_request_select_bucket;

/*
 * Parameter types of CMD_SET_PARAM command.
 */
typedef enum : int {
    protocol_binary_engine_param_flush = 1, /* flusher-related param type */
    protocol_binary_engine_param_replication, /* replication param type */
    protocol_binary_engine_param_checkpoint, /* checkpoint-related param type */
    protocol_binary_engine_param_dcp, /* dcp param type */
    protocol_binary_engine_param_vbucket /* vbucket param type */
} protocol_binary_engine_param_t;

/**
 * CMD_SET_PARAM command message to set engine parameters.
 * flush, checkpoint, dcp and vbucket parameter types are currently
 * supported.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            protocol_binary_engine_param_t param_type;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) +
                  sizeof(protocol_binary_engine_param_t)];
} protocol_binary_request_set_param;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t size;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_set_batch_count;

/**
 * This flag is used by the setWithMeta/addWithMeta/deleteWithMeta packets
 * to specify that the operation should be forced. The update will not
 * be subject to conflict resolution and the target vb can be active/pending or
 * replica.
 */
#define FORCE_WITH_META_OP 0x01

/**
 * This flag is used to indicate that the *_with_meta should be accepted
 * regardless of the bucket config. LWW buckets require this flag.
 */
#define FORCE_ACCEPT_WITH_META_OPS 0x02

/**
 * This flag asks that the server regenerates the CAS. The server requires
 * that SKIP_CONFLICT_RESOLUTION_FLAG is set along with this option.
 */
#define REGENERATE_CAS 0x04

/**
 * This flag is used by the setWithMeta/addWithMeta/deleteWithMeta packets
 * to specify that the conflict resolution mechanism should be skipped for
 * this operation.
 */
#define SKIP_CONFLICT_RESOLUTION_FLAG 0x08

/**
 * This flag is used with the get meta response packet. If set it
 * specifies that the item recieved has been deleted, but that the
 * items meta data is still contained in ep-engine. Eg. the item
 * has been soft deleted.
 */
#define GET_META_ITEM_DELETED_FLAG 0x01

/**
 * The physical layout for the CMD_SET_WITH_META looks like the the normal
 * set request with the addition of a bulk of extra meta data stored
 * at the <b>end</b> of the package.
 */
union protocol_binary_request_set_with_meta {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t flags;
            uint32_t expiration;
            uint64_t seqno;
            uint64_t cas;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 24];
};

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t flags;
            uint32_t delete_time;
            uint64_t seqno;
            uint64_t cas;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 24];
} protocol_binary_request_delete_with_meta;

/**
 * The message format for getLocked engine API
 */
typedef protocol_binary_request_gat protocol_binary_request_getl;

/**
 * The physical layout for a CMD_GET_META command returns the meta-data
 * section for an item:
 */
typedef protocol_binary_request_no_extras protocol_binary_request_get_meta;

/**
 * Structure holding getMeta command response fields
 */
#pragma pack(1)

struct GetMetaResponse {
    uint32_t deleted;
    uint32_t flags;
    uint32_t expiry;
    uint64_t seqno;
    uint8_t datatype;

    GetMetaResponse() : deleted(0), flags(0), expiry(0), seqno(0), datatype(0) {
    }

    GetMetaResponse(uint32_t deleted,
                    uint32_t flags,
                    uint32_t expiry,
                    uint64_t seqno,
                    uint8_t datatype)
        : deleted(deleted),
          flags(flags),
          expiry(expiry),
          seqno(seqno),
          datatype(datatype) {
    }
};

#pragma pack()

static_assert(sizeof(GetMetaResponse) == 21, "Incorrect compiler padding");

/* Meta data versions for GET_META */
enum class GetMetaVersion : uint8_t {
    V1 = 1, // returns deleted, flags, expiry and seqno
    V2 = 2, // The 'spock' version returns V1 + the datatype
};

/**
 * The response for CMD_SET_WITH_META does not carry any user-data and the
 * status of the operation is signalled in the status bits.
 */
typedef protocol_binary_response_no_extras
        protocol_binary_response_set_with_meta;

typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t file_version;
            uint64_t header_offset;
            uint32_t vbucket_state_updated;
            uint32_t state;
            uint64_t checkpoint;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 32];
} protocol_binary_request_notify_vbucket_update;
typedef protocol_binary_response_no_extras
        protocol_binary_response_notify_vbucket_update;

/**
 * The physical layout for the CMD_RETURN_META
 */
namespace cb {
namespace mcbp {
namespace request {

enum class ReturnMetaType : uint32_t { Set = 1, Add = 2, Del = 3 };

class ReturnMetaPayload {
public:
    ReturnMetaType getMutationType() const {
        return static_cast<ReturnMetaType>(ntohl(mutation_type));
    }
    void setMutationType(ReturnMetaType mutation_type) {
        ReturnMetaPayload::mutation_type =
                htonl(static_cast<uint32_t>(mutation_type));
    }
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        ReturnMetaPayload::flags = htonl(flags);
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        ReturnMetaPayload::expiration = htonl(expiration);
    }

protected:
    uint32_t mutation_type;
    uint32_t flags;
    uint32_t expiration;
};
static_assert(sizeof(ReturnMetaPayload) == 12, "Unexpected struct size");
} // namespace request
} // namespace mcbp
} // namespace cb
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t mutation_type;
            uint32_t flags;
            uint32_t expiration;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 12];
} protocol_binary_request_return_meta;

/**
 * Message format for CMD_SET_CONFIG
 */
typedef protocol_binary_request_no_extras
        protocol_binary_request_set_cluster_config;

/**
 * Message format for CMD_GET_CONFIG
 */
typedef protocol_binary_request_no_extras
        protocol_binary_request_get_cluster_config;

/**
 * Message format for CMD_GET_ADJUSTED_TIME
 *
 * The PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME command will be
 * used by XDCR to retrieve the vbucket's latest adjusted_time
 * which is calculated based on the driftCounter if timeSync
 * has been enabled.
 *
 * Request:-
 *
 * Header: Contains a vbucket id.
 *
 * Response:-
 *
 * The response will contain the adjusted_time (type: int64_t)
 * as part of the body if in case of a SUCCESS, or else a NOTSUP
 * in case of timeSync not being enabled.
 *
 * The request packet's header will contain the vbucket_id.
 */
typedef protocol_binary_request_no_extras
        protocol_binary_request_get_adjusted_time;

/**
 * Message format for CMD_SET_DRIFT_COUNTER_STATE
 *
 * The PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE command will be
 * used by GO-XDCR to set the initial drift counter and enable/disable
 * the time synchronization for the vbucket.
 *
 * Request:-
 *
 * Header: Contains a vbucket id.
 * Extras: Contains the initial drift value which is of type int64_t and
 * the time sync state (0x00 for disable, 0x01 for enable),
 *
 * Response:-
 *
 * The response will return a SUCCESS after saving the settings, the
 * body will contain the vbucket uuid (type: uint64_t) and the vbucket
 * high seqno (type: int64_t).
 * A NOT_MY_VBUCKET (along with cluster config) is returned if the
 * vbucket isn't found.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            int64_t initial_drift;
            uint8_t time_sync;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 9];
} protocol_binary_request_set_drift_counter_state;

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
/**
 * Message format for CMD_COMPACT_DB
 *
 * The PROTOCOL_BINARY_CMD_COMPACT_DB is used by ns_server to
 * issue a compaction request to ep-engine to compact the
 * underlying store's database files
 *
 * Request:
 *
 * Header: Contains the vbucket id. The vbucket id will be used
 *         to identify the database file if the backend is
 *         couchstore. If the vbucket id is set to 0xFFFF, then
 *         the db_file_id field will be used for compaction.
 * Body:
 * - purge_before_ts:  Deleted items whose expiry timestamp is less
 *                     than purge_before_ts will be purged.
 * - purge_before_seq: Deleted items whose sequence number is less
 *                     than purge_before_seq will be purged.
 * - drop_deletes:     whether to purge deleted items or not.
 * - db_file_id  :     Database file id for the underlying store.
 *
 * Response:
 *
 * The response will return a SUCCESS after compaction is done
 * successfully and a NOT_MY_VBUCKET (along with cluster config)
 * if the vbucket isn't found.
 */
class CompactDbPayload {
public:
    uint64_t getPurgeBeforeTs() const {
        return ntohll(purge_before_ts);
    }
    void setPurgeBeforeTs(uint64_t purge_before_ts) {
        CompactDbPayload::purge_before_ts = htonll(purge_before_ts);
    }
    uint64_t getPurgeBeforeSeq() const {
        return ntohll(purge_before_seq);
    }
    void setPurgeBeforeSeq(uint64_t purge_before_seq) {
        CompactDbPayload::purge_before_seq = htonll(purge_before_seq);
    }
    uint8_t getDropDeletes() const {
        return drop_deletes;
    }
    void setDropDeletes(uint8_t drop_deletes) {
        CompactDbPayload::drop_deletes = drop_deletes;
    }
    const Vbid getDbFileId() const {
        return db_file_id.ntoh();
    }
    void setDbFileId(const Vbid& db_file_id) {
        CompactDbPayload::db_file_id = db_file_id.hton();
    }

    // Generate a method which use align_pad1 and 3 to avoid the compiler
    // to generate a warning about unused member (because we
    bool validate() const {
        return align_pad1 == 0 && align_pad3 == 0;
    }

protected:
    uint64_t purge_before_ts = 0;
    uint64_t purge_before_seq = 0;
    uint8_t drop_deletes = 0;
    uint8_t align_pad1 = 0;
    Vbid db_file_id;
    uint32_t align_pad3 = 0;
};
#pragma pack()
static_assert(sizeof(CompactDbPayload) == 24, "Unexpected struct size");
} // namespace request
} // namespace mcbp
} // namespace cb

typedef protocol_binary_request_get protocol_binary_request_get_random;

#define OBS_STATE_NOT_PERSISTED 0x00
#define OBS_STATE_PERSISTED 0x01
#define OBS_STATE_NOT_FOUND 0x80
#define OBS_STATE_LOGICAL_DEL 0x81

/**
 * The physical layout for the PROTOCOL_BINARY_CMD_AUDIT_PUT
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint32_t id;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 4];
} protocol_binary_request_audit_put;

typedef protocol_binary_response_no_extras protocol_binary_response_audit_put;

/**
 * The shutdown message is sent from ns_server to memcached to tell
 * memcached to initiate a clean shutdown. This is a privileged
 * command and carries no payload, but the CAS field needs to be
 * set to the current session token (see GET/SET_CTRL_TOKEN)
 */
typedef protocol_binary_request_no_extras protocol_binary_request_shutdown;
typedef protocol_binary_response_no_extras protocol_binary_response_shutdown;

/**
 * The rbac_refresh message is sent from ns_server to memcached to tell
 * memcached to reload the RBAC configuration file. This is a privileged
 * command and carries no payload.
 */
typedef protocol_binary_request_no_extras protocol_binary_request_rbac_refresh;
typedef protocol_binary_response_no_extras
        protocol_binary_response_rbac_refresh;

/**
 * The PROTOCOL_BINARY_CMD_OBSERVE_SEQNO command is used by the
 * client to retrieve information about the vbucket in order to
 * find out if a particular mutation has been persisted or
 * replicated at the server side. In order to do so, the client
 * would pass the vbucket uuid of the vbucket that it wishes to
 * observe to the serve.  The response would contain the last
 * persisted sequence number and the latest sequence number in the
 * vbucket. For example, if a client sends a request to observe
 * the vbucket 0 with uuid 12345 and if the response contains the
 * values <58, 65> and then the client can infer that sequence
 * number 56 has been persisted, 60 has only been replicated and
 * not been persisted yet and 68 has not been replicated yet.
 */

/**
 * Definition of the request packet for the observe_seqno command.
 *
 * Header: Contains the vbucket id of the vbucket that the client
 *         wants to observe.
 *
 * Body: Contains the vbucket uuid of the vbucket that the client
 *       wants to observe. The vbucket uuid is of type uint64_t.
 *
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t uuid;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 8];
} protocol_binary_request_observe_seqno;

/**
 * Definition of the response packet for the observe_seqno command.
 * Body: Contains a tuple of the form
 *       <format_type, vbucket id, vbucket uuid, last_persisted_seqno,
 * current_seqno>
 *
 *       - format_type is of type uint8_t and it describes whether
 *         the vbucket has failed over or not. 1 indicates a hard
 *         failover, 0 indicates otherwise.
 *       - vbucket id is of type Vbid and it is the identifier for
 *         the vbucket.
 *       - vbucket uuid is of type uint64_t and it represents a UUID for
 *          the vbucket.
 *       - last_persisted_seqno is of type uint64_t and it is the
 *         last sequence number that was persisted for this
 *         vbucket.
 *       - current_seqno is of the type uint64_t and it is the
 *         sequence number of the latest mutation in the vbucket.
 *
 *       In the case of a hard failover, the tuple is of the form
 *       <format_type, vbucket id, vbucket uuid, last_persisted_seqno,
 * current_seqno, old vbucket uuid, last_received_seqno>
 *
 *       - old vbucket uuid is of type uint64_t and it is the
 *         vbucket UUID of the vbucket prior to the hard failover.
 *
 *       - last_received_seqno is of type uint64_t and it is the
 *         last received sequence number in the old vbucket uuid.
 *
 *       The other fields are the same as that mentioned in the normal case.
 */
typedef protocol_binary_response_no_extras
        protocol_binary_response_observe_seqno;

/**
 * Definition of the request packet for the command
 * PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS
 *
 * Header: Only opcode field is used.
 *
 * Body: Contains the vbucket state for which the vb sequence numbers are
 *       requested.
 *       Please note that this field is optional, header.request.extlen is
 *       checked to see if it is present. If not present, it implies request
 *       is for all vbucket states.
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            vbucket_state_t state;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) +
                  sizeof(vbucket_state_t)];
} protocol_binary_request_get_all_vb_seqnos;

/**
 * Definition of the payload in the PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS
 * response.
 *
 * The body contains a "list" of "vbucket id - seqno pairs" for all
 * active and replica buckets on the node in network byte order.
 *
 *
 *    Byte/     0       |       1       |       2       |       3       |
 *       /              |               |               |               |
 *      |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
 *      +---------------+---------------+---------------+---------------+
 *     0| VBID          | VBID          | SEQNO         | SEQNO         |
 *      +---------------+---------------+---------------+---------------+
 *     4| SEQNO         | SEQNO         | SEQNO         | SEQNO         |
 *      +---------------+---------------+---------------+---------------+
 *     4| SEQNO         | SEQNO         |
 *      +---------------+---------------+
 */
typedef protocol_binary_response_no_extras
        protocol_binary_response_get_all_vb_seqnos;

/**
 * Message format for PROTOCOL_BINARY_CMD_GET_KEYS
 *
 * The extras field may contain a 32 bit integer specifying the number
 * of keys to fetch. If no value specified 1000 keys is transmitted.
 *
 * Key is mandatory and specifies the starting key
 *
 * Get keys is used to fetch a sequence of keys from the server starting
 * at the specified key.
 */
typedef protocol_binary_request_no_extras protocol_binary_request_get_keys;


enum class TimeType : uint8_t {
    TimeOfDay,
    Uptime
};
/**
 * Definition of the packet used to adjust timeofday and memcached uptime
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint64_t offset;
            TimeType timeType;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + sizeof(uint64_t) + sizeof(TimeType)];
} protocol_binary_adjust_time;

/**
 * Definition of the packet returned by adjust_timeofday
 */
typedef protocol_binary_response_no_extras protocol_binary_adjust_time_response;

/**
 * Message format for PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL
 *
 * See engines/ewouldblock_engine for more information.
 */

namespace cb {
namespace mcbp {
namespace request {
class EWB_Payload {
public:
    uint32_t getMode() const {
        return ntohl(mode);
    }
    void setMode(uint32_t m) {
        mode = htonl(m);
    }
    uint32_t getValue() const {
        return ntohl(value);
    }
    void setValue(uint32_t v) {
        value = htonl(v);
    }
    uint32_t getInjectError() const {
        return ntohl(inject_error);
    }
    void setInjectError(uint32_t ie) {
        inject_error = htonl(ie);
    }

protected:
    uint32_t mode = 0; // See EWB_Engine_Mode
    uint32_t value = 0;
    uint32_t inject_error = 0; // ENGINE_ERROR_CODE to inject.
};
static_assert(sizeof(EWB_Payload) == 12, "Invalid size for WEB_Payload");
} // namespace request
} // namespace mcbp
} // namespace cb

/**
 * Message format for PROTOCOL_BINARY_CMD_GET_ERRORMAP
 *
 * The payload (*not* specified as extras) contains a 2 byte payload
 * containing a 16 bit encoded version number. This version number should
 * indicate the highest version number of the error map the client is able
 * to understand. The server will return a JSON-formatted error map
 * which is formatted to either the version requested by the client, or
 * a lower version (thus, clients must be ready to parse lower version
 * formats).
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            uint16_t version;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) + 2];
} protocol_binary_request_get_errmap;

typedef protocol_binary_response_no_extras protocol_binary_response_get_errmap;

/**
 * Message format for PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST
 *
 * The body contains a JSON collections manifest.
 * No key and no extras
 */
typedef union {
    struct {
        protocol_binary_request_header header;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header)];
} protocol_binary_collections_set_manifest;

typedef protocol_binary_response_no_extras
        protocol_binary_response_collections_set_manifest;

/**
 * @}
 */
inline protocol_binary_subdoc_flag operator|(protocol_binary_subdoc_flag a,
                                             protocol_binary_subdoc_flag b) {
    return protocol_binary_subdoc_flag(static_cast<uint8_t>(a) |
                                       static_cast<uint8_t>(b));
}

namespace mcbp {
namespace subdoc {
inline mcbp::subdoc::doc_flag operator|(mcbp::subdoc::doc_flag a,
                                        mcbp::subdoc::doc_flag b) {
    return mcbp::subdoc::doc_flag(static_cast<uint8_t>(a) |
                                  static_cast<uint8_t>(b));
}

inline mcbp::subdoc::doc_flag operator&(mcbp::subdoc::doc_flag a,
                                        mcbp::subdoc::doc_flag b) {
    return mcbp::subdoc::doc_flag(static_cast<uint8_t>(a) &
                                  static_cast<uint8_t>(b));
}

inline mcbp::subdoc::doc_flag operator~(mcbp::subdoc::doc_flag a) {
    return mcbp::subdoc::doc_flag(~static_cast<uint8_t>(a));
}

inline std::string to_string(mcbp::subdoc::doc_flag a) {
    switch (a) {
    case mcbp::subdoc::doc_flag::None:
        return "None";
    case mcbp::subdoc::doc_flag::Mkdoc:
        return "Mkdoc";
    case mcbp::subdoc::doc_flag::AccessDeleted:
        return "AccessDeleted";
    case mcbp::subdoc::doc_flag::Add:
        return "Add";
    }
    return std::to_string(static_cast<uint8_t>(a));
}

inline bool hasAccessDeleted(mcbp::subdoc::doc_flag a) {
    return (a & mcbp::subdoc::doc_flag::AccessDeleted) !=
           mcbp::subdoc::doc_flag::None;
}

inline bool hasMkdoc(mcbp::subdoc::doc_flag a) {
    return (a & mcbp::subdoc::doc_flag::Mkdoc) != mcbp::subdoc::doc_flag::None;
}

inline bool hasAdd(mcbp::subdoc::doc_flag a) {
    return (a & mcbp::subdoc::doc_flag::Add) != mcbp::subdoc::doc_flag::None;
}

inline bool isNone(mcbp::subdoc::doc_flag a) {
    return a == mcbp::subdoc::doc_flag::None;
}
inline bool impliesMkdir_p(mcbp::subdoc::doc_flag a) {
    return hasAdd(a) || hasMkdoc(a);
}
} // namespace subdoc
} // namespace mcbp


namespace mcbp {

namespace cas {
/**
 * The special value used as a wildcard and match all CAS values
 */
const uint64_t Wildcard = 0x0;
} // namespace cas
} // namespace mcbp
