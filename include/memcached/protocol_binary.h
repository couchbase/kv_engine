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

#include "dockey.h"

#include <memcached/vbucket.h>
#include <platform/socket.h>
#include <gsl/gsl>

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <cstdint>
#include <stdexcept>
#include <string>

namespace cb {
namespace durability {
enum class Level : uint8_t;
}
} // namespace cb

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
 * Definition of the packet used by set, add and replace
 * See section 4
 */
namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
class MutationPayload {
public:
    /// The memcached core keep the flags stored in network byte order
    /// internally as it does not use them for anything else than sending
    /// them back to the client
    uint32_t getFlagsInNetworkByteOrder() const {
        return flags;
    }

    uint32_t getFlags() const {
        return ntohl(flags);
    }

    void setFlags(uint32_t flags) {
        MutationPayload::flags = htonl(flags);
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        MutationPayload::expiration = htonl(expiration);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags = 0;
    uint32_t expiration = 0;
};
static_assert(sizeof(MutationPayload) == 8, "Unexpected struct size");

class ArithmeticPayload {
public:
    uint64_t getDelta() const {
        return ntohll(delta);
    }
    void setDelta(uint64_t delta) {
        ArithmeticPayload::delta = htonll(delta);
    }
    uint64_t getInitial() const {
        return ntohll(initial);
    }
    void setInitial(uint64_t initial) {
        ArithmeticPayload::initial = htonll(initial);
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        ArithmeticPayload::expiration = htonl(expiration);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

private:
    uint64_t delta = 0;
    uint64_t initial = 0;
    uint32_t expiration = 0;
};
static_assert(sizeof(ArithmeticPayload) == 20, "Unexpected struct size");

class SetClusterConfigPayload {
public:
    int32_t getRevision() const {
        return ntohl(revision);
    }

    void setRevision(int32_t rev) {
        revision = htonl(rev);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    int32_t revision{};
};
static_assert(sizeof(SetClusterConfigPayload) == 4, "Unexpected struct size");

class VerbosityPayload {
public:
    uint32_t getLevel() const {
        return ntohl(level);
    }
    void setLevel(uint32_t level) {
        VerbosityPayload::level = htonl(level);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t level = 0;
};
static_assert(sizeof(VerbosityPayload) == 4, "Unexpected size");

class TouchPayload {
public:
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        TouchPayload::expiration = htonl(expiration);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t expiration = 0;
};
static_assert(sizeof(TouchPayload) == 4, "Unexpected size");
using GatPayload = TouchPayload;
using GetLockedPayload = TouchPayload;

class SetCtrlTokenPayload {
public:
    uint64_t getCas() const {
        return ntohll(cas);
    }
    void setCas(uint64_t cas) {
        SetCtrlTokenPayload::cas = htonll(cas);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t cas = 0;
};
static_assert(sizeof(SetCtrlTokenPayload) == 8, "Unexpected size");

#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
class SubdocPayloadParser {
public:
    explicit SubdocPayloadParser(cb::const_byte_buffer extras)
        : extras(extras) {
    }

    uint16_t getPathlen() const {
        return ntohs(*reinterpret_cast<const uint16_t*>(extras.data()));
    }

    protocol_binary_subdoc_flag getSubdocFlags() const {
        return static_cast<protocol_binary_subdoc_flag>(
                extras[sizeof(uint16_t)]);
    }

    uint32_t getExpiry() const {
        if (extras.size() == 7 || extras.size() == 8) {
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data() + 3));
        }
        return 0;
    }

    ::mcbp::subdoc::doc_flag getDocFlag() const {
        if (extras.size() == 4 || extras.size() == 8) {
            return static_cast<::mcbp::subdoc::doc_flag>(extras.back());
        }
        return ::mcbp::subdoc::doc_flag::None;
    }

    bool isValid() const {
        using ::mcbp::subdoc::doc_flag;
        switch (extras.size()) {
        case 3: // just the mandatory fields
        case 4: // including doc flags
        case 7: // including expiry
        case 8: // including expiry and doc flags
            if ((uint8_t(0xf8) & uint8_t(getDocFlag())) != 0) {
                return false;
            }
            return true;
        default:
            return false;
        }
    }

protected:
    cb::const_byte_buffer extras;
};

class SubdocMultiPayloadParser {
public:
    explicit SubdocMultiPayloadParser(cb::const_byte_buffer extras)
        : extras(extras) {
    }

    uint32_t getExpiry() const {
        if (extras.size() == 4 || extras.size() == 5) {
            return ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
        }
        return 0;
    }

    ::mcbp::subdoc::doc_flag getDocFlag() const {
        if (extras.size() == 1 || extras.size() == 5) {
            return static_cast<::mcbp::subdoc::doc_flag>(extras.back());
        }
        return ::mcbp::subdoc::doc_flag::None;
    }

    bool isValid() const {
        using ::mcbp::subdoc::doc_flag;
        switch (extras.size()) {
        case 0: // None specified
        case 1: // Only doc flag
        case 4: // Expiry time
        case 5: // Expiry time and doc flag
            if ((uint8_t(0xf8) & uint8_t(getDocFlag())) != 0) {
                return false;
            }
            return true;
        default:
            return false;
        }
    }

protected:
    cb::const_byte_buffer extras;
};

#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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

/* DCP related stuff */

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
class DcpOpenPayload {
public:
    uint32_t getSeqno() const {
        return ntohl(seqno);
    }
    void setSeqno(uint32_t seqno) {
        DcpOpenPayload::seqno = htonl(seqno);
    }
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpOpenPayload::flags = htonl(flags);
    }

    // Flags is a bitmask where the following values is used
    static const uint32_t Producer = 1;
    static const uint32_t Notifier = 2;
    /**
     * Indicate that the server include the documents' XATTRs
     * within mutation and deletion bodies.
     */
    static const uint32_t IncludeXattrs = 4;
    /**
     * Indicate that the server should strip off the values (note,
     * if you add INCLUDE_XATTR those will be present)
     */
    static const uint32_t NoValue = 8;
    static const uint32_t Unused = 16;
    /**
     * Request that DCP delete message include the time the a delete was
     * persisted. This only applies to deletes being backfilled from storage,
     * in-memory deletes will have a delete time of 0
     */
    static const uint32_t IncludeDeleteTimes = 32;

    /**
     * Indicates that the server should strip off the values, but return the
     * datatype of the underlying document (note, if you add
     * INCLUDE_XATTR those will be present).
     * Note this differs from DCP_OPEN_NO_VALUE in that the datatype field will
     * contain the underlying datatype of the document; not the datatype of the
     * transmitted payload.
     * This flag can be used to obtain the full, original datatype for a
     * document without the user's value. Not valid to specify with
     * DCP_OPEN_NO_VALUE.
     */
    static const uint32_t NoValueWithUnderlyingDatatype = 64;

protected:
    uint32_t seqno = 0;
    uint32_t flags = 0;
};
static_assert(sizeof(DcpOpenPayload) == 8, "Unexpected struct size");
} // namespace request

namespace response {
class DcpAddStreamPayload {
public:
    uint32_t getOpaque() const {
        return ntohl(opaque);
    }
    void setOpaque(uint32_t opaque) {
        DcpAddStreamPayload::opaque = htonl(opaque);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t opaque = 0;
};
static_assert(sizeof(DcpAddStreamPayload) == 4, "Unexpected struct size");
} // namespace response

namespace request {
class DcpAddStreamPayload {
public:
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpAddStreamPayload::flags = htonl(flags);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
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
    uint32_t flags = 0;
};
static_assert(sizeof(DcpAddStreamPayload) == 4, "Unexpected struct size");

class DcpStreamReqPayload {
public:
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpStreamReqPayload::flags = htonl(flags);
    }
    uint32_t getReserved() const {
        return ntohl(reserved);
    }
    void setReserved(uint32_t reserved) {
        DcpStreamReqPayload::reserved = htonl(reserved);
    }
    uint64_t getStartSeqno() const {
        return ntohll(start_seqno);
    }
    void setStartSeqno(uint64_t start_seqno) {
        DcpStreamReqPayload::start_seqno = htonll(start_seqno);
    }
    uint64_t getEndSeqno() const {
        return ntohll(end_seqno);
    }
    void setEndSeqno(uint64_t end_seqno) {
        DcpStreamReqPayload::end_seqno = htonll(end_seqno);
    }
    uint64_t getVbucketUuid() const {
        return ntohll(vbucket_uuid);
    }
    void setVbucketUuid(uint64_t vbucket_uuid) {
        DcpStreamReqPayload::vbucket_uuid = htonll(vbucket_uuid);
    }
    uint64_t getSnapStartSeqno() const {
        return ntohll(snap_start_seqno);
    }
    void setSnapStartSeqno(uint64_t snap_start_seqno) {
        DcpStreamReqPayload::snap_start_seqno = htonll(snap_start_seqno);
    }
    uint64_t getSnapEndSeqno() const {
        return ntohll(snap_end_seqno);
    }
    void setSnapEndSeqno(uint64_t snap_end_seqno) {
        DcpStreamReqPayload::snap_end_seqno = htonll(snap_end_seqno);
    }

protected:
    uint32_t flags = 0;
    uint32_t reserved = 0;
    uint64_t start_seqno = 0;
    uint64_t end_seqno = 0;
    uint64_t vbucket_uuid = 0;
    uint64_t snap_start_seqno = 0;
    uint64_t snap_end_seqno = 0;
};
static_assert(sizeof(DcpStreamReqPayload) == 48, "Unexpected struct size");

class DcpStreamEndPayload {
public:
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpStreamEndPayload::flags = htonl(flags);
    }

protected:
    /**
     * All flags set to 0 == OK,
     * 1: state changed
     */
    uint32_t flags = 0;
};
static_assert(sizeof(DcpStreamEndPayload) == 4, "Unexpected struct size");

class DcpSnapshotMarkerV1Payload {
public:
    uint64_t getStartSeqno() const {
        return ntohll(start_seqno);
    }
    void setStartSeqno(uint64_t start_seqno) {
        DcpSnapshotMarkerV1Payload::start_seqno = htonll(start_seqno);
    }
    uint64_t getEndSeqno() const {
        return ntohll(end_seqno);
    }
    void setEndSeqno(uint64_t end_seqno) {
        DcpSnapshotMarkerV1Payload::end_seqno = htonll(end_seqno);
    }
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpSnapshotMarkerV1Payload::flags = htonl(flags);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t start_seqno = 0;
    uint64_t end_seqno = 0;
    uint32_t flags = 0;
};
static_assert(sizeof(DcpSnapshotMarkerV1Payload) == 20,
              "Unexpected struct size");

enum class DcpSnapshotMarkerFlag : uint32_t {
    Memory = 0x01,
    Disk = 0x02,
    Checkpoint = 0x04,
    Acknowledge = 0x08
};

enum class DcpSnapshotMarkerV2xVersion : uint8_t { Zero = 0 };

// Version 2.x
class DcpSnapshotMarkerV2xPayload {
public:
    DcpSnapshotMarkerV2xPayload(DcpSnapshotMarkerV2xVersion v) : version(v) {
    }
    DcpSnapshotMarkerV2xVersion getVersion() const {
        return version;
    }
    void setVersion(DcpSnapshotMarkerV2xVersion v) {
        version = v;
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    DcpSnapshotMarkerV2xVersion version{DcpSnapshotMarkerV2xVersion::Zero};
};
static_assert(sizeof(DcpSnapshotMarkerV2xPayload) == 1,
              "Unexpected struct size");

class DcpSnapshotMarkerV2_0Value : public DcpSnapshotMarkerV1Payload {
public:
    uint64_t getMaxVisibleSeqno() const {
        return ntohll(maxVisibleSeqno);
    }
    void setMaxVisibleSeqno(uint64_t maxVisibleSeqno) {
        DcpSnapshotMarkerV2_0Value::maxVisibleSeqno = htonll(maxVisibleSeqno);
    }
    uint64_t getHighCompletedSeqno() const {
        return ntohll(highCompletedSeqno);
    }
    void setHighCompletedSeqno(uint64_t highCompletedSeqno) {
        DcpSnapshotMarkerV2_0Value::highCompletedSeqno =
                htonll(highCompletedSeqno);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t maxVisibleSeqno{0};
    uint64_t highCompletedSeqno{0};
};
static_assert(sizeof(DcpSnapshotMarkerV2_0Value) == 36,
              "Unexpected struct size");

class DcpMutationPayload {
public:
    DcpMutationPayload() = default;
    DcpMutationPayload(uint64_t by_seqno,
                       uint64_t rev_seqno,
                       uint32_t flags,
                       uint32_t expiration,
                       uint32_t lock_time,
                       uint8_t nru)
        : by_seqno(htonll(by_seqno)),
          rev_seqno(htonll(rev_seqno)),
          flags(flags),
          expiration(htonl(expiration)),
          lock_time(htonl(lock_time)),
          nru(nru) {
    }
    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpMutationPayload::by_seqno = htonll(by_seqno);
    }
    uint64_t getRevSeqno() const {
        return ntohll(rev_seqno);
    }
    void setRevSeqno(uint64_t rev_seqno) {
        DcpMutationPayload::rev_seqno = htonll(rev_seqno);
    }
    uint32_t getFlags() const {
        return flags;
    }
    void setFlags(uint32_t flags) {
        DcpMutationPayload::flags = flags;
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        DcpMutationPayload::expiration = htonl(expiration);
    }
    uint32_t getLockTime() const {
        return ntohl(lock_time);
    }
    void setLockTime(uint32_t lock_time) {
        DcpMutationPayload::lock_time = htonl(lock_time);
    }
    uint16_t getNmeta() const {
        return ntohs(nmeta);
    }
    uint8_t getNru() const {
        return nru;
    }
    void setNru(uint8_t nru) {
        DcpMutationPayload::nru = nru;
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    uint32_t flags = 0;
    uint32_t expiration = 0;
    uint32_t lock_time = 0;
    /// We don't set this anymore, but old servers may send it to us
    /// but we'll ignore it
    const uint16_t nmeta = 0;
    uint8_t nru = 0;
};
static_assert(31, "Unexpected struct size");

class DcpDeletionV1Payload {
public:
    DcpDeletionV1Payload(uint64_t _by_seqno, uint64_t _rev_seqno)
        : by_seqno(htonll(_by_seqno)), rev_seqno(htonll(_rev_seqno)) {
    }
    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpDeletionV1Payload::by_seqno = htonll(by_seqno);
    }
    uint64_t getRevSeqno() const {
        return ntohll(rev_seqno);
    }
    void setRevSeqno(uint64_t rev_seqno) {
        DcpDeletionV1Payload::rev_seqno = htonll(rev_seqno);
    }
    uint16_t getNmeta() const {
        return ntohs(nmeta);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    const uint16_t nmeta = 0;
};
static_assert(sizeof(DcpDeletionV1Payload) == 18, "Unexpected struct size");

class DcpDeleteRequestV1 {
public:
    DcpDeleteRequestV1(uint32_t opaque,
                       Vbid vbucket,
                       uint64_t cas,
                       uint16_t keyLen,
                       uint32_t valueLen,
                       protocol_binary_datatype_t datatype,
                       uint64_t bySeqno,
                       uint64_t revSeqno)
        : req{}, body(bySeqno, revSeqno) {
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        req.setExtlen(gsl::narrow<uint8_t>(sizeof(body)));
        req.setKeylen(keyLen);
        req.setBodylen(gsl::narrow<uint32_t>(sizeof(body) + keyLen + valueLen));
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));
    }

protected:
    cb::mcbp::Request req;
    DcpDeletionV1Payload body;
};
static_assert(sizeof(DcpDeleteRequestV1) == 42, "Unexpected struct size");

class DcpDeletionV2Payload {
public:
    DcpDeletionV2Payload(uint64_t by_seqno,
                         uint64_t rev_seqno,
                         uint32_t delete_time)
        : by_seqno(htonll(by_seqno)),
          rev_seqno(htonll(rev_seqno)),
          delete_time(htonl(delete_time)) {
    }
    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpDeletionV2Payload::by_seqno = htonll(by_seqno);
    }
    uint64_t getRevSeqno() const {
        return ntohll(rev_seqno);
    }
    void setRevSeqno(uint64_t rev_seqno) {
        DcpDeletionV2Payload::rev_seqno = htonll(rev_seqno);
    }
    uint32_t getDeleteTime() const {
        return ntohl(delete_time);
    }
    void setDeleteTime(uint32_t delete_time) {
        DcpDeletionV2Payload::delete_time = htonl(delete_time);
    }
    uint8_t getCollectionLen() const {
        return collection_len;
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    uint32_t delete_time = 0;
    const uint8_t collection_len = 0;
};
static_assert(sizeof(DcpDeletionV2Payload) == 21, "Unexpected struct size");

class DcpDeleteRequestV2 {
public:
    DcpDeleteRequestV2(uint32_t opaque,
                       Vbid vbucket,
                       uint64_t cas,
                       uint16_t keyLen,
                       uint32_t valueLen,
                       protocol_binary_datatype_t datatype,
                       uint64_t bySeqno,
                       uint64_t revSeqno,
                       uint32_t deleteTime)
        : req{}, body(bySeqno, revSeqno, deleteTime) {
        req.setMagic(cb::mcbp::Magic::ClientRequest);
        req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        req.setExtlen(gsl::narrow<uint8_t>(sizeof(body)));
        req.setKeylen(keyLen);
        req.setBodylen(gsl::narrow<uint32_t>(sizeof(body) + keyLen + valueLen));
        req.setOpaque(opaque);
        req.setVBucket(vbucket);
        req.setCas(cas);
        req.setDatatype(cb::mcbp::Datatype(datatype));
    }

protected:
    cb::mcbp::Request req;
    DcpDeletionV2Payload body;
};
static_assert(sizeof(DcpDeleteRequestV2) == 45, "Unexpected struct size");

class DcpExpirationPayload {
public:
    DcpExpirationPayload() = default;
    DcpExpirationPayload(uint64_t by_seqno,
                         uint64_t rev_seqno,
                         uint32_t delete_time)
        : by_seqno(htonll(by_seqno)),
          rev_seqno(htonll(rev_seqno)),
          delete_time(htonl(delete_time)) {
    }

    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpExpirationPayload::by_seqno = htonll(by_seqno);
    }
    uint64_t getRevSeqno() const {
        return ntohll(rev_seqno);
    }
    void setRevSeqno(uint64_t rev_seqno) {
        DcpExpirationPayload::rev_seqno = htonll(rev_seqno);
    }
    uint32_t getDeleteTime() const {
        return ntohl(delete_time);
    }
    void setDeleteTime(uint32_t delete_time) {
        DcpExpirationPayload::delete_time = htonl(delete_time);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    uint32_t delete_time = 0;
};
static_assert(sizeof(DcpExpirationPayload) == 20, "Unexpected struct size");

class DcpSetVBucketState {
public:
    uint8_t getState() const {
        return state;
    }
    void setState(uint8_t state) {
        DcpSetVBucketState::state = state;
    }

    bool isValid() const {
        return is_valid_vbucket_state_t(state);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint8_t state;
};
static_assert(sizeof(DcpSetVBucketState) == 1, "Unexpected struct size");

class DcpBufferAckPayload {
public:
    uint32_t getBufferBytes() const {
        return ntohl(buffer_bytes);
    }
    void setBufferBytes(uint32_t buffer_bytes) {
        DcpBufferAckPayload::buffer_bytes = htonl(buffer_bytes);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t buffer_bytes = 0;
};

static_assert(sizeof(DcpBufferAckPayload) == 4, "Unexpected struct size");

enum class DcpOsoSnapshotFlags : uint32_t {
    Start = 0x01,
    End = 0x02,
};

class DcpOsoSnapshotPayload {
public:
    DcpOsoSnapshotPayload(uint32_t flags) : flags(htonl(flags)) {
    }
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    void setFlags(uint32_t flags) {
        DcpOsoSnapshotPayload::flags = htonl(flags);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags = 0;
};
static_assert(sizeof(DcpOsoSnapshotPayload) == 4, "Unexpected struct size");

#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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
} // namespace systemevent
} // namespace mcbp

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)

class DcpSystemEventPayload {
public:
    DcpSystemEventPayload() = default;
    DcpSystemEventPayload(uint64_t by_seqno,
                          ::mcbp::systemevent::id event,
                          ::mcbp::systemevent::version version)
        : by_seqno(htonll(by_seqno)),
          event(htonl(static_cast<uint32_t>(event))),
          version(static_cast<uint8_t>(version)) {
    }

    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpSystemEventPayload::by_seqno = htonll(by_seqno);
    }
    uint32_t getEvent() const {
        return ntohl(event);
    }
    void setEvent(uint32_t event) {
        DcpSystemEventPayload::event = htonl(event);
    }
    uint8_t getVersion() const {
        return version;
    }
    void setVersion(uint8_t version) {
        DcpSystemEventPayload::version = version;
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

    /**
     * Validate that the uint32_t event field represents a valid systemevent::id
     */
    bool isValidEvent() const {
        using ::mcbp::systemevent::id;
        switch (id(getEvent())) {
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
     * Validate that the uint8_t version represents a valid systemevent::version
     */
    bool isValidVersion() const {
        using ::mcbp::systemevent::version;
        switch (version(getVersion())) {
        case version::version0:
        case version::version1:
            return true;
        }
        return false;
    }

protected:
    uint64_t by_seqno = 0;
    uint32_t event = 0;
    uint8_t version = 0;
};
static_assert(sizeof(DcpSystemEventPayload) == 13, "Unexpected struct size");

class DcpPreparePayload {
public:
    DcpPreparePayload() = default;
    DcpPreparePayload(uint64_t by_seqno,
                      uint64_t rev_seqno,
                      uint32_t flags,
                      uint32_t expiration,
                      uint32_t lock_time,
                      uint8_t nru)
        : by_seqno(htonll(by_seqno)),
          rev_seqno(htonll(rev_seqno)),
          flags(flags),
          expiration(htonl(expiration)),
          lock_time(htonl(lock_time)),
          nru(nru) {
    }
    uint64_t getBySeqno() const {
        return ntohll(by_seqno);
    }
    void setBySeqno(uint64_t by_seqno) {
        DcpPreparePayload::by_seqno = htonll(by_seqno);
    }
    uint64_t getRevSeqno() const {
        return ntohll(rev_seqno);
    }
    void setRevSeqno(uint64_t rev_seqno) {
        DcpPreparePayload::rev_seqno = htonll(rev_seqno);
    }
    uint32_t getFlags() const {
        return flags;
    }
    void setFlags(uint32_t flags) {
        DcpPreparePayload::flags = flags;
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        DcpPreparePayload::expiration = htonl(expiration);
    }
    uint32_t getLockTime() const {
        return ntohl(lock_time);
    }
    void setLockTime(uint32_t lock_time) {
        DcpPreparePayload::lock_time = htonl(lock_time);
    }
    uint8_t getNru() const {
        return nru;
    }
    void setNru(uint8_t nru) {
        DcpPreparePayload::nru = nru;
    }

    uint8_t getDeleted() const {
        return deleted;
    }
    void setDeleted(uint8_t deleted) {
        DcpPreparePayload::deleted = deleted;
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

    cb::durability::Level getDurabilityLevel() const;

    void setDurabilityLevel(cb::durability::Level level);

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    uint32_t flags = 0;
    uint32_t expiration = 0;
    uint32_t lock_time = 0;
    uint8_t nru = 0;
    // set to true if this is a document deletion
    uint8_t deleted = 0;
    uint8_t durability_level = 0;
};

static_assert(sizeof(DcpPreparePayload) == 31, "Unexpected struct size");

class DcpSeqnoAcknowledgedPayload {
public:
    explicit DcpSeqnoAcknowledgedPayload(uint64_t prepared)
        : prepared_seqno(htonll(prepared)) {
    }

    uint64_t getPreparedSeqno() const {
        return ntohll(prepared_seqno);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    // Stored in network order.
    uint64_t prepared_seqno = 0;
};
static_assert(sizeof(DcpSeqnoAcknowledgedPayload) == 8,
              "Unexpected struct size");

class DcpCommitPayload {
public:
    DcpCommitPayload(uint64_t prepared, uint64_t committed)
        : prepared_seqno(htonll(prepared)), commit_seqno(htonll(committed)) {
    }

    uint64_t getPreparedSeqno() const {
        return ntohll(prepared_seqno);
    }
    void setPreparedSeqno(uint64_t prepared_seqno) {
        DcpCommitPayload::prepared_seqno = htonll(prepared_seqno);
    }
    uint64_t getCommitSeqno() const {
        return ntohll(commit_seqno);
    }
    void setCommitSeqno(uint64_t commit_seqno) {
        DcpCommitPayload::commit_seqno = htonll(commit_seqno);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t prepared_seqno = 0;
    uint64_t commit_seqno = 0;
};
static_assert(sizeof(DcpCommitPayload) == 16, "Unexpected struct size");

class DcpAbortPayload {
public:
    DcpAbortPayload(uint64_t prepared, uint64_t aborted)
        : prepared_seqno(htonll(prepared)), abort_seqno(htonll(aborted)) {
    }

    uint64_t getPreparedSeqno() const {
        return ntohll(prepared_seqno);
    }

    void setPreparedSeqno(uint64_t seqno) {
        prepared_seqno = htonll(seqno);
    }

    uint64_t getAbortSeqno() const {
        return ntohll(abort_seqno);
    }

    void setAbortSeqno(uint64_t seqno) {
        abort_seqno = htonll(seqno);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t prepared_seqno = 0;
    uint64_t abort_seqno = 0;
};
static_assert(sizeof(DcpAbortPayload) == 16, "Unexpected struct size");

class SetParamPayload {
public:
    enum class Type : uint32_t {
        Flush = 1,
        Replication,
        Checkpoint,
        Dcp,
        Vbucket
    };

    Type getParamType() const {
        return static_cast<Type>(ntohl(param_type));
    }

    void setParamType(Type param_type) {
        SetParamPayload::param_type = htonl(static_cast<uint32_t>(param_type));
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

    bool validate() const {
        switch (getParamType()) {
        case Type::Flush:
        case Type::Replication:
        case Type::Checkpoint:
        case Type::Dcp:
        case Type::Vbucket:
            return true;
        }
        return false;
    }

protected:
    uint32_t param_type = 0;
};
static_assert(sizeof(SetParamPayload) == 4, "Unexpected size");
#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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
 * This flag is used by deleteWithMeta packets to specify if the delete sent
 * instead represents an expiration.
 */
#define IS_EXPIRATION 0x10

/**
 * This flag is used with the get meta response packet. If set it
 * specifies that the item recieved has been deleted, but that the
 * items meta data is still contained in ep-engine. Eg. the item
 * has been soft deleted.
 */
#define GET_META_ITEM_DELETED_FLAG 0x01

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)
class SetWithMetaPayload {
public:
    uint32_t getFlags() const {
        return ntohl(flags);
    }
    uint32_t getFlagsInNetworkByteOrder() const {
        return flags;
    }
    void setFlags(uint32_t flags) {
        SetWithMetaPayload::flags = htonl(flags);
    }
    void setFlagsInNetworkByteOrder(uint32_t flags) {
        SetWithMetaPayload::flags = flags;
    }
    uint32_t getExpiration() const {
        return ntohl(expiration);
    }
    void setExpiration(uint32_t expiration) {
        SetWithMetaPayload::expiration = htonl(expiration);
    }
    uint64_t getSeqno() const {
        return ntohll(seqno);
    }
    void setSeqno(uint64_t seqno) {
        SetWithMetaPayload::seqno = htonll(seqno);
    }
    uint64_t getCas() const {
        return ntohll(cas);
    }
    void setCas(uint64_t cas) {
        SetWithMetaPayload::cas = htonll(cas);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags = 0;
    uint32_t expiration = 0;
    uint64_t seqno = 0;
    uint64_t cas = 0;
};
static_assert(sizeof(SetWithMetaPayload) == 24, "Unexpected struct size");

class DelWithMetaPayload {
public:
    DelWithMetaPayload(uint32_t flags,
                       uint32_t delete_time,
                       uint64_t seqno,
                       uint64_t cas)
        : flags(htonl(flags)),
          delete_time(htonl(delete_time)),
          seqno(htonll(seqno)),
          cas(htonll(cas)) {
    }

    uint32_t getFlags() const {
        return ntohl(flags);
    }
    uint32_t getFlagsInNetworkByteOrder() const {
        return flags;
    }
    void setFlags(uint32_t flags) {
        DelWithMetaPayload::flags = htonl(flags);
    }
    uint32_t getDeleteTime() const {
        return ntohl(delete_time);
    }
    void setDeleteTime(uint32_t delete_time) {
        DelWithMetaPayload::delete_time = htonl(delete_time);
    }
    uint64_t getSeqno() const {
        return ntohll(seqno);
    }
    void setSeqno(uint64_t seqno) {
        DelWithMetaPayload::seqno = htonll(seqno);
    }
    uint64_t getCas() const {
        return ntohll(cas);
    }
    void setCas(uint64_t cas) {
        DelWithMetaPayload::cas = htonll(cas);
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags = 0;
    uint32_t delete_time = 0;
    uint64_t seqno = 0;
    uint64_t cas = 0;
};
static_assert(sizeof(DelWithMetaPayload) == 24, "Unexpected struct size");
#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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
 * The physical layout for the CMD_RETURN_META
 */
namespace cb {
namespace mcbp {
namespace request {

#pragma pack(1)

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
    uint32_t mutation_type = 0;
    uint32_t flags = 0;
    uint32_t expiration = 0;
};
static_assert(sizeof(ReturnMetaPayload) == 12, "Unexpected struct size");

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
    Vbid db_file_id = Vbid{0};
    uint32_t align_pad3 = 0;
};
#pragma pack()
static_assert(sizeof(CompactDbPayload) == 24, "Unexpected struct size");
} // namespace request
} // namespace mcbp
} // namespace cb

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
 * Body: Contains the vBucket state and/or collection id for which the vb
 *       sequence numbers are requested.
 *       Please note that these fields are optional, header.request.extlen is
 *       checked to see if they are present. If a vBucket state is not
 *       present or 0 it implies request is for all vbucket states. If
 *       collection id is not present it it implies the request is for the
 *       vBucket high seqno number.
 *
 */
typedef union {
    struct {
        protocol_binary_request_header header;
        struct {
            RequestedVBState state;
            CollectionIDType cid;
        } body;
    } message;
    uint8_t bytes[sizeof(protocol_binary_request_header) +
                  sizeof(RequestedVBState) + sizeof(CollectionIDType)];
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
 *     4| SEQNO         | SEQNO         | VBID          | VBID          |
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

namespace cb {
namespace mcbp {
namespace request {
#pragma pack(1)

class AdjustTimePayload {
public:
    enum class TimeType : uint8_t { TimeOfDay = 0, Uptime = 1 };

    uint64_t getOffset() const {
        return ntohll(offset);
    }
    void setOffset(uint64_t offset) {
        AdjustTimePayload::offset = htonll(offset);
    }
    TimeType getTimeType() const {
        return time_type;
    }
    void setTimeType(TimeType time_type) {
        AdjustTimePayload::time_type = time_type;
    }

    bool isValid() const {
        switch (getTimeType()) {
        case TimeType::TimeOfDay:
        case TimeType::Uptime:
            return true;
        }
        return false;
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t offset = 0;
    TimeType time_type = TimeType::TimeOfDay;
};
static_assert(sizeof(AdjustTimePayload) == 9, "Unexpected struct size");

/**
 * Message format for PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL
 *
 * See engines/ewouldblock_engine for more information.
 */
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

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t mode = 0; // See EWB_Engine_Mode
    uint32_t value = 0;
    uint32_t inject_error = 0; // ENGINE_ERROR_CODE to inject.
};
static_assert(sizeof(EWB_Payload) == 12, "Unepected struct size");

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
class GetErrmapPayload {
public:
    uint16_t getVersion() const {
        return ntohs(version);
    }
    void setVersion(uint16_t version) {
        GetErrmapPayload::version = htons(version);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint16_t version = 0;
};
static_assert(sizeof(GetErrmapPayload) == 2, "Unexpected struct size");
#pragma pack()
} // namespace request
} // namespace mcbp
} // namespace cb

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
