/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#pragma once

#include "dockey_view.h"

#include <gsl/gsl-lite.hpp>
#include <memcached/vbucket.h>
#include <platform/define_enum_class_bitmask_functions.h>
#include <platform/socket.h>

#ifndef WIN32
#include <arpa/inet.h>
#endif
#include <cstdint>
#include <stdexcept>
#include <string>

namespace cb::durability {
enum class Level : uint8_t;
} // namespace cb::durability

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
#include <mcbp/protocol/dcp_stream_end_status.h>
#include <mcbp/protocol/feature.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>
#include <memcached/range_scan_id.h>

#pragma pack(1)

/**
 * Definition of the packet used by set, add and replace
 * See section 4
 */
namespace cb::mcbp::request {
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
    int64_t getEpoch() const {
        return ntohll(epoch);
    }

    void setEpoch(int64_t ep) {
        epoch = htonll(ep);
    }

    int64_t getRevision() const {
        return ntohll(revision);
    }

    void setRevision(int64_t rev) {
        revision = htonll(rev);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

    void validate() const {
        if (getRevision() < 1) {
            throw std::invalid_argument(
                    "Revision number must not be less than 1");
        }

        if (getEpoch() < 1 && getEpoch() != -1) {
            throw std::invalid_argument(
                    "Epoch must not be less than 1 (or -1)");
        }
    }

protected:
    int64_t epoch{0};
    int64_t revision{0};
};
static_assert(sizeof(SetClusterConfigPayload) == 16, "Unexpected struct size");
using GetClusterConfigPayload = SetClusterConfigPayload;

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

class SetBucketDataLimitExceededPayload {
public:
    cb::mcbp::Status getStatus() const {
        return cb::mcbp::Status(ntohs(status));
    }
    void setStatus(cb::mcbp::Status val) {
        status = htons(uint16_t(val));
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint16_t status = 0;
};
static_assert(sizeof(SetBucketDataLimitExceededPayload) == sizeof(uint16_t),
              "Unexpected struct size");
} // namespace cb::mcbp::request

namespace cb::mcbp::subdoc {

/// Definitions of sub-document path flags (this is a bitmap)
enum class PathFlag : uint8_t {
    /// No flags set
    None = 0x0,
    /// (Mutation) Should non-existent intermediate paths be created?
    Mkdir_p = 0x01,
    /// 0x02 is unused
    Unused_0x02 = 0x02,
    /// If set, the path refers to an Extended Attribute (XATTR).
    /// If clear, the path refers to a path inside the document body.
    XattrPath = 0x04,
    /// 0x08 is unused
    Unused_0x08 = 0x08,
    /// Expand macro values inside extended attributes. The request is
    /// invalid if this flag is set without SUBDOC_FLAG_XATTR_PATH being set.
    ExpandMacros = 0x10,
    /// The provided value is a binary value
    BinaryValue = 0x20,

};
std::string format_as(PathFlag flag);

DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(PathFlag);

inline bool hasExpandedMacros(PathFlag flag) {
    return isFlagSet(flag, PathFlag::ExpandMacros);
}
inline bool hasXattrPath(PathFlag flag) {
    return isFlagSet(flag, PathFlag::XattrPath);
}
inline bool hasMkdirP(PathFlag flag) {
    return isFlagSet(flag, PathFlag::Mkdir_p);
}
inline bool hasBinaryValue(PathFlag flag) {
    return isFlagSet(flag, PathFlag::BinaryValue);
}

/// Definitions of sub-document doc flags (this is a bitmap).
enum class DocFlag : uint8_t {
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
     *
     * If specified with CreateAsDeleted, then the document must not have a
     * tombstone (in addition to not being in the alive state) - i.e.
     * Add|CreateAsDeleted can only succeed if there is no alive document or
     * no tombstone for the given key.
     */
    Add = 0x02,

    /**
     * Allow access to XATTRs for deleted documents (instead of
     * returning KEY_ENOENT). The result of mutations on a deleted
     * document is still a deleted document unless ReviveDocument is
     * being used.
     */
    AccessDeleted = 0x04,

    /**
     * (Mutation) Used with Mkdoc / Add; if the document does not exist then
     * create it in the Deleted state, instead of the normal Alive state.
     * Not valid unless Mkdoc or Add specified.
     */
    CreateAsDeleted = 0x08,

    /**
     * (Mutation) If the document exists and isn't deleted the operation
     * will fail with SubdocCanOnlyReviveDeletedDocuments. If the input
     * document _is_ deleted the result of the operation will store the
     * document as a "live" document instead of a deleted document.
     */
    ReviveDocument = 0x10,

    /**
     * (Lookup) Operate on a replica vbucket instead of an active one
     */
    ReplicaRead = 0x20,
};
std::string format_as(DocFlag flag);

DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(DocFlag);

/**
 * Used for validation at parsing the doc-flags.
 * The value depends on how many bits the doc_flag enum is actually
 * using and must change accordingly.
 */
static constexpr DocFlag extrasDocFlagMask =
        ~(DocFlag::Mkdoc | DocFlag::Add | DocFlag::AccessDeleted |
          DocFlag::CreateAsDeleted | DocFlag::ReviveDocument |
          DocFlag::ReplicaRead);

inline bool hasAccessDeleted(DocFlag a) {
    return isFlagSet(a, DocFlag::AccessDeleted);
}
inline bool hasReplicaRead(DocFlag a) {
    return isFlagSet(a, DocFlag::ReplicaRead);
}
inline bool hasMkdoc(DocFlag a) {
    return isFlagSet(a, DocFlag::Mkdoc);
}
inline bool hasAdd(DocFlag a) {
    return isFlagSet(a, DocFlag::Add);
}
inline bool hasReviveDocument(DocFlag a) {
    return isFlagSet(a, DocFlag::ReviveDocument);
}
inline bool hasCreateAsDeleted(DocFlag a) {
    return isFlagSet(a, DocFlag::CreateAsDeleted);
}
inline bool isNone(DocFlag a) {
    return a == DocFlag::None;
}
inline bool impliesMkdir_p(DocFlag a) {
    return hasAdd(a) || hasMkdoc(a);
}

} // namespace cb::mcbp::subdoc

/**
 * Definition of the packet used by SUBDOCUMENT single-path commands.
 *
 * The path, which is always required, is in the Body, after the Key.
 *
 *   Header:                        24 @0: <cb::mcbp::Request>
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
struct protocol_binary_request_subdocument {
    cb::mcbp::Request header;
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
};
static_assert(sizeof(protocol_binary_request_subdocument) == 27);

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
 *    Header:                24 @0:  <cb::mcbp::Request>
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

struct protocol_binary_subdoc_multi_lookup_spec {
    cb::mcbp::ClientOpcode opcode;
    uint8_t flags;
    uint16_t pathlen;
    /* uint8_t path[pathlen] */
};
static_assert(sizeof(protocol_binary_subdoc_multi_lookup_spec) == 4);

/*
 *
 * SUBDOC_MULTI_MUTATION
 *    Header:                24 @0:  <cb::mcbp::Request>
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
struct protocol_binary_subdoc_multi_mutation_spec {
    cb::mcbp::ClientOpcode opcode;
    uint8_t flags;
    uint16_t pathlen;
    uint32_t valuelen;
    /* uint8_t path[pathlen] */
    /* uint8_t value[valuelen]  */
};
static_assert(sizeof(protocol_binary_subdoc_multi_mutation_spec) == 8);

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

/* DCP related stuff */

namespace cb::mcbp {

/// DcpOpenFlag is a bitmask where the following values are used:
enum class DcpOpenFlag : uint32_t {
    /// No flags set
    None = 0x0,
    /**
     * If set a Producer connection should be opened, if clear a Consumer
     * connection should be opened.
     */
    Producer = 1,
    /// Invalid - should not be set (Previously the Notifier flag)
    Invalid = 2,
    /**
     * Indicate that the server include the documents' XATTRs
     * within mutation and deletion bodies.
     */
    IncludeXattrs = 4,
    /**
     * Indicate that the server should strip off the values (note,
     * if you add INCLUDE_XATTR those will be present)
     */
    NoValue = 8,
    Unused = 16,
    /**
     * Request that DCP delete message include the time the a delete was
     * persisted. This only applies to deletes being backfilled from storage,
     * in-memory deletes will have a delete time of 0
     */
    IncludeDeleteTimes = 32,
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
    NoValueWithUnderlyingDatatype = 64,
    /// The old (now removed) PiTR
    PiTR_Unsupported = 128,
    /**
     * Indicates that the server includes the document UserXattrs within
     * deletion values.
     */
    IncludeDeletedUserXattrs = 256,
};
std::string format_as(DcpOpenFlag flag);

DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(DcpOpenFlag);

/// DcpAddStreamFlag is a bitmask where the following values are used:
enum class DcpAddStreamFlag : uint32_t {
    None = 0,
    TakeOver = 1,
    DiskOnly = 2,
    /**
     * Request that the server sets the end-seqno (ignoring any client input)
     * The end-seqno is set to the current high-seqno of the requested vbucket.
     */
    ToLatest = 4,
    /**
     * This flag is not used anymore, and should NOT be
     * set. It is replaced by DCP_OPEN_NO_VALUE.
     */
    NoValue = 8,
    /**
     * Indicate the server to add stream only if the vbucket
     * is active.
     * If the vbucket is not active, the stream request fails with
     * error cb::engine_errc::not_my_vbucket
     */
    ActiveVbOnly = 16,
    /**
     * Indicate the server to check for vb_uuid match even at start_seqno 0
     * before adding the stream successfully. If the flag is set and there is a
     * vb_uuid mismatch at start_seqno 0, then the server returns
     * cb::engine_errc::rollback error.
     */
    StrictVbUuid = 32,
    /**
     * Request that the server sets the start-seqno to the vbucket high-seqno.
     * The client is stating they have no DCP history and are not resuming, thus
     * the input snapshot start/end and UUID are ignored. Only supported for
     * stream_request (produce from latest)
     */
    FromLatest = 64,
    /**
     * Request that the server skips rolling back if the client is behind the
     * purge seqno, but the request is otherwise valid and satifiable (i.e. no
     * other rollback checks such as UUID mismatch fail). The client could end
     * up missing purged tombstones (and hence could end up never being told
     * about a document deletion). The intent of this flag is to allow clients
     * who ignore deletes to avoid rollbacks to zero which are sorely due to
     * them being behind the purge seqno.
     */
    IgnorePurgedTombstones = 128
};
std::string format_as(DcpAddStreamFlag flag);
DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(DcpAddStreamFlag);

namespace request {

class DcpOpenPayload {
public:
    uint32_t getSeqno() const {
        return ntohl(seqno);
    }
    void setSeqno(uint32_t seqno) {
        DcpOpenPayload::seqno = htonl(seqno);
    }
    DcpOpenFlag getFlags() const {
        return static_cast<DcpOpenFlag>(ntohl(flags));
    }
    void setFlags(DcpOpenFlag value) {
        flags = htonl(static_cast<uint32_t>(value));
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

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
    DcpAddStreamFlag getFlags() const {
        return static_cast<DcpAddStreamFlag>(ntohl(flags));
    }
    void setFlags(DcpAddStreamFlag flag) {
        flags = htonl(static_cast<uint32_t>(flag));
    }
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags = 0;
};

static_assert(sizeof(DcpAddStreamPayload) == 4, "Unexpected struct size");

class DcpStreamReqPayloadV1 {
public:
    DcpAddStreamFlag getFlags() const {
        return static_cast<DcpAddStreamFlag>(ntohl(flags));
    }
    void setFlags(DcpAddStreamFlag value) {
        flags = htonl(static_cast<uint32_t>(value));
    }
    uint32_t getReserved() const {
        return ntohl(reserved);
    }
    void setReserved(uint32_t reserved) {
        DcpStreamReqPayloadV1::reserved = htonl(reserved);
    }
    uint64_t getStartSeqno() const {
        return ntohll(start_seqno);
    }
    void setStartSeqno(uint64_t start_seqno) {
        DcpStreamReqPayloadV1::start_seqno = htonll(start_seqno);
    }
    uint64_t getEndSeqno() const {
        return ntohll(end_seqno);
    }
    void setEndSeqno(uint64_t end_seqno) {
        DcpStreamReqPayloadV1::end_seqno = htonll(end_seqno);
    }
    uint64_t getVbucketUuid() const {
        return ntohll(vbucket_uuid);
    }
    void setVbucketUuid(uint64_t vbucket_uuid) {
        DcpStreamReqPayloadV1::vbucket_uuid = htonll(vbucket_uuid);
    }
    uint64_t getSnapStartSeqno() const {
        return ntohll(snap_start_seqno);
    }
    void setSnapStartSeqno(uint64_t snap_start_seqno) {
        DcpStreamReqPayloadV1::snap_start_seqno = htonll(snap_start_seqno);
    }
    uint64_t getSnapEndSeqno() const {
        return ntohll(snap_end_seqno);
    }
    void setSnapEndSeqno(uint64_t snap_end_seqno) {
        DcpStreamReqPayloadV1::snap_end_seqno = htonll(snap_end_seqno);
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
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
static_assert(sizeof(DcpStreamReqPayloadV1) == 48, "Unexpected struct size");

class DcpStreamEndPayload {
public:
    DcpStreamEndStatus getStatus() const {
        return DcpStreamEndStatus(ntohl(status));
    }
    void setStatus(DcpStreamEndStatus status) {
        DcpStreamEndPayload::status = htonl(uint32_t(status));
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    /**
     * Note the following is maintained in network/big endian
     * see protocol/dcp_stream_end_status.h for values
     */
    uint32_t status = 0;
};
static_assert(sizeof(DcpStreamEndPayload) == 4, "Unexpected struct size");

enum class DcpSnapshotMarkerFlag : uint32_t {
    None = 0x00,
    Memory = 0x01,
    Disk = 0x02,
    Checkpoint = 0x04,
    Acknowledge = 0x08,
    History = 0x10,
    MayContainDuplicates = 0x20
};
std::string format_as(DcpSnapshotMarkerFlag flag);

template <typename BasicJsonType>
void to_json(BasicJsonType& j, DcpSnapshotMarkerFlag type) {
    j = format_as(type);
}

DEFINE_ENUM_CLASS_BITMASK_FUNCTIONS(DcpSnapshotMarkerFlag);

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
    DcpSnapshotMarkerFlag getFlags() const {
        return static_cast<DcpSnapshotMarkerFlag>(ntohl(flags));
    }
    void setFlags(DcpSnapshotMarkerFlag value) {
        flags = htonl(static_cast<uint32_t>(value));
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

enum class DcpSnapshotMarkerV2xVersion : uint8_t { Zero = 0, One = 1 };

// Version 2.x
class DcpSnapshotMarkerV2xPayload {
public:
    explicit DcpSnapshotMarkerV2xPayload(DcpSnapshotMarkerV2xVersion v)
        : version(v) {
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

    std::string_view getBuffer() const {
        return {reinterpret_cast<const char*>(this), sizeof(*this)};
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
static_assert(sizeof(DcpMutationPayload) == 31, "Unexpected struct size");

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
        req.setDatatype(datatype);
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
    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
    uint64_t rev_seqno = 0;
    uint32_t delete_time = 0;
    const uint8_t unused = 0;
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
        req.setDatatype(datatype);
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
    explicit DcpOsoSnapshotPayload(uint32_t flags) : flags(htonl(flags)) {
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

class DcpSeqnoAdvancedPayload {
public:
    explicit DcpSeqnoAdvancedPayload(uint64_t seqno) : by_seqno(htonll(seqno)) {
    }
    [[nodiscard]] uint64_t getSeqno() const {
        return ntohll(by_seqno);
    }
    void setSeqno(uint64_t seqno) {
        DcpSeqnoAdvancedPayload::by_seqno = htonll(seqno);
    }
    [[nodiscard]] cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t by_seqno = 0;
};
static_assert(sizeof(DcpSeqnoAdvancedPayload) == 8, "Unexpected struct size");

} // namespace request
} // namespace cb::mcbp

/**
 * Events that the system may send
 */
namespace mcbp::systemevent {

enum class id : uint32_t {
    CreateCollection = 0, // Since 7.0 (epoch of SystemEvents)
                          // Since 8.0 can move (be seen again/updated for a
                          // flush). todo: rename as Collection
    DeleteCollection = 1, // Since 7.0 (epoch of SystemEvents)
    CreateScope = 3, // Since 7.0 (epoch of SystemEvents)
    DropScope = 4, // Since 7.0 (epoch of SystemEvents)
    ModifyCollection = 5 // Since 7.2 (requires opt-in on DCP)
};

// versions define the format of the value transmitted by a SystemEvent.
// see docs/dcp/documentation/commands/system_event.md
enum class version : uint8_t { version0 = 0, version1 = 1, version2 = 2 };
} // namespace mcbp::systemevent

namespace cb::mcbp::request {

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
        case id::CreateScope:
        case id::DropScope:
        case id::ModifyCollection:
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
        case version::version2:
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
} // namespace cb::mcbp::request

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

namespace cb::mcbp::request {

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
} // namespace cb::mcbp::request

/**
 * Structure holding getMeta command response fields
 */

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

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }
};

static_assert(sizeof(GetMetaResponse) == 21, "Incorrect compiler padding");

/* Meta data versions for GET_META */
enum class GetMetaVersion : uint8_t {
    V1 = 1, // returns deleted, flags, expiry and seqno
    V2 = 2, // The 'spock' version returns V1 + the datatype
};

/**
 * The physical layout for the CMD_RETURN_META
 */
namespace cb::mcbp::request {

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

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
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
 *         the vbid field will be used for compaction.
 * Body:
 * - purge_before_ts:  Deleted items whose expiry timestamp is less
 *                     than purge_before_ts will be purged.
 * - purge_before_seq: Deleted items whose sequence number is less
 *                     than purge_before_seq will be purged.
 * - drop_deletes:     whether to purge deleted items or not.
 * - vbid  :     Database file id for the underlying store.
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

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint64_t purge_before_ts = 0;
    uint64_t purge_before_seq = 0;
    uint8_t drop_deletes = 0;
    uint8_t align_pad1 = 0;
    Vbid db_file_id = Vbid{0};
    uint32_t align_pad3 = 0;
};
static_assert(sizeof(CompactDbPayload) == 24, "Unexpected struct size");
} // namespace cb::mcbp::request

/// The values for the state of a key in Observe
enum class ObserveKeyState : uint8_t {
    NotPersisted = 0x00,
    Persisted = 0x01,
    NotFound = 0x80,
    LogicalDeleted = 0x81
};

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
struct protocol_binary_request_get_all_vb_seqnos {
    cb::mcbp::Request header;
    struct {
        RequestedVBState state;
        CollectionIDType cid;
    } body;
};
static_assert(sizeof(protocol_binary_request_get_all_vb_seqnos) ==
              sizeof(cb::mcbp::Request) + sizeof(RequestedVBState) +
                      sizeof(CollectionIDType));

namespace cb::mcbp::request {

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
    uint32_t inject_error = 0; // cb::engine_errc to inject.
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
} // namespace cb::mcbp::request

/**
 * @}
 */

namespace cb::mcbp::cas {
/// The special value used as a wildcard and match all CAS values
constexpr uint64_t Wildcard = 0x0;

/// The special value returned from Get and similar when document is locked.
constexpr uint64_t Locked = 0xffff'ffff'ffff'ffff;
} // namespace cb::mcbp::cas

namespace cb::mcbp::request {

// Payload for get_collection_id opcode 0xbb, data stored in network byte order
class GetCollectionIDPayload {
public:
    GetCollectionIDPayload() = default;
    GetCollectionIDPayload(uint64_t manifestId, CollectionID collectionId)
        : manifestId(htonll(manifestId)),
          collectionId(htonl(uint32_t(collectionId))) {
    }

    CollectionID getCollectionId() const {
        return ntohl(collectionId);
    }

    uint64_t getManifestId() const {
        return ntohll(manifestId);
    }

    std::string_view getBuffer() const {
        return {reinterpret_cast<const char*>(this), sizeof(*this)};
    }

protected:
    uint64_t manifestId{0};
    uint32_t collectionId{0};
};
static_assert(sizeof(GetCollectionIDPayload) == 12,
              "Incorrect compiler padding");

// Payload for get_scope_id opcode 0xbc, data stored in network byte order
class GetScopeIDPayload {
public:
    GetScopeIDPayload() = default;
    GetScopeIDPayload(uint64_t manifestId, ScopeID scopeId)
        : manifestId(htonll(manifestId)), scopeId(htonl(uint32_t(scopeId))) {
    }
    ScopeID getScopeId() const {
        return ntohl(scopeId);
    }

    uint64_t getManifestId() const {
        return ntohll(manifestId);
    }

    std::string_view getBuffer() const {
        return {reinterpret_cast<const char*>(this), sizeof(*this)};
    }

protected:
    uint64_t manifestId{0};
    uint32_t scopeId{0};
};
static_assert(sizeof(GetScopeIDPayload) == 12, "Incorrect compiler padding");

// Payload for get_rando_key opcode 0xb6, data stored in network byte order
class GetRandomKeyPayload {
public:
    GetRandomKeyPayload() = default;
    explicit GetRandomKeyPayload(uint32_t collectionId)
        : collectionId(htonl(collectionId)) {
    }

    CollectionID getCollectionId() const {
        return ntohl(collectionId);
    }

    std::string_view getBuffer() const {
        return {reinterpret_cast<const char*>(this), sizeof(*this)};
    }

protected:
    CollectionIDType collectionId{0};
};
static_assert(sizeof(GetRandomKeyPayload) == 4, "Incorrect compiler padding");

// Payload for range_scan_continue opcode 0xdb, data in network byte order
class RangeScanContinuePayload {
public:
    RangeScanContinuePayload() = default;
    explicit RangeScanContinuePayload(cb::rangescan::Id id,
                                      uint32_t itemLimit,
                                      uint32_t timeLimit,
                                      uint32_t byteLimit)
        : id(id),
          itemLimit(htonl(itemLimit)),
          timeLimit(htonl(timeLimit)),
          byteLimit(htonl(byteLimit)) {
    }

    cb::rangescan::Id getId() const {
        return id;
    }

    uint32_t getItemLimit() const {
        return ntohl(itemLimit);
    }

    uint32_t getTimeLimit() const {
        return ntohl(timeLimit);
    }

    size_t getByteLimit() const {
        return ntohll(byteLimit);
    }

    std::string_view getBuffer() const {
        return {reinterpret_cast<const char*>(this), sizeof(*this)};
    }

protected:
    cb::rangescan::Id id{};
    uint32_t itemLimit{0};
    uint32_t timeLimit{0};
    uint32_t byteLimit{0};
};
static_assert(sizeof(RangeScanContinuePayload) == 28,
              "Incorrect compiler padding");

} // namespace cb::mcbp::request

namespace cb::mcbp::response {

class RangeScanContinueMetaResponse {
public:
    RangeScanContinueMetaResponse() = default;

    explicit RangeScanContinueMetaResponse(uint32_t flags,
                                           uint32_t expiry,
                                           uint64_t seqno,
                                           uint64_t cas,
                                           uint8_t datatype)
        : flags(flags),
          expiry(htonl(expiry)),
          seqno(htonll(seqno)),
          cas(htonll(cas)),
          datatype(datatype) {
    }

    uint32_t getFlags() const {
        return flags;
    }
    uint32_t getExpiry() const {
        return htonl(expiry);
    }
    uint64_t getSeqno() const {
        return htonll(seqno);
    }
    uint64_t getCas() const {
        return htonll(cas);
    }
    uint8_t getDatatype() const {
        return datatype;
    }

    cb::const_byte_buffer getBuffer() const {
        return {reinterpret_cast<const uint8_t*>(this), sizeof(*this)};
    }

protected:
    uint32_t flags{0};
    uint32_t expiry{0};
    uint64_t seqno{0};
    uint64_t cas{0};
    uint8_t datatype{0};
};

static_assert(sizeof(RangeScanContinueMetaResponse) == 25,
              "Incorrect compiler padding");

} // namespace cb::mcbp::response

#pragma pack()
