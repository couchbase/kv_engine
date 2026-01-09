/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <fmt/ostream.h>
#include <mcbp/protocol/datatype.h>
#include <memcached/define_yes_no_enum.h>
#include <memcached/dockey_view.h>
#include <platform/byte_literals.h>
#include <platform/n_byte_integer.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <optional>

#ifdef WIN32
// Need DWORD and ssize_t (used to be defined in platform/platform.h
#include <folly/portability/Windows.h>
using ssize_t = SSIZE_T;
#else
#include <sys/uio.h>
#endif

#include "vbucket.h"

/**
 * Time relative to server start. Smaller than time_t on 64-bit systems.
 */
using rel_time_t = uint32_t;

/// The various semantics to use for a store operation
enum class StoreSemantics { Add, Set, Replace, CAS };
std::string to_string(const StoreSemantics ss);
std::ostream& operator<<(std::ostream& os, const StoreSemantics& ss);
inline auto format_as(StoreSemantics ss) {
    return to_string(ss);
}

enum class ConnectionPriority : uint8_t { High, Medium, Low };
std::string to_string(const ConnectionPriority cp);
std::ostream& operator<<(std::ostream& os, const ConnectionPriority& cp);
inline auto format_as(ConnectionPriority cp) {
    return to_string(cp);
}

enum class EngineParamCategory { Flush, Checkpoint, Dcp, Vbucket, Config };

std::string to_string(const EngineParamCategory epc);
std::ostream& operator<<(std::ostream& os, const EngineParamCategory& epc);
inline auto format_as(EngineParamCategory epc) {
    return to_string(epc);
}

constexpr uint64_t DEFAULT_REV_SEQ_NUM = 1;

/**
 * The ItemMetaData structure is used to pass meta data information of
 * an Item.
 */
class ItemMetaData {
public:
    ItemMetaData()
        : cas(0), revSeqno(DEFAULT_REV_SEQ_NUM), flags(0), exptime(0) {
    }

    ItemMetaData(uint64_t c, uint64_t s, uint32_t f, uint32_t e)
        : cas(c),
          revSeqno(s == 0 ? DEFAULT_REV_SEQ_NUM : s),
          flags(f),
          exptime(e) {
    }

    bool operator==(const ItemMetaData&) const = default;

    uint64_t cas;
    cb::uint48_t revSeqno;
    uint32_t flags;
    uint32_t exptime;
};
std::ostream& operator<<(std::ostream& os, const ItemMetaData& md);

DEFINE_YES_NO_ENUM(CheckConflicts)
DEFINE_YES_NO_ENUM(GenerateBySeqno)
DEFINE_YES_NO_ENUM(GenerateRevSeqno)
DEFINE_YES_NO_ENUM(GenerateCas)
DEFINE_YES_NO_ENUM(GenerateDeleteTime)

/**
 * Data common to any item stored in memcached.
 */
class ItemIface {
public:
    virtual ~ItemIface() = default;

    /// Return the Item's key.
    virtual DocKeyView getDocKey() const = 0;

    /// Return the Item's datatype.
    virtual protocol_binary_datatype_t getDataType() const = 0;

    /// Set the Item's datatype
    virtual void setDataType(protocol_binary_datatype_t val) = 0;

    /// Return the Item's CAS.
    virtual uint64_t getCas() const = 0;

    /// Set the Item's CAS
    virtual void setCas(uint64_t val) = 0;

    /// Return the user flags.
    virtual uint32_t getFlags() const = 0;

    /// Return the time the item will expire (or 0 if no expiration).
    virtual uint32_t getExptime() const = 0;

    /// Return a read-only view of the Item's raw value.
    virtual std::string_view getValueView() const = 0;

    /// Return a Read-Write "view" of the Items raw value
    virtual cb::char_buffer getValueBuffer() = 0;
};

std::ostream& operator<<(std::ostream& os, const ItemIface& item);

/**
 * A unique_ptr to use with items returned from the engine interface.
 */
namespace cb {
class ItemDeleter;
using unique_item_ptr = std::unique_ptr<ItemIface, ItemDeleter>;
} // namespace cb

/**
 * Constant value representing the masked CAS we return if an item is under lock
 */
static constexpr uint64_t LOCKED_CAS = std::numeric_limits<uint64_t>::max();

/**
 * The legal state a document may be in (from the cores perspective)
 */
enum class DocumentState : uint8_t {
    /**
     * The document is deleted from the users perspective, and trying
     * to fetch the document will return KEY_NOT_FOUND unless one
     * asks specifically for deleted documents. The Deleted documents
     * will not hang around forever and may be reaped by the purger
     * at any time (from the core's perspective. That's an internal
     * detail within the underlying engine).
     */
    Deleted = 0x0F,

    /**
     * The document is alive and all operations should work as expected.
     */
    Alive = 0xF0,
};

std::string to_string(const DocumentState& ds);
std::ostream& operator<<(std::ostream& os, const DocumentState& ds);
inline auto format_as(DocumentState ds) {
    return to_string(ds);
}

/**
 * The DocumentStateFilter is an enum which allows you to specify the state(s)
 * a document may have.
 */
enum class DocStateFilter : uint8_t {
    /// Only alive documents match this filter
    Alive = uint8_t(DocumentState::Alive),
    /// Only deleted documents match this filter
    Deleted = uint8_t(DocumentState::Deleted),
    /// The document may be alive or deleted.
    AliveOrDeleted = uint8_t(uint8_t(Alive) | uint8_t(Deleted))
};

std::string to_string(const DocStateFilter& filter);
std::ostream& operator<<(std::ostream& os, const DocStateFilter& ds);
inline auto format_as(const DocStateFilter dsf) {
    return to_string(dsf);
}

struct item_info {
    uint64_t cas{0};
    uint64_t vbucket_uuid{0};
    uint64_t seqno{0};
    uint64_t revid{0};
    uint32_t exptime{0}; /**< When the item will expire (absolute time) */
    uint32_t nbytes{0}; /**< The total size of the data (in bytes) */
    /// Flags associated with the item (in network byte order)
    uint32_t flags{0};
    uint8_t datatype{0};

    /**
     * The current state of the document (Deleted or Alive).
     */
    DocumentState document_state{DocumentState::Deleted};

    /**
     * If the xattr bit is set in datatype the first uint32_t contains
     * the size of the extended attributes which follows next, and
     * finally the actual document payload.
     */
    struct iovec value[1]{};

    /**
     * True if the CAS is a HLC timestamp
     */
    bool cas_is_hlc{false};

    /**
     * Item's DocKey
     */
    DocKeyView key{nullptr, 0, DocKeyEncodesCollectionId::No};
};

/* Information to uniquely identify (and order) a mutation. */
struct mutation_descr_t {
    uint64_t vbucket_uuid = 0; /// vBucket UUID for this mutation.
    uint64_t seqno = 0; /// sequence number of the mutation.
};

/* Value used to distinguish one bucket from another */
using bucket_id_t = uint32_t;

namespace cb {
struct vbucket_info {
    /// has the vbucket has had xattr documents written to it
    bool mayContainXattrs;
};

using ExpiryLimit = std::optional<std::chrono::seconds>;

static const ExpiryLimit NoExpiryLimit{};
}

/**
 * DeleteSource denotes the source of an item's deletion;
 * either explicitly or TTL (expired).
 */
enum class DeleteSource : uint8_t { Explicit = 0, TTL = 1 };

/**
 * Convert deletionSource to string for visual output
 */
std::string to_string(DeleteSource deleteSource);

/**
 * The committed state of the Item.
 *
 * Consists of six states: CommittedViaMutation, CommittedViaPrepare
 * Pending, PreparedMaybeVisible, PrepareAborted, and PrepareCommitted.
 * The first two are generally considered as the same 'Committed' state
 * by external observers, but internally we need to differentiate between
 * them to write to disk / send over DCP hence having two different states.
 *
 * Similarly, Pending and PreparedMaybeVisible are generally treated the same,
 * but after warmup or failover SyncWrites which _may_ already have been
 * Committed (but haven't yet been Committed locally) are marked as
 * PreparedMaybeVisible.
 *
 * PrepareAborted and PrepareCommitted are used to identify prepares that exist
 * in the HashTable after they have been completed which is required for
 * Ephemeral.
 *
 * Used in a bitfield in StoredValue hence explicit values for enums required.
 */
enum class CommittedState : char {
    /// Item is committed, by virtue of being a plain mutation - i.e. not added
    /// via a SyncWrite.
    CommittedViaMutation = 0,
    /// Item is committed by virtue of previously being a pending SyncWrite
    /// which was committed.
    CommittedViaPrepare = 1,
    /// Item is pending (is not yet committed) and hence not visible to
    /// external clients yet.
    Pending = 2,
    /// Item is prepared, but *may* have already been committed by another node
    /// or before a restart, and as such cannot allow access to *any* value
    /// for this key until the SyncWrite is committed.
    /// Same semantics as 'Pending, with the addition of blocking reads to any
    /// existing value.
    PreparedMaybeVisible = 3,
    /// Item is prepared but has been aborted. This is required mainly for
    /// Ephemeral where we need to keep completed prepares in the HashTable and
    /// need to be able to distinguish if a prepare is in-flight or completed.
    /// We also need to be able to distinguish between aborted and committed
    /// items for DCP backfill.
    PrepareAborted = 4,
    /// Item is prepared but has been committed. See also comment for
    /// PrepareAborted.
    PrepareCommitted = 5,
};

#ifndef ATOMIC_FORMATTER_DEFINED
#define ATOMIC_FORMATTER_DEFINED 1
#include <atomic>
template <typename T>
struct fmt::formatter<std::atomic<T>> : ostream_formatter {};
#endif
template <>
struct fmt::formatter<ItemIface> : ostream_formatter {};
