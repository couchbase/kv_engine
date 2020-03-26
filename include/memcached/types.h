#pragma once

#include <memcached/dockey.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <optional>

#ifdef WIN32
// Need DWORD and ssize_t (used to be defined in platform/platform.h
#ifndef WIN32_LEAN_AND_MEAN
#define DO_UNDEF_WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <folly/portability/Windows.h>
#ifdef DO_UNDEF_WIN32_LEAN_AND_MEAN
#undef WIN32_LEAN_AND_MEAN
#endif
typedef SSIZE_T ssize_t;
typedef unsigned int useconds_t;
#else
#include <sys/uio.h>
#endif

#include "vbucket.h"

/**
 * Time relative to server start. Smaller than time_t on 64-bit systems.
 */
typedef uint32_t rel_time_t;

/**
 * Engine storage operations.
 */
typedef enum {
    OPERATION_ADD = 1, /**< Store with add semantics */
    OPERATION_SET = 2, /**< Store with set semantics */
    OPERATION_REPLACE = 3, /**< Store with replace semantics */
    OPERATION_CAS = 6 /**< Store with set semantics. */
} ENGINE_STORE_OPERATION;

typedef enum {
    CONN_PRIORITY_HIGH,
    CONN_PRIORITY_MED,
    CONN_PRIORITY_LOW
} CONN_PRIORITY;

/**
 * Data common to any item stored in memcached.
 */
typedef void item;

/**
 * A unique_ptr to use with items returned from the engine interface.
 */
namespace cb {
class ItemDeleter;
using unique_item_ptr = std::unique_ptr<item, ItemDeleter>;
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

struct item_info {
    uint64_t cas{0};
    uint64_t vbucket_uuid{0};
    uint64_t seqno{0};
    uint64_t revid{0};
    time_t exptime{0}; /**< When the item will expire (absolute time) */
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
    DocKey key{nullptr, 0, DocKeyEncodesCollectionId::No};
};

/* Forward declaration of the server handle -- to be filled in later */
typedef struct server_handle_v1_t SERVER_HANDLE_V1;

/* Information to uniquely identify (and order) a mutation. */
typedef struct {
    uint64_t vbucket_uuid; /** vBucket UUID for this mutation. */
    uint64_t seqno; /** sequence number of the mutation. */
} mutation_descr_t;

/* Value used to distinguish one bucket from another */
typedef uint32_t bucket_id_t;

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
