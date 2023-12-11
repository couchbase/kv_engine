/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "queue_op.h"
#include "utilities/lock_utilities.h"

#include <folly/container/F14Map-fwd.h>
#include <memcached/vbucket.h>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

// Forward declarations for types defined elsewhere.
class Item;
class Checkpoint;
class CheckpointCursor;

template <class T, class Pointer, class Deleter>
class SingleThreadedRCPtr;

using queued_item = SingleThreadedRCPtr<Item, Item*, std::default_delete<Item>>;
using UniqueItemPtr = std::unique_ptr<Item>;

// Enumerations representing binary states - more explicit than using a generic
// bool.
enum class GenerateBySeqno : char { No, Yes };
enum class GenerateRevSeqno : char { No, Yes };
enum class GenerateCas : char { No, Yes };
enum class GenerateDeleteTime { No, Yes };
enum class TrackCasDrift : char { No, Yes };
enum class WantsDeleted : char { No, Yes };
enum class TrackReference : char { No, Yes };
enum class CheckConflicts : char { No, Yes };
enum class IsSystem : char { No, Yes };
enum class IsDeleted : char { No, Yes };
enum class IsCommitted : char { No, Yes };
enum class IsCompaction : char { No, Yes };
enum class IsPiTR : char { No, Yes };
enum class CanDeduplicate : char { No, Yes };

static inline CanDeduplicate getCanDeduplicateFromHistory(bool value) {
    // history:true => CanDeduplicate::No
    return value ? CanDeduplicate::No : CanDeduplicate::Yes;
}

static inline bool getHistoryFromCanDeduplicate(CanDeduplicate value) {
    // CanDeduplicate::No => history:true
    return value == CanDeduplicate::No;
}

std::string to_string(CanDeduplicate);
std::ostream& operator<<(std::ostream&, const CanDeduplicate&);

// Is the compaction callback invoked for the latest revision only, or any
// revision?
enum class CompactionCallbacks : char {
    // Compaction callbacks are made for only the latest revision of a document
    // i.e. Couchstore
    LatestRevision,
    // Compactions callbacks may be made for any revision of a document (not
    // necessarily the latest revision i.e. Magma
    AnyRevision
};

/// Types of write operations that can be issued to a KVStore.
enum class WriteOperation : char {
    /// Upsert first does a lookup and accordingly either updates or inserts an
    /// item.
    Upsert,
    /// Insert always inserts an item without checking if it already exists.
    /// This improves write performance for some KVStore implementations (e.g.
    /// Magma).
    Insert
};

enum class CheckpointType : uint8_t {
    /**
     * Disk checkpoints are received from disk snapshots from active nodes but
     * may exist on an active node due to replica promotion.
     */
    Disk = 0,

    /**
     * Default checkpoint type
     */
    Memory,

    /**
     * Initial disk checkpoints are disk snapshots received by a replica when
     * the replica has no items for the vbucket being streamed i.e. its
     * highSeqno=0.
     *
     * This type is a subtype of Disk checkpoint i.e. every InitialDisk
     * checkpoint is a Disk checkpoint. In most cases, method
     * isDiskCheckpointType can be used when the distinction between the two is
     * not required.
     */
    InitialDisk,
};

// Returns true if given type is either a Disk checkpoint or its subtype.
bool isDiskCheckpointType(CheckpointType type);

/**
 * Encodes whether the snapshot stored in a checkpoint is part of a historical
 * sequence of mutation.
 */
enum class CheckpointHistorical : uint8_t { No, Yes };

enum class ConflictResolutionMode {
    /// Resolve conflicts based on document revision id (revid).
    RevisionId,
    /// Resolve conflicts based on "last write" (most recently modified)
    LastWriteWins,
    /// Resolve conflicts based on custom conflict resolution. Currently
    /// acts the same as 'LastWriteWins' in ep-engine.
    Custom,
};

/**
 * We need to send SyncWrite Commits as Mutations when backfilling from disk as
 * the preceding prepare may have been de-duped and the replica needs to know
 * what to commit.
 */
enum class SendCommitSyncWriteAs : char { Commit, Mutation };

/**
 * Interface for event-driven checking of SyncWrites which have exceeded their
 * timeout and aborting them.
 * A VBucket's ActiveDurabilityMonitor will call updateNextExpiryTime()
 * with the time when the "next" SyncWrite will expire. At that time, the
 * implementation of this interface will call into the VBucket to check for
 * (and abort if found) any expired SyncWrites.
 */
struct EventDrivenDurabilityTimeoutIface {
    virtual ~EventDrivenDurabilityTimeoutIface() = default;

    /// Update the time when the next SyncWrite will expire.
    virtual void updateNextExpiryTime(
            std::chrono::steady_clock::time_point next) = 0;

    /**
     * Cancel the next run of the durability timeout task (as there are no
     * SyncWrites requiring timeout).
     */
    virtual void cancelNextExpiryTime() = 0;
};

/**
 * Used by setVBucketState - indicates that the vbucket is transferred
 * to the active post a failover and/or rebalance.
 */
enum class TransferVB : char { No, Yes };
std::ostream& operator<<(std::ostream&, TransferVB transfer);

std::string to_string(GenerateBySeqno generateBySeqno);
std::string to_string(GenerateCas generateCas);
std::string to_string(TrackCasDrift trackCasDrift);
std::string to_string(CheckpointType checkpointType);

static inline bool isCheckpointHistorical(CheckpointHistorical historical) {
    return historical == CheckpointHistorical::Yes;
}
std::string to_string(CheckpointHistorical historical);
std::ostream& operator<<(std::ostream&, const CheckpointHistorical& policy);

struct snapshot_range_t {
    snapshot_range_t(uint64_t start, uint64_t end) : start(start), end(end) {
        checkInvariant();
    }

    void setStart(uint64_t value) {
        start = value;
        checkInvariant();
    }

    void setEnd(uint64_t value) {
        end = value;
        checkInvariant();
    }

    uint64_t getStart() const {
        return start;
    }

    uint64_t getEnd() const {
        return end;
    }

    bool operator==(const snapshot_range_t& rhs) const {
        return (start == rhs.start) && (end == rhs.end);
    }

private:
    void checkInvariant() const {
        if (start > end) {
            throw std::runtime_error(
                    "snapshot_range_t(" + std::to_string(start) + "," +
                    std::to_string(end) + ") requires start <= end");
        }
    }

    uint64_t start;
    uint64_t end;
};

std::ostream& operator<<(std::ostream&, const snapshot_range_t&);

struct snapshot_info_t {
    snapshot_info_t(uint64_t start, snapshot_range_t range)
        : start(start), range(range) {
    }
    uint64_t start;
    snapshot_range_t range;
};

std::ostream& operator<<(std::ostream&, const snapshot_info_t&);

/**
 * The following options can be specified
 * for retrieving an item for get calls
 */
enum get_options_t {
    NONE = 0x0000, // no option
    TRACK_STATISTICS = 0x0001, // whether statistics need to be tracked or not
    QUEUE_BG_FETCH = 0x0002, // whether a background fetch needs to be queued
    HONOR_STATES = 0x0004, // whether a retrieval should depend on the state
    // of the vbucket
    TRACK_REFERENCE = 0x0008, // whether NRU bit needs to be set for the item
    DELETE_TEMP = 0x0010, // whether temporary items need to be deleted
    HIDE_LOCKED_CAS = 0x0020, // whether locked items should have their CAS
    // hidden (return -1).
    GET_DELETED_VALUE = 0x0040, // whether to retrieve value of a deleted item
    ALLOW_META_ONLY = 0x0080 // Allow only the meta to be returned for an item
};

/// Used to identify if QUEUE_BG_FETCH option is set
enum class QueueBgFetch {Yes, No};

/**
 * Used to inform a function whether a get request is for the replica or active
 * item
 */
enum class ForGetReplicaOp { No, Yes };

/// Allow for methods to optionally accept a seqno
using OptionalSeqno = std::optional<int64_t>;

/// Determine the GenerateBySeqno value from an OptionalSeqno
GenerateBySeqno getGenerateBySeqno(const OptionalSeqno& seqno);

/**
 * Result of a HighPriorityVB request
 */
enum class HighPriorityVBReqStatus {
    NotSupported,
    RequestScheduled,
    RequestNotScheduled
};

/**
 * Value of vbucket's hlc seqno epoch before any data is stored
 */
const int64_t HlcCasSeqnoUninitialised = -1;

/**
 * Item eviction policy
 */
enum class EvictionPolicy {
    Value, // Only evict an item's value.
    Full // Evict an item's key, metadata and value together.
};

std::string to_string(EvictionPolicy);
std::ostream& operator<<(std::ostream&, const EvictionPolicy& policy);

/**
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum class ExpireBy { Pager, Compactor, Access };
std::string to_string(ExpireBy);
std::ostream& operator<<(std::ostream& out, const ExpireBy& source);

enum class TaskStatus {
    Reschedule, /* Reschedule for later */
    Complete, /* Complete in this run */
    Abort /* Abort task immediately */
};

enum class VBucketStatsDetailLevel {
    State, // Only the vbucket state
    PreviousState, // Only the vb.initialState
    Durability, // state, high_seqno, topology, high_prepared_seqno
    Full, // All the vbucket stats
};

namespace internal {
struct VBucketStateLockTag;
} // namespace internal

/**
 * An opaque reference to a lock on the state of the VBucket.
 */
using VBucketStateLockRef = cb::SharedLockRef<internal::VBucketStateLockTag>;

/**
 * A mapping from Vbid to a state lock held on that VBucket.
 */
template <typename Lock>
using VBucketStateLockMap = folly::F14FastMap<Vbid, Lock>;

/**
 * Properties of the storage layer.
 */
class StorageProperties {
public:
    enum class ByIdScan : bool { Yes, No };

    /**
     * Will the KVStore de-dupe items such that only the highest seqno for any
     * given key in a single flush batch is persisted?
     */
    enum class AutomaticDeduplication : bool { Yes, No };

    /**
     * Will the KVStore count items in the prepare namespace (and update the
     * values appropriately in the vbstate)
     */
    enum class PrepareCounting : bool { Yes, No };

    /**
     * Will the KVStore make callbacks with stale (superseded) items during
     * compaction?
     */
    enum class CompactionStaleItemCallbacks : bool { Yes, No };

    /**
     * Does the KVStore support history retention (suitable for change streams)
     */
    enum class HistoryRetentionAvailable : bool { Yes, No };

    StorageProperties(ByIdScan byIdScan,
                      AutomaticDeduplication automaticDeduplication,
                      PrepareCounting prepareCounting,
                      CompactionStaleItemCallbacks compactionStaleItemCallbacks,
                      HistoryRetentionAvailable historyRetentionAvailable)
        : byIdScan(byIdScan),
          automaticDeduplication(automaticDeduplication),
          prepareCounting(prepareCounting),
          compactionStaleItemCallbacks(compactionStaleItemCallbacks),
          historyRetentionAvailable(historyRetentionAvailable) {
    }

    bool hasByIdScan() const {
        return byIdScan == ByIdScan::Yes;
    }

    bool hasAutomaticDeduplication() const {
        return automaticDeduplication == AutomaticDeduplication::Yes;
    }

    bool hasPrepareCounting() const {
        return prepareCounting == PrepareCounting::Yes;
    }

    bool hasCompactionStaleItemCallbacks() const {
        return compactionStaleItemCallbacks ==
               CompactionStaleItemCallbacks::Yes;
    }

    bool canRetainHistory() const {
        return historyRetentionAvailable == HistoryRetentionAvailable::Yes;
    }

private:
    ByIdScan byIdScan;
    AutomaticDeduplication automaticDeduplication;
    PrepareCounting prepareCounting;
    CompactionStaleItemCallbacks compactionStaleItemCallbacks;
    HistoryRetentionAvailable historyRetentionAvailable;
};

// The type of the snapshot
//
// History - A single snapshot that represents history, all updates to keys
//           will be returned.
// NoHistory - A single snapshot which does not have history, all keys are
//             the most recent updates.
//
// Note a CDC backfill can be the combination of NoHistory then History and
// the following two values indicate this distinction.
//
// NoHistoryPrecedingHistory - A snapshot that represents no-history and will
//                     precede a snapshot which does include history.
// HistoryFollowingNoHistory - A history snapshot which follows the non-history.
//
// NoHistoryPrecedingHistory/HistoryFollowingNoHistory exists to indicate the
// case when a disk snapshot has both History and NoHistory ranges - in this
// case markDiskSnapshot for example will get invoked twice by the same source
// backfill, and markDiskSnapshot needs to ensure cursors don't get
// re-registered for example.
enum SnapshotType {
    History,
    NoHistory,
    NoHistoryPrecedingHistory,
    HistoryFollowingNoHistory
};

namespace cb {

/**
 * How should errors be handled?
 */
enum class ErrorHandlingMethod {
    // Log the error
    Log,
    // Log the error then throw an exception
    Throw,
    // Log the error then abort
    Abort
};

} // namespace cb

#include <fmt/ostream.h>
#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<CanDeduplicate> : ostream_formatter {};
template <>
struct fmt::formatter<TransferVB> : ostream_formatter {};
template <>
struct fmt::formatter<CheckpointHistorical> : ostream_formatter {};
template <>
struct fmt::formatter<snapshot_range_t> : ostream_formatter {};
template <>
struct fmt::formatter<snapshot_info_t> : ostream_formatter {};
template <>
struct fmt::formatter<EvictionPolicy> : ostream_formatter {};
template <>
struct fmt::formatter<ExpireBy> : ostream_formatter {};
#endif
