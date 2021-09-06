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

#include <memcached/vbucket.h>
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
enum class QueueExpired : char { No, Yes };
enum class CheckConflicts : char { No, Yes };
enum class SyncWriteOperation : char { No, Yes };
enum class IsSystem : char { No, Yes };
enum class IsDeleted : char { No, Yes };
enum class IsCommitted : char { No, Yes };
enum class WantsDropped : char { No, Yes };
enum class IsCompaction : char { No, Yes };

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

enum class CheckpointType : char {
    /**
     * Disk checkpoints are received from disk snapshots from active nodes but
     * may exist on an active node due to replica promotion.
     */
    Disk,

    /**
     * Default checkpoint type
     */
    Memory,
};

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
 * Used by setVBucketState - indicates that the vbucket is transferred
 * to the active post a failover and/or rebalance.
 */
enum class TransferVB : char { No, Yes };
std::ostream& operator<<(std::ostream&, TransferVB transfer);

std::string to_string(GenerateBySeqno generateBySeqno);
std::string to_string(GenerateCas generateCas);
std::string to_string(TrackCasDrift trackCasDrift);
std::string to_string(CheckpointType checkpointType);

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
 * Indicates the type of the HighPriorityVB request causing the notify
 */
enum class HighPriorityVBNotify { Seqno, ChkPersistence };

/**
 * Overloads the to_string method to give the string format of a member of the
 * class HighPriorityVBNotify
 */
std::string to_string(HighPriorityVBNotify hpNotifyType);

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

/**
 * The following will be used to identify
 * the source of an item's expiration.
 */
enum class ExpireBy { Pager, Compactor, Access };

std::ostream& operator<<(std::ostream&, const EvictionPolicy& policy);

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
