/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <boost/optional/optional_fwd.hpp>

#include <memory>
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

/// Allow for methods to optionally accept a seqno
using OptionalSeqno = boost::optional<int64_t>;

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

std::ostream& operator<<(std::ostream&, const EvictionPolicy& policy);

enum class TaskStatus {
    Reschedule, /* Reschedule for later */
    Complete, /* Complete in this run */
    Abort /* Abort task immediately */
};
