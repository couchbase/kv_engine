/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/*
 * Types relating to the VBucket class. Exist in their own header to allow
 * them to be used without #including all of vbucket.h which is a large and
 * costly header.
 */

#include "checkpoint_types.h"
#include "ep_types.h"
#include <memcached/engine_error.h>
#include <functional>
#include <memory>

struct CheckpointSnapshotRange;
class CookieIface;
struct EventDrivenDurabilityTimeoutIface;
class Item;
class Vbid;
class VBucket;

/**
 * Callback function to be invoked by ActiveDurabilityMonitor when SyncWrite(s)
 * are ready to be resolved (either met requirements and should be Committed, or
 * cannot meet requirements and should be Aborted).
 *
 * Will normally call the DurabilityCompletionTask to wake up and process
 * those resolved SyncWrites.
 */
using SyncWriteResolvedCallback = std::function<void(Vbid vbid)>;

/**
 * Callback function invoked when an accepted SyncWrite operation has been
 * completed (has been committed / aborted / times out).
 */
using SyncWriteCompleteCallback =
        std::function<void(const CookieIface* cookie, cb::engine_errc status)>;

/// Instance of SyncWriteCompleteCallback which does nothing.
const SyncWriteCompleteCallback NoopSyncWriteCompleteCb =
        [](const CookieIface* cookie, cb::engine_errc status) {};

/**
 * Callback function invoked at Replica for sending a SeqnoAck message to the
 * Active. That is triggered at Replica by High Prepared Seqno updates within
 * the PassiveDurabilityMonitor.
 */
using SeqnoAckCallback = std::function<void(Vbid vbid, int64_t seqno)>;

using SyncWriteTimeoutHandlerFactory =
        std::function<std::unique_ptr<EventDrivenDurabilityTimeoutIface>(
                VBucket&)>;

extern const SyncWriteTimeoutHandlerFactory NoopSyncWriteTimeoutFactory;

// @todo: Remove this structure and use CM::ItemsForCursor, they are almost
//  identical. That can be easily done after we have removed the reject
//  queue, so we may want to do that within the reject queue removal.
//  Doing as part of MB-37280 otherwise.
struct ItemsToFlush {
    std::vector<queued_item> items;
    std::vector<CheckpointSnapshotRange> ranges;
    bool moreAvailable = false;
    std::optional<uint64_t> maxDeletedRevSeqno = {};
    CheckpointType checkpointType = CheckpointType::Memory;

    // See CM::ItemsForCursor for details.
    UniqueFlushHandle flushHandle;
};

/**
 * Stores flush stats for deferred update after flush-success.
 */
class AggregatedFlushStats {
public:
    /**
     * Add the stored values from the Item
     * @param Item being accounted
     */
    void accountItem(const Item& item);

    size_t getNumItems() const {
        return numItems;
    }

    size_t getTotalBytes() const {
        return totalBytes;
    }

    size_t getTotalAgeInMilliseconds() const {
        return totalAgeInMilliseconds;
    }

private:
    size_t numItems = 0;
    size_t totalBytes = 0;
    size_t totalAgeInMilliseconds = 0;
};
