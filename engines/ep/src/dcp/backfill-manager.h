/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

/**
 * The BackfillManager is responsible for multiple DCP backfill
 * operations owned by a single DCP connection.
 * It consists of two main classes:
 *
 * - BackfillManager, which acts as the main interface for adding new
 *    streams.
 * - BackfillManagerTask, which runs on a background AUXIO thread and
 *    performs most of the actual backfilling operations.
 *
 * One main purpose of the BackfillManager is to impose a limit on the
 * in-memory buffer space a streams' backfills consume - often
 * ep-engine can read data from disk faster than the client connection
 * can consume it, and so without any limits we could exhaust the
 * bucket quota and cause Items to be evicted from the HashTable,
 * which is Bad. At a high level, these limits are based on giving
 * each DCP connection a maximum amount of buffer space, and pausing
 * backfills if the buffer limit is reached. When the buffers are
 * sufficiently drained (by sending to the client), backfilling can be
 * resumed.
 *
 * Significant configuration parameters affecting backfill:
 * - dcp_scan_byte_limit
 * - dcp_scan_item_limit
 * - dcp_backfill_byte_limit
 */
#pragma once

#include "dcp/backfill.h"
#include <memcached/engine_common.h>
#include <memcached/types.h>
#include <list>
#include <mutex>

class DcpProducer;
class EventuallyPersistentEngine;
class GlobalTask;
class VBucket;
using ExTask = std::shared_ptr<GlobalTask>;

struct BackfillScanBuffer {
    size_t bytesRead;
    size_t itemsRead;
    size_t maxBytes;
    size_t maxItems;
};

class BackfillManager : public std::enable_shared_from_this<BackfillManager> {
public:
    BackfillManager(EventuallyPersistentEngine& e);

    virtual ~BackfillManager();

    void addStats(DcpProducer& conn, const AddStatFn& add_stat, const void* c);

    /**
     * Explicitly schedule a seqno range scan to perform a backfill.
     */
    void schedule(VBucket& vb,
                  std::shared_ptr<ActiveStream> stream,
                  uint64_t start,
                  uint64_t end);

    /**
     * Schedule a collection-ID backfill, this may choose to use a name
     * index to retrieve items in name order.
     */
    void schedule(VBucket& vb,
                  std::shared_ptr<ActiveStream> stream,
                  CollectionID cid);

    /**
     * Checks if the read size can fit into the backfill buffer and scan
     * buffer and reads only if the read can fit.
     *
     * @param bytes read size
     *
     * @return true upon read success
     *         false if the buffer(s) is(are) full
     */
    bool bytesCheckAndRead(size_t bytes);

    /**
     * Reads the backfill item irrespective of whether backfill buffer or
     * scan buffer is full.
     *
     * @param bytes read size
     */
    void bytesForceRead(size_t bytes);

    void bytesSent(size_t bytes);

    // Called by the managerTask to acutally perform backfilling & manage
    // backfills between the different queues.
    backfill_status_t backfill();

    void wakeUpTask();

protected:
    /**
     * Get the current number of tracked backfills.
     *
     * Only used within tests.
     */
    size_t getNumBackfills() const {
        return activeBackfills.size() + snoozingBackfills.size() +
               pendingBackfills.size();
    }

    /**
     * Common code for adding a new backfill
     */
    void addBackfill_UNLOCKED(VBucket& vb,
                              std::shared_ptr<ActiveStream> stream,
                              UniqueDCPBackfillPtr& backfill);

    //! The buffer is the total bytes used by all backfills for this connection
    struct {
        size_t bytesRead;
        size_t maxBytes;
        size_t nextReadSize;
        bool full;
    } buffer;

    //! The scan buffer is for the current stream being backfilled
    BackfillScanBuffer scanBuffer;

private:

    void moveToActiveQueue();

    std::mutex lock;
    std::list<UniqueDCPBackfillPtr> activeBackfills;
    std::list<std::pair<rel_time_t, UniqueDCPBackfillPtr> > snoozingBackfills;
    //! When the number of (activeBackfills + snoozingBackfills) crosses a
    //!   threshold we use waitingBackfills
    std::list<UniqueDCPBackfillPtr> pendingBackfills;
    EventuallyPersistentEngine& engine;
    ExTask managerTask;
};
