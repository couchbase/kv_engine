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
 *
 * Implementation
 * --------------
 *
 * The BackfillManager owns a number of Backfill objects which are advanced by
 * an asynchronous (background) BackfillManagerTask. The BackfillManagerTask
 * is repeatedly scheduled as long as there is at least one Backfill ready to
 * run.
 *
 * The different Backfill objects reside in a series of queues, which are
 * used to (a) limit the number of Backfills in progress at any one time
 * (b) apply suitable scheduling to the active Backfills.
 *
 * The following queues exist:
 *
 * - initializing - Newly-scheduled Backfills are initially placed here. These
 *                  Backfills will be run first before any others.
 * - pending - Newly-scheduled Backfills are initially placed here if too many
 *             Backfills are already scheduled.
 * - active - Backfills which are actively being run. These backfills are run
 *            in queue order as long as no backfills exist in
 *            initializingBackfills. The order these are executed in depends on
 *            scheduleOrder.
 * - snoozing - Backfills which are not ready to run at present. They will be
 *              periodically be re-considered for running (and moving to
 *              activeBackfills)
 *
 * The lifecycle of a Backfill is:
 *
 *          schedule()
 *               |
 *               V
 *     exceeded active limit?
 *       /                \
 *      Yes                No
 *      |                  |
 *      V                  |
 *    [pendingBackfills]   |
 *    [back       front]   |
 *                    |    |
 *                    +----/
 *                    |
 *                    V
 *              [initializingBackfills]
 *              [back            front]
 *                                   |    /----------------------\
 *                                   |    |                      |
 *                                   V    V                      |
 *                              [activeBackfills]                |
 *                              [back      front]                |
 *                                            |                  |
 *                                          run()                |
 *                                            |                  |
 *                                     backfill result?          |
 *                                    /       |       \          |
 *                                   /        |        \         |
 *                              snooze    finished    success    |
 *                                 /          |          \______/|
 *       /------------------------/           V                  |
 *      V                                   [END]                |
 *   [snoozingBackfills]                                         |
 *   [back        front]                                         |
 *                    |                                          |
 *        activeBackfills space available                        |
 *                    \_________________________________________/
 *
 */
#pragma once

#include "dcp/backfill.h"
#include <memcached/engine_common.h>
#include <memcached/types.h>
#include <list>
#include <mutex>

class Configuration;
struct BackfillTrackingIface;
class DcpProducer;
class GlobalTask;
class KVBucket;
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
    /**
     * Construct a BackfillManager to manage backfills for a DCP Producer.
     * @param kvBucket Bucket DCP Producer belongs to (used to check memory
     *        usage and if Backfills should be paused).
     * @param backfillTracker Object which tracks how many backfills are
     *        in progress, and tells BackfillManager when it should
     *        set new backfills as pending.
     * @param scanByteLimit Maximum number of bytes a single scan() call can
     *        produce from disk before yielding.
     * @param scanItemLimit Maximum number of items a single scan() call can
     *        produce from disk before yielding.
     * @param backfillByteLimit Maximum number of bytes allowed in backfill
     *        buffer before pausing Backfills (until bytes are drained via
     *        bytesSent()).
     */
    BackfillManager(KVBucket& kvBucket,
                    BackfillTrackingIface& backfillTracker,
                    size_t scanByteLimit,
                    size_t scanItemLimit,
                    size_t backfillByteLimit);

    /**
     * Construct a BackfillManager, using values for scanByteLimit,
     * scanItemLimit and backfillByteLimit from the specified Configuration
     * object.
     */
    BackfillManager(KVBucket& kvBucket,
                    BackfillTrackingIface& dcpConnmap,
                    const Configuration& config);

    virtual ~BackfillManager();

    void addStats(DcpProducer& conn, const AddStatFn& add_stat, const void* c);

    /// The scheduling order for DCPBackfills
    enum class ScheduleOrder {
        /**
         * Run the first DCPBackfill, if not finished then add to back of queue
         * before running the next DCPBackfill.
         * This is the default.
         */
        RoundRobin,
        /// Run the first DCPBackfill to completion before starting on the next.
        Sequential,
    };
    /**
     * Sets the order by which backfills are scheduled:
     */
    void setBackfillOrder(ScheduleOrder order);

    enum class ScheduleResult {
        Active,
        Pending,
    };
    /**
     * Transfer ownership of the specified DCPBackfill to the BackfillManager.
     * If the maximum backfills have not been reached, then add to the set of
     * active Backfills, waking up the BackfillTask if necessary.
     * If the maximum has been reached, then add to the set of pending
     * backfills.
     */
    ScheduleResult schedule(UniqueDCPBackfillPtr backfill);

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

    /**
     * Get the current number of tracked backfills.
     *
     * Only used within tests.
     */
    size_t getNumBackfills() const {
        return initializingBackfills.size() + activeBackfills.size() +
               snoozingBackfills.size() + pendingBackfills.size();
    }

    std::string to_string(ScheduleOrder order);

protected:
    //! The buffer is the total bytes used by all backfills for this connection
    struct {
        size_t bytesRead;
        size_t maxBytes;
        size_t nextReadSize;
        bool full;
    } buffer;

    /**
     * This records the amount of data backfilled by the current backfill scan.
     * When either the maximum item or byte limit is reached then the backfill
     * is paused and yields.
     * This ensures that a single execution of the BackfillManager task doesn't
     * monopolise an AuxIO thread unfairly.
     */
    BackfillScanBuffer scanBuffer;

private:
    /**
     * Move Backfills which are pending to the New backfill queue while there
     * is available capacity.
     */
    void movePendingToInitializing();

    /**
     * Move Backfills which are snoozing to the Active queue if they have
     * snoozed for long enough.
     */
    void moveSnoozingToActiveQueue();

    /// The source queue of a dequeued backfill
    enum class Source {
        Initializing, // initializingQueue
        Active, // activeQueue
    };

    /**
     * Dequeues the next Backfill to run from the new / activeBackfills queue
     * as appropriate.
     * @returns the next Backfill to run, or null if there are no backfills
     *         available.
     *         The queue which the backfill was dequeued off, if non-null.
     */
    std::pair<UniqueDCPBackfillPtr, Source> dequeueNextBackfill(
            std::unique_lock<std::mutex>&);

    std::mutex lock;

    // List of backfills which have just been scheduled (added to Backfill
    // Manager) and not yet been run.
    //
    // BackfillManager will select from this list first (before
    // activeBackfills) to ensure that newly added backfills can initialise
    // themselves in a timely fashion (e.g. open a disk file as soon as
    // possible to minimise inconsistency across different vBucket files being
    // opened).
    std::list<UniqueDCPBackfillPtr> initializingBackfills;

    // List of backfills in the "Active" state - i.e. have been initialised
    // and are ready to be run to provide more data. The next backfill to
    // be run is taken from the head of this list.
    std::list<UniqueDCPBackfillPtr> activeBackfills;

    // List of backfills which are "snoozed" - they are not ready to run yet
    // (for example the requested seqno is not yet persisted).
    // Each element is a pair of the last run time and the Backfill.
    // They are re-added to activeBackfills every sleepTime seconds to retry.
    std::list<std::pair<rel_time_t, UniqueDCPBackfillPtr> > snoozingBackfills;

    //! When the number of (activeBackfills + snoozingBackfills) crosses a
    //!   threshold we use pendingBackfills
    std::list<UniqueDCPBackfillPtr> pendingBackfills;
    // KVBucket this BackfillManager is associated with.
    KVBucket& kvBucket;
    // The object tracking how many backfills are in progress. This tells
    // BackfillManager when to place new Backfills on the pending list (if
    // too many are already in progress).
    BackfillTrackingIface& backfillTracker;
    ExTask managerTask;
    ScheduleOrder scheduleOrder{ScheduleOrder::RoundRobin};
};
