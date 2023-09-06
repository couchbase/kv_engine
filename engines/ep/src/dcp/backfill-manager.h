/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
 * - dcp_backfill_in_progress_per_connection_limit
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
 * Backfills are in one of two states:
 * - pending - Backfills which are waiting to be started. In this state they
 *             do not consume any disk resources. Newly-scheduled Backfills
 *             are initially placed here if too many Backfills are already
 *             in-progress.
 *
 * - in-progress - Backfills which have opened a (typically disk) snapshot and
 *                 are in the middle of reading data. We limit the number of
 *                 in-progress backfills - both at the bucket level and
 *                 per-BackfillManager as all in-progress Backfills consume
 *                 resources (file descriptors, memory for ephemeral) and hence
 *                 we only allow a finite number.
 *
 * For in-progress backfills the following sub-types exist:
 *
 * - initializing - Newly-scheduled Backfills are initially placed here. These
 *                  Backfills will be run first before any others.
 * - active - Backfills which are actively being run. These backfills are run
 *            in queue order as long as no backfills exist in
 *            initializingBackfills. The order these are executed in depends on
 *            scheduleOrder.
 * - snoozing - Backfills which are not ready to run at present. They will be
 *              periodically be re-considered for running (and moving to
 *              activeBackfills)
 *
 * There are four queues - pendingBackfills, initializingBackfills,
 * activeBackfills and snoozingBackfills - which hold the backfills of each
 * (sub)type.
 *
 * The lifecycle of a Backfill is:
 *
 *          schedule()
 *               |
 *               V
 *     exceeded in-progress limit?
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
 *        snoozed for more than snoozeTime?                      |
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
class CookieIface;
class KVStoreScanTracker;
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
     * @param scanTracker Object which tracks how many scans are
     *        in progress, and tells BackfillManager when it should
     *        set new backfills as pending.
     * @param name The name of the BackfillManager; used for logging etc.
     * @param scanByteLimit Maximum number of bytes a single scan() call can
     *        produce from disk before yielding.
     * @param scanItemLimit Maximum number of items a single scan() call can
     *        produce from disk before yielding.
     * @param backfillByteLimit Maximum number of bytes allowed in backfill
     *        buffer before pausing Backfills (until bytes are drained via
     *        bytesSent()).
     */
    BackfillManager(KVBucket& kvBucket,
                    KVStoreScanTracker& scanTracker,
                    std::string name,
                    size_t scanByteLimit,
                    size_t scanItemLimit,
                    size_t backfillByteLimit);

    /**
     * Construct a BackfillManager, using values for scanByteLimit,
     * scanItemLimit and backfillByteLimit from the specified Configuration
     * object.
     */
    BackfillManager(KVBucket& kvBucket,
                    KVStoreScanTracker& scanTracker,
                    std::string name,
                    const Configuration& config);

    virtual ~BackfillManager();

    void addStats(DcpProducer& conn, const AddStatFn& add_stat, CookieIface& c);

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
     * Notify the BackfillManager that the given number of bytes have been
     * sent (consumed) by the DcpProducer to the client. Reduces the amount
     * of buffer consumed by the given value. If the buffer was previously
     * full but is no longer, wakes up BackfillManagerTask.
     */
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

    /**
     * Look in all of the internal containers for a backfill matching the given
     * UID - if found remove it.
     *
     * Note that this function has a O(n) worst case cost, where n is sum(size)
     * of all 4 internal lists.
     *
     * @param backfillUID The uid of the backfill to try and remove
     * @return true if found (and removed), false if not found
     */
    bool removeBackfill(uint64_t backfillUID);

    std::string to_string(ScheduleOrder order);

    void setBackfillByteLimit(size_t bytes);

    size_t getBackfillByteLimit() const;

    /// The name of the BackfillManager, used for logging etc
    const std::string name;

protected:
    /**
     * The structure is used for controlling how many bytes backfill can push to
     * the streams readyQ(s) for a Producer connection.
     * Bytes are "written" (added) into bytesRead every time a backfill pushes
     * some data to its stream readyQ.
     * Bytes are "read" (removed) from bytesRead every time a the frontend
     * processes the Producer connection, pulls data from a stream readyQ and
     * sends data over the network.
     * Backfills managed by this Producer connection are paused if/when
     * bytesRead hits maxBytes.
     */
    struct {
        size_t bytesRead;
        size_t maxBytes;
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

    /**
     * Move Backfills which are pending to the New backfill queue while there
     * is available capacity.
     */
    void movePendingToInitializing(const std::unique_lock<std::mutex>& lh);

    /**
     * Move Backfills which are snoozing to the Active queue if they have
     * snoozed for long enough.
     */
    void moveSnoozingToActiveQueue();

    /// @returns the number of backfills in progress
    int getNumInProgressBackfills(const std::unique_lock<std::mutex>& lh) const;

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

    /**
     * @param lock Ref to this BackfillManager lock
     * @return Whether all backfill queues are empty
     */
    bool emptyQueues(std::unique_lock<std::mutex>& lock) const;

    mutable std::mutex lock;

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

    /**
     * Count of backfills which are in progress (initializing, active, snoozing)
     * but are not currently tracked in one of the backfill queues - i.e. they
     * have been dequeued for running, and not yet added back to the appropriate
     * list.
     * Used by getNumInProgressBackfills() to enforce the limit on the number
     * of running backfills.
     */
    int numInProgressUntrackedBackfills{0};

    // KVBucket this BackfillManager is associated with.
    KVBucket& kvBucket;
    // The object tracking how many scans are in progress. This tells
    // BackfillManager when to place new Backfills on the pending list (if
    // too many are already in progress).
    KVStoreScanTracker& scanTracker;
    ExTask managerTask;
    ScheduleOrder scheduleOrder{ScheduleOrder::RoundRobin};
};
