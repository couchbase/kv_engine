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

#include "range_scans/range_scan.h"

#include <boost/functional/hash.hpp>
#include <folly/Synchronized.h>
#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan_id.h>
#include <memcached/vbucket.h>

#include <queue>
#include <unordered_map>
#include <unordered_set>

class Configuration;
class EPBucket;
class KVStoreIface;
class RangeScanContinueTask;

/**
 * ReadyRangeScans keeps a reference (shared_ptr) to all scans that are ready
 * for execution on a I/O task. These are scans that the client has continued or
 * cancelled.
 */
class ReadyRangeScans {
public:
    ReadyRangeScans(const Configuration& config);

    /**
     * Add a scan to the 'ready' scans container and if required create a new
     * RangeScanContinueTask for processing the scan.
     *
     * RangeScans are 'exeucted' by a set of RangeScanContinueTasks and
     * there can be fewer tasks than scans. The set of tasks can keep asking
     * for work (see takeNextScan) until all scans are completed. Tasks then
     * exit until calls to addScan re-populate the set of tasks.
     *
     * @param bucket The bucket of the scan - needed for task creation
     * @param scan The scan to add (increasing shared ownership)
     */
    void addScan(EPBucket& bucket, std::shared_ptr<RangeScan> scan);

    /**
     * Take the next available scan out of the 'ready' scans container. Range
     * scans are 'executed' by the RangeScanContinueTask, and those tasks are
     * crated by this object (as part of addScan). The task taking the next
     * scan must provide their taskId so that this class can account for the
     * case where the task is told to terminate (return value is null).
     *
     * @param taskId id of the RangeScanContinueTask that is asking for work
     * @return the next task to execute or nullptr. A return value of null also
     *         requires the task to exit.
     */
    std::shared_ptr<RangeScan> takeNextScan(size_t taskId);

    /**
     * Method will set concurrentTaskLimit using the parameter value and the
     * AUXIO thread pool size. The parameter specifies the number of threads
     * that can run a range scan or if 0 auto configure to use num_auxio -1
     * @param maxContinueTasksValue value to use for setting concurrentTaskLimit
     */
    void setConcurrentTaskLimit(size_t maxContinueTasksValue);

    void addStats(const StatCollector& collector) const;

protected:
    size_t getReadyQueueSize() const {
        return rangeScans.rlock()->size();
    }

    size_t getTaskQueueSize() const {
        return continueTasks.rlock()->size();
    }

    std::atomic<size_t> concurrentTaskLimit;

    folly::Synchronized<std::queue<std::shared_ptr<RangeScan>>> rangeScans;

    // The IDs of the tasks that will run the range scans. The size() of this
    // container is the value that limits concurrency of continue.
    folly::Synchronized<std::unordered_set<size_t>> continueTasks;
};

namespace VB {

/**
 * RangeScanOwner owns all of the RangeScan objects (for 1 vbucket) that are
 * available to be continued or cancelled and provides methods to drive a
 * RangeScan
 *
 * Having a VBucket level container (instead of 1 KVBucket container)
 * simplifies some aspects of RangeScan life-time for when a VBucket is deleted.
 *
 */
class RangeScanOwner {
public:
    /**
     * Construct the owner with a pointer to the ReadyRangeScans (bucket
     * container). This is a pointer as some unit tests create vbuckets with
     * no bucket.
     */
    RangeScanOwner(ReadyRangeScans* scans);

    /**
     * Destructor will mark all scans as cancelled, thus if any scans happen
     * to be on task or waiting, they come to a 'quick' end
     */
    ~RangeScanOwner();

    /**
     * Add a new scan to the set of available scans.
     *
     * @param scan to add
     * @return success if added
     */
    cb::engine_errc addNewScan(std::shared_ptr<RangeScan> scan);

    /**
     * Handler for a range-scan-continue operation. Method will locate the
     * scan and make it available for running.
     *
     * Failure to locate the scan -> cb::engine_errc::no_such_key
     * Scan already continued -> cb::engine_errc::too_busy
     *
     * @param bucket The bucket of the scan
     * @param id of the scan to continue
     * @param cookie client cookie requesting the continue
     * @param itemLimit limit for the items that can be read in this continue
     * @param timeLimit an optional limit for how long the scan can run for, 0
     *        for no limit.
     * @param byteLimit A trip wire value, when the number of bytes included in
     *        the scan exceeds this value, the continue is complete.
     *        Value of 0 disables this trigger.
     * @return success or other status (see above)
     */
    cb::engine_errc continueScan(EPBucket& bucket,
                                 cb::rangescan::Id id,
                                 const CookieIface& cookie,
                                 size_t itemLimit,
                                 std::chrono::milliseconds timeLimit,
                                 size_t byteLimit);

    /**
     * Handler for a range-scan-cancel operation or a force cancel due to some
     * error. Method will locate the scan and mark it cancelled and remove it
     * from the set of known scans.
     *
     * Failure to locate the scan -> cb::engine_errc::no_such_key
     *
     * @param bucket The bucket of the scan
     * @param id of the scan to cancel
     * @param addScan should the cancelled scan be added to ::RangeScans
     * @return success or other status (see above)
     */
    cb::engine_errc cancelScan(EPBucket& bucket,
                               cb::rangescan::Id id,
                               bool addScan);

    /**
     * Call RangeScan::addStats on all RangeScan objects in the rangeScans map
     */
    cb::engine_errc doStats(const StatCollector& collector);

    /**
     * Find the scan for the given id
     */
    std::shared_ptr<RangeScan> getScan(cb::rangescan::Id id) const;

    /**
     * Handler for completed scans. A completed scan will be removed from the
     * set of known scans and allowed to destruct. The destruction is intended
     * to happen in the caller. It is possible that this call does nothing if
     * a cancellation occurs ahead of this call.
     *
     * @param id scan to complete
     */
    void completeScan(cb::rangescan::Id id);

    /**
     * Check if the caller can progress the scan with the given id by doing
     * a privilege check.
     *
     * @param id The id of the scan to check
     * @param cookie The cookie of the connection
     * @param engine required to call checkPrivilege
     * @return success if privileged
     */
    cb::engine_errc hasPrivilege(
            cb::rangescan::Id id,
            const CookieIface& cookie,
            const EventuallyPersistentEngine& engine) const;

protected:
    std::shared_ptr<RangeScan> processScanRemoval(cb::rangescan::Id id,
                                                  bool cancelled);

    ReadyRangeScans* readyScans;

    /**
     * All scans that are available for continue/cancel
     */
    folly::Synchronized<std::unordered_map<cb::rangescan::Id,
                                           std::shared_ptr<RangeScan>,
                                           boost::hash<boost::uuids::uuid>>>
            rangeScans;
};
} // namespace VB