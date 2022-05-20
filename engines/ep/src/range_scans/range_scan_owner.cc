/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_owner.h"

#include "bucket_logger.h"
#include "configuration.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"
#include "range_scans/range_scan_continue_task.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <executor/executorpool.h>
#include <fmt/ostream.h>
#include <statistics/cbstat_collector.h>

ReadyRangeScans::ReadyRangeScans(const Configuration& config) {
    setConcurrentTaskLimit(config.getRangeScanMaxContinueTasks());
}

void ReadyRangeScans::setConcurrentTaskLimit(size_t maxContinueTasksValue) {
    auto poolSize = ExecutorPool::get()->getNumAuxIO();

    if (maxContinueTasksValue) {
        concurrentTaskLimit = std::min(maxContinueTasksValue, poolSize);
    } else {
        concurrentTaskLimit = std::max(size_t(1), poolSize - 1);
    }
}

void ReadyRangeScans::addScan(EPBucket& bucket,
                              std::shared_ptr<RangeScan> scan) {
    auto lockedQueue = rangeScans.wlock();

    // RangeScan should only be queued once. It is ok for the state to change
    // whilst queued. This isn't overly critical, but prevents a
    // continue->cancel placing the same shared_ptr in the queue twice resulting
    // in two runs of the continue task
    if (scan->isQueued()) {
        return;
    }
    lockedQueue->push(scan);
    scan->setQueued(true);

    auto lockedTasks = continueTasks.wlock();
    // If more scans that tasks, see if we can create a new task
    if (lockedQueue->size() > lockedTasks->size() &&
        lockedTasks->size() < concurrentTaskLimit) {
        // new task
        auto [itr, emplaced] =
                lockedTasks->emplace(ExecutorPool::get()->schedule(
                        std::make_shared<RangeScanContinueTask>(bucket)));
        if (!emplaced) {
            throw std::runtime_error(
                    fmt::format("ReadyRangeScans::addScan failed to add a new "
                                "task, ID collision {}",
                                *itr));
        }
    }
}

std::shared_ptr<RangeScan> ReadyRangeScans::takeNextScan(size_t taskId) {
    // Need access to both containers, but can drop the tasks set lock early
    auto lockedScans = rangeScans.wlock();
    {
        auto lockedTasks = continueTasks.wlock();
        // If no scans or the number of tasks now exceeds the limit
        // this calling task is told to exit
        if (lockedScans->empty() || lockedTasks->size() > concurrentTaskLimit) {
            // Remove the calling task from the set of tasks
            if (lockedTasks->erase(taskId) == 0) {
                throw std::runtime_error(
                        fmt::format("ReadyRangeScans::takeNextScan failed to "
                                    "remove the task {}",
                                    taskId));
            }
            // return nothing, this tells the calling task to exit
            return {};
        }
    }

    auto scan = lockedScans->front();
    lockedScans->pop();
    scan->setQueued(false);
    return scan;
}

void ReadyRangeScans::addStats(const StatCollector& collector) const {
    collector.addStat("concurrent_task_limit", concurrentTaskLimit);
    collector.addStat("tasks_size", getTaskQueueSize());
    collector.addStat("ready_queue_size", getReadyQueueSize());
}

VB::RangeScanOwner::RangeScanOwner(ReadyRangeScans* scans) : readyScans(scans) {
}

VB::RangeScanOwner::~RangeScanOwner() {
    auto locked = rangeScans.wlock();
    for (const auto& [id, scan] : *locked) {
        // mark everything we know as cancelled
        scan->setStateCancelled();
    }
}

cb::engine_errc VB::RangeScanOwner::addNewScan(
        std::shared_ptr<RangeScan> scan) {
    Expects(readyScans);
    if (rangeScans.withWLock([&scan](auto& map) {
            auto [itr, emplaced] =
                    map.try_emplace(scan->getUuid(), std::move(scan));
            return emplaced;
        })) {
        return cb::engine_errc::success;
    }
    EP_LOG_WARN("VB::RangeScanOwner::addNewScan failed to insert for uuid:{}",
                scan->getUuid());
    return cb::engine_errc::key_already_exists;
}

cb::engine_errc VB::RangeScanOwner::continueScan(
        EPBucket& bucket,
        cb::rangescan::Id id,
        const CookieIface& cookie,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit) {
    Expects(readyScans);
    EP_LOG_DEBUG(
            "VB::RangeScanOwner::continueScan {} itemLimit:{} timeLimit:{}",
            id,
            itemLimit,
            timeLimit.count());
    auto locked = rangeScans.wlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return cb::engine_errc::no_such_key;
    }

    // Only an idle scan can be continued
    if (!itr->second->isIdle()) {
        return cb::engine_errc::too_busy;
    }

    // set scan to 'continuing'
    itr->second->setStateContinuing(cookie, itemLimit, timeLimit);

    // Make the scan available to I/O task(s)
    // addScan will check if a task needs creating or scheduling to process the
    // continue
    readyScans->addScan(bucket, itr->second);

    return cb::engine_errc::success;
}

cb::engine_errc VB::RangeScanOwner::cancelScan(EPBucket& bucket,
                                               cb::rangescan::Id id,
                                               bool addScan) {
    Expects(readyScans);
    EP_LOG_DEBUG("VB::RangeScanOwner::cancelScan {} addScan:{}", id, addScan);
    auto scan = processScanRemoval(id, true);
    if (!scan) {
        return cb::engine_errc::no_such_key;
    }

    if (addScan) {
        // Make the scan available to I/O task(s) for final closure of data file
        // addScan will check if a task needs creating or scheduling to process
        // the cancel
        readyScans->addScan(bucket, scan);
    }

    // scan should now destruct here if addScan==false this case is used when
    // the I/O task itself calls cancelRangeScan, not when the worker thread
    // does.

    return cb::engine_errc::success;
}

cb::engine_errc VB::RangeScanOwner::doStats(const StatCollector& collector) {
    Expects(readyScans);
    readyScans->addStats(collector);
    auto locked = rangeScans.rlock();
    for (const auto& scan : *locked) {
        scan.second->addStats(collector);
    }
    return cb::engine_errc::success;
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::getScan(
        cb::rangescan::Id id) const {
    auto locked = rangeScans.rlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return {};
    }
    return itr->second;
}

void VB::RangeScanOwner::completeScan(cb::rangescan::Id id) {
    processScanRemoval(id, false);
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::processScanRemoval(
        cb::rangescan::Id id, bool cancelled) {
    std::shared_ptr<RangeScan> scan;
    auto locked = rangeScans.wlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return {};
    }
    // obtain the scan
    scan = itr->second;
    if (cancelled) {
        scan->setStateCancelled();
    } else {
        scan->setStateCompleted();
    }

    // Erase from the map, no further continue/cancel allowed.
    locked->erase(itr);
    return scan;
}
