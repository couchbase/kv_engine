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
#include "ep_vb.h"
#include "event_driven_timeout_task.h"
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

ReadyRangeScans::ReadyRangeScans(const Configuration& config)
    : maxDuration(std::chrono::seconds(config.getRangeScanMaxLifetime())) {
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
    auto lockedScans = rangeScans.wlock();

    // RangeScan should only be queued once. It is ok for the state to change
    // whilst queued. This isn't overly critical, but prevents a
    // continue->cancel placing the same shared_ptr in the queue twice resulting
    // in two runs of the continue task
    if (scan->isQueued()) {
        return;
    }
    lockedScans->push(scan);
    scan->setQueued(true);

    auto lockedTasks = continueTasks.wlock();
    // If more scans than tasks, see if we can create a new task
    if (lockedScans->size() > lockedTasks->size() &&
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
    collector.addStat("max_duration", maxDuration.load().count());
    // Log the "current time" according to RangeScan so that any logged
    // timestamps can be better understood
    collector.addStat("current_time",
                      RangeScan::getTime().time_since_epoch().count());
}

// Task used by RangeScans for checking that any scans of a vbucket have not
// exceeded the "deadline", a configurable number of seconds each scan cannot
// exceed.
class RangeScanTimeoutTask : public EpTask {
public:
    RangeScanTimeoutTask(EventuallyPersistentEngine& engine,
                         EPVBucket& vBucket,
                         std::chrono::seconds initialSleep)
        : EpTask(engine,
                 TaskId::RangeScanTimeoutTask,
                 initialSleep.count(),
                 false),
          vBucket(vBucket),
          vbid(vBucket.getId()) {
    }

    std::string getDescription() const override {
        return fmt::format("RangeScanTimeoutTask for {}", vbid);
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo: calibrate. This is copied over from the durability equivalent
        return std::chrono::milliseconds{10};
    }

protected:
    bool run() override {
        // Call into the vbucket with the current max duration. All scans that
        // have exceeded this duration will be cancelled
        vBucket.cancelRangeScansExceedingDuration(
                vBucket.getRangeScans().getMaxDuration());

        // Task must re-run (if not shutting down). The sleep time of the task
        // is adjusted inside cancelRangeScansExceedingDuration
        return !engine->getEpStats().isShutdown;
    }

private:
    EPVBucket& vBucket;
    // Need a separate vbid member variable as getDescription() can be
    // called during Bucket shutdown (after VBucket has been deleted)
    // as part of cleaning up tasks (see
    // EventuallyPersistentEngine::waitForTasks) - and hence calling
    // into vBucket->getId() would be accessing a deleted object.
    const Vbid vbid;
};

VB::RangeScanOwner::RangeScanOwner(EPBucket* bucket, EPVBucket& vb) {
    if (bucket) {
        readyScans = bucket->getReadyRangeScans();
    }
}

VB::RangeScanOwner::~RangeScanOwner() {
    auto locked = syncData.wlock();
    for (const auto& [id, scan] : locked->rangeScans) {
        // mark everything we know as cancelled
        scan->setStateCancelled(cb::engine_errc::range_scan_cancelled);
    }
}

cb::engine_errc VB::RangeScanOwner::addNewScan(
        std::shared_ptr<RangeScan> scan,
        EPVBucket& vb,
        EventuallyPersistentEngine& engine) {
    Expects(readyScans);
    if (syncData.withWLock([&scan, &vb, &engine, this](auto& syncData) {
            auto [itr, emplaced] = syncData.rangeScans.try_emplace(
                    scan->getUuid(), std::move(scan));
            if (!syncData.timeoutTask) {
                // Create the timeout task and set the initial sleep time of the
                // task to be maxScanDuration. The task will then wake-up after
                // that duration has passed and ensure the scan is cancelled
                // if it still exists.
                syncData.timeoutTask = std::make_unique<EventDrivenTimeoutTask>(
                        std::make_shared<RangeScanTimeoutTask>(
                                engine, vb, readyScans->getMaxDuration()));
            }

            return emplaced;
        })) {
        return cb::engine_errc::success;
    }
    EP_LOG_WARN("VB::RangeScanOwner::addNewScan failed to insert for uuid:{}",
                scan->getUuid());
    return cb::engine_errc::key_already_exists;
}

std::pair<cb::engine_errc, std::unique_ptr<RangeScanContinueResult>>
VB::RangeScanOwner::continueScan(
        EPBucket& bucket,
        CookieIface& cookie,
        bool ioCompletePhase,
        const cb::rangescan::ContinueParameters& params) {
    Expects(readyScans);
    EP_LOG_DEBUG(
            "VB::RangeScanOwner::continueScan {} itemLimit:{} timeLimit:{} "
            "byteLimit:{}",
            params.uuid,
            params.itemLimit,
            params.timeLimit.count(),
            params.byteLimit);
    std::unique_ptr<RangeScanContinueResult> result;
    auto locked = syncData.wlock();
    auto itr = locked->rangeScans.find(params.uuid);
    if (itr == locked->rangeScans.end()) {
        // If the scan is gone and this is IO complete, it has been cancelled.
        // there is no data to return
        return {ioCompletePhase ? cb::engine_errc::range_scan_cancelled
                                : cb::engine_errc::no_such_key,
                nullptr};
    }

    // Note that if processScanRemoval is called the iterator will become
    // invalid. From here on down only touch the scan object directly
    auto [uuid, scan] = *itr;

    cb::engine_errc status = params.currentStatus;

    if (scan->isIdle()) {
        Expects(!ioCompletePhase);
        // set scan to 'continuing'
        scan->setStateContinuing(
                cookie, params.itemLimit, params.timeLimit, params.byteLimit);
        status = cb::engine_errc::would_block;
    } else if (scan->isContinuing()) {
        if (!ioCompletePhase) {
            // cannot begin a continue on an already continuing scan
            return {cb::engine_errc::too_busy, nullptr};
        }

        switch (status) {
        case cb::engine_errc::success:
            // success on the I/O complete phase means the I/O task finished
            // because the send buffer is full. The buffered data can now be
            // shipped off to the connection and then the I/O task runs again.
            result = scan->continuePartialOnFrontendThread(cookie);
            status = cb::engine_errc::would_block;
            break;
        case cb::engine_errc::range_scan_more:
            // range_scan_more on the I/O complete phase means the I/O task
            // reached a defined limit and the continue is complete. The buffer
            // can be shipped off to the connection and the scan set to idle
            // ready for a future continue/cancel.
            result = scan->continueMoreOnFrontendThread();
            scan->setStateIdle();
            break;
        case cb::engine_errc::range_scan_complete:
            // range_scan_complete on the I/O complete phase means the I/O task
            // reached the end of the scan range, the scan is complete. The
            // buffer can be shipped off to the connection and the scan removed.
            result = scan->completeOnFrontendThread();
            processScanRemoval(*locked, params.uuid, status);
            break;
        default:
            // Any other status means the scan is cancelled
            result = scan->cancelOnFrontendThread();
            if (status == cb::engine_errc::unknown_collection) {
                bucket.getEPEngine().setUnknownCollectionErrorContext(
                        cookie, scan->getManifestUid());
            }
            processScanRemoval(*locked, params.uuid, status);
        }
    } else {
        // If scan is in the map it's Idle or Continuing (checked above) else
        // Cancelled/Complete scans are removed from the map and should not of
        // been found.
        throw std::runtime_error(fmt::format(
                "VB::RangeScanOwner::continueScan not in an expected state {}",
                *scan));
    }

    // All statuses except more require the IO task to run, either continuing
    // the scan or triggering it to destruct (cancel/complete)
    if (status != cb::engine_errc::range_scan_more) {
        readyScans->addScan(bucket, scan);
    }

    return {status, std::move(result)};
}

cb::engine_errc VB::RangeScanOwner::cancelScan(EPBucket& bucket,
                                               cb::rangescan::Id id) {
    Expects(readyScans);
    EP_LOG_DEBUG("VB::RangeScanOwner::cancelScan {}", id);
    auto scan = processCancelledScan(id);
    if (!scan) {
        return cb::engine_errc::no_such_key;
    }

    // Make the scan available to I/O task(s) for final closure of data file
    // addScan will check if a task needs creating or scheduling to process
    // the cancel
    readyScans->addScan(bucket, scan);

    // scan should now destruct here if addScan==false this case is used when
    // the I/O task itself calls cancelRangeScan, not when the worker thread
    // does.

    return cb::engine_errc::success;
}

cb::engine_errc VB::RangeScanOwner::doStats(const StatCollector& collector) {
    Expects(readyScans);
    readyScans->addStats(collector);
    auto locked = syncData.rlock();
    for (const auto& scan : locked->rangeScans) {
        scan.second->addStats(collector);
    }
    return cb::engine_errc::success;
}

std::optional<std::chrono::seconds>
VB::RangeScanOwner::cancelAllExceedingDuration(EPBucket& bucket,
                                               std::chrono::seconds duration) {
    // Part of finding all expired tasks is also to find the task which would
    // expire next so that we can update the timeoutTask to wake-up again
    auto nextExpiry = std::chrono::seconds::max();
    auto locked = syncData.wlock();

    for (auto itr = locked->rangeScans.begin();
         itr != locked->rangeScans.end();) {
        auto remainingTime = itr->second->getRemainingTime(duration);
        if (remainingTime == std::chrono::seconds(0)) {
            itr->second->setStateCancelled(
                    cb::engine_errc::range_scan_cancelled);
            itr->second->logForTimeout();
            auto scan = itr->second;
            itr = locked->rangeScans.erase(itr);
            readyScans->addScan(bucket, scan);
        } else {
            nextExpiry = std::min(nextExpiry, remainingTime);
            ++itr;
        }
    }

    // empty before the loop or after, either case return std::nullopt as
    // there's no deadline available and cancel the timeout task (via destruct)
    if (locked->rangeScans.empty()) {
        locked->timeoutTask.reset();
        return std::nullopt;
    }

    locked->timeoutTask->updateNextExpiryTime(std::chrono::steady_clock::now() +
                                              nextExpiry);
    return nextExpiry;
}

void VB::RangeScanOwner::cancelAllScans(EPBucket& bucket) {
    EP_LOG_DEBUG_RAW("VB::RangeScanOwner::cancelAllScans");

    auto locked = syncData.wlock();
    for (auto it = locked->rangeScans.begin();
         it != locked->rangeScans.end();) {
        // Note: processScanRemoval() erases the entry from unordered_map, which
        // invalidates any reference/iterator to the erased entry. This, we
        // need to precompute 'next'.
        auto next = std::next(it);
        processScanRemoval(
                *locked, it->first, cb::engine_errc::range_scan_cancelled);
        it = next;
    }
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::getScan(
        cb::rangescan::Id id) const {
    auto locked = syncData.rlock();
    auto itr = locked->rangeScans.find(id);
    if (itr == locked->rangeScans.end()) {
        return {};
    }
    return itr->second;
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::processCancelledScan(
        cb::rangescan::Id id) {
    auto locked = syncData.wlock();
    return processScanRemoval(
            *locked, id, cb::engine_errc::range_scan_cancelled);
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::processScanRemoval(
        SynchronizedData& data,
        cb::rangescan::Id id,
        cb::engine_errc finalStatus) {
    Expects(finalStatus != cb::engine_errc::success);

    std::shared_ptr<RangeScan> scan;
    auto itr = data.rangeScans.find(id);
    if (itr == data.rangeScans.end()) {
        return {};
    }
    // obtain the scan
    scan = itr->second;
    if (finalStatus == cb::engine_errc::range_scan_complete) {
        scan->setStateCompleted();
    } else {
        scan->setStateCancelled(finalStatus);
    }

    // Erase from the map, no further continue/cancel allowed.
    data.rangeScans.erase(itr);

    if (data.rangeScans.empty()) {
        data.timeoutTask.reset();
    }
    return scan;
}

cb::engine_errc VB::RangeScanOwner::checkPrivileges(
        cb::rangescan::Id id,
        CookieIface& cookie,
        const EventuallyPersistentEngine& engine) const {
    auto scan = getScan(id);
    if (!scan) {
        return cb::engine_errc::no_such_key;
    }
    auto status = scan->hasPrivilege(cookie, engine);

    if (status == cb::engine_errc::success) {
        // Also update whether caller has access to the system xattrs
        scan->updateSystemXattrsPrivilege(cookie, engine);
    }

    return status;
}

size_t VB::RangeScanOwner::size() const {
    return syncData.rlock()->rangeScans.size();
}

std::chrono::seconds VB::RangeScanOwner::getMaxDuration() const {
    if (readyScans) {
        return readyScans->getMaxDuration();
    }
    return std::chrono::seconds(0);
}
