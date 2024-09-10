/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill-manager.h"
#include "bucket_logger.h"
#include "connmap.h"
#include "dcp/active_stream.h"
#include "dcp/backfill_disk.h"
#include "dcp/dcpconnmap.h"
#include "dcp/producer.h"
#include "ep_task.h"
#include "ep_time.h"
#include "kv_bucket.h"
#include <executor/executorpool.h>

#include <phosphor/phosphor.h>

#include <memory>
#include <utility>

static const size_t sleepTime = 1;

using namespace std::string_literals;

class BackfillManagerTask : public EpTask {
public:
    BackfillManagerTask(EventuallyPersistentEngine& e,
                        std::shared_ptr<BackfillManager> mgr,
                        double sleeptime = 0,
                        bool completeBeforeShutdown = false)
        : EpTask(e,
                 TaskId::BackfillManagerTask,
                 sleeptime,
                 completeBeforeShutdown),
          weak_manager(mgr),
          description("Backfilling items for "s + mgr->name) {
    }

    bool run() override;

    std::string getDescription() const override;

    std::chrono::microseconds maxExpectedDuration() const override;

private:
    // A weak pointer to the backfill manager which owns this
    // task. The manager is owned by the DcpProducer, but we need to
    // give the BackfillManagerTask access to the manager as it runs
    // concurrently in a different thread.
    // If the manager is deleted (by the DcpProducer) then the
    // ManagerTask simply cancels itself and stops running.
    std::weak_ptr<BackfillManager> weak_manager;

    /// The description of this task. Set during construction to the name
    /// of the BackfillManager.
    const std::string description;
};

bool BackfillManagerTask::run() {
    TRACE_EVENT0("ep-engine/task", "BackFillManagerTask");
    // Create a new shared_ptr to the manager for the duration of this
    // execution.
    auto manager = weak_manager.lock();
    if (!manager) {
        // backfill manager no longer exists - cancel ourself and stop
        // running.
        cancel();
        return false;
    }

    backfill_status_t status = manager->backfill();
    if (status == backfill_finished) {
        return false;
    }
    if (status == backfill_snooze) {
        snooze(sleepTime);
    }

    if (engine->getEpStats().isShutdown) {
        return false;
    }

    return true;
}

std::string BackfillManagerTask::getDescription() const {
    return description;
}

std::chrono::microseconds BackfillManagerTask::maxExpectedDuration() const {
    // Empirical evidence suggests this task runs under 300ms 99.999% of
    // the time.
    // This should also be kept in-line with dcp_backfill_run_duration_limit.
    return std::chrono::milliseconds(310);
}

BackfillManager::BackfillManager(KVBucket& kvBucket,
                                 KVStoreScanTracker& scanTracker,
                                 std::string name,
                                 const Configuration& config)
    : name(std::move(name)),
      kvBucket(kvBucket),
      scanTracker(scanTracker),
      managerTask(nullptr) {
    scanBuffer.bytesRead = 0;
    scanBuffer.itemsRead = 0;
    scanBuffer.maxBytes = config.getDcpScanByteLimit();
    scanBuffer.maxItems = config.getDcpScanItemLimit();

    buffer.bytesRead = 0;
    buffer.maxBytes = config.getDcpBackfillByteLimit();
    buffer.full = false;
    buffer.drainRatio = config.getDcpBackfillByteDrainRatio();
}

void BackfillManager::addStats(DcpProducer& conn,
                               const AddStatFn& add_stat,
                               CookieIface& c) {
    std::unique_lock<std::mutex> lh(lock);
    auto bufferCopy = buffer;
    auto initializingBackfillsSize = initializingBackfills.size();
    auto activeBackfillsSize = activeBackfills.size();
    auto snoozingBackfillsSize = snoozingBackfills.size();
    auto pendingBackfillsSize = pendingBackfills.size();
    auto order = scheduleOrder;
    lh.unlock();

    conn.addStat(
            "backfill_buffer_bytes_read", bufferCopy.bytesRead, add_stat, c);
    conn.addStat("backfill_buffer_max_bytes", bufferCopy.maxBytes, add_stat, c);
    conn.addStat("backfill_buffer_full", bufferCopy.full, add_stat, c);
    conn.addStat("backfill_num_initializing",
                 initializingBackfillsSize,
                 add_stat,
                 c);
    conn.addStat("backfill_num_active", activeBackfillsSize, add_stat, c);
    conn.addStat("backfill_num_snoozing", snoozingBackfillsSize, add_stat, c);
    conn.addStat("backfill_num_pending", pendingBackfillsSize, add_stat, c);
    conn.addStat("backfill_order", to_string(order), add_stat, c);
}

BackfillManager::~BackfillManager() {
    if (managerTask) {
        managerTask->cancel();
        managerTask.reset();
    }

    while (!initializingBackfills.empty()) {
        UniqueDCPBackfillPtr backfill =
                std::move(initializingBackfills.front());
        initializingBackfills.pop_front();
        backfill->cancel();
        scanTracker.decrNumRunningBackfills();
    }

    while (!activeBackfills.empty()) {
        UniqueDCPBackfillPtr backfill = std::move(activeBackfills.front());
        activeBackfills.pop_front();
        backfill->cancel();
        scanTracker.decrNumRunningBackfills();
    }

    while (!snoozingBackfills.empty()) {
        UniqueDCPBackfillPtr backfill =
                std::move((snoozingBackfills.front()).second);
        snoozingBackfills.pop_front();
        backfill->cancel();
        scanTracker.decrNumRunningBackfills();
    }

    while (!pendingBackfills.empty()) {
        UniqueDCPBackfillPtr backfill = std::move(pendingBackfills.front());
        pendingBackfills.pop_front();
        backfill->cancel();
    }
}

void BackfillManager::setBackfillOrder(BackfillManager::ScheduleOrder order) {
    scheduleOrder = order;
}

BackfillManager::ScheduleResult BackfillManager::schedule(
        UniqueDCPBackfillPtr backfill) {
    std::unique_lock<std::mutex> lh(lock);
    ScheduleResult result;
    if (scanTracker.canCreateBackfill(getNumInProgressBackfills(lh))) {
        initializingBackfills.push_back(std::move(backfill));
        result = ScheduleResult::Active;
    } else {
        pendingBackfills.push_back(std::move(backfill));
        result = ScheduleResult::Pending;
    }

    if (managerTask && !managerTask->isdead()) {
        auto id = managerTask->getId();
        lh.unlock();
        ExecutorPool::get()->wake(id);
    } else {
        // Reducing the lock scope of this branch, only dropping the lock once
        // assigned to managerTask - so we don't get multiple schedules seeing
        // !managerTask. However call schedule with a locally scoped newTask
        // because managerTask could become reset once the lock is released.
        // See ::backfill()
        auto newTask = std::make_shared<BackfillManagerTask>(
                kvBucket.getEPEngine(), shared_from_this());
        managerTask = newTask;
        lh.unlock();
        ExecutorPool::get()->schedule(newTask);
    }
    return result;
}

bool BackfillManager::bytesCheckAndRead(size_t bytes) {
    std::lock_guard<std::mutex> lh(lock);

    buffer.bytesRead += bytes;
    scanBuffer.itemsRead++;
    scanBuffer.bytesRead += bytes;

    // Note: For both backfill/scan buffers, the logic allows reading bytes when
    // 'bytesRead == 0'. That is for ensuring that we allow DCP streaming in a
    // scenario where 'buffer-size < data-size' (eg, imagine 10MB buffer-size
    // and a 15MB document). Backfill would block forever otherwise.

    // Space available in the backfill buffer?
    const bool bufferAvailable = buffer.bytesRead == 0 ||
                                 buffer.bytesRead < buffer.maxBytes;
    if (!bufferAvailable) {
        buffer.full = true;
        return false;
    }

    // Space available for the current scan?
    const bool scanAvailable =
            (scanBuffer.itemsRead < scanBuffer.maxItems) &&
            (scanBuffer.bytesRead == 0 ||
             scanBuffer.bytesRead < scanBuffer.maxBytes);
    if (!scanAvailable) {
        return false;
    }

    return true;
}

void BackfillManager::bytesSent(size_t bytes) {
    std::unique_lock<std::mutex> lh(lock);
    if (bytes > buffer.bytesRead) {
        throw std::invalid_argument(
                "BackfillManager::bytesSent: bytes "
                "(which is " +
                std::to_string(bytes) +
                ") is greater than "
                "buffer.bytesRead (which is " +
                std::to_string(buffer.bytesRead) + ")");
    }
    buffer.bytesRead -= bytes;

    // Clear the full-flag if the buffer usage has dropped below the defined
    // threshold. Note that for bytesRead==0 the buffer is empty regardless of
    // any user-defined param.
    const bool drainedEnough =
            buffer.bytesRead == 0 ||
            buffer.bytesRead < buffer.maxBytes * (1.0 - buffer.drainRatio);
    if (buffer.full && drainedEnough) {
        buffer.full = false;
        if (managerTask) {
            auto id = managerTask->getId();
            lh.unlock();
            ExecutorPool::get()->wake(id);
        }
    }
}

backfill_status_t BackfillManager::backfill() {
    std::unique_lock<std::mutex> lh(lock);

    // If no backfills remaining in any of the queues then we can
    // stop the background task and finish.
    if (emptyQueues(lh)) {
        managerTask.reset();
        return backfill_finished;
    }

    if (kvBucket.isMemUsageAboveBackfillThreshold()) {
        lh.unlock();
        EP_LOG_INFO_RAW(
                "DCP backfilling task temporarily suspended "
                "because the current memory usage is too high");
        return backfill_snooze;
    }

    movePendingToInitializing(lh);
    moveSnoozingToActiveQueue();

    if (buffer.full) {
        // If the buffer is full ask each backfill if it should be cancelled.
        // shouldCancel can check for dead streams or with MB-62703 streams that
        // appear stalled for an unacceptably long period of time. Backfills in
        // this state can then be cancelled freeing up buffer space and
        // releasing backfill resources (critically releasing any reference to
        // disk snapshot).
        std::list<UniqueDCPBackfillPtr> toDelete;
        for (auto a_itr = activeBackfills.begin();
             a_itr != activeBackfills.end();) {
            if ((*a_itr)->shouldCancel()) {
                toDelete.push_back(std::move(*a_itr));
                a_itr = activeBackfills.erase(a_itr);
                scanTracker.decrNumRunningBackfills();
            } else {
                ++a_itr;
            }
        }

        lh.unlock();
        bool reschedule = !toDelete.empty();
        while (!toDelete.empty()) {
            UniqueDCPBackfillPtr backfill = std::move(toDelete.front());
            // cancel is done after the lock is dropped as it may call setDead
            // on the stream which can call back into this object for bytes
            // sent/read accounting.
            backfill->cancel();
            toDelete.pop_front();
        }
        return reschedule ? backfill_success : backfill_snooze;
    }

    auto [backfill, source] = dequeueNextBackfill(lh);

    // If no backfills ready to run then snooze
    if (!backfill) {
        return backfill_snooze;
    }

    // The backfill is about to run for the first time, set the create mode.
    if (Source::Initializing == source) {
        backfill->setCreateMode(getCreateMode());
    }

    lh.unlock();
    backfill_status_t status = backfill->run();
    lh.lock();

    scanBuffer.bytesRead = 0;
    scanBuffer.itemsRead = 0;

    // Irrespective of status of backfill, it will no longer be "untracked" -
    // if status is backfill_finished then it's no longer in progress; if
    // status is success or snooze then it is added back to the appropriate
    // in-progress queue.
    numInProgressUntrackedBackfills--;

    switch (status) {
        case backfill_success:
            switch (scheduleOrder) {
            case ScheduleOrder::RoundRobin:
                activeBackfills.push_back(std::move(backfill));
                break;
            case ScheduleOrder::Sequential:
                switch (source) {
                case Source::Active:
                    // If the source is active then this is the "current"
                    // backfill we are working our way through, we want
                    // to put it back on the front of the active queue to
                    // run next time.
                    activeBackfills.push_front(std::move(backfill));
                    break;
                case Source::Initializing:
                    // New - this was only run to initialise it; it should
                    // now go to the back of the active queue so the
                    // "current" Backfill can resume to completion.
                    // round-robin and sequential:
                    activeBackfills.push_back(std::move(backfill));
                    break;
                }
                break;
            }
            break;
        case backfill_finished:
            lh.unlock();
            scanTracker.decrNumRunningBackfills();
            break;
        case backfill_snooze: {
            snoozingBackfills.emplace_back(ep_current_time(),
                                           std::move(backfill));
            break;
        }
    }

    return backfill_success;
}

void BackfillManager::movePendingToInitializing(
        const std::unique_lock<std::mutex>& lh) {
    while (!pendingBackfills.empty() &&
           scanTracker.canCreateBackfill(getNumInProgressBackfills(lh))) {
        initializingBackfills.splice(initializingBackfills.end(),
                                     pendingBackfills,
                                     pendingBackfills.begin());
    }
}

void BackfillManager::moveSnoozingToActiveQueue() {
    while (!snoozingBackfills.empty()) {
        std::pair<rel_time_t, UniqueDCPBackfillPtr> snoozer =
                std::move(snoozingBackfills.front());
        snoozingBackfills.pop_front();
        // If snoozing task is found to be sleeping for greater than
        // allowed snoozetime, push into active queue
        if (snoozer.first + sleepTime <= ep_current_time()) {
            activeBackfills.push_back(std::move(snoozer.second));
        } else {
            // Push back the popped snoozing backfill
            snoozingBackfills.push_back(std::move(snoozer));
            break;
        }
    }
}

int BackfillManager::getNumInProgressBackfills(
        const std::unique_lock<std::mutex>& lh) const {
    return initializingBackfills.size() + activeBackfills.size() +
           snoozingBackfills.size() + numInProgressUntrackedBackfills;
}

std::pair<UniqueDCPBackfillPtr, BackfillManager::Source>
BackfillManager::dequeueNextBackfill(std::unique_lock<std::mutex>&) {
    // Dequeue from initializingBackfills if non-empty, else activeBackfills.
    auto& queue = initializingBackfills.empty() ? activeBackfills
                                                : initializingBackfills;
    auto source = initializingBackfills.empty() ? Source::Active
                                                : Source::Initializing;
    if (!queue.empty()) {
        auto next = std::move(queue.front());
        queue.pop_front();
        numInProgressUntrackedBackfills++;
        return {std::move(next), source};
    }
    return {};
}

void BackfillManager::wakeUpTask() {
    std::unique_lock<std::mutex> lh(lock);
    if (!managerTask) {
        return;
    }
    auto id = managerTask->getId();
    lh.unlock();
    ExecutorPool::get()->wake(id);
}

bool BackfillManager::removeBackfill(uint64_t backfillUID) {
    std::array<std::reference_wrapper<std::list<UniqueDCPBackfillPtr>>, 3>
            lists{{pendingBackfills, activeBackfills, initializingBackfills}};

    std::unique_lock<std::mutex> lh(lock);
    for (auto& ref : lists) {
        auto& list = ref.get();
        for (auto itr = list.begin(); itr != list.end(); itr++) {
            if ((*itr)->getUID() == backfillUID) {
                list.erase(itr);

                // Insertion into the pending list does not increment the
                // tracker stat, so skip for pending
                if (&ref.get() != &pendingBackfills) {
                    lh.unlock();
                    scanTracker.decrNumRunningBackfills();
                }
                return true;
            }
        }
    }

    // Finally the snoozing queue
    for (auto itr = snoozingBackfills.begin(); itr != snoozingBackfills.end();
         itr++) {
        if (itr->second->getUID() == backfillUID) {
            snoozingBackfills.erase(itr);
            lh.unlock();
            scanTracker.decrNumRunningBackfills();
            return true;
        }
    }
    return false;
}

std::string BackfillManager::to_string(BackfillManager::ScheduleOrder order) {
    switch (order) {
    case BackfillManager::ScheduleOrder::RoundRobin:
        return "round-robin";
    case BackfillManager::ScheduleOrder::Sequential:
        return "sequential";
    }
    folly::assume_unreachable();
}

bool BackfillManager::emptyQueues(std::unique_lock<std::mutex>& lock) const {
    return initializingBackfills.empty() && activeBackfills.empty() &&
           snoozingBackfills.empty() && pendingBackfills.empty();
}

DCPBackfillCreateMode BackfillManager::getCreateMode() const {
    switch (scheduleOrder) {
    case BackfillManager::ScheduleOrder::RoundRobin:
        return DCPBackfillCreateMode::CreateAndScan;
    case BackfillManager::ScheduleOrder::Sequential:
        return DCPBackfillCreateMode::CreateOnly;
    }
    folly::assume_unreachable();
}

void BackfillManager::setBackfillByteLimit(size_t bytes) {
    std::lock_guard<std::mutex> lh(lock);
    buffer.maxBytes = bytes;
}

size_t BackfillManager::getBackfillByteLimit() const {
    std::lock_guard<std::mutex> lh(lock);
    return buffer.maxBytes;
}

size_t BackfillManager::getBackfillBytesRead() const {
    std::lock_guard<std::mutex> lh(lock);
    return buffer.bytesRead;
}

double BackfillManager::getBackfillBytesDrainRatio() const {
    return buffer.drainRatio;
}

bool BackfillManager::isBufferFull() const {
    std::lock_guard<std::mutex> lh(lock);
    return buffer.full;
}

const DCPBackfillIface& BackfillManager::getBackfill(uint64_t uid) const {
    std::lock_guard<std::mutex> lh(lock);
    for (const auto queue :
         {&initializingBackfills, &activeBackfills, &pendingBackfills}) {
        for (const auto& backfill : *queue) {
            if (backfill->getUID() == uid) {
                return *backfill;
            }
        }
    }
    for (const auto& snooze : snoozingBackfills) {
        if (snooze.second->getUID() == uid) {
            return *snooze.second;
        }
    }
    throw std::invalid_argument("BackfillManager::getBackfill - uid not found");
}