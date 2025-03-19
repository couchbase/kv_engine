/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "item_freq_decayer.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "item_freq_decayer_visitor.h"
#include "kv_bucket.h"
#include "objectregistry.h"
#include "stored-value.h"
#include <executor/executorpool.h>

#include <daemon/nobucket_taskable.h>
#include <phosphor/phosphor.h>

#include <algorithm>
#include <limits>

ItemFreqDecayerTaskManager::ItemFreqDecayerTaskManager()
    : crossBucketDecayer(std::make_shared<CrossBucketItemFreqDecayer>()) {
}

ItemFreqDecayerTaskManager& ItemFreqDecayerTaskManager::get() {
    static ItemFreqDecayerTaskManager manager = []() {
        NonBucketAllocationGuard guard;
        return ItemFreqDecayerTaskManager();
    }();
    return manager;
}

std::shared_ptr<ItemFreqDecayerTask> ItemFreqDecayerTaskManager::create(
        EventuallyPersistentEngine& e, uint16_t percentage) {
    if (e.getConfiguration().isCrossBucketHtQuotaSharing()) {
        // There's more bookkeeping to do for buckets sharing quota:
        // The task must signal it's completion to the
        // CrossBucketDecayer. We should also signal up if the task has been
        // destroyed as then it might not complete. This is necessary to
        // orchestrate the execution across buckets.
        class ItemFreqDecayerTaskWithCallback : public ItemFreqDecayerTask {
        public:
            // Create an item decayer task which signals its completion to the
            // task manager. The task should only be woken up from
            // CrossBucketItemFreqDecayer::schedule for quota sharing configs,
            // so we set scheduleNow to false. This is so that we never lose
            // wakeups.
            ItemFreqDecayerTaskWithCallback(EventuallyPersistentEngine& e,
                                            uint16_t percentage)
                : ItemFreqDecayerTask(e, percentage, /* scheduleNow= */ false) {
            }
            void onCompleted() override {
                // Signal on completion
                signalCompleted(*this);
            }
            ~ItemFreqDecayerTaskWithCallback() noexcept override {
                // Signal on destruction
                signalCompleted(*this);
            }
        };

        auto task = std::make_shared<ItemFreqDecayerTaskWithCallback>(
                e, percentage);
        // Push to the list so we can get it back when we need to run the
        // cross-bucket item decayer.
        tasksForQuotaSharing.push(task);
        return task;
    }
    return std::make_shared<ItemFreqDecayerTask>(e, percentage);
}

std::vector<std::shared_ptr<ItemFreqDecayerTask>>
ItemFreqDecayerTaskManager::getTasksForQuotaSharing() const {
    return tasksForQuotaSharing.getNonExpired();
}

std::shared_ptr<CrossBucketItemFreqDecayer>
ItemFreqDecayerTaskManager::getCrossBucketDecayer() const {
    return crossBucketDecayer;
}

void ItemFreqDecayerTaskManager::signalCompleted(ItemFreqDecayerTask& task) {
    get().getCrossBucketDecayer()->itemDecayerCompleted(task);
}

void CrossBucketItemFreqDecayer::schedule() {
    bool expected = false;
    if (!notified.compare_exchange_strong(expected, true)) {
        return;
    }

    // Get the tasks we need to wakeup and wait for
    auto tasks = ItemFreqDecayerTaskManager::get().getTasksForQuotaSharing();
    EP_LOG_DEBUG("CrossBucketItemFreqDecayer waking up {} decayer tasks",
                 tasks.size());
    // Add the tasks to the pending task set
    pendingSubTasks.withLock([&tasks](auto& locked) {
        Expects(locked.empty());
        for (const auto& task : tasks) {
// The task is only ever scheduled from here. We shouldn't reach here if
// any tasks are waiting to run or running because of the notified
// guard, which we reset in itemDecayerCompleted.
#if CB_DEVELOPMENT_ASSERTS
            // Assert the above is true for debug builds.
            Expects(task->getWaketime() ==
                    cb::time::steady_clock::time_point::max());
#endif // CB_DEVELOPMENT_ASSERTS
            task->wakeup();
            locked.insert(task.get());
        }
    });
}

void CrossBucketItemFreqDecayer::itemDecayerCompleted(ItemFreqDecayerTask& t) {
    // Reset the notified flag once all tasks have completed.
    // This method also gets called when the task object is destroyed,
    // or when the bucket is warmed up and the task is executed without the
    // CrossBucketItemFreqDecayer waking it up, so erase() can return 0.
    auto remainingTasks = pendingSubTasks.withLock([tptr = &t](auto& locked) {
        locked.erase(tptr);
        return locked.size();
    });
    if (remainingTasks == 0) {
        EP_LOG_DEBUG_RAW(
                "CrossBucketItemFreqDecayer all decayer tasks have "
                "completed");
        notified.store(false);
    }
}

ItemFreqDecayerTask::ItemFreqDecayerTask(EventuallyPersistentEngine& e,
                                         uint16_t percentage_,
                                         bool scheduleNow)
    : EpTask(e, TaskId::ItemFreqDecayerTask, scheduleNow ? 0 : INT_MAX, false),
      completed(false),
      epstore_position(engine->getKVBucket()->startPosition()),
      notified(false),
      percentage(percentage_) {
}

ItemFreqDecayerTask::~ItemFreqDecayerTask() = default;

bool ItemFreqDecayerTask::run() {
    TRACE_EVENT0("ep-engine/task", "ItemFreqDecayerTask");

    // Setup so that we will sleep before clearing notified.
    snooze(std::numeric_limits<int>::max());

    ++(engine->getEpStats().freqDecayerRuns);

    // Get our pause/resume visitor. If we didn't finish the previous pass,
    // then resume from where we last were, otherwise create a new visitor
    // starting from the beginning.
    if (!prAdapter) {
        prAdapter = std::make_unique<PauseResumeVBAdapter>(
                std::make_unique<ItemFreqDecayerVisitor>(percentage));
        epstore_position = engine->getKVBucket()->startPosition();
        completed = false;
    }

    // Print start status.
    if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (epstore_position == engine->getKVBucket()->startPosition()) {
            ss << " starting. ";
        } else {
            ss << " resuming from " << epstore_position << ", ";
            ss << prAdapter->getHashtablePosition() << ".";
        }
        ss << " Using chunk_duration=" << getChunkDuration().count() << " ms.";
        EP_LOG_DEBUG("{}", ss.str());
    }

    // Prepare the underlying visitor.
    auto& visitor = getItemFreqDecayerVisitor();
    const auto start = cb::time::steady_clock::now();
    const auto deadline = start + getChunkDuration();
    visitor.setDeadline(deadline);
    visitor.clearStats();

    // Do it - set off the visitor.
    epstore_position = engine->getKVBucket()->pauseResumeVisit(
            *prAdapter, epstore_position);
    const auto end = cb::time::steady_clock::now();

    // Check if the visitor completed a full pass.
    completed = (epstore_position == engine->getKVBucket()->endPosition());

    // Print status.
    if (getGlobalBucketLogger()->should_log(spdlog::level::debug)) {
        std::stringstream ss;
        ss << getDescription() << " for bucket '" << engine->getName() << "'";
        if (completed) {
            ss << " finished.";
        } else {
            ss << " paused at position " << epstore_position << ".";
        }
        std::chrono::microseconds duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                      start);
        ss << " Took " << duration.count() << " us. to visit "
           << visitor.getVisitedCount() << " documents.";
        EP_LOG_DEBUG("{}", ss.str());
    }

    // Delete(reset) visitor and allow to be notified if it finished.
    if (completed) {
        prAdapter.reset();
        notified.store(false);
        onCompleted();
    } else {
        // We have not completed decaying all the items so wake the task back
        // up
        wakeUp();
    }

    if (engine->getEpStats().isShutdown) {
        return false;
    }

    return true;
}

void ItemFreqDecayerTask::stop() {
    if (uid) {
        if (ExecutorPool::get()->cancel(uid)) {
            onCompleted();
        }
    }
}

void ItemFreqDecayerTask::wakeup() {
    bool expected = false;
    if (notified.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(getId());
    }
}

std::string ItemFreqDecayerTask::getDescription() const {
    return "Item frequency count decayer task";
}

std::chrono::microseconds ItemFreqDecayerTask::maxExpectedDuration() const {
    // ItemFreqDecayerTask processes items in chunks, with each chunk
    // constrained by a ChunkDuration runtime, so we expect to only take that
    // long.  However, the ProgressTracker used estimates the time remaining,
    // so apply some headroom to that figure so we don't get inundated with
    // spurious "slow tasks" which only just exceed the limit.
    return getChunkDuration() * 10;
}

std::chrono::milliseconds ItemFreqDecayerTask::getChunkDuration() const {
    return std::chrono::milliseconds(
            engine->getConfiguration().getItemFreqDecayerChunkDuration());
}

ItemFreqDecayerVisitor& ItemFreqDecayerTask::getItemFreqDecayerVisitor() {
    return dynamic_cast<ItemFreqDecayerVisitor&>(prAdapter->getHTVisitor());
}
