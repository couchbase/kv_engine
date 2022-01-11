/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "durability_timeout_task.h"
#include "bucket_logger.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "vbucket.h"
#include <executor/executorpool.h>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <phosphor/phosphor.h>

class DurabilityTimeoutTask::ConfigChangeListener
    : public ValueChangedListener {
public:
    explicit ConfigChangeListener(DurabilityTimeoutTask& timeoutTask)
        : timeoutTask(timeoutTask) {
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key == "durability_timeout_task_interval") {
            timeoutTask.setSleepTime(std::chrono::milliseconds(value));
        }
    }

private:
    DurabilityTimeoutTask& timeoutTask;
};

DurabilityTimeoutTask::DurabilityTimeoutTask(EventuallyPersistentEngine& engine,
                                             std::chrono::milliseconds interval)
    : GlobalTask(&engine,
                 TaskId::DurabilityTimeoutTask,
                 0 /*initial sleep-time in seconds*/,
                 false /*completeBeforeShutdown*/),
      sleepTime(interval) {
    engine.getConfiguration().addValueChangedListener(
            "durability_timeout_task_interval",
            std::make_unique<ConfigChangeListener>(*this));
}

bool DurabilityTimeoutTask::run() {
    TRACE_EVENT0("ep-engine/task", "DurabilityTimeoutTask");

    // @todo: A meaningful value will be the P99.99
    const auto maxExpectedDurationForVisitorTask =
            std::chrono::milliseconds(100);

    auto& kvBucket = *engine->getKVBucket();
    kvBucket.visitAsync(std::make_unique<DurabilityTimeoutVisitor>(),
                        "DurabilityTimeoutVisitor",
                        TaskId::DurabilityTimeoutVisitor,
                        maxExpectedDurationForVisitorTask);

    // Note: Default unit for std::duration is seconds, so the following gives
    // the seconds-representation (as double) of the given millis (sleepTime)
    snooze(std::chrono::duration_cast<std::chrono::duration<double>>(
                   sleepTime.load())
                   .count());

    // Schedule again if not shutting down
    return !engine->getEpStats().isShutdown;
}

void DurabilityTimeoutVisitor::visitBucket(VBucket& vb) {
    vb.processDurabilityTimeout(startTime);
}

/**
 * Task used by VBucketDurabilityTimeoutHandler which is run() whenever the
 * SyncWrite at the head of trackedWrites exceeds it's durability timeout.
 * The task will abort that SyncWrite (if it hasn't already been completed)
 * and any others which have also since expired for the given VBucket.
 */
class VBucketSyncWriteTimeoutTask : public GlobalTask {
public:
    VBucketSyncWriteTimeoutTask(Taskable& taskable, VBucket& vBucket)
        : GlobalTask(taskable, TaskId::DurabilityTimeoutTask, INT_MAX, false),
          vBucket(vBucket),
          vbid(vBucket.getId()) {
    }

    std::string getDescription() const override {
        return fmt::format("Expired SyncWrite callback for {}", vbid);
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Calibrated to observed p99.9 duration in system tests.
        return std::chrono::milliseconds{10};
    }

protected:
    bool run() override {
        // Inform the vBucket that it should process (and abort) any pending
        // SyncWrites which have timed out as of now.
        vBucket.processDurabilityTimeout(std::chrono::steady_clock::now());

        // Note that while run() returns 'true' here (to re-schedule based on
        // ::waketime), there's no explicit snooze() in this method. This is
        // because processDurabilityTimeout() above will update the snooze time
        // (via SyncWriteScheduledExpiry::{update,cancel}NextExpiryTime) during
        // processing.
        return true;
    }

private:
    VBucket& vBucket;
    // Need a separate vbid member variable as getDescription() can be
    // called during Bucket shutdown (after VBucket has been deleted)
    // as part of cleaning up tasks (see
    // EventuallyPersistentEngine::waitForTasks) - and hence calling
    // into vBucket->getId() would be accessing a deleted object.
    const Vbid vbid;
};

EventDrivenDurabilityTimeout::EventDrivenDurabilityTimeout(Taskable& taskable,
                                                           VBucket& vbucket)
    : taskId(ExecutorPool::get()->schedule(
              std::make_shared<VBucketSyncWriteTimeoutTask>(taskable,
                                                            vbucket))) {
}

EventDrivenDurabilityTimeout::~EventDrivenDurabilityTimeout() {
    ExecutorPool::get()->cancel(taskId);
}

void EventDrivenDurabilityTimeout::updateNextExpiryTime(
        std::chrono::steady_clock::time_point nextExpiry) {
    auto snoozeTime = std::chrono::duration<double>(
            nextExpiry - std::chrono::steady_clock::now());
    EP_LOG_DEBUG(
            "SyncWriteScheduledExpiry::updateNextExpiryTime taskId:{} "
            "snooze:{}",
            taskId,
            snoozeTime);
    ExecutorPool::get()->snoozeAndWait(taskId, snoozeTime.count());
}

void EventDrivenDurabilityTimeout::cancelNextExpiryTime() {
    EP_LOG_DEBUG("SyncWriteScheduledExpiry::cancelNextExpiryTime taskId:{}",
                 taskId);
    ExecutorPool::get()->snoozeAndWait(taskId, INT_MAX);
}
