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
#include "ep_engine.h"
#include "kv_bucket.h"

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

void DurabilityTimeoutVisitor::visitBucket(const VBucketPtr& vb) {
    vb->processDurabilityTimeout(startTime);
}
