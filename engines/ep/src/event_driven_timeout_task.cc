/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "event_driven_timeout_task.h"

#include "bucket_logger.h"

#include <executor/executorpool.h>

EventDrivenTimeoutTask::EventDrivenTimeoutTask(std::shared_ptr<GlobalTask> task)
    : taskId(ExecutorPool::get()->schedule(task)) {
}

EventDrivenTimeoutTask::~EventDrivenTimeoutTask() {
    ExecutorPool::get()->cancel(taskId);
}

void EventDrivenTimeoutTask::updateNextExpiryTime(
        std::chrono::steady_clock::time_point nextExpiry) {
    auto snoozeTime = std::chrono::duration<double>(
            nextExpiry - std::chrono::steady_clock::now());
    EP_LOG_DEBUG(
            "EventDrivenTimeoutTask::updateNextExpiryTime taskId:{} "
            "snooze:{}",
            taskId,
            snoozeTime.count());
    ExecutorPool::get()->snoozeAndWait(taskId, snoozeTime.count());
}

void EventDrivenTimeoutTask::cancelNextExpiryTime() {
    EP_LOG_DEBUG("EventDrivenTimeoutTask::cancelNextExpiryTime taskId:{}",
                 taskId);
    ExecutorPool::get()->snoozeAndWait(taskId, INT_MAX);
}
