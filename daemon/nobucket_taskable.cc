/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "nobucket_taskable.h"
#include "memcached.h"
#include "stats.h"
#include <logger/logger.h>
#include <platform/json_log_conversions.h>
#include <platform/timeutils.h>

const std::string& NoBucketTaskable::getName() const {
    return name;
}

task_gid_t NoBucketTaskable::getGID() const {
    return uintptr_t(this);
}

bucket_priority_t NoBucketTaskable::getWorkloadPriority() const {
    return HIGH_BUCKET_PRIORITY;
}

void NoBucketTaskable::setWorkloadPriority(bucket_priority_t prio) {
    // ignore
}

WorkLoadPolicy& NoBucketTaskable::getWorkLoadPolicy() {
    return policy;
}

bool NoBucketTaskable::isShutdown() const {
    return is_memcached_shutting_down();
}

NoBucketTaskable& NoBucketTaskable::instance() {
    static NoBucketTaskable instance;
    return instance;
}

void NoBucketTaskable::logQTime(const GlobalTask& task,
                                std::string_view threadName,
                                std::chrono::steady_clock::duration enqTime) {
    const auto taskType = GlobalTask::getTaskType(task.getTaskId());
    if ((taskType == TaskType::NonIO && enqTime > std::chrono::seconds(1)) ||
        (taskType == TaskType::AuxIO && enqTime > std::chrono::seconds(10))) {
        LOG_WARNING_CTX("Slow scheduling",
                        {"task", task.getDescription()},
                        {"thread", threadName},
                        {"overhead", enqTime});
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(enqTime);
    stats.taskSchedulingHistogram[static_cast<int>(task.getTaskId())].add(us);
}

void NoBucketTaskable::logRunTime(const GlobalTask& task,
                                  std::string_view threadName,
                                  std::chrono::steady_clock::duration runtime) {
    // Check if exceeded expected duration; and if so log.
    if (runtime > task.maxExpectedDuration()) {
        LOG_WARNING_CTX("Slow runtime",
                        {"task", task.getDescription()},
                        {"thread", threadName},
                        {"runtime", runtime});
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(runtime);
    stats.taskRuntimeHistogram[static_cast<int>(task.getTaskId())].add(us);
}

void NoBucketTaskable::invokeViaTaskable(std::function<void()> fn) {
    // No specific memory tracking for NoBucketTaskable, memory is just
    // accounted to the global (no bucket) client.
    fn();
}
