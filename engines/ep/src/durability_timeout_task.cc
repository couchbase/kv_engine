/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "durability_timeout_task.h"
#include "ep_engine.h"
#include "kv_bucket.h"

#include <phosphor/phosphor.h>

DurabilityTimeoutTask::DurabilityTimeoutTask(EventuallyPersistentEngine& engine,
                                             std::chrono::milliseconds interval)
    : GlobalTask(&engine,
                 TaskId::DurabilityTimeoutTask,
                 0 /*initial sleep-time in seconds*/,
                 false /*completeBeforeShutdown*/),
      sleepTime(interval) {
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
    snooze(std::chrono::duration<double>(sleepTime).count());

    // Schedule again if not shutting down
    return !engine->getEpStats().isShutdown;
}

void DurabilityTimeoutVisitor::visitBucket(const VBucketPtr& vb) {
    vb->processDurabilityTimeout(startTime);
}
