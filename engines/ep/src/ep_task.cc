/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_task.h"
#include "ep_engine.h"
#include "objectregistry.h"
#include <executor/executorpool.h>
#include <folly/lang/Hint.h>

EpTask::EpTask(EventuallyPersistentEngine& e,
               TaskId taskId,
               double initialSleepTime,
               bool completeBeforeShutdown)
    : GlobalTask(e.getTaskable(),
                 taskId,
                 initialSleepTime,
                 completeBeforeShutdown) {
    GlobalTask::engine = &e;
}

EpTask::~EpTask() {
    // Why is this here? We are dereferencing this pointer to try and catch in
    // CV any destruction ordering issues (where the engine was destructed
    // before the task). ASAN should catch such an issue. Note that the engine
    // can be null in some unit tests
    if (engine) {
        folly::compiler_must_not_elide(engine->getConfiguration());
    }
}

bool EpTask::execute(std::string_view threadName) {
    // Invoke the parent execute() method with the engine as the target for
    // alloc/dalloc
    BucketAllocationGuard guard(engine);

    return GlobalTask::execute(threadName);
}

EpNotifiableTask::EpNotifiableTask(EventuallyPersistentEngine& e,
                                   TaskId id,
                                   double sleeptime,
                                   bool completeBeforeShutdown)
    : EpTask(e, id, sleeptime, completeBeforeShutdown) {
}

bool EpNotifiableTask::run() {
    snooze(std::chrono::duration<double>(getSleepTime()).count());
    bool manuallyNotified = pendingRun.exchange(false);
    return runInner(manuallyNotified);
}

void EpNotifiableTask::wakeup() {
    bool expected = false;
    if (pendingRun.compare_exchange_strong(expected, true)) {
        ExecutorPool::get()->wake(static_cast<size_t>(uid));
    }
}
