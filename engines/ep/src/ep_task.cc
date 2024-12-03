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
#include <platform/cb_arena_malloc.h>

static cb::executor::Tracer& getThreadLocalTracer() {
    cb::NoArenaGuard guard;
    thread_local cb::executor::Tracer tracer;
    return tracer;
}

cb::executor::Tracer& EpTaskTracerMixin::getTracer() {
    return getThreadLocalTracer();
}

const cb::executor::Tracer& EpTaskTracerMixin::getTracer() const {
    return getThreadLocalTracer();
}

EpTask::EpTask(EventuallyPersistentEngine& e,
               TaskId taskId,
               double initialSleepTime,
               bool completeBeforeShutdown)
    : GlobalTask(e.getTaskable(),
                 taskId,
                 initialSleepTime,
                 completeBeforeShutdown),
      engine(&e) {
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

const cb::executor::Profile* EpTask::getRuntimeProfile() const {
    return &getTracer().getProfile();
}

bool EpTask::execute(std::string_view threadName) {
    // Invoke the parent execute() method with the engine as the target for
    // alloc/dalloc
    BucketAllocationGuard guard(engine);
    // Clear the profiling data from the previous execution.
    getTracer().clear();
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
        ExecutorPool::get()->wake(uid);
    }
}

EpLimitedConcurrencyTask::EpLimitedConcurrencyTask(
        EventuallyPersistentEngine& e,
        TaskId id,
        cb::AwaitableSemaphore& semaphore,
        bool completeBeforeShutdown)
    : LimitedConcurrencyBase(semaphore),
      EpNotifiableTask(
              e, id, 0 /* initial sleeptime*/, completeBeforeShutdown) {
}

void EpLimitedConcurrencyTask::signal() {
    wakeup();
}

bool EpSignalTask::run() {
    bool res = EpNotifiableTask::run();
    signalWaiters();
    return res;
}

void EpSignalTask::wakeupAndGetNotified(
        const std::shared_ptr<cb::Waiter>& waiter) {
    waiters.lock()->pushUnique(waiter);
    wakeup();
}

void EpSignalTask::signalWaiters() {
    auto prevWaiters = waiters.exchange({});
    for (auto& waiter : prevWaiters.getWaiters()) {
        if (auto w = waiter.lock()) {
            w->signal();
        }
    }
}
