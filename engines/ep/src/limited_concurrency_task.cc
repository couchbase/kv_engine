/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "limited_concurrency_task.h"

LimitedConcurrencyTask::LimitedConcurrencyTask(
        EventuallyPersistentEngine& e,
        TaskId id,
        cb::AwaitableSemaphore& semaphore,
        bool completeBeforeShutdown)
    : NotifiableTask(e, id, 0 /* initial sleeptime*/, completeBeforeShutdown),
      semaphore(semaphore) {
}

void LimitedConcurrencyTask::signal() {
    wakeup();
}

cb::SemaphoreGuard<cb::Semaphore*> LimitedConcurrencyTask::acquireOrWait() {
    if (semaphore.acquire_or_wait(shared_from_this())) {
        // token was acquired! Return an RAII guard holding it, and let
        // the task continue.
        return {&semaphore, cb::adopt_token};
    }
    // could not acquire a token. We have been queued as a waiter, and will
    // be NotifiableTask::wakeup()-ed when a token becomes available.
    return {};
}