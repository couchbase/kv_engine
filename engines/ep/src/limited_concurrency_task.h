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

#pragma once

#include <executor/notifiable_task.h>
#include <platform/awaitable_semaphore.h>
#include <platform/semaphore_guard.h>

/**
 * Base type for tasks which need to be able to limit how many instances run
 * concurrently, like CompactTask.
 *
 * A cb::AwaitableSemaphore limits how many instances may run. Tasks must
 * acquire a token before running. If none are available, the task will snooze
 * forever. When tokens become available, the task will be notified to run
 * again.
 *
 * This is not currently transparent to the task - it is required that the
 * task call:
 *
 * bool runInner() override {
 *     auto guard = acquireOrWait();
 *     if (!guard) {
 *         // could not acquire a token, queued for notification.
 *         // already snooze()-ed forever, just return true to
 *         // reschedule.
 *         return true;
 *     }
 *     // Do concurrency-limited work
 * }
 *
 * However, a future refactor could avoid this by, for example,
 * restructuring as a mixin or re-implementing at the thread pool level.
 */
class LimitedConcurrencyTask : public cb::Waiter, public NotifiableTask {
public:
    /**
     * Construct a task which will be concurrency limited by the provided
     * semaphore.
     *
     * @param e engine pointer
     * @param id task id
     * @param semaphore semphore from which a token must be acquired before the
     *                  task can run
     * @param completeBeforeShutdown should the task be required to complete
     *                               before shutdown
     */
    LimitedConcurrencyTask(EventuallyPersistentEngine& e,
                           TaskId id,
                           cb::AwaitableSemaphore& semaphore,
                           bool completeBeforeShutdown);

    /**
     * Subtypes should provide an implementation for the task.
     *
     * Note: overriden in this class for informative/documentation
     * purposes, this is part of the NotifiableTask interface.
     */
    bool runInner() override = 0;

    /**
     * Called by cb::AwaitableSemaphore when tokens become available.
     *
     * Notifies the task to run.
     *
     * Implements the cb::Waiter interface.
     */
    void signal() override;

protected:
    /**
     * Attempt to acquire a token from the semaphore provided at construction.
     *
     * If a token is available, return a valid guard. The task should then
     * proceed.
     *
     * If no tokens are available, snooze() the task forever and queue to be
     * notified by the semaphore when tokens become available.
     *
     * @return a guard, valid if a token was acquired and execution can continue
     */
    cb::SemaphoreGuard<cb::Semaphore*> acquireOrWait();

    // semaphore used to restrict how many tasks can run at once
    cb::AwaitableSemaphore& semaphore;
};