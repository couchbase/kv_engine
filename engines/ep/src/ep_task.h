/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "executor/globaltask.h"
#include "executor/limited_concurrency_task.h"

class EventuallyPersistentEngine;

/**
 * EpTask is a subclass of GlobalTask which owned by a specific
 * EventuallyPersistentEngine instance.
 * EpTasks differ from their parent in a few significant ways:
 * - When they are run(), they change the calling thread's current engine
 *   to the engine they are associated with, to ensure any memory or objects
 *   (de)allocated while they are running is accounted to the appropriate
 *   bucket.
 */
class EpTask : public GlobalTask {
public:
    EpTask(EventuallyPersistentEngine& e,
           TaskId taskId,
           double sleeptime = 0,
           bool completeBeforeShutdown = true);

    ~EpTask() override;

    bool execute(std::string_view threadName) override;

    /**
     * Gets the engine that this task was scheduled from
     *
     * @returns A handle to the engine
     */
    EventuallyPersistentEngine* getEngine() {
        return engine;
    }

protected:
    /// The engine which owns this task.
    EventuallyPersistentEngine* const engine;
};

/**
 * EpNotifiableTask is a subclass of EpTask which implements Notifiable -
 * tasks that needs to be reliably notified to run by the user.
 *
 * c.f. NotifiableTask - this is identical except it is for EpEngine specific
 * tasks, so switches the current thread to the associated engine instance to
 * ensure correct memory accounting.
 */
class EpNotifiableTask : public EpTask {
public:
    EpNotifiableTask(EventuallyPersistentEngine& e,
                     TaskId id,
                     double sleeptime,
                     bool completeBeforeShutdown = false);

    /**
     * Implementation of run() virtual method, which will call derived class'
     * runInner(), informing if if a manual notification has occurred.
     * Marked as final as subclasses should not override this, but instead
     * should implement runInner().
     */
    bool run() final;

    /**
     * Wake up the task and schedule it to run again, if there isn't already
     * a pending run scheduled.
     *
     * A call to wakeup() schedules another run only if the task has not entered
     * the inner/user-defined runInner() method. The call is a nop otherwise.
     * The implementation ensures that wake-ups aren't lost, but it doesn't
     * prevent 1 extra run from being unnecessarily scheduled, eg:
     *   1. task is in the middle of the processing
     *   2. user detects a state that requires a task run -> run is scheduled
     *   3. the current processing at (1) completes and makes the newly
     *      scheduled run (2) unnecessary
     *   4. run (2) executes
     */
    void wakeup();

protected:
    /**
     * User defined processing function, executed at run().
     *
     * @param manuallyNotified Was the task manually notified to run the last
     * time it started executing? false if the task was woken by the periodic
     * scheduler.
     *
     * @return true if the task needs to be re-scheduled, false otherwise
     */
    virtual bool runInner(bool manuallyNotified) = 0;

    virtual std::chrono::microseconds getSleepTime() const {
        return std::chrono::seconds(INT_MAX);
    }

    std::atomic<bool> pendingRun{false};
};

/**
 * Base type for ep tasks which need to be able to limit how many instances run
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
class EpLimitedConcurrencyTask : public LimitedConcurrencyBase,
                                 public EpNotifiableTask {
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
    EpLimitedConcurrencyTask(EventuallyPersistentEngine& e,
                             TaskId id,
                             cb::AwaitableSemaphore& semaphore,
                             bool completeBeforeShutdown);

    bool runInner(bool) override {
        // Ignore the extra parameter from NotifiableTask as it is not needed.
        return runInner();
    }

    /**
     * Subtypes should provide an implementation for the task.
     */
    virtual bool runInner() = 0;

    /**
     * Called by cb::AwaitableSemaphore when tokens become available.
     *
     * Notifies the task to run.
     *
     * Implements the cb::Waiter interface.
     */
    void signal() override;
};
