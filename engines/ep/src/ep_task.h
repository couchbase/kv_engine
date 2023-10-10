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
