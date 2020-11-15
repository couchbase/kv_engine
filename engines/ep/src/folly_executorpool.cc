/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "folly_executorpool.h"

#include "bucket_logger.h"
#include "ep_time.h"
#include "globaltask.h"
#include "taskable.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/thread_factory/PriorityThreadFactory.h>
#include <nlohmann/json.hpp>
#include <platform/string_hex.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>

using namespace std::string_literals;

/**
 * Thread factory for CPU pool threads.
 *  - Gives each thread name based on the given prefix.
 *  - Sets each thread to the given priority.
 */
class CBPriorityThreadFactory : public folly::ThreadFactory {
public:
    CBPriorityThreadFactory(std::string prefix, int priority)
        : priorityThreadFactory(
                  std::make_shared<folly::NamedThreadFactory>(prefix),
                  priority) {
    }

    std::thread newThread(folly::Func&& func) override {
        return priorityThreadFactory.newThread(std::move(func));
    }

    folly::PriorityThreadFactory priorityThreadFactory;
};

/**
 * Proxy object recorded for each registered (scheduled) Task. Inherits from
 * HHWheelTimer::Callback so we can use this as the Callback object for
 * scheduling future executions of the task via HHWheelTimer. When the
 * GlobalTask is ready to run, we call it's run() method, re-scheduling the
 * Task to run again if true is returned.
 *
 */
struct FollyExecutorPool::TaskProxy : public folly::HHWheelTimer::Callback {
    TaskProxy(FollyExecutorPool& executor,
              folly::CPUThreadPoolExecutor& pool,
              ExTask task_)
        : task(std::move(task_)),
          taskId(task->getId()),
          executor(executor),
          cpuPool(pool) {
    }

    ~TaskProxy() override {
        // To ensure that we do not destruct GlobalTask objects on
        // arbitrary threads (if the TaskProxy is the last owner), we
        // should have already called task.reset() before desturction
        // of the TaskProxy.
        Expects(!task &&
                "task shared_ptr should already be empty before destructing "
                "TaskProxy");
    }

    void timeoutExpired() noexcept override {
        // This should be run in the IO pool eventBase, just after the
        // timeout has fired so should not be currently scheduled.
        Expects(!isScheduled());

        EP_LOG_TRACE("TaskProxy::timeoutExpired() id:{} name:{}",
                     task->getId(),
                     GlobalTask::getTaskName(task->getTaskId()));

        scheduleViaCPUPool();
    }

    void callbackCanceled() noexcept override {
        // Callback cancelled. nothing to be done.
        EP_LOG_TRACE("TaskProxy::timeoutCanceled() id:{} name{}",
                     task->getId(),
                     GlobalTask::getTaskName(task->getTaskId()));
    }

    /**
     * Schedules this task to run as soon as possible on its associated CPU
     * pool.
     */
    void scheduleViaCPUPool() {
        using namespace std::chrono;

        EP_LOG_TRACE("TaskProxy::scheduleViaCPUPool() id:{} name:{} descr:{}",
                     task->getId(),
                     GlobalTask::getTaskName(task->getTaskId()),
                     task->getDescription());

        // Mark that the task cannot be re-scheduled at this time - once the
        // task is enqueued onto its CPU pool then we cannot re-schedule it
        // until it is dequeued and run, without potentially running it more
        // than once at the same time. For example, consider this scenario if
        // we _didn't_ inhibit scheduling:
        //
        // 1. Task is enqueued onto CPU pool (this function). This function
        //    returns to eventLoop.
        // 2. Another thread attempts to snooze(short time) the task.
        //    That work will be done on the eventLoop thread, which
        //    re-schedules the Task on IO pool timer.
        // 3. Timer expires for timeout set at (2). timeoutExpired() is called
        //    again, Task is enqueued onto CPU pool.
        //
        // This would result in two copies of the Task enqueued onto the CPU
        // pool :(
        Expects(!scheduledOnCpuPool);
        scheduledOnCpuPool = true;

        // Perform work on the appropriate CPU pool.
        cpuPool.add([& proxy = *this] {
            Expects(proxy.task.get());

            EP_LOG_TRACE("FollyExecutorPool: Run task \"{}\" id {}",
                         proxy.task->getDescription(),
                         proxy.task->getId());
            bool runAgain = false;
            // Check if Task is still alive. If not don't run.
            if (!proxy.task->isdead()) {
                // Call GlobalTask::run(), noting the result.
                // If true: Read GlobalTask::wakeTime. If "now", then re-queue
                // directly on the CPUThreadPool. If some time in the future,
                // then schedule on the IOThreadPool for the given time.
                // If false: Cancel task, will not run again.

                const auto start = steady_clock::now();
                proxy.task->updateLastStartTime(start);

                // Calculate and record scheduler overhead.
                auto scheduleOverhead = start - proxy.task->getWaketime();
                // scheduleOverhead can be a negative number if the task has
                // been woken up before we expected it too be. In this case this
                // means that we have no schedule overhead and thus need to set
                // it too 0.
                if (scheduleOverhead < steady_clock::duration::zero()) {
                    scheduleOverhead = steady_clock::duration::zero();
                }
                proxy.task->getTaskable().logQTime(proxy.task->getTaskId(),
                                                   scheduleOverhead);

                proxy.task->setState(TASK_RUNNING, TASK_SNOOZED);
                runAgain = proxy.task->execute();

                const auto end = steady_clock::now();
                auto runtime = end - start;
                proxy.task->getTaskable().logRunTime(proxy.task->getTaskId(),
                                                     runtime);
                proxy.task->updateRuntime(runtime);
            }

            // If runAgain is false, then task should be cancelled. This is
            // performed in this thread to ensure cancancellation serialised -
            // i.e. when the CPU thread finishes running this function, the
            // task is flagged as dead before any other task (which could check
            // it's status) runs.
            if (!runAgain) {
                proxy.task->cancel();
            }

            // Re-schedule this task. All logic needs to be
            // performed in EventBase so scheduling
            // checks are serialised, to avoid lost wakeup /
            // double-running etc.
            proxy.executor.futurePool->getEventBase()->runInEventBaseThread(
                    [&proxy] {
                        auto& executor = proxy.executor;
                        executor.rescheduleTaskAfterRun(proxy);
                    });
        });
    }

    void resetTaskPtrViaCpuPool() {
        using namespace std::chrono;

        EP_LOG_TRACE(
                "TaskProxy::resetTaskPtrViaCpuPool() id:{} name:{} descr:'{}' "
                "enqueuing func to reset 'task' shared_ptr",
                task->getId(),
                GlobalTask::getTaskName(task->getTaskId()),
                task->getDescription());

        // Move `task` from this object (leaving it as null)
        cpuPool.add([ptrToReset = std::move(task), &proxy = *this]() mutable {
            EP_LOG_TRACE(
                    "FollyExecutorPool::resetTaskPtrViaCpuPool lambda() id:{} "
                    "name:{}",
                    ptrToReset->getId(),
                    GlobalTask::getTaskName(ptrToReset->getTaskId()));

            // Reset the shared_ptr, decrementing it's refcount and potentially
            // deleting the owned object if no other objects (Engine etc) have
            // retained a refcount.
            // Must account this to the relevent bucket.
            {
                BucketAllocationGuard guard(ptrToReset->getEngine());
                ptrToReset.reset();
            }
            // Finally, remove the taskProxy from taskOwners.
            proxy.executor.futurePool->getEventBase()->runInEventBaseThread(
                    [&proxy]() mutable {
                        auto& executor = proxy.executor;
                        executor.removeTaskAfterRun(proxy);
                    });
        });
    }

    /**
     * Updates the timeout to the value of the GlobalTasks' wakeTime
     */
    void updateTimeoutFromWakeTime() {
        auto* eventBase = executor.futurePool->getEventBase();

        // Should only be called from within EventBase thread.
        Expects(eventBase->inRunningEventBaseThread());

        // Check if already scheduled to run on CPU pool - if so then don't
        // want to re-schedule the timeout on the IO pool as that could lead
        // to running the task twice. At the end of the CPU pool executing,
        // it will update the timeout if required.
        if (scheduledOnCpuPool) {
            return;
        }

        using namespace std::chrono;
        auto timeout = duration_cast<milliseconds>(task->getWaketime() -
                                                   steady_clock::now());
        if (timeout > milliseconds::zero()) {
            eventBase->timer().scheduleTimeout(this, timeout);
        } else {
            // Due now - cancel any previously existing timeout and
            // schedule directly on CPU pool.
            cancelTimeout();
            scheduleViaCPUPool();
        }
    }

    void wake() {
        // Should only be called from within EventBase thread.
        Expects(executor.futurePool->getEventBase()
                        ->inRunningEventBaseThread());

        if (!task) {
            // Task has been cancelled ('task' shared ptr reset to
            // null via resetTaskPtrViaCpuPool), but TaskProxy not yet
            // been cleaned up) - i.e. a wake and cancel have raced.
            // Cannot wake (until GlobalTask is re-scheduled) -
            // return.
            return;
        }

        // Cancel any previously set future execution of the
        // task.
        cancelTimeout();

        // Run directly on the appropriate cpuPool, if not
        // already enqueued.
        if (!scheduledOnCpuPool) {
            scheduleViaCPUPool();
        } else {
            // Set wakeTime to "now" - so when the current
            // execution finishes the task will be re-scheduled
            // immediately.
            task->updateWaketime(std::chrono::steady_clock::now());
            task->setState(TASK_RUNNING, TASK_SNOOZED);
        }
    }

    /// shared_ptr to the GlobalTask.
    ExTask task;

    // identifier of the task. This is a copy of data within
    // `task`, because we reset `task` before removing the TaskProxy
    // entry from taskOwners, and need the taskId for that.
    const size_t taskId;

    // Flag used to block re-scheduling of a task while it's in the process
    // of running on CPU pool. See comments in timeoutExpired().
    bool scheduledOnCpuPool{false};

    // Did we re-use the same TaskProxy after a re-schedule?  Used for
    // sanity checking if removeTaskAfterRun() finds a non-null 'task'
    // when executed.
    bool proxyReused{false};

private:
    // Associated FollyExecutorPool (needed for re-scheduling / cancelling
    // dead tasks).
    FollyExecutorPool& executor;

    // TaskPool to be run on.
    folly::CPUThreadPoolExecutor& cpuPool;
};

/*
 * Map of task uniqueIDs to their TaskProxy object, owned via
 * unique_ptr.

 * The IO thead (via FollyExecutorPool::State::taskOwners) owns all
 * TaskProxy objects, but gives (non-owning) pointers to CPU thread
 * pool when a task is scheduled to execute (see scheduleViaCPUPool).
 */
using TaskLocator =
        std::unordered_map<size_t,
                           std::unique_ptr<FollyExecutorPool::TaskProxy>>;

struct TaskOwner {
    /**
     * True if this task owner is currently registered and can have new
     * tasks scheduled against it.
     * Set to false during unregisterTaskable, to prevent new tasks
     * being added once unregister has begun (which is a multi-stage
     * process).
     */
    bool registered{true};

    /// Map of taskID to TaskProxy.
    TaskLocator locator;
};

/// Map of task owners (buckets) to the tasks they own.
using TaskOwnerMap = std::unordered_map<const Taskable*, TaskOwner>;

/**
 * Internal state of the FollyExecutorPool.
 * This should only be accessed by the future (IO) thread, by executing
 * via the eventBase. This constraint serves two purposes:
 * 1. We can avoid locking in this object, given only one thread accesses it.
 * 2. It serialises all access to scheduling state - given there's only one
 *    future (IO) thread, then if we are calling one method (e.g. cancelTask to
 *    cancel a task) it's not possible that that task is elsewhere being
 *    re-scheduled. This significantly simplifies reasoning about potential
 *    concurrently issues.
 *
 * Note that while only the future thread can be accessing this structure,
 * the CPU thread pools (reader/writer/auxIO/nonIO) *can* be running a Task
 * at the same time, so not all concurrency is avoided.
 */
struct FollyExecutorPool::State {
    void addTaskable(Taskable& taskable) {
        taskOwners.try_emplace(&taskable);
    }

    void removeTaskable(Taskable& taskable) {
        taskOwners.erase(&taskable);
    }

    /**
     * Schedule the given Task, adding it to the set of tasks owned by
     * its taskable.
     * @param executor FollyExecutorPool owning the task.
     * @param task The Task to schedule.
     */
    bool scheduleTask(FollyExecutorPool& executor,
                      folly::CPUThreadPoolExecutor& pool,
                      ExTask task) {
        auto& owner = taskOwners.at(&task->getTaskable());
        if (!owner.registered) {
            EP_LOG_WARN(
                    "FollyExecutorPool::scheduleTask(): Attempting to schedule "
                    "task id:{} name:'{}' when Taskable '{}' is not "
                    "registered.",
                    task->getId(),
                    GlobalTask::getTaskName(task->getTaskId()),
                    task->getTaskable().getName());
            return false;
        }

        auto [it, inserted] = owner.locator.try_emplace(
                task->getId(), std::unique_ptr<TaskProxy>{});
        if (!inserted) {
            // taskId already present - i.e. this taskId has already been
            // scheduled.
            // It it only valid to re-schedule a task if it was previously
            // cancelled, but we hadn't cleaned up the cancellation - the
            // 'task' shared_ptr is null.
            if (it->second->task) {
                return false;
            }
            // re-assign task to the one passed in.
            it->second->task = task;
            it->second->proxyReused = true;
        } else {
            // Inserted a new entry into map - create a TaskProxy object for it.
            it->second = std::make_unique<TaskProxy>(executor, pool, task);
        }

        // If we are rescheduling a previously cancelled task, we should
        // reset the task state to the initial value of running.
        it->second->task->setState(TASK_RUNNING, TASK_DEAD);

        it->second->updateTimeoutFromWakeTime();
        return true;
    }

    /**
     * Wakes the task with the given identifier.
     * @returns true if a task was found with the given id, else false.
     */
    bool wakeTask(size_t taskId) {
        // Search for the given taskId across all buckets.
        // PERF: CB3ExecutorPool uses a secondary map (taskLocator) to allow
        // O(1) lookup by taskId, which is reasonable given
        // ExecutorPool::wake() is called frequently. However, that essentially
        // forces an ExecutorPool implementation to have a serialization point
        // to locate the actual ExTask object.
        // Option 1 - Change the API of wake() (and other similar methods like
        // cancel()) to take a GlobalTask& object directly, avoiding the
        // lookup.
        // Option 2 - Keep the API unchanged, but add a taskLocator map to
        // FollyExecutorPool say guarded by folly::Synchronized, allowing
        // multiple readers (most lookups will be not be adding/removing
        // things from taskLocator).
        TaskLocator::iterator it;
        for (auto& owner : taskOwners) {
            auto& tasks = owner.second;
            it = tasks.locator.find(taskId);
            if (it != tasks.locator.end()) {
                it->second->wake();
                return true;
                break;
            }
        }
        return false;
    }

    /**
     * Snoozes the task with the given identifier, to sleep for the specified
     * number of seconds.
     * @returns true if a task was found with the given id, else false.
     */
    bool snoozeTask(size_t taskId, double toSleep) {
        // Search for the given taskId across all buckets.
        // PERF: CB3ExecutorPool uses a secondary map (taskLocator) - see
        // comments in wakeTask().
        TaskLocator::iterator it;
        for (auto& owner : taskOwners) {
            auto& tasks = owner.second;
            it = tasks.locator.find(taskId);
            if (it != tasks.locator.end()) {
                it->second->task->snooze(toSleep);
                it->second->updateTimeoutFromWakeTime();
                return true;
                break;
            }
        }
        return false;
    }

    /**
     * Cancels all tasks owned by the given taskable, returning a vector
     * of the cancelled tasks.
     * @param taskable Taskable to cancel tasks for
     * @param force Should task cancellation be forced?
     */
    std::vector<ExTask> cancelTasksOwnedBy(const Taskable& taskable,
                                           bool force) {
        std::vector<ExTask> removedTasks;
        auto& taskOwner = taskOwners.at(&taskable);
        taskOwner.registered = false;

        for (auto& it : taskOwner.locator) {
            auto& tProxy = it.second;
            if (!tProxy->task) {
                // Task already cancelled (shared pointer reset to null) by
                // canelTask() - skip.
                continue;
            }
            EP_LOG_DEBUG(
                    "FollyExecutorPool::cancelTasksOwnedBy(): Stopping "
                    "Task id:{} taskable:{} description:'{}'",
                    tProxy->task->getId(),
                    tProxy->task->getTaskable().getName(),
                    tProxy->task->getDescription());

            // If force flag is set during shutdown, cancel all tasks
            // without considering the blockShutdown status of the task.
            if (force || !tProxy->task->blockShutdown) {
                tProxy->task->cancel();
            }
            tProxy->wake();

            // Copy the task from the (now cancelled) TaskInfo to the return
            // vector.
            // Note we cannot move (set tProxy->task) to nullptr) as it is
            // possible the Task is currently running on a CPU thread.
            removedTasks.push_back(tProxy->task);
        }
        return removedTasks;
    }

    /**
     * Cancel the specified task.
     *
     * @param taskId Task to cancel
     * @return True if task found, else false.
     */
    bool cancelTask(size_t taskId) {
        // Search for the given taskId across all buckets.
        // PERF: CB3ExecutorPool uses a secondary map (taskLocator)
        // to allow O(1) lookup by taskId, however cancel() isn't a
        // particularly hot function so I'm not sure if the extra
        // complexity is warranted. If this shows up as hot then
        // consider adding a similar structure to FollyExecutorPool.
        TaskLocator::iterator it;
        for (auto& [owner, tasks] : taskOwners) {
            it = tasks.locator.find(taskId);
            if (it != tasks.locator.end()) {
                EP_LOG_TRACE(
                        "FollyExecutorPool::cancelTask() id:{} found for "
                        "owner:'{}'",
                        taskId,
                        owner->getName());

                if (!it->second->task) {
                    // Task already cancelled (shared pointer reset to null) by
                    // some previous call to cancelTask().
                    return false;
                }

                it->second->task->cancel();

                // Now `task` has been cancelled, we need to remove our
                // reference (shared ownership) to the owned GlobalTask and from
                // taskOwners.  Decrementing our refcount could delete the
                // GlobalTask (if we are the last owner). This must occur on a
                // CPU thread given GlobalTask destruction can be an arbitrary
                // amount of work.
                if (it->second->scheduledOnCpuPool) {
                    // Currently scheduled on CPU pool - TaskProxy is "owned" by
                    // CPU thread.  Given we just called cancel() on the
                    // GlobalTask, we can rely on rescheduleTaskAfterRun to
                    // reset the TaskProxy (when it calls cancelTask()).
                    return true;
                }

                // Not currently scheduled on CPU pool - this thread (IO pool)
                // owns it. To perform refcount drop on CPu thread, we move the
                // shared_ptr from taskOwners (taskOwners entry remains but is
                // null), then pass the moved shared_ptr to CPU pool to perform
                // refcount decrement (and potential GlobalTask
                // (IO pool) owns it.
                // First cancel any pending timeout - shouldn't run again.
                it->second->cancelTimeout();

                // Next, to perform refcount drop on CPU thread, we move the
                // shared_ptr from taskOwners (taskOwners entry remains but is
                // null), then pass the moved shared_ptr to CPU pool to perform
                // refcount decrement (and potential GlobalTask deletion).
                // Finally CPU pool will schedule a final IO thread function to
                // actually erase element from taskOwners.
                it->second->resetTaskPtrViaCpuPool();
                return true;
            }
        }
        return false;
    }

    /**
     * Remove the cancelled task from taskOwners.
     *
     * @param taskId Task to remove
     * @return True if task found, else false.
     */
    bool removeTask(size_t taskId) {
        for (auto& [owner, tasks] : taskOwners) {
            auto it = tasks.locator.find(taskId);
            if (it != tasks.locator.end()) {
                EP_LOG_TRACE(
                        "FollyExecutorPool::State::removeTask() erasing task "
                        "id:{} for "
                        "owner:'{}'",
                        taskId,
                        owner->getName());
                Expects(!it->second->task &&
                        "removeTask: 'proxy->task' should be null before "
                        "removing element from taskOwners");
                tasks.locator.erase(it);
                return true;
            }
        }
        return false;
    }

    /// @returns the number of taskables registered.
    int numTaskables() const {
        return taskOwners.size();
    }

    /**
     * Returns the number of tasks owned by the specified taskable.
     */
    int numTasksForOwner(Taskable& taskable) {
        return taskOwners.at(&taskable).locator.size();
    };

    /**
     * Returns a vector of all tasks owned by the given Taskable
     */
    std::vector<ExTask> copyTasksForOwner(Taskable& taskable) const {
        std::vector<ExTask> tasks;
        for (auto& it : taskOwners.at(&taskable).locator) {
            if (it.second->task) {
                tasks.push_back(it.second->task);
            }
        }
        return tasks;
    }

    /**
     * @returns counts of how many tasks are waiting to run
     * (isScheduled() == true) for each task group.
     */
    std::array<int, NUM_TASK_GROUPS> getWaitingTasksPerGroup() {
        std::array<int, NUM_TASK_GROUPS> waitingTasksPerGroup;
        for (const auto& owner : taskOwners) {
            for (const auto& task : owner.second.locator) {
                if (task.second->isScheduled()) {
                    const auto type = GlobalTask::getTaskType(
                            task.second->task->getTaskId());
                    waitingTasksPerGroup[type]++;
                }
            }
        }
        return waitingTasksPerGroup;
    }

private:
    /// Map of registered task owners (Taskables) to the Tasks they own.
    TaskOwnerMap taskOwners;
};

FollyExecutorPool::FollyExecutorPool(size_t maxThreads,
                                     ThreadPoolConfig::ThreadCount maxReaders_,
                                     ThreadPoolConfig::ThreadCount maxWriters_,
                                     size_t maxAuxIO_,
                                     size_t maxNonIO_)
    : ExecutorPool(maxThreads),
      state(std::make_unique<State>()),
      maxReaders(calcNumReaders(maxReaders_)),
      maxWriters(calcNumWriters(maxWriters_)),
      maxAuxIO(calcNumAuxIO(maxAuxIO_)),
      maxNonIO(calcNumNonIO(maxNonIO_)) {
    /*
     * Define a function to create thread factory with a given prefix,
     * and priority where supported.
     *
     * Only setting priority for Linux at present:
     *  - On Windows folly's getpriority() compatibility function changes the
     *    priority of the entire process.
     *  - On macOS setpriority(PRIO_PROCESS) affects the entire process (unlike
     *    Linux where it's only the current thread), hence calling
     *    setpriority() would be pointless.
     */
    auto makeThreadFactory = [](std::string prefix, task_type_t taskType) {
#if defined(__linux__)
        return std::make_shared<CBPriorityThreadFactory>(
                prefix, ExecutorPool::getThreadPriority(taskType));
#else
        return std::make_shared<folly::NamedThreadFactory>(prefix);
#endif
    };

    futurePool = std::make_unique<folly::IOThreadPoolExecutor>(
            1, std::make_shared<folly::NamedThreadFactory>("SchedulerPool"));

    readerPool = std::make_unique<folly::CPUThreadPoolExecutor>(
            maxReaders, makeThreadFactory("ReaderPool", READER_TASK_IDX));
    writerPool = std::make_unique<folly::CPUThreadPoolExecutor>(
            maxWriters, makeThreadFactory("WriterPool", WRITER_TASK_IDX));
    auxPool = std::make_unique<folly::CPUThreadPoolExecutor>(
            maxAuxIO, makeThreadFactory("AuxIoPool", AUXIO_TASK_IDX));
    nonIoPool = std::make_unique<folly::CPUThreadPoolExecutor>(
            maxNonIO, makeThreadFactory("NonIoPool", NONIO_TASK_IDX));
}

FollyExecutorPool::~FollyExecutorPool() {
    // We need a custom dtor because Tasks running on IO thread read the value
    // of futurePool; therefore we cannot rely on the default dtor to simply
    // call reset() and set ptr to null before all IO threads have stopped.
    // Instead we must explicitly stop all IO threads, then reset()
    // the futurePool.
    nonIoPool.reset();
    auxPool.reset();
    writerPool.reset();
    readerPool.reset();

    auto* eventBase = futurePool->getEventBase();
    eventBase->runInEventBaseThreadAndWait(
            [eventBase] { eventBase->timer().cancelAll(); });
    futurePool.reset();
}

size_t FollyExecutorPool::getNumWorkersStat() {
    return readerPool->numThreads() + writerPool->numThreads() +
           auxPool->numThreads() + nonIoPool->numThreads();
}

size_t FollyExecutorPool::getNumReaders() {
    return calcNumReaders(ThreadPoolConfig::ThreadCount(maxReaders));
}

size_t FollyExecutorPool::getNumWriters() {
    return calcNumWriters(ThreadPoolConfig::ThreadCount(maxWriters));
}

size_t FollyExecutorPool::getNumAuxIO() {
    return auxPool->getPoolStats().threadCount;
}

size_t FollyExecutorPool::getNumNonIO() {
    return nonIoPool->getPoolStats().threadCount;
}

void FollyExecutorPool::setNumReaders(ThreadPoolConfig::ThreadCount v) {
    maxReaders = calcNumReaders(v);
    readerPool->setNumThreads(maxReaders);
}

void FollyExecutorPool::setNumWriters(ThreadPoolConfig::ThreadCount v) {
    maxWriters = calcNumWriters(v);
    writerPool->setNumThreads(maxWriters);
}

void FollyExecutorPool::setNumAuxIO(uint16_t v) {
    maxAuxIO = v;
    auxPool->setNumThreads(maxAuxIO);
}

void FollyExecutorPool::setNumNonIO(uint16_t v) {
    maxNonIO = v;
    nonIoPool->setNumThreads(maxNonIO);
}

size_t FollyExecutorPool::getNumSleepers() {
    return readerPool->getPoolStats().idleThreadCount +
           writerPool->getPoolStats().idleThreadCount +
           auxPool->getPoolStats().idleThreadCount +
           nonIoPool->getPoolStats().idleThreadCount;
}

size_t FollyExecutorPool::getNumReadyTasks() {
    return readerPool->getPendingTaskCount() +
           writerPool->getPendingTaskCount() + auxPool->getPendingTaskCount() +
           nonIoPool->getPendingTaskCount();
}

void FollyExecutorPool::registerTaskable(Taskable& taskable) {
    NonBucketAllocationGuard guard;

    if (taskable.getWorkLoadPolicy().getBucketPriority() <
        HIGH_BUCKET_PRIORITY) {
        taskable.setWorkloadPriority(LOW_BUCKET_PRIORITY);
        EP_LOG_INFO("Taskable {} registered with low priority",
                    taskable.getName());
    } else {
        taskable.setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        EP_LOG_INFO("Taskable {} registered with high priority",
                    taskable.getName());
    }

    futurePool->getEventBase()->runInEventBaseThreadAndWait(
            [state = this->state.get(), &taskable]() {
                state->addTaskable(taskable);
            });
}

std::vector<ExTask> FollyExecutorPool::unregisterTaskable(Taskable& taskable,
                                                          bool force) {
    NonBucketAllocationGuard guard;

    EP_LOG_TRACE(
            "FollyExecutorPool::unregisterTaskable() taskable:'{}' force:{}",
            taskable.getName(),
            force);

    // We need to ensure that all tasks owned by this taskable have
    // stopped when this function returns. Tasks can be in one of three
    // states:
    // 1. Snoozing, waiting to run at some future time.
    // 2. Scheduled to run on a CPU pool thread.
    // 3. Currently running on a CPU pool thread.
    //
    // For (1), we can cancel their timeout. If we perform this on the eventBase
    // of the futurePool thread then we are guaranteed to have no more tasks
    // scheduled after, given scheduling always happens on the futurePool
    // eventBase.
    //
    // For (2) and (3), they are equivalent - between performing any check
    // on the CPU pool and examaning the result, new CPU work could have been
    // executeed. Therefore we handle these by:
    // - Marking all tasks as dead (during 1)
    // - polling the taskOwners structure (on the futurePool thread) for all
    //   tasks to be cancelled (and hence removed) - which happens once a
    //   task finishes running in the CPU pool.
    // Once taskOwners is empty we are done.
    std::vector<ExTask> removedTasks;

    // Step 1 - Have the eventbase of the futurePool cancel all tasks
    // associated with this Taskable.
    // Performing on futurePool guarantees that none of the Taskable's tasks
    // can currently be running on the futurePool (and hence being added to
    // CPU pool.
    auto* eventBase = futurePool->getEventBase();
    eventBase->runInEventBaseThreadAndWait(
            [state = this->state.get(), &taskable, force, &removedTasks] {
                removedTasks = state->cancelTasksOwnedBy(taskable, force);
            });

    // Step 2 - poll for taskOwners to become empty. This will only
    // occur once all outstanding, running tasks have been cancelled.
    auto isTaskOwnersEmpty = [eventBase, &state = this->state, &taskable] {
        bool empty = false;
        eventBase->runInEventBaseThreadAndWait([&state, &taskable, &empty] {
            empty = state->numTasksForOwner(taskable) == 0;
        });
        return empty;
    };

    while (!isTaskOwnersEmpty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }

    // Finally, remove entry for the unregistered Taskable.
    eventBase->runInEventBaseThreadAndWait(
            [& state = this->state, &taskable]() mutable {
                state->removeTaskable(taskable);
            });

    return removedTasks;
}

size_t FollyExecutorPool::getNumTaskables() const {
    int numTaskables = 0;
    futurePool->getEventBase()->runInEventBaseThreadAndWait(
            [state = this->state.get(), &numTaskables] {
                numTaskables = state->numTaskables();
            });
    return numTaskables;
}

size_t FollyExecutorPool::schedule(ExTask task) {
    NonBucketAllocationGuard guard;

    using namespace std::chrono;
    EP_LOG_TRACE(
            "FollyExecutorPool::schedule() id:{} name:{} type:{} wakeTime:{}",
            task->getId(),
            GlobalTask::getTaskName(task->getTaskId()),
            GlobalTask::getTaskType(task->getTaskId()),
            task->getWaketime().time_since_epoch().count());

    auto* eventBase = futurePool->getEventBase();
    auto* pool = getPoolForTaskType(GlobalTask::getTaskType(task->getTaskId()));
    Expects(pool);
    eventBase->runInEventBaseThreadAndWait([eventBase, this, pool, &task] {
        state->scheduleTask(*this, *pool, task);
    });

    return task->getId();
}

bool FollyExecutorPool::cancel(size_t taskId, bool eraseTask) {
    NonBucketAllocationGuard guard;

    EP_LOG_TRACE("FollyExecutorPool::cancel() id:{} eraseTask:{}",
                 taskId,
                 eraseTask);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runInEventBaseThreadAndWait(
            [&found, state = state.get(), taskId] {
                state->cancelTask(taskId);
            });
    return found;
}

void FollyExecutorPool::wake(size_t taskId) {
    NonBucketAllocationGuard guard;

    EP_LOG_TRACE("FollyExecutorPool::wake() id:{}", taskId);

    auto* eventBase = futurePool->getEventBase();
    eventBase->runInEventBaseThread(
            [state = state.get(), taskId] { state->wakeTask(taskId); });
}

bool FollyExecutorPool::wakeAndWait(size_t taskId) {
    NonBucketAllocationGuard guard;

    EP_LOG_TRACE("FollyExecutorPool::wakeAndWait() id:{}", taskId);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runInEventBaseThreadAndWait(
            [&found, state = state.get(), taskId] {
                found = state->wakeTask(taskId);
            });
    return found;
}

bool FollyExecutorPool::snooze(size_t taskId, double toSleep) {
    NonBucketAllocationGuard guard;
    using namespace std::chrono;
    EP_LOG_TRACE(
            "FollyExecutorPool::snooze() id:{} toSleep:{}", taskId, toSleep);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runInEventBaseThreadAndWait(
            [&found, eventBase, state = state.get(), taskId, toSleep] {
                found = state->snoozeTask(taskId, toSleep);
            });
    return found;
}

void FollyExecutorPool::doWorkerStat(Taskable& taskable,
                                     const void* cookie,
                                     const AddStatFn& add_stat) {
    // It's not possible directly introspect what a Folly ExecutorThread
    // is running.
    // We _could_ implement similar functionality by manually tracking what is
    // running on each thread, but that would add additional costs & complexity
    // to TaskProxy, and it's been rare that the per-thread currently-running
    // task has been of use.
    // As such, no worker stats currently provided.
}

void FollyExecutorPool::doTasksStat(Taskable& taskable,
                                    const void* cookie,
                                    const AddStatFn& add_stat) {
    NonBucketAllocationGuard guard;

    // Take a copy of the taskOwners map.
    auto* eventBase = futurePool->getEventBase();
    std::vector<ExTask> tasks;
    eventBase->runInEventBaseThreadAndWait(
            [state = this->state.get(), &taskable, &tasks] {
                tasks = state->copyTasksForOwner(taskable);
            });

    // Convert to JSON for output.
    nlohmann::json list = nlohmann::json::array();

    for (const auto& task : tasks) {
        nlohmann::json obj;

        obj["tid"] = task->getId();
        obj["state"] = to_string(task->getState());
        obj["name"] = GlobalTask::getTaskName(task->getTaskId());
        obj["this"] = cb::to_hex(reinterpret_cast<uint64_t>(task.get()));
        obj["bucket"] = task->getTaskable().getName();
        obj["description"] = task->getDescription();
        obj["priority"] = task->getQueuePriority();
        obj["waketime_ns"] = task->getWaketime().time_since_epoch().count();
        obj["total_runtime_ns"] = task->getTotalRuntime().count();
        obj["last_starttime_ns"] =
                to_ns_since_epoch(task->getLastStartTime()).count();
        obj["previous_runtime_ns"] = task->getPrevRuntime().count();
        obj["num_runs"] = task->getRunCount();
        obj["type"] = to_string(GlobalTask::getTaskType(task->getTaskId()));

        list.push_back(obj);
    }

    add_casted_stat("ep_tasks:tasks", list.dump(), add_stat, cookie);
    add_casted_stat("ep_tasks:cur_time",
                    to_ns_since_epoch(std::chrono::steady_clock::now()).count(),
                    add_stat,
                    cookie);
    add_casted_stat("ep_tasks:uptime_s", ep_current_time(), add_stat, cookie);
}

void FollyExecutorPool::doTaskQStat(Taskable& taskable,
                                    const void* cookie,
                                    const AddStatFn& add_stat) {
    NonBucketAllocationGuard guard;

    // Count how many tasks of each type are waiting to run - defined by
    // having an outstanding timeout.
    // Note: This mimics the behaviour of CB3ExecutorPool, which counts _all_
    // tasks across all taskables. This may or may not be the correct
    // behaviour...
    // The counting is done on the eventbase thread given it would be
    // racy to directly access the taskOwners from this thread.
    auto* eventBase = futurePool->getEventBase();
    std::array<int, NUM_TASK_GROUPS> waitingTasksPerGroup;
    eventBase->runInEventBaseThreadAndWait(
            [state = this->state.get(), &waitingTasksPerGroup] {
                waitingTasksPerGroup = state->getWaitingTasksPerGroup();
            });

    // Currently FollyExecutorPool implements a single task queue (per task
    // type) - report that as low priority.
    fmt::memory_buffer buf;
    add_casted_stat("ep_workload:LowPrioQ_Writer:InQsize",
                    waitingTasksPerGroup[WRITER_TASK_IDX],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_Reader:InQsize",
                    waitingTasksPerGroup[READER_TASK_IDX],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_AuxIO:InQsize",
                    waitingTasksPerGroup[AUXIO_TASK_IDX],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_NonIO:InQsize",
                    waitingTasksPerGroup[NONIO_TASK_IDX],
                    add_stat,
                    cookie);

    add_casted_stat("ep_workload:LowPrioQ_Writer:OutQsize",
                    writerPool->getTaskQueueSize(),
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_Reader:OutQsize",
                    readerPool->getTaskQueueSize(),
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_AuxIO:OutQsize",
                    auxPool->getTaskQueueSize(),
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_NonIO:OutQsize",
                    nonIoPool->getTaskQueueSize(),
                    add_stat,
                    cookie);
}

folly::CPUThreadPoolExecutor* FollyExecutorPool::getPoolForTaskType(
        task_type_t type) {
    switch (type) {
    case NO_TASK_TYPE:
        folly::assume_unreachable();
    case WRITER_TASK_IDX:
        return writerPool.get();
    case READER_TASK_IDX:
        return readerPool.get();
    case AUXIO_TASK_IDX:
        return auxPool.get();
    case NONIO_TASK_IDX:
        return nonIoPool.get();
    case NUM_TASK_GROUPS:
        folly::assume_unreachable();
    }
    folly::assume_unreachable();
}

void FollyExecutorPool::rescheduleTaskAfterRun(TaskProxy& proxy) {
    // Should only be called from within EventBase thread.
    auto* eventBase = futurePool->getEventBase();
    Expects(eventBase->inRunningEventBaseThread());

    EP_LOG_TRACE(
            "FollyExecutorPool::rescheduleTaskAfterRun() id:{} name:{} "
            "descr:'{}' "
            "state:{}",
            proxy.task->getId(),
            GlobalTask::getTaskName(proxy.task->getTaskId()),
            proxy.task->getDescription(),
            to_string(proxy.task->getState()));

    // We have just finished running the task on the CPU thread pool, therefore
    // it should _not_ be scheduled to run again yet (given that's what we are
    // about to calculate in a moment). This is important as if we were already
    // scheduled, it's possible the task could be run again _before_ we've
    // logically completed the current instance of it.
    Expects(!proxy.isScheduled());

    // Scheduling should have been inhibited when the Task was first enqueued
    // on CPU pool.
    Expects(proxy.scheduledOnCpuPool);
    // ... but now we are done with the CPU pool - clear flag so it can be
    // re-scheduled if necessary.
    proxy.scheduledOnCpuPool = false;

    if (proxy.task->isdead()) {
        // Deschedule the task, in case it was already scheduled
        proxy.cancelTimeout();

        // Begin process of cancelling the task - mark as cancelled in
        // taskOwners and schedule another CPU thread pool function to decrement
        // the refcount and potentially delete the GlobalTask.
        state->cancelTask(proxy.task->getId());

        return;
    }

    // Task still alive, so should be run again. In the future or immediately?
    using namespace std::chrono;
    const auto timeout = duration_cast<milliseconds>(proxy.task->getWaketime() -
                                                     steady_clock::now());
    if (timeout > milliseconds::zero()) {
        eventBase->timer().scheduleTimeout(&proxy, timeout);
    } else {
        // Due now - schedule directly on CPU pool.
        proxy.scheduleViaCPUPool();
    }
}

void FollyExecutorPool::removeTaskAfterRun(TaskProxy& proxy) {
    EP_LOG_TRACE("TaskProxy::removeTaskAfterRun() id:{} name:{}",
                 proxy.taskId,
                 proxy.task ? ("RESURRECTED:"s +
                               GlobalTask::getTaskName(proxy.task->getTaskId()))
                            : "<null>"s);

    if (proxy.task) {
        Expects(proxy.proxyReused);
        return;
    }

    // Deschedule the task, in case it was already scheduled
    proxy.cancelTimeout();

    // Erase the task from taskOwners. If the TaskProxy is the last
    // shared owner of the GlobalTask, that will be deleted here.

    // PERF: CB3ExecutorPool uses a secondary map (taskLocator) to
    // allow O(1) lookup of ownwe by taskId, however cancelling
    // isn't a particularly hot function so I'm not sure if the
    // extra complexity is warranted. If this shows up as hot then
    // consider adding a similar structure to FollyExecutorPool.
    bool taskFound = state->removeTask(proxy.taskId);
    if (!taskFound) {
        auto msg = fmt::format(
                "FollyExecutorPool::rescheduleTaskAfterRun(): Failed to locate "
                "an owner for task id:{}",
                proxy.taskId);
        throw std::logic_error(msg);
    }
}
