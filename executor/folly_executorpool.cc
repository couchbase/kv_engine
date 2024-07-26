/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "folly_executorpool.h"

#include "cancellable_cpu_executor.h"
#include "globaltask.h"
#include "taskable.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/thread_factory/PriorityThreadFactory.h>
#include <logger/logger.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_arena_malloc.h>
#include <platform/string_hex.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>

using namespace std::string_literals;

/**
 * Thread factory wrapper.
 *
 * Wraps another thread factory, and registers/deregisters created threads with
 * Phosphor for tracing.
 */
class CBRegisteredThreadFactory : public folly::ThreadFactory {
public:
    CBRegisteredThreadFactory(
            std::shared_ptr<folly::ThreadFactory> threadFactory)
        : threadFactory(std::move(threadFactory)) {
    }
    std::thread newThread(folly::Func&& func) override {
        return threadFactory->newThread([func = std::move(func)]() mutable {
            auto threadNameOpt = folly::getCurrentThreadName();

            phosphor::TraceLog::getInstance().registerThread(
                    threadNameOpt.value_or(""));
            func();
            phosphor::TraceLog::getInstance().deregisterThread();
        });
    }

private:
    std::shared_ptr<folly::ThreadFactory> threadFactory;
};

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
              CancellableCPUExecutor& pool,
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

        LOG_TRACE("TaskProxy::timeoutExpired() id:{} name:{}",
                  task->getId(),
                  GlobalTask::getTaskName(task->getTaskId()));

        scheduleViaCPUPool();
    }

    void callbackCanceled() noexcept override {
        // Callback cancelled. nothing to be done.
        LOG_TRACE("TaskProxy::timeoutCanceled() id:{} name{}",
                  task->getId(),
                  GlobalTask::getTaskName(task->getTaskId()));
    }

    /**
     * Schedules this task to run as soon as possible on its associated CPU
     * pool.
     */
    void scheduleViaCPUPool() {
        using namespace std::chrono;

        LOG_TRACE("TaskProxy::scheduleViaCPUPool() id:{} name:{} descr:{}",
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
        cpuPool.add(task.get(), [&proxy = *this] {
            Expects(proxy.task.get());

            LOG_TRACE("FollyExecutorPool: Run task \"{}\" id {}",
                      proxy.task->getDescription(),
                      proxy.task->getId());
            bool runAgain = proxy.task->execute(proxy.getCurrentThreadName());

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

    void resetTaskPtr(std::atomic<int>& pendingResets, bool resetOnScheduler) {
        LOG_TRACE(
                "FollyExecutorPool::TaskProxy::resetTaskPtr() id:{} "
                "name:{} descr:'{}' "
                "enqueuing func to reset 'task' shared_ptr resetOnScheduler:{}",
                task->getId(),
                GlobalTask::getTaskName(task->getTaskId()),
                task->getDescription(),
                resetOnScheduler);

        using namespace std::chrono;

        auto resetLambda = [ptrToReset = std::move(task),
                            &pendingResets,
                            resetOnScheduler]() mutable {
            LOG_TRACE(
                    "FollyExecutorPool::TaskProxy::resetTaskPtr() "
                    "lambda() id:{} name:{} state:{} resetOnScheduler:{}",
                    ptrToReset->getId(),
                    GlobalTask::getTaskName(ptrToReset->getTaskId()),
                    to_string(ptrToReset->getState()),
                    resetOnScheduler);

            // Reset the shared_ptr, decrementing it's refcount and potentially
            // deleting the owned object if no other objects (Engine etc) have
            // retained a refcount.
            // Must account this to the relevent bucket.
            {
                ptrToReset->getTaskable().invokeViaTaskable(
                        [&ptrToReset, &pendingResets]() {
                            ptrToReset.reset();
                            pendingResets--;
                        });
            }
        };

        if (resetOnScheduler) {
            resetLambda();
        } else {
            cpuPool.add(nullptr, std::move(resetLambda));
        }
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

        // Don't attempt to schedule when sleeping forever
        if (task->isSleepingForever()) {
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
            // null via resetTaskPtr), but TaskProxy not yet been cleaned up)
            // - i.e. a wake and cancel have raced. Cannot wake (until
            // GlobalTask is re-scheduled) - return.
            return;
        }

        // Cancel any previously set future execution of the
        // task, and set its waketime (scheduled time) to now.
        cancelTimeout();
        task->updateWaketime(std::chrono::steady_clock::now());
        task->setState(TASK_RUNNING, TASK_SNOOZED);

        if (!scheduledOnCpuPool) {
            // Not currently scheduled on cpuPool - schedule to run now.
            scheduleViaCPUPool();
        } else {
            // Task already scheduled on cpuPool - given wakeTime was set to
            // "now" just above, when the current execution finishes the task
            // will be re-scheduled to run immediately.
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

private:
    /**
     * Helper method to retrieve the name of the currently running thread.
     *
     * Result is cached in thread-local as thread name lookup can require a
     * system call; hence wouldn't handle changing thread names (but we don't
     * do that).
     */
    std::string_view getCurrentThreadName() const {
        static thread_local std::string name =
                folly::getCurrentThreadName().value_or("unknown");
        return name;
    }

    // Associated FollyExecutorPool (needed for re-scheduling / cancelling
    // dead tasks).
    FollyExecutorPool& executor;

    // TaskPool to be run on.
    CancellableCPUExecutor& cpuPool;
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

    /// Number of tasks which are pending reset (on the relevent CPU pool),
    /// used to determine when it is safe for unregisterTaskable() to complete.
    std::atomic<int> pendingTaskResets{0};
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
                      CancellableCPUExecutor& pool,
                      ExTask task) {
        auto& owner = taskOwners.at(&task->getTaskable());
        if (!owner.registered) {
            LOG_WARNING(
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
            // It is not valid to schedule a task twice.
            LOG_WARNING(
                    "FollyExecutorPool::scheduleTask(): Task with id:{} "
                    "({}) is already registered against Taskable '{}'",
                    task->getId(),
                    task->getDescription(),
                    GlobalTask::getTaskName(task->getTaskId()));
            return false;
        }
        // Inserted a new entry into map - create a TaskProxy object for it.
        it->second = std::make_unique<TaskProxy>(executor, pool, task);

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
                auto& proxy = it->second;
                if (!proxy->task) {
                    // Task has been cancelled ('task' shared ptr reset to
                    // null via resetTaskPtr), but TaskProxy not yet been
                    // cleaned up) - i.e. a snooze and cancel have raced. Treat
                    // as if cancellation has completed and task no longer
                    // present.
                    return false;
                }
                proxy->task->snooze(toSleep);
                proxy->updateTimeoutFromWakeTime();
                return true;
                break;
            }
        }
        return false;
    }

    /**
     * Cancels all tasks owned by the given taskable.
     * @param taskable Taskable to cancel tasks for
     * @param force Should task cancellation be forced?
     */
    void cancelTasksOwnedBy(const Taskable& taskable, bool force) {
        auto& taskOwner = taskOwners.at(&taskable);
        taskOwner.registered = false;

        for (auto& it : taskOwner.locator) {
            auto& tProxy = it.second;
            if (!tProxy->task) {
                // Task already cancelled (shared pointer reset to null) by
                // canelTask() - skip.
                continue;
            }
            LOG_DEBUG(
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
        }
    }

    /**
     * Cancel the specified task.
     *
     * @param taskId Task to cancel
     * @param force forcefully cancel the task
     * @return True if task found, else false.
     */
    bool cancelTask(size_t taskId, bool force = false) {
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
                LOG_TRACE(
                        "FollyExecutorPool::State::cancelTask() id:{} found "
                        "for "
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
                // reference (shared ownership) to the owned GlobalTask and
                // delete the TaskProxy from  taskOwners.  Decrementing our
                // refcount could delete the GlobalTask (if we are the last
                // owner).
                if (it->second->scheduledOnCpuPool && !force) {
                    // Task is currently scheduled on CPU pool - TaskProxy is
                    // in use by CPU thread.  Given we just called cancel() on
                    // the GlobalTask, we can rely on rescheduleTaskAfterRun to
                    // reset the TaskProxy (when it calls cancelTask()).
                    return true;
                }

                // Not currently scheduled on CPU pool - this thread (IO pool)
                // owns it.
                // First cancel any pending timeout - shouldn't run again.
                it->second->cancelTimeout();

                // Increment count of tasks which are pending reset;
                // we must wait for this to be zero (i.e. the reset has
                // executed on the CPU pool) before a Taskable can
                // be considered unregistered.
                tasks.pendingTaskResets++;

                // Next, to perform refcount drop, we reset the
                // shared_ptr<GlobalTask> from TaskProxy to perform refcount
                // decrement (and potential GlobalTask deletion). If we are
                // running this during shutdown then we want to run the
                // resetTaskPtr work on the scheduler thread to avoid it having
                // to wait in the cpuPool. If it has to wait then the slowness
                // may cause a rebalance failure due to bucket deletion timeout.
                // owner->isShutdown() should be set before we start calling
                // unregisterTaskable so the owner->isShutdown() check also
                // covers when force (unregisterTaskable path) is true.
                it->second->resetTaskPtr(tasks.pendingTaskResets,
                                         owner->isShutdown());

                // We can now erase the TaxkProxy from taskOwners.
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
     * Returns the number of tasks owned by the specified taskable, plus
     * any pending reset after being cancelled.
     */
    int numTasksForOwner(const Taskable& taskable) {
        auto& owner = taskOwners.at(&taskable);
        return owner.locator.size() + owner.pendingTaskResets;
    }

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
    std::array<int, static_cast<size_t>(TaskType::Count)>
    getWaitingTasksPerGroup() {
        std::array<int, static_cast<size_t>(TaskType::Count)>
                waitingTasksPerGroup{};
        for (const auto& owner : taskOwners) {
            for (const auto& task : owner.second.locator) {
                if (task.second->isScheduled()) {
                    const auto type =
                            static_cast<size_t>(GlobalTask::getTaskType(
                                    task.second->task->getTaskId()));
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

FollyExecutorPool::FollyExecutorPool(
        size_t maxThreads,
        ThreadPoolConfig::ThreadCount maxReaders_,
        ThreadPoolConfig::ThreadCount maxWriters_,
        ThreadPoolConfig::AuxIoThreadCount maxAuxIO_,
        ThreadPoolConfig::NonIoThreadCount maxNonIO_,
        ThreadPoolConfig::IOThreadsPerCore ioThreadsPerCore)
    : ExecutorPool(maxThreads, ioThreadsPerCore),
      state(std::make_unique<State>()),
      maxReaders(calcNumReaders(maxReaders_)),
      maxWriters(calcNumWriters(maxWriters_)),
      maxAuxIO(calcNumAuxIO(maxAuxIO_)),
      maxNonIO(calcNumNonIO(maxNonIO_)) {
    LOG_TRACE(
            "FollyExecutorPool ctor: Creating with maxThreads:{} maxReaders:{} "
            "maxWriters:{} maxAuxIO:{} maxNonIO:{} ioThreadsPerCore:{}",
            maxThreads,
            maxReaders,
            maxWriters,
            maxAuxIO,
            maxNonIO,
            ioThreadsPerCore);

    // Disable dynamic thread creation / destruction to match CB3ExecutorPool
    // behaviour. At least AtomicQueue cannot be used when this is set to true.
    FLAGS_dynamic_cputhreadpoolexecutor = false;

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
    auto makeThreadFactory = [](std::string prefix, TaskType taskType) {
        return std::make_shared<CBRegisteredThreadFactory>(
#if defined(__linux__)
                std::make_shared<CBPriorityThreadFactory>(
                        prefix, ExecutorPool::getThreadPriority(taskType))
#else
                std::make_shared<folly::NamedThreadFactory>(prefix)
#endif
        );
    };

    futurePool = std::make_unique<folly::IOThreadPoolExecutor>(
            1, std::make_shared<folly::NamedThreadFactory>("SchedulerPool"));

    readerPool = std::make_unique<CancellableCPUExecutor>(
            maxReaders, makeThreadFactory("ReaderPool", TaskType::Reader));
    writerPool = std::make_unique<CancellableCPUExecutor>(
            maxWriters, makeThreadFactory("WriterPool", TaskType::Writer));
    auxPool = std::make_unique<CancellableCPUExecutor>(
            maxAuxIO, makeThreadFactory("AuxIoPool", TaskType::AuxIO));
    nonIoPool = std::make_unique<CancellableCPUExecutor>(
            maxNonIO, makeThreadFactory("NonIoPool", TaskType::NonIO));
}

FollyExecutorPool::~FollyExecutorPool() {
    // We need a custom dtor because Tasks running on IO thread read the value
    // of futurePool; therefore we cannot rely on the default dtor to simply
    // call reset() and set ptr to null before all IO threads have stopped.
    // Instead we must explicitly stop all IO threads, then reset()
    // the futurePool.
    nonIoPool->join();
    nonIoPool.reset();
    auxPool->join();
    auxPool.reset();
    writerPool->join();
    writerPool.reset();
    readerPool->join();
    readerPool.reset();

    auto* eventBase = futurePool->getEventBase();
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [eventBase] { eventBase->timer().cancelAll(); });
    futurePool.reset();
}

size_t FollyExecutorPool::getNumWorkersStat() const {
    return readerPool->numThreads() + writerPool->numThreads() +
           auxPool->numThreads() + nonIoPool->numThreads();
}

size_t FollyExecutorPool::getNumReaders() const {
    return calcNumReaders(ThreadPoolConfig::ThreadCount(maxReaders));
}

size_t FollyExecutorPool::getNumWriters() const {
    return calcNumWriters(ThreadPoolConfig::ThreadCount(maxWriters));
}

size_t FollyExecutorPool::getNumAuxIO() const {
    return auxPool->getPoolStats().threadCount;
}

size_t FollyExecutorPool::getNumNonIO() const {
    return nonIoPool->getPoolStats().threadCount;
}

void FollyExecutorPool::setNumReaders(ThreadPoolConfig::ThreadCount v) {
    maxReaders = calcNumReaders(v);
    readerPool->setNumThreads(maxReaders);
}

void FollyExecutorPool::setNumReadersExactly(uint16_t v) {
    maxReaders = v;
    readerPool->setNumThreads(maxReaders);
}

size_t FollyExecutorPool::getNumReadersExactly() const {
    return readerPool->numThreads();
}

void FollyExecutorPool::setNumWriters(ThreadPoolConfig::ThreadCount v) {
    maxWriters = calcNumWriters(v);
    writerPool->setNumThreads(maxWriters);
}

void FollyExecutorPool::setNumAuxIO(ThreadPoolConfig::AuxIoThreadCount v) {
    maxAuxIO = calcNumAuxIO(v);
    auxPool->setNumThreads(maxAuxIO);
}

void FollyExecutorPool::setNumNonIO(ThreadPoolConfig::NonIoThreadCount v) {
    maxNonIO = calcNumNonIO(v);
    nonIoPool->setNumThreads(maxNonIO);
}

size_t FollyExecutorPool::getNumSleepers() const {
    return readerPool->getPoolStats().idleThreadCount +
           writerPool->getPoolStats().idleThreadCount +
           auxPool->getPoolStats().idleThreadCount +
           nonIoPool->getPoolStats().idleThreadCount;
}

size_t FollyExecutorPool::getNumReadyTasks() const {
    return readerPool->getPendingTaskCount() +
           writerPool->getPendingTaskCount() + auxPool->getPendingTaskCount() +
           nonIoPool->getPendingTaskCount();
}

void FollyExecutorPool::registerTaskable(Taskable& taskable) {
    cb::NoArenaGuard guard;

    if (taskable.getWorkLoadPolicy().getBucketPriority() <
        HIGH_BUCKET_PRIORITY) {
        taskable.setWorkloadPriority(LOW_BUCKET_PRIORITY);
        LOG_INFO("Taskable {} registered with low priority",
                 taskable.getName());
    } else {
        taskable.setWorkloadPriority(HIGH_BUCKET_PRIORITY);
        LOG_INFO("Taskable {} registered with high priority",
                 taskable.getName());
    }

    futurePool->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
            [state = this->state.get(), &taskable]() {
                state->addTaskable(taskable);
            });
}

void FollyExecutorPool::unregisterTaskable(Taskable& taskable, bool force) {
    cb::NoArenaGuard guard;

    LOG_TRACE("FollyExecutorPool::unregisterTaskable() taskable:'{}' force:{}",
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
    // on the CPU pool and examining the result, new CPU work could have been
    // executed. Therefore we handle these by:
    // - Marking all tasks as dead (during 1)
    // - polling the taskOwners structure (on the futurePool thread) for all
    //   tasks to be cancelled (and hence removed) - which happens once a
    //   task finishes running in the CPU pool.
    // Once taskOwners is empty we are done.

    // Step 1 - Have the eventbase of the futurePool cancel all tasks
    // associated with this Taskable.
    // Performing on futurePool guarantees that none of the Taskable's tasks
    // can currently be running on the futurePool (and hence being added to
    // CPU pool.
    auto* eventBase = futurePool->getEventBase();
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [state = this->state.get(), &taskable, force] {
                state->cancelTasksOwnedBy(taskable, force);
            });

    // Step 2 - Have the eventbase of the futurePool remove tasks associated
    // with this Taskable from the queues of the ExecutorPools (things due to be
    // run immediately) to speed up shutdown
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait([this, &taskable]() {
        LOG_DEBUG("Removing tasks from read pool for taskable {}",
                  taskable.getName());
        for (auto task : readerPool->removeTasksForTaskable(taskable)) {
            LOG_DEBUG("Cancelling task from runningQ with ID:{}",
                      task->getId());
            state->cancelTask(task->getId(), true);
        }

        LOG_DEBUG("Removing tasks from writer pool for taskable {}",
                  taskable.getName());
        for (auto task : writerPool->removeTasksForTaskable(taskable)) {
            LOG_DEBUG("Cancelling task from runningQ with ID:{}",
                      task->getId());
            state->cancelTask(task->getId(), true);
        }

        LOG_DEBUG("Removing tasks from aux pool for taskable {}",
                  taskable.getName());
        for (auto task : auxPool->removeTasksForTaskable(taskable)) {
            LOG_DEBUG("Cancelling task from runningQ with ID:{}",
                      task->getId());
            state->cancelTask(task->getId(), true);
        }

        LOG_DEBUG("Removing tasks from nonIo pool for taskable {}",
                  taskable.getName());
        for (auto task : nonIoPool->removeTasksForTaskable(taskable)) {
            LOG_DEBUG("Cancelling task from runningQ with ID:{}",
                      task->getId());
            state->cancelTask(task->getId(), true);
        }
    });

    unregisterTaskablePostCancelHook();

    // Step 3 - poll for taskOwners to become empty. This will only
    // occur once all outstanding, running tasks have been cancelled and
    // cleaned up.
    auto isTaskOwnersEmpty = [eventBase, &state = this->state, &taskable] {
        bool empty = false;
        eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
                [&state, &taskable, &empty] {
                    empty = state->numTasksForOwner(taskable) == 0;
                });
        return empty;
    };

    while (!isTaskOwnersEmpty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }

    // Finally, remove entry for the unregistered Taskable.
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [&state = this->state, &taskable]() mutable {
                state->removeTaskable(taskable);
            });
}

size_t FollyExecutorPool::getNumTaskables() const {
    cb::NoArenaGuard guard;
    int numTaskables = 0;
    futurePool->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
            [state = this->state.get(), &numTaskables] {
                numTaskables = state->numTaskables();
            });
    return numTaskables;
}

size_t FollyExecutorPool::schedule(ExTask task) {
    cb::NoArenaGuard guard;

    using namespace std::chrono;
    LOG_TRACE("FollyExecutorPool::schedule() id:{} name:{} type:{} wakeTime:{}",
              task->getId(),
              GlobalTask::getTaskName(task->getTaskId()),
              GlobalTask::getTaskType(task->getTaskId()),
              task->getWaketime().time_since_epoch().count());

    auto* eventBase = futurePool->getEventBase();
    auto* pool = getPoolForTaskType(GlobalTask::getTaskType(task->getTaskId()));
    Expects(pool);
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [eventBase, this, pool, &task] {
                state->scheduleTask(*this, *pool, task);
            });

    return task->getId();
}

bool FollyExecutorPool::cancel(size_t taskId, bool eraseTask) {
    cb::NoArenaGuard guard;

    LOG_TRACE("FollyExecutorPool::cancel() id:{} eraseTask:{}",
              taskId,
              eraseTask);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [&found, state = state.get(), taskId] {
                found = state->cancelTask(taskId);
            });
    return found;
}

void FollyExecutorPool::wake(size_t taskId) {
    cb::NoArenaGuard guard;

    LOG_TRACE("FollyExecutorPool::wake() id:{}", taskId);

    auto* eventBase = futurePool->getEventBase();
    eventBase->runInEventBaseThread(
            [state = state.get(), taskId] { state->wakeTask(taskId); });
}

bool FollyExecutorPool::wakeAndWait(size_t taskId) {
    cb::NoArenaGuard guard;

    LOG_TRACE("FollyExecutorPool::wakeAndWait() id:{}", taskId);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [&found, state = state.get(), taskId] {
                found = state->wakeTask(taskId);
            });
    return found;
}

void FollyExecutorPool::snooze(size_t taskId, double toSleep) {
    cb::NoArenaGuard guard;
    using namespace std::chrono;
    LOG_TRACE("FollyExecutorPool::snooze() id:{} toSleep:{}", taskId, toSleep);

    futurePool->getEventBase()->runInEventBaseThread(
            [state = state.get(), taskId, toSleep] {
                state->snoozeTask(taskId, toSleep);
            });
}

bool FollyExecutorPool::snoozeAndWait(size_t taskId, double toSleep) {
    cb::NoArenaGuard guard;
    using namespace std::chrono;
    LOG_TRACE("FollyExecutorPool::snoozeAndWait() id:{} toSleep:{}",
              taskId,
              toSleep);

    auto* eventBase = futurePool->getEventBase();
    bool found = false;
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [&found, state = state.get(), taskId, toSleep] {
                found = state->snoozeTask(taskId, toSleep);
            });
    return found;
}

void FollyExecutorPool::doWorkerStat(Taskable& taskable,
                                     CookieIface& cookie,
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
                                    CookieIface& cookie,
                                    const AddStatFn& add_stat) {
    cb::NoArenaGuard guard;

    // Take a copy of the taskOwners map.
    auto* eventBase = futurePool->getEventBase();
    std::vector<ExTask> tasks;
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
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

    add_casted_stat(fmt::format("ep_tasks:tasks:{}", taskable.getName()),
                    list.dump(),
                    add_stat,
                    cookie);
    add_casted_stat(fmt::format("ep_tasks:cur_time:{}", taskable.getName()),
                    to_ns_since_epoch(std::chrono::steady_clock::now()).count(),
                    add_stat,
                    cookie);

    // It is possible that elements of `tasks` are now the last reference to
    // a GlobalTask, if the GlobalTask was cancelled while this function was
    // running. As such, we need to ensure that if the task is deleted, its
    // memory is accounted to the correct bucket.
    for (auto& task : tasks) {
        task->getTaskable().invokeViaTaskable([&task]() { task.reset(); });
    }
}

void FollyExecutorPool::doTaskQStat(Taskable& taskable,
                                    CookieIface& cookie,
                                    const AddStatFn& add_stat) {
    cb::NoArenaGuard guard;

    // Count how many tasks of each type are waiting to run - defined by
    // having an outstanding timeout.
    // Note: This mimics the behaviour of CB3ExecutorPool, which counts _all_
    // tasks across all taskables. This may or may not be the correct
    // behaviour...
    // The counting is done on the eventbase thread given it would be
    // racy to directly access the taskOwners from this thread.
    auto* eventBase = futurePool->getEventBase();
    std::array<int, static_cast<size_t>(TaskType::Count)> waitingTasksPerGroup;
    eventBase->runImmediatelyOrRunInEventBaseThreadAndWait(
            [state = this->state.get(), &waitingTasksPerGroup] {
                waitingTasksPerGroup = state->getWaitingTasksPerGroup();
            });

    // Currently FollyExecutorPool implements a single task queue (per task
    // type) - report that as low priority.
    fmt::memory_buffer buf;
    add_casted_stat("ep_workload:LowPrioQ_Writer:InQsize",
                    waitingTasksPerGroup[static_cast<size_t>(TaskType::Writer)],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_Reader:InQsize",
                    waitingTasksPerGroup[static_cast<size_t>(TaskType::Reader)],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_AuxIO:InQsize",
                    waitingTasksPerGroup[static_cast<size_t>(TaskType::AuxIO)],
                    add_stat,
                    cookie);
    add_casted_stat("ep_workload:LowPrioQ_NonIO:InQsize",
                    waitingTasksPerGroup[static_cast<size_t>(TaskType::NonIO)],
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

CancellableCPUExecutor* FollyExecutorPool::getPoolForTaskType(TaskType type) {
    switch (type) {
    case TaskType::None:
        folly::assume_unreachable();
    case TaskType::Writer:
        return writerPool.get();
    case TaskType::Reader:
        return readerPool.get();
    case TaskType::AuxIO:
        return auxPool.get();
    case TaskType::NonIO:
        return nonIoPool.get();
    case TaskType::Count:
        folly::assume_unreachable();
    }
    folly::assume_unreachable();
}

void FollyExecutorPool::rescheduleTaskAfterRun(TaskProxy& proxy) {
    // Should only be called from within EventBase thread.
    auto* eventBase = futurePool->getEventBase();
    Expects(eventBase->inRunningEventBaseThread());

    LOG_TRACE(
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
        // taskOwners, decrement the refcount and potentially delete the
        // GlobalTask.
        state->cancelTask(proxy.task->getId());
        return;
    }

    // Don't attempt to schedule when sleeping forever
    if (proxy.task->isSleepingForever()) {
        return;
    }

    using namespace std::chrono;
    auto now = steady_clock::now();

    // if a task is not dead and has not set snooze, update its waketime to now
    // before rescheduling for more accurate timing histograms and to avoid
    // erroneous slow scheduling logs.
    proxy.task->updateWaketimeIfLessThan(now);

    // Task still alive, so should be run again. In the future or immediately?
    const auto timeout =
            duration_cast<milliseconds>(proxy.task->getWaketime() - now);
    if (timeout > milliseconds::zero()) {
        eventBase->timer().scheduleTimeout(&proxy, timeout);
    } else {
        // Due now - schedule directly on CPU pool.
        proxy.scheduleViaCPUPool();
    }
}
