/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_common.h>
#include <memcached/thread_pool_config.h>
#include <utilities/testing_hook.h>

#include "task_type.h"
#include <atomic>
#include <memory>
#include <mutex>

class CookieIface;
class GlobalTask;
class Taskable;
using ExTask = std::shared_ptr<GlobalTask>;

class ExecutorPool {
public:
    enum class Backend {
        /// The Folly backend is the one currently in use by Couchbase server
        Folly,
        /// The CB3 is the backend previously used by Couchbase Server
        CB3,
        /// The Fake backend is a single threaded one which ignores
        /// all thread settings provided in the constructor. It is
        /// only used by unit tests
        Fake,
        /// The Mock backend is used by unit testing
        Mock,
        Default = Folly
    };

    static void create(Backend backend = Backend::Default,
                       size_t maxThreads = 0,
                       ThreadPoolConfig::ThreadCount maxReaders =
                               ThreadPoolConfig::ThreadCount::Default,
                       ThreadPoolConfig::ThreadCount maxWriters =
                               ThreadPoolConfig::ThreadCount::Default,
                       size_t maxAuxIO = 0,
                       size_t maxNonIO = 0);

    /// Is the ExecutorPool created or not
    static bool exists();

    /**
     * @returns the singleton instance of ExecutorPool
     * @throws std::logic_error if the instance isn't created yet
     */
    static ExecutorPool* get();

    /**
     * Destroys the singleton instance of ExecutorPool, joining and terminating
     * all pool threads.
     */
    static void shutdown();

    /********************* Thread Management *********************************/

    /**
     * Returns the total number of worker threads which currently exist
     * across all thread types.
     */
    virtual size_t getNumWorkersStat() = 0;

    /// @returns the number of Reader IO threads.
    virtual size_t getNumReaders() = 0;

    /// @returns the number of Writer IO threads.
    virtual size_t getNumWriters() = 0;

    /// @returns the number of Auxillary IO threads.
    virtual size_t getNumAuxIO() = 0;

    /// @returns the number of Non-IO threads.
    virtual size_t getNumNonIO() = 0;

    /// Set the number of Reader IO threads to the specified number.
    virtual void setNumReaders(ThreadPoolConfig::ThreadCount v) = 0;

    /// Set the number of Writer IO threads to the specified number.
    virtual void setNumWriters(ThreadPoolConfig::ThreadCount v) = 0;

    /// Set the number of Auxillary IO threads to the specified number.
    virtual void setNumAuxIO(uint16_t v) = 0;

    /// Set the number of Non-IO threads to the specified number.
    virtual void setNumNonIO(uint16_t v) = 0;

    /// @returns the number of threads currently sleeping.
    virtual size_t getNumSleepers() = 0;

    /// @returns the number of Tasks ready to run.
    virtual size_t getNumReadyTasks() = 0;

    /***************** Task Ownership ***************************************/

    /**
     * Registers a "Taskable" - a task owner with the executorPool.
     */
    virtual void registerTaskable(Taskable& taskable) = 0;

    /**
     * Remove the client via the Taskable interface.
     * Calling this method will find and cancel all tasks of the client.
     *
     * @param taskable caller's taskable interface (getGID used to find tasks)
     * @param force should the shutdown be forced (may not wait for tasks)
     */
    virtual void unregisterTaskable(Taskable& taskable, bool force) = 0;

    /// @returns the number of registered Taskables.
    virtual size_t getNumTaskables() const = 0;

    /***************** Task Scheduling **************************************/

    /**
     * Allows task to be scheduled for future execution by a thread of the
     * associated task->getTaskType. The task's 'wakeTime' determines
     * approximately when the task will be executed (no guarantees).
     * @returns The unique taskId for the task object.
     */
    virtual size_t schedule(ExTask task) = 0;

    /**
     * cancel the task with taskId and optionally remove from taskLocator.
     * Removing from the taskLocator will eventually trigger deletion of the
     * task as references to the shared_ptr are dropped.
     *
     * @param taskId Task to cancel
     * @param remove true if the task should be removed from taskLocator
     * @return true if the task was found and cancelled
     */
    virtual bool cancel(size_t taskId, bool remove = false) = 0;

    /**
     * The wake method allows for a caller to request that the task matching
     * taskId be executed by its thread-type now'. The tasks wakeTime is
     * modified so that it has a wakeTime of now and a thread of the correct
     * type is signaled to wake-up and perform fetching. The woken task will
     * have to wait for any current tasks to be executed first, but it will
     * jump ahead of other tasks as tasks that are ready to run are ordered
     * by their priority.
     */
    virtual void wake(size_t taskId) {
        wakeAndWait(taskId);
    }

    /**
     * Same as wake(), except it returns true if the given task was registered
     * and successfully requested to wake.
     * This method may be slower than wake() for some ExecutorPool
     * implementations, if additional work is required to "synchronously" wake
     * the task.
     * Note: even if this method returns true, it doesn't guarantee the task
     * will run - the task could still be cancelled before it has chance to
     * execute.
     */
    virtual bool wakeAndWait(size_t taskId) = 0;

    /**
     * The snooze method will locate the task matching taskId and adjust its
     * wakeTime to account for the toSleep value.
     */
    virtual void snooze(size_t taskId, double tosleep) {
        snoozeAndWait(taskId, tosleep);
    }

    /**
     * Same as snooze(), except it returns true if the given task was
     * registered and successfully requested to snooze.
     * This method may be slower than snooze() for some ExecutorPool
     * implementations, if additional work is required to "synchronously"
     * snooze the task.
     * Note: even if this method returns true, it doesn't guarantee the task
     * will sleep for specified time - the task could still be cancelled
     * (or woken by someone else) before it has chance to sleep that long.
     */
    virtual bool snoozeAndWait(size_t taskId, double tosleep) = 0;

    /*************** Statistics *********************************************/

    /**
     * @returns statistics about worker threads.
     */
    virtual void doWorkerStat(Taskable& taskable,
                              const CookieIface& cookie,
                              const AddStatFn& add_stat) = 0;

    /**
     * Generates stats regarding currently running tasks, as displayed by
     * cbstats tasks.
     */
    virtual void doTasksStat(Taskable& taskable,
                             const CookieIface& cookie,
                             const AddStatFn& add_stat) = 0;

    /**
     * Generates stats regarding queued tasks.
     */
    virtual void doTaskQStat(Taskable& taskable,
                             const CookieIface& cookie,
                             const AddStatFn& add_stat) = 0;

    virtual ~ExecutorPool() = default;

    /**
     * Return the thread priority to use for threads of the given task type.
     * @param taskType
     * @return priority to use, as passed to setpriority().
     */
    static int getThreadPriority(task_type_t taskType);

    /************** Testing *************************************************/

    // Testing hook for MB-48925 - called inside unregisterTaskable after
    // tasks have been cancelled.
    TestingHook<> unregisterTaskablePostCancelHook;

protected:
    ExecutorPool(size_t maxThreads);

    /**
     * Calculate the number of Reader threads to use for the given thread limit.
     */
    size_t calcNumReaders(ThreadPoolConfig::ThreadCount threadCount) const;

    /**
     * Calculate the number of Writer threads to use for the given thread limit.
     */
    size_t calcNumWriters(ThreadPoolConfig::ThreadCount threadCount) const;

    /**
     * Calculate the number of Auxiliary IO threads to use for the given thread
     * limit.
     */
    size_t calcNumAuxIO(size_t threadCount) const;

    /**
     * Calculate the number of Non-IO threads to use for the given thread limit.
     */
    size_t calcNumNonIO(size_t threadCount) const;

    // Return a reference to the singleton ExecutorPool.
    static std::unique_ptr<ExecutorPool>& getInstance();

    /**
     * Maximum number of threads of any given class (Reader, Writer, AuxIO,
     * NonIO).
     * If not overridden by maxThreads ctor arg, set to the number
     * of available CPU cores.
     */
    const size_t maxGlobalThreads;
};
