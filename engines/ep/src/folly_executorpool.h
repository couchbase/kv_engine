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
#pragma once

#include "executorpool.h"
#include "task_type.h"

#include <memory>

namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
}

/**
 * ExecutorPool implemented using folly Executors.
 *
 * Initial implementation matches the CB3 Executor pool, which is not
 * idiomatic to Folly but gives a starting point to evolve / simplify later:
 *
 * 1. 4 pools of folly::CPUThreadPoolExecutors are used for each of the
 *    Reader/Writer/AuxIO/NonIO thread pools.
 *    This can likely be simplified - for example combining the IO pools into
 *    a single pool, then using priorities to ensure correct priorization.
 *
 * 2. A folly::IOThreadPoolExecutor is used to track all GlobalTasks scheduled
 *    for future execution.
 *    This acts as a combined futureQ for all task types. This is split
 *    in 4 queues in CB3Executor, which all threads drain from, however I
 *    anticipate this won't be a problem for FollyExecutorPool given all
 *    actual task execution is flipped over to the CPUThreadPoolExecutors.
 *    When a FollyTask becomes ready, execution is transferred to the relevant
 *    CPUThreadPool to actually run the task.
 *
 * 3. Given CB3ExecutorPool owns its tasks (via shared_ptr<GlobalTask), we
 *    need to do the same here.
 *
 * 4. The primary methods to ExecutorPool are:
 *
 *    a) scheduleule(ExTask) - Register a task, to be run at it's wakeTime.
 *    b) cancel(ExTask) - Cancel a task so it should no longer run.
 *    c) wake(ExTask) - Change a task's current schedule, to be executed asap.
 *    d) snooze(ExTask) - Change a task's current to be executed at the
 *                        specified time.
 *
 *    The main challenge with these methods is there is potential for raciness
 *    with cancel, wake and snooze; given the task may already be running on
 *    a background thread when they are called.
 *    We must ensure that wake-ups are not lost, nor should tasks be run
 *    more once concurrently (say on different threads of the same pool).
 *
 *    Currently these issues are avoided by ensuring that _all_ scheduling
 *    changes (wakeup / snooze / cancel) are performed on the single IO thread
 *    via it's eventBase. This enforces a sequential order on any scheduling
 *    change - it is not possible to also be attempting to change the schedule
 *    on a different thread.
 *    This differs to the approach taken by CB3ExecutorPool, where cancel /
 *    wake / snooze execute on the thread calling them, with judicious use of
 *    mutexes to ensure correctness.
 *
 *    In practical terms, this means that to perform a wake() -> run() ->
 *    snooze() sequence with FollyExecutorPool the following work occurs:
 *
 *      [B] : Blocking call
 *
 *      Calling Thread          IO Thread            CPU Thread Pool
 *
 *      1. [B] wake()
 *         run on IO thread --> 2. Update timeout
 *                                 CPUPool::enqueue()
 *                                 <--
 *         <--
 *         wake() done.
 *
 *      ... When next CPU thread available ...
 *                                                   3. CPUPool::dequeue()
 *                                                   [B] GlobalTask::execute()
 *                                                   ... perform work ...
 *                                                   Reschedule task
 *                              4. [B] snooze()  <--
 *                                 Update timeout
 *                                               -->    ... done..
 *
 *    This results in two context switches for wake() - switch to IO thread the
 *    back to calling thread, then two context switches for snooze() - to IO
 *    thread for snooze then back to CPU pool thread.
 *
 *    CB3ExecutorPool however performs the wake() and snooze() on the calling
 *    thread (but with added mutexes), so in theory it has _zero_ context-
 *    switches. However that is only the case if all mutexes are uncontended -
 *    if there is contention then threads will have to yield and wait to acquire
 *    the mutex - which could result in >4 context switches...
 *
 *    Compared to the CB3ExecutorPool, this approach has:
 *
 *    Pros
 *    + Simpler to reason about / get correct.
 *    + Reduces amount of locking needed, so should allow actual execution
 *      to scale better with more threads (CPU threads shouldn't have to
 *      block much).
 *
 *    Cons:
 *    - More context switches in the best case.
 *    - Single-threaded, potentially limiting scheduling throughput on highly
 *      threaded / high load environments.
 *
 *    Note that Folly claims that an IO thread handle milions of events per
 *    second [1], so for an initial implementation this  seems a reasonable
 *    design - if the single IO thread / context switches are a bottleneck we
 *    can revisit down the line.
 *
 * [1] https://github.com/facebook/folly/blob/master/folly/io/async/README.md
 */
class FollyExecutorPool : public ExecutorPool {
public:
    /// Forward-declare the internal proxy object used to wrap GlobalTask.
    struct TaskProxy;

    /**
     * @param maxThreads Maximum number of threads in any given thread class
     *                   (Reader, Writer, NonIO, AuxIO). A value of 0 means
     *                   use number of CPU cores.
     * @param maxReaders Number of Reader threads to create.
     * @param maxWriters Number of Writer threads to create.
     * @param maxAuxIO Number of AuxIO threads to create (0 = auto-configure).
     * @param maxNonIO Number of NonIO threads to create (0 = auto-configure).
     */
    FollyExecutorPool(size_t maxThreads,
                      ThreadPoolConfig::ThreadCount maxReaders,
                      ThreadPoolConfig::ThreadCount maxWriters,
                      size_t maxAuxIO,
                      size_t maxNonIO);

    ~FollyExecutorPool() override;

    size_t getNumWorkersStat() override;
    size_t getNumReaders() override;
    size_t getNumWriters() override;
    size_t getNumAuxIO() override;
    size_t getNumNonIO() override;
    void setNumReaders(ThreadPoolConfig::ThreadCount v) override;
    void setNumWriters(ThreadPoolConfig::ThreadCount v) override;
    void setNumAuxIO(uint16_t v) override;
    void setNumNonIO(uint16_t v) override;
    size_t getNumSleepers() override;
    size_t getNumReadyTasks() override;

    void registerTaskable(Taskable& taskable) override;
    std::vector<ExTask> unregisterTaskable(Taskable& taskable,
                                           bool force) override;
    size_t getNumTaskables() const override;

    size_t schedule(ExTask task) override;
    bool cancel(size_t taskId, bool eraseTask) override;
    void wake(size_t taskId) override;
    bool wakeAndWait(size_t taskId) override;
    void snooze(size_t taskId, double tosleep) override;
    bool snoozeAndWait(size_t taskId, double tosleep) override;
    void doWorkerStat(Taskable& taskable,
                      const void* cookie,
                      const AddStatFn& add_stat) override;
    void doTasksStat(Taskable& taskable,
                     const void* cookie,
                     const AddStatFn& add_stat) override;
    void doTaskQStat(Taskable& taskable,
                     const void* cookie,
                     const AddStatFn& add_stat) override;

private:
    /// @returns the CPU pool to use for the given task type.
    folly::CPUThreadPoolExecutor* getPoolForTaskType(task_type_t type);

    /// Reschedule the given task based on it's current sleepTime and if
    /// the task is dead (or should run again).
    void rescheduleTaskAfterRun(TaskProxy& proxy);

    /// Remove the given taskProxy from the tracked tasks.
    /// Should only be called at the end of scheduleViaCPUPool.
    void removeTaskAfterRun(TaskProxy& proxy);

    struct State;
    /**
     * FollyExecutorPool internal state. unique_ptr for pimpl.
     * Note: this exists before the thread pools as we must destruct the
     * thread pools before the State (given thread pools can be accessing it
     * on other threads).
     */
    std::unique_ptr<State> state;

    /// Underlying Folly thread pools.
    std::unique_ptr<folly::IOThreadPoolExecutor> futurePool;
    std::unique_ptr<folly::CPUThreadPoolExecutor> readerPool;
    std::unique_ptr<folly::CPUThreadPoolExecutor> writerPool;
    std::unique_ptr<folly::CPUThreadPoolExecutor> auxPool;
    std::unique_ptr<folly::CPUThreadPoolExecutor> nonIoPool;

    size_t maxReaders;
    size_t maxWriters;
    size_t maxAuxIO;
    size_t maxNonIO;

    /// Grant friendship to TaskProxy as it needs to be able to re-schedule
    /// itself using the futurePool.
    friend TaskProxy;
};
