/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/*
 * Unit tests for the ExecutorPool class
 */

#include "executorpool_test.h"
#include "lambda_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

ExTask makeTask(Taskable& taskable, ThreadGate& tg, TaskId taskId) {
    return std::make_shared<LambdaTask>(
            taskable, taskId, 0, true, [&]() -> bool {
                tg.threadUp();
                return false;
            });
}

::std::ostream& operator<<(::std::ostream& os,
                           const ThreadCountsParams& expected) {
    return os << expected.in_reader_writer << "_CPU" << expected.maxThreads
              << "_W" << expected.writer << "_R" << expected.reader << "_A"
              << expected.auxIO << "_N" << expected.nonIO;
}

TEST_F(ExecutorPoolTest, register_taskable_test) {
    TestExecutorPool pool(10, // MaxThreads
                          ThreadPoolConfig::ThreadCount(2), // MaxNumReaders
                          ThreadPoolConfig::ThreadCount(2), // MaxNumWriters
                          2, // MaxNumAuxio
                          2 // MaxNumNonio
    );

    MockTaskable taskable;
    MockTaskable taskable2;

    ASSERT_EQ(0, pool.getNumWorkersStat());
    ASSERT_EQ(0, pool.getNumBuckets());

    pool.registerTaskable(taskable);

    ASSERT_EQ(8, pool.getNumWorkersStat());
    ASSERT_EQ(1, pool.getNumBuckets());

    pool.registerTaskable(taskable2);

    ASSERT_EQ(8, pool.getNumWorkersStat());
    ASSERT_EQ(2, pool.getNumBuckets());

    pool.unregisterTaskable(taskable2, false);

    ASSERT_EQ(8, pool.getNumWorkersStat());
    ASSERT_EQ(1, pool.getNumBuckets());

    pool.unregisterTaskable(taskable, false);

    ASSERT_EQ(0, pool.getNumWorkersStat());
    ASSERT_EQ(0, pool.getNumBuckets());
}

/* This test creates an ExecutorPool, and attempts to verify that calls to
 * setNumWriters are able to dynamically create more workers than were present
 * at initialisation. A ThreadGate is used to confirm that two tasks
 * of type WRITER_TASK_IDX can run concurrently
 *
 */
TEST_F(ExecutorPoolTest, increase_workers) {
    const size_t numReaders = 1;
    const size_t numWriters = 1;
    const size_t numAuxIO = 1;
    const size_t numNonIO = 1;

    const size_t originalWorkers =
            numReaders + numWriters + numAuxIO + numNonIO;

    // This will allow us to check that numWriters + 1 writer tasks can run
    // concurrently after setNumWriters has been called.
    ThreadGate tg{numWriters + 1};

    TestExecutorPool pool(5, // MaxThreads
                          ThreadPoolConfig::ThreadCount(numReaders),
                          ThreadPoolConfig::ThreadCount(numWriters),
                          numAuxIO,
                          numNonIO);

    MockTaskable taskable;
    pool.registerTaskable(taskable);

    std::vector<ExTask> tasks;

    for (size_t i = 0; i < numWriters + 1; ++i) {
        // Use any Writer thread task (StatSnap) for the TaskId.
        ExTask task = makeTask(taskable, tg, TaskId::StatSnap);
        pool.schedule(task);
        tasks.push_back(task);
    }

    EXPECT_EQ(numWriters, pool.getNumWriters());
    ASSERT_EQ(originalWorkers, pool.getNumWorkersStat());

    pool.setNumWriters(ThreadPoolConfig::ThreadCount(numWriters + 1));

    EXPECT_EQ(numWriters + 1, pool.getNumWriters());
    ASSERT_EQ(originalWorkers + 1, pool.getNumWorkersStat());

    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to run";

    pool.unregisterTaskable(taskable, false);
}

// Verifies the priority of the different thread types. On Windows and Linux
// the Writer threads should be low priority.
TEST_F(ExecutorPoolTest, ThreadPriorities) {
    // Create test pool and register a (mock) taskable to start all threads.
    TestExecutorPool pool(10, // MaxThreads
                          ThreadPoolConfig::ThreadCount(2), // MaxNumReaders
                          ThreadPoolConfig::ThreadCount(2), // MaxNumWriters
                          2, // MaxNumAuxio
                          2 // MaxNumNonio
    );

    const size_t totalNumThreads = 8;

    // Given we have to wait for all threads to be running (called
    // ::run()) before their priority will be set, use a ThreadGate
    // with a simple Task which calls threadUp() to ensure all threads
    // have started before checking priorities.
    MockTaskable taskable;
    pool.registerTaskable(taskable);
    std::vector<ExTask> tasks;
    ThreadGate tg{totalNumThreads};

    // Need 2 tasks of each type, so both threads of each type are
    // started.
    // Reader
    tasks.push_back(makeTask(taskable, tg, TaskId::MultiBGFetcherTask));
    tasks.push_back(makeTask(taskable, tg, TaskId::MultiBGFetcherTask));
    // Writer
    tasks.push_back(makeTask(taskable, tg, TaskId::FlusherTask));
    tasks.push_back(makeTask(taskable, tg, TaskId::FlusherTask));
    // AuxIO
    tasks.push_back(makeTask(taskable, tg, TaskId::AccessScanner));
    tasks.push_back(makeTask(taskable, tg, TaskId::AccessScanner));
    // NonIO
    tasks.push_back(makeTask(taskable, tg, TaskId::ItemPager));
    tasks.push_back(makeTask(taskable, tg, TaskId::ItemPager));

    for (auto& task : tasks) {
        pool.schedule(task);
    }
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to start";

    // Windows (via folly portability) uses 20 for default (normal) priority.
    const int defaultPriority = folly::kIsWindows ? 20 : 0;

    // We only set Writer threads to a non-default level on Linux.
    const int expectedWriterPriority = folly::kIsLinux ? 19 : defaultPriority;

    auto threads = pool.getThreads();
    ASSERT_EQ(totalNumThreads, threads.size());
    for (const auto* thread : threads) {
        switch (thread->getTaskType()) {
        case WRITER_TASK_IDX:
            EXPECT_EQ(expectedWriterPriority, thread->getPriority())
                    << "for thread: " << thread->getName();
            break;
        case READER_TASK_IDX:
        case AUXIO_TASK_IDX:
        case NONIO_TASK_IDX:
            EXPECT_EQ(defaultPriority, thread->getPriority())
                    << "for thread: " << thread->getName();
            break;
        default:
            FAIL() << "Unexpected task type: " << thread->getTaskType();
        }
    }

    pool.unregisterTaskable(taskable, false);
}

TEST_F(ExecutorPoolDynamicWorkerTest, decrease_workers) {
    ASSERT_EQ(2, pool->getNumWriters());
    pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));
    EXPECT_EQ(1, pool->getNumWriters());
}

TEST_F(ExecutorPoolDynamicWorkerTest, setDefault) {
    ASSERT_EQ(2, pool->getNumWriters());
    ASSERT_EQ(2, pool->getNumReaders());

    pool->setNumWriters(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(4, pool->getNumWriters())
            << "num_writers should be 4 with ThreadCount::Default";

    pool->setNumReaders(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(16, pool->getNumReaders())
            << "num_writers should be capped at 16 with ThreadCount::Default";
}

TEST_F(ExecutorPoolDynamicWorkerTest, setDiskIOOptimized) {
    ASSERT_EQ(2, pool->getNumWriters());

    pool->setNumWriters(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(MaxThreads, pool->getNumWriters());

    pool->setNumReaders(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(MaxThreads, pool->getNumReaders());
}

TEST_P(ExecutorPoolTestWithParam, max_threads_test_parameterized) {
    ThreadCountsParams expected = GetParam();

    MockTaskable taskable;

    TestExecutorPool pool(expected.maxThreads, // MaxThreads
                          expected.in_reader_writer,
                          expected.in_reader_writer,
                          0, // MaxNumAuxio
                          0 // MaxNumNonio
    );

    pool.registerTaskable(taskable);

    EXPECT_EQ(expected.reader, pool.getNumReaders())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.writer, pool.getNumWriters())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.auxIO, pool.getNumAuxIO())
            << "When maxThreads=" << expected.maxThreads;
    EXPECT_EQ(expected.nonIO, pool.getNumNonIO())
            << "When maxThreads=" << expected.maxThreads;

    pool.unregisterTaskable(taskable, false);
    pool.shutdown();
}

std::vector<ThreadCountsParams> threadCountValues = {
        {ThreadPoolConfig::ThreadCount::Default, 1, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 2, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 4, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 8, 8, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::Default, 10, 10, 4, 1, 3},
        {ThreadPoolConfig::ThreadCount::Default, 14, 14, 4, 2, 4},
        {ThreadPoolConfig::ThreadCount::Default, 20, 16, 4, 2, 6},
        {ThreadPoolConfig::ThreadCount::Default, 24, 16, 4, 3, 7},
        {ThreadPoolConfig::ThreadCount::Default, 32, 16, 4, 4, 8},
        {ThreadPoolConfig::ThreadCount::Default, 48, 16, 4, 5, 8},
        {ThreadPoolConfig::ThreadCount::Default, 64, 16, 4, 7, 8},
        {ThreadPoolConfig::ThreadCount::Default, 128, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 1, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 2, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 4, 4, 4, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 8, 8, 8, 1, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 10, 10, 10, 1, 3},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 14, 14, 14, 2, 4},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 20, 20, 20, 2, 6},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 24, 24, 24, 3, 7},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 32, 32, 32, 4, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 48, 48, 48, 5, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 64, 64, 64, 7, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 128, 64, 64, 8, 8}};

INSTANTIATE_TEST_SUITE_P(ThreadCountTest,
                         ExecutorPoolTestWithParam,
                         ::testing::ValuesIn(threadCountValues),
                         ::testing::PrintToStringParamName());

TEST_F(ExecutorPoolDynamicWorkerTest, new_worker_naming_test) {
    EXPECT_EQ(2, pool->getNumWriters());
    std::vector<std::string> names = pool->getThreadNames();

    EXPECT_TRUE(pool->threadExists("writer_worker_0"));
    EXPECT_TRUE(pool->threadExists("writer_worker_1"));

    pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));

    EXPECT_TRUE(pool->threadExists("writer_worker_0"));
    EXPECT_FALSE(pool->threadExists("writer_worker_1"));

    pool->setNumWriters(ThreadPoolConfig::ThreadCount(2));

    EXPECT_TRUE(pool->threadExists("writer_worker_0"));
    EXPECT_TRUE(pool->threadExists("writer_worker_1"));
}

/* Make sure that a task that has run once and been cancelled can be
 * rescheduled and will run again properly.
 */
TEST_F(ExecutorPoolDynamicWorkerTest, reschedule_dead_task) {
    size_t runCount{0};

    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&] {
                ++runCount;
                return false;
            });

    ASSERT_EQ(TASK_RUNNING, task->getState())
            << "Initial task state should be RUNNING";

    pool->schedule(task);
    pool->waitForEmptyTaskLocator();

    EXPECT_EQ(TASK_DEAD, task->getState())
            << "Task has completed and been cleaned up, state should be DEAD";

    pool->schedule(task);
    pool->waitForEmptyTaskLocator();

    EXPECT_EQ(TASK_DEAD, task->getState())
            << "Task has completed and been cleaned up, state should be DEAD";

    EXPECT_EQ(2, runCount);
}

/* Testing to ensure that repeatedly scheduling a task does not result in
 * multiple entries in the taskQueue - this could cause a deadlock in
 * _unregisterTaskable when the taskLocator is empty but duplicate tasks remain
 * in the queue.
 */
TEST_F(SingleThreadedExecutorPoolTest, ignore_duplicate_schedule) {
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 10, true, [&] { return false; });

    size_t taskId = task->getId();

    ASSERT_EQ(taskId, pool->schedule(task));
    ASSERT_EQ(taskId, pool->schedule(task));

    std::map<size_t, TaskQpair> taskLocator =
            dynamic_cast<SingleThreadedExecutorPool*>(ExecutorPool::get())
                    ->getTaskLocator();

    TaskQueue* queue = taskLocator.find(taskId)->second.second;

    EXPECT_EQ(1, queue->getFutureQueueSize())
            << "Task should only appear once in the taskQueue";

    pool->cancel(taskId, true);
}

class ScheduleOnDestruct {
public:
    ScheduleOnDestruct(ExecutorPool& pool,
                       EventuallyPersistentEngine& e,
                       CB3ExecutorThread& thread)
        : pool(pool), engine(e), thread(thread) {
    }
    ~ScheduleOnDestruct();

    ExecutorPool& pool;
    EventuallyPersistentEngine& engine;
    CB3ExecutorThread& thread;
};

class ScheduleOnDestructTask : public GlobalTask {
public:
    ScheduleOnDestructTask(EventuallyPersistentEngine& e,
                           TaskId taskId,
                           double sleeptime,
                           bool completeBeforeShutdown,
                           ExecutorPool& pool,
                           CB3ExecutorThread& thread)
        : GlobalTask(&e, taskId, sleeptime, completeBeforeShutdown),
          scheduleOnDestruct(pool, e, thread) {
    }

    bool run() override {
        return false;
    }

    std::string getDescription() override {
        return "ScheduleOnDestructTask";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(60);
    }

    ScheduleOnDestruct scheduleOnDestruct;
};

class StopTask : public GlobalTask {
public:
    StopTask(EventuallyPersistentEngine& e,
             TaskId taskId,
             double sleeptime,
             bool completeBeforeShutdown,
             CB3ExecutorThread& thread)
        : GlobalTask(&e, taskId, sleeptime, completeBeforeShutdown),
          thread(thread) {
    }

    bool run() override {
        thread.stop(false);
        return false;
    }

    std::string getDescription() override {
        return "StopTask";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        return std::chrono::seconds(60);
    }

    CB3ExecutorThread& thread;
};

ScheduleOnDestruct::~ScheduleOnDestruct() {
    // Schedule StopTask from the destructor. This task will ensure that
    // ::run terminates its inner run-loop.
    ExTask task = std::make_shared<StopTask>(
            engine, TaskId::ItemPager, 10, true, thread);
    // MB-40517: This deadlocked
    pool.schedule(task);
    pool.wake(task->getId());
}

/*
 * At least one object in KV-engine (VBucket) requires to schedule a new
 * task from its destructor - and that destructor can be called from a task's
 * destructor. Thus it's a requirement of ExecutorPool/KV that a task
 * destructing within the scope of ExecutorPool::cancel() can safely call
 * ExecutorPool::schedule(). MB-40517 broke this as a double lock (deadlock)
 * occurred.
 *
 * This test simulates the requirements with the following:
 * 1) schedule the "ScheduleOnDestructTask", a task which will schedule another
 *    task when it destructs.
 * 2) Use the real CB3ExecutorThread and run the task
 * 3) ~ScheduleOnDestructTask is called from CB3ExecutorThread::run
 * 4) From the destructor, schedule a new task, StopTask. When this task runs it
 *    calls into ExeuctorThread::stop so the run-loop terminates and the test
 *    can finish.
 * Without the MB-40517 fix, step 4 would deadlock because the ExecutorPool
 * lock is already held by the calling thread.
 */
TEST_F(SingleThreadedExecutorPoolTest, cancel_can_schedule) {
    // TODO: Refactor this to support FollyExecutorPool when introduced.
    CB3ExecutorThread thr(&dynamic_cast<CB3ExecutorPool&>(*pool),
                          NONIO_TASK_IDX,
                          "cancel_can_schedule");
    auto engine = SynchronousEPEngine::build("");
    auto memUsedA = engine->getEpStats().getPreciseTotalMemoryUsed();
    ExTask task = std::make_shared<ScheduleOnDestructTask>(
            *engine.get(), TaskId::ItemPager, 10, true, *pool, thr);

    auto id = task->getId();
    ASSERT_EQ(id, pool->schedule(task));
    task.reset(); // only the pool has a reference

    // Wake our task and call run. The task is a 'one-shot' task which will
    // then be cancelled, which as part of cancel calls schedule
    pool->wake(id);
    thr.run();

    EXPECT_EQ(memUsedA, engine->getEpStats().getPreciseTotalMemoryUsed());
}
