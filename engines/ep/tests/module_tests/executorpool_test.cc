/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the ExecutorPool class
 */

#include "executorpool_test.h"
#include "../mock/mock_add_stat_fn.h"
#include "lambda_task.h"
#include "test_helpers.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "thread_gate.h"
#include <executor/folly_executorpool.h>

#include <folly/portability/GMock.h>
#include <folly/synchronization/Baton.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <programs/engine_testapp/mock_cookie.h>

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace testing;

/**
 * Creates a LambdaTask which increments the given ThreadGate when run
 */
ExTask makeTask(Taskable& taskable,
                ThreadGate& tg,
                TaskId taskId,
                double initialSleepTime = 0) {
    return std::make_shared<LambdaTask>(
            taskable, taskId, initialSleepTime, true, [&](LambdaTask&) -> bool {
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

template <typename T>
void ExecutorPoolTest<T>::makePool(int maxThreads,
                                   int numReaders,
                                   int numWriters,
                                   int numAuxIO,
                                   int numNonIO) {
    pool = std::make_unique<T>(maxThreads,
                               ThreadPoolConfig::ThreadCount(numReaders),
                               ThreadPoolConfig::ThreadCount(numWriters),
                               numAuxIO,
                               numNonIO);
}

using ExecutorPoolTypes = ::testing::Types<TestExecutorPool, FollyExecutorPool>;
TYPED_TEST_SUITE(ExecutorPoolTest, ExecutorPoolTypes);

/**
 * Tests basic registration / unregistration of Taskables.
 */
TYPED_TEST(ExecutorPoolTest, register_taskable_test) {
    this->makePool(10);

    MockTaskable taskable;
    MockTaskable taskable2;

    // Only CB3ExecutorPool starts / stops threads on first / last
    // taskable.
    const auto expectedInitialWorkers =
            (typeid(TypeParam) == typeid(TestExecutorPool)) ? 0 : 8;

    ASSERT_EQ(expectedInitialWorkers, this->pool->getNumWorkersStat());
    ASSERT_EQ(0, this->pool->getNumTaskables());

    this->pool->registerTaskable(taskable);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(1, this->pool->getNumTaskables());

    this->pool->registerTaskable(taskable2);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(2, this->pool->getNumTaskables());

    this->pool->unregisterTaskable(taskable2, false);

    ASSERT_EQ(8, this->pool->getNumWorkersStat());
    ASSERT_EQ(1, this->pool->getNumTaskables());

    this->pool->unregisterTaskable(taskable, false);

    ASSERT_EQ(expectedInitialWorkers, this->pool->getNumWorkersStat());
    ASSERT_EQ(0, this->pool->getNumTaskables());
}

/// Test that forcefully unregistering a tasktable cancels all outstanding
/// tasks.
TYPED_TEST(ExecutorPoolTest, UnregisterForce) {
    this->makePool(1);
    MockTaskable taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which should never run, as when taskable is unregistered
    // it is done forcefully.
    const auto sleepTime = 60.0 * 60.0; // 1 hour
    std::atomic<bool> taskRan = false;
    ExTask task{new LambdaTask(
            taskable, TaskId::ItemPager, sleepTime, true, [&](LambdaTask&) {
                taskRan = true;
                return false;
            })};
    this->pool->schedule(task);

    // Test: Unregister the taskable forcefully - task should be cancelled and
    // not run.
    this->pool->unregisterTaskable(taskable, true);

    EXPECT_FALSE(taskRan);
}

/// Test that after unregisterTaskable returns no more tasks of that Taskable
/// run.
TYPED_TEST(ExecutorPoolTest, UnregisterTaskablesCancelsTasks) {
    this->makePool(1);
    // Taskable deliberately put on heap to highlight any use-after-free issues
    // if any Tasks access it after unregister.
    auto taskable = std::make_unique<MockTaskable>();
    this->pool->registerTaskable(*taskable);

    // Create a task which runs constantly until cancelled.
    folly::Baton taskStart;

    auto taskFn = [&taskStart, taskablePtr = taskable.get()](LambdaTask& task) {
        // Post that this task has started.
        taskStart.post();

        // Sleep here to increase chance it is still running on CPU pool when
        // its taskable is unregistered.
        std::this_thread::sleep_for(std::chrono::milliseconds{1});

        // Access it's taskable - if this runs after unregisterTaskable() than
        // that would be invalid.
        taskablePtr->logRunTime(task, {}, {});
        return false;
    };
    auto task = std::make_shared<LambdaTask>(
            *taskable, TaskId::ItemPager, 0, false, taskFn);
    this->pool->schedule(task);

    // Wait for task to be running.
    taskStart.wait();

    // Test: Unregister the taskable, then delete the taskable. Taskable should
    // not be accessed.
    this->pool->unregisterTaskable(*taskable, false);
    taskable.reset();
}

/**
 * Test that after unregisterTaskable returns all tasks it owns (no references
 * retained by others) are deleted.
 * Regression test for MB-42049.
 */
TYPED_TEST(ExecutorPoolTest, UnregisterTaskablesDeletesTask) {
    // Test Task which set the given flag to true deleted.
    struct OnDeleteTask : public LambdaTask {
        OnDeleteTask(Taskable& t, bool& deleted)
            : LambdaTask(t,
                         TaskId::ItemPager,
                         /*sleeptime*/ 0,
                         /*completeBeforeShutdown*/ false,
                         [](LambdaTask&) {
                             std::this_thread::yield();
                             return true;
                         }),
              deleted(deleted) {
        }
        ~OnDeleteTask() override {
            std::this_thread::yield();
            deleted = true;
        }
        bool& deleted;
    };

    this->makePool(1);

    // Not interested in any calls to logQTime when task runs - just ignore them
    // using NiceMock.
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create an OnDeleteTask, and transfer ownership to ExecutorPool.
    // This should be destructed before unregisterTaskable returns.
    bool deleted = false;
    this->pool->schedule(std::make_shared<OnDeleteTask>(taskable, deleted));

    // Test: Unregister the taskable. 'deleted' should be true as soon as
    // unregisterTaskable finishes.
    this->pool->unregisterTaskable(taskable, false);
    EXPECT_TRUE(deleted)
            << "Task should be deleted when unregisterTaskable returns";
}

/**
 * Test that attempting to schedule a new task during unregisterTaskable
 * doesn't result in unregisterTaskable hanging.
 * For FollyExecutorPool, unregisterTaskable is a multi-stage process. If
 * a new Task is schedule in the middle of it, unregisterTaskable can hang
 * forever, as the newly-scheduled task missed being cancelled.
 *
 * Regression test for MB-42204
 */
TYPED_TEST(ExecutorPoolTest, UnregisterTaskableConcurrentSchedule) {
    this->makePool(1);

    // Setup Taskable. Not interested in any calls to logQTime when task runs
    // - just ignore them using NiceMock.
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Setup a test task which initially sleeps, with
    // completeBeforeShutdown=true so will be woken during unregisterTaskable,
    // at which point it attempts to schedule a second Task.
    this->pool->schedule(ExTask(
            new LambdaTask(taskable,
                           TaskId::ItemPager,
                           INT_MAX,
                           /*completeBeforeShutdown*/ true,
                           [this, &taskable](LambdaTask&) {
                               // Schedule a second task which just sits around
                               // forever.
                               this->pool->schedule(ExTask(new LambdaTask(
                                       taskable,
                                       TaskId::ItemPager,
                                       INT_MAX,
                                       /*completeBeforeShutdown*/ false,
                                       [](LambdaTask&) { return false; })));
                               return false;
                           })));

    // Test: Call unregisterTaskable. This should return and not hang.
    this->pool->unregisterTaskable(taskable, false);
}

/**
 * Test that unregisterTaskable waits for all running tasks to finish
 * before unregisterTaskable returns - i.e. that the user can assume that a
 * Taskable is safe to delete (as all registered Tasks have completed) once
 * unregistered.
 */
TYPED_TEST(ExecutorPoolTest, UnregisterTaskableWaitsForTasks) {
    // Not interested in any calls to logQTime when task runs - just ignore them
    // using NiceMock.
    auto taskable = std::make_unique<NiceMock<MockTaskable>>();
    this->makePool(1);
    this->pool->registerTaskable(*taskable);

    // Test task which whose destructor dereferences its taskable; to
    // check that the Task is deleted _before_ unregisterTaskable returns.
    // Relies on ASan to flag a use-after-free bug.
    struct TestTask : public LambdaTask {
        TestTask(Taskable& t)
            : LambdaTask(t,
                         TaskId::ItemPager,
                         /*sleeptime*/ 0,
                         /*completeBeforeShutdown*/ false,
                         [](LambdaTask&) {
                             std::this_thread::yield();
                             return true;
                         }) {
        }
        ~TestTask() override {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            getTaskable().isShutdown();
        }
    };
    this->pool->schedule(std::make_shared<TestTask>(*taskable));

    // Test: unregisterTaskable and then delete it; TestTask should be deleted
    // before this.
    this->pool->unregisterTaskable(*taskable, false);
    taskable.reset();
}

/// Test that tasks are run immediately when they are woken.
TYPED_TEST(ExecutorPoolTest, Wake) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a lambda task which when run increments ThreadGate, but whose
    // initial sleep time is 1 hour and hence will not run until then.
    ThreadGate tg{1};
    auto sleepTime = 60.0 * 60.0; // 1 hour.
    const auto taskId = this->pool->schedule(
            makeTask(taskable, tg, TaskId::ItemPager, sleepTime));

    // Test: Wake the task, and expect to run "immediately" (give a generous
    // deadline of 10s.
    this->pool->wake(taskId);
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for task to wake";

    this->pool->unregisterTaskable(taskable, true);
}

/// Test that calling wake() on a Task multiple times while it is executing
// only wakes it once.
TYPED_TEST(ExecutorPoolTest, WakeMultiple) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which increments runCount everytime it runs, then re-wakes
    // itself twice on first run.
    // We expect the two wake-ups to be coaleacsed into one execution, _after_
    // this task run completes - i.e. the task shouldn't run again concurrently
    // in a different thread.
    SyncObject cv;
    int runCount = 0;
    auto task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, false, [&](LambdaTask& task) {
                // Initially put the task to sleep "forever".
                task.snooze(INT_MAX);
                {
                    std::unique_lock<std::mutex> guard(cv);
                    runCount++;
                    if (runCount == 1) {
                        // Attempt to wake multiple times. This should only
                        // run the task once (a second time), wake while
                        // already queued to run should have no effect.
                        this->pool->wake(task.getId());
                        this->pool->wake(task.getId());
                        this->pool->wake(task.getId());
                    }
                    cv.notify_one();
                }

                return true;
            });
    this->pool->schedule(task);

    // Test - wait for the task to run (hopefully only) two times.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount >= 2; });
    }

    // Unregister the taskable. This should run any pending tasks to run
    // to completion (note the above task is created with
    // completeBeforeShutdown==true).
    this->pool->unregisterTaskable(taskable, false);
    // Check the runCount again - no more instances should have occurred
    // while shutting down.
    {
        std::unique_lock<std::mutex> guard(cv);
        EXPECT_EQ(2, runCount)
                << "Task should only have run twice after wakeup";
    }
}

// Wake negative test - attempt to wake a task which hasn't been scheduled.
TYPED_TEST(ExecutorPoolTest, WakeWithoutSchedule) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Doesn't matter what the task is, we shouldn't run it.
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                return false;
            });

    EXPECT_FALSE(this->pool->wakeAndWait(task->getId()));

    this->pool->unregisterTaskable(taskable, false);
}

/// Test that waketime (the time a task desires to be run) is correctly updated
/// when a wake() is called to run a task immediately.
TYPED_TEST(ExecutorPoolTest, WakeUpdatesWaketime) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a lambda task with an initial sleep time of 1 hour and hence will
    // not run until explicitly woken below. When is is woken and runs, it
    // checks its waketime has been updated - i.e. is no longer 1 hour in
    // the future.
    folly::Baton baton;
    auto sleepTime = 60.0 * 60.0; // 1 hour.
    auto task = std::make_shared<LambdaTask>(
            taskable,
            TaskId::ItemPager,
            sleepTime,
            false,
            [&baton](LambdaTask& t) {
                EXPECT_LE(t.getWaketime(), std::chrono::steady_clock::now());
                baton.post();
                return false;
            });
    const auto taskId = this->pool->schedule(task);

    // Saity check - waketime should be ~60 mins after schedule (shouldn't take
    // anywhere close to 1minuteto actually schedule the task!).
    ASSERT_GT(task->getWaketime(),
              std::chrono::steady_clock::now() + std::chrono::minutes{59})
            << "Waketime should be ~60 mins in future";

    // Test: Wake the task, and expect to run "immediately" (give a generous
    // deadline of 10s.
    // Note check inside lamda itself,
    this->pool->wake(taskId);
    EXPECT_TRUE(baton.try_wait_for(std::chrono::seconds{10}));

    this->pool->unregisterTaskable(taskable, true);
}

/// Test that snooze correctly updates the waketime of a task.
TYPED_TEST(ExecutorPoolTest, Snooze) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which when run increments ThreadGate, but whose
    // initial sleep time is 1 hour and hence will not run until then.
    ThreadGate tg{1};
    auto initialSleepTime = 60.0 * 60.0; // 1 hour.
    const auto taskId = this->pool->schedule(
            makeTask(taskable, tg, TaskId::ItemPager, initialSleepTime));

    // Test: Wake the task, and expect to run "immediately" (give a generous
    // deadline of 10s.
    // Test: re-snooze the task to run in a fraction of a second.
    this->pool->snooze(taskId, std::numeric_limits<double>::min());
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete())
            << "Timeout waiting for task to run after snooze";

    // Cleanup.
    this->pool->unregisterTaskable(taskable, false);
}

// Test that waking a task supercedes any previous schedule time, and the task
// runs just once.
TYPED_TEST(ExecutorPoolTest, SnoozeThenWake) {
    using namespace std::chrono;

    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task which increments runCount everytime it runs, initally
    // sleeping for 10ms.
    // It also blocks on a threadGate - this is because if the task finishes
    // it'll either be re-scheduled to run (return true), or
    // be cancelled (return false) - either way this wouldn't allow us to
    // test what happens to the original scheduled time.
    duration<double> sleepTime{0.01};
    // Using a thread gate with count 2 - we only expect one thread to checkin
    // to the gate, the second count will be used to unblock the task.
    ThreadGate tg{2};
    SyncObject cv;
    int runCount = 0;
    auto task = std::make_shared<LambdaTask>(
            taskable,
            TaskId::ItemPager,
            sleepTime.count(),
            true,
            [&](LambdaTask& t) {
                {
                    std::unique_lock<std::mutex> guard(cv);
                    runCount++;
                    cv.notify_one();
                }
                tg.threadUp();
                return false;
            });
    this->pool->schedule(task);

    // Test: Wake the task, expecting it to run immediately, but not a second
    // time (the previous schedule time should have been superceded).
    this->pool->wake(task->getId());
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 1; });
    }

    // Wait for a further 2x 'sleepTime' s to check that the original scheduled
    // wake time was correcty cancelled, and the task isn't run more than once.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait_for(guard,
                    duration_cast<nanoseconds>(2 * sleepTime),
                    [&runCount] { return runCount > 1; });
    }
    EXPECT_EQ(1, runCount);

    // Cleanup - increment thread gate to unblock task.
    tg.threadUp();
    EXPECT_EQ(2, tg.getCount());

    this->pool->unregisterTaskable(taskable, false);
}

// Snooze negative test - attempt to wake a task which hasn't been scheduled.
TYPED_TEST(ExecutorPoolTest, SnoozeWithoutSchedule) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Doesn't matter what the task is, we shouldn't run it.
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                return false;
            });

    EXPECT_FALSE(this->pool->snoozeAndWait(task->getId(), 1.0));

    this->pool->unregisterTaskable(taskable, false);
}

/// Test that cancel() followed by snooze(); where cancel has only partially
/// completed is handled correctly.
/// Regression test for MB-43546 (which affected FollyExecutorPool).
TYPED_TEST(ExecutorPoolTest, CancelThenSnooze) {
    if (typeid(TypeParam) == typeid(TestExecutorPool)) {
        // CB3ExecutorPool deadlocks if one attempts to call snooze() while
        // cancel() is still running.
        // Could probably re-write test to call snooze() on a different thread
        // with appropriate synchronization, but given this issue doesn't
        // affect CB3, just skip the test.
        GTEST_SKIP();
    }

    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // To force the scheduling we want, need to call snooze after cancel()
    // has set TaskProxy::task to null; but before that TaskProxy is removed
    // from the owners. To do this, use a custom GlobalTask whose dtor
    // calls snooze - GlobalTask is destroyed before removing TaskProxy from
    // owners.
    struct TestTask : public GlobalTask {
        TestTask(Taskable& taskable, ExecutorPool& pool)
            : GlobalTask(taskable, TaskId::ItemPager, INT_MAX), pool(pool) {
        }

        ~TestTask() override {
            pool.snooze(getId(), 10.0);
        }

        std::string getDescription() const override {
            return "TestTask - "s + GlobalTask::getTaskName(taskId);
        }

        std::chrono::microseconds maxExpectedDuration() const override {
            return {};
        }

    protected:
        bool run() override {
            ADD_FAILURE() << "TestTask should never run";
            return false;
        }
        ExecutorPool& pool;
    };

    auto taskPtr = std::make_shared<TestTask>(taskable, *this->pool);
    auto taskId = this->pool->schedule(taskPtr);

    // reset our local taskPtr so only the ExecutorPool has a reference.
    taskPtr.reset();

    // Test: We want to setup the follow sequence:
    // 1. cancel() the task; which should cause TaskProxy::task in taskOwners
    //    to be set to null.
    // 2. Before the taskOwners element is removed
    //    (via FollyExecutorPool::removeTaskAfterRun), the refcount of the
    //    ExTask is decremented, which given our above TestTask has just one
    //    reference should trigger it's destruction.
    // 3. In TestTask dtor; attempt to snooze() task again. This _should be
    //    handled correctly (ignored) given task is already in the process of
    //    being cancelled, but in MB-43546 snooze() incorrectly attempted to
    //    dereference the null TaskProxy::task member.
    // 4. (Not really important) taskOwners entry is removed via
    //    FollyExecutorPool::removeTaskAfterRun
    this->pool->cancel(taskId, true);

    // Cleanup.
    this->pool->unregisterTaskable(taskable, false);
}

/**
 * Regression test for MB-47451 - ensure that a sequence of schedule(),
 * cancel(), schedule(), cancel() where the asynchronous cancel cleanup is
 * delayed cannot result in a use-after-free.
 */
TYPED_TEST(ExecutorPoolTest, ScheduleCancelx2) {
    // Scenario: The use-after-free occurs when the asynchronous task deletion
    // (which runs on a NonIO thread) is executed _after_ the task has already
    // been re-scheduled.
    // Specifically the following sequence of calls across threads - note
    // numbering indicates logical order of the 6 steps, but vertical position
    // indicates when tasks _actually_ are executed temporally:
    //
    //   IO (futurePool) thread               CPU (NonIO) thread
    //   ----------------------               ------------------
    //   1. scheduleTask()
    //   2. cancelTask()
    //      calls resetTaskPtrViaCpuPool()
    //      - enqueue work on NonIO
    //        thread to reset GlobalTask
    //        shared_ptr.
    //   4. scheduleTask()
    //   5. cancelTask()
    //      (same as step 2).
    //
    //                                        3. <<enqueued at 2>>
    //                                           call resetTaskPtrViaCpuPool
    //                                           lamba
    //                                           - Reset GlobalTaskShared ptr
    //                                           - enqueue work on futurePool
    //                                             to remove Proxy from
    //                                             taskOwners.
    //
    //                                        6. <<enqueued at 2>>
    //                                           call resetTaskPtrViaCpuPool
    //                                           lamba
    //                                           - Reset GlobalTaskShared ptr
    //                                           - enqueue work on futurePool
    //                                             to remove Proxy from
    //                                             taskOwners.
    //                                           *** user-after-free of Proxy
    //                                           ***
    //
    // The use-after-free occurs during the second resetTaskPtrViaCpuPool
    // lambda (6) is executing; as we are essentially re-freeing the Proxy
    // which was freed at (3). This is because the code assumes that (6) will
    // be executed after (4) - when a new Proxy would normally be created.
    //
    // Note that we don't currently see any failure at (4) - where we are
    // logically re-using the same proxy (it's not yet deleted at (3) as one
    // might expect) - as that is a "valid" scenario as per the changes made for
    // MB-42029 which allow a TaskProxy to be re-used if a task is re-scheduled
    // before the cancel cleanup is completed.
    // The above scenario highlights that the fix for MB-42029 (and allowing
    // reuse of TaskProxy objects) is flawed :)

    // Setup - create pool and taskable.
    // Note: Starting with zero CPU worker threads (reader/writer/auxio/nonio)
    // - this is so the tasks scheduled on the NonIO CPU worker threads
    // are not immediately run - see below.
    // (Note: makePool assumes '0' means "auto-configure" for thread counts,
    // hence must make explicit call to setNonIO(0) after construction.)
    this->makePool(1, 1, 1, 1);
    this->pool->setNumNonIO(0);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Setup - simple test task which does nothing.
    // 1 hour - i.e. we don't want it to run when initially scheduled.
    auto sleepTime = 60.0 * 60.0;
    auto task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, sleepTime, false, [](LambdaTask&) {
                return false;
            });

    // 1. schedule.
    auto taskId = this->pool->schedule(task);
    // 2. cancel. Note that (3) cannot occcur yet (as per desired sequence
    //    above) as there are zero NonIO threads running.
    EXPECT_TRUE(this->pool->cancel(taskId, false));
    EXPECT_EQ(TASK_DEAD, task->getState());

    // 3. schedule a second time
    taskId = this->pool->schedule(task);
    // 4. cancel. Again, (5) cannot run yet as there's nonIO threads available
    //    for it to run on.
    EXPECT_TRUE(this->pool->cancel(taskId, false));

    // 3. (and 6). Release the hounds^Wqueued NonIO tasks by setting the NonIO
    // thread count to one. We will see the user-after-free when step 6 occurs
    // (second attempt to delete the TaskProxy).
    this->pool->setNumNonIO(1);

    // Cleanup; we will have crashed before here with the bug.
    this->pool->unregisterTaskable(taskable, false);
}

/**
 * MB-45211: Test that a task returning true without snoozing correctly updates
 * its waketime.
 *
 * If the waketime is not updated, the next time the task runs it will
 * use the _previous_ waketime to calculate the scheduling delay.
 * This can lead to erroneous slow scheduling logging.
 */
TYPED_TEST(ExecutorPoolTest, ReturnTrueWithoutSnoozeUpdatesWakeTime) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    bool firstRun = true;
    folly::Baton<> taskFinished;

    std::chrono::steady_clock::time_point firstWaketime;

    using namespace std::chrono_literals;

    // create a task which runs, returns true to be run again ASAP without
    // snoozing first
    auto task = [&](LambdaTask& task) {
        if (firstRun) {
            firstRun = false;
            firstWaketime = task.getWaketime();
            // return without snoozing
            return true;
        } else {
            // If the waketime was not updated before running the task a
            // second time, the apparent scheduling delay will be from the
            // _original_ wake time.
            EXPECT_LT(firstWaketime, task.getWaketime());
            taskFinished.post();
            return false;
        }
    };

    auto initialSleepTime = 0.0; // run ASAP

    this->pool->schedule(std::make_shared<LambdaTask>(taskable,
                                                      TaskId::ItemPager,
                                                      initialSleepTime,
                                                      true,
                                                      std::move(task)));

    taskFinished.timed_wait(10s);

    // Cleanup.
    this->pool->unregisterTaskable(taskable, false);
}

/* This test creates an ExecutorPool, and attempts to verify that calls to
 * setNumWriters are able to dynamically create more workers than were present
 * at initialisation. A ThreadGate is used to confirm that two tasks
 * of type WRITER_TASK_IDX can run concurrently
 *
 */
TYPED_TEST(ExecutorPoolTest, increase_workers) {
    const size_t numReaders = 1;
    const size_t numWriters = 1;
    const size_t numAuxIO = 1;
    const size_t numNonIO = 1;

    const size_t originalWorkers =
            numReaders + numWriters + numAuxIO + numNonIO;

    // This will allow us to check that numWriters + 1 writer tasks can run
    // concurrently after setNumWriters has been called.
    ThreadGate tg{numWriters + 1};

    this->makePool(5, numReaders, numWriters, numAuxIO, numNonIO);

    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    std::vector<ExTask> tasks;

    for (size_t i = 0; i < numWriters + 1; ++i) {
        // Use any Writer thread task (StatSnap) for the TaskId.
        ExTask task = makeTask(taskable, tg, TaskId::StatSnap);
        this->pool->schedule(task);
        tasks.push_back(task);
    }

    EXPECT_EQ(numWriters, this->pool->getNumWriters());
    ASSERT_EQ(originalWorkers, this->pool->getNumWorkersStat());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(numWriters + 1));

    EXPECT_EQ(numWriters + 1, this->pool->getNumWriters());
    ASSERT_EQ(originalWorkers + 1, this->pool->getNumWorkersStat());

    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to run";

    this->pool->unregisterTaskable(taskable, false);
}

// Verifies the priority of the different thread types. On Windows and Linux
// the Writer threads should be low priority.
TYPED_TEST(ExecutorPoolTest, ThreadPriorities) {
    // Create test pool and register a (mock) taskable to start all threads.
    this->makePool(10);

    const size_t totalNumThreads = 8;

    // Given we have to wait for all threads to be running (called
    // ::run()) before their priority will be set, use a ThreadGate
    // with a simple Task which calls threadUp() to ensure all threads
    // have started before checking priorities.
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);
    ThreadGate tg{totalNumThreads};

    // Task which when run records the priority of the thread it is executing
    // on, then increments threadGate.
    struct CheckPriorityTask : public GlobalTask {
        CheckPriorityTask(Taskable& taskable,
                          TaskId taskId,
                          ThreadGate& threadGate)
            : GlobalTask(taskable, taskId), threadGate(threadGate) {
        }

        std::string getDescription() const override {
            return "CheckPriorityTask - "s + GlobalTask::getTaskName(taskId);
        }

        std::chrono::microseconds maxExpectedDuration() const override {
            return {};
        }

    protected:
        bool run() override {
            threadPriority = getpriority(PRIO_PROCESS, 0);
            threadGate.threadUp();
            return false;
        }

    public:
        ThreadGate& threadGate;
        /// Observed priority of the thread this task was run on.
        int threadPriority{-1};
    };

    // Need 2 tasks of each type, so both threads of each type are
    // started.
    auto makePriorityTask = [&taskable, &tg](TaskId taskId) {
        return std::make_shared<CheckPriorityTask>(taskable, taskId, tg);
    };
    std::vector<std::shared_ptr<CheckPriorityTask>> tasks;
    // Reader
    tasks.push_back(makePriorityTask(TaskId::MultiBGFetcherTask));
    tasks.push_back(makePriorityTask(TaskId::MultiBGFetcherTask));
    // Writer
    tasks.push_back(makePriorityTask(TaskId::FlusherTask));
    tasks.push_back(makePriorityTask(TaskId::FlusherTask));
    // AuxIO
    tasks.push_back(makePriorityTask(TaskId::AccessScanner));
    tasks.push_back(makePriorityTask(TaskId::AccessScanner));
    // NonIO
    tasks.push_back(makePriorityTask(TaskId::ItemPager));
    tasks.push_back(makePriorityTask(TaskId::ItemPager));

    for (auto& task : tasks) {
        this->pool->schedule(task);
    }
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for threads to start";

    // Windows (via folly portability) uses 20 for default (normal) priority.
    const int defaultPriority = folly::kIsWindows ? 20 : 0;

    // We only set Writer threads to a non-default level on Linux.
    const int expectedWriterPriority = folly::kIsLinux ? 19 : defaultPriority;

    for (const auto& task : tasks) {
        const auto type = GlobalTask::getTaskType(task->getTaskId());
        switch (type) {
        case WRITER_TASK_IDX:
            EXPECT_EQ(expectedWriterPriority, task->threadPriority)
                    << "for task: " << task->getDescription();
            break;
        case READER_TASK_IDX:
        case AUXIO_TASK_IDX:
        case NONIO_TASK_IDX:
            EXPECT_EQ(defaultPriority, task->threadPriority)
                    << "for task: " << task->getDescription();
            break;
        default:
            FAIL() << "Unexpected task type: " << type;
        }
    }

    this->pool->unregisterTaskable(taskable, false);
}

// Test that Task queue stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, TaskQStats) {
    this->makePool(1);
    MockTaskable lowPriBucket{"LowPriMockTaskable", LOW_BUCKET_PRIORITY};
    this->pool->registerTaskable(lowPriBucket);

    // Create some tasks with long sleep times to check for non-zero InQueue
    // (futureQueue) sizes.
    auto scheduleTask = [&](Taskable& bucket, TaskId id) {
        this->pool->schedule(ExTask(new LambdaTask(
                bucket, id, 600.0, true, [&](LambdaTask&) { return false; })));
    };

    // Reader.
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    // Writer
    scheduleTask(lowPriBucket, TaskId::FlusherTask);
    scheduleTask(lowPriBucket, TaskId::FlusherTask);
    scheduleTask(lowPriBucket, TaskId::FlusherTask);
    // AuxIO
    scheduleTask(lowPriBucket, TaskId::AccessScanner);
    // NonIO - left empty.

    MockAddStat mockAddStat;

    using ::testing::_;
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:InQsize", "3", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:InQsize", "2", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:InQsize", "1", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:OutQsize", "0", nullptr));

    this->pool->doTaskQStat(lowPriBucket, nullptr, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(lowPriBucket, true);
}

// Test that Task queue stats for different prioritoes are reported correctly.
TYPED_TEST(ExecutorPoolTest, TaskQStatsMultiPriority) {
    if (typeid(TypeParam) == typeid(FollyExecutorPool)) {
        // Not yet implemented for FollyExecutorPool.
        GTEST_SKIP();
    }
    this->makePool(1);
    // Create and register both high and low priority buckets so stats for
    // both priorities are reported.
    MockTaskable lowPriBucket{"LowPriMockTaskable", LOW_BUCKET_PRIORITY};
    MockTaskable highPriBucket;
    this->pool->registerTaskable(lowPriBucket);
    this->pool->registerTaskable(highPriBucket);

    // Create some tasks with long sleep times to check for non-zero InQueue
    // (futureQueue) sizes.
    auto scheduleTask = [&](Taskable& bucket, TaskId id) {
        this->pool->schedule(ExTask(new LambdaTask(
                bucket, id, 600.0, true, [&](LambdaTask&) { return false; })));
    };

    // Reader.
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    scheduleTask(lowPriBucket, TaskId::MultiBGFetcherTask);
    // Writer
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    scheduleTask(highPriBucket, TaskId::FlusherTask);
    // AuxIO
    scheduleTask(highPriBucket, TaskId::AccessScanner);
    // NonIO - left empty.

    MockAddStat mockAddStat;

    using ::testing::_;
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Writer:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:InQsize", "2", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_Reader:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_AuxIO:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:LowPrioQ_NonIO:OutQsize", "0", nullptr));

    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Writer:InQsize", "3", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Writer:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Reader:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_Reader:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_AuxIO:InQsize", "1", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_AuxIO:OutQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_NonIO:InQsize", "0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("ep_workload:HiPrioQ_NonIO:OutQsize", "0", nullptr));

    this->pool->doTaskQStat(lowPriBucket, nullptr, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(lowPriBucket, true);
    this->pool->unregisterTaskable(highPriBucket, true);
}

// Test that worker stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, WorkerStats) {
    if (typeid(TypeParam) == typeid(FollyExecutorPool)) {
        // Not yet implemented for FollyExecutorPool.
        GTEST_SKIP();
    }

    this->makePool(1, 1, 1, 1, 1);
    // Create two buckets so they have different names.
    NiceMock<MockTaskable> bucket0("bucket0");
    NiceMock<MockTaskable> bucket1("bucket1");
    this->pool->registerTaskable(bucket0);
    this->pool->registerTaskable(bucket1);

    // Create a task which just runs forever (until task is cancelled) so
    // we have a running task to show stats for.
    ThreadGate tg{1};
    this->pool->schedule(ExTask(new LambdaTask(
            bucket0, TaskId::ItemPager, 0, false, [&tg](LambdaTask& me) {
                tg.threadUp();
                while (!me.isdead()) {
                    std::this_thread::yield();
                }
                return false;
            })));
    tg.waitFor(std::chrono::seconds(10));
    ASSERT_TRUE(tg.isComplete());

    MockAddStat mockAddStat;

    using namespace testing;
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:state", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:task", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:cur_time", _, nullptr));

    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:state", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:task", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:cur_time", _, nullptr));

    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:state", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:task", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:cur_time", _, nullptr));

    // NonIO pool has the above ItemPager task running on it.
    EXPECT_CALL(mockAddStat,
                callback("NonIO_worker_0:bucket", "bucket0", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("NonIO_worker_0:state", "running", nullptr));
    EXPECT_CALL(mockAddStat,
                callback("NonIO_worker_0:task", "Lambda Task", nullptr));
    // The 'runtime' stat is only reported if the thread is in state
    // EXECUTOR_RUNNING. Nominally it will only be in that state when actually
    // running a task, however it also briefly exists in that state when
    // first started, before it determines if there is a Task to run.
    // As such, allow 'runtime' to be reported, but it is optional for all but
    // the NonIO pool, where we *know* we have the above test task running.
    EXPECT_CALL(mockAddStat, callback("Reader_worker_0:runtime", _, nullptr))
            .Times(AtMost(1));
    EXPECT_CALL(mockAddStat, callback("Writer_worker_0:runtime", _, nullptr))
            .Times(AtMost(1));
    EXPECT_CALL(mockAddStat, callback("AuxIO_worker_0:runtime", _, nullptr))
            .Times(AtMost(1));

    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:runtime", _, nullptr));
    EXPECT_CALL(mockAddStat, callback("NonIO_worker_0:cur_time", _, nullptr));

    this->pool->doWorkerStat(bucket0, nullptr, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(bucket0, true);
    this->pool->unregisterTaskable(bucket1, true);
}

// Test that task stats are reported correctly.
TYPED_TEST(ExecutorPoolTest, TaskStats) {
    this->makePool(1, 1, 1, 1, 1);
    // Create two buckets so they have different names.
    NiceMock<MockTaskable> bucket0("bucket0");
    NiceMock<MockTaskable> bucket1("bucket1");
    this->pool->registerTaskable(bucket0);
    this->pool->registerTaskable(bucket1);

    // Create two tasks which just run once, then sleep forever, so we have
    // some task stats to report.
    ThreadGate tg{2};
    auto sleepyFn = [&tg](LambdaTask& me) {
        tg.threadUp();
        me.snooze(INT_MAX);
        return true;
    };
    this->pool->schedule(std::make_shared<LambdaTask>(
            bucket0, TaskId::ItemPager, 0, false, sleepyFn));
    this->pool->schedule(std::make_shared<LambdaTask>(
            bucket1, TaskId::FlusherTask, 0, false, sleepyFn));
    tg.waitFor(std::chrono::seconds(10));
    ASSERT_TRUE(tg.isComplete());

    MockAddStat mockAddStat;

    using ::testing::_;

    // "ep_tasks:tasks" is returned as a JSON object.
    // To check it's value, use a function which decodes the JSON and checks
    // various expected fields.
    auto tasksChecker = [](std::string s) -> std::string {
        auto tasks = nlohmann::json::parse(s);
        if (!tasks.is_array()) {
            return "Not an array:" + tasks.dump();
        }
        for (auto t : tasks) {
            if (!t.is_object()) {
                return "Task not an object: " + t.dump();
            }
            for (auto field : {"tid"s,
                               "state"s,
                               "name"s,
                               "this"s,
                               "bucket"s,
                               "description"s,
                               "priority"s,
                               "waketime_ns"s,
                               "total_runtime_ns"s,
                               "last_starttime_ns"s,
                               "previous_runtime_ns"s,
                               "num_runs"s,
                               "type"s}) {
                if (t.count(field) != 1) {
                    return "Missing field '" + field + "':" + t.dump();
                }
            }
        }
        // Return empty string to indicate success
        return "";
    };

    EXPECT_CALL(mockAddStat,
                callback("ep_tasks:tasks",
                         testing::ResultOf(tasksChecker, ""),
                         nullptr));
    EXPECT_CALL(mockAddStat, callback("ep_tasks:cur_time", _, nullptr));

    this->pool->doTasksStat(bucket0, nullptr, mockAddStat.asStdFunction());

    // Set force==true to cancel all the pending tasks.
    this->pool->unregisterTaskable(bucket0, true);
    this->pool->unregisterTaskable(bucket1, true);
}

// Test that scheduler stats (long long tasks have been waiting to run for)
// are reported correctly.
TYPED_TEST(ExecutorPoolTest, SchedulerStats) {
    this->makePool(1);
    NiceMock<MockTaskable> taskable;
    this->pool->registerTaskable(taskable);

    // Create a task to run, which we can check scheduler times for.
    ThreadGate tg{1};
    auto task = makeTask(taskable, tg, TaskId::ItemPager, 0);

    // Set expectation on logQTime
    using ::testing::_;
    EXPECT_CALL(taskable, logQTime(_, _, _));

    this->pool->schedule(task);

    // Wait for the task to run (and notify threadGate).
    tg.waitFor(std::chrono::seconds(10));
    EXPECT_TRUE(tg.isComplete()) << "Timeout waiting for task to wake";

    this->pool->unregisterTaskable(taskable, true);
}

TYPED_TEST_SUITE(ExecutorPoolDynamicWorkerTest, ExecutorPoolTypes);

TYPED_TEST(ExecutorPoolDynamicWorkerTest, decrease_workers) {
    ASSERT_EQ(2, this->pool->getNumWriters());
    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));
    EXPECT_EQ(1, this->pool->getNumWriters());
}

TYPED_TEST(ExecutorPoolDynamicWorkerTest, setDefault) {
    ASSERT_EQ(2, this->pool->getNumWriters());
    ASSERT_EQ(2, this->pool->getNumReaders());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(4, this->pool->getNumWriters())
            << "num_writers should be 4 with ThreadCount::Default";

    this->pool->setNumReaders(ThreadPoolConfig::ThreadCount::Default);
    EXPECT_EQ(16, this->pool->getNumReaders())
            << "num_writers should be capped at 16 with ThreadCount::Default";
}

TYPED_TEST(ExecutorPoolDynamicWorkerTest, setDiskIOOptimized) {
    ASSERT_EQ(2, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(this->MaxThreads, this->pool->getNumWriters());

    this->pool->setNumReaders(ThreadPoolConfig::ThreadCount::DiskIOOptimized);
    EXPECT_EQ(this->MaxThreads, this->pool->getNumReaders());
}

TEST_P(ExecutorPoolTestWithParam, max_threads_test_parameterized) {
    ThreadCountsParams expected = GetParam();

    NiceMock<MockTaskable> taskable;

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
        {ThreadPoolConfig::ThreadCount::Default, 1, 4, 4, 2, 2},
        {ThreadPoolConfig::ThreadCount::Default, 2, 4, 4, 2, 2},
        {ThreadPoolConfig::ThreadCount::Default, 4, 4, 4, 4, 2},
        {ThreadPoolConfig::ThreadCount::Default, 8, 8, 4, 8, 2},
        {ThreadPoolConfig::ThreadCount::Default, 10, 10, 4, 8, 3},
        {ThreadPoolConfig::ThreadCount::Default, 14, 14, 4, 8, 4},
        {ThreadPoolConfig::ThreadCount::Default, 20, 16, 4, 8, 6},
        {ThreadPoolConfig::ThreadCount::Default, 24, 16, 4, 8, 7},
        {ThreadPoolConfig::ThreadCount::Default, 32, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::Default, 48, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::Default, 64, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::Default, 128, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::Default, 256, 16, 4, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 1, 4, 4, 2, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 2, 4, 4, 2, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 4, 4, 4, 4, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 8, 8, 8, 8, 2},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 10, 10, 10, 8, 3},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 14, 14, 14, 8, 4},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 20, 20, 20, 8, 6},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 24, 24, 24, 8, 7},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 32, 32, 32, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 48, 48, 48, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 64, 64, 64, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 128, 128, 128, 8, 8},
        {ThreadPoolConfig::ThreadCount::DiskIOOptimized, 256, 128, 128, 8, 8}};

INSTANTIATE_TEST_SUITE_P(ThreadCountTest,
                         ExecutorPoolTestWithParam,
                         ::testing::ValuesIn(threadCountValues),
                         ::testing::PrintToStringParamName());

TYPED_TEST(ExecutorPoolDynamicWorkerTest, new_worker_naming_test) {
    EXPECT_EQ(2, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(1));

    EXPECT_EQ(1, this->pool->getNumWriters());

    this->pool->setNumWriters(ThreadPoolConfig::ThreadCount(2));

    EXPECT_EQ(2, this->pool->getNumWriters());
}

/* Make sure that a task that has run once and been cancelled can be
 * rescheduled and will run again properly.
 */
TYPED_TEST(ExecutorPoolDynamicWorkerTest, reschedule_dead_task) {
    // Must have a single NonIO thread to ensure serialization of `task` and
    // `sentinelTask`.
    this->pool->setNumNonIO(1);

    SyncObject cv;
    size_t runCount{0};
    ExTask task = std::make_shared<LambdaTask>(
            this->taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                std::unique_lock<std::mutex> guard(cv);
                ++runCount;
                cv.notify_one();
                return false;
            });

    ASSERT_EQ(TASK_RUNNING, task->getState())
            << "Initial task state should be RUNNING";

    this->pool->schedule(task);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 1; });
    }

    // To know when `task` has actually finished running and executor thread
    // has set it to dead (and not just got as far notifying the condvar in
    // it's run() method), insert and schedule a "sentinal" task. Once that
    // task has started running we know our main task must have completed
    // all execution.
    bool sentinelExecuted = false;
    ExTask sentinelTask = std::make_shared<LambdaTask>(
            this->taskable, TaskId::ItemPager, 0, true, [&](LambdaTask&) {
                std::unique_lock<std::mutex> guard(cv);
                sentinelExecuted = true;
                cv.notify_one();
                return false;
            });
    this->pool->schedule(sentinelTask);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&sentinelExecuted] { return sentinelExecuted; });
    }

    EXPECT_EQ(TASK_DEAD, task->getState())
            << "Task has completed and been cleaned up, state should be DEAD";

    // Schedule main task again.
    this->pool->schedule(task);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&runCount] { return runCount == 2; });
    }

    // As above, use sentinal task to ensure task has indeed finished execution.
    {
        std::unique_lock<std::mutex> guard(cv);
        sentinelExecuted = false;
    }
    this->pool->schedule(sentinelTask);
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&sentinelExecuted] { return sentinelExecuted; });
    }

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
            taskable, TaskId::ItemPager, 10, true, [&](LambdaTask&) {
                return false;
            });

    size_t taskId = task->getId();

    ASSERT_EQ(taskId, this->pool->schedule(task));
    ASSERT_EQ(taskId, this->pool->schedule(task));

    std::map<size_t, TaskQpair> taskLocator =
            dynamic_cast<SingleThreadedExecutorPool*>(ExecutorPool::get())
                    ->getTaskLocator();

    TaskQueue* queue = taskLocator.find(taskId)->second.second;

    EXPECT_EQ(1, queue->getFutureQueueSize())
            << "Task should only appear once in the taskQueue";

    this->pool->cancel(taskId, true);
}

template <>
ExecutorPoolEpEngineTest<TestExecutorPool>::ExecutorPoolEpEngineTest() {
    config = "executor_pool_backend=cb3;dbname=" + dbnameFromCurrentGTestInfo();
}

template <>
ExecutorPoolEpEngineTest<FollyExecutorPool>::ExecutorPoolEpEngineTest() {
    config = "executor_pool_backend=folly;dbname=" +
             dbnameFromCurrentGTestInfo();
}

template <typename T>
void ExecutorPoolEpEngineTest<T>::SetUp() {
    if (config.find("cb3") != std::string::npos) {
        ExecutorPool::create(ExecutorPool::Backend::CB3);
    } else if (config.find("folly") != std::string::npos) {
        ExecutorPool::create(ExecutorPool::Backend::Folly);
    } else {
        throw std::runtime_error(
                "ExecutorPoolEpEngineTest<T>::SetUp(): Unknown backend");
    }
    engine = SynchronousEPEngine::build(config);
    ExecutorPoolTest<T>::SetUp();
}

template <typename T>
void ExecutorPoolEpEngineTest<T>::TearDown() {
    engine.reset();
    ExecutorPool::shutdown();
}

TYPED_TEST_SUITE(ExecutorPoolEpEngineTest, ExecutorPoolTypes);

class ScheduleOnDestruct {
public:
    ScheduleOnDestruct(ExecutorPool& pool,
                       Taskable& taskable,
                       SyncObject& cv,
                       bool& stopTaskHasRun)
        : pool(pool),
          taskable(taskable),
          cv(cv),
          stopTaskHasRun(stopTaskHasRun) {
    }
    ~ScheduleOnDestruct();

    ExecutorPool& pool;
    Taskable& taskable;
    SyncObject& cv;
    bool& stopTaskHasRun;
};

class ScheduleOnDestructTask : public GlobalTask {
public:
    ScheduleOnDestructTask(EventuallyPersistentEngine& bucket,
                           Taskable& taskable,
                           ExecutorPool& pool,
                           SyncObject& cv,
                           bool& stopTaskHasRun)
        : GlobalTask(&bucket, TaskId::ItemPager, 10, true),
          scheduleOnDestruct(pool, taskable, cv, stopTaskHasRun) {
    }

    bool run() override {
        return false;
    }

    std::string getDescription() const override {
        return "ScheduleOnDestructTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(60);
    }

    ScheduleOnDestruct scheduleOnDestruct;
};

class StopTask : public GlobalTask {
public:
    StopTask(Taskable& taskable, SyncObject& cv, bool& taskHasRun)
        : GlobalTask(taskable, TaskId::ItemPager, 10, false),
          taskHasRun(taskHasRun),
          cv(cv) {
    }

    bool run() override {
        std::lock_guard<std::mutex> guard(cv);
        taskHasRun = true;
        cv.notify_one();
        return false;
    }

    std::string getDescription() const override {
        return "StopTask";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::seconds(60);
    }

    // Flag and CV to notify main thread that this task has run.
    bool& taskHasRun;
    SyncObject& cv;
};

ScheduleOnDestruct::~ScheduleOnDestruct() {
    // We don't want to have the StopTask's heap allocation accounted to
    // the bucket we are testing with. This is because we cannot easily
    // determine when StopTask itself is cleaned up by the underlying executor
    // thread, and if it isn't destroyed by the time we check memory at the
    // end of the test then the memory comparison will fail.
    // We don't otherwise care when this Task actually is cleaned up, so
    // simply ignore it's memory usage when allocating, and associate it
    // with mockTaskable so when it's later freed the deallocation is
    // also ignored.
    BucketAllocationGuard guard(nullptr);

    ExTask task = std::make_shared<StopTask>(taskable, cv, stopTaskHasRun);
    // Schedule a Task from the destructor. This is done to validate we
    // do not deadlock when scheduling when called via cancel().

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
 * 2) Use the real ExecutorPool and run the task
 * 3) ~ScheduleOnDestructTask is called from ExecutorThread::run
 * 4) From the destructor, schedule a new task, StopTask. When this task runs it
 *    calls into ExeuctorThread::stop so the run-loop terminates and the test
 *    can finish.
 * Without the MB-40517 fix, step 4 would deadlock because the ExecutorPool
 * lock is already held by the calling thread.
 */
TYPED_TEST(ExecutorPoolEpEngineTest, cancel_can_schedule) {
    // Note: Need to use global ExecutorPool (and not this->pool as typical for
    // ExecutorPoolTest) as we need an EPEngine object for testing memory
    // tracking, and EpEngine will create theglobal ExecutorPool if it doesn't
    // already exist. We don't want two different pools in action...
    auto* pool = ExecutorPool::get();
    // Config: Require only 1 NonIO thread as we want ScheduleOnDestructTask and
    // StopTask to be serialised with respect to each other.
    pool->setNumNonIO(1);

    // Create and register a MockTaskable to use for the StopTask, as we don't
    // want to account it's memory to the bucket under test. See further
    // comments in ~ScheduleOnDestruct().
    NiceMock<MockTaskable> mockTaskable;
    pool->registerTaskable(mockTaskable);

    // Condition variable and flag to allow this main thread to wait on the
    // final StopTask to be run.
    SyncObject cv;
    bool stopTaskHasRun = false;
    auto memUsedA = this->engine->getEpStats().getPreciseTotalMemoryUsed();
    ExTask task = std::make_shared<ScheduleOnDestructTask>(
            *this->engine, mockTaskable, *pool, cv, stopTaskHasRun);

    auto id = task->getId();
    ASSERT_EQ(id, pool->schedule(task));
    task.reset(); // only the pool has a reference

    // Wake our task and call run. The task is a 'one-shot' task which will
    // then be cancelled, which as part of cancel calls schedule (for StopTask).
    pool->wake(id);

    // Wait on StopTask to run.
    {
        std::unique_lock<std::mutex> guard(cv);
        cv.wait(guard, [&stopTaskHasRun] { return stopTaskHasRun; });
    }

    EXPECT_EQ(memUsedA, this->engine->getEpStats().getPreciseTotalMemoryUsed());

    // Cleanup - want to ensure global ExecutorPool is destroyed at the end of
    // this test, which requires engine is destructed first.
    this->engine.reset();
    pool->unregisterTaskable(mockTaskable, true);
    pool->shutdown();
}

/**
 * Waits for up to 10 seconds for the given engine to have memory usage
 * equal to expected.
 * @param engine The engine to test for memory usage
 * @param expected The expected memory usage
 * @param[out] actual The last read memory used value. If function returns
 *             true, this will equal expected.
 * @returns true on success, or false if memory never became equal to expected
 *          within the time limit.
 */
static bool waitForMemUsedToBe(EventuallyPersistentEngine& engine,
                               size_t expected,
                               size_t& actual) {
    return waitForPredicate(
            [&engine, expected, &actual] {
                actual = engine.getEpStats().getPreciseTotalMemoryUsed();
                return expected == actual;
            },
            std::chrono::seconds{10});
}

// Test that per-bucket memory tracking is handled correctly when
// tasks are running on background threads.
TYPED_TEST(ExecutorPoolEpEngineTest, MemoryTracking_Run) {
    if (folly::kIsSanitizeAddress || folly::kIsSanitizeThread) {
        // ASan/TSan perform their own interposing of global new / delete, which
        // means KV-Engine's memory tracking (which this test relies on)
        // doesn't work.
        GTEST_SKIP();
    }
    // Setup - create a second engine, so there are two buckets which
    // memory should be accounted to.
    auto secondEngine = SynchronousEPEngine::build(this->config +
                                                   ";couch_bucket=\"engine2\"");
    EventuallyPersistentEngine* engine1 = this->engine.get();
    EventuallyPersistentEngine* engine2 = secondEngine.get();

    // Task which:
    // - does nothing on first execution
    // - allocates memory on second execution
    // - deallocates memory on third execution.
    // Note it initially sleeps forever, must be explicitly woken to run.
    struct MemoryAllocTask : public GlobalTask {
        MemoryAllocTask(EventuallyPersistentEngine* engine)
            : GlobalTask(engine, TaskId::ItemPager, INT_MAX, false) {
        }

        std::string getDescription() const override {
            return "MemoryAllocTask";
        }
        std::chrono::microseconds maxExpectedDuration() const override {
            return std::chrono::microseconds(1);
        }

    protected:
        bool run() override {
            // Sleep until we are explicitly told to wake.
            this->snooze(INT_MAX);

            std::lock_guard<std::mutex> guard(cv);
            runCount++;
            if (runCount == 1) {
                // do nothing.
            } else if (runCount == 2) {
                payload.resize(4096);
            } else if (runCount == 3) {
                payload.resize(0);
                payload.shrink_to_fit();
            }
            cv.notify_one();
            return true;
        }

    public:
        // Used to guard updates to runCount / notfiy waiting test program.
        SyncObject cv;
        int runCount{0};
        // Some test data which is later expanded / shrunk when task runs.
        std::vector<char> payload;
    };

    // Disassociate current thread with any engine - we create various testing
    // objects which we don't want interfering with specific bucket memory.
    ObjectRegistry::onSwitchThread(nullptr);

    // Establish initial memory accounting of the bucket, including the
    // initial payload vector memory.
    auto getMemUsed = [](EventuallyPersistentEngine* engine) {
        return engine->getEpStats().getPreciseTotalMemoryUsed();
    };
    const auto memoryInitial1 = getMemUsed(engine1);
    const auto memoryInitial2 = getMemUsed(engine2);

    // Setup - create an instance of MemoryAllocTask for each bucket.
    auto task1 = [&] {
        BucketAllocationGuard guard(engine1);
        return std::make_shared<MemoryAllocTask>(engine1);
    }();

    auto task2 = [&] {
        BucketAllocationGuard guard(engine2);
        return std::make_shared<MemoryAllocTask>(engine2);
    }();

    // Sanity check - memory should have increased by at least
    // sizeof(MemoryAllocTask) for each engine.
    const auto memoryPostTaskCreate1 = getMemUsed(engine1);
    ASSERT_GE(memoryPostTaskCreate1 - memoryInitial1, sizeof(MemoryAllocTask))
            << "engine1 mem_used has not grown by at least sizeof(task) after "
               "creating it.";

    const auto memoryPostTaskCreate2 = getMemUsed(engine2);
    ASSERT_GE(memoryPostTaskCreate2 - memoryInitial2, sizeof(MemoryAllocTask))
            << "engine2 mem_used has not grown by at least sizeof(task) after "
               "creating it.";

    // Test 1: Schedule both tasks - memory usage shouldn't have changed - any
    // internal memory used by ExecutorPool to track task shouldn't be accounted
    // to bucket.
    const auto task1Id = ExecutorPool::get()->schedule(task1);
    const auto task2Id = ExecutorPool::get()->schedule(task2);
    const auto memoryPostTaskSchedule1 = getMemUsed(engine1);
    ASSERT_EQ(memoryPostTaskCreate1, memoryPostTaskSchedule1)
            << "engine1 mem_used has unexpectedly changed after calling "
               "ExecutorPool::schedule()";
    const auto memoryPostTaskSchedule2 = getMemUsed(engine2);
    ASSERT_EQ(memoryPostTaskCreate2, memoryPostTaskSchedule2)
            << "engine2 mem_used has unexpectedly changed after calling "
               "ExecutorPool::schedule()";

    // Test 2 - wake each buckets' MemoryAllocTask, then check mem_used has
    // not changed on first execution.
    ExecutorPool::get()->wake(task1Id);
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 1; });
    }
    ExecutorPool::get()->wake(task2Id);
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 1; });
    }
    const auto memoryPostTaskFirstRun1 = getMemUsed(engine1);
    EXPECT_EQ(memoryPostTaskFirstRun1, memoryPostTaskCreate1)
            << "engine1 mem_used has unexpectedly changed after running task "
               "once";
    const auto memoryPostTaskFirstRun2 = getMemUsed(engine2);
    EXPECT_GE(memoryPostTaskFirstRun2, memoryPostTaskCreate2)
            << "engine2 mem_used has unexpectedly changed after running task "
               "once";

    // Test 3 - wake each buckets' MemoryAllocTask, then check mem_used has
    // changed after second execution.
    ExecutorPool::get()->wake(task1Id);
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 2; });
    }
    ExecutorPool::get()->wake(task2Id);
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 2; });
    }
    const auto memoryPostTaskSecondRun1 = getMemUsed(engine1);
    EXPECT_GE(memoryPostTaskSecondRun1,
              memoryPostTaskCreate1 + task1->payload.size())
            << "engine1 mem_used has not grown by payload.size() after "
               "running task twice.";
    const auto memoryPostTaskSecondRun2 = getMemUsed(engine2);
    EXPECT_GE(memoryPostTaskSecondRun2,
              memoryPostTaskCreate2 + task2->payload.size())
            << "engine2 mem_used has not grown by payload.size() after "
               "running task twice.";

    // Test 4 - wake each task again, this time the memory usage should drop
    // back down.
    ExecutorPool::get()->wake(task1Id);
    {
        std::unique_lock<std::mutex> guard(task1->cv);
        task1->cv.wait(guard, [&task1] { return task1->runCount == 3; });
    }
    ExecutorPool::get()->wake(task2Id);
    {
        std::unique_lock<std::mutex> guard(task2->cv);
        task2->cv.wait(guard, [&task2] { return task2->runCount == 3; });
    }
    const auto memoryPostTaskThirdRun1 = getMemUsed(engine1);
    EXPECT_EQ(memoryPostTaskThirdRun1, memoryPostTaskCreate1)
            << "engine1 mem_used has not returned back to previous value after "
               "running task a third time.";
    const auto memoryPostTaskThirdRun2 = getMemUsed(engine2);
    EXPECT_EQ(memoryPostTaskThirdRun2, memoryPostTaskCreate2)
            << "engine2 mem_used has not returned back to previous value after "
               "running task a third time.";

    // test 5 - cancel each task; removing from ExecutorPool tracking. Memory
    // usage should decrease to original value before tasks created.

    // Given cancel is (partially) async for both ExecutorPools (task deletion
    // will happen on one of the background threads, need to poll until memory
    // usage returns to expected.
    task1.reset();
    ExecutorPool::get()->cancel(task1Id);
    size_t memoryPostCancel1;
    EXPECT_TRUE(waitForMemUsedToBe(*engine1, memoryInitial1, memoryPostCancel1))
            << "Exceeded wait time for memoryPostCancel1 (which is "
            << memoryPostCancel1 << ") to return to memoryInitial1 (which is "
            << memoryInitial1 << ")";

    task2.reset();
    ExecutorPool::get()->cancel(task2Id);
    size_t memoryPostCancel2;
    EXPECT_TRUE(waitForMemUsedToBe(*engine2, memoryInitial2, memoryPostCancel2))
            << "Exceeded wait time for memoryPostCancel2 (which is "
            << memoryPostCancel2 << ") to return to memoryInitial2 (which is "
            << memoryInitial2 << ")";
}

// Test that a task being cancelled during task stats being generated doesn't
// cause a memory accounting issue (due to the Task being deleted via
// doTasksStat.
TYPED_TEST(ExecutorPoolEpEngineTest, TaskStats_MemAccounting) {
    if (folly::kIsSanitizeAddress || folly::kIsSanitizeThread) {
        // ASan/TSan perform their own interposing of global new / delete, which
        // means KV-Engine's memory tracking (which this test relies on)
        // doesn't work.
        GTEST_SKIP();
    }

    // Disassociate current thread with any engine - we need to create various
    // testing object (GoogleMock objects, etc) which we don't want interfering
    // with specific bucket memory. This does require that we correctly switch
    // _to_ the engine where necessary.
    ObjectRegistry::onSwitchThread(nullptr);

    // Establish initial memory accounting of the bucket, including the
    // initial payload vector memory.
    auto getMemUsed = [](EventuallyPersistentEngine& engine) {
        return engine.getEpStats().getPreciseTotalMemoryUsed();
    };
    const auto memoryInitial = getMemUsed(*this->engine);

    // Setup - create an instance of a test task (with correct memory
    // accounting) and schedule it.
    auto task = [&] {
        BucketAllocationGuard guard(this->engine.get());
        return std::make_shared<LambdaTask>(
                this->engine.get(),
                TaskId::ItemPager,
                INT_MAX,
                false,
                [&](LambdaTask& task) { return false; });
    }();
    auto taskId = ExecutorPool::get()->schedule(std::move(task));

    // Sanity check - memory should have increased by at least
    // sizeof(LambdaTask) for the engine.
    const auto memoryPostTaskCreate = getMemUsed(*this->engine);
    ASSERT_GE(memoryPostTaskCreate - memoryInitial, sizeof(LambdaTask))
            << "engine1 mem_used has not grown by at least sizeof(task) after "
               "creating it.";

    // We need to call cancel() while doTasksStat is running, to end up with
    // the final reference to task being owned by doTaskStat's copy of all
    // tasks, so it is freed when doTasksStat returns.
    // To achive this setup a AddStat mock which calls cancel on the first
    // call to it (doTasksStat makes 3 calls to this).
    MockAddStat mockAddStat;
    auto& taskable = this->engine->getTaskable();
    EXPECT_CALL(mockAddStat, callback(_, _, _))
            .WillOnce([taskId](std::string, std::string, const void*) {
                ExecutorPool::get()->cancel(taskId);
            })
            .WillRepeatedly(Return());

    const auto memoryPostMock = getMemUsed(*this->engine);
    ASSERT_EQ(memoryPostTaskCreate, memoryPostMock);

    // Test: Call doTasksStat, which should cause the mock to cancel the
    // task while it's running. Then check memory usage is same as before
    // task created.
    MockCookie cookie;
    ExecutorPool::get()->doTasksStat(
            taskable, &cookie, mockAddStat.asStdFunction());

    // Poll for 10s waiting for memory usage to return to initial.
    size_t memoryPostCancel;
    EXPECT_TRUE(
            waitForMemUsedToBe(*this->engine, memoryInitial, memoryPostCancel))
            << "Exceeded wait time for memoryPostCancel (which is "
            << memoryPostCancel << ") to return to memoryInitial (which is "
            << memoryInitial << ")";
}

TYPED_TEST(ExecutorPoolEpEngineTest, PoolThreadsAreRegisteredWithPhosphor) {
    // Confirm executor pool threads record traces in phosphor

#if PHOSPHOR_DISABLED
    // if phosphor is disabled the test trace won't be recorded anyway
    GTEST_SKIP();
#endif

    EpEngineTaskable& taskable = this->engine->getTaskable();
    auto& pool = *ExecutorPool::get();

    auto& instance = phosphor::TraceLog::getInstance();
    instance.start(phosphor::TraceConfig::fromString(
            "buffer-mode:ring;buffer-size:4096;enabled-categories:*"));

    ThreadGate tg(1);

    // Task which just traces an event, then exits.
    ExTask task = std::make_shared<LambdaTask>(
            taskable, TaskId::ItemPager, 0, true, [&tg](LambdaTask&) -> bool {
                { TRACE_EVENT0("ep-engine/task", "TestTask"); }
                tg.threadUp();
                return false;
            });

    // schedule the task
    pool.schedule(task);

    // wait until the traced event has occurred (or timeout if not)

    tg.waitFor(std::chrono::seconds(5));

    EXPECT_TRUE(tg.isComplete());

    instance.stop();

    auto context = instance.getTraceContext();
    auto* buffer = context.getBuffer();
    ASSERT_NE(nullptr, buffer);
    for (const auto& event : *buffer) {
        if (event.getName() == std::string_view("TestTask")) {
            // success, trace was recorded from NonIO thread
            return;
        }
    }

    FAIL();
}
