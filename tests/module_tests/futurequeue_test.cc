/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <gtest/gtest.h>

#include "futurequeue.h"

class FutureQueueTest : public ::testing::TestWithParam<std::string> {
public:
    FutureQueue<> queue;
};

class TestTask : public GlobalTask {
public:
    TestTask(EventuallyPersistentEngine* e,
             TaskId id,
             int o = 0)
      : GlobalTask(e, id, 0.0, false),
        order(o) {}

    // returning true will also drive the ExecutorPool::reschedule path.
    bool run() { return true; }

    std::string getDescription() {
        return std::string("TestTask ") + GlobalTask::getTaskName(getTypeId());
    }

    int order;
};

TEST_F(FutureQueueTest, initAssumptions) {
    EXPECT_EQ(0, queue.size());
    EXPECT_TRUE(queue.empty());
}

TEST_F(FutureQueueTest, push1) {
    ExTask hpTask = new TestTask(nullptr,
                                 TaskId::PendingOpsNotification);

    queue.push(hpTask);
    EXPECT_EQ(1, queue.size());
    EXPECT_FALSE(queue.empty());

    EXPECT_EQ(TaskId::PendingOpsNotification, queue.top()->getTypeId());
}

TEST_F(FutureQueueTest, pushn) {
    ExTask hpTask = new TestTask(nullptr,
                                 TaskId::PendingOpsNotification);

    const int n = 10;
    for (int i = 0; i < n; i++) {
        queue.push(hpTask);
    }
    EXPECT_EQ(n, queue.size());
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(TaskId::PendingOpsNotification, queue.top()->getTypeId());
}

/*
 * Push n TestTask objects, each with an id of their push order but with
 * a decreasing waketime, i.e. last element pushed has the smallest wakeTime.
 */
TEST_F(FutureQueueTest, pushOrder) {
    const int n = 10;
    for (int i = 0; i <= n; i++) {
        ExTask hpTask;
        hpTask = new TestTask(nullptr,
                              TaskId::PendingOpsNotification,
                              i);
        hpTask->updateWaketime(hrtime_t(n - i));
        queue.push(hpTask);
    }

    // last task pushed must be the first one in the queue
    EXPECT_EQ(n, static_cast<TestTask*>(queue.top().get())->order);
}

/*
 * Push n TestTask objects, each with an id of their push order but with
 * a decreasing waketime.
 *
 * Then use the queue updateWake time to move a task to the front
 *
 */
TEST_F(FutureQueueTest, updateWaketime) {
    const int n = 10;
    ExTask middleTask;
    for (int i = 0; i <= n; i++) {
        ExTask hpTask;
        hpTask = new TestTask(nullptr,
                              TaskId::PendingOpsNotification,
                              i);
        hpTask->updateWaketime(hrtime_t((n*2) - i));
        queue.push(hpTask);

        if (i == n/2) {
            middleTask = hpTask;
        }
    }

    ASSERT_NE(nullptr, middleTask.get());

    // last task pushed must be the first one in the queue
    EXPECT_EQ(n, static_cast<TestTask*>(queue.top().get())->order);
    EXPECT_NE(static_cast<TestTask*>(middleTask.get())->order,
              static_cast<TestTask*>(queue.top().get())->order);

    // Now update the n/2 task's time and expect it to become the front task
    EXPECT_TRUE(queue.updateWaketime(middleTask, 0));

    // Now the middleTask is queue.top
    EXPECT_EQ(static_cast<TestTask*>(middleTask.get())->order,
              static_cast<TestTask*>(queue.top().get())->order);
}

/*
 * Push n TestTask objects, each with an id of their push order but with
 * a decreasing waketime.
 *
 * Then use the snooze method to move a task from the front
 *
 */
TEST_F(FutureQueueTest, snooze) {
    const int n = 10;

    for (int i = 0; i <= n; i++) {
        ExTask hpTask;
        hpTask = new TestTask(nullptr,
                              TaskId::PendingOpsNotification,
                              i);
        hpTask->updateWaketime(hrtime_t((n*2) - i));
        queue.push(hpTask);
    }

    // Now update the top task's time and expect it to become the last task
    // we can't see the back, so will pop/top all..
    int top = static_cast<TestTask*>(queue.top().get())->order;
    EXPECT_TRUE(queue.snooze(queue.top(), n*3));

    // The top task is not the old top
    EXPECT_NE(top,
              static_cast<TestTask*>(queue.top().get())->order);

    ExTask lastTask;
    while (!queue.empty()) {
        if (lastTask) {
            EXPECT_LT(lastTask->getWaketime(),
                      queue.top()->getWaketime());
        }
        lastTask = queue.top();
        queue.pop();
    }

    EXPECT_EQ(top, static_cast<TestTask*>(lastTask.get())->order);
}

/*
 * snooze/wake a task not in the queue, the queue is also empty.
 */
TEST_F(FutureQueueTest, taskNotInEmptyQueue) {
    ExTask task = new TestTask(nullptr, TaskId::PendingOpsNotification);

    hrtime_t wake = task->getWaketime();
    queue.snooze(task, 5.0);
    // snooze uses gethrtime so we'll only check that the tasks time changed.
    EXPECT_NE(wake, task->getWaketime());

    EXPECT_EQ(0, queue.size());
    EXPECT_TRUE(queue.empty());

    EXPECT_FALSE(queue.updateWaketime(task, 5));
    EXPECT_EQ(5, task->getWaketime());

    EXPECT_EQ(0, queue.size());
    EXPECT_TRUE(queue.empty());
}

/*
 * snooze/wake a task not in the queue
 */
TEST_F(FutureQueueTest, taskNotInQueue) {
    const int nTasks = 5;
    for (int ii = 1; ii < nTasks; ii++) {
        ExTask t = new TestTask(nullptr, TaskId::PendingOpsNotification);
        t->updateWaketime(1+ii);
        queue.push(t);
    }
    // Finally push a task with an obvious ID value of -1
    ExTask task = new TestTask(nullptr, TaskId::PendingOpsNotification, -1);
    task->updateWaketime(0);
    queue.push(task);

    // Now operate with a new task not in the queue
    task = new TestTask(nullptr, TaskId::PendingOpsNotification);
    hrtime_t wake = task->getWaketime();
    EXPECT_FALSE(queue.snooze(task, 5.0));

    // snooze uses gethrtime so we'll only check that the tasks time changed.
    EXPECT_NE(wake, task->getWaketime());

    EXPECT_EQ(nTasks, queue.size());
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(-1,
              static_cast<TestTask*>(queue.top().get())->order);

    EXPECT_FALSE(queue.updateWaketime(task, 5));
    EXPECT_EQ(5, task->getWaketime());

    EXPECT_EQ(nTasks, queue.size());
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(-1,
              static_cast<TestTask*>(queue.top().get())->order);
}
