/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <memory>
#include <daemon/executorpool.h>
#include <daemon/task.h>
#include <platform/backtrace.h>

class ExecutorTest : public ::testing::Test {
protected:
    void SetUp() {
        executorpool = new ExecutorPool(4);
    }

    void TearDown() {
        delete executorpool;
    }

    ExecutorPool* executorpool;
};

/**
 * Lets create a Task that just increments an integer and signals a
 * condition variable so that we can communicate with another thread.
 */
class BasicTestTask : public Task {
public:
    BasicTestTask(int max_)
        : Task(),
          runcount(0),
          max(max_),
          executionComplete(false) {
    }

    virtual ~BasicTestTask() {

    }

    virtual bool execute() override {
        ++runcount;
        if (runcount < max) {
            cond.notify_one();
            return false;
        }

        return true;
    }


    virtual void notifyExecutionComplete() override {
        executionComplete = true;
        cond.notify_one();
    }

    int runcount;
    int max;
    std::condition_variable cond;
    bool executionComplete;
};

TEST_F(ExecutorTest, SingleExecution) {
    BasicTestTask* cmd = new BasicTestTask(1);

    std::shared_ptr<Task> task(cmd);

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    cmd->cond.wait(lock);
    EXPECT_EQ(1, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

TEST_F(ExecutorTest, MultipleExecution) {
    BasicTestTask* cmd = new BasicTestTask(10);

    std::shared_ptr<Task> task(cmd);

    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    for (int ii = 1; ii < cmd->max; ++ii) {
        cmd->cond.wait(lock);
        EXPECT_EQ(ii, cmd->runcount);
        cmd->makeRunnable();
    }

    cmd->cond.wait(lock);
    EXPECT_EQ(cmd->max, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

TEST_F(ExecutorTest, ScheduleMissingLock) {
    BasicTestTask* cmd = new BasicTestTask(1);
    std::shared_ptr<Task> task(cmd);
    EXPECT_THROW(executorpool->schedule(task), std::logic_error);
}

TEST_F(ExecutorTest, RescheduleMissingLock) {
    BasicTestTask* cmd = new BasicTestTask(2);
    std::shared_ptr<Task> task(cmd);
    std::unique_lock<std::mutex> lock(task->getMutex());
    executorpool->schedule(task);

    cmd->cond.wait(lock);
    EXPECT_EQ(1, cmd->runcount);

    lock.unlock();
    EXPECT_THROW(cmd->makeRunnable(), std::logic_error);

    lock.lock();
    cmd->makeRunnable();
    cmd->cond.wait(lock);
    EXPECT_EQ(2, cmd->runcount);
    EXPECT_TRUE(cmd->executionComplete);
}

static std::terminate_handler default_terminate_handler;

static void my_terminate_handler() {
    char callstack[1024];

    if (print_backtrace_to_buffer("    ", callstack, sizeof(callstack))) {
        std::cerr << "*** Fatal error ***" << std::endl
                  << "Call stack:" << std::endl
                  << callstack;
    } else {
        std::cerr << "*** Falal error, but failed to grab callstack"
                  << std::endl;
    }

    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    _exit(EXIT_FAILURE);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Interpose our own C++ terminate handler to print backtrace upon failures
    default_terminate_handler = std::set_terminate(my_terminate_handler);

    return RUN_ALL_TESTS();
}
