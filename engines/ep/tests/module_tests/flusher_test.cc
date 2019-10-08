/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "fakes/fake_executorpool.h"
#include "flusher.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <engines/ep/src/bucket_logger.h>
#include <folly/portability/GTest.h>
#include <programs/engine_testapp/mock_server.h>

class FlusherTest : public ::testing::Test {
protected:
    void SetUp() override {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        engine = SynchronousEPEngine::build({});
        task_executor = reinterpret_cast<SingleThreadedExecutorPool*>(
                ExecutorPool::get());

        flusher = dynamic_cast<MockEPBucket*>(engine->getKVBucket())
                          ->getFlusherNonConst(Vbid(0));

        // Create flusher and advance to running state.
        flusher->start();
        task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
    }

    void TearDown() override {
        // Cleanup; stop flusher and run it once to have it complete.
        flusher->stop();
        task_executor->runNextTask(WRITER_TASK_IDX, flusherName);

        destroy_mock_event_callbacks();
        engine.reset();
        ExecutorPool::shutdown();
    }

    SynchronousEPEngineUniquePtr engine;

    // Non-owning poitner to SingleThreadedExecutorPool.
    SingleThreadedExecutorPool* task_executor;

    // Non-owning pointer to first shard's flusher.
    Flusher* flusher;

    static constexpr const char* flusherName =
            "Running a flusher loop: shard 0";
};

// Regression test for MB-36380 - if the Flusher receives a wakeup for a vBucket
// between calculating if it can sleep and actually calling snooze(), then the
// wakeup is lost.
TEST_F(FlusherTest, DISABLED_MissingWakeupBeforeSnooze) {
    // Setup: Mark a vBucket as active, and notify a pending mutation on a
    // vBucket.
    engine->getKVBucket()->setVBucketState(Vbid(0), vbucket_state_active);
    flusher->notifyFlushEvent();

    // Setup: Set the testing hook in Flusher::step() to trigger another notify
    // event just before we have decided to snooze (but before snooze() is
    // actually called).
    flusher->stepPreSnoozeHook = [this]() {
        this->flusher->notifyFlushEvent();
    };

    // Test: Run the flusher task. This should flush the oustanding setVBstate
    // for vBucket 0, but should _also_ re-schedule the Flusher to run a second
    // time given a new FlushEvent was scheduled just after Flusher::flushVB
    // completed (via stepPreSnoozeHook).
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);

    // Check the Flusher is indeed ready to run a second time.
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
}
