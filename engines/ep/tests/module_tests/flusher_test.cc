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
#include "item.h"
#include "test_helpers.h"

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
                          ->getFlusherNonConst(vbid0);

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

    const Vbid vbid0 = Vbid(0);
};

// Regression test for MB-36380 - if the Flusher receives a wakeup for a vBucket
// between calculating if it can sleep and actually calling snooze(), then the
// wakeup is lost.
TEST_F(FlusherTest, MissingWakeupBeforeSnooze) {
    // Setup: Mark a vBucket as active, and notify a pending mutation on a
    // vBucket.
    engine->getKVBucket()->setVBucketState(vbid0, vbucket_state_active);

    // Setup: Set the testing hook in Flusher::step() to trigger another notify
    // event just before we have decided to snooze (but before snooze() is
    // actually called).
    flusher->stepPreSnoozeHook = [this]() {
        this->flusher->notifyFlushEvent(vbid0);
    };

    // Test: Run the flusher task. This should flush the oustanding setVBstate
    // for vBucket 0, but should _also_ re-schedule the Flusher to run a second
    // time given a new FlushEvent was scheduled just after Flusher::flushVB
    // completed (via stepPreSnoozeHook).
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);

    // Check the Flusher is indeed ready to run a second time.
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
}

/**
 * MB-37332: Test for MB-37294.
 * A pending entry in the Flusher::hpVbs prevents any low-priority
 * vbucket from being flushed.
 */
TEST_F(FlusherTest, GetToLowPrioWhenSomeHighPriIsPending) {
    const auto hpVbid = vbid0;
    // We might have our vBuckets split across shards and we need two on the
    // same shard. The distribution algorithm is a simple modulus so we can just
    // pick the vBucket with id == number of shards and vBucket 0.
    auto shards = engine->getKVBucket()->getVBuckets().getNumShards();
    const auto lpVbid = Vbid(shards);

    auto kvBucket = engine->getKVBucket();
    ASSERT_TRUE(kvBucket);
    kvBucket->setVBucketState(hpVbid, vbucket_state_replica);
    kvBucket->setVBucketState(lpVbid, vbucket_state_replica);

    // SetVBucketState notifies the flusher but we don't want to consider that
    // in this test so just run the flusher to clear the queue to set up for the
    // rest of the test
    while (flusher->getLPQueueSize() != 0) {
        task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
    }
    ASSERT_EQ(0, flusher->getLPQueueSize());

    // Ensure that the 2 vbuckets are managed under the same shard/flusher
    ASSERT_EQ(flusher,
              dynamic_cast<MockEPBucket*>(engine->getKVBucket())
                      ->getFlusherNonConst(lpVbid));

    // Simulate a SEQNO_PERSISTENCE request. Note that hpVBucket is empty.
    auto hpVBucket = engine->getVBucket(hpVbid);
    ASSERT_EQ(0, hpVBucket->getHighSeqno());
    ASSERT_EQ(0, flusher->getHighPriorityCount());
    hpVBucket->checkAddHighPriorityVBEntry(
            1 /*seqno*/, nullptr /*cookie*/, HighPriorityVBNotify::Seqno);
    ASSERT_EQ(1, flusher->getHighPriorityCount());

    // Run the flusher
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
    // hpVBucket is still in high-priority queue as we have never
    // received and flushed any seqno:1
    ASSERT_EQ(1, flusher->getHighPriorityCount());

    // Receive an item for the low-priority vbucket
    ASSERT_EQ(0, flusher->getLPQueueSize());
    auto item = make_item(lpVbid, makeStoredDocKey("key"), "value");
    item.setCas();
    uint64_t seqno;
    // Simulate PassiveStream::processMessage
    ASSERT_EQ(ENGINE_SUCCESS,
              kvBucket->setWithMeta(item,
                                    0 /*cas*/,
                                    &seqno,
                                    nullptr /*cookie*/,
                                    {vbucket_state_active,
                                     vbucket_state_replica,
                                     vbucket_state_pending},
                                    CheckConflicts::No,
                                    /*allowExisting*/ true));
    ASSERT_EQ(1, flusher->getLPQueueSize());

    // Run the FLusher again, should drain the low-priority queue
    task_executor->runNextTask(WRITER_TASK_IDX, flusherName);
    ASSERT_EQ(0, flusher->getLPQueueSize());
}