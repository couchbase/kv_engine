/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
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

#include "evp_store_test.h"

#include "fakes/fake_executorpool.h"
#include "taskqueue.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_dcp_consumer.h"

/*
 * A subclass of EventuallyPersistentStoreTest which uses a fake ExecutorPool,
 * which will not spawn ExecutorThreads and hence not run any tasks
 * automatically in the background. All tasks must be manually run().
 */
class SingleThreadedEPStoreTest : public EventuallyPersistentStoreTest {
    void SetUp() {
        SingleThreadedExecutorPool::replaceExecutorPoolWithFake();
        EventuallyPersistentStoreTest::SetUp();

        task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
            (ExecutorPool::get());
    }

    void TearDown() {
        shutdownAndPurgeTasks();
        EventuallyPersistentStoreTest::TearDown();
    }
protected:

    /*
     * Run the next task from the taskQ
     * The task must match the expectedTaskName parameter
     */
    void runNextTask(TaskQueue& taskQ, const std::string& expectedTaskName) {
        CheckedExecutor executor(task_executor, taskQ);

        // Run the task
        executor.runCurrentTask(expectedTaskName);
    }

    /*
     * Run the next task from the taskQ
     */
    void runNextTask(TaskQueue& taskQ) {
        CheckedExecutor executor(task_executor, taskQ);

        // Run the task
        executor.runCurrentTask();
    }

    /*
     * Change the vbucket state and run the VBStatePeristTask
     * On return the state will be changed and the task completed.
     */
    void setVBucketStateAndRunPersistTask(uint16_t vbid, vbucket_state_t newState) {
        auto& lpWriterQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];

        // Change state - this should add 1 VBStatePersistTask to the WRITER queue.
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->setVBucketState(vbid, newState, /*transfer*/false));

        runNextTask(lpWriterQ, "Persisting a vbucket state for vbucket: "
                               + std::to_string(vbid));
    }

    /*
     * Set the stats isShutdown and attempt to drive all tasks to cancel
     */
    void shutdownAndPurgeTasks() {
        engine->getEpStats().isShutdown = true;
        task_executor->cancelAll();

        for (task_type_t t :
             {WRITER_TASK_IDX, READER_TASK_IDX, AUXIO_TASK_IDX, NONIO_TASK_IDX}) {

            // Define a lambda to drive all tasks from the queue, if hpTaskQ
            // is implemented then trivial to add a second call to runTasks.
            auto runTasks = [=](TaskQueue& queue) {
                while (queue.getFutureQueueSize() > 0 || queue.getReadyQueueSize()> 0){
                    runNextTask(queue);
                }
            };
            runTasks(*task_executor->getLpTaskQ()[t]);
        }
    }

    /*
     * Fake callback emulating dcp_add_failover_log
     */
    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(vbucket_failover_t* entry,
                                                   size_t nentries,
                                                   const void *cookie) {
        return ENGINE_SUCCESS;
    }

    SingleThreadedExecutorPool* task_executor;
};

/* Regression / reproducer test for MB-19695 - an exception is thrown
 * (and connection disconnected) if a couchstore file hasn't been re-created
 * yet when doTapVbTakeoverStats() is called as part of
 * tapNotify / TAP_OPAQUE_INITIAL_VBUCKET_STREAM.
 */
TEST_F(SingleThreadedEPStoreTest, MB19695_doTapVbTakeoverStats) {
    auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
        (ExecutorPool::get());

    // Should start with no tasks registered on any queues.
    for (auto& queue : task_executor->getLpTaskQ()) {
        ASSERT_EQ(0, queue->getFutureQueueSize());
        ASSERT_EQ(0, queue->getReadyQueueSize());
    }

    // [[1] Set our state to replica.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto& lpWriterQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    // [[2]] Perform a vbucket reset. This will perform some work synchronously,
    // but also schedules 3 tasks:
    //   1. vbucket memory deletion (NONIO)
    //   2. vbucket disk deletion (WRITER)
    //   3. VBStatePersistTask (WRITER)
    // MB-19695: If we try to get the number of persisted deletes between
    // tasks (2) and (3) running then an exception is thrown (and client
    // disconnected).
    EXPECT_TRUE(store->resetVBucket(vbid));

    runNextTask(lpNonioQ, "Removing (dead) vbucket 0 from memory");
    runNextTask(lpWriterQ, "Deleting VBucket:0");

    // [[2]] Ok, let's see if we can get TAP takeover stats. This will
    // fail with MB-19695.
    // Dummy callback to pass into the stats function below.
    auto dummy_cb = [](const char *key, const uint16_t klen,
                          const char *val, const uint32_t vlen,
                          const void *cookie) {};
    std::string key{"MB19695_doTapVbTakeoverStats"};
    EXPECT_NO_THROW(engine->public_doTapVbTakeoverStats
                    (nullptr, dummy_cb, key, vbid));

    // Also check DCP variant (MB-19815)
    EXPECT_NO_THROW(engine->public_doDcpVbTakeoverStats
                    (nullptr, dummy_cb, key, vbid));

    // Cleanup - run the 3rd task - VBStatePersistTask.
    runNextTask(lpWriterQ, "Persisting a vbucket state for vbucket: 0");
}

/*
 * Test that
 * 1. We cannot create a stream against a dead vb (MB-17230)
 * 2. No tasks are scheduled as a side-effect of the streamRequest attempt.
 */
TEST_F(SingleThreadedEPStoreTest, MB19428_no_streams_against_dead_vbucket) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, "key", "value");

    // Directly flush the vbucket
    EXPECT_EQ(1, store->flushVBucket(vbid));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead);
    auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
        (ExecutorPool::get());
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

    {
        // Create a Mock Dcp producer
        dcp_producer_t producer = new MockDcpProducer(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*notifyOnly*/false);

        // Creating a producer will schedule one ActiveStreamCheckpointProcessorTask
        // that task though sleeps forever, so won't run until woken.
        EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());

        uint64_t rollbackSeqno;
        auto err = producer->streamRequest(/*flags*/0,
                                           /*opaque*/0,
                                           /*vbucket*/vbid,
                                           /*start_seqno*/0,
                                           /*end_seqno*/-1,
                                           /*vb_uuid*/0xabcd,
                                           /*snap_start*/0,
                                           /*snap_end*/0,
                                           &rollbackSeqno,
                                           SingleThreadedEPStoreTest::fakeDcpAddFailoverLog);

        EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, err) << "Unexpected error code";

        // The streamRequest failed and should not of created anymore tasks.
        EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());
    }
}
