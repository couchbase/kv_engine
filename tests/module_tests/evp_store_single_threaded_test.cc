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

// Check that in-progress disk backfills (`CouchKVStore::backfill`) are
// correctly deleted when we delete a bucket. If not then we leak vBucket file
// descriptors, which can prevent ns_server from cleaning up old vBucket files
// and consequently re-adding a node to the cluster.
//
TEST_F(SingleThreadedEPStoreTest, MB19892_BackfillNotDeleted) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Perform one SET, then close it's checkpoint. This means that we no
    // longer have all sequence numbers in memory checkpoints, forcing the
    // DCP stream request to go to disk (backfill).
    store_item(vbid, "key", "value");

    // Force a new checkpoint.
    auto vb = store->getVbMap().getBucket(vbid);
    auto& ckpt_mgr = vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    // Directly flush the vbucket, ensuring data is on disk.
    //  (This would normally also wake up the checkpoint remover task, but
    //   as that task was never registered with the ExecutorPool in this test
    //   environment, we need to manually remove the prev checkpoint).
    EXPECT_EQ(1, store->flushVBucket(vbid));

    bool new_ckpt_created;
    EXPECT_EQ(1,
              ckpt_mgr.removeClosedUnrefCheckpoints(vb, new_ckpt_created));

    // Create a DCP producer, and start a stream request.
    std::string name{"test_producer"};
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcpOpen(cookie, /*opaque:unused*/{}, /*seqno:unused*/{},
                              DCP_OPEN_PRODUCER, name.data(), name.size()));

    uint64_t rollbackSeqno;
    auto dummy_dcp_add_failover_cb = [](vbucket_failover_t* entry,
                                       size_t nentries, const void *cookie) {
        return ENGINE_SUCCESS;
    };

    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine.get()->dcp.stream_req(
                      &engine.get()->interface, cookie, /*flags*/0,
                      /*opaque*/0, /*vbucket*/vbid, /*start_seqno*/0,
                      /*end_seqno*/-1, /*vb_uuid*/0xabcd, /*snap_start*/0,
                      /*snap_end*/0, &rollbackSeqno,
                      dummy_dcp_add_failover_cb));
}

/*
 * Test that the DCP processor returns a 'yield' return code when
 * working on a large enough buffer size.
 */
TEST_F(SingleThreadedEPStoreTest, MB18452_yield_dcp_processor) {

    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Create a MockDcpConsumer
    dcp_consumer_t consumer = new MockDcpConsumer(*engine, cookie, "test");

    // Add the stream
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/0, vbid, /*flags*/0));

    // The processBufferedItems should yield every "yield * batchSize"
    // So add '(n * (yield * batchSize)) + 1' messages and we should see
    // processBufferedMessages return 'more_to_process' 'n' times and then
    // 'all_processed' once.
    const int n = 4;
    const int yield = engine->getConfiguration().getDcpConsumerProcessBufferedMessagesYieldLimit();
    const int batchSize = engine->getConfiguration().getDcpConsumerProcessBufferedMessagesBatchSize();
    const int messages = n * (batchSize * yield);

    // Force the stream to buffer rather than process messages immediately
    const ssize_t queueCap = engine->getEpStats().replicationThrottleWriteQueueCap;
    engine->getEpStats().replicationThrottleWriteQueueCap = 0;

    // 1. Add the first message, a snapshot marker.
    consumer->snapshotMarker(/*opaque*/1, vbid, /*startseq*/0,
                             /*endseq*/messages, /*flags*/0);

    // 2. Now add the rest as mutations.
    for (int ii = 0; ii <= messages; ii++) {
        std::string key = "key" + std::to_string(ii);
        std::string value = "value";
        consumer->mutation(/*opaque*/1, key.c_str(), key.length(),
                           value.c_str(), value.length(), /*cas*/0,
                           vbid, /*flags*/0, /*datatype*/0, /*locktime*/0,
                           /*bySeqno*/ii, /*revSeqno*/0, /*exptime*/0,
                           /*nru*/0, /*meta*/nullptr, /*nmeta*/0);
    }

    // Set the throttle back to the original value
    engine->getEpStats().replicationThrottleWriteQueueCap = queueCap;

    // Get our target stream ready.
    static_cast<MockDcpConsumer*>(consumer.get())->public_notifyVbucketReady(vbid);

    // 3. processBufferedItems returns more_to_process n times
    for (int ii = 0; ii < n; ii++) {
        EXPECT_EQ(more_to_process, consumer->processBufferedItems());
    }

    // 4. processBufferedItems returns a final all_processed
    EXPECT_EQ(all_processed, consumer->processBufferedItems());

    // Drop the stream
    consumer->closeStream(/*opaque*/0, vbid);
}
