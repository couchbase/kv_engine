/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_item_freq_decayer.h"
#include "../mock/mock_stream.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/response.h"
#include "failover-table.h"
#include "replicationthrottle.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/test_task.h"
#include "vbucket.h"
#include <executor/executorpool.h>
#include <executor/globaltask.h>

TEST_F(SingleThreadedEPBucketTest, FlusherBatchSizeLimitLimitChange) {
    auto& bucket = getEPBucket();
    auto writers = ExecutorPool::get()->getNumWriters();

    // This is the default value
    ASSERT_EQ(4, writers);

    auto totalLimit = engine->getConfiguration().getFlusherTotalBatchLimit();

    auto expected = totalLimit / writers;
    EXPECT_EQ(expected, bucket.getFlusherBatchSplitTrigger());

    totalLimit = 40000;
    bucket.setFlusherBatchSplitTrigger(totalLimit);
    expected = totalLimit / writers;
    EXPECT_EQ(expected, bucket.getFlusherBatchSplitTrigger());
}

TEST_F(SingleThreadedEPBucketTest, FlusherBatchSizeLimitWritersChange) {
    auto& bucket = getEPBucket();
    auto writers = ExecutorPool::get()->getNumWriters();

    // This is the default value
    ASSERT_EQ(4, writers);

    auto totalLimit = engine->getConfiguration().getFlusherTotalBatchLimit();

    auto expected = totalLimit / writers;
    EXPECT_EQ(expected, bucket.getFlusherBatchSplitTrigger());

    engine->set_num_writer_threads(ThreadPoolConfig::ThreadCount(writers * 2));
    writers = ExecutorPool::get()->getNumWriters();

    expected = totalLimit / writers;
    EXPECT_EQ(expected, bucket.getFlusherBatchSplitTrigger());
}

/*
 * The following test checks to see if we call handleSlowStream when in a
 * backfilling state, but the backfillTask is not running, we
 * drop the existing cursor and set pendingBackfill to true.
 */
TEST_F(SingleThreadedEPBucketTest, MB22421_backfilling_but_task_finished) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*notifyOnly*/ false);
    // Create a Mock Active Stream
    auto mock_stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(engine.get()),
            producer,
            /*flags*/ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);

    mock_stream->transitionStateToBackfilling();
    ASSERT_TRUE(mock_stream->isInMemory())
            << "stream state should have transitioned to InMemory";
    // Have a persistence cursor and DCP cursor
    ASSERT_EQ(2, ckpt_mgr.getNumOfCursors());
    // Set backfilling task to true so can transition to Backfilling State
    mock_stream->public_setBackfillTaskRunning(true);
    mock_stream->transitionStateToBackfilling();
    ASSERT_TRUE(mock_stream->isBackfilling())
            << "stream state should not have transitioned to Backfilling";
    // Set backfilling task to false for test
    mock_stream->public_setBackfillTaskRunning(false);
    mock_stream->handleSlowStream();
    // The call to handleSlowStream should result in setting pendingBackfill
    // flag to true and the DCP cursor being dropped
    EXPECT_TRUE(mock_stream->public_getPendingBackfill());
    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/*
 * The following test checks to see if a cursor is re-registered after it is
 * dropped in handleSlowStream. In particular the test is for when
 * scheduleBackfill_UNLOCKED is called however the backfill task does not need
 * to be scheduled and therefore the cursor is not re-registered in
 * markDiskSnapshot.  The cursor must therefore be registered from within
 * scheduleBackfill_UNLOCKED.
 *
 * At the end of the test we should have 2 cursors: 1 persistence cursor and 1
 * DCP stream cursor.
 */
TEST_F(SingleThreadedEPBucketTest, MB22421_reregister_cursor) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    // Create a Mock Active Stream
    auto mock_stream = std::make_shared<MockActiveStream>(
            static_cast<EventuallyPersistentEngine*>(engine.get()),
            producer,
            /*flags*/ 0,
            /*opaque*/ 0,
            *vb,
            /*st_seqno*/ 0,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);

    mock_stream->transitionStateToBackfilling();
    EXPECT_TRUE(mock_stream->isInMemory())
            << "stream state should have transitioned to StreamInMemory";
    // Have a persistence cursor and DCP cursor
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());

    mock_stream->public_setBackfillTaskRunning(true);
    mock_stream->transitionStateToBackfilling();
    EXPECT_TRUE(mock_stream->isBackfilling())
            << "stream state should not have transitioned to StreamBackfilling";
    mock_stream->handleSlowStream();
    // The call to handleSlowStream should result in setting pendingBackfill
    // flag to true and the DCP cursor being dropped
    EXPECT_TRUE(mock_stream->public_getPendingBackfill());
    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

    mock_stream->public_setBackfillTaskRunning(false);

    // schedule a backfill
    mock_stream->next(*producer);
    // Calling scheduleBackfill_UNLOCKED(reschedule == true) will not actually
    // schedule a backfill task because backfillStart (is lastReadSeqno + 1) is
    // 1 and backfillEnd is 0, however the cursor still needs to be
    // re-registered.
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

TEST_F(SingleThreadedEPBucketTest, ReadyQueueMaintainsWakeTimeOrder) {
    class TestTask : public GlobalTask {
    public:
        TestTask(Taskable& t, TaskId id, double s) : GlobalTask(t, id, s) {
        }
        bool run() override {
            return false;
        }

        std::string getDescription() const override {
            return "Task uid:" + std::to_string(getId());
        }

        std::chrono::microseconds maxExpectedDuration() const override {
            return std::chrono::seconds(0);
        }
    };

    ExTask task1 = std::make_shared<TestTask>(
            engine->getTaskable(), TaskId::FlusherTask, 0);
    // Create one of our tasks with a negative wake time. This is equivalent
    // to scheduling the task then waiting 1 second, but our current test
    // CheckedExecutor doesn't deal with TimeTraveller and I don't want to add
    // sleeps to tests.
    ExTask task2 = std::make_shared<TestTask>(
            engine->getTaskable(), TaskId::FlusherTask, -1);

    task_executor->schedule(task1);
    task_executor->schedule(task2);

    // TEST
    // We expect task2 to run first because it should have an earlier wake time
    TaskQueue& lpWriteQ = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    runNextTask(lpWriteQ, "Task uid:" + std::to_string(task2->getId()));
    runNextTask(lpWriteQ, "Task uid:" + std::to_string(task1->getId()));
}

/*
 * Test that TaskQueue::wake results in a sensible ExecutorPool work count
 * Incorrect counting can result in the run loop spinning for many threads.
 */
TEST_F(SingleThreadedEPBucketTest, MB20235_wake_and_work_count) {
    class TestTask : public GlobalTask {
    public:
        TestTask(EventuallyPersistentEngine* e, double s)
            : GlobalTask(e, TaskId::AccessScanner, s) {
        }
        bool run() override {
            return false;
        }

        std::string getDescription() const override {
            return "Test MB20235";
        }

        std::chrono::microseconds maxExpectedDuration() const override {
            return std::chrono::seconds(0);
        }
    };

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

    // New task with a massive sleep
    ExTask task = std::make_shared<TestTask>(engine.get(), 99999.0);
    EXPECT_EQ(0, lpAuxioQ.getFutureQueueSize());

    // schedule the task, futureQueue grows
    task_executor->schedule(task);
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(), task_executor->getTotReadyTasks());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(),
              task_executor->getNumReadyTasksOfType(AUXIO_TASK_IDX));
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());

    // Wake task, but stays in futureQueue (fetch can now move it)
    task_executor->wake(task->getId());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(), task_executor->getTotReadyTasks());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(),
              task_executor->getNumReadyTasksOfType(AUXIO_TASK_IDX));
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ.getReadyQueueSize());

    runNextTask(lpAuxioQ);
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(), task_executor->getTotReadyTasks());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(),
              task_executor->getNumReadyTasksOfType(AUXIO_TASK_IDX));
    EXPECT_EQ(0, lpAuxioQ.getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ.getReadyQueueSize());
}

/*
 * Test that the DCP processor returns a 'yield' return code when
 * working on a large enough buffer size.
 */
TEST_F(SingleThreadedEPBucketTest, MB18452_yield_dcp_processor) {
    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Create a MockDcpConsumer
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "test");

    // Add the stream
    EXPECT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // The processBufferedItems should yield every "yield * batchSize"
    // So add '(n * (yield * batchSize)) + 1' messages and we should see
    // processBufferedMessages return 'more_to_process' 'n' times and then
    // 'all_processed' once.
    const int n = 4;
    const int yield =
            engine->getConfiguration()
                    .getDcpConsumerProcessBufferedMessagesYieldLimit();
    const int batchSize =
            engine->getConfiguration()
                    .getDcpConsumerProcessBufferedMessagesBatchSize();
    const int numItems = n * (batchSize * yield);

    // Force the stream to buffer rather than process messages immediately
    auto& stats = engine->getEpStats();
    stats.replicationThrottleThreshold = 0;
    ASSERT_EQ(ReplicationThrottle::Status::Pause,
              engine->getReplicationThrottle().getStatus());

    // 1. Add the first message, a snapshot marker.
    consumer->snapshotMarker(/*opaque*/ 1,
                             vbid,
                             /*startseq*/ 0,
                             /*endseq*/ numItems,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

    // 2. Now add the rest as mutations.
    for (int ii = 1; ii <= numItems; ii++) {
        const std::string key = "key" + std::to_string(ii);
        const DocKey docKey{key, DocKeyEncodesCollectionId::No};
        std::string value = "value";

        consumer->mutation(1 /*opaque*/,
                           docKey,
                           {(const uint8_t*)value.c_str(), value.length()},
                           0, // privileged bytes
                           PROTOCOL_BINARY_RAW_BYTES, // datatype
                           0, // cas
                           vbid, // vbucket
                           0, // flags
                           ii, // bySeqno
                           0, // revSeqno
                           0, // exptime
                           0, // locktime
                           {}, // meta
                           0); // nru
    }

    // Check that all items + snap-marker were buffered
    ASSERT_EQ(numItems + 1,
              consumer->getVbucketStream(vbid)->getNumBufferItems());
    // Unblock consumer
    stats.replicationThrottleThreshold = 99;

    // Get our target stream ready.
    static_cast<MockDcpConsumer*>(consumer.get())
            ->public_notifyVbucketReady(vbid);

    // 3. processBufferedItems returns more_to_process n times
    for (int ii = 0; ii < n; ii++) {
        EXPECT_EQ(more_to_process, consumer->processBufferedItems());
    }

    // 4. processBufferedItems returns a final all_processed
    EXPECT_EQ(all_processed, consumer->processBufferedItems());

    // Drop the stream
    consumer->closeStream(/*opaque*/ 0, vbid);
}

/*
 * MB-18953 is triggered by the executorpool wake path moving tasks directly
 * into the readyQueue, thus allowing for high-priority tasks to dominiate
 * a taskqueue.
 */
TEST_F(SingleThreadedEPBucketTest, MB18953_taskWake) {
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    ExTask hpTask = std::make_shared<TestTask>(engine->getTaskable(),
                                               TaskId::PendingOpsNotification);
    task_executor->schedule(hpTask);

    ExTask lpTask = std::make_shared<TestTask>(engine->getTaskable(),
                                               TaskId::DefragmenterTask);
    task_executor->schedule(lpTask);

    runNextTask(lpNonioQ,
                "TestTask PendingOpsNotification"); // hptask goes first
    // Ensure that a wake to the hpTask doesn't mean the lpTask gets ignored
    lpNonioQ.wake(hpTask);

    // Check 1 task is ready
    EXPECT_EQ(1, task_executor->getTotReadyTasks());
    EXPECT_EQ(1, task_executor->getNumReadyTasksOfType(NONIO_TASK_IDX));

    runNextTask(lpNonioQ, "TestTask DefragmenterTask"); // lptask goes second

    // Run the tasks again to check that coming from ::reschedule our
    // expectations are still met.
    runNextTask(lpNonioQ,
                "TestTask PendingOpsNotification"); // hptask goes first

    // Ensure that a wake to the hpTask doesn't mean the lpTask gets ignored
    lpNonioQ.wake(hpTask);

    // Check 1 task is ready
    EXPECT_EQ(1, task_executor->getTotReadyTasks());
    EXPECT_EQ(1, task_executor->getNumReadyTasksOfType(NONIO_TASK_IDX));
    runNextTask(lpNonioQ, "TestTask DefragmenterTask"); // lptask goes second
}

/*
 * MB-20735 waketime is not correctly picked up on reschedule
 */
TEST_F(SingleThreadedEPBucketTest, MB20735_rescheduleWaketime) {
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    class SnoozingTestTask : public TestTask {
    public:
        SnoozingTestTask(Taskable& t, TaskId id) : TestTask(t, id) {
        }

        bool run() override {
            snooze(0.1); // snooze for 100milliseconds only
            // Rescheduled to run 100 milliseconds later..
            return true;
        }
    };

    auto task = std::make_shared<SnoozingTestTask>(
            engine->getTaskable(), TaskId::PendingOpsNotification);
    ExTask hpTask = task;
    task_executor->schedule(hpTask);

    std::chrono::steady_clock::time_point waketime =
            runNextTask(lpNonioQ, "TestTask PendingOpsNotification");
    EXPECT_EQ(waketime, task->getWaketime())
            << "Rescheduled to much later time!";
}

/*
 * Tests that we stream from only active vbuckets for DCP clients with that
 * preference
 */
TEST_F(SingleThreadedEPBucketTest, stream_from_active_vbucket_only) {
    std::map<vbucket_state_t, bool> states;
    states[vbucket_state_active] = true; /* Positive test case */
    states[vbucket_state_replica] = false; /* Negative test case */
    states[vbucket_state_pending] = false; /* Negative test case */
    states[vbucket_state_dead] = false; /* Negative test case */

    for (auto& it : states) {
        setVBucketStateAndRunPersistTask(vbid, it.first);

        /* Create a Mock Dcp producer */
        auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                          cookie,
                                                          "test_producer",
                                                          /*flags*/ 0);

        /* Try to open stream on replica vb with
           DCP_ADD_STREAM_ACTIVE_VB_ONLY flag */
        uint64_t rollbackSeqno;
        auto err = producer->streamRequest(/*flags*/
                                           DCP_ADD_STREAM_ACTIVE_VB_ONLY,
                                           /*opaque*/ 0,
                                           /*vbucket*/ vbid,
                                           /*start_seqno*/ 0,
                                           /*end_seqno*/ -1,
                                           /*vb_uuid*/ 0,
                                           /*snap_start*/ 0,
                                           /*snap_end*/ 0,
                                           &rollbackSeqno,
                                           SingleThreadedEPBucketTest::
                                                   fakeDcpAddFailoverLog,
                                           {});

        if (it.second) {
            EXPECT_EQ(cb::engine_errc::success, err) << "Unexpected error code";
            producer->closeStream(/*opaque*/ 0, /*vbucket*/ vbid);
        } else {
            EXPECT_EQ(cb::engine_errc::not_my_vbucket, err)
                    << "Unexpected error code";
        }

        // Stop Producer checkpoint processor task
        producer->cancelCheckpointCreatorTask();
    }
}

// Test the item freq decayer task.  A mock version of the task is used,
// which has the ChunkDuration reduced to 0ms which mean as long as the
// number of documents is greater than
// ProgressTracker:INITIAL_VISIT_COUNT_CHECK the task will require multiple
// runs to complete.  If the task takes less than or more than two passes to
// complete then an error will be reported.
TEST_F(SingleThreadedEPBucketTest, ItemFreqDecayerTaskTest) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // ProgressTracker:INITIAL_VISIT_COUNT_CHECK = 100 and therefore
    // add 110 documents to the hash table to ensure all documents cannot be
    // visited in a single pass.
    for (uint32_t ii = 1; ii < 110; ii++) {
        auto key = makeStoredDocKey("DOC_" + std::to_string(ii));
        store_item(vbid, key, "value");
    }

    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    auto itemFreqDecayerTask =
            std::make_shared<MockItemFreqDecayerTask>(engine.get(), 50);

    EXPECT_EQ(0, lpNonioQ.getFutureQueueSize());
    task_executor->schedule(itemFreqDecayerTask);
    EXPECT_EQ(1, lpNonioQ.getFutureQueueSize());
    itemFreqDecayerTask->wakeup();

    EXPECT_FALSE(itemFreqDecayerTask->isCompleted());
    runNextTask(lpNonioQ, "Item frequency count decayer task");
    EXPECT_FALSE(itemFreqDecayerTask->isCompleted());
    runNextTask(lpNonioQ, "Item frequency count decayer task");
    // The item freq decayer task should have completed.
    EXPECT_TRUE(itemFreqDecayerTask->isCompleted());
}

// Test to confirm that the ItemFreqDecayerTask gets created on kv_bucket
// initialisation.  The task should be runnable.  However once run should
// enter a "snoozed" state.
TEST_F(SingleThreadedEPBucketTest, CreatedItemFreqDecayerTask) {
    store->initialize();
    EXPECT_FALSE(isItemFreqDecayerTaskSnoozed());
    store->runItemFreqDecayerTask();
    EXPECT_TRUE(isItemFreqDecayerTaskSnoozed());
}

TEST_F(SingleThreadedEPBucketTest, takeoverUnblockingRaceWhenBufferLogFull) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // 1) First store some keys which we will backfill
    std::array<std::string, 3> keys = {{"k1", "k2", "k3"}};
    for (const auto& key : keys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flush_vbucket_to_disk(vbid, keys.size());

    // Simplest way to ensure DCP has to do a backfill - 'wipe memory'
    resetEngineAndWarmup();

    // Setup DCP, 1 producer and we will do a takeover of the vbucket
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "takeoverBlocking",
                                                      /*flags*/ 0);

    producer->createCheckpointProcessorTask();

    MockDcpMessageProducers producers;

    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto mockStream = producer->mockActiveStreamRequest(
            DCP_ADD_STREAM_FLAG_TAKEOVER, // flags
            1, // opaque
            *vb,
            0, // start_seqno
            vb->getHighSeqno(), // end_seqno
            vb->failovers->getLatestUUID(),
            0, // snap_start_seqno
            vb->getHighSeqno() // snap_end_seqno
    );

    // Manually drive the backfill (not using notifyAndStepToCheckpoint)
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()

    // We special case takeoverStart being set to 0 (and time starts at 0 for
    // unit tests) so bump time to 1
    TimeTraveller offset(1);
    runNextTask(lpAuxioQ);

    // Now drain all items before we proceed to complete
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    for (const auto& key : keys) {
        EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(key, producers.last_key);
    }

    producers.last_op = cb::mcbp::ClientOpcode::Invalid;

    // Next the backfill should switch to takeover-send
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());
    auto* as0 = static_cast<ActiveStream*>(vb0Stream.get());
    EXPECT_TRUE(as0->isTakeoverSend());

    // Add some more keys because we don't want to run immediately through
    // takeover
    for (const auto& key : keys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flush_vbucket_to_disk(vbid, keys.size());

    // Sent all items in the readyQueue, but there are some in the checkpoint
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));

    // So run the checkpoint task to pull them into the readyQueue
    producer->getCheckpointSnapshotTask()->run();

    // Travel forward in time - want to set takeoverBackedUp to block front end
    // ops
    TimeTraveller t(engine->getConfiguration().getDcpTakeoverMaxTime() + 1);

    // Send the snapshot marker
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_TRUE(vb->isTakeoverBackedUp());

    // Send 3 items
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverSend());
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverSend());
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverSend());

    // Hitting waitForSnapshot, unblock it by "acking" from the consumer
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverSend());
    as0->snapshotMarkerAckReceived();

    // Shouldn't be able to store an item as takeover is blocked
    EXPECT_TRUE(vb->isTakeoverBackedUp());
    std::vector<cb::engine_errc> expected = {
            cb::engine_errc::temporary_failure};
    store_item(vbid, makeStoredDocKey("testest"), "val", 0, expected);

    // Hook to set buffer log size whilst in ActiveStream::takeoverSendPhase()
    // required as we check the capacity in DcpProducer::getNextItem() and would
    // otherwise not make it that far
    mockStream->setTakeoverSendPhaseHook([&]() {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(
                          1,
                          "connection_buffer_size",
                          std::to_string(producer->getBytesOutstanding())));
    });

    // Takeover still blocked, before the fix this would have reset
    // takeoverBackedUp
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverSend());
    EXPECT_TRUE(vb->isTakeoverBackedUp());
    store_item(vbid, makeStoredDocKey("testest"), "val", 0, expected);

    // Resize buffer log to unblock us
    EXPECT_EQ(cb::engine_errc::success,
              producer->control(
                      1,
                      "connection_buffer_size",
                      std::to_string(producer->getBytesOutstanding() * 2)));

    // Resetting hook to unblock buffer log check
    mockStream->setTakeoverSendPhaseHook([]() {});

    // If we hadn't notified the stream when the buffer log was full then this
    // would return would_block
    EXPECT_EQ(cb::engine_errc::success, producer->step(producers));
    EXPECT_TRUE(as0->isTakeoverWait());
    EXPECT_TRUE(vb->isTakeoverBackedUp());

    as0->setVBucketStateAckRecieved(*producer);

    EXPECT_EQ(vbucket_state_dead, vb->getState());
    EXPECT_FALSE(vb->isTakeoverBackedUp());
}

// Verify that handleResponse against an unknown stream returns true, MB-32724
// demonstrated a case where false will cause a failure.
TEST_F(SingleThreadedEPBucketTest, MB_32724) {
    auto p = std::make_shared<MockDcpProducer>(*engine, cookie, "mb-32724", 0);

    p->createCheckpointProcessorTask();

    MockDcpMessageProducers producers;

    cb::mcbp::Response message{};
    message.setMagic(cb::mcbp::Magic::ClientResponse);
    message.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    EXPECT_TRUE(p->handleResponse(message));
}

void SingleThreadedEPBucketTest::producerReadyQLimitOnBackfill(
        const BackfillBufferLimit limitType) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);

    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "test-producer", 0 /*flags*/, false /*startTask*/);

    auto stream = std::make_shared<MockActiveStream>(
            engine.get(),
            producer,
            DCP_ADD_STREAM_FLAG_DISKONLY /* flags */,
            0 /* opaque */,
            *vb);

    stream->transitionStateToBackfilling();
    size_t limit = 0;
    size_t valueSize = 0;
    switch (limitType) {
    case BackfillBufferLimit::StreamByte:
        limit = engine->getConfiguration().getDcpScanByteLimit();
        valueSize = 1024 * 1024;
        break;
    case BackfillBufferLimit::StreamItem:
        limit = engine->getConfiguration().getDcpScanItemLimit();
        // Note: I need to set a valueSize so that we don't reach the
        //     DcpScanByteLimit before the DcpScanItemLimit.
        //     Currently, byteLimit=4MB and itemLimit=4096.
        valueSize = 1;
        break;
    case BackfillBufferLimit::ConnectionByte:
        limit = engine->getConfiguration().getDcpBackfillByteLimit();
        // We want to test the connection-limit (currently max size for
        // buffer is 20MB). So, disable the stream-limits by setting high values
        // for maxBytes (1GB) and maxItems (1M)
        auto& scanBuffer = producer->public_getBackfillScanBuffer();
        scanBuffer.maxBytes = 1024 * 1024 * 1024;
        scanBuffer.maxItems = 1000000;
        valueSize = 1024 * 1024;
        break;
    }
    ASSERT_GT(limit, 0);
    ASSERT_GT(valueSize, 0);

    std::string value(valueSize, 'a');
    int64_t seqno = 1;
    int64_t expectedLastSeqno = seqno;
    bool ret = false;
    // Note: this loop would block forever (until timeout) if we don't enforce
    //     any limit on BackfillManager::scanBuffer
    do {
        auto item = std::make_unique<Item>(
                makeStoredDocKey("key_" + std::to_string(seqno)),
                0 /*flags*/,
                0 /*expiry*/,
                value.data(),
                value.size(),
                PROTOCOL_BINARY_RAW_BYTES,
                0 /*cas*/,
                seqno,
                stream->getVBucket());

        // Simulate the Cache/Disk callbacks here
        ret = stream->backfillReceived(std::move(item),
                                       backfill_source_t::BACKFILL_FROM_DISK);

        if (limitType == BackfillBufferLimit::ConnectionByte) {
            // Check that we are constantly well below the stream-limits.
            // We want to be sure that we are really hitting the
            // connection-limit here.
            auto& scanBuffer = producer->public_getBackfillScanBuffer();
            ASSERT_LT(scanBuffer.bytesRead, scanBuffer.maxBytes / 2);
            ASSERT_LT(scanBuffer.itemsRead, scanBuffer.maxItems / 2);
        }

        if (ret) {
            ASSERT_EQ(seqno, stream->public_readyQ().size());
            expectedLastSeqno = seqno;
            seqno++;
        } else {
            ASSERT_EQ(seqno - 1, stream->public_readyQ().size());
        }
    } while (ret);

    // Check that we have pushed some items to the Stream::readyQ
    auto lastSeqno = stream->getLastBackfilledSeqno();
    ASSERT_GT(lastSeqno, 1);
    ASSERT_EQ(lastSeqno, expectedLastSeqno);
    // Check that we have not pushed more than what expected given the limit.
    // Note: this logic applies to both BackfillScanLimit::byte and
    //     BackfillScanLimit::item
    const size_t upperBound = limit / valueSize + 1;
    ASSERT_LT(lastSeqno, upperBound);
}

EPBucket& SingleThreadedEPBucketTest::getEPBucket() {
    return dynamic_cast<EPBucket&>(*store);
}

/*
 * Test that an ActiveStream does not push items to Stream::readyQ
 * indefinitely as we enforce a stream byte-limit on backfill.
 */
TEST_F(SingleThreadedEPBucketTest, ProducerReadyQStreamByteLimitOnBackfill) {
    producerReadyQLimitOnBackfill(BackfillBufferLimit::StreamByte);
}

/*
 * Test that an ActiveStream does not push items to Stream::readyQ
 * indefinitely as we enforce a stream item-limit on backfill.
 */
TEST_F(SingleThreadedEPBucketTest, ProducerReadyQStreamItemLimitOnBackfill) {
    producerReadyQLimitOnBackfill(BackfillBufferLimit::StreamItem);
}

/*
 * Test that an ActiveStream does not push items to Stream::readyQ
 * indefinitely as we enforce a connection byte-limit on backfill.
 */
TEST_F(SingleThreadedEPBucketTest, ProducerReadyQConnectionLimitOnBackfill) {
    producerReadyQLimitOnBackfill(BackfillBufferLimit::ConnectionByte);
}

// MB-34850: Check that a consumer correctly handles (and ignores) stream-level
// messages (Mutation/Deletion/Prepare/Commit/Abort/...) received after
// CloseStream response but *before* the Producer sends STREAM_END.
TEST_F(SingleThreadedEPBucketTest,
       MB_34850_ConsumerRecvMessagesAfterCloseStream) {
    // Setup: Create replica VB and create stream for vbid.
    // Have the consumer receive a snapshot marker(1..10), and then close the
    // stream .
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "conn");
    consumer->enableV7DcpStatus();

    int opaque = 1;
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(opaque, vbid, /*flags*/ 0));
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       1,
                                       10,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
    ASSERT_EQ(cb::engine_errc::success, consumer->closeStream(opaque, vbid));

    // Test: Have the producer send further messages on the stream (before the
    // final STREAM_END. These should all be accepted (but discarded) by the
    // replica.
    auto testAllStreamLevelMessages = [&consumer, this, opaque](
                                              cb::engine_errc expected) {
        auto key = makeStoredDocKey("key");
        auto dtype = PROTOCOL_BINARY_RAW_BYTES;
        EXPECT_EQ(expected,
                  consumer->mutation(opaque,
                                     key,
                                     {},
                                     0,
                                     dtype,
                                     {},
                                     vbid,
                                     {},
                                     1,
                                     {},
                                     {},
                                     {},
                                     {},
                                     {}));

        EXPECT_EQ(expected,
                  consumer->deletion(
                          opaque, key, {}, 0, dtype, {}, vbid, 2, {}, {}));

        EXPECT_EQ(expected,
                  consumer->deletionV2(
                          opaque, key, {}, 0, dtype, {}, vbid, 3, {}, {}));

        EXPECT_EQ(expected,
                  consumer->expiration(
                          opaque, key, {}, 0, dtype, {}, vbid, 4, {}, {}));

        EXPECT_EQ(
                expected,
                consumer->setVBucketState(opaque, vbid, vbucket_state_active));
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        EXPECT_EQ(vbucket_state_replica, vb->getState());

        EXPECT_EQ(expected,
                  consumer->systemEvent(opaque,
                                        vbid,
                                        mcbp::systemevent::id::CreateCollection,
                                        5,
                                        mcbp::systemevent::version::version1,
                                        {},
                                        {}));

        EXPECT_EQ(expected,
                  consumer->prepare(opaque,
                                    key,
                                    {},
                                    0,
                                    dtype,
                                    {},
                                    vbid,
                                    {},
                                    6,
                                    {},
                                    {},
                                    {},
                                    {},
                                    {},
                                    cb::durability::Level::Majority));

        EXPECT_EQ(expected, consumer->commit(opaque, vbid, key, 6, 7));

        EXPECT_EQ(expected, consumer->abort(opaque, vbid, key, 6, 7));

        EXPECT_EQ(expected,
                  consumer->snapshotMarker(opaque,
                                           vbid,
                                           11,
                                           11,
                                           MARKER_FLAG_CHK,
                                           {} /*HCS*/,
                                           {} /*maxVisibleSeqno*/));
    };
    testAllStreamLevelMessages(cb::engine_errc::success);

    // Setup (phase 2): Receive a STREAM_END message - after which all of the
    // above stream-level messages should be rejected as ENOENT.
    ASSERT_EQ(cb::engine_errc::success,
              consumer->streamEnd(
                      opaque, vbid, cb::mcbp::DcpStreamEndStatus::Closed));

    // Test (phase 2): Have the producer send all the above stream-level
    // messages to the consumer. Should all be rejected this time.
    testAllStreamLevelMessages(cb::engine_errc::stream_not_found);
}

// MB-34951: Check that a consumer correctly handles (and ignores) a StreamEnd
// request from the producer if it has already created a new stream (for the
// same vb) with a different opaque.
TEST_F(SingleThreadedEPBucketTest,
       MB_34951_ConsumerRecvStreamEndAfterAddStream) {
    // Setup: Create replica VB and create stream for vbid, then close it
    // and add another stream (same vbid).
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "conn");
    const int opaque1 = 1;
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(opaque1, vbid, {}));
    ASSERT_EQ(cb::engine_errc::success, consumer->closeStream(opaque1, vbid));
    const int opaque2 = 2;
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(opaque2, vbid, {}));

    // Test: Have the producer send a StreamEnd with the "old" opaque.
    EXPECT_EQ(cb::engine_errc::success,
              consumer->streamEnd(
                      opaque1, vbid, cb::mcbp::DcpStreamEndStatus::Closed));
}

TEST_F(SingleThreadedEPBucketTest, TestConsumerSendEEXISTSIfOpaqueWrong) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "conn");

    const int opaque1 = 1;
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(opaque1, vbid, {}));
    ASSERT_EQ(cb::engine_errc::success, consumer->closeStream(opaque1, vbid));

    const int opaque2 = 2;
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(opaque2, vbid, {}));

    ASSERT_EQ(cb::engine_errc::success,
              consumer->closeStream(opaque1, vbid, {}));

    auto key = makeStoredDocKey("key");
    auto dtype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              consumer->prepare(opaque1,
                                key,
                                {},
                                0,
                                dtype,
                                {},
                                vbid,
                                {},
                                6,
                                {},
                                {},
                                {},
                                {},
                                {},
                                cb::durability::Level::Majority));
}
