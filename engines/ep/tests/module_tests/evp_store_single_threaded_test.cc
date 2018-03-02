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

#include "evp_store_single_threaded_test.h"

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_global_task.h"
#include "../mock/mock_item_freq_decayer.h"
#include "../mock/mock_stream.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "dcp/dcpconnmap.h"
#include "ep_time.h"
#include "evp_store_test.h"
#include "fakes/fake_executorpool.h"
#include "item_freq_decayer_visitor.h"
#include "programs/engine_testapp/mock_server.h"
#include "taskqueue.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/test_task.h"

#include <libcouchstore/couch_db.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <thread>

ProcessClock::time_point SingleThreadedKVBucketTest::runNextTask(
        TaskQueue& taskQ, const std::string& expectedTaskName) {
    CheckedExecutor executor(task_executor, taskQ);

    // Run the task
    executor.runCurrentTask(expectedTaskName);
    return executor.completeCurrentTask();
}

ProcessClock::time_point SingleThreadedKVBucketTest::runNextTask(TaskQueue& taskQ) {
    CheckedExecutor executor(task_executor, taskQ);

    // Run the task
    executor.runCurrentTask();
    return executor.completeCurrentTask();
}

void SingleThreadedKVBucketTest::SetUp() {
    SingleThreadedExecutorPool::replaceExecutorPoolWithFake();

    // Disable warmup - we don't want to have to run/wait for the Warmup tasks
    // to complete (and there's nothing to warmup from anyways).
    if (!config_string.empty()) {
        config_string += ";";
    }
    config_string += "warmup=false";

    KVBucketTest::SetUp();

    task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
    (ExecutorPool::get());
}

void SingleThreadedKVBucketTest::TearDown() {
    shutdownAndPurgeTasks();
    KVBucketTest::TearDown();
}

void SingleThreadedKVBucketTest::setVBucketStateAndRunPersistTask(uint16_t vbid,
                                                                 vbucket_state_t
                                                                 newState) {
    // Change state - this should add 1 set_vbucket_state op to the
    //VBuckets' persistence queue.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, newState, /*transfer*/false));

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Trigger the flusher to flush state to disk.
        auto& ep = dynamic_cast<EPBucket&>(*store);
        EXPECT_EQ(std::make_pair(false, size_t(0)), ep.flushVBucket(vbid));
    }
}

void SingleThreadedKVBucketTest::shutdownAndPurgeTasks() {
    engine->getEpStats().isShutdown = true;
    task_executor->cancelAndClearAll();

    for (task_type_t t :
         {WRITER_TASK_IDX, READER_TASK_IDX, AUXIO_TASK_IDX, NONIO_TASK_IDX}) {

        // Define a lambda to drive all tasks from the queue, if hpTaskQ
        // is implemented then trivial to add a second call to runTasks.
        auto runTasks = [=](TaskQueue& queue) {
            while (queue.getFutureQueueSize() > 0 || queue.getReadyQueueSize() > 0) {
                runNextTask(queue);
            }
        };
        runTasks(*task_executor->getLpTaskQ()[t]);
        task_executor->stopTaskGroup(engine->getTaskable().getGID(), t,
                                     engine->getEpStats().forceShutdown);
    }
}

void SingleThreadedKVBucketTest::cancelAndPurgeTasks() {
    task_executor->cancelAll();
    for (task_type_t t :
        {WRITER_TASK_IDX, READER_TASK_IDX, AUXIO_TASK_IDX, NONIO_TASK_IDX}) {

        // Define a lambda to drive all tasks from the queue, if hpTaskQ
        // is implemented then trivial to add a second call to runTasks.
        auto runTasks = [=](TaskQueue& queue) {
            while (queue.getFutureQueueSize() > 0 || queue.getReadyQueueSize() > 0) {
                runNextTask(queue);
            }
        };
        runTasks(*task_executor->getLpTaskQ()[t]);
        task_executor->stopTaskGroup(engine->getTaskable().getGID(), t,
                                     engine->getEpStats().forceShutdown);
    }
}

void SingleThreadedKVBucketTest::runReadersUntilWarmedUp() {
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    while (engine->getKVBucket()->isWarmingUp()) {
        runNextTask(readerQueue);
    }
}

/**
 * Destroy engine and replace it with a new engine that can be warmed up.
 * Finally, run warmup.
 */
void SingleThreadedKVBucketTest::resetEngineAndWarmup(std::string new_config) {
    shutdownAndPurgeTasks();
    std::string config = config_string;

    // check if warmup=false needs replacing with warmup=true
    size_t pos;
    std::string warmupT = "warmup=true";
    std::string warmupF = "warmup=false";
    if ((pos = config.find(warmupF)) != std::string::npos) {
        config.replace(pos, warmupF.size(), warmupT);
    } else {
        config += warmupT;
    }

    if (new_config.length() > 0) {
        config += ";";
        config += new_config;
    }

    reinitialise(config);

    engine->getKVBucket()->initializeWarmupTask();
    engine->getKVBucket()->startWarmupTask();

    // Now get the engine warmed up
    runReadersUntilWarmedUp();
}

std::shared_ptr<MockDcpProducer> SingleThreadedKVBucketTest::createDcpProducer(
        const void* cookie,
        const std::string& filter,
        bool dcpCollectionAware,
        IncludeDeleteTime deleteTime) {
    int flags = DCP_OPEN_INCLUDE_XATTRS;
    if (dcpCollectionAware) {
        flags |= DCP_OPEN_COLLECTIONS;
    }
    if (deleteTime == IncludeDeleteTime::Yes) {
        flags |= DCP_OPEN_INCLUDE_DELETE_TIMES;
    }
    auto newProducer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            flags,
            cb::const_byte_buffer(
                    reinterpret_cast<const uint8_t*>(filter.data()),
                    filter.size()),
            false /*startTask*/);

    // Create the task object, but don't schedule
    newProducer->createCheckpointProcessorTask();

    // Need to enable NOOP for XATTRS (and collections).
    newProducer->setNoopEnabled(true);

    return newProducer;
}

extern uint8_t dcp_last_op;
void SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(
        MockDcpProducer& producer,
        dcp_message_producers& producers,
        cb::mcbp::ClientOpcode expectedOp,
        bool fromMemory) {
    auto vb = store->getVBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    if (fromMemory) {
        producer.notifySeqnoAvailable(vbid, vb->getHighSeqno());
        runCheckpointProcessor(producer, producers);
    } else {
        // Run a backfill
        auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
        // backfill:create()
        runNextTask(lpAuxioQ);
        // backfill:scan()
        runNextTask(lpAuxioQ);
        // backfill:complete()
        runNextTask(lpAuxioQ);
        // backfill:finished()
        runNextTask(lpAuxioQ);
    }

    // Next step which will process a snapshot marker and then the caller
    // should now be able to step through the checkpoint
    if (expectedOp != cb::mcbp::ClientOpcode::Invalid) {
        EXPECT_EQ(ENGINE_WANT_MORE, producer.step(&producers));
        EXPECT_EQ(uint8_t(expectedOp), dcp_last_op);
    } else {
        EXPECT_EQ(ENGINE_SUCCESS, producer.step(&producers));
    }
}

void SingleThreadedKVBucketTest::runCheckpointProcessor(
        MockDcpProducer& producer, dcp_message_producers& producers) {
    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_SUCCESS, producer.step(&producers));

    EXPECT_EQ(1, producer.getCheckpointSnapshotTask().queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP
    // stream
    producer.getCheckpointSnapshotTask().run();
}

static ENGINE_ERROR_CODE dcpAddFailoverLog(vbucket_failover_t* entry,
                                           size_t nentries,
                                           gsl::not_null<const void*> cookie) {
    return ENGINE_SUCCESS;
}
void SingleThreadedKVBucketTest::createDcpStream(MockDcpProducer& producer) {
    uint64_t rollbackSeqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer.streamRequest(0, // flags
                                     1, // opaque
                                     vbid,
                                     0, // start_seqno
                                     ~0ull, // end_seqno
                                     0, // vbucket_uuid,
                                     0, // snap_start_seqno,
                                     0, // snap_end_seqno,
                                     &rollbackSeqno,
                                     &dcpAddFailoverLog));
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
     auto& ckpt_mgr = *vb->checkpointManager;

     // Create a Mock Dcp producer
     auto producer = std::make_shared<MockDcpProducer>(
             *engine,
             cookie,
             "test_producer",
             /*notifyOnly*/ false,
             cb::const_byte_buffer() /*no json*/);
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
    auto& ckpt_mgr = *vb->checkpointManager;

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);
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

    //schedule a backfill
    mock_stream->next();
    // Calling scheduleBackfill_UNLOCKED(reschedule == true) will not actually
    // schedule a backfill task because backfillStart (is lastReadSeqno + 1) is
    // 1 and backfillEnd is 0, however the cursor still needs to be
    // re-registered.
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/*
 * The following test checks to see if data is lost after a cursor is
 * re-registered after being dropped.
 *
 * It first sets-up an active stream associated with the active vbucket 0.  We
 * then move the stream into a StreamInMemory state, which results in creating
 * a DCP cursor (in addition to the persistence cursor created on construction
 * of the stream).
 *
 * We then add two documents closing the previous checkpoint and opening a new
 * one after each add.  This means that after adding 2 documents we have 3
 * checkpoints, (and 2 cursors).
 *
 * We then call handleSlowStream which results in the DCP cursor being dropped,
 * the steam being moved into the StreamBackfilling state and, the
 * pendingBackfill flag being set.
 *
 * As the DCP cursor is dropped we can remove the first checkpoint which the
 * persistence cursor has moved past.  As the DCP stream no longer has its own
 * cursor it will use the persistence cursor.  Therefore we need to schedule a
 * backfill task, which clears the pendingBackfill flag.
 *
 * The key part of the test is that we now move the persistence cursor on by
 * adding two more documents, and again closing the previous checkpoint and
 * opening a new one after each add.
 *
 * Now that the persistence cursor has moved on we can remove the earlier
 * checkpoints.
 *
 * We now run the backfill task that we scheduled for the active stream.
 * And the key result of the test is whether it backfills all 4 documents.
 * If it does then we have demonstrated that data is not lost.
 *
 */

// This callback function is called every time a backfill is performed on
// test MB22960_cursor_dropping_data_loss.
void MB22960callbackBeforeRegisterCursor(
        EPBucket* store,
        MockActiveStreamWithOverloadedRegisterCursor& mock_stream,
        VBucketPtr vb,
        size_t& registerCursorCount) {
    EXPECT_LE(registerCursorCount, 1);
    // The test performs two backfills, and the callback is only required
    // on the first, so that it can test what happens when checkpoints are
    // moved forward during a backfill.
    if (registerCursorCount == 0) {
        bool new_ckpt_created;
        CheckpointManager& ckpt_mgr = *vb->checkpointManager;

        //pendingBackfill has now been cleared
        EXPECT_FALSE(mock_stream.public_getPendingBackfill())
                << "pendingBackfill is not false";
        // we are now in backfill mode
        EXPECT_TRUE(mock_stream.public_isBackfillTaskRunning())
                << "isBackfillRunning is not true";

        // This method is bypassing store->set to avoid a test only lock
        // inversion with collections read locks
        queued_item qi1(new Item(makeStoredDocKey("key3"),
                                 0,
                                 0,
                                 "v",
                                 1,
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 vb->getId()));

        // queue an Item and close previous checkpoint
        vb->checkpointManager->queueDirty(*vb,
                                          qi1,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*preLinkDocCtx*/ nullptr);

        EXPECT_EQ(std::make_pair(false, size_t(1)),
                  store->flushVBucket(vb->getId()));
        ckpt_mgr.createNewCheckpoint();
        EXPECT_EQ(3, ckpt_mgr.getNumCheckpoints());
        EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

        // Now remove the earlier checkpoint
        EXPECT_EQ(1, ckpt_mgr.removeClosedUnrefCheckpoints(
                *vb, new_ckpt_created));
        EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());
        EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

        queued_item qi2(new Item(makeStoredDocKey("key3"),
                                 0,
                                 0,
                                 "v",
                                 1,
                                 PROTOCOL_BINARY_RAW_BYTES,
                                 0,
                                 -1,
                                 vb->getId()));

        // queue an Item and close previous checkpoint
        vb->checkpointManager->queueDirty(*vb,
                                          qi2,
                                          GenerateBySeqno::Yes,
                                          GenerateCas::Yes,
                                          /*preLinkDocCtx*/ nullptr);

        EXPECT_EQ(std::make_pair(false, size_t(1)),
                  store->flushVBucket(vb->getId()));
        ckpt_mgr.createNewCheckpoint();
        EXPECT_EQ(3, ckpt_mgr.getNumCheckpoints());
        EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

        // Now remove the earlier checkpoint
        EXPECT_EQ(1, ckpt_mgr.removeClosedUnrefCheckpoints(
                *vb, new_ckpt_created));
        EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());
        EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());
    }
}

TEST_F(SingleThreadedEPBucketTest, MB22960_cursor_dropping_data_loss) {
    // Records the number of times ActiveStream::registerCursor is invoked.
    size_t registerCursorCount = 0;
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr = *vb->checkpointManager;
    EXPECT_EQ(1, ckpt_mgr.getNumCheckpoints());
    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    // Since we are creating a mock active stream outside of
    // DcpProducer::streamRequest(), and we want the checkpt processor task,
    // create it explicitly here
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a Mock Active Stream
    auto mock_stream =
            std::make_shared<MockActiveStreamWithOverloadedRegisterCursor>(
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

    auto& mockStreamObj = *mock_stream;
    mock_stream->setCallbackBeforeRegisterCursor(
            [this, &mockStreamObj, vb, &registerCursorCount]() {
                MB22960callbackBeforeRegisterCursor(
                        &getEPBucket(), mockStreamObj, vb, registerCursorCount);
            });

    mock_stream->setCallbackAfterRegisterCursor(
            [&mock_stream, &registerCursorCount]() {
                // This callback is called every time a backfill is performed.
                // It is called immediately after completing
                // ActiveStream::registerCursor.
                registerCursorCount++;
                if (registerCursorCount == 1) {
                    EXPECT_TRUE(mock_stream->public_getPendingBackfill());
                } else {
                    EXPECT_EQ(2, registerCursorCount);
                    EXPECT_FALSE(mock_stream->public_getPendingBackfill());
                }
            });

    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());
    mock_stream->transitionStateToBackfilling();
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());
    // When we call transitionStateToBackfilling going from a StreamPending
    // state to a StreamBackfilling state, we end up calling
    // scheduleBackfill_UNLOCKED and as no backfill is required we end-up in a
    // StreamInMemory state.
    EXPECT_TRUE(mock_stream->isInMemory())
        << "stream state should have transitioned to StreamInMemory";

    store_item(vbid, makeStoredDocKey("key1"), "value");
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    ckpt_mgr.createNewCheckpoint();
    EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());

    store_item(vbid, makeStoredDocKey("key2"), "value");
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    ckpt_mgr.createNewCheckpoint();
    EXPECT_EQ(3, ckpt_mgr.getNumCheckpoints());

    // can't remove checkpoint because of DCP stream.
    bool new_ckpt_created;
    EXPECT_EQ(0, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());

    mock_stream->handleSlowStream();

    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());
    EXPECT_TRUE(mock_stream->isInMemory())
        << "stream state should not have changed";
    EXPECT_TRUE(mock_stream->public_getPendingBackfill())
        << "pendingBackfill is not true";
    EXPECT_EQ(3, ckpt_mgr.getNumCheckpoints());

    // Because we dropped the cursor we can now remove checkpoint
    EXPECT_EQ(1, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));
    EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());

    //schedule a backfill
    mock_stream->next();

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(2, lpAuxioQ.getFutureQueueSize());
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()
    runNextTask(lpAuxioQ);
    // backfill:complete()
    runNextTask(lpAuxioQ);
    // backfill:finished()
    runNextTask(lpAuxioQ);
    // inMemoryPhase and pendingBackfill is true and so transitions to
    // backfillPhase
    // take snapshot marker off the ReadyQ
    auto resp = mock_stream->next();
    // backfillPhase() - take doc "key1" off the ReadyQ
    resp = mock_stream->next();
    // backfillPhase - take doc "key2" off the ReadyQ
    resp = mock_stream->next();
    runNextTask(lpAuxioQ);
    runNextTask(lpAuxioQ);
    runNextTask(lpAuxioQ);
    runNextTask(lpAuxioQ);
    // Assert that the callback (and hence backfill) was only invoked twice
    ASSERT_EQ(2, registerCursorCount);
    // take snapshot marker off the ReadyQ
    resp = mock_stream->next();
    // backfillPhase - take doc "key3" off the ReadyQ
    resp = mock_stream->next();
    // backfillPhase() - take doc "key4" off the ReadyQ
    // isBackfillTaskRunning is not running and ReadyQ is now empty so also
    // transitionState from StreamBackfilling to StreamInMemory
    resp = mock_stream->next();
    EXPECT_TRUE(mock_stream->isInMemory())
        << "stream state should have transitioned to StreamInMemory";
    // inMemoryPhase.  ReadyQ is empty and pendingBackfill is false and so
    // return NULL
    resp = mock_stream->next();
    EXPECT_EQ(nullptr, resp);
    EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());

    // BackfillManagerTask
    runNextTask(lpAuxioQ);

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/* The following is a regression test for MB25056, which came about due the fix
 * for MB22960 having a bug where it is set pendingBackfill to true too often.
 *
 * To demonstrate the issue we need:
 *
 * 1. vbucket state to be replica
 *
 * 2. checkpoint state to be similar to the following:
 * CheckpointManager[0x10720d908] with numItems:3 checkpoints:1
 *   Checkpoint[0x10723d2a0] with seqno:{2,4} state:CHECKPOINT_OPEN items:[
 *   {1,empty,dummy_key}
 *   {2,checkpoint_start,checkpoint_start}
 *   {2,set,key2}
 *   {4,set,key3}
 * ]
 *   connCursors:[
 *       persistence: CheckpointCursor[0x7fff5ca0cf98] with name:persistence
 *       currentCkpt:{id:1 state:CHECKPOINT_OPEN} currentPos:2 offset:2
 *       ckptMetaItemsRead:1
 *
 *       test_producer: CheckpointCursor[0x7fff5ca0cf98] with name:test_producer
 *       currentCkpt:{id:1 state:CHECKPOINT_OPEN} currentPos:1 offset:0
 *       ckptMetaItemsRead:0
 *   ]
 *
 * 3. active stream to the vbucket requesting start seqno=0 and end seqno=4
 *
 * The test behaviour is that we perform a backfill.  In markDiskSnapshot (which
 * is invoked when we perform a backfill) we merge items in the open checkpoint.
 * In the test below this means the snapshot {start, end} is originally {0, 2}
 * but is extended to {0, 4}.
 *
 * We then call registerCursor with the lastProcessedSeqno of 2, which then
 * calls through to registerCursorBySeqno and returns 4.  Given that
 * 4 - 1 > 2 in the original fix for MB25056 we incorrectly set pendingBackfill
 * to true.  However by checking if the seqno returned is the first in the
 * checkpoint we can confirm whether a backfill is actually required, and hence
 * whether pendingBackfill should be set to true.
 *
 * In this test the result is not the first seqno in the checkpoint and so
 * pendingBackfill should be false.
 */

TEST_F(SingleThreadedEPBucketTest, MB25056_do_not_set_pendingBackfill_to_true) {
    // Records the number of times registerCursor is invoked.
    size_t registerCursorCount = 0;
    // Make vbucket a replica.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr = *vb->checkpointManager;
    EXPECT_EQ(1, ckpt_mgr.getNumCheckpoints());
    EXPECT_EQ(1, ckpt_mgr.getNumOfCursors());

    // Add an item and flush to vbucket
    auto item = make_item(vbid, makeStoredDocKey("key1"), "value");
    item.setCas(1);
    uint64_t seqno;
    store->setWithMeta(std::ref(item),
                       0,
                       &seqno,
                       cookie,
                       {vbucket_state_replica},
                       CheckConflicts::No,
                       /*allowExisting*/ true);
    getEPBucket().flushVBucket(vbid);

    // Close the first checkpoint and create a second one
    ckpt_mgr.createNewCheckpoint();

    // Remove the first checkpoint
    bool new_ckpt_created;
    ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created);

    // Add a second item and flush to bucket
    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "value");
    item2.setCas(1);
    store->setWithMeta(std::ref(item2),
                       0,
                       &seqno,
                       cookie,
                       {vbucket_state_replica},
                       CheckConflicts::No,
                       /*allowExisting*/ true);
    getEPBucket().flushVBucket(vbid);

    // Add 2 further items to the second checkpoint.  As both have the key
    // "key3" the first of the two items will be de-duplicated away.
    // Do NOT flush to vbucket.
    for (int ii = 0; ii < 2; ii++) {
        auto item = make_item(vbid, makeStoredDocKey("key3"), "value");
        item.setCas(1);
        store->setWithMeta(std::ref(item),
                           0,
                           &seqno,
                           cookie,
                           {vbucket_state_replica},
                           CheckConflicts::No,
                           /*allowExisting*/ true);
    }

    // Create a Mock Dcp producer
    const std::string testName("test_producer");
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            testName,
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);

    // Since we are creating a mock active stream outside of
    // DcpProducer::streamRequest(), and we want the checkpt processor task,
    // create it explicitly here
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a Mock Active Stream
    auto mock_stream =
            std::make_shared<MockActiveStreamWithOverloadedRegisterCursor>(
                    static_cast<EventuallyPersistentEngine*>(engine.get()),
                    producer,
                    /*flags*/ 0,
                    /*opaque*/ 0,
                    *vb,
                    /*st_seqno*/ 0,
                    /*en_seqno*/ 4,
                    /*vb_uuid*/ 0xabcd,
                    /*snap_start_seqno*/ 0,
                    /*snap_end_seqno*/ ~0,
                    IncludeValue::Yes,
                    IncludeXattrs::Yes);

    mock_stream->setCallbackBeforeRegisterCursor(
            [this, vb, &registerCursorCount]() {
                // This callback function is called every time a backfill is
                // performed. It is called immediately prior to executing
                // ActiveStream::registerCursor.
                EXPECT_EQ(0, registerCursorCount);
            });

    mock_stream->setCallbackAfterRegisterCursor(
            [&mock_stream, &registerCursorCount]() {
                // This callback function is called every time a backfill is
                // performed. It is called immediately after completing
                // ActiveStream::registerCursor.
                // The key point of the test is pendingBackfill is set to false
                registerCursorCount++;
                EXPECT_EQ(1, registerCursorCount);
                EXPECT_FALSE(mock_stream->public_getPendingBackfill());
            });

    // transitioning to Backfilling results in calling
    // scheduleBackfill_UNLOCKED(false)
    mock_stream->transitionStateToBackfilling();
    // schedule the backfill
    mock_stream->next();

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(2, lpAuxioQ.getFutureQueueSize());
    // backfill:create()
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    // backfill:scan()
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    // backfill:complete()
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    // backfill:finished()
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    // inMemoryPhase and pendingBackfill is true and so transitions to
    // backfillPhase
    // take snapshot marker off the ReadyQ
    std::unique_ptr<DcpResponse> resp =
            static_cast< std::unique_ptr<DcpResponse> >(mock_stream->next());
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, resp->getEvent());

    // backfillPhase() - take doc "key1" off the ReadyQ
    resp = mock_stream->next();
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(std::string("key1"),
              dynamic_cast<MutationResponse*>(resp.get())->
              getItem()->getKey().c_str());

    // backfillPhase - take doc "key2" off the ReadyQ
    resp = mock_stream->next();
    EXPECT_EQ(DcpResponse::Event::Mutation, resp->getEvent());
    EXPECT_EQ(std::string("key2"),
              dynamic_cast<MutationResponse*>(resp.get())->
              getItem()->getKey().c_str());

    EXPECT_TRUE(mock_stream->isInMemory())
            << "stream state should have transitioned to StreamInMemory";

    resp = mock_stream->next();
    EXPECT_FALSE(resp);

    EXPECT_EQ(1, ckpt_mgr.getNumCheckpoints());
    EXPECT_EQ(2, ckpt_mgr.getNumOfCursors());
    // Assert that registerCursor (and hence backfill) was only invoked once
    ASSERT_EQ(1, registerCursorCount);

    // ActiveStreamCheckpointProcessorTask
    runNextTask(lpAuxioQ, "Process checkpoint(s) for DCP producer " + testName);
    // BackfillManagerTask
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/**
 * Regression test for MB-22451: When handleSlowStream is called and in
 * StreamBackfilling state and currently have a backfill scheduled (or running)
 * ensure that when the backfill completes pendingBackfill remains true,
 * isBackfillTaskRunning is false and, the stream state remains set to
 * StreamBackfilling.
 */
TEST_F(SingleThreadedEPBucketTest, test_mb22451) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Store a single Item
    store_item(vbid, makeStoredDocKey("key"), "value");
    // Ensure that it has persisted to disk
    flush_vbucket_to_disk(vbid);

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test_producer",
            /*flags*/ 0,
            cb::const_byte_buffer() /*no json*/);
    // Create a Mock Active Stream
    auto vb = store->getVBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
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

    /**
      * The core of the test follows:
      * Call completeBackfill whilst we are in the state of StreamBackfilling
      * and the pendingBackfill flag is set to true.
      * We expect that on leaving completeBackfill the isBackfillRunning flag is
      * set to true.
      */
    mock_stream->public_setBackfillTaskRunning(true);
    mock_stream->transitionStateToBackfilling();
    mock_stream->handleSlowStream();
    // The call to handleSlowStream should result in setting pendingBackfill
    // flag to true
    EXPECT_TRUE(mock_stream->public_getPendingBackfill())
        << "handleSlowStream should set pendingBackfill to True";
    mock_stream->completeBackfill();
    EXPECT_FALSE(mock_stream->public_isBackfillTaskRunning())
        << "completeBackfill should set isBackfillTaskRunning to False";
    EXPECT_TRUE(mock_stream->isBackfilling())
        << "stream state should not have changed";
    // Required to ensure that the backfillMgr is deleted
    producer->closeAllStreams();

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/* Regression / reproducer test for MB-19815 - an exception is thrown
 * (and connection disconnected) if a couchstore file hasn't been re-created
 * yet when doDcpVbTakeoverStats() is called.
 */
TEST_F(SingleThreadedEPBucketTest, MB19815_doDcpVbTakeoverStats) {
    auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>
        (ExecutorPool::get());

    // Should start with no tasks registered on any queues.
    for (auto& queue : task_executor->getLpTaskQ()) {
        ASSERT_EQ(0, queue->getFutureQueueSize());
        ASSERT_EQ(0, queue->getReadyQueueSize());
    }

    // [[1] Set our state to replica.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // [[2]] Perform a vbucket reset. This will perform some work synchronously,
    // but also creates the task that will delete the VB.
    //   * vbucket memory and disk deletion (AUXIO)
    // MB-19695: If we try to get the number of persisted deletes between
    // steps [[2]] and [[3]] running then an exception is thrown (and client
    // disconnected).
    EXPECT_TRUE(store->resetVBucket(vbid));
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    runNextTask(lpAuxioQ, "Removing (dead) vb:0 from memory and disk");

    // [[3]] Ok, let's see if we can get DCP takeover stats.
    // Dummy callback to pass into the stats function below.
    auto dummy_cb = [](const char* key,
                       const uint16_t klen,
                       const char* val,
                       const uint32_t vlen,
                       gsl::not_null<const void*> cookie) {};
    std::string key{"MB19815_doDCPVbTakeoverStats"};

    // We can't call stats with a nullptr as the cookie. Given that
    // the callback don't use the cookie "at all" we can just use the key
    // as the cookie
    EXPECT_NO_THROW(engine->public_doDcpVbTakeoverStats(
            static_cast<const void*>(key.c_str()), dummy_cb, key, vbid));

    // Cleanup - run flusher.
    EXPECT_EQ(std::make_pair(false, size_t(0)),
              getEPBucket().flushVBucket(vbid));
}

/*
 * Test that
 * 1. We cannot create a stream against a dead vb (MB-17230)
 * 2. No tasks are scheduled as a side-effect of the streamRequest attempt.
 */
TEST_F(SingleThreadedEPBucketTest, MB19428_no_streams_against_dead_vbucket) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key"), "value");

    // Directly flush the vbucket
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead);
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];

    {
        // Create a Mock Dcp producer
        auto producer = std::make_shared<MockDcpProducer>(
                *engine,
                cookie,
                "test_producer",
                /*flags*/ 0,
                cb::const_byte_buffer() /*no json*/);

        // Creating a producer will not create an
        // ActiveStreamCheckpointProcessorTask until a stream is created.
        EXPECT_EQ(0, lpAuxioQ.getFutureQueueSize());

        uint64_t rollbackSeqno;
        auto err = producer->streamRequest(
                /*flags*/ 0,
                /*opaque*/ 0,
                /*vbucket*/ vbid,
                /*start_seqno*/ 0,
                /*end_seqno*/ -1,
                /*vb_uuid*/ 0,
                /*snap_start*/ 0,
                /*snap_end*/ 0,
                &rollbackSeqno,
                SingleThreadedEPBucketTest::fakeDcpAddFailoverLog);

        EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, err) << "Unexpected error code";

        // The streamRequest failed and should not of created anymore tasks than
        // ActiveStreamCheckpointProcessorTask.
        EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());

        // Stop Producer checkpoint processor task
        producer->cancelCheckpointCreatorTask();
    }
}

/*
 * Test that TaskQueue::wake results in a sensible ExecutorPool work count
 * Incorrect counting can result in the run loop spinning for many threads.
 */
TEST_F(SingleThreadedEPBucketTest, MB20235_wake_and_work_count) {
    class TestTask : public GlobalTask {
    public:
        TestTask(EventuallyPersistentEngine *e, double s) :
                 GlobalTask(e, TaskId::ActiveStreamCheckpointProcessorTask, s) {}
        bool run() {
            return false;
        }

        cb::const_char_buffer getDescription() {
            return "Test MB20235";
        }

        std::chrono::microseconds maxExpectedDuration() {
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
              task_executor->getNumReadyTasks(AUXIO_TASK_IDX));
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());

    // Wake task, but stays in futureQueue (fetch can now move it)
    task_executor->wake(task->getId());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(), task_executor->getTotReadyTasks());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(),
              task_executor->getNumReadyTasks(AUXIO_TASK_IDX));
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ.getReadyQueueSize());

    runNextTask(lpAuxioQ);
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(), task_executor->getTotReadyTasks());
    EXPECT_EQ(lpAuxioQ.getReadyQueueSize(),
              task_executor->getNumReadyTasks(AUXIO_TASK_IDX));
    EXPECT_EQ(0, lpAuxioQ.getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ.getReadyQueueSize());
}

// Check that in-progress disk backfills (`CouchKVStore::backfill`) are
// correctly deleted when we delete a bucket. If not then we leak vBucket file
// descriptors, which can prevent ns_server from cleaning up old vBucket files
// and consequently re-adding a node to the cluster.
//
TEST_F(SingleThreadedEPBucketTest, MB19892_BackfillNotDeleted) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Perform one SET, then close it's checkpoint. This means that we no
    // longer have all sequence numbers in memory checkpoints, forcing the
    // DCP stream request to go to disk (backfill).
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Force a new checkpoint.
    auto vb = store->getVBuckets().getBucket(vbid);
    auto& ckpt_mgr = *vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    // Directly flush the vbucket, ensuring data is on disk.
    //  (This would normally also wake up the checkpoint remover task, but
    //   as that task was never registered with the ExecutorPool in this test
    //   environment, we need to manually remove the prev checkpoint).
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    bool new_ckpt_created;
    EXPECT_EQ(1, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));

    // Create a DCP producer, and start a stream request.
    std::string name{"test_producer"};
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcpOpen(cookie,
                              /*opaque:unused*/ {},
                              /*seqno:unused*/ {},
                              DCP_OPEN_PRODUCER,
                              name,
                              {}));

    uint64_t rollbackSeqno;
    auto dummy_dcp_add_failover_cb = [](vbucket_failover_t* entry,
                                        size_t nentries,
                                        gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    };

    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine.get()->dcp.stream_req(&engine.get()->interface,
                                           cookie,
                                           /*flags*/ 0,
                                           /*opaque*/ 0,
                                           /*vbucket*/ vbid,
                                           /*start_seqno*/ 0,
                                           /*end_seqno*/ -1,
                                           /*vb_uuid*/ 0,
                                           /*snap_start*/ 0,
                                           /*snap_end*/ 0,
                                           &rollbackSeqno,
                                           dummy_dcp_add_failover_cb));
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
        const std::string key = "key" + std::to_string(ii);
        const DocKey docKey{key, DocNamespace::DefaultCollection};
        std::string value = "value";

        consumer->mutation(1/*opaque*/,
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

/*
 * Test that the DCP processor returns a 'yield' return code when
 * working on a large enough buffer size.
 */
TEST_F(SingleThreadedEPBucketTest, MB_27457) {
    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Create a MockDcpConsumer
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "test");

    // Add the stream
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // 1. Add the first message, a snapshot marker.
    consumer->snapshotMarker(/*opaque*/ 1,
                             vbid,
                             /*startseq*/ 0,
                             /*endseq*/ 2,
                             /*flags*/ 0);
    std::string data = R"({"json":"yes"})";
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
                                data.size()};
    // 2. Now add two deletions, one without deleteTime, one with
    consumer->deletionV2(/*opaque*/ 1,
                         {"key1", DocNamespace::DefaultCollection},
                         /*values*/ value,
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_DATATYPE_JSON,
                         /*cas*/ 0,
                         /*vbucket*/ vbid,
                         /*bySeqno*/ 1,
                         /*revSeqno*/ 0,
                         /*deleteTime*/ 0);

    const uint32_t deleteTime = 10;
    consumer->deletionV2(/*opaque*/ 1,
                         {"key2", DocNamespace::DefaultCollection},
                         /*values*/ value,
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_DATATYPE_JSON,
                         /*cas*/ 0,
                         /*vbucket*/ vbid,
                         /*bySeqno*/ 2,
                         /*revSeqno*/ 0,
                         deleteTime);

    EXPECT_EQ(std::make_pair(false, size_t(2)),
              getEPBucket().flushVBucket(vbid));

    // Drop the stream
    consumer->closeStream(/*opaque*/ 0, vbid);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Now read back and verify key2 has our test deleteTime of 10
    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData(makeStoredDocKey("key1"),
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData(makeStoredDocKey("key1"),
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    EXPECT_EQ(1, deleted);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, datatype);
    EXPECT_NE(0, metadata.exptime); // A locally created deleteTime

    deleted = 0;
    datatype = 0;
    EXPECT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData(makeStoredDocKey("key2"),
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    EXPECT_EQ(ENGINE_SUCCESS,
              store->getMetaData(makeStoredDocKey("key2"),
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    EXPECT_EQ(1, deleted);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, datatype);
    EXPECT_EQ(deleteTime, metadata.exptime); // Our replicated deleteTime!
}

/*
 * Background thread used by MB20054_onDeleteItem_during_bucket_deletion
 */
static void MB20054_run_backfill_task(EventuallyPersistentEngine* engine,
                                      CheckedExecutor& backfill,
                                      SyncObject& backfill_cv,
                                      SyncObject& destroy_cv,
                                      TaskQueue* lpAuxioQ) {
    std::unique_lock<std::mutex> destroy_lh(destroy_cv);
    ObjectRegistry::onSwitchThread(engine);

    // Run the BackfillManagerTask task to push items to readyQ. In sherlock
    // upwards this runs multiple times - so should return true.
    backfill.runCurrentTask("Backfilling items for a DCP Connection");

    // Notify the main thread that it can progress with destroying the
    // engine [A].
    {
        // if we can get the lock, then we know the main thread is waiting
        std::lock_guard<std::mutex> backfill_lock(backfill_cv);
        backfill_cv.notify_one(); // move the main thread along
    }

    // Now wait ourselves for destroy to be completed [B].
    destroy_cv.wait(destroy_lh);

    // This is the only "hacky" part of the test - we need to somehow
    // keep the DCPBackfill task 'running' - i.e. not call
    // completeCurrentTask - until the main thread is in
    // ExecutorPool::_stopTaskGroup. However we have no way from the test
    // to properly signal that we are *inside* _stopTaskGroup -
    // called from EVPStore's destructor.
    // Best we can do is spin on waiting for the DCPBackfill task to be
    // set to 'dead' - and only then completeCurrentTask; which will
    // cancel the task.
    while (!backfill.getCurrentTask()->isdead()) {
        // spin.
    }
    backfill.completeCurrentTask();
}

static ENGINE_ERROR_CODE dummy_dcp_add_failover_cb(
        vbucket_failover_t* entry,
        size_t nentries,
        gsl::not_null<const void*> cookie) {
    return ENGINE_SUCCESS;
}

// Test performs engine deletion interleaved with tasks so redefine TearDown
// for this tests needs.
class MB20054_SingleThreadedEPStoreTest : public SingleThreadedEPBucketTest {
public:
    void SetUp() {
        SingleThreadedEPBucketTest::SetUp();
        engine->initializeConnmap();
    }

    void TearDown() {
        ExecutorPool::shutdown();
    }
};

// Check that if onDeleteItem() is called during bucket deletion, we do not
// abort due to not having a valid thread-local 'engine' pointer. This
// has been observed when we have a DCPBackfill task which is deleted during
// bucket shutdown, which has a non-zero number of Items which are destructed
// (and call onDeleteItem).
TEST_F(MB20054_SingleThreadedEPStoreTest, MB20054_onDeleteItem_during_bucket_deletion) {

    // [[1] Set our state to active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Perform one SET, then close it's checkpoint. This means that we no
    // longer have all sequence numbers in memory checkpoints, forcing the
    // DCP stream request to go to disk (backfill).
    store_item(vbid, makeStoredDocKey("key"), "value");

    // Force a new checkpoint.
    VBucketPtr vb = store->getVBuckets().getBucket(vbid);
    CheckpointManager& ckpt_mgr = *vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();
    auto lpWriterQ = task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    EXPECT_EQ(0, lpWriterQ->getFutureQueueSize());
    EXPECT_EQ(0, lpWriterQ->getReadyQueueSize());

    auto lpAuxioQ = task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Directly flush the vbucket, ensuring data is on disk.
    //  (This would normally also wake up the checkpoint remover task, but
    //   as that task was never registered with the ExecutorPool in this test
    //   environment, we need to manually remove the prev checkpoint).
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    bool new_ckpt_created;
    EXPECT_EQ(1, ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created));
    vb.reset();

    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Create a DCP producer, and start a stream request.
    std::string name("test_producer");
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcpOpen(cookie,
                              /*opaque:unused*/ {},
                              /*seqno:unused*/ {},
                              DCP_OPEN_PRODUCER,
                              name,
                              {}));

    // ActiveStreamCheckpointProcessorTask and DCPBackfill task are created
    // when the first DCP stream is created.
    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    uint64_t rollbackSeqno;
    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->dcp.stream_req(&engine->interface,
                                     cookie,
                                     /*flags*/ 0,
                                     /*opaque*/ 0,
                                     /*vbucket*/ vbid,
                                     /*start_seqno*/ 0,
                                     /*end_seqno*/ -1,
                                     /*vb_uuid*/ 0,
                                     /*snap_start*/ 0,
                                     /*snap_end*/ 0,
                                     &rollbackSeqno,
                                     dummy_dcp_add_failover_cb));

    // FutureQ should now have an additional DCPBackfill task.
    EXPECT_EQ(2, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    // Create an executor 'thread' to obtain shared ownership of the next
    // AuxIO task (which should be BackfillManagerTask). As long as this
    // object has it's currentTask set to BackfillManagerTask, the task
    // will not be deleted.
    // Essentially we are simulating a concurrent thread running this task.
    CheckedExecutor backfill(task_executor, *lpAuxioQ);

    // This is the one action we really need to perform 'concurrently' - delete
    // the engine while a DCPBackfill task is still running. We spin up a
    // separate thread which will run the DCPBackfill task
    // concurrently with destroy - specifically DCPBackfill must start running
    // (and add items to the readyQ) before destroy(), it must then continue
    // running (stop after) _stopTaskGroup is invoked.
    // To achieve this we use a couple of condition variables to synchronise
    // between the two threads - the timeline needs to look like:
    //
    //  auxIO thread:  [------- DCPBackfill ----------]
    //   main thread:          [destroy()]       [ExecutorPool::_stopTaskGroup]
    //
    //  --------------------------------------------------------> time
    //
    SyncObject backfill_cv;
    SyncObject destroy_cv;
    std::thread concurrent_task_thread;

    {
        // scope for the backfill lock
        std::unique_lock<std::mutex> backfill_lh(backfill_cv);

        concurrent_task_thread = std::thread(MB20054_run_backfill_task,
                                             engine.get(),
                                             std::ref(backfill),
                                             std::ref(backfill_cv),
                                             std::ref(destroy_cv),
                                             lpAuxioQ);
        // [A] Wait for DCPBackfill to complete.
        backfill_cv.wait(backfill_lh);
    }

    ObjectRegistry::onSwitchThread(engine.get());
    // 'Destroy' the engine - this doesn't delete the object, just shuts down
    // connections, marks streams as dead etc.
    engine->destroy(/*force*/false);

    {
        // If we can get the lock we know the thread is waiting for destroy.
        std::lock_guard<std::mutex> lh(destroy_cv);
        destroy_cv.notify_one(); // move the thread on.
    }

    // Force all tasks to cancel (so we can shutdown)
    cancelAndPurgeTasks();

    // Mark the connection as dead for clean shutdown
    destroy_mock_cookie(cookie);
    engine->getDcpConnMap().manageConnections();

    // Nullify TLS engine and reset the smart pointer to force destruction.
    // We need null as the engine to stop ~CheckedExecutor path from trying
    // to touch the engine
    ObjectRegistry::onSwitchThread(nullptr);
    engine.reset();
    destroy_mock_event_callbacks();
    concurrent_task_thread.join();
}

/*
 * MB-18953 is triggered by the executorpool wake path moving tasks directly
 * into the readyQueue, thus allowing for high-priority tasks to dominiate
 * a taskqueue.
 */
TEST_F(SingleThreadedEPBucketTest, MB18953_taskWake) {
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    ExTask hpTask = std::make_shared<TestTask>(engine.get(),
                                               TaskId::PendingOpsNotification);
    task_executor->schedule(hpTask);

    ExTask lpTask =
            std::make_shared<TestTask>(engine.get(), TaskId::DefragmenterTask);
    task_executor->schedule(lpTask);

    runNextTask(lpNonioQ, "TestTask PendingOpsNotification"); // hptask goes first
    // Ensure that a wake to the hpTask doesn't mean the lpTask gets ignored
    lpNonioQ.wake(hpTask);

    // Check 1 task is ready
    EXPECT_EQ(1, task_executor->getTotReadyTasks());
    EXPECT_EQ(1, task_executor->getNumReadyTasks(NONIO_TASK_IDX));

    runNextTask(lpNonioQ, "TestTask DefragmenterTask"); // lptask goes second

    // Run the tasks again to check that coming from ::reschedule our
    // expectations are still met.
    runNextTask(lpNonioQ, "TestTask PendingOpsNotification"); // hptask goes first

    // Ensure that a wake to the hpTask doesn't mean the lpTask gets ignored
    lpNonioQ.wake(hpTask);

    // Check 1 task is ready
    EXPECT_EQ(1, task_executor->getTotReadyTasks());
    EXPECT_EQ(1, task_executor->getNumReadyTasks(NONIO_TASK_IDX));
    runNextTask(lpNonioQ, "TestTask DefragmenterTask"); // lptask goes second
}

/*
 * MB-20735 waketime is not correctly picked up on reschedule
 */
TEST_F(SingleThreadedEPBucketTest, MB20735_rescheduleWaketime) {
    auto& lpNonioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];

    class SnoozingTestTask : public TestTask {
    public:
        SnoozingTestTask(EventuallyPersistentEngine* e, TaskId id)
            : TestTask(e, id) {
        }

        bool run() override {
            snooze(0.1); // snooze for 100milliseconds only
            // Rescheduled to run 100 milliseconds later..
            return true;
        }
    };

    auto task = std::make_shared<SnoozingTestTask>(
            engine.get(), TaskId::PendingOpsNotification);
    ExTask hpTask = task;
    task_executor->schedule(hpTask);

    ProcessClock::time_point waketime = runNextTask(lpNonioQ,
                                    "TestTask PendingOpsNotification");
    EXPECT_EQ(waketime, task->getWaketime()) <<
                           "Rescheduled to much later time!";
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
        auto producer = std::make_shared<MockDcpProducer>(
                *engine,
                cookie,
                "test_producer",
                /*flags*/ 0,
                cb::const_byte_buffer() /*no json*/);

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
                                                   fakeDcpAddFailoverLog);

        if (it.second) {
            EXPECT_EQ(ENGINE_SUCCESS, err) << "Unexpected error code";
            producer->closeStream(/*opaque*/0, /*vbucket*/vbid);
        } else {
            EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, err) << "Unexpected error code";
        }

        // Stop Producer checkpoint processor task
        producer->cancelCheckpointCreatorTask();
    }
}

TEST_F(SingleThreadedEPBucketTest, pre_expiry_xattrs) {
    auto& kvbucket = *engine->getKVBucket();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto xattr_data = createXattrValue("value");

    auto itm = store_item(vbid,
                          makeStoredDocKey("key"),
                          xattr_data,
                          1,
                          {cb::engine_errc::success},
                          PROTOCOL_BINARY_DATATYPE_XATTR);

    ItemMetaData metadata;
    uint32_t deleted;
    uint8_t datatype;
    kvbucket.getMetaData(makeStoredDocKey("key"), vbid, cookie, metadata,
                         deleted, datatype);
    auto prev_revseqno = metadata.revSeqno;
    EXPECT_EQ(1, prev_revseqno) << "Unexpected revision sequence number";
    itm.setRevSeqno(1);
    kvbucket.deleteExpiredItem(itm, ep_real_time() + 1, ExpireBy::Pager);

    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
                                                       TRACK_STATISTICS |
                                                       GET_DELETED_VALUE);
    GetValue gv = kvbucket.get(makeStoredDocKey("key"), vbid, cookie, options);
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());

    auto get_itm = gv.item.get();
    auto get_data = const_cast<char*>(get_itm->getData());

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf);

    const std::string& cas_str{"{\"cas\":\"0xdeadbeefcafefeed\"}"};
    const std::string& sync_str = to_string(new_blob.get("_sync"));

    EXPECT_EQ(cas_str, sync_str) << "Unexpected system xattrs";
    EXPECT_TRUE(new_blob.get("user").empty())
            << "The user attribute should be gone";
    EXPECT_TRUE(new_blob.get("meta").empty())
            << "The meta attribute should be gone";

    kvbucket.getMetaData(makeStoredDocKey("key"), vbid, cookie, metadata,
                         deleted, datatype);
    EXPECT_EQ(prev_revseqno + 1, metadata.revSeqno) <<
             "Unexpected revision sequence number";

}

class WarmupTest : public SingleThreadedKVBucketTest {
public:
    /**
     * Test is currently using couchstore API directly to make the VB appear old
     */
    static void rewriteVBStateAs25x(uint16_t vbucket) {
        std::string filename = std::string(test_dbname) + "/" +
                               std::to_string(vbucket) + ".couch.1";
        Db* handle;
        couchstore_error_t err = couchstore_open_db(
                filename.c_str(), COUCHSTORE_OPEN_FLAG_CREATE, &handle);

        ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to open new database";

        // Create a 2.5 _local/vbstate
        std::string vbstate2_5_x =
                "{\"state\": \"active\","
                " \"checkpoint_id\": \"1\","
                " \"max_deleted_seqno\": \"0\"}";
        LocalDoc vbstate;
        vbstate.id.buf = (char*)"_local/vbstate";
        vbstate.id.size = sizeof("_local/vbstate") - 1;
        vbstate.json.buf = (char*)vbstate2_5_x.c_str();
        vbstate.json.size = vbstate2_5_x.size();
        vbstate.deleted = 0;

        err = couchstore_save_local_document(handle, &vbstate);
        ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to write local document";
        couchstore_commit(handle);
        couchstore_close_file(handle);
        couchstore_free_db(handle);
    }

    void resetEngineAndWarmup(std::string new_config = "") {
        resetEngineAndEnableWarmup(new_config);
        // Now get the engine warmed up
        runReadersUntilWarmedUp();
    }

    /**
     * Destroy engine and replace it with a new engine that can be warmed up.
     * Finally, run warmup.
     */
    void resetEngineAndEnableWarmup(std::string new_config = "") {
        shutdownAndPurgeTasks();
        std::string config = config_string;

        // check if warmup=false needs replacing with warmup=true
        size_t pos;
        std::string warmupT = "warmup=true";
        std::string warmupF = "warmup=false";
        if ((pos = config.find(warmupF)) != std::string::npos) {
            config.replace(pos, warmupF.size(), warmupT);
        } else {
            config += warmupT;
        }

        if (new_config.length() > 0) {
            config += ";";
            config += new_config;
        }

        reinitialise(config);

        engine->getKVBucket()->initializeWarmupTask();
        engine->getKVBucket()->startWarmupTask();
    }
};

TEST_F(WarmupTest, hlcEpoch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteVBStateAs25x(vbid);

    resetEngineAndWarmup();

    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        // We've warmed up from a down-level vbstate, so expect epoch to be
        // HlcCasSeqnoUninitialised
        ASSERT_EQ(HlcCasSeqnoUninitialised, vb->getHLCEpochSeqno());

        // Store a new key, the flush will change hlc_epoch to be the next seqno
        // (2)
        store_item(vbid, makeStoredDocKey("key2"), "value");
        flush_vbucket_to_disk(vbid);

        EXPECT_EQ(2, vb->getHLCEpochSeqno());

        // Store a 3rd item
        store_item(vbid, makeStoredDocKey("key3"), "value");
        flush_vbucket_to_disk(vbid);
    }

    // Warmup again, hlcEpoch will still be 2
    resetEngineAndWarmup();
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(2, vb->getHLCEpochSeqno());

    // key1 stored before we established the epoch should have cas_is_hlc==false
    auto item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_FALSE(info1.cas_is_hlc);

    // key2 should have a CAS generated from the HLC
    auto item2 = store->get(makeStoredDocKey("key2"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item2.getStatus());
    auto info2 = engine->getItemInfo(*item2.item);
    EXPECT_TRUE(info2.cas_is_hlc);
}

TEST_F(WarmupTest, fetchDocInDifferentCompressionModes) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    std::string valueData("{\"product\": \"car\",\"price\": \"100\"},"
                          "{\"product\": \"bus\",\"price\": \"1000\"},"
                          "{\"product\": \"Train\",\"price\": \"100000\"}");

    //Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), valueData);
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup();
    auto item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, info1.datatype);

    resetEngineAndWarmup("compression_mode=passive");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);

    resetEngineAndWarmup("compression_mode=active");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(ENGINE_SUCCESS, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);
}

TEST_F(WarmupTest, mightContainXattrs) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteVBStateAs25x(vbid);

    resetEngineAndWarmup();
    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        EXPECT_FALSE(vb->mightContainXattrs());

        auto xattr_data = createXattrValue("value");

        auto itm = store_item(vbid,
                              makeStoredDocKey("key"),
                              xattr_data,
                              1,
                              {cb::engine_errc::success},
                              PROTOCOL_BINARY_DATATYPE_XATTR);

        EXPECT_TRUE(vb->mightContainXattrs());

        flush_vbucket_to_disk(vbid);
    }
    // Warmup - we should have xattr dirty
    resetEngineAndWarmup();

    EXPECT_TRUE(engine->getKVBucket()->getVBucket(vbid)->mightContainXattrs());
}

/**
 * Performs the following operations
 * 1. Store an item
 * 2. Delete the item
 * 3. Recreate the item
 * 4. Perform a warmup
 * 5. Get meta data of the key to verify the revision seq no is
 *    equal to number of updates on it
 */
TEST_F(WarmupTest, MB_27162) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("key");

    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    delete_item(vbid, key);
    flush_vbucket_to_disk(vbid);

    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup();

    ItemMetaData itemMeta;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    auto doGetMetaData = [&]() {
        return store->getMetaData(key, vbid, cookie, itemMeta, deleted,
                                  datatype);
    };
    auto engineResult = doGetMetaData();

    ASSERT_EQ(ENGINE_SUCCESS, engineResult);
    EXPECT_EQ(3, itemMeta.revSeqno);
}

TEST_F(WarmupTest, MB_25197) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);

    resetEngineAndEnableWarmup();

    // Manually run the reader queue so that the warmup tasks execute whilst we
    // perform setVbucketState calls
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    EXPECT_EQ(nullptr, store->getVBuckets().getBucket(vbid));
    auto notifications = get_number_of_mock_cookie_io_notifications(cookie);
    while (engine->getKVBucket()->shouldSetVBStateBlock(cookie)) {
        CheckedExecutor executor(task_executor, readerQueue);
        // Do a setVBState but don't flush it through. This call should be
        // failed ewouldblock whilst warmup has yet to attempt to create VBs.
        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  store->setVBucketState(vbid,
                                         vbucket_state_active,
                                         /*transfer*/ false,
                                         cookie));
        executor.runCurrentTask();
    }

    EXPECT_GT(get_number_of_mock_cookie_io_notifications(cookie),
              notifications);
    EXPECT_NE(nullptr, store->getVBuckets().getBucket(vbid));
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid,
                                     vbucket_state_active,
                                     /*transfer*/ false));

    // finish warmup so the test can exit
    while (engine->getKVBucket()->isWarmingUp()) {
        CheckedExecutor executor(task_executor, readerQueue);
        executor.runCurrentTask();
    }
}

// Test that we can push a DCP_DELETION which pretends to be from a delete
// with xattrs, i.e. the delete has a value containing only system xattrs
// The MB was created because this code would actually trigger an exception
TEST_F(SingleThreadedEPBucketTest, mb25273) {
    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");
    int opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, /*flags*/ 0));

    std::string key = "key";
    std::string body = "body";

    // Manually manage the xattr blob - later we will prune user keys
    cb::xattr::Blob blob;

    blob.set("key1", "{\"author\":\"bubba\"}");
    blob.set("_sync", "{\"cas\":\"0xdeadbeefcafefeed\"}");

    auto xattr_value = blob.finalize();

    std::string data;
    std::copy(xattr_value.buf,
              xattr_value.buf + xattr_value.len,
              std::back_inserter(data));
    std::copy(
            body.c_str(), body.c_str() + body.size(), std::back_inserter(data));

    const DocKey docKey{key, DocNamespace::DefaultCollection};
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
                                data.size()};

    // Send mutation in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, vbid, bySeqno, bySeqno, MARKER_FLAG_CHK));
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->mutation(opaque,
                                 docKey,
                                 value,
                                 0, // priv bytes
                                 PROTOCOL_BINARY_DATATYPE_XATTR,
                                 2, // cas
                                 vbid,
                                 0xf1a95, // flags
                                 bySeqno,
                                 0, // rev seqno
                                 0, // exptime
                                 0, // locktime
                                 {}, // meta
                                 0)); // nru
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    bySeqno++;

    // Send deletion in a single seqno snapshot and send a doc with only system
    // xattrs to simulate what an active would send
    blob.prune_user_keys();
    auto finalizedXttr = blob.finalize();
    value = {reinterpret_cast<const uint8_t*>(finalizedXttr.data()),
             finalizedXttr.size()};
    EXPECT_NE(0, value.size());
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, vbid, bySeqno, bySeqno, MARKER_FLAG_CHK));
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->deletion(opaque,
                                 docKey,
                                 value,
                                 /*priv_bytes*/ 0,
                                 PROTOCOL_BINARY_DATATYPE_XATTR,
                                 /*cas*/ 3,
                                 vbid,
                                 bySeqno,
                                 /*revSeqno*/ 0,
                                 /*meta*/ {}));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    /* Close stream before deleting the connection */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(docKey, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    gv = store->get(docKey, vbid, cookie, GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // check it's there and deleted with the expected value length
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(0, gv.item->getFlags()); // flags also still zero
    EXPECT_EQ(3, gv.item->getCas());
    EXPECT_EQ(value.size(), gv.item->getValue()->valueSize());
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

extern uint32_t dcp_last_delete_time;
extern std::string dcp_last_key;
// Combine warmup and DCP so we can check deleteTimes come back from disk
TEST_F(WarmupTest, produce_delete_times) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto t1 = ep_real_time();
    storeAndDeleteItem(
            vbid, {"KEY1", DocNamespace::DefaultCollection}, "value");
    auto t2 = ep_real_time();
    // Now warmup to ensure that DCP will have to go to disk.
    resetEngineAndWarmup();

    auto cookie = create_mock_cookie();
    auto producer =
            createDcpProducer(cookie, {}, false, IncludeDeleteTime::Yes);
    auto producers = get_dcp_producers(
            reinterpret_cast<ENGINE_HANDLE*>(engine.get()),
            reinterpret_cast<ENGINE_HANDLE_V1*>(engine.get()));

    createDcpStream(*producer);

    // noop off as we will play with time travel
    producer->setNoopEnabled(false);

    auto step = [this, producer, &producers](bool inMemory) {
        notifyAndStepToCheckpoint(*producer,
                                  *producers,
                                  cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                                  inMemory);

        // Now step the producer to transfer the delete/tombstone
        EXPECT_EQ(ENGINE_WANT_MORE, producer->step(producers.get()));
    };

    step(false);
    EXPECT_NE(0, dcp_last_delete_time);
    EXPECT_GE(dcp_last_delete_time, t1);
    EXPECT_LE(dcp_last_delete_time, t2);
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_DELETION, dcp_last_op);
    EXPECT_EQ("KEY1", dcp_last_key);

    // Now a new delete, in-memory will also have a delete time
    t1 = ep_real_time();
    storeAndDeleteItem(
            vbid, {"KEY2", DocNamespace::DefaultCollection}, "value");
    t2 = ep_real_time();

    step(true);

    EXPECT_NE(0, dcp_last_delete_time);
    EXPECT_GE(dcp_last_delete_time, t1);
    EXPECT_LE(dcp_last_delete_time, t2);
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_DELETION, dcp_last_op);
    EXPECT_EQ("KEY2", dcp_last_key);

    // Finally expire a key and check that the delete_time we receive is the
    // expiry time, not actually the time it was deleted.
    auto expiryTime = ep_real_time() + 32000;
    store_item(vbid,
               {"KEY3", DocNamespace::DefaultCollection},
               "value",
               expiryTime);

    step(true);

    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_last_op);
    TimeTraveller arron(64000);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocNamespace::DefaultCollection}, vbid, cookie, NONE);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    step(true);

    EXPECT_EQ(expiryTime, dcp_last_delete_time);
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_DELETION, dcp_last_op);
    EXPECT_EQ("KEY3", dcp_last_key);

    destroy_mock_cookie(cookie);
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}
