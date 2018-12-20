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
#include "../mock/mock_synchronous_ep_engine.h"
#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "dcp/dcpconnmap.h"
#include "ephemeral_tombstone_purger.h"
#include "ep_time.h"
#include "evp_store_test.h"
#include "failover-table.h"
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
#include <engines/ep/src/ephemeral_vb.h>

std::chrono::steady_clock::time_point SingleThreadedKVBucketTest::runNextTask(
        TaskQueue& taskQ, const std::string& expectedTaskName) {
    CheckedExecutor executor(task_executor, taskQ);

    // Run the task
    executor.runCurrentTask(expectedTaskName);
    return executor.completeCurrentTask();
}

std::chrono::steady_clock::time_point SingleThreadedKVBucketTest::runNextTask(
        TaskQueue& taskQ) {
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
    shutdownAndPurgeTasks(engine.get());
    KVBucketTest::TearDown();
}

void SingleThreadedKVBucketTest::setVBucketStateAndRunPersistTask(
        Vbid vbid, vbucket_state_t newState) {
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

void SingleThreadedKVBucketTest::shutdownAndPurgeTasks(
        EventuallyPersistentEngine* ep) {
    ep->getEpStats().isShutdown = true;
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
        task_executor->stopTaskGroup(
                ep->getTaskable().getGID(), t, ep->getEpStats().forceShutdown);
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
    shutdownAndPurgeTasks(engine.get());
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
        IncludeDeleteTime deleteTime) {
    int flags = cb::mcbp::request::DcpOpenPayload::IncludeXattrs;
    if (deleteTime == IncludeDeleteTime::Yes) {
        flags |= cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes;
    }
    auto newProducer = std::make_shared<MockDcpProducer>(*engine,
                                                         cookie,
                                                         "test_producer",
                                                         flags,
                                                         false /*startTask*/);

    // Create the task object, but don't schedule
    newProducer->createCheckpointProcessorTask();

    // Need to enable NOOP for XATTRS (and collections).
    newProducer->setNoopEnabled(true);

    return newProducer;
}

void SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(
        MockDcpProducer& producer,
        MockDcpMessageProducers& producers,
        cb::mcbp::ClientOpcode expectedOp,
        bool fromMemory) {
    auto vb = store->getVBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    if (fromMemory) {
        producer.notifySeqnoAvailable(vbid, vb->getHighSeqno());
        runCheckpointProcessor(producer, producers);
    } else {
        // Run the backfill task, which has a number of steps to complete
        auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
        // backfill:create()
        runNextTask(lpAuxioQ);
        // backfill:scan()
        runNextTask(lpAuxioQ);
        // backfill:complete()
        runNextTask(lpAuxioQ);

        // 1 Extra step for persistent backfill
        if (engine->getConfiguration().getBucketType() != "ephemeral") {
            // backfill:finished()
            runNextTask(lpAuxioQ);
        }
    }

    // Next step which will process a snapshot marker and then the caller
    // should now be able to step through the checkpoint
    if (expectedOp != cb::mcbp::ClientOpcode::Invalid) {
        EXPECT_EQ(ENGINE_SUCCESS, producer.step(&producers));
        EXPECT_EQ(expectedOp, producers.last_op);
    } else {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, producer.step(&producers));
    }
}

void SingleThreadedKVBucketTest::runCheckpointProcessor(
        MockDcpProducer& producer, dcp_message_producers& producers) {
    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer.step(&producers));

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
    createDcpStream(producer, vbid);
}

void SingleThreadedKVBucketTest::createDcpStream(MockDcpProducer& producer,
                                                 Vbid vbid) {
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
                                     &dcpAddFailoverLog,
                                     {}));
}

void SingleThreadedKVBucketTest::runCompaction(uint64_t purgeBeforeTime,
                                               uint64_t purgeBeforeSeq) {
    CompactionConfig compactConfig;
    compactConfig.purge_before_ts = purgeBeforeTime;
    compactConfig.purge_before_seq = purgeBeforeSeq;
    compactConfig.drop_deletes = false;
    compactConfig.db_file_id = vbid;
    store->scheduleCompaction(vbid, compactConfig, nullptr);
    // run the compaction task
    runNextTask(*task_executor->getLpTaskQ()[WRITER_TASK_IDX],
                "Compact DB file 0");
}

void SingleThreadedKVBucketTest::runCollectionsEraser() {
    ASSERT_TRUE(engine->getConfiguration().getBucketType() == "persistent")
            << "No support for ephemeral collections erase";
    // run the compaction task. assuming it was scheduled by the test, will
    // fail the runNextTask expect if not scheduled.
    runNextTask(*task_executor->getLpTaskQ()[WRITER_TASK_IDX],
                "Compact DB file 0");
}

/*
 * MB-31175
 * The following test checks to see that when we call handleSlowStream in an
 * in memory state and drop the cursor/schedule a backfill as a result, the
 * resulting backfill checks the purgeSeqno and tells the stream to rollback
 * if purgeSeqno > startSeqno.
 */
TEST_P(STParameterizedBucketTest, SlowStreamBackfillPurgeSeqnoCheck) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_TRUE(vb.get());

    // Store two items
    std::array<std::string, 2> initialKeys = {{"k1", "k2"}};
    for (const auto& key : initialKeys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flushVBucketToDiskIfPersistent(vbid, initialKeys.size());

    // Delete the items so that we can advance the purgeSeqno using
    // compaction later
    for (const auto& key : initialKeys) {
        delete_item(vbid, makeStoredDocKey(key));
    }
    flushVBucketToDiskIfPersistent(vbid, initialKeys.size());

    auto& ckpt_mgr = *vb->checkpointManager;

    // Create a Mock Dcp producer
    // Create the Mock Active Stream with a startSeqno of 1
    // as a startSeqno is always valid
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
            /*st_seqno*/ 1,
            /*en_seqno*/ ~0,
            /*vb_uuid*/ 0xabcd,
            /*snap_start_seqno*/ 0,
            /*snap_end_seqno*/ ~0,
            IncludeValue::Yes,
            IncludeXattrs::Yes);

    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    mock_stream->transitionStateToBackfilling();
    ASSERT_TRUE(mock_stream->isInMemory())
    << "stream state should have transitioned to InMemory";

    // Check number of expected cursors (might not have persistence cursor)
    int expectedCursors = persistent() ? 2 : 1;
    EXPECT_EQ(expectedCursors, ckpt_mgr.getNumOfCursors());

    EXPECT_TRUE(mock_stream->handleSlowStream());
    EXPECT_TRUE(mock_stream->public_getPendingBackfill());

    // Might not have persistence cursor
    expectedCursors = persistent() ? 1 : 0;
    EXPECT_EQ(expectedCursors, ckpt_mgr.getNumOfCursors())
    << "stream cursor should have been dropped";

    // This will schedule the backfill
    mock_stream->transitionStateToBackfilling();
    ASSERT_TRUE(mock_stream->isBackfilling());

    // Advance the purgeSeqno
    if (persistent()) {
        runCompaction(~0, 3);
    } else {
        EphemeralVBucket::HTTombstonePurger purger(0);
        auto vbptr = store->getVBucket(vbid);
        EphemeralVBucket* evb = dynamic_cast<EphemeralVBucket*>(vbptr.get());
        purger.setCurrentVBucket(*evb);
        evb->ht.visit(purger);
        evb->purgeStaleItems();
    }

    ASSERT_EQ(3, vb->getPurgeSeqno());

    // Run the backfill we scheduled when we transitioned to the backfilling
    // state
    auto& bfm = producer->getBFM();
    bfm.backfill();

    // The backfill should have set the stream state to dead because
    // purgeSeqno > startSeqno
    EXPECT_TRUE(mock_stream->isDead());

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();

    cancelAndPurgeTasks();
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
    auto& ckpt_mgr = *vb->checkpointManager;

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

/**
 * The following test checks to see that if a cursor drop (and subsequent
 * re-registration) is a safe operation in that the background checkpoint
 * processor task cannot advance the streams cursor whilst backfilling is
 * occurring.
 *
 * Check to see that cursor dropping correctly handles the following scenario:
 *
 * 1. vBucket is state:in-memory. Cursor dropping occurs
 *    (ActiveStream::handleSlowStream)
 *   a. Cursor is removed
 *   b. pendingBackfill is set to true.
 * 2. However, assume that ActiveStreamCheckpointProcessorTask has a pending
 *    task for this vbid.
 * 3. ActiveStream changes from state:in-memory to state:backfilling.
 * 4. Backfill starts, re-registers cursor (ActiveStream::markDiskSnapshot) to
 *    resume from after the end of the backfill.
 * 5. ActiveStreamCheckpointProcessorTask wakes up, and finds the pending task
 *    for this vb. At this point the newly woken task should be blocked from
 *    doing any work (and return early).
 */
TEST_F(SingleThreadedEPBucketTest,
       MB29369_CursorDroppingPendingCkptProcessorTask) {
    // Create a Mock Dcp producer and schedule on executorpool.
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    producer->scheduleCheckpointProcessorTask();

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize()) << "Expected to have "
                                                   "ActiveStreamCheckpointProce"
                                                   "ssorTask in AuxIO Queue";

    // Create dcp_producer_snapshot_marker_yield_limit + 1 streams -
    // this means that we don't process all pending vBuckets on a single
    // execution of ActiveStreamCheckpointProcessorTask - which can result
    // in vBIDs being "left over" in ActiveStreamCheckpointProcessorTask::queue
    // after an execution.
    // This means that subsequently when we drop the cursor for this vb,
    // there's a "stale" job queued for it.
    const auto iterationLimit =
            engine->getConfiguration().getDcpProducerSnapshotMarkerYieldLimit();
    std::shared_ptr<MockActiveStream> stream;
    for (size_t id = 0; id < iterationLimit + 1; id++) {
        Vbid vbid = Vbid(id);
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        auto vb = store->getVBucket(vbid);
        stream = producer->mockActiveStreamRequest(/*flags*/ 0,
                                                   /*opaque*/ 0,
                                                   *vb,
                                                   /*st_seqno*/ 0,
                                                   /*en_seqno*/ ~0,
                                                   /*vb_uuid*/ 0xabcd,
                                                   /*snap_start_seqno*/ 0,
                                                   /*snap_end_seqno*/ ~0);

        // Request an item from each stream, so they all advance from
        // backfilling to in-memory
        auto result = stream->next();
        EXPECT_FALSE(result);
        EXPECT_TRUE(stream->isInMemory())
                << vbid << " should be state:in-memory at start";

        // Create an item and flush to disk (so ActiveStream::nextCheckpointItem
        // will have data available when call next() - and will add vb to
        // ActiveStreamCheckpointProcessorTask's queue.
        EXPECT_TRUE(queueNewItem(*vb, "key1"));
        EXPECT_EQ(std::make_pair(false, size_t(1)),
                  getEPBucket().flushVBucket(vbid));

        // And then request another item, to add the VBID to
        // ActiveStreamCheckpointProcessorTask's queue.
        result = stream->next();
        EXPECT_FALSE(result);
        EXPECT_EQ(id + 1, producer->getCheckpointSnapshotTask().queueSize())
                << "Should have added " << vbid << " to ProcessorTask queue";
    }

    // Should now have dcp_producer_snapshot_marker_yield_limit + 1 items
    // in ActiveStreamCheckpointProcessorTask's pending VBs.
    EXPECT_EQ(iterationLimit + 1,
              producer->getCheckpointSnapshotTask().queueSize())
            << "Should have all vBuckets in ProcessorTask queue";

    // Use last Stream as the one we're going to drop the cursor on (this is
    // also at the back of the queue).
    auto vb = store->getVBuckets().getBucket(Vbid(iterationLimit));
    auto& ckptMgr = *vb->checkpointManager;

    // 1. Now trigger cursor dropping for this stream.
    EXPECT_TRUE(stream->handleSlowStream());
    EXPECT_TRUE(stream->isInMemory())
            << "should be state:in-memory immediately after handleSlowStream";
    EXPECT_EQ(1, ckptMgr.getNumOfCursors()) << "Should only have persistence "
                                               "cursor registered after "
                                               "cursor dropping.";

    // 2. Request next item from stream. Will transition to backfilling as part
    // of this.
    auto result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_TRUE(stream->isBackfilling()) << "should be state:backfilling "
                                            "after next() following "
                                            "handleSlowStream";

    // *Key point*:
    //
    // ActiveStreamCheckpointProcessorTask and Backfilling task are both
    // waiting to run. However, ActiveStreamCheckpointProcessorTask
    // has more than iterationLimit VBs in it, so when it runs it won't
    // handle them all; and will sleep with the last VB remaining.
    // If the Backfilling task then runs, which returns a disk snapshot and
    // re-registers the cursor; we still have an
    // ActiveStreamCheckpointProcessorTask outstanding with the vb in the queue.
    EXPECT_EQ(2, lpAuxioQ.getFutureQueueSize());

    // Run the ActiveStreamCheckpointProcessorTask; which should re-schedule
    // due to having items outstanding.
    runNextTask(lpAuxioQ,
                "Process checkpoint(s) for DCP producer test_producer");

    // Now run backfilling task.
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // After Backfilltask scheduled create(); should have received a disk
    // snapshot; which in turn calls markDiskShapshot to re-register cursor.
    EXPECT_EQ(2, ckptMgr.getNumOfCursors()) << "Expected both persistence and "
                                               "replication cursors after "
                                               "markDiskShapshot";

    result = stream->next();
    ASSERT_TRUE(result);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, result->getEvent())
            << "Expected Snapshot marker after running backfill task.";

    // Add another item to the VBucket; after the cursor has been re-registered.
    EXPECT_TRUE(queueNewItem(*vb, "key2"));

    // Now run chkptProcessorTask to complete it's queue. With the bug, this
    // results in us discarding the last item we just added to vBucket.
    runNextTask(lpAuxioQ,
                "Process checkpoint(s) for DCP producer test_producer");

    // Let the backfill task complete running (it requires multiple steps to
    // complete).
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // Validate. We _should_ get two mutations: key1 & key2, but we have to
    // respin the checkpoint task for key2
    result = stream->next();
    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key1", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected Event::Mutation named 'key1'";
    }

    // No items ready, but this should of rescheduled vb10
    EXPECT_EQ(nullptr, stream->next());
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize())
            << "Should have 1 vBucket in ProcessorTask queue";

    // Now run chkptProcessorTask to complete it's queue, this will now be able
    // to access the checkpoint and get key2
    runNextTask(lpAuxioQ,
                "Process checkpoint(s) for DCP producer test_producer");

    result = stream->next();
    ASSERT_TRUE(result);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, result->getEvent())
            << "Expected Snapshot marker after running snapshot task.";
    result = stream->next();

    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key2", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected second Event::Mutation named 'key2'";
    }

    result = stream->next();
    EXPECT_FALSE(result) << "Expected no more than 2 mutatons.";

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

// Test is demonstrating that if a checkpoint processor scheduled by a stream
// that is subsequently closed/re-created, if that checkpoint processor runs
// whilst the new stream is backfilling, it can't interfere with the new stream.
// This issue was raised by MB-29585 but is fixed by MB-29369
TEST_F(SingleThreadedEPBucketTest, MB29585_backfilling_whilst_snapshot_runs) {
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    producer->scheduleCheckpointProcessorTask();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_EQ(1, lpAuxioQ.getFutureQueueSize()) << "Expected to have "
                                                   "ActiveStreamCheckpointProce"
                                                   "ssorTask in AuxIO Queue";

    // Create first stream
    auto vb = store->getVBucket(vbid);
    auto stream = producer->mockActiveStreamRequest(/*flags*/ 0,
                                                    /*opaque*/ 0,
                                                    *vb,
                                                    /*st_seqno*/ 0,
                                                    /*en_seqno*/ ~0,
                                                    /*vb_uuid*/ 0xabcd,
                                                    /*snap_start_seqno*/ 0,
                                                    /*snap_end_seqno*/ ~0);

    // Write an item
    EXPECT_TRUE(queueNewItem(*vb, "key1"));
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    // Request an item from the stream, so it advances from to in-memory
    auto result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_TRUE(stream->isInMemory());

    // Now step the in-memory stream to schedule the checkpoint task
    result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask().queueSize());

    // Now close the stream
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(0 /*opaque*/, vbid));

    // Next we to ensure the recreated stream really does a backfill, so drop
    // in-memory items
    bool newcp;
    vb->checkpointManager->createNewCheckpoint();
    // Force persistence into new CP
    queueNewItem(*vb, "key2");
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    EXPECT_EQ(1,
              vb->checkpointManager->removeClosedUnrefCheckpoints(*vb, newcp));

    // Now store another item, without MB-29369 fix we would lose this item
    store_item(vbid, makeStoredDocKey("key3"), "value");

    // Re-create the new stream
    stream = producer->mockActiveStreamRequest(/*flags*/ 0,
                                               /*opaque*/ 0,
                                               *vb,
                                               /*st_seqno*/ 0,
                                               /*en_seqno*/ ~0,
                                               /*vb_uuid*/ 0xabcd,
                                               /*snap_start_seqno*/ 0,
                                               /*snap_end_seqno*/ ~0);

    // Step the stream which will now schedule a backfill
    result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_TRUE(stream->isBackfilling());

    // Next we must deque, but not run the snapshot task, we will interleave it
    // with backfill later
    CheckedExecutor checkpointTask(task_executor, lpAuxioQ);
    EXPECT_STREQ("Process checkpoint(s) for DCP producer test_producer",
                 checkpointTask.getTaskName().data());

    // Now start the backfilling task.
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // After Backfilltask scheduled create(); should have received a disk
    // snapshot; which in turn calls markDiskShapshot to re-register cursor.
    EXPECT_EQ(2, vb->checkpointManager->getNumOfCursors())
            << "Expected persistence + replication cursors after "
               "markDiskShapshot";

    result = stream->next();
    ASSERT_TRUE(result);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, result->getEvent())
            << "Expected Snapshot marker after running backfill task.";

    // Let the backfill task complete running through its various states
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");
    runNextTask(lpAuxioQ, "Backfilling items for a DCP Connection");

    // Now run the checkpoint processor task, whilst still backfilling
    // With MB-29369 this should be safe
    checkpointTask.runCurrentTask(
            "Process checkpoint(s) for DCP producer test_producer");
    checkpointTask.completeCurrentTask();

    // Poke another item in
    store_item(vbid, makeStoredDocKey("key4"), "value");

    // Finally read back all the items and we should get two snapshots and
    // key1/key2 key3/key4
    result = stream->next();
    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key1", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected Event::Mutation named 'key1'";
    }

    result = stream->next();
    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key2", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected Event::Mutation named 'key2'";
    }

    runNextTask(lpAuxioQ,
                "Process checkpoint(s) for DCP producer test_producer");

    result = stream->next();
    ASSERT_TRUE(result);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, result->getEvent())
            << "Expected Snapshot marker after running snapshot task.";

    result = stream->next();
    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key3", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected Event::Mutation named 'key3'";
    }

    result = stream->next();
    if (result && result->getEvent() == DcpResponse::Event::Mutation) {
        auto* mutation = dynamic_cast<MutationResponse*>(result.get());
        EXPECT_STREQ("key4", mutation->getItem()->getKey().c_str());
    } else {
        FAIL() << "Expected Event::Mutation named 'key4'";
    }

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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      testName,
                                                      /*flags*/ 0);

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
            [vb, &registerCursorCount]() {
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
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
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
        auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                          cookie,
                                                          "test_producer",
                                                          /*flags*/ 0);

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
                SingleThreadedEPBucketTest::fakeDcpAddFailoverLog,
                {});

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

        std::string getDescription() {
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
                              cb::mcbp::request::DcpOpenPayload::Producer,
                              name));

    uint64_t rollbackSeqno;
    auto dummy_dcp_add_failover_cb = [](vbucket_failover_t* entry,
                                        size_t nentries,
                                        gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    };

    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine.get()->stream_req(cookie,
                                       /*flags*/ 0,
                                       /*opaque*/ 0,
                                       /*vbucket*/ vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ -1,
                                       /*vb_uuid*/ 0,
                                       /*snap_start*/ 0,
                                       /*snap_end*/ 0,
                                       &rollbackSeqno,
                                       dummy_dcp_add_failover_cb,
                                       {}));
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
        const DocKey docKey{key, DocKeyEncodesCollectionId::No};
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

/**
 * MB-29861: Ensure that a delete time is generated for a document
 * that is received on the consumer side as a result of a disk
 * backfill
 */
TEST_F(SingleThreadedEPBucketTest, MB_29861) {
    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Create a MockDcpConsumer
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "test");

    // Add the stream
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // 1. Add the first message, a snapshot marker to ensure that the
    //    vbucket goes to the backfill state
    consumer->snapshotMarker(/*opaque*/ 1,
                             vbid,
                             /*startseq*/ 0,
                             /*endseq*/ 2,
                             /*flags*/ MARKER_FLAG_DISK);

    // 2. Now add a deletion.
    consumer->deletion(/*opaque*/ 1,
                       {"key1", DocKeyEncodesCollectionId::No},
                       /*value*/ {},
                       /*priv_bytes*/ 0,
                       /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                       /*cas*/ 0,
                       /*vbucket*/ vbid,
                       /*bySeqno*/ 1,
                       /*revSeqno*/ 0,
                       /*meta*/ {});

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    // Drop the stream
    consumer->closeStream(/*opaque*/ 0, vbid);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Now read back and verify key1 has a non-zero delete time
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
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, datatype);
    EXPECT_NE(0, metadata.exptime); // A locally created deleteTime
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
    // 2. Now add two deletions, one without deleteTime, one with
    consumer->deletionV2(/*opaque*/ 1,
                         {"key1", DocKeyEncodesCollectionId::No},
                         /*values*/ {},
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                         /*cas*/ 0,
                         /*vbucket*/ vbid,
                         /*bySeqno*/ 1,
                         /*revSeqno*/ 0,
                         /*deleteTime*/ 0);

    const uint32_t deleteTime = 10;
    consumer->deletionV2(/*opaque*/ 1,
                         {"key2", DocKeyEncodesCollectionId::No},
                         /*value*/ {},
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
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
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, datatype);
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
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, datatype);
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
                              cb::mcbp::request::DcpOpenPayload::Producer,
                              name));

    // ActiveStreamCheckpointProcessorTask and DCPBackfill task are created
    // when the first DCP stream is created.
    EXPECT_EQ(0, lpAuxioQ->getFutureQueueSize());
    EXPECT_EQ(0, lpAuxioQ->getReadyQueueSize());

    uint64_t rollbackSeqno;
    // Actual stream request method (EvpDcpStreamReq) is static, so access via
    // the engine_interface.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->stream_req(cookie,
                                 /*flags*/ 0,
                                 /*opaque*/ 0,
                                 /*vbucket*/ vbid,
                                 /*start_seqno*/ 0,
                                 /*end_seqno*/ -1,
                                 /*vb_uuid*/ 0,
                                 /*snap_start*/ 0,
                                 /*snap_end*/ 0,
                                 &rollbackSeqno,
                                 dummy_dcp_add_failover_cb,
                                 {}));

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
    engine->destroyInner(/*force*/ false);

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

    ExTask hpTask = std::make_shared<TestTask>(engine->getTaskable(),
                                               TaskId::PendingOpsNotification);
    task_executor->schedule(hpTask);

    ExTask lpTask = std::make_shared<TestTask>(engine->getTaskable(),
                                               TaskId::DefragmenterTask);
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
            EXPECT_EQ(ENGINE_SUCCESS, err) << "Unexpected error code";
            producer->closeStream(/*opaque*/0, /*vbucket*/vbid);
        } else {
            EXPECT_EQ(ENGINE_NOT_MY_VBUCKET, err) << "Unexpected error code";
        }

        // Stop Producer checkpoint processor task
        producer->cancelCheckpointCreatorTask();
    }
}

class XattrSystemUserTest : public SingleThreadedEPBucketTest,
                            public ::testing::WithParamInterface<bool> {
};

TEST_P(XattrSystemUserTest, pre_expiry_xattrs) {
    auto& kvbucket = *engine->getKVBucket();

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto xattr_data = createXattrValue("value", GetParam());

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
    cb::xattr::Blob new_blob(value_buf, false);

    // If testing with system xattrs
    if (GetParam()) {
        const std::string& cas_str{"{\"cas\":\"0xdeadbeefcafefeed\"}"};
        const std::string& sync_str = to_string(new_blob.get("_sync"));

        EXPECT_EQ(cas_str, sync_str) << "Unexpected system xattrs";
    }
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
    static void rewriteVBStateAs25x(Vbid vbucket) {
        std::string filename = std::string(test_dbname) + "/" +
                               std::to_string(vbucket.get()) + ".couch.1";
        Db* handle;
        couchstore_error_t err = couchstore_open_db(
                filename.c_str(), COUCHSTORE_OPEN_FLAG_CREATE, &handle);

        ASSERT_EQ(COUCHSTORE_SUCCESS, err) << "Failed to open new database";

        // Create a 2.5 _local/vbstate
        // Note: adding 'collections_supported' keeps these tests running
        // when collections is enabled, a true offline upgrade would do this
        // as well (as long as couchfile_upgrade was invoked).
        std::string vbstate2_5_x =
                R"({"state": "active",
                    "checkpoint_id": "1",
                    "max_deleted_seqno": "0",
                    "collections_supported":true})";
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
        shutdownAndPurgeTasks(engine.get());
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

// Test that the FreqSaturatedCallback of a vbucket is initialized and after
// warmup is set to the "wakeup" function of ItemFreqDecayerTask.
TEST_F(WarmupTest, setFreqSaturatedCallback) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // The FreqSaturatedCallback should be initialised
    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        EXPECT_TRUE(vb->ht.getFreqSaturatedCallback());
    }
    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteVBStateAs25x(vbid);

    // Resetting the engine and running warmup will result in the
    // Warmup::createVBuckets being invoked for vbid.
    resetEngineAndWarmup();

    dynamic_cast<MockEPBucket*>(store)->createItemFreqDecayerTask();
    auto itemFreqTask =
            dynamic_cast<MockEPBucket*>(store)->getMockItemFreqDecayerTask();
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // The FreqSaturatedCallback should be initialised
    EXPECT_TRUE(vb->ht.getFreqSaturatedCallback());
    ASSERT_FALSE(itemFreqTask->wakeupCalled);
    // We now invoke the FreqSaturatedCallback function.
    vb->ht.getFreqSaturatedCallback()();
    // This should have resulted in calling the wakeup function of the
    // MockItemFreqDecayerTask.
    EXPECT_TRUE(itemFreqTask->wakeupCalled);
}

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

    resetEngineAndWarmup("compression_mode=off");
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

    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
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

// Combine warmup and DCP so we can check deleteTimes come back from disk
TEST_F(WarmupTest, produce_delete_times) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto t1 = ep_real_time();
    storeAndDeleteItem(vbid, {"KEY1", DocKeyEncodesCollectionId::No}, "value");
    auto t2 = ep_real_time();
    // Now warmup to ensure that DCP will have to go to disk.
    resetEngineAndWarmup();

    auto cookie = create_mock_cookie();
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    MockDcpMessageProducers producers(engine.get());

    createDcpStream(*producer);

    // noop off as we will play with time travel
    producer->setNoopEnabled(false);

    auto step = [this, producer, &producers](bool inMemory) {
        notifyAndStepToCheckpoint(*producer,
                                  producers,
                                  cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                                  inMemory);

        // Now step the producer to transfer the delete/tombstone
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    };

    step(false);
    EXPECT_NE(0, producers.last_delete_time);
    EXPECT_GE(producers.last_delete_time, t1);
    EXPECT_LE(producers.last_delete_time, t2);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpDeletion, producers.last_op);
    EXPECT_EQ("KEY1", producers.last_key);
    size_t expectedBytes = SnapshotMarker::baseMsgBytes +
                           MutationResponse::deletionV2BaseMsgBytes +
                           (sizeof("KEY1") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());

    // Now a new delete, in-memory will also have a delete time
    t1 = ep_real_time();
    storeAndDeleteItem(vbid, {"KEY2", DocKeyEncodesCollectionId::No}, "value");
    t2 = ep_real_time();

    step(true);

    EXPECT_NE(0, producers.last_delete_time);
    EXPECT_GE(producers.last_delete_time, t1);
    EXPECT_LE(producers.last_delete_time, t2);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpDeletion, producers.last_op);
    EXPECT_EQ("KEY2", producers.last_key);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     MutationResponse::deletionV2BaseMsgBytes +
                     (sizeof("KEY2") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());

    // Finally expire a key and check that the delete_time we receive is the
    // expiry time, not actually the time it was deleted.
    auto expiryTime = ep_real_time() + 32000;
    store_item(
            vbid, {"KEY3", DocKeyEncodesCollectionId::No}, "value", expiryTime);

    step(true);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     MutationResponse::mutationBaseMsgBytes +
                     (sizeof("value") - 1) + (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());

    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    TimeTraveller arron(64000);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocKeyEncodesCollectionId::No}, vbid, cookie, NONE);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    step(true);

    EXPECT_EQ(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpDeletion, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     MutationResponse::deletionV2BaseMsgBytes +
                     (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());

    destroy_mock_cookie(cookie);
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}

// MB-26907
TEST_P(STParameterizedBucketTest, enable_expiry_output) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto cookie = create_mock_cookie();
    auto producer = createDcpProducer(cookie, IncludeDeleteTime::Yes);
    MockDcpMessageProducers producers(engine.get());

    createDcpStream(*producer);

    // noop off as we will play with time travel
    producer->setNoopEnabled(false);
    // Enable DCP Expiry opcodes
    producer->setDCPExpiry(true);

    auto step = [this, producer, &producers](bool inMemory) {
        notifyAndStepToCheckpoint(*producer,
                                  producers,
                                  cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                                  inMemory);

        // Now step the producer to transfer the delete/tombstone
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    };

    // Expire a key and check that the delete_time we receive is the
    // expiry time, not actually the time it was deleted.
    auto expiryTime = ep_real_time() + 32000;
    store_item(
            vbid, {"KEY3", DocKeyEncodesCollectionId::No}, "value", expiryTime);

    step(true);
    size_t expectedBytes = SnapshotMarker::baseMsgBytes +
                           MutationResponse::mutationBaseMsgBytes +
                           (sizeof("value") - 1) + (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());
    EXPECT_EQ(1, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    TimeTraveller arron(64000);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocKeyEncodesCollectionId::No}, vbid, cookie, NONE);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    step(true);

    EXPECT_EQ(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     MutationResponse::deletionV2BaseMsgBytes +
                     (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());
    EXPECT_EQ(0, store->getVBucket(vbid)->getNumItems());

    destroy_mock_cookie(cookie);
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}

TEST_P(XattrSystemUserTest, MB_29040) {
    auto& kvbucket = *engine->getKVBucket();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid,
               {"key", DocKeyEncodesCollectionId::No},
               createXattrValue("{}", GetParam()),
               ep_real_time() + 1 /*1 second TTL*/,
               {cb::engine_errc::success},

               PROTOCOL_BINARY_DATATYPE_XATTR | PROTOCOL_BINARY_DATATYPE_JSON);

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    TimeTraveller ted(64000);
    runCompaction();
    // An expired item should of been pushed to the checkpoint
    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));
    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    GetValue gv = kvbucket.get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);

    gv = kvbucket.get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    auto get_itm = gv.item.get();
    auto get_data = const_cast<char*>(get_itm->getData());

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf, false);

    // If testing with system xattrs
    if (GetParam()) {
        const std::string& cas_str{"{\"cas\":\"0xdeadbeefcafefeed\"}"};
        const std::string& sync_str = to_string(new_blob.get("_sync"));

        EXPECT_EQ(cas_str, sync_str) << "Unexpected system xattrs";
        EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_XATTR, get_itm->getDataType())
                << "Wrong datatype Item:" << *get_itm;
    } else {
        EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, get_itm->getDataType())
                << "Wrong datatype Item:" << *get_itm;
    }

    // Non-system xattrs should be removed
    EXPECT_TRUE(new_blob.get("user").empty())
            << "The user attribute should be gone";
    EXPECT_TRUE(new_blob.get("meta").empty())
            << "The meta attribute should be gone";
}

class MB_29287 : public SingleThreadedEPBucketTest {
public:
    void SetUp() override {
        SingleThreadedEPBucketTest::SetUp();
        cookie = create_mock_cookie();
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

        // 1. Mock producer
        producer = std::make_shared<MockDcpProducer>(
                *engine, cookie, "test_producer", 0);
        producer->createCheckpointProcessorTask();

        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        auto vb = store->getVBuckets().getBucket(vbid);
        ASSERT_NE(nullptr, vb.get());
        // 2. Mock active stream
        producer->mockActiveStreamRequest(0, // flags
                                          1, // opaque
                                          *vb,
                                          0, // start_seqno
                                          ~0, // end_seqno
                                          0, // vbucket_uuid,
                                          0, // snap_start_seqno,
                                          0); // snap_end_seqno,

        store_item(vbid, makeStoredDocKey("1"), "value1");
        store_item(vbid, makeStoredDocKey("2"), "value2");
        store_item(vbid, makeStoredDocKey("3"), "value3");
        flush_vbucket_to_disk(vbid, 3);
        notifyAndStepToCheckpoint(*producer, *producers);

        for (int i = 0; i < 3; i++) { // 1, 2 and 3
            EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        }

        store_item(vbid, makeStoredDocKey("4"), "value4");

        auto stream = producer->findStream(vbid);
        auto* mockStream = static_cast<MockActiveStream*>(stream.get());
        mockStream->preGetOutstandingItemsCallback =
                std::bind(&MB_29287::closeAndRecreateStream, this);

        // call next - get success (nothing ready, but task has been scheduled)
        EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

        // Run the snapshot task and step (triggering
        // preGetOutstandingItemsCallback)
        notifyAndStepToCheckpoint(*producer, *producers);
    }

    void TearDown() override {
        destroy_mock_cookie(cookie);
        producer->closeAllStreams();
        producer->cancelCheckpointCreatorTask();
        producer.reset();
        SingleThreadedEPBucketTest::TearDown();
    }

    void closeAndRecreateStream() {
        // Without the fix, 5 will be lost
        store_item(vbid, makeStoredDocKey("5"), "don't lose me");
        producer->closeStream(1, Vbid(0));
        auto vb = store->getVBuckets().getBucket(vbid);
        ASSERT_NE(nullptr, vb.get());
        producer->mockActiveStreamRequest(DCP_ADD_STREAM_FLAG_TAKEOVER,
                                          1, // opaque
                                          *vb,
                                          3, // start_seqno
                                          ~0, // end_seqno
                                          vb->failovers->getLatestUUID(),
                                          3, // snap_start_seqno
                                          ~0); // snap_end_seqno
    }

    const void* cookie = nullptr;
    std::shared_ptr<MockDcpProducer> producer;
    std::unique_ptr<MockDcpMessageProducers> producers;
};

// NEXT two test are TEMP disabled as this commit will cause a deadlock
// because the same thread is calling back with streamMutex held onto a function
// which wants to acquire...

// Stream takeover with no more writes
TEST_F(MB_29287, DISABLED_dataloss_end) {
    auto stream = producer->findStream(vbid);
    auto* as = static_cast<ActiveStream*>(stream.get());

    EXPECT_TRUE(stream->isTakeoverSend());
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    producers->last_op = cb::mcbp::ClientOpcode::Invalid;
    EXPECT_EQ("4", producers->last_key);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    producers->last_op = cb::mcbp::ClientOpcode::Invalid;
    EXPECT_EQ("5", producers->last_key);

    // Snapshot received
    as->snapshotMarkerAckReceived();

    // set-vb-state now underway
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers->last_op);

    // Move stream to pending and vb to dead
    as->setVBucketStateAckRecieved();

    // Cannot store anymore items
    store_item(vbid,
               makeStoredDocKey("K6"),
               "value6",
               0,
               {cb::engine_errc::not_my_vbucket});

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers->last_op);
    as->setVBucketStateAckRecieved();
    EXPECT_TRUE(!stream->isActive());

    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    // Have persistence cursor only (dcp now closed down)
    EXPECT_EQ(1, vb->checkpointManager->getNumOfCursors());
}

// takeover when more writes occur
TEST_F(MB_29287, DISABLED_dataloss_hole) {
    auto stream = producer->findStream(vbid);
    auto* as = static_cast<ActiveStream*>(stream.get());

    store_item(vbid, makeStoredDocKey("6"), "value6");

    EXPECT_TRUE(stream->isTakeoverSend());
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    producers->last_op = cb::mcbp::ClientOpcode::Invalid;
    EXPECT_EQ("4", producers->last_key);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    producers->last_op = cb::mcbp::ClientOpcode::Invalid;
    EXPECT_EQ("5", producers->last_key);

    // Snapshot received
    as->snapshotMarkerAckReceived();

    // More data in the checkpoint (key 6)

    // call next - get success (nothing ready, but task has been scheduled)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    // Run the snapshot task and step
    notifyAndStepToCheckpoint(*producer, *producers);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("6", producers->last_key);

    // Snapshot received
    as->snapshotMarkerAckReceived();

    // Now send
    EXPECT_TRUE(stream->isTakeoverSend());

    // set-vb-state now underway
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers->last_op);
    producers->last_op = cb::mcbp::ClientOpcode::Invalid;

    // Move stream to pending and vb to dead
    as->setVBucketStateAckRecieved();

    // Cannot store anymore items
    store_item(vbid,
               makeStoredDocKey("K6"),
               "value6",
               0,
               {cb::engine_errc::not_my_vbucket});

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers->last_op);
    as->setVBucketStateAckRecieved();
    EXPECT_TRUE(!stream->isActive());

    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    // Have persistence cursor only (dcp now closed down)
    EXPECT_EQ(1, vb->checkpointManager->getNumOfCursors());
}

class XattrCompressedTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<::testing::tuple<bool, bool>> {
public:
    bool isXattrSystem() const {
        return ::testing::get<0>(GetParam());
    }
    bool isSnappy() const {
        return ::testing::get<1>(GetParam());
    }
};

// Create a replica VB and consumer, then send it an xattr value which should
// of been stripped at the source, but wasn't because of MB29040. Then check
// the consumer sanitises the document. Run the test with user/system xattrs
// and snappy on/off
TEST_P(XattrCompressedTest, MB_29040_sanitise_input) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookie, "MB_29040_sanitise_input");
    int opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, /*flags*/ 0));

    std::string body;
    if (!isXattrSystem()) {
        body.assign("value");
    }
    auto value = createXattrValue(body, isXattrSystem(), isSnappy());

    // Send deletion in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, vbid, bySeqno, bySeqno, MARKER_FLAG_CHK));

    cb::const_byte_buffer valueBuf{
            reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    EXPECT_EQ(
            ENGINE_SUCCESS,
            consumer->deletion(
                    opaque,
                    {"key", DocKeyEncodesCollectionId::No},
                    valueBuf,
                    /*priv_bytes*/ 0,
                    PROTOCOL_BINARY_DATATYPE_XATTR |
                            (isSnappy() ? PROTOCOL_BINARY_DATATYPE_SNAPPY : 0),
                    /*cas*/ 3,
                    vbid,
                    bySeqno,
                    /*revSeqno*/ 0,
                    /*meta*/ {}));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    // Switch to active
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    gv = store->get({"key", DocKeyEncodesCollectionId::No},
                    vbid,
                    cookie,
                    GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // This is the only system key test_helpers::createXattrValue gives us
    cb::xattr::Blob blob;
    blob.set("_sync", "{\"cas\":\"0xdeadbeefcafefeed\"}");

    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(0, gv.item->getFlags());
    EXPECT_EQ(3, gv.item->getCas());
    EXPECT_EQ(isXattrSystem() ? blob.size() : 0,
              gv.item->getValue()->valueSize());
    EXPECT_EQ(isXattrSystem() ? PROTOCOL_BINARY_DATATYPE_XATTR
                              : PROTOCOL_BINARY_RAW_BYTES,
              gv.item->getDataType());
}

// Create a replica VB and consumer, then send it an delete with value which
// should never of been created on the source.
TEST_F(SingleThreadedEPBucketTest, MB_31141_sanitise_input) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookie, "MB_31141_sanitise_input");
    int opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, /*flags*/ 0));

    std::string body = "value";

    // Send deletion in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, vbid, bySeqno, bySeqno, MARKER_FLAG_CHK));

    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->deletion(opaque,
                                 {"key", DocKeyEncodesCollectionId::No},
                                 {reinterpret_cast<const uint8_t*>(body.data()),
                                  body.size()},
                                 /*priv_bytes*/ 0,
                                 PROTOCOL_BINARY_DATATYPE_SNAPPY |
                                         PROTOCOL_BINARY_RAW_BYTES,
                                 /*cas*/ 3,
                                 vbid,
                                 bySeqno,
                                 /*revSeqno*/ 0,
                                 /*meta*/ {}));

    EXPECT_EQ(std::make_pair(false, size_t(1)),
              getEPBucket().flushVBucket(vbid));

    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    // Switch to active
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    get_options_t options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    gv = store->get({"key", DocKeyEncodesCollectionId::No},
                    vbid,
                    cookie,
                    GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(0, gv.item->getFlags());
    EXPECT_EQ(3, gv.item->getCas());
    EXPECT_EQ(0, gv.item->getValue()->valueSize());
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, gv.item->getDataType());
}

// Test highlighting MB_29480 - this is not demonstrating the issue is fixed.
TEST_F(SingleThreadedEPBucketTest, MB_29480) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    producer->createCheckpointProcessorTask();

    MockDcpMessageProducers producers(engine.get());

    producer->mockActiveStreamRequest(0, // flags
                                      1, // opaque
                                      *vb,
                                      0, // start_seqno
                                      ~0, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0); // snap_end_seqno,

    // 1) First store 5 keys
    std::array<std::string, 2> initialKeys = {{"k1", "k2"}};
    for (const auto& key : initialKeys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flush_vbucket_to_disk(vbid, initialKeys.size());

    // 2) And receive them, client knows of k1,k2,k3,k4,k5
    notifyAndStepToCheckpoint(*producer, producers);
    for (const auto& key : initialKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(key, producers.last_key);
        producers.last_op = cb::mcbp::ClientOpcode::Invalid;
    }

    auto stream = producer->findStream(vbid);
    auto* mock_stream = static_cast<MockActiveStream*>(stream.get());

    // 3) Next delete k1/k2, compact (purging the tombstone)
    // NOTE: compaction will not purge a tombstone if it is the highest item
    // in the seqno index, hence why k1 will be purged but k2 won't
    for (const auto& key : initialKeys) {
        delete_item(vbid, makeStoredDocKey(key));
    }
    flush_vbucket_to_disk(vbid, initialKeys.size());

    // 4) Compact drop tombstones less than time=maxint and below seqno 3
    // as per earlier comment, only seqno 1 will be purged...
    runCompaction(~0, 3);

    // 5) Begin cursor dropping
    mock_stream->handleSlowStream();

    // Kick the stream into backfill
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    // 6) Store more items (don't flush these)
    std::array<std::string, 2> extraKeys = {{"k3", "k4"}};
    for (const auto& key : extraKeys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    EXPECT_TRUE(vb0Stream->isBackfilling());

    // 7) Backfill now starts up, but should quickly cancel
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX]);

    // Stream is now dead
    EXPECT_FALSE(vb0Stream->isActive());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

// MB-29512: Ensure if compaction ran in between stream-request and backfill
// starting, we don't backfill from before the purge-seqno.
TEST_F(SingleThreadedEPBucketTest, MB_29512) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    // Create a Mock Dcp producer
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);

    producer->createCheckpointProcessorTask();

    MockDcpMessageProducers producers(engine.get());

    // 1) First store k1/k2 (creating seq 1 and seq 2)
    std::array<std::string, 2> initialKeys = {{"k1", "k2"}};
    for (const auto& key : initialKeys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flush_vbucket_to_disk(vbid, initialKeys.size());

    // Assume the DCP client connects here and receives seq 1 and 2 then drops

    // 2) delete k1/k2 (creating seq 3 and seq 4)
    for (const auto& key : initialKeys) {
        delete_item(vbid, makeStoredDocKey(key));
    }
    flush_vbucket_to_disk(vbid, initialKeys.size());

    // Disk index now has two items, seq3 and seq4 (deletes of k1/k2)

    // 3) Force all memory items out so DCP will definitely go to disk and
    //    not memory.
    bool newcp;
    vb->checkpointManager->createNewCheckpoint();
    // Force persistence into new CP
    store_item(vbid, makeStoredDocKey("k3"), "k3");
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(2,
              vb->checkpointManager->removeClosedUnrefCheckpoints(*vb, newcp));

    // 4) Stream request picking up where we left off.
    uint64_t rollbackSeqno = 0;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vb->getId(),
                                      2, // start_seqno
                                      ~0, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno,
                                      2,
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      {})); // snap_end_seqno,

    // 5) Now compaction kicks in, which will purge the deletes of k1/k2 setting
    //    the purgeSeqno to seq 4 (the last purged seqno)
    runCompaction(~0, 5);

    EXPECT_EQ(vb->getPurgeSeqno(), 4);

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    EXPECT_TRUE(vb0Stream->isBackfilling());

    // 6) Backfill now starts up, but should quickly cancel
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX]);

    EXPECT_FALSE(vb0Stream->isActive());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

TEST_F(SingleThreadedEPBucketTest, MB_29541) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // 1) First store 2 keys which we will backfill
    std::array<std::string, 2> keys = {{"k1", "k2"}};
    for (const auto& key : keys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }
    flush_vbucket_to_disk(vbid, keys.size());

    // Simplest way to ensure DCP has todo a backfill - 'wipe memory'
    resetEngineAndWarmup();

    // Setup DCP, 1 producer and we will do a takeover of the vbucket
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "mb-29541",
                                                      /*flags*/ 0);

    producer->createCheckpointProcessorTask();

    MockDcpMessageProducers producers(engine.get());

    uint64_t rollbackSeqno = 0;
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(DCP_ADD_STREAM_FLAG_TAKEOVER, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      vb->getHighSeqno(), // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno
                                      vb->getHighSeqno(), // snap_end_seqno
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      {}));

    // This MB also relies on the consumer draining the stream as the backfill
    // runs, rather than running the backfill then sequentially then draining
    // the readyQ, basically when backfill complete occurs we should have
    // shipped all items to ensure the state transition to takeover-send would
    // indeed block (unless we have the fix applied...)

    // Manually drive the backfill (not using notifyAndStepToCheckpoint)

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()
    runNextTask(lpAuxioQ);

    // Now drain all items before we proceed to complete
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    for (const auto& key : keys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        EXPECT_EQ(key, producers.last_key);
    }

    // backfill:complete()
    runNextTask(lpAuxioQ);
    // backfill:finished()
    runNextTask(lpAuxioQ);

    producers.last_op = cb::mcbp::ClientOpcode::Invalid;

    // Next the backfill should switch to takeover-send and progress to close
    // with the correct sequence of step/ack

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());
    // However without the fix from MB-29541 this would return success, meaning
    // the front-end thread should sleep until notified the stream is ready.
    // However no notify will ever come if MB-29541 is not applied
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers.last_op);

    EXPECT_TRUE(vb0Stream->isTakeoverWait());

    // For completeness step to end
    // we must ack the VB state
    protocol_binary_response_header message;
    message.response.setMagic(cb::mcbp::Magic::ClientResponse);
    message.response.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    message.response.setOpaque(1);
    EXPECT_TRUE(producer->handleResponse(&message));

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSetVbucketState, producers.last_op);

    EXPECT_TRUE(producer->handleResponse(&message));
    EXPECT_FALSE(vb0Stream->isActive());
    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

/* When a backfill is activated along with a slow stream trigger,
 * the stream end message gets stuck in the readyQ as the stream is
 * never notified as ready to send it. As the stream transitions state
 * to InMemory as well as having sent all requested sequence numbers,
 * the stream is meant to end but Stream::itemsReady can cause this
 * to never trigger. This means that DCP consumers can hang waiting
 * for this closure message.
 * This test checks that the DCP stream actually sends the end stream
 * message when triggering this problematic sequence.
 */
TEST_F(SingleThreadedEPBucketTest, MB_31481) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // 1) First store 2 keys which we will backfill
    std::array<std::string, 2> keys = {{"k1", "k2"}};
    store_item(vbid, makeStoredDocKey(keys[0]), keys[0]);
    store_item(vbid, makeStoredDocKey(keys[1]), keys[1]);

    flush_vbucket_to_disk(vbid, keys.size());

    // Simplest way to ensure DCP has to do a backfill - 'wipe memory'
    resetEngineAndWarmup();

    // Setup DCP, 1 producer and we will do a takeover of the vbucket
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "mb-31481",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(engine.get());

    ASSERT_TRUE(producer->getReadyQueue().empty());

    uint64_t rollbackSeqno = 0;
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      vb->getHighSeqno(), // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno
                                      vb->getHighSeqno(), // snap_end_seqno
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      {}));

    auto vb0Stream =
            dynamic_cast<ActiveStream*>(producer->findStream(vbid).get());
    ASSERT_NE(nullptr, vb0Stream);

    // Manually drive the backfill (not using notifyAndStepToCheckpoint)
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // Trigger slow stream handle
    ASSERT_TRUE(vb0Stream->handleSlowStream());
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()
    runNextTask(lpAuxioQ);

    ASSERT_TRUE(producer->getReadyQueue().exists(vbid));

    // Now drain all items before we proceed to complete, which triggers disk
    // snapshot.
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);
    for (const auto& key : keys) {
        ASSERT_EQ(ENGINE_SUCCESS, producer->step(&producers));
        ASSERT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
        ASSERT_EQ(key, producers.last_key);
    }

    // Another producer step should report EWOULDBLOCK (no more data) as all
    // items have been backfilled.
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));
    // Also the readyQ should be empty
    EXPECT_TRUE(producer->getReadyQueue().empty());

    // backfill:complete()
    runNextTask(lpAuxioQ);

    // Notified to allow stream to transition to in-memory phase.
    EXPECT_TRUE(producer->getReadyQueue().exists(vbid));

    // Step should cause stream closed message, previously this would
    // keep the "ENGINE_EWOULDBLOCK" response due to the itemsReady flag,
    // which is not expected with that message already being in the readyQ.
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);

    // Stepping forward should now show that stream end message has been
    // completed and no more messages are needed to send.
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    // Similarly, the readyQ should be empty again
    EXPECT_TRUE(producer->getReadyQueue().empty());

    // backfill:finished() - just to cleanup.
    runNextTask(lpAuxioQ);

    // vb0Stream should be closed
    EXPECT_FALSE(vb0Stream->isActive());

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
}

void SingleThreadedEPBucketTest::backfillExpiryOutput(bool xattr) {
    auto flags = xattr ? cb::mcbp::request::DcpOpenPayload::IncludeXattrs : 0;

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Expire a key;
    auto expiryTime = ep_real_time() + 256;

    std::string value;
    if (xattr) {
        value = createXattrValue("body");
        store_item(vbid,
                   {"KEY3", DocKeyEncodesCollectionId::No},
                   value,
                   expiryTime,
                   {cb::engine_errc::success},
                   PROTOCOL_BINARY_DATATYPE_XATTR);
    } else {
        value = "value";
        store_item(vbid,
                   {"KEY3", DocKeyEncodesCollectionId::No},
                   value,
                   expiryTime);
    }

    // Trigger expiry on the stored item
    TimeTraveller arron(1024);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocKeyEncodesCollectionId::No}, vbid, cookie, NONE);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Now flush to disk and wipe memory to ensure that DCP will have to do
    // a backfill
    flush_vbucket_to_disk(vbid, 1);
    resetEngineAndWarmup();

    // Setup DCP, 1 producer and we will do a takeover of the vbucket
    auto producer = std::make_shared<MockDcpProducer>(
            *engine, cookie, "mb-26907", flags);

    MockDcpMessageProducers producers(engine.get());

    ASSERT_TRUE(producer->getReadyQueue().empty());

    // noop on as could be using xattr's
    producer->setNoopEnabled(true);

    // Enable DCP Expiry opcodes
    producer->setDCPExpiry(true);

    // Clear last_op to make sure it isn't just carried over
    producers.clear_dcp_data();

    uint64_t rollbackSeqno = 0;
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      vb->getHighSeqno(), // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno
                                      vb->getHighSeqno(), // snap_end_seqno
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      {}));

    notifyAndStepToCheckpoint(*producer,
                              producers,
                              cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false);

    // Now step the producer to transfer the delete/tombstone
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    EXPECT_EQ(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);

    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}
// MB-26907
TEST_F(SingleThreadedEPBucketTest, backfill_expiry_output) {
    backfillExpiryOutput(false);
}

// MB-26907
TEST_F(SingleThreadedEPBucketTest, backfill_expiry_output_xattr) {
    backfillExpiryOutput(true);
}
// MB-26907
// This tests the success of expiry opcodes being sent over DCP
// during a backfill after a slow stream request on any type of bucket.
TEST_P(STParameterizedBucketTest, slow_stream_backfill_expiry) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Expire a key;
    auto expiryTime = ep_real_time() + 32000;
    store_item(
            vbid, {"KEY3", DocKeyEncodesCollectionId::No}, "value", expiryTime);

    // Trigger expiry on the stored item
    TimeTraveller arron(64000);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocKeyEncodesCollectionId::No}, vbid, cookie, NONE);
    ASSERT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Now flush to disk
    flushVBucketToDiskIfPersistent(vbid, 1);

    auto vb = store->getVBuckets().getBucket(vbid);

    // Remove closed checkpoint so that backfill will take place
    bool newcp;
    EXPECT_EQ(1,
              vb->checkpointManager->removeClosedUnrefCheckpoints(*vb, newcp));

    // Setup DCP
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "mb-26907",
                                                      /*flags*/ 0);

    MockDcpMessageProducers producers(engine.get());

    ASSERT_TRUE(producer->getReadyQueue().empty());

    // Enable DCP Expiry opcodes
    producer->setDCPExpiry(true);

    uint64_t rollbackSeqno = 0;
    ASSERT_NE(nullptr, vb.get());
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      vb->getHighSeqno(), // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno
                                      vb->getHighSeqno(), // snap_end_seqno
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      {}));

    auto vb0Stream =
            dynamic_cast<ActiveStream*>(producer->findStream(vbid).get());
    ASSERT_NE(nullptr, vb0Stream);

    ASSERT_TRUE(vb0Stream->handleSlowStream());

    // Clear last_op to make sure it isn't just carried over
    producers.clear_dcp_data();

    // Run a backfill
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()
    runNextTask(lpAuxioQ);
    // backfill:complete()
    runNextTask(lpAuxioQ);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);

    if (persistent()) {
        // backfill:finished() - "CheckedExecutor failed fetchNextTask" thrown
        // if called with an ephemeral bucket.
        runNextTask(lpAuxioQ);
    }
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}

void SingleThreadedEPBucketTest::producerReadyQLimitOnBackfill(
        const BackfillBufferLimit limitType) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);

    auto producer = std::make_shared<MockDcpProducer>(
            *engine,
            cookie,
            "test-producer",
            0 /*flags*/,
            false /*startTask*/);

    auto stream = std::make_shared<MockActiveStream>(
            engine.get(),
            producer,
            DCP_ADD_STREAM_FLAG_DISKONLY /* flags */,
            0 /* opaque */,
            *vb,
            0 /* startSeqno */,
            std::numeric_limits<uint64_t>::max() /* endSeqno */,
            0 /* vbUuid */,
            0 /* snapStartSeqno */,
            0 /* snapEndSeqno */);

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
                                       backfill_source_t::BACKFILL_FROM_DISK,
                                       false /*force*/);

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
    auto lastSeqno = stream->getLastReadSeqno();
    ASSERT_GT(lastSeqno, 1);
    ASSERT_EQ(lastSeqno, expectedLastSeqno);
    // Check that we have not pushed more than what expected given the limit.
    // Note: this logic applies to both BackfillScanLimit::byte and
    //     BackfillScanLimit::item
    const size_t upperBound = limit / valueSize + 1;
    ASSERT_LT(lastSeqno, upperBound);
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

/*
 * Test to verify that if retain_erroneous_tombstones is set to
 * true, then the compactor will retain the tombstones, and if
 * it is set to false, they get purged
 */
TEST_F(SingleThreadedEPBucketTest, testRetainErroneousTombstones) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& epstore = getEPBucket();
    epstore.setRetainErroneousTombstones(true);
    ASSERT_TRUE(epstore.isRetainErroneousTombstones());

    auto key1 = makeStoredDocKey("key1");
    store_item(vbid, key1, "value");
    flush_vbucket_to_disk(vbid);

    delete_item(vbid, key1);
    flush_vbucket_to_disk(vbid);

    // In order to simulate an erroneous tombstone, use the
    // KVStore layer to set the delete time to 0.
    auto* kvstore = epstore.getVBucket(vbid)->getShard()
                                            ->getRWUnderlying();
    GetValue gv = kvstore->get(key1, Vbid(0));
    std::unique_ptr<Item> itm = std::move(gv.item);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    ASSERT_TRUE(itm->isDeleted());
    itm->setExpTime(0);

    class DummyCallback : public Callback<TransactionContext, int> {
    public:
        void callback(TransactionContext&, int&) {
        }
    } dc;
    kvstore->begin(std::make_unique<TransactionContext>());
    kvstore->del(*(itm.get()), dc);
    Collections::VB::Flush f(epstore.getVBucket(vbid)->getManifest());
    kvstore->commit(f);

    // Add another item to ensure that seqno of the deleted item
    // gets purged. KV-engine doesn't purge a deleted item with
    // the highest seqno
    auto key2 = makeStoredDocKey("key2");
    store_item(vbid, key2, "value");
    flush_vbucket_to_disk(vbid);

    // Now read back and verify key1 has a non-zero delete time
    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    ASSERT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData(key1,
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));

    auto vb = store->getVBucket(vbid);

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    vb->getShard()->getBgFetcher()->run(&mockTask);
    ASSERT_EQ(ENGINE_SUCCESS,
              store->getMetaData(key1,
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    ASSERT_EQ(1, deleted);
    ASSERT_EQ(0, metadata.exptime);

    // Run compaction. Ensure that compaction hasn't purged the tombstone
    runCompaction(~0, 3);
    EXPECT_EQ(0, vb->getPurgeSeqno());

    // Now, make sure erroneous tombstones get purged by the compactor
    epstore.setRetainErroneousTombstones(false);
    ASSERT_FALSE(epstore.isRetainErroneousTombstones());

    // Run compaction and verify that the tombstone is purged
    runCompaction(~0, 3);
    EXPECT_EQ(2, vb->getPurgeSeqno());
}

/**
 * Test to verify that in case retain_erroneous_tombstones is set to true, then
 * a tombstone with a valid expiry time will get purged
 */
TEST_F(SingleThreadedEPBucketTest, testValidTombstonePurgeOnRetainErroneousTombstones) {
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& epstore = getEPBucket();
    epstore.setRetainErroneousTombstones(true);
    ASSERT_TRUE(epstore.isRetainErroneousTombstones());

    auto key1 = makeStoredDocKey("key1");
    store_item(vbid, key1, "value");
    flush_vbucket_to_disk(vbid);

    delete_item(vbid, key1);
    flush_vbucket_to_disk(vbid);

    // Add another item to ensure that seqno of the deleted item
    // gets purged. KV-engine doesn't purge a deleted item with
    // the highest seqno
    auto key2 = makeStoredDocKey("key2");
    store_item(vbid, key2, "value");
    flush_vbucket_to_disk(vbid);

    // Now read back and verify key1 has a non-zero delete time
    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    ASSERT_EQ(ENGINE_EWOULDBLOCK,
              store->getMetaData(key1,
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));

    // Manually run the bgfetch task.
    MockGlobalTask mockTask(engine->getTaskable(), TaskId::MultiBGFetcherTask);
    store->getVBucket(vbid)->getShard()->getBgFetcher()->run(&mockTask);
    ASSERT_EQ(ENGINE_SUCCESS,
              store->getMetaData(key1,
                                 vbid,
                                 cookie,
                                 metadata,
                                 deleted,
                                 datatype));
    ASSERT_EQ(1, deleted);
    ASSERT_NE(0, metadata.exptime); // A locally created deleteTime

    // deleted key1 should be purged
    runCompaction(~0, 3);

    EXPECT_EQ(2, store->getVBucket(vbid)->getPurgeSeqno());
}

TEST_F(SingleThreadedEPBucketTest, Durability_DoNotPersistPendings) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto item = makePendingItem(makeStoredDocKey("key"), "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));

    const auto& ckptMgr = getEPBucket().getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, ckptMgr->getNumOpenChkItems());
    ASSERT_EQ(1, ckptMgr->getNumItemsForPersistence());
    ASSERT_EQ(1, engine->getEpStats().diskQueueSize);

    EXPECT_EQ(std::make_pair(false, size_t(0)),
              getEPBucket().flushVBucket(vbid));
    EXPECT_EQ(0, ckptMgr->getNumItemsForPersistence());
    EXPECT_EQ(0, engine->getEpStats().diskQueueSize);
}

INSTANTIATE_TEST_CASE_P(XattrSystemUserTest,
                        XattrSystemUserTest,
                        ::testing::Bool(), );

INSTANTIATE_TEST_CASE_P(XattrCompressedTest,
                        XattrCompressedTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()), );

static auto allConfigValues = ::testing::Values(
        std::make_tuple(std::string("ephemeral"), std::string("auto_delete")),
        std::make_tuple(std::string("ephemeral"), std::string("fail_new_data")),
        std::make_tuple(std::string("persistent"), std::string{}));

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_CASE_P(EphemeralOrPersistent,
                        STParameterizedBucketTest,
                        allConfigValues, );
