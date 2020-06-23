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

#include "../couchstore/src/internal.h"
#include "../mock/mock_checkpoint_manager.h"
#include "../mock/mock_couch_kvstore.h"
#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_conn_map.h"
#include "../mock/mock_dcp_consumer.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_global_task.h"
#include "../mock/mock_item_freq_decayer.h"
#include "../mock/mock_stream.h"
#include "bgfetcher.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "checkpoint_utils.h"
#include "couch-kvstore/couch-kvstore-config.h"
#include "couch-kvstore/couch-kvstore.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "ep_bucket.h"
#include "ep_time.h"
#include "ep_vb.h"
#include "ephemeral_tombstone_purger.h"
#include "ephemeral_vb.h"
#include "evp_store_test.h"
#include "failover-table.h"
#include "fakes/fake_executorpool.h"
#include "item_freq_decayer_visitor.h"
#include "kv_bucket.h"
#ifdef EP_USE_MAGMA
#include "../mock/mock_magma_kvstore.h"
#include "magma-kvstore/magma-kvstore_config.h"
#endif
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "taskqueue.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/test_task.h"
#include "tests/module_tests/thread_gate.h"
#include "tests/test_fileops.h"
#include "vb_commit.h"
#include "vbucket_state.h"

#include <platform/dirutils.h>
#include <string_utilities.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

#include <thread>

using FlushResult = EPBucket::FlushResult;
using MoreAvailable = EPBucket::MoreAvailable;
using WakeCkptRemover = EPBucket::WakeCkptRemover;

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
        Vbid vbid,
        vbucket_state_t newState,
        const nlohmann::json& meta,
        TransferVB transfer) {
    // Change state - this should add 1 set_vbucket_state op to the
    //VBuckets' persistence queue.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, newState, meta, transfer));

    if (engine->getConfiguration().getBucketType() == "persistent") {
        // Trigger the flusher to flush state to disk.
        const auto res = dynamic_cast<EPBucket&>(*store).flushVBucket(vbid);
        EXPECT_EQ(MoreAvailable::No, res.moreAvailable);
        EXPECT_EQ(0, res.numFlushed);
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
void SingleThreadedKVBucketTest::resetEngineAndEnableWarmup(
        std::string new_config) {
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
    if (engine->getConfiguration().getBucketType() == "persistent") {
        static_cast<EPBucket*>(engine->getKVBucket())->initializeWarmupTask();
        static_cast<EPBucket*>(engine->getKVBucket())->startWarmupTask();
    }
}

/**
 * Destroy engine and replace it with a new engine that can be warmed up.
 * Finally, run warmup.
 */
void SingleThreadedKVBucketTest::resetEngineAndWarmup(std::string new_config) {
    resetEngineAndEnableWarmup(new_config);

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

void SingleThreadedKVBucketTest::runBackfill() {
    // Run the backfill task, which has a number of steps to complete
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);
    // backfill:scan()
    runNextTask(lpAuxioQ);

    // 1 Extra step for persistent backfill
    if (engine->getConfiguration().getBucketType() != "ephemeral") {
        // backfill:finished()
        runNextTask(lpAuxioQ);
    }
}

void SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(
        MockDcpProducer& producer,
        MockDcpMessageProducers& producers,
        cb::mcbp::ClientOpcode expectedOp,
        bool fromMemory) {
    auto vb = store->getVBucket(vbid);
    ASSERT_NE(nullptr, vb.get());

    if (fromMemory) {
        producer.notifySeqnoAvailable(
                vbid, vb->getHighSeqno(), SyncWriteOperation::No);
        runCheckpointProcessor(producer, producers);
    } else {
        runBackfill();
    }

    // Next step which will process a snapshot marker and then the caller
    // should now be able to step through the checkpoint
    if (expectedOp != cb::mcbp::ClientOpcode::Invalid) {
        EXPECT_EQ(ENGINE_SUCCESS, producer.stepWithBorderGuard(producers));
        EXPECT_EQ(expectedOp, producers.last_op);
        if (expectedOp == cb::mcbp::ClientOpcode::DcpSnapshotMarker) {
            if (fromMemory) {
                EXPECT_EQ(MARKER_FLAG_MEMORY,
                          producers.last_flags & MARKER_FLAG_MEMORY);
            } else {
                EXPECT_EQ(MARKER_FLAG_DISK,
                          producers.last_flags & MARKER_FLAG_DISK);
            }
        }
    } else {
        EXPECT_EQ(ENGINE_EWOULDBLOCK, producer.stepWithBorderGuard(producers));
    }
}

void SingleThreadedKVBucketTest::runCheckpointProcessor(
        MockDcpProducer& producer, dcp_message_producers& producers) {
    // Step which will notify the snapshot task
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer.step(&producers));

    EXPECT_EQ(1, producer.getCheckpointSnapshotTask()->queueSize());

    // Now call run on the snapshot task to move checkpoint into DCP
    // stream
    producer.getCheckpointSnapshotTask()->run();
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
                                               uint64_t purgeBeforeSeq,
                                               bool dropDeletes) {
    CompactionConfig compactConfig;
    compactConfig.purge_before_ts = purgeBeforeTime;
    compactConfig.purge_before_seq = purgeBeforeSeq;
    compactConfig.drop_deletes = dropDeletes;
    compactConfig.db_file_id = vbid;
    store->scheduleCompaction(vbid, compactConfig, nullptr);
    // run the compaction task
    runNextTask(*task_executor->getLpTaskQ()[WRITER_TASK_IDX],
                "Compact DB file 0");
}

void SingleThreadedKVBucketTest::runCollectionsEraser() {
    if (engine->getConfiguration().getBucketType() == "persistent") {
        // run the compaction task. assuming it was scheduled by the test, will
        // fail the runNextTask expect if not scheduled.
        runNextTask(*task_executor->getLpTaskQ()[WRITER_TASK_IDX],
                    "Compact DB file 0");

        EXPECT_TRUE(store->getVBucket(vbid)
                            ->getShard()
                            ->getRWUnderlying()
                            ->getDroppedCollections(vbid)
                            .empty());
    } else {
        auto vb = store->getVBuckets().getBucket(vbid);
        auto* evb = dynamic_cast<EphemeralVBucket*>(vb.get());
        evb->purgeStaleItems();
    }
}

void SingleThreadedKVBucketTest::replaceCouchKVStoreWithMock() {
    ASSERT_EQ(engine->getConfiguration().getBucketType(), "persistent");
    ASSERT_EQ(engine->getConfiguration().getBackend(), "couchdb");
    auto rwro = store->takeRWRO(0);
    auto& config = const_cast<CouchKVStoreConfig&>(
            dynamic_cast<const CouchKVStoreConfig&>(rwro.rw->getConfig()));
    auto rw = std::make_unique<MockCouchKVStore>(config);
    store->setRWRO(0, std::move(rw), std::move(rwro.ro));
}

void STParameterizedBucketTest::SetUp() {
    if (!config_string.empty()) {
        config_string += ";";
    }
    auto bucketType = std::get<0>(GetParam());
    if (bucketType == "persistentRocksdb") {
        config_string += "bucket_type=persistent;backend=rocksdb";
    } else if (bucketType == "persistentMagma") {
        config_string += "bucket_type=persistent;backend=magma";
    } else {
        config_string += "bucket_type=" + bucketType;
    }
    auto evictionPolicy = std::get<1>(GetParam());

    if (!evictionPolicy.empty()) {
        if (persistent()) {
            config_string += ";item_eviction_policy=" + evictionPolicy;
        } else {
            config_string += ";ephemeral_full_policy=" + evictionPolicy;
        }
    }

    SingleThreadedKVBucketTest::SetUp();
}

/// @returns a string representing this tests' parameters.
std::string STParameterizedBucketTest::PrintToStringParamName(
        const ::testing::TestParamInfo<ParamType>& info) {
    auto bucket = std::get<0>(info.param);

    auto evictionPolicy = std::get<1>(info.param);
    if (evictionPolicy.empty()) {
        return bucket;
    }
    return bucket + "_" + evictionPolicy;
}

/**
 * MB-37702 highlighted a potential race condition during bucket shutdown. This
 * race condition is as follows:
 *
 * 1) Bucket shutdown starts (due to hard failover has we need DcpConsumers to
 *    still consider KV to be "up". This has to then reach the point where it
 *    starts to tear down DCP connections.
 *
 * 2) In DcpProducer we need to set all of our streams to dead but not yet
 *    destroy the backfill manager.
 *
 * 3) A stream request now needs to come in and enter a backfilling state.
 *
 * 4) Bucket shutdown continues and destroys the backfill manager object for the
 *    DcpProducer.
 *
 * 5) Memcached attempts to disconnect the DcpProducer again as we will tell it
 *    to step the connection (but bucket shutdown is in progress) and then we
 *    will seg fault as we attempt to destroy the stream because the backfill
 *    manager has been reset.
 */
TEST_P(STParameterizedBucketTest, StreamReqAcceptedAfterBucketShutdown) {
    auto& mockConnMap =
            static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
    auto vb = engine->getKVBucket()->getVBucket(vbid);

    // 1) Store an item and ensure it isn't in the CheckpointManager so that our
    // stream will enter backfilling state later
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // 2) Create Producer and ensure it is in the ConnMap so that we can emulate
    // the shutdown
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    producer->createCheckpointProcessorTask();
    mockConnMap.addConn(cookie, producer);

    // 3) Set our hook to perform a StreamRequest after we remove the streams
    // from the Producer but before we reset the backfillMgr.
    producer->setCloseAllStreamsHook([this, &producer, &vb]() {
        producer->createCheckpointProcessorTask();
        uint64_t rollbackSeqno;
        EXPECT_EQ(ENGINE_DISCONNECT,
                  producer->streamRequest(/*flags*/ 0,
                                          /*opaque*/ 0,
                                          vbid,
                                          /*st_seqno*/ 0,
                                          /*en_seqno*/ ~0,
                                          /*vb_uuid*/ 0xabcd,
                                          /*snap_start_seqno*/ 0,
                                          /*snap_end_seqno*/ ~0,
                                          &rollbackSeqno,
                                          fakeDcpAddFailoverLog,
                                          {}));

        // Stream should not have been created
        auto stream = std::dynamic_pointer_cast<ActiveStream>(
                producer->findStream(vbid));
        ASSERT_FALSE(stream);
    });

    // 4) Emulate the shutdown
    mockConnMap.shutdownAllConnections();

    // 5) Emulate memcached disconnecting the connection. Before the bug-fix we
    // would segfault at this line in ActiveStream::endStream() when we call
    // BackfillManager::bytesSent(...)
    mockConnMap.disconnect(cookie);

    producer->cancelCheckpointCreatorTask();
    producer.reset();
    mockConnMap.manageConnections();

    // DcpConnMapp.manageConnections() will reset our cookie so we need to
    // recreate it for the normal test TearDown to work
    cookie = create_mock_cookie();
}

TEST_P(STParameterizedBucketTest, ConcurrentProducerCloseAllStreams) {
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
    auto vb = engine->getKVBucket()->getVBucket(vbid);

    // 1) Store an item and ensure it isn't in the CheckpointManager so that our
    // stream will enter backfilling state later
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);
    removeCheckpoint(*vb, 1);

    // 2) Create Producer and ensure it is in the ConnMap so that we can emulate
    // the shutdown
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    producer->createCheckpointProcessorTask();
    mockConnMap.addConn(cookie, producer);

    // Break in closeAllStreams after we have taken the lock
    // of the first vBucket.
    std::thread thread1;
    ThreadGate tg1{2};

    producer->setCloseAllStreamsPostLockHook(
            [this, &producer, &mockConnMap, &thread1, &tg1]() {
                if (!tg1.isComplete()) {
                    producer->setCloseAllStreamsPreLockHook(
                            [&tg1] { tg1.threadUp(); });

                    // First hit of this will spawn a second thread that will
                    // call disconnect and try to enter the same block of code
                    // concurrently.
                    thread1 = std::thread{[this, &mockConnMap]() {
                        mockConnMap.disconnect(cookie);
                    }};

                    tg1.threadUp();
                } else {
                    // Before the fix we would fail here
                    EXPECT_FALSE(producer->getBFMPtr());
                }
            });

    // 3) Segfault would normally happen here as we exit
    // DcpProducer::closeAllStreams (for the second time).
    mockConnMap.shutdownAllConnections();

    thread1.join();

    // Reset the hook or we will call it again when we disconnect our cookie as
    // part of TearDown
    producer->setCloseAllStreamsPostLockHook([]() {});

    producer->cancelCheckpointCreatorTask();
    producer.reset();
    mockConnMap.manageConnections();

    // DcpConnMap.manageConnections() will reset our cookie so we need to
    // recreate it for the normal test TearDown to work
    cookie = create_mock_cookie();
}

/**
 * MB-37827 highlighted a potential race condition during bucket shutdown. This
 * race condition is as follows:
 *
 * 1) Bucket shutdown starts but has not yet destroyed streams of our given
 *    producer.
 *
 * 2) A seqno ack comes in and gets partially processed. We find the stream in
 *    the producer but not yet process the ack.
 *
 * 3) Bucket shutdown continues and destroys the stream object by removing the
 *    owning shared_ptr in DcpProducer::closeALlStreams
 *
 * 4) Seqno ack processing continues and segfaults when attempting to access the
 *    stream.
 */
TEST_P(STParameterizedBucketTest, SeqnoAckAfterBucketShutdown) {
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    engine->getKVBucket()->setVBucketState(vbid, vbucket_state_active);
    auto vb = engine->getKVBucket()->getVBucket(vbid);

    // 1) Create Producer and ensure it is in the ConnMap so that we can emulate
    // the shutdown
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    mockConnMap.addConn(cookie, producer);

    // 2) Need to enable sync rep for seqno acking and create our stream
    producer->control(0, "consumer_name", "consumer");
    producer->setSyncReplication(SyncReplication::SyncReplication);

    uint64_t rollbackSeqno;
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(/*flags*/ 0,
                                      /*opaque*/ 0,
                                      vbid,
                                      /*st_seqno*/ 0,
                                      /*en_seqno*/ ~0,
                                      /*vb_uuid*/ 0xabcd,
                                      /*snap_start_seqno*/ 0,
                                      /*snap_end_seqno*/ ~0,
                                      &rollbackSeqno,
                                      fakeDcpAddFailoverLog,
                                      {}));

    // 3) Set our hook, we just need to simulate bucket shutdown in the hook
    producer->setSeqnoAckHook(
            [this, &mockConnMap]() { mockConnMap.shutdownAllConnections(); });

    // 4) Seqno ack. Previously this would segfault due to the stream being
    // destroyed mid seqno ack.
    EXPECT_EQ(ENGINE_SUCCESS, producer->seqno_acknowledged(0, vbid, 0));

    mockConnMap.disconnect(cookie);
    mockConnMap.manageConnections();
    producer.reset();

    // DcpConnMapp.manageConnections() will reset our cookie so we need to
    // recreate it for the normal test TearDown to work
    cookie = create_mock_cookie();
}

ENGINE_ERROR_CODE STParameterizedBucketTest::checkKeyExists(
        StoredDocKey& key, Vbid vbid, get_options_t options) {
    auto rc = store->get(key, vbid, cookie, options).getStatus();
    if (needBGFetch(rc)) {
        rc = store->get(key, vbid, cookie, options).getStatus();
    }
    return rc;
}

ENGINE_ERROR_CODE STParameterizedBucketTest::setItem(Item& itm,
                                                     const void* cookie) {
    auto rc = store->set(itm, cookie);
    if (needBGFetch(rc)) {
        rc = store->set(itm, cookie);
    }
    return rc;
}

ENGINE_ERROR_CODE STParameterizedBucketTest::addItem(Item& itm,
                                                     const void* cookie) {
    auto rc = store->add(itm, cookie);
    if (needBGFetch(rc)) {
        rc = store->add(itm, cookie);
    }
    return rc;
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

    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

    // Create a Mock Dcp producer
    // Create the Mock Active Stream with a startSeqno of 1
    // as a startSeqno is always valid
    auto producer = std::make_shared<MockDcpProducer>(*engine,
                                                      cookie,
                                                      "test_producer",
                                                      /*flags*/ 0);
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a Mock Active Stream
    auto mock_stream = producer->mockActiveStreamRequest(/*flags*/ 0,
                                                         /*opaque*/ 0,
                                                         *vb,
                                                         /*st_seqno*/ 1,
                                                         /*en_seqno*/ ~0,
                                                         /*vb_uuid*/ 0xabcd,
                                                         /*snap_start_seqno*/ 0,
                                                         /*snap_end_seqno*/ ~0,
                                                         IncludeValue::Yes,
                                                         IncludeXattrs::Yes);

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

    // Remove checkpoint, forcing the stream to Backfill.
    removeCheckpoint(*vb, 2);

    // This will schedule the backfill
    mock_stream->transitionStateToBackfilling();
    ASSERT_TRUE(mock_stream->isBackfilling());

    // Advance the purgeSeqno
    if (persistent()) {
        runCompaction(~0, 3);
    } else {
        EphemeralVBucket::HTTombstonePurger purger(0);
        auto vbptr = store->getVBucket(vbid);
        auto* evb = dynamic_cast<EphemeralVBucket*>(vbptr.get());
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
     auto& ckpt_mgr = *(
             static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

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
    auto& ckpt_mgr = *(
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

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
class MB29369_SingleThreadedEPBucketTest : public SingleThreadedEPBucketTest {
protected:
    MB29369_SingleThreadedEPBucketTest() {
        // Need dcp_producer_snapshot_marker_yield_limit + 1 (11) vBuckets for
        // this test.
        config_string = "max_vbuckets=11";
    }
};

// @TODO get working for magma
TEST_F(MB29369_SingleThreadedEPBucketTest,
       CursorDroppingPendingCkptProcessorTask) {
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

        // Create an item, create a new checkpoint and then flush to disk.
        // This ensures that:
        // a) ActiveStream::nextCheckpointItem will have data available when
        //    call next() - and will add vb to
        //    ActiveStreamCheckpointProcessorTask's queue.
        // b) After cursor is dropped we can remove the previously closed and
        //    flushed checkpoint to force ActiveStream into backfilling state.
        EXPECT_TRUE(queueNewItem(*vb, "key1"));
        vb->checkpointManager->createNewCheckpoint();
        EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
                  getEPBucket().flushVBucket(vbid));

        // And then request another item, to add the VBID to
        // ActiveStreamCheckpointProcessorTask's queue.
        result = stream->next();
        EXPECT_FALSE(result);
        EXPECT_EQ(id + 1, producer->getCheckpointSnapshotTask()->queueSize())
                << "Should have added " << vbid << " to ProcessorTask queue";
    }

    // Should now have dcp_producer_snapshot_marker_yield_limit + 1 items
    // in ActiveStreamCheckpointProcessorTask's pending VBs.
    EXPECT_EQ(iterationLimit + 1,
              producer->getCheckpointSnapshotTask()->queueSize())
            << "Should have all vBuckets in ProcessorTask queue";

    // Use last Stream as the one we're going to drop the cursor on (this is
    // also at the back of the queue).
    auto vb = store->getVBuckets().getBucket(Vbid(iterationLimit));
    auto& ckptMgr = *(
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

    // 1. Now trigger cursor dropping for this stream.
    EXPECT_TRUE(stream->handleSlowStream());
    EXPECT_TRUE(stream->isInMemory())
            << "should be state:in-memory immediately after handleSlowStream";
    EXPECT_EQ(1, ckptMgr.getNumOfCursors()) << "Should only have persistence "
                                               "cursor registered after "
                                               "cursor dropping.";

    // Remove the closed checkpoint to force stream to backfill.
    removeCheckpoint(*vb, 1);

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
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize())
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
TEST_P(STParamPersistentBucketTest, MB29585_backfilling_whilst_snapshot_runs) {
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));

    // Request an item from the stream, so it advances from to in-memory
    auto result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_TRUE(stream->isInMemory());

    // Now step the in-memory stream to schedule the checkpoint task
    result = stream->next();
    EXPECT_FALSE(result);
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());

    // Now close the stream
    EXPECT_EQ(ENGINE_SUCCESS, producer->closeStream(0 /*opaque*/, vbid));

    // Next we to ensure the recreated stream really does a backfill, so drop
    // in-memory items
    bool newcp;
    vb->checkpointManager->createNewCheckpoint();
    // Force persistence into new CP
    queueNewItem(*vb, "key2");
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
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
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());

    EXPECT_EQ(2, checkpointManager->getNumOfCursors())
            << "Expected persistence + replication cursors after "
               "markDiskShapshot";

    result = stream->next();
    ASSERT_TRUE(result);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, result->getEvent())
            << "Expected Snapshot marker after running backfill task.";

    // Let the backfill task complete running through its various states
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
        auto& ckpt_mgr =
                *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));

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

        EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
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

        EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
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

TEST_P(STParamPersistentBucketTest, MB22960_cursor_dropping_data_loss) {
    // Records the number of times ActiveStream::registerCursor is invoked.
    size_t registerCursorCount = 0;
    // Make vbucket active.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));
    EXPECT_FALSE(ckpt_mgr.hasClosedCheckpointWhichCanBeRemoved());
    ckpt_mgr.createNewCheckpoint();
    EXPECT_EQ(2, ckpt_mgr.getNumCheckpoints());

    store_item(vbid, makeStoredDocKey("key2"), "value");
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));
    EXPECT_FALSE(ckpt_mgr.hasClosedCheckpointWhichCanBeRemoved());
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

    // MB-37150: cursors are now registered before deciding if a backfill is
    // needed. to retain the original intent of this test, manually drop the
    // cursor _again_, to return the stream to the desired state: about to
    // backfill, without a cursor. The test can then check that the cursor is
    // registered again at the correct seqno, defending the original change as
    // intended.
    ckpt_mgr.removeCursor(mock_stream->getCursor().lock().get());

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
 *   cursors:[
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

TEST_P(STParamPersistentBucketTest,
       MB25056_do_not_set_pendingBackfill_to_true) {
    // Records the number of times registerCursor is invoked.
    size_t registerCursorCount = 0;
    // Make vbucket a replica.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto vb = store->getVBuckets().getBucket(vbid);
    ASSERT_NE(nullptr, vb.get());
    auto& ckpt_mgr =
            *(static_cast<MockCheckpointManager*>(vb->checkpointManager.get()));
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
TEST_P(STParamPersistentBucketTest, test_mb22451) {
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
TEST_P(STParamPersistentBucketTest, MB19815_doDcpVbTakeoverStats) {
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
    auto dummy_cb = [](std::string_view key,
                       std::string_view value,
                       gsl::not_null<const void*> cookie) {};
    std::string key{"MB19815_doDCPVbTakeoverStats"};

    // We can't call stats with a nullptr as the cookie. Given that
    // the callback don't use the cookie "at all" we can just use the key
    // as the cookie
    EXPECT_NO_THROW(engine->public_doDcpVbTakeoverStats(
            static_cast<const void*>(key.c_str()), dummy_cb, key, vbid));

    // Cleanup - run flusher.
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));
}

/*
 * Test that
 * 1. We cannot create a stream against a dead vb (MB-17230)
 * 2. No tasks are scheduled as a side-effect of the streamRequest attempt.
 */
TEST_P(STParamPersistentBucketTest, MB19428_no_streams_against_dead_vbucket) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key"), "value");

    // Directly flush the vbucket
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
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

TEST_F(SingleThreadedEPBucketTest, ReadyQueueMaintainsWakeTimeOrder) {
    class TestTask : public GlobalTask {
    public:
        TestTask(Taskable& t, TaskId id, double s) : GlobalTask(t, id, s) {
        }
        bool run() override {
            return false;
        }

        std::string getDescription() override {
            return "Task uid:" + std::to_string(getId());
        }

        std::chrono::microseconds maxExpectedDuration() override {
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
        TestTask(EventuallyPersistentEngine *e, double s) :
                 GlobalTask(e, TaskId::ActiveStreamCheckpointProcessorTask, s) {}
        bool run() override {
            return false;
        }

        std::string getDescription() override {
            return "Test MB20235";
        }

        std::chrono::microseconds maxExpectedDuration() override {
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
TEST_P(STParamPersistentBucketTest, MB19892_BackfillNotDeleted) {
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
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
    consumer->snapshotMarker(/*opaque*/ 1,
                             vbid,
                             /*startseq*/ 0,
                             /*endseq*/ messages,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});

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
TEST_P(STParamPersistentBucketTest, MB_29861) {
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
                             /*flags*/ MARKER_FLAG_DISK,
                             /*HCS*/ 0,
                             /*maxVisibleSeqno*/ {});

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

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
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

    runBGFetcherTask();
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
 * Test that the consumer will use the delete time given, and that
 * a delete time far in the future is handled correctly.
 */
TEST_P(STParameterizedBucketTest, MB_27457_ReplicateDeleteTimeFuture) {
    // Choose a delete time in the future (2032-01-24T23:52:45).
    time_t futureTime = 1958601165;
    struct timeval now;
    ASSERT_EQ(0, cb_get_timeofday(&now));
    ASSERT_LT(now.tv_sec, futureTime);
    test_replicateDeleteTime(futureTime);
}

/*
 * Test that the consumer will use the delete time given, and that
 * a delete time before this node started is handled correctly (for example
 * a replica node started after the active node's item was deleted.
 */
TEST_P(STParameterizedBucketTest, MB_39993_ReplicateDeleteTimePast) {
    // Choose a delete time in the past, but less than the metadata purge
    // interval (so tombstone isn't immediately purged).
    struct timeval now;
    ASSERT_EQ(0, cb_get_timeofday(&now));
    // 6 hours in the past.
    time_t pastTime = now.tv_sec - (6 * 60 * 60);
    test_replicateDeleteTime(pastTime);
}

void STParameterizedBucketTest::test_replicateDeleteTime(time_t deleteTime) {
    // We need a replica VB
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    // Create a MockDcpConsumer
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "test");

    // Bump forwards by 1 hour so ep_current_time cannot be 0
    TimeTraveller biff(3600);

    // Add the stream
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // 1. Add the first message, a snapshot marker.
    consumer->snapshotMarker(/*opaque*/ 1,
                             vbid,
                             /*startseq*/ 0,
                             /*endseq*/ 2,
                             /*flags*/ 0,
                             /*HCS*/ {},
                             /*maxVisibleSeqno*/ {});
    // 2. Now add two deletions, one without deleteTime, one with
    consumer->deletionV2(/*opaque*/ 1,
                         {"key1", DocKeyEncodesCollectionId::No},
                         /*values*/ {},
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                         /*cas*/ 1,
                         /*vbucket*/ vbid,
                         /*bySeqno*/ 1,
                         /*revSeqno*/ 0,
                         /*deleteTime*/ 0);

    consumer->deletionV2(/*opaque*/ 1,
                         {"key2", DocKeyEncodesCollectionId::No},
                         /*value*/ {},
                         /*priv_bytes*/ 0,
                         /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                         /*cas*/ 2,
                         /*vbucket*/ vbid,
                         /*bySeqno*/ 2,
                         /*revSeqno*/ 0,
                         deleteTime);

    flushVBucketToDiskIfPersistent(vbid, 2);

    // Drop the stream
    consumer->closeStream(/*opaque*/ 0, vbid);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Now read back and verify key2 has our test deleteTime of 10
    ItemMetaData metadata;
    uint32_t deleted = 0;
    uint8_t datatype = 0;
    time_t tombstoneTime;
    if (persistent()) {
        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  store->getMetaData(makeStoredDocKey("key1"),
                                     vbid,
                                     cookie,
                                     metadata,
                                     deleted,
                                     datatype));
        runBGFetcherTask();
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->getMetaData(makeStoredDocKey("key1"),
                                     vbid,
                                     cookie,
                                     metadata,
                                     deleted,
                                     datatype));
        tombstoneTime = metadata.exptime;
    } else {
        //  Ephemeral tombstone time is not in the expiry field, we can only
        // check the value by directly peeking at the StoredValue
        auto vb = store->getVBucket(vbid);
        auto ro = vb->ht.findForRead(makeStoredDocKey("key1"),
                                     TrackReference::No,
                                     WantsDeleted::Yes);
        auto* sv = ro.storedValue;
        ASSERT_NE(nullptr, sv);
        deleted = sv->isDeleted();
        tombstoneTime = sv->toOrderedStoredValue()->getCompletedOrDeletedTime();
    }

    EXPECT_EQ(1, deleted);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, datatype);
    EXPECT_GE(tombstoneTime, biff.get())
            << "Expected a tombstone to have been set which is equal or "
               "greater than our time traveller jump";

    deleted = 0;
    datatype = 0;
    if (persistent()) {
        EXPECT_EQ(ENGINE_EWOULDBLOCK,
                  store->getMetaData(makeStoredDocKey("key2"),
                                     vbid,
                                     cookie,
                                     metadata,
                                     deleted,
                                     datatype));
        runBGFetcherTask();
        EXPECT_EQ(ENGINE_SUCCESS,
                  store->getMetaData(makeStoredDocKey("key2"),
                                     vbid,
                                     cookie,
                                     metadata,
                                     deleted,
                                     datatype));

        tombstoneTime = metadata.exptime;
    } else {
        auto vb = store->getVBucket(vbid);
        auto ro = vb->ht.findForRead(makeStoredDocKey("key2"),
                                     TrackReference::No,
                                     WantsDeleted::Yes);
        auto* sv = ro.storedValue;
        ASSERT_NE(nullptr, sv);
        deleted = sv->isDeleted();
        tombstoneTime = sv->toOrderedStoredValue()->getCompletedOrDeletedTime();
    }
    EXPECT_EQ(1, deleted);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, datatype);
    EXPECT_EQ(deleteTime, tombstoneTime)
            << "key2 did not have our replicated deleteTime:" << deleteTime;
}

/*
 * Background thread used by MB20054_onDeleteItem_during_bucket_deletion
 */
static void MB20054_run_backfill_task(EventuallyPersistentEngine* engine,
                                      CheckedExecutor& backfill,
                                      bool& backfill_signaled,
                                      SyncObject& backfill_cv,
                                      bool& destroy_signaled,
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
        backfill_signaled = true;
        backfill_cv.notify_one(); // move the main thread along
    }

    // Now wait ourselves for destroy to be completed [B].
    destroy_cv.wait(destroy_lh,
                    [&destroy_signaled]() { return destroy_signaled; });

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
class MB20054_SingleThreadedEPStoreTest : public STParamPersistentBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        engine->initializeConnmap();
    }

    void TearDown() override {
        // Cannot use base class TearDown as this test has already partially
        // destroyed the engine. Therefore manually call the parts we do need.
        engine.reset();
        ExecutorPool::shutdown();
        // Cleanup any files we created.
        cb::io::rmrf(test_dbname);
    }
};

// Check that if onDeleteItem() is called during bucket deletion, we do not
// abort due to not having a valid thread-local 'engine' pointer. This
// has been observed when we have a DCPBackfill task which is deleted during
// bucket shutdown, which has a non-zero number of Items which are destructed
// (and call onDeleteItem).
TEST_P(MB20054_SingleThreadedEPStoreTest,
       MB20054_onDeleteItem_during_bucket_deletion) {
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
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
    bool backfill_signaled = false;
    SyncObject destroy_cv;
    bool destroy_signaled = false;
    std::thread concurrent_task_thread;

    {
        // scope for the backfill lock
        std::unique_lock<std::mutex> backfill_lh(backfill_cv);

        concurrent_task_thread = std::thread(MB20054_run_backfill_task,
                                             engine.get(),
                                             std::ref(backfill),
                                             std::ref(backfill_signaled),
                                             std::ref(backfill_cv),
                                             std::ref(destroy_signaled),
                                             std::ref(destroy_cv),
                                             lpAuxioQ);
        // [A] Wait for DCPBackfill to complete.
        backfill_cv.wait(backfill_lh,
                         [&backfill_signaled]() { return backfill_signaled; });
    }

    ObjectRegistry::onSwitchThread(engine.get());
    // 'Destroy' the engine - this doesn't delete the object, just shuts down
    // connections, marks streams as dead etc.
    engine->destroyInner(/*force*/ false);

    {
        // If we can get the lock we know the thread is waiting for destroy.
        std::lock_guard<std::mutex> lh(destroy_cv);
        // suppress clang static analyzer false positive as destroy_signaled
        // is used after its written to in another thread.
#ifndef __clang_analyzer__
        destroy_signaled = true;
#endif
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

    // Call unregisterTaskable which will call _stopTaskGroup, but we keep the
    // engine alive to ensure it is deleted after all tasks (CheckedExecutor is
    // holding the backfill task)
    ExecutorPool::get()->unregisterTaskable(engine->getTaskable(), false);
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

    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
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
        const std::string& cas_str{R"({"cas":"0xdeadbeefcafefeed"})"};
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

// Test that we can push a DCP_DELETION which pretends to be from a delete
// with xattrs, i.e. the delete has a value containing only system xattrs
// The MB was created because this code would actually trigger an exception
TEST_P(STParamPersistentBucketTest, mb25273) {
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

    blob.set("key1", R"({"author":"bubba"})");
    blob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");

    auto xattr_value = blob.finalize();

    std::string data;
    std::copy(xattr_value.begin(), xattr_value.end(), std::back_inserter(data));
    std::copy(
            body.c_str(), body.c_str() + body.size(), std::back_inserter(data));

    const DocKey docKey{key, DocKeyEncodesCollectionId::No};
    cb::const_byte_buffer value{reinterpret_cast<const uint8_t*>(data.data()),
                                data.size()};

    // Send mutation in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       bySeqno,
                                       bySeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
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
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       bySeqno,
                                       bySeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
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
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
              getEPBucket().flushVBucket(vbid));
    /* Close stream before deleting the connection */
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(docKey, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();
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

// MB-26907
TEST_P(STParameterizedBucketTest, enable_expiry_output) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto cookie = create_mock_cookie(engine.get());
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
        EXPECT_EQ(ENGINE_SUCCESS, producer->stepWithBorderGuard(producers));
    };

    // Finally expire a key and check that the delete_time we receive is not the
    // expiry time, the delete time should always be re-created by the server to
    // ensure old/future expiry times don't disrupt tombstone purging (MB-33919)
    auto expiryTime = ep_real_time() + 32000;
    store_item(
            vbid, {"KEY3", DocKeyEncodesCollectionId::No}, "value", expiryTime);

    // Trigger a flush to disk (ensure full-eviction numItems stat is
    // up-to-date).
    flushVBucketToDiskIfPersistent(vbid, 1);

    step(true);
    size_t expectedBytes =
            SnapshotMarker::baseMsgBytes +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
            MutationResponse::mutationBaseMsgBytes + (sizeof("value") - 1) +
            (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());
    EXPECT_EQ(1, store->getVBucket(vbid)->getNumItems());
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers.last_op);
    TimeTraveller arron(64000);

    // Trigger expiry on a GET
    auto gv = store->get(
            {"KEY3", DocKeyEncodesCollectionId::No}, vbid, cookie, NONE);
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());

    // Trigger a flush to disk (ensure full-eviction numItems stat is
    // up-to-date).
    flushVBucketToDiskIfPersistent(vbid, 1);

    step(true);

    EXPECT_NE(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
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

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));
    TimeTraveller ted(64000);
    runCompaction();
    // An expired item should of been pushed to the checkpoint
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
              getEPBucket().flushVBucket(vbid));
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    GetValue gv = kvbucket.get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();

    gv = kvbucket.get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    auto get_itm = gv.item.get();
    auto get_data = const_cast<char*>(get_itm->getData());

    cb::char_buffer value_buf{get_data, get_itm->getNBytes()};
    cb::xattr::Blob new_blob(value_buf, false);

    // If testing with system xattrs
    if (GetParam()) {
        const std::string& cas_str{R"({"cas":"0xdeadbeefcafefeed"})"};
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
        cookie = create_mock_cookie(engine.get());
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

    EXPECT_TRUE(as->isTakeoverSend());
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
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    EXPECT_EQ(1, checkpointManager->getNumOfCursors());
}

// takeover when more writes occur
TEST_F(MB_29287, DISABLED_dataloss_hole) {
    auto stream = producer->findStream(vbid);
    auto* as = static_cast<ActiveStream*>(stream.get());

    store_item(vbid, makeStoredDocKey("6"), "value6");

    EXPECT_TRUE(as->isTakeoverSend());
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
    EXPECT_TRUE(as->isTakeoverSend());

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
    auto* checkpointManager =
            static_cast<MockCheckpointManager*>(vb->checkpointManager.get());
    EXPECT_EQ(1, checkpointManager->getNumOfCursors());
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

    // MB-37374: Since 6.6, the validation covered in this test is enforce only
    // for producers that don't enable IncludeDeletedUserXattrs. So we need to
    // simulate the related Consumer negotiation before proceeding.
    consumer->public_setIncludeDeletedUserXattrs(IncludeDeletedUserXattrs::No);

    std::string body;
    if (!isXattrSystem()) {
        body.assign("value");
    }
    auto value = createXattrValue(body, isXattrSystem(), isSnappy());

    // Send deletion in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       bySeqno,
                                       bySeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

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

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));

    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    // Switch to active
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();
    gv = store->get({"key", DocKeyEncodesCollectionId::No},
                    vbid,
                    cookie,
                    GET_DELETED_VALUE);
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());

    // This is the only system key test_helpers::createXattrValue gives us
    cb::xattr::Blob blob;
    blob.set("_sync", R"({"cas":"0xdeadbeefcafefeed"})");

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
TEST_P(STParamPersistentBucketTest, MB_31141_sanitise_input) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookie, "MB_31141_sanitise_input");
    int opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, /*flags*/ 0));

    std::string body = "value";

    // Send deletion in a single seqno snapshot
    int64_t bySeqno = 1;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       bySeqno,
                                       bySeqno,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));

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

    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              getEPBucket().flushVBucket(vbid));

    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    // Switch to active
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS | GET_DELETED_VALUE);
    auto gv = store->get(
            {"key", DocKeyEncodesCollectionId::No}, vbid, cookie, options);
    EXPECT_EQ(ENGINE_EWOULDBLOCK, gv.getStatus());

    runBGFetcherTask();
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
TEST_P(STParamPersistentBucketTest, MB_29480) {
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

    // create a new checkpoint to allow the current one to be removed
    // after flushing
    auto& ckpt_mgr = *vb->checkpointManager;
    ckpt_mgr.createNewCheckpoint();

    flush_vbucket_to_disk(vbid, initialKeys.size());

    // 4) Compact drop tombstones less than time=maxint and below seqno 3
    // as per earlier comment, only seqno 1 will be purged...
    runCompaction(~0, 3);

    // 5) Begin cursor dropping
    mock_stream->handleSlowStream();

    // remove the previous checkpoint to force a backfill
    bool new_ckpt_created = false;
    auto removed = ckpt_mgr.removeClosedUnrefCheckpoints(*vb, new_ckpt_created);
    EXPECT_EQ(2, removed);

    // Kick the stream into backfill
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(&producers));

    // 6) Store more items (don't flush these)
    std::array<std::string, 2> extraKeys = {{"k3", "k4"}};
    for (const auto& key : extraKeys) {
        store_item(vbid, makeStoredDocKey(key), key);
    }

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    auto* as0 = static_cast<ActiveStream*>(vb0Stream.get());
    EXPECT_TRUE(as0->isBackfilling());

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
TEST_P(STParamPersistentBucketTest, MB_29512) {
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

    auto* as0 = static_cast<ActiveStream*>(vb0Stream.get());
    EXPECT_TRUE(as0->isBackfilling());

    // 6) Backfill now starts up, but should quickly cancel
    runNextTask(*task_executor->getLpTaskQ()[AUXIO_TASK_IDX]);

    EXPECT_FALSE(vb0Stream->isActive());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers.last_op);

    // Stop Producer checkpoint processor task
    producer->cancelCheckpointCreatorTask();
    producer->closeAllStreams();
}

TEST_P(STParamPersistentBucketTest, MB_29541) {
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

    auto* as0 = static_cast<ActiveStream*>(vb0Stream.get());
    EXPECT_TRUE(as0->isTakeoverWait());

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

// Verify that handleResponse against an unknown stream returns true, MB-32724
// demonstrated a case where false will cause a failure.
TEST_F(SingleThreadedEPBucketTest, MB_32724) {
    auto p = std::make_shared<MockDcpProducer>(*engine, cookie, "mb-32724", 0);

    p->createCheckpointProcessorTask();

    MockDcpMessageProducers producers(engine.get());

    protocol_binary_response_header message;
    message.response.setMagic(cb::mcbp::Magic::ClientResponse);
    message.response.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    EXPECT_TRUE(p->handleResponse(&message));
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
TEST_P(STParamPersistentBucketTest, MB_31481) {
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

void STParamPersistentBucketTest::backfillExpiryOutput(bool xattr) {
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

    // The delete time should always be re-created by the server to
    // ensure old/future expiry times don't disrupt tombstone purging (MB-33919)
    EXPECT_NE(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);

    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}
// MB-26907
TEST_P(STParamPersistentBucketTest, backfill_expiry_output) {
    backfillExpiryOutput(false);
}

// MB-26907
TEST_P(STParamPersistentBucketTest, backfill_expiry_output_xattr) {
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
    runBackfill();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers.last_op);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(&producers));

    // The delete time should always be re-created by the server to
    // ensure old/future expiry times don't disrupt tombstone purging (MB-33919)
    EXPECT_NE(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpExpiration, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);

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

EPBucket& STParamPersistentBucketTest::getEPBucket() {
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

/*
 * Test to verify that if retain_erroneous_tombstones is set to
 * true, then the compactor will retain the tombstones, and if
 * it is set to false, they get purged
 */
TEST_P(STParamPersistentBucketTest, testRetainErroneousTombstones) {
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
    {
        GetValue gv = kvstore->get(DiskDocKey{key1}, Vbid(0));
        std::unique_ptr<Item> itm = std::move(gv.item);
        ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
        ASSERT_TRUE(itm->isDeleted());
        itm->setExpTime(0);
        itm->setBySeqno(itm->getBySeqno() + 1);

        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        // Release the item (from the unique ptr) as the queued_item we create
        // will destroy it later
        kvstore->del(itm.release());
        VB::Commit f(epstore.getVBucket(vbid)->getManifest());
        kvstore->commit(f);
    }

    // Add another item to ensure that seqno of the deleted item
    // gets purged. KV-engine doesn't purge a deleted item with
    // the highest seqno
    {
        auto key2 = makeStoredDocKey("key2");
        auto itm = makeCommittedItem(key2, "value");
        itm->setBySeqno(4);
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        kvstore->del(itm);
        VB::Commit f(epstore.getVBucket(vbid)->getManifest());
        kvstore->commit(f);
    }

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

    runBGFetcherTask();
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

    size_t expected;
    if (isMagma()) {
        // Magma doesn't susuffer from MB-30015 so doesn't retain these
        // tombstones
        expected = 0;
    } else {
        expected = 3;
    }

    EXPECT_EQ(expected, vb->getPurgeSeqno());
}

/**
 * Test to verify that in case retain_erroneous_tombstones is set to true, then
 * a tombstone with a valid expiry time will get purged
 */
TEST_P(STParamPersistentBucketTest,
       testValidTombstonePurgeOnRetainErroneousTombstones) {
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

    runBGFetcherTask();
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
    int opaque = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque, vbid, /*flags*/ 0));
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(opaque,
                                       vbid,
                                       1,
                                       10,
                                       MARKER_FLAG_CHK,
                                       {} /*HCS*/,
                                       {} /*maxVisibleSeqno*/));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque, vbid));

    // Test: Have the producer send further messages on the stream (before the
    // final STREAM_END. These should all be accepted (but discarded) by the
    // replica.
    auto testAllStreamLevelMessages = [&consumer, this, opaque](
                                              ENGINE_ERROR_CODE expected) {
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
    testAllStreamLevelMessages(ENGINE_SUCCESS);

    // Setup (phase 2): Receive a STREAM_END message - after which all of the
    // above stream-level messages should be rejected as ENOENT.
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->streamEnd(opaque, vbid, END_STREAM_CLOSED));

    // Test (phase 2): Have the producer send all the above stream-level
    // messages to the consumer. Should all be rejected this time.
    testAllStreamLevelMessages(ENGINE_KEY_ENOENT);
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
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque1, vbid, {}));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque1, vbid));
    const int opaque2 = 2;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque2, vbid, {}));

    // Test: Have the producer send a StreamEnd with the "old" opaque.
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->streamEnd(opaque1, vbid, END_STREAM_CLOSED));
}

TEST_F(SingleThreadedEPBucketTest, TestConsumerSendEEXISTSIfOpaqueWrong) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto consumer = std::make_shared<MockDcpConsumer>(*engine, cookie, "conn");

    const int opaque1 = 1;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque1, vbid, {}));
    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque1, vbid));

    const int opaque2 = 2;
    ASSERT_EQ(ENGINE_SUCCESS, consumer->addStream(opaque2, vbid, {}));

    ASSERT_EQ(ENGINE_SUCCESS, consumer->closeStream(opaque1, vbid, {}));

    auto key = makeStoredDocKey("key");
    auto dtype = PROTOCOL_BINARY_RAW_BYTES;
    EXPECT_EQ(ENGINE_KEY_EEXISTS,
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

TEST_P(STParameterizedBucketTest, produce_delete_times) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto t1 = ep_real_time();
    storeAndDeleteItem(vbid, {"KEY1", DocKeyEncodesCollectionId::No}, "value");
    auto t2 = ep_real_time();

    // Clear checkpoint so DCP will goto backfill
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    vb->checkpointManager->clear(*vb, 2);

    auto cookie = create_mock_cookie(engine.get());
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

        // Now step the producer to transfer the delete/tombstone.
        EXPECT_EQ(ENGINE_SUCCESS, producer->stepWithBorderGuard(producers));
    };

    step(false);
    EXPECT_NE(0, producers.last_delete_time);
    EXPECT_GE(producers.last_delete_time, t1);
    EXPECT_LE(producers.last_delete_time, t2);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpDeletion, producers.last_op);
    EXPECT_EQ("KEY1", producers.last_key);
    size_t expectedBytes =
            SnapshotMarker::baseMsgBytes +
            sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
            MutationResponse::deletionV2BaseMsgBytes + (sizeof("KEY1") - 1);
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
                     sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
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
                     sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
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

    // The delete time should always be re-created by the server to
    // ensure old/future expiry times don't disrupt tombstone purging (MB-33919)
    EXPECT_NE(expiryTime, producers.last_delete_time);
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpDeletion, producers.last_op);
    EXPECT_EQ("KEY3", producers.last_key);
    expectedBytes += SnapshotMarker::baseMsgBytes +
                     sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
                     MutationResponse::deletionV2BaseMsgBytes +
                     (sizeof("KEY3") - 1);
    EXPECT_EQ(expectedBytes, producer->getBytesOutstanding());

    destroy_mock_cookie(cookie);
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    producer.reset();
}

// Test simulates a simplified set of steps that demonstrate MB-34380, that is
// in a no traffic situation and some state changes we can end up with no
// vbucket file on disk.
TEST_P(STParameterizedBucketTest, MB_34380) {
    if (!persistent()) {
        return;
    }
    // 1) Create replica VB and simulate the replica connecting to it's active
    // and having the failover table replaced (via stream_request response)
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_replica, {}));

    // 1.1) force the failover to a specific value
    std::string failover = R"([{"id":101,"seq":0}])";
    {
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        vb->failovers = std::make_unique<FailoverTable>(failover, 5, 0);

        // 2) Now flush so the vbstate cache is updated and the file is created
        flushVBucketToDiskIfPersistent(vbid, 0);

        // 2.1) We should be able to call this method with no exception.
        EXPECT_NO_THROW(vb->getShard()->getRWUnderlying()->getDbFileInfo(vbid));
    }
    // 3) Delete the vbucket, the cached vbstate will remain untouched
    EXPECT_EQ(ENGINE_SUCCESS, store->deleteVBucket(vbid));

    // 4) Re-create the vbucket, and again simulate the connection to active,
    // forcing the failover table to the specific value.
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_replica, {}));

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    vb->failovers = std::make_unique<FailoverTable>(failover, 5, 0);

    // The bug...
    // 5) Flush the state change, without the fix the flush is skipped because
    // the cached vbstate matches the current state
    flushVBucketToDiskIfPersistent(vbid, 0);

    // Now simulate the bug, do something which requires the file, with the bug
    // this will throw.
    EXPECT_NO_THROW(vb->getShard()->getRWUnderlying()->getDbFileInfo(vbid));
}

INSTANTIATE_TEST_SUITE_P(XattrSystemUserTest,
                         XattrSystemUserTest,
                         ::testing::Bool());

INSTANTIATE_TEST_SUITE_P(XattrCompressedTest,
                         XattrCompressedTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         STParameterizedBucketTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

using FlushResult = EPBucket::FlushResult;

class STParamCouchstoreBucketTest : public STParamPersistentBucketTest {};

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistNonMetaItems(
        couchstore_error_t failureCode) {
    // About the first two COUCHSTORE_SUCCESS:
    // - firts, set-vbstate precommit()
    // - then, set-vbstate couchstore_commit()
    // They both sync(), see couchstore code for details.
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(failureCode))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Active receives PRE(keyA):1, M(keyB):2, D(keyB):3
    // Note that the set of mutation is just functional to testing that we write
    // to disk all the required vbstate entries at flush
    const std::string valueA = "valueA";
    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   valueA,
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {cb::durability::Requirements()});
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "valueB",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    delete_item(vbid, makeStoredDocKey("keyB"));

    // M(keyB):2 deduplicated, just 2 items for cursor
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(2, vb.checkpointManager->getNumItemsForPersistence());
    EXPECT_EQ(2, vb.dirtyQueueSize);

    const auto checkPreFlushHTState = [&vb]() -> void {
        const auto resA = vb.ht.findForUpdate(makeStoredDocKey("keyA"));
        ASSERT_TRUE(resA.pending);
        ASSERT_FALSE(resA.pending->isDeleted());
        ASSERT_TRUE(resA.pending->isDirty());
        ASSERT_FALSE(resA.committed);

        const auto resB = vb.ht.findForUpdate(makeStoredDocKey("keyB"));
        ASSERT_FALSE(resB.pending);
        ASSERT_TRUE(resB.committed);
        ASSERT_TRUE(resB.committed->isDeleted());
        ASSERT_TRUE(resB.committed->isDirty());
    };
    checkPreFlushHTState();

    // Note: Because of MB-37920 we may cache a stale vbstate. So, checking the
    //  cached vbstate for verifying persistence would invalidate the test.
    //  Check the actual vbstate on disk instead.
    auto& kvStore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    const auto checkPersistedVBState = [this, &kvStore](
                                               uint64_t lastSnapStart,
                                               uint64_t lastSnapEnd,
                                               uint64_t highSeqno,
                                               CheckpointType type,
                                               uint64_t hps,
                                               uint64_t hcs,
                                               uint64_t maxDelRevSeqno) {
        const auto vbs = kvStore.readVBState(vbid);
        EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
        EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
        EXPECT_EQ(highSeqno, vbs.highSeqno);
        EXPECT_EQ(type, vbs.checkpointType);
        EXPECT_EQ(hps, vbs.highPreparedSeqno);
        EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
        EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
    };

    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkPersistedVBState(0 /*lastSnapStart*/,
                              0 /*lastSnapEnd*/,
                              0 /*highSeqno*/,
                              CheckpointType::Memory,
                              0 /*HPS*/,
                              0 /*HCS*/,
                              0 /*maxDelRevSeqno*/);
        checkPreFlushHTState();
    }

    // Check nothing persisted to disk
    auto kvstore = store->getRWUnderlying(vbid);
    const auto keyA = makeDiskDocKey("keyA", true);
    auto docA = kvstore->get(keyA, vbid);
    EXPECT_EQ(ENGINE_KEY_ENOENT, docA.getStatus());
    ASSERT_FALSE(docA.item);
    const auto keyB = makeDiskDocKey("keyB");
    auto docB = kvstore->get(keyB, vbid);
    EXPECT_EQ(ENGINE_KEY_ENOENT, docB.getStatus());
    ASSERT_FALSE(docB.item);

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    EXPECT_EQ(FlushResult(MoreAvailable::No, 2, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        // Notes: expected (snapStart = snapEnd) for complete snap flushed,
        //  which is always the case at Active
        checkPersistedVBState(3 /*lastSnapStart*/,
                              3 /*lastSnapEnd*/,
                              3 /*highSeqno*/,
                              CheckpointType::Memory,
                              1 /*HPS*/,
                              0 /*HCS*/,
                              2 /*maxDelRevSeqno*/);

        // Check HT state
        const auto resA = vb.ht.findForUpdate(makeStoredDocKey("keyA"));
        ASSERT_TRUE(resA.pending);
        ASSERT_FALSE(resA.pending->isDeleted());
        ASSERT_FALSE(resA.pending->isDirty());
        ASSERT_FALSE(resA.committed);

        const auto resB = vb.ht.findForUpdate(makeStoredDocKey("keyB"));
        ASSERT_FALSE(resB.pending);
        ASSERT_FALSE(resB.committed);
    }

    // Check persisted docs
    docA = kvstore->get(keyA, vbid);
    EXPECT_EQ(ENGINE_SUCCESS, docA.getStatus());
    ASSERT_TRUE(docA.item);
    ASSERT_GT(docA.item->getNBytes(), 0);
    EXPECT_EQ(std::string_view(valueA.c_str(), valueA.size()),
              std::string_view(docA.item->getData(), docA.item->getNBytes()));
    EXPECT_FALSE(docA.item->isDeleted());
    docB = kvstore->get(keyB, vbid);
    EXPECT_EQ(ENGINE_SUCCESS, docB.getStatus());
    EXPECT_EQ(0, docB.item->getNBytes());
    EXPECT_TRUE(docB.item->isDeleted());
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistNonMetaItems_ErrorWrite) {
    testFlushFailureAtPersistNonMetaItems(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistNonMetaItems_NoSuchFile) {
    testFlushFailureAtPersistNonMetaItems(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (2) by checking that we persist the new vbstate
 * when we re-attempt flush.
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistVBStateOnly(
        couchstore_error_t failureCode) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(failureCode))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    // Note: Because of MB-37920 we may cache a stale vbstate. So, checking the
    //  cached vbstate for verifying persistence would invalidate the test.
    //  Check the actual vbstate on disk instead.
    auto& kvStore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    const auto checkPersistedVBState =
            [this, &kvStore](vbucket_state_t expectedState) -> void {
        const auto vbs = kvStore.readVBState(vbid);
        ASSERT_EQ(expectedState, vbs.transition.state);
    };

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    {
        SCOPED_TRACE("");
        checkPersistedVBState(vbucket_state_active);
    }

    const auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(0, vb.dirtyQueueSize);

    const auto checkSetVBStateItemForCursor = [&vb]() -> void {
        const auto& manager = *vb.checkpointManager;
        auto pos = CheckpointCursorIntrospector::getCurrentPos(
                *manager.getPersistenceCursor());
        ASSERT_EQ(queue_op::set_vbucket_state, (*(pos++))->getOperation());
    };

    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_replica));
    {
        SCOPED_TRACE("");
        checkPersistedVBState(vbucket_state_active);
        checkSetVBStateItemForCursor();
        EXPECT_EQ(1, vb.dirtyQueueSize);
    }

    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(1, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkPersistedVBState(vbucket_state_active);
        checkSetVBStateItemForCursor();
    }

    // @todo MB-37920: Remove this step, necessary as a workaround.
    // To trigger a flush to disk, we need to alter the current cached vbstate,
    // as at the first (failed) flush we wrongly cached the vbstate that we did
    // not persist. If we skip this step then we would not even re-attempt the
    // flush as the optimization logic at KVStore::updateCachedVBState sees
    // "don't need to persist the new vbstate".
    kvStore.getVBucketState(vbid)->transition.state = vbucket_state_active;

    // This flush succeeds, we must write the new vbstate on disk
    // Note: set-vbstate items are not accounted in numFlushed
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkPersistedVBState(vbucket_state_replica);
    }
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistVBStateOnly_ErrorWrite) {
    testFlushFailureAtPersistVBStateOnly(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureAtPersistVBStateOnly_NoSuchFile) {
    testFlushFailureAtPersistVBStateOnly(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

/**
 * Check that flush stats are updated only at flush success.
 * Covers the case where the number of items pulled from the CheckpointManager
 * is different (higher) than the actual number of items flushed. Ie, flusher
 * deduplication occurs.
 *
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
void STParamPersistentBucketTest::testFlushFailureStatsAtDedupedNonMetaItems(
        couchstore_error_t failureCode, bool vbDeletion) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(failureCode))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Do we want to test the case where the flusher is running on a vbucket set
    // set for deferred deletion?
    // Nothing changes in the logic of this test, just that we hit an additional
    // code-path where flush-stats are wrongly updated at flush failure
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    if (vbDeletion) {
        vb.setDeferredDeletion(true);
    }

    // Active receives M(keyA):1, M(keyA):2.
    // They are queued into different checkpoints. We enforce that as we want to
    // stress deduplication at flush-vbucket, so we just avoid checkpoint dedup.

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    manager.createNewCheckpoint();
    ASSERT_EQ(0, manager.getNumOpenChkItems());

    const auto storedKey = makeStoredDocKey("keyA");
    const std::string value2 = "value2";
    {
        SCOPED_TRACE("");
        store_item(vbid,
                   storedKey,
                   value2,
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    ASSERT_EQ(2, manager.getNumItemsForPersistence());

    EXPECT_EQ(2, vb.dirtyQueueSize);

    const auto checkPreFlushHTState = [&vb, &storedKey]() -> void {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_TRUE(res.committed);
        ASSERT_FALSE(res.committed->isDeleted());
        ASSERT_TRUE(res.committed->isDirty());
    };
    checkPreFlushHTState();

    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb.dirtyQueueSize);
    // HT state
    checkPreFlushHTState();
    // No doc on disk
    auto kvstore = store->getRWUnderlying(vbid);
    const auto diskKey = makeDiskDocKey("keyA");
    auto doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(ENGINE_KEY_ENOENT, doc.getStatus());
    ASSERT_FALSE(doc.item);

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    // Flusher deduplication, just 1 item flushed
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::Yes),
              epBucket.flushVBucket(vbid));
    EXPECT_TRUE(vb.checkpointManager->hasClosedCheckpointWhichCanBeRemoved());
    // Flush stats updated
    EXPECT_EQ(0, vb.dirtyQueueSize);
    // HT state
    const auto res = vb.ht.findForUpdate(storedKey);
    ASSERT_FALSE(res.pending);
    ASSERT_TRUE(res.committed);
    ASSERT_FALSE(res.committed->isDeleted());
    ASSERT_FALSE(res.committed->isDirty());
    // doc persisted
    doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(ENGINE_SUCCESS, doc.getStatus());
    ASSERT_TRUE(doc.item);
    ASSERT_GT(doc.item->getNBytes(), 0);
    EXPECT_EQ(std::string_view(value2.c_str(), value2.size()),
              std::string_view(doc.item->getData(), doc.item->getNBytes()));
    EXPECT_FALSE(doc.item->isDeleted());

    // Cleanup: reset the flag to avoid that we schedule the actual deletion at
    //  TearDown, the ExecutorPool will be already gone at that point and the
    //  test will SegFault
    vb.setDeferredDeletion(false);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_ErrorWrite) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_NoSuchFile) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

TEST_P(STParamCouchstoreBucketTest,
       FlushFailureStatsAtDedupedNonMetaItems_VBDeletion) {
    testFlushFailureStatsAtDedupedNonMetaItems(COUCHSTORE_ERROR_WRITE, true);
}

/**
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
TEST_P(STParamCouchstoreBucketTest,
       BucketCreationFlagClearedOnlyAtFlushSuccess_PersistVBStateOnly) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    ASSERT_FALSE(engine->getKVBucket()->getVBucket(vbid));

    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(
                      vbid,
                      vbucket_state_active,
                      {{"topology",
                        nlohmann::json::array({{"active", "replica"}})}}));

    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);

    ASSERT_TRUE(vb->isBucketCreation());

    // This flush fails, the bucket creation flag must be still set
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(1, vb->dirtyQueueSize);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(1, vb->dirtyQueueSize);
    EXPECT_TRUE(vb->isBucketCreation());

    // This flush succeeds
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb->dirtyQueueSize);
    EXPECT_FALSE(vb->isBucketCreation());
}

/**
 * @TODO magma: Test does not run for magma as we don't yet have a way of inject
 * errors.
 */
TEST_P(STParamCouchstoreBucketTest,
       BucketCreationFlagClearedOnlyAtFlushSuccess_PersistVBStateAndMutations) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_ERROR_WRITE))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    ASSERT_FALSE(engine->getKVBucket()->getVBucket(vbid));

    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(
                      vbid,
                      vbucket_state_active,
                      {{"topology",
                        nlohmann::json::array({{"active", "replica"}})}}));

    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_TRUE(vb);

    ASSERT_TRUE(vb->isBucketCreation());

    store_item(vbid,
               makeStoredDocKey("key"),
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    // This flush fails, the bucket creation flag must be still set
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(2, vb->dirtyQueueSize);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(2, vb->dirtyQueueSize);
    EXPECT_TRUE(vb->isBucketCreation());

    // This flush succeeds
    // Note: the returned num-flushed does not account meta-items
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    EXPECT_EQ(0, vb->dirtyQueueSize);
    EXPECT_FALSE(vb->isBucketCreation());
}

void STParamPersistentBucketTest::testAbortDoesNotIncrementOpsDelete(
        bool flusherDedup) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(0, vb.getNumTotalItems());
    EXPECT_EQ(0, vb.opsDelete);

    // Active receives PRE:1
    const auto key = makeStoredDocKey("key");
    store_item(vbid,
               key,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::sync_write_pending} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES,
               {cb::durability::Requirements()});

    const auto& manager = *vb.checkpointManager;
    if (!flusherDedup) {
        // Flush PRE now (avoid Flush dedup)
        EXPECT_EQ(1, manager.getNumItemsForPersistence());
        flush_vbucket_to_disk(vbid, 1);
        EXPECT_EQ(0, manager.getNumItemsForPersistence());
        EXPECT_EQ(0, vb.getNumTotalItems());
        EXPECT_EQ(0, vb.opsDelete);
    }

    // ABORT:2
    ASSERT_EQ(1, manager.getHighSeqno());
    EXPECT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       1 /*prepareSeqno*/,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Flush ABORT
    EXPECT_EQ(flusherDedup ? 2 : 1, manager.getNumItemsForPersistence());
    flush_vbucket_to_disk(vbid, 1);
    EXPECT_EQ(0, manager.getNumItemsForPersistence());
    EXPECT_EQ(0, vb.getNumTotalItems());
    EXPECT_EQ(0, vb.opsDelete);
}

TEST_P(STParamPersistentBucketTest, AbortDoesNotIncrementOpsDelete) {
    testAbortDoesNotIncrementOpsDelete(true /*flusherDedup*/);
}

TEST_P(STParamPersistentBucketTest,
       AbortDoesNotIncrementOpsDelete_FlusherDedup) {
    testAbortDoesNotIncrementOpsDelete(false /*flusherDedup*/);
}

/**
 * Check that when persisting a delete and the flush fails:
 *  - flush-stats are not updated
 *  - the (deleted) item is not removed from the HashTable
 */
void STParamPersistentBucketTest::testFlushFailureAtPersistDelete(
        couchstore_error_t failureCode, bool vbDeletion) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);
    EXPECT_CALL(ops, sync(testing::_, testing::_))
            .Times(testing::AnyNumber())
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(COUCHSTORE_SUCCESS))
            .WillOnce(testing::Return(failureCode))
            .WillRepeatedly(testing::Return(COUCHSTORE_SUCCESS));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    if (vbDeletion) {
        vb.setDeferredDeletion(true);
    }

    // Active receives M(keyA):1 and deletion, M is deduplicated.
    const auto storedKey = makeStoredDocKey("keyA");
    store_item(vbid,
               storedKey,
               "value",
               0 /*exptime*/,
               {cb::engine_errc::success} /*expected*/,
               PROTOCOL_BINARY_RAW_BYTES);

    delete_item(vbid, storedKey);

    auto& manager = *vb.checkpointManager;
    ASSERT_EQ(1, manager.getNumOpenChkItems());
    // Mutation deduplicated, just deletion
    ASSERT_EQ(1, manager.getNumItemsForPersistence());

    // Pre-conditions:
    // - stats account for the deletion in the write queue
    // - the deletion is in the HashTable
    EXPECT_EQ(1, vb.dirtyQueueSize);
    const auto checkPreFlushHTState = [&vb, &storedKey]() -> void {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_TRUE(res.committed);
        ASSERT_TRUE(res.committed->isDeleted());
        ASSERT_TRUE(res.committed->isDirty());
    };
    checkPreFlushHTState();

    // Test: flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    ASSERT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));

    // Post-conditions:
    //  - no doc on disk
    //  - flush stats not updated
    //  - the deletion is still dirty in the HashTable
    auto kvstore = store->getRWUnderlying(vbid);
    const auto diskKey = makeDiskDocKey("keyA");
    auto doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(ENGINE_KEY_ENOENT, doc.getStatus());
    ASSERT_FALSE(doc.item);
    EXPECT_EQ(1, vb.dirtyQueueSize);
    checkPreFlushHTState();

    // Check out that all goes well when we re-attemp the flush

    // This flush succeeds, we must write all the expected items on disk.
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Doc on disk, flush stats updated and deletion removed from the HT
    doc = kvstore->get(diskKey, vbid);
    EXPECT_EQ(ENGINE_SUCCESS, doc.getStatus());
    EXPECT_EQ(0, doc.item->getNBytes());
    EXPECT_TRUE(doc.item->isDeleted());
    EXPECT_EQ(0, vb.dirtyQueueSize);
    {
        const auto res = vb.ht.findForUpdate(storedKey);
        ASSERT_FALSE(res.pending);
        ASSERT_FALSE(res.committed);
    }

    // All done, nothing to flush
    ASSERT_EQ(0, manager.getNumItemsForPersistence());
    EXPECT_EQ(FlushResult(MoreAvailable::No, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));

    vb.setDeferredDeletion(false);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_ErrorWrite) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_WRITE);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_NoSuchFile) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_NO_SUCH_FILE);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPerstingDelete_VBDeletion) {
    testFlushFailureAtPersistDelete(COUCHSTORE_ERROR_WRITE, true);
}

TEST_P(STParamCouchstoreBucketTest, FlushFailureAtPersistingCollectionChange) {
    ::testing::NiceMock<MockOps> ops(create_default_file_ops());
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceCouchKVStore(dynamic_cast<CouchKVStoreConfig&>(nonConstConfig), ops);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    CollectionsManifest cm(CollectionEntry::dairy);
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    vb->updateFromManifest({cm});

    // Check nothing persisted to disk, only default collection exists
    auto* kvstore = store->getRWUnderlying(vbid);
    auto m1 = kvstore->getCollectionsManifest(vbid);
    EXPECT_EQ(1, m1.collections.size());
    const Collections::CollectionMetaData defaultState;
    EXPECT_EQ(defaultState, m1.collections[0].metaData);
    EXPECT_EQ(0, m1.collections[0].startSeqno);
    // This flush fails, we have not written anything to disk
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    {
        EXPECT_CALL(ops, open(testing::_, testing::_, testing::_, testing::_))
                .WillOnce(testing::Return(COUCHSTORE_ERROR_OPEN_FILE))
                .RetiresOnSaturation();
        EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
                  epBucket.flushVBucket(vbid));
        // Flush stats not updated
        EXPECT_EQ(1, vb->dirtyQueueSize);
    }
    ops.DelegateToFake();
    EXPECT_CALL(ops, open(testing::_, testing::_, testing::_, testing::_))
            .Times(::testing::AnyNumber())
            .RetiresOnSaturation();
    // Check nothing persisted to disk, only default collection exists
    auto m2 = kvstore->getCollectionsManifest(vbid);
    EXPECT_EQ(1, m2.collections.size());
    EXPECT_EQ(defaultState, m2.collections[0].metaData);
    EXPECT_EQ(0, m2.collections[0].startSeqno);

    // This flush succeeds
    EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb->dirtyQueueSize);

    auto m3 = kvstore->getCollectionsManifest(vbid);
    EXPECT_EQ(2, m3.collections.size());

    Collections::CollectionMetaData dairyState{ScopeID::Default,
                                               CollectionEntry::dairy,
                                               CollectionEntry::dairy.name,
                                               {/*no ttl*/}};
    // no ordering of returned collections, both default and dairy must exist
    for (const auto& c : m3.collections) {
        if (c.metaData.cid == CollectionID::Default) {
            EXPECT_EQ(c.metaData, defaultState);
            EXPECT_EQ(0, c.startSeqno);
        } else {
            EXPECT_EQ(c.metaData, dairyState);
            EXPECT_EQ(1, c.startSeqno);
        }
    }
}

#ifdef EP_USE_MAGMA
class STParamMagmaBucketTest : public STParamPersistentBucketTest {};

/**
 * We flush if we have at least:
 *  1) one non-meta item
 *  2) or, one set-vbstate item in the write queue
 * In the two cases we execute two different code paths that may both fail and
 * trigger the reset of the persistence cursor.
 * This test verifies scenario (1) by checking that we persist all the expected
 * items when we re-attempt flush.
 *
 * @TODO MB-38377: With proper magma IO error injection we should turn off
 * background threads and use the IO error injection instead of mock functions.
 */
TEST_P(STParamMagmaBucketTest, ResetPCursorAtPersistNonMetaItems) {
    const auto& config = store->getRWUnderlying(vbid)->getConfig();
    auto& nonConstConfig = const_cast<KVStoreConfig&>(config);
    replaceMagmaKVStore(dynamic_cast<MagmaKVStoreConfig&>(nonConstConfig));

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Active receives PRE(keyA):1, M(keyB):2, D(keyB):3
    // Note that the set of mutation is just functional to testing that we write
    // to disk all the required vbstate entries at flush

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyA"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::sync_write_pending} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {cb::durability::Requirements()});
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES);
    }

    {
        SCOPED_TRACE("");
        store_item(vbid,
                   makeStoredDocKey("keyB"),
                   "value",
                   0 /*exptime*/,
                   {cb::engine_errc::success} /*expected*/,
                   PROTOCOL_BINARY_RAW_BYTES,
                   {} /*dur-reqs*/,
                   true /*deleted*/);
    }

    // M(keyB):2 deduplicated, just 2 items for cursor
    const auto vb = engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(2, vb->checkpointManager->getNumItemsForPersistence());
    EXPECT_EQ(2, vb->dirtyQueueSize);

    // Note: Because of MB-37920 we may cache a stale vbstate. So, checking the
    //  cached vbstate for verifying persistence would invalidate the test.
    //  Check the actual vbstate on disk instead.
    auto& kvStore =
            dynamic_cast<MockMagmaKVStore&>(*store->getRWUnderlying(vbid));
    const auto checkPersistedVBState = [this, &kvStore](
                                               uint64_t lastSnapStart,
                                               uint64_t lastSnapEnd,
                                               uint64_t highSeqno,
                                               CheckpointType type,
                                               uint64_t hps,
                                               uint64_t hcs,
                                               uint64_t maxDelRevSeqno) {
        const auto vbs = kvStore.readVBStateFromDisk(vbid).vbstate;
        EXPECT_EQ(lastSnapStart, vbs.lastSnapStart);
        EXPECT_EQ(lastSnapEnd, vbs.lastSnapEnd);
        EXPECT_EQ(highSeqno, vbs.highSeqno);
        EXPECT_EQ(type, vbs.checkpointType);
        EXPECT_EQ(hps, vbs.highPreparedSeqno);
        EXPECT_EQ(hcs, vbs.persistedCompletedSeqno);
        EXPECT_EQ(maxDelRevSeqno, vbs.maxDeletedSeqno);
    };

    // This flush fails, we have not written anything to disk
    kvStore.saveDocsErrorInjector = [](VB::Commit& cmt,
                                       kvstats_ctx& ctx) -> int {
        return magma::Status::IOError;
    };
    auto& epBucket = dynamic_cast<EPBucket&>(*store);
    EXPECT_EQ(FlushResult(MoreAvailable::Yes, 0, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats not updated
    EXPECT_EQ(2, vb->dirtyQueueSize);
    {
        SCOPED_TRACE("");
        checkPersistedVBState(0 /*lastSnapStart*/,
                              0 /*lastSnapEnd*/,
                              0 /*highSeqno*/,
                              CheckpointType::Memory,
                              0 /*HPS*/,
                              0 /*HCS*/,
                              0 /*maxDelRevSeqno*/);
    }

    // This flush succeeds, we must write all the expected items and new vbstate
    // on disk
    kvStore.saveDocsErrorInjector = nullptr;
    EXPECT_EQ(FlushResult(MoreAvailable::No, 2, WakeCkptRemover::No),
              epBucket.flushVBucket(vbid));
    // Flush stats updated
    EXPECT_EQ(0, vb->dirtyQueueSize);
    {
        SCOPED_TRACE("");
        // Notes: expected (snapStart = snapEnd) for complete snap flushed,
        //  which is always the case at Active
        checkPersistedVBState(3 /*lastSnapStart*/,
                              3 /*lastSnapEnd*/,
                              3 /*highSeqno*/,
                              CheckpointType::Memory,
                              1 /*HPS*/,
                              0 /*HCS*/,
                              2 /*maxDelRevSeqno*/);
    }
}

// We want to test what happens during an implicit magma compaction (in
// particular in regards to the CompactionConfig). Given that we call the same
// functions with a slightly different compaction_ctx object we can just test
// this by creating the compaction_ctx in the same way that we do for an
// implicit compaction and perform a normal compaction with this ctx.
// This test requires the full engine to ensure that we get correct timestamps
// for items as we delete them and all the required callbacks to perform
// compactions.
TEST_P(STParamMagmaBucketTest, implicitCompactionContext) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto firstDeletedKey = makeStoredDocKey("keyA");
    auto secondDeletedKey = makeStoredDocKey("keyB");

    store_item(vbid, firstDeletedKey, "value");
    delete_item(vbid, firstDeletedKey);
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Time travel 5 days, we want to drop the tombstone for this when we
    // compact
    TimeTraveller timmy{60 * 60 * 24 * 5};

    // Add a second tombstone to check that we don't drop everything
    store_item(vbid, secondDeletedKey, "value");
    delete_item(vbid, secondDeletedKey);

    // And a dummy item because we can't drop the final seqno
    store_item(vbid, makeStoredDocKey("dummy"), "value");

    flushVBucketToDiskIfPersistent(vbid, 2);

    auto magmaKVStore =
            dynamic_cast<MagmaKVStore*>(store->getRWUnderlying(vbid));
    ASSERT_TRUE(magmaKVStore);

    // Assert the state of the first key on disk
    auto gv = magmaKVStore->get(DiskDocKey(firstDeletedKey), Vbid(0));
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    ASSERT_TRUE(gv.item);
    ASSERT_TRUE(gv.item->isDeleted());

    // Assert the second of the first key on disk
    gv = magmaKVStore->get(DiskDocKey(secondDeletedKey), Vbid(0));
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    ASSERT_TRUE(gv.item);
    ASSERT_TRUE(gv.item->isDeleted());

    // And compact
    auto cctx = magmaKVStore->makeCompactionContext(vbid);

    EXPECT_TRUE(magmaKVStore->compactDB(cctx));

    // Check the first key on disk - should not exist
    gv = magmaKVStore->get(DiskDocKey(firstDeletedKey), Vbid(0));
    EXPECT_EQ(ENGINE_KEY_ENOENT, gv.getStatus());
    EXPECT_FALSE(gv.item);

    // Check the second key on disk - should be a tombstone
    gv = magmaKVStore->get(DiskDocKey(secondDeletedKey), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item);
    EXPECT_TRUE(gv.item->isDeleted());
}
#endif

INSTANTIATE_TEST_SUITE_P(Persistent,
                         STParamPersistentBucketTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(STParamPersistentBucketTest,
                         MB20054_SingleThreadedEPStoreTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(STParamCouchstoreBucketTest,
                         STParamCouchstoreBucketTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#ifdef EP_USE_MAGMA
INSTANTIATE_TEST_SUITE_P(STParamMagmaBucketTest,
                         STParamMagmaBucketTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
#endif

TEST_P(STParamCouchstoreBucketTest, FlusherMarksCleanBySeqno) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Used to synchronize this-thread (which simulate a frontend thread) and
    // the flusher-thread below (which simulate the flusher running in a bg
    // thread) so that we produce the exec interleaving of a scenario that
    // allows the user reading a stale seqno from disk.
    // Before the fix, that is possible because he flusher marks-clean items in
    // in the HashTable by CAS. Fixed by using Seqno instead.
    // Note: The scenario showed here a perfectly legal case of XDCR setup where
    // 2 different source clusters replicate to the same destination cluster.
    ThreadGate tg{2};

    auto& kvstore = dynamic_cast<CouchKVStore&>(*store->getRWUnderlying(vbid));
    kvstore.setPostFlushHook([&tg]() {
        // The hook is executed after we have flushed to disk but before we call
        // back into the PersistenceCallback. Here we use the hook only for
        // blocking the flusher and allowing a frontend write before it proceeds
        tg.threadUp();
    });

    const std::string key = "key";
    const auto setWithMeta = [this, &key](uint64_t cas,
                                          uint64_t revSeqno,
                                          uint64_t expectedSeqno) -> void {
        const std::string value = "value";
        const auto valBuf = cb::const_byte_buffer{
                reinterpret_cast<const uint8_t*>(key.data()), key.size()};
        uint64_t opCas = 0;
        uint64_t seqno = 0;
        const auto res = engine->public_setWithMeta(
                vbid,
                engine->public_makeDocKey(cookie, key),
                valBuf,
                {cas, revSeqno, 0 /*flags*/, 0 /*exp*/},
                false /*isDeleted*/,
                PROTOCOL_BINARY_RAW_BYTES,
                opCas,
                &seqno,
                cookie,
                {vbucket_state_active} /*permittedVBStates*/,
                CheckConflicts::Yes,
                true /*allowExisting*/,
                GenerateBySeqno::Yes,
                GenerateCas::No,
                {} /*extendedMetaData*/);
        ASSERT_EQ(ENGINE_SUCCESS, res);
        EXPECT_EQ(cas, opCas); // Note: CAS is not regenerated
        EXPECT_EQ(expectedSeqno, seqno);
    };

    // This-thread issues the first setWithMeta(s:1) and then blocks.
    // It must resume only when the flusher has persisted but not yet executed
    // into the PersistenceCallback.
    const uint64_t cas = 0x0123456789abcdef;
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 1 /*revSeqno*/, 1 /*expectedSeqno*/);
    }
    auto& vb = *engine->getKVBucket()->getVBucket(vbid);
    ASSERT_EQ(1, vb.checkpointManager->getNumItemsForPersistence());

    // Run the flusher in a bg-thread
    const auto flush = [this]() -> void {
        auto& epBucket = dynamic_cast<EPBucket&>(*store);
        const auto res = epBucket.flushVBucket(vbid);
        EXPECT_EQ(FlushResult(MoreAvailable::No, 1, WakeCkptRemover::No), res);
    };
    auto flusher = std::thread(flush);

    // This-thread issues a second setWithMeta(s:2), but only when the the
    // flusher is blocked into the postFlushHook.
    while (tg.getCount() < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // setWithMeta(s:2) with same CAS and higher revSeqno, so s:2 wins conflict
    // resolution and the operation succeeds.
    {
        SCOPED_TRACE("");
        setWithMeta(cas, 2 /*revSeqno*/, 2 /*expectedSeqno*/);
    }

    // Now I want the flusher to proceed and call into the PersistenceCallback.
    // Before the fix, the flusher uses CAS for identifying the StoredValue to
    // mark clean in the HashTable, so in this scenario that makes clean a
    // StoreValue (s:2) that has never been persisted.
    // Note: The flusher was already running before s:2 was queued for
    // persistence, so s:2 is not being persisted in this flusher run.
    tg.threadUp();
    flusher.join();

    // Now the ItemPager runs. In the HashTable we have s:2: it must be dirty
    // and so not eligible for eviction.
    auto& epVB = dynamic_cast<EPVBucket&>(vb);
    const auto docKey = makeStoredDocKey(key);
    {
        const auto readHandle = vb.lockCollections();
        auto res = vb.ht.findOnlyCommitted(docKey);
        ASSERT_TRUE(res.storedValue);
        ASSERT_EQ(2, res.storedValue->getBySeqno());
        EXPECT_TRUE(res.storedValue->isDirty());
        EXPECT_FALSE(epVB.pageOut(readHandle, res.lock, res.storedValue));
    }

    // Note: The flusher has never persisted s:2
    ASSERT_EQ(1, vb.getPersistenceSeqno());

    // Try a get, it must fetch s:2 from the HashTable
    const auto res = engine->get(cookie, docKey, vbid, DocStateFilter::Alive);
    // Note: Before the fix we get EWOULDBLOCK as s:2 would be evicted
    ASSERT_EQ(cb::engine_errc::success, res.first);
    const auto* it = reinterpret_cast<const Item*>(res.second.get());
    EXPECT_EQ(2, it->getBySeqno());
}