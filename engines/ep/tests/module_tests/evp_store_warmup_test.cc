/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_item_freq_decayer.h"
#include "../mock/mock_kvstore.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/response.h"
#include "durability/durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_time.h"
#include "evp_store_durability_test.h"
#include "evp_store_single_threaded_test.h"
#include "failover-table.h"
#include "flusher.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "vbucket.h"
#include "vbucket_state.h"
#include "warmup.h"
#include <boost/filesystem.hpp>
#include <engines/ep/src/tasks.h>
#include <folly/synchronization/Baton.h>

class WarmupTest : public SingleThreadedKVBucketTest {
public:
    void MB_31450(bool newCheckpoint);

    void initialiseAndWarmupFully() {
        initialise(buildNewWarmupConfig(""));
        task_executor = reinterpret_cast<SingleThreadedExecutorPool*>(
                ExecutorPool::get());
        if (isPersistent()) {
            static_cast<EPBucket*>(engine->getKVBucket())
                    ->initializeWarmupTask();
            static_cast<EPBucket*>(engine->getKVBucket())->startWarmupTask();
        }
        runReadersUntilWarmedUp();
    }

    void tidyUpAbortedShutdown() {
        // switch context for when we destroy the engine
        ObjectRegistry::onSwitchThread(engine.get());
        // Mark the connection as dead for clean shutdown
        destroy_mock_cookie(cookie);
        engine->getDcpConnMap().manageConnections();
        // ensure all the task have done away so we can shutdown
        shutdownAndPurgeTasks(engine.get());
        // release the engine object which will cleanly clean up any objects
        // still in memory
        engine.reset(nullptr);
        ObjectRegistry::onSwitchThread(nullptr);
    };

    void testOperationsInterlockedWithWarmup(bool abortWarmup);
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
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

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
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

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
    ASSERT_EQ(cb::engine_errc::success, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_FALSE(info1.cas_is_hlc);

    // key2 should have a CAS generated from the HLC
    auto item2 = store->get(makeStoredDocKey("key2"), vbid, nullptr, {});
    ASSERT_EQ(cb::engine_errc::success, item2.getStatus());
    auto info2 = engine->getItemInfo(*item2.item);
    EXPECT_TRUE(info2.cas_is_hlc);
}

TEST_F(WarmupTest, fetchDocInDifferentCompressionModes) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), valueData);
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup("compression_mode=off");
    auto item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(cb::engine_errc::success, item1.getStatus());
    auto info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, info1.datatype);

    resetEngineAndWarmup("compression_mode=passive");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(cb::engine_errc::success, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);

    resetEngineAndWarmup("compression_mode=active");
    item1 = store->get(makeStoredDocKey("key1"), vbid, nullptr, {});
    ASSERT_EQ(cb::engine_errc::success, item1.getStatus());
    info1 = engine->getItemInfo(*item1.item);
    EXPECT_EQ((PROTOCOL_BINARY_DATATYPE_JSON | PROTOCOL_BINARY_DATATYPE_SNAPPY),
              info1.datatype);
}

TEST_F(WarmupTest, mightContainXattrs) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item, then make the VB appear old ready for warmup
    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);
    rewriteCouchstoreVBState(vbid, test_dbname, 1);

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
        return store->getMetaData(
                key, vbid, cookie, itemMeta, deleted, datatype);
    };
    auto engineResult = doGetMetaData();

    ASSERT_EQ(cb::engine_errc::success, engineResult);
    EXPECT_EQ(3, itemMeta.revSeqno);
}

TEST_F(WarmupTest, OperationsInterlockedWithWarmup) {
    testOperationsInterlockedWithWarmup(false);
}

TEST_F(WarmupTest, OperationsInterlockedWithWarmupCancelled) {
    testOperationsInterlockedWithWarmup(true);
}

/**
 * MB-25197, MB-34422 and MB-47851.
 * Some operations must block until warmup has loaded the vbuckets
 * Two variants of test - either let warmup complete successfully
 * (abortWarmup=false), or abort it before cookies would normally be notified.
 * In both cases all cookies should be unblocked.
 */
void WarmupTest::testOperationsInterlockedWithWarmup(bool abortWarmup) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key1"), "value");
    flush_vbucket_to_disk(vbid);

    resetEngineAndEnableWarmup();

    // Manually run the reader queue so that the warmup tasks execute whilst we
    // perform the interlocked operations
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    EXPECT_EQ(nullptr, store->getVBuckets().getBucket(vbid));
    auto* setVBStateCookie = create_mock_cookie(engine.get());
    auto* getFailoverCookie = create_mock_cookie(engine.get());
    auto* statsCookie1 = create_mock_cookie(engine.get());
    auto* statsCookie2 = create_mock_cookie(engine.get());
    auto* statsCookie3 = create_mock_cookie(engine.get());
    auto* delVbCookie = create_mock_cookie(engine.get());

    std::array<MockCookie*, 6> notifications{{setVBStateCookie,
                                              getFailoverCookie,
                                              statsCookie1,
                                              statsCookie2,
                                              statsCookie3,
                                              delVbCookie}};

    auto dummyAddStats = [](std::string_view, std::string_view, const void*) {

    };

    // Perform requests for each of the different calls which should return
    // EWOULDBLOCK if called before populateVBucketMap has completed.

    // Do a setVBState but don't flush it through. This call should be
    // failed ewouldblock whilst warmup has yet to attempt to create VBs.
    EXPECT_EQ(cb::engine_errc::would_block,
              store->setVBucketState(vbid,
                                     vbucket_state_active,
                                     {},
                                     TransferVB::No,
                                     setVBStateCookie));

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->get_failover_log(*getFailoverCookie,
                                       1 /*opaque*/,
                                       vbid,
                                       fakeDcpAddFailoverLog));

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->get_stats(*statsCookie1, "vbucket", {}, dummyAddStats));

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->get_stats(
                      *statsCookie2, "vbucket-details", {}, dummyAddStats));

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->get_stats(
                      *statsCookie3, "vbucket-seqno", {}, dummyAddStats));

    EXPECT_EQ(cb::engine_errc::would_block,
              engine->deleteVBucket(*delVbCookie, vbid, true));

    // Unblock cookies - either by aborting warmup, or advancing warmup
    // far enough that the request can be issued successfully.
    if (abortWarmup) {
        engine->initiate_shutdown();
        engine->cancel_all_operations_in_ewb_state();
    } else {
        // Per warmup advance normally, until we have completed
        // populateVBucketMap.
        while (engine->getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
            CheckedExecutor executor(task_executor, readerQueue);
            executor.runCurrentTask();
        }
        EXPECT_NE(nullptr, store->getVBuckets().getBucket(vbid));
    }

    // Should have received one more notification than started with, and
    // with appropriate status code.
    const auto expectedStatus = abortWarmup ? cb::engine_errc::disconnect
                                            : cb::engine_errc::success;
    for (const auto& n : notifications) {
        EXPECT_EQ(expectedStatus, mock_waitfor_cookie(n));
    }

    if (!abortWarmup) {
        EXPECT_EQ(cb::engine_errc::success,
                  store->setVBucketState(vbid,
                                         vbucket_state_active,
                                         {},
                                         TransferVB::No,
                                         setVBStateCookie));

        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_failover_log(*getFailoverCookie,
                                           1 /*opaque*/,
                                           vbid,
                                           fakeDcpAddFailoverLog));

        EXPECT_EQ(
                cb::engine_errc::success,
                engine->get_stats(*statsCookie1, "vbucket", {}, dummyAddStats));

        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_stats(
                          *statsCookie2, "vbucket-details", {}, dummyAddStats));

        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_stats(
                          *statsCookie3, "vbucket-seqno", {}, dummyAddStats));

        EXPECT_EQ(cb::engine_errc::success,
                  engine->deleteVBucket(*delVbCookie, vbid, false));
    }

    // finish warmup so the test can exit
    while (engine->getKVBucket()->isWarmupLoadingData()) {
        CheckedExecutor executor(task_executor, readerQueue);
        executor.runCurrentTask();
    }

    for (const auto& n : notifications) {
        destroy_mock_cookie(n);
    }
}

/**
 * WarmupTest.MB_32577
 *
 * This test checks that we do not open DCP consumer connections until
 * warmup has finished. To prevent a race condition where a DCP deletion request
 * can be received for a replica and flushed to disk before the vbucket has
 * been fully initialisation.
 *
 * The test if performed by the following steps:
 * 1. Create a replica vbucket with a document in
 * 2. Warm up to till the point WarmupState::State::LoadingCollectionCounts
 *    at this point stop warmup and continue (we should be part warmed up)
 * 3. Open a DCP connection to the replica vbucket (this should fail with
 *    cb::engine_errc::temporary_failure as the vbucket is not warmed up)
 * 4. Send a DCP deletion for the document to the vbucket (this should fail with
 *    cb::engine_errc::disconnect as there shouldn't be a DCP connection open)
 * 5. Try and flush the vbucket, nothing should be flushed as the deletion
 *    should have failed
 * 6. Finish warming up the vbucket
 * 7. repeat steps 3, 4 and 5 which should now return cb::engine_errc::success
 * and the item should be deleted from disk as we are know fully warmed up.
 */
TEST_F(WarmupTest, MB_32577) {
    std::string keyName("key");
    std::string value("value");
    uint32_t zeroFlags{0};

    // create an active vbucket
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item to the vbucket
    store_item(vbid, makeStoredDocKey(keyName), value);
    // Change the type of vbucket to a replica
    store->setVBucketState(
            vbid, vbucket_state_replica, nullptr, TransferVB::Yes);
    // flush all document to disk
    flush_vbucket_to_disk(vbid);

    // check that the item has been recoded by collections
    ASSERT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // shutdown memcached
    shutdownAndPurgeTasks(engine.get());

    // reinitialise memcached and set everything up for warm up
    reinitialise("");
    if (engine->getConfiguration().getBucketType() == "persistent") {
        static_cast<EPBucket*>(engine->getKVBucket())->initializeWarmupTask();
        static_cast<EPBucket*>(engine->getKVBucket())->startWarmupTask();
    } else {
        FAIL() << "Should not reach here - persistent buckets only. type:"
               << engine->getConfiguration().getBucketType();
    }

    // get hold of a pointer to the WarmUp object
    auto* warmupPtr = store->getWarmup();

    // Run though all the warmup tasks till LoadingCollectionCounts task
    // at this point we want to stop as this is when we want to send a delete
    // request using DCP
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    while (store->isWarmupLoadingData()) {
        if (warmupPtr->getWarmupState() ==
            WarmupState::State::LoadingCollectionCounts) {
            break;
        }
        runNextTask(readerQueue);
    }

    // Try and set a DCP connection, this should return
    // cb::engine_errc::temporary_failure as were in warm up but without the fix
    // for MB-32577 this will return cb::engine_errc::success
    EXPECT_EQ(
            cb::engine_errc::temporary_failure,
            engine->open(*cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // The core will block all DCP commands for non-DCP connections

    // finish warmup so we can check the number of items in the engine
    while (store->isWarmupLoadingData()) {
        runNextTask(readerQueue);
    }

    // We shouldn't have deleted the doc and thus it should still be there
    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // Try and set a DCP connection, this should return cb::engine_errc::success
    // as we have now warmed up.
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->open(*cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // create a stream to send the delete request so we can send vbucket on the
    // consumer
    EXPECT_EQ(cb::engine_errc::success,
              engine->add_stream(*cookie, 0, vbid, 0));

    // create snapshot so we can delete the document
    EXPECT_EQ(cb::engine_errc::success,
              engine->snapshot_marker(*cookie,
                                      /*opaque*/ 1,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 100,
                                      zeroFlags,
                                      /*HCS*/ {},
                                      /*maxVisibleSeqno*/ {}));

    // create a DocKey object for the delete request
    const DocKey docKey{reinterpret_cast<const uint8_t*>(keyName.data()),
                        keyName.size(),
                        DocKeyEncodesCollectionId::No};

    // Try and delete the doc
    EXPECT_EQ(cb::engine_errc::success,
              engine->deletion(*cookie,
                               /*opaque*/ 1,
                               /*key*/ docKey,
                               /*value*/ {},
                               /*priv_bytes*/ 0,
                               /*datatype*/ PROTOCOL_BINARY_RAW_BYTES,
                               /*cas*/ 0,
                               /*vbucket*/ vbid,
                               /*bySeqno*/ 2,
                               /*revSeqno*/ 0,
                               /*meta*/ {}));

    // flush delete to disk
    EXPECT_NO_THROW(dynamic_cast<EPBucket&>(*store).flushVBucket(vbid));

    // We shouldn't have deleted the doc and thus it should still be there
    EXPECT_EQ(0,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // Close stream before deleting the connection
    engine->handleDisconnect(cookie);

    shutdownAndPurgeTasks(engine.get());
}

// KV-engine should warmup dead vbuckets
TEST_F(WarmupTest, MB_35599) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key"), "value");
    flush_vbucket_to_disk(vbid);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead);

    resetEngineAndWarmup();

    // We should be able to switch the vbucket back to active/replica and read
    // the value
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);

    auto gv = store->getReplica(makeStoredDocKey("key"), vbid, cookie, {});

    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ(0, memcmp("value", gv.item->getData(), 5));
}

// Check that two state changes don't de-duplicate, that replica is the state
// which lands in persistence. Note the addition of the key helped find an issue
// where the flusher re-ordered the flush batch, allowing the older set-vbstate
// to be flushed after the newer state (losing the replica state)
TEST_F(WarmupTest, SetVBState) {
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active));
    store_item(vbid, makeStoredDocKey("key"), "value");
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_replica));
    flush_vbucket_to_disk(vbid, 1);

    resetEngineAndWarmup();

    EXPECT_EQ(vbucket_state_replica, store->getVBucket(vbid)->getState());
}

TEST_F(WarmupTest, TwoStateChangesAtSameSeqno) {
    // 1) Do a normal state change to replica
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_replica));

    // 2) Make a change to the failover table as we would if we had just created
    // a stream to an active node
    auto failoverTableLastSeqno = 999;
    { // VBPtr scope (not valid after warmup)
        auto vb = store->getVBucket(vbid);
        vb->failovers->createEntry(failoverTableLastSeqno);
        EXPECT_EQ(2, vb->failovers->getNumEntries());
        EXPECT_EQ(failoverTableLastSeqno,
                  vb->failovers->getLatestEntry().by_seqno);
    }
    store->scheduleVBStatePersist(vbid);

    // 3) Flush the two state changes together and warmup so that we can more
    // easily read what was persisted
    flush_vbucket_to_disk(vbid, 0);
    resetEngineAndWarmup();

    // Test
    auto vb = store->getVBucket(vbid);
    auto state = store->getVBucket(vbid)->getTransitionState();
    EXPECT_EQ(vbucket_state_replica, vb->getState());
    EXPECT_EQ(failoverTableLastSeqno, vb->failovers->getLatestEntry().by_seqno);
}

// Test uses a sequence of operations that will mean delete is de-duplicated
// resulting in the flusher missing the update to the on-disk max-deleted
// revision.
void WarmupTest::MB_31450(bool newCheckpoint) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    store_item(vbid, makeStoredDocKey("key1"), "value"); // rev:=1
    store_item(vbid, makeStoredDocKey("key1"), "value"); // rev:=2
    delete_item(vbid, makeStoredDocKey("key1")); // delrev:= rev + 1
    if (newCheckpoint) {
        engine->getVBucket(vbid)->checkpointManager->createNewCheckpoint();
    }
    store_item(vbid, makeStoredDocKey("key1"), "value"); // de-dup the delete

    // Now flush, without the fix delrev on disk is 0.
    flush_vbucket_to_disk(vbid);

    // Now ADD an item, it is given an initial rev of delrev + 1 (4)
    // Then do a setWithMeta with a lower rev and expect conflict resolution to
    // fail the operation
    store_item(vbid, makeStoredDocKey("key2"), "value");
    auto item1 = make_item(vbid, makeStoredDocKey("key2"), "fail-me");
    item1.setCas(1);
    item1.setRevSeqno(3);
    uint64_t seqno;
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              store->setWithMeta(std::ref(item1),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));

    // Now warmup, the ADD of key2 is lost, we will redo it post warmup
    resetEngineAndWarmup();

    store_item(vbid, makeStoredDocKey("key2"), "value");
    auto item2 = make_item(vbid, makeStoredDocKey("key2"), "fail-me");
    item2.setCas(1);
    item2.setRevSeqno(3);

    // This expects would fail without the fix, the ADD of key2 previously
    // resulted in a rev of 1 (delrev + 1) and the setWithMeta would succeed.
    EXPECT_EQ(cb::engine_errc::key_already_exists,
              store->setWithMeta(std::ref(item2),
                                 0,
                                 &seqno,
                                 cookie,
                                 {vbucket_state_active},
                                 CheckConflicts::Yes,
                                 /*allowExisting*/ true));
}

TEST_F(WarmupTest, MB_31450_newCp) {
    MB_31450(true);
}

TEST_F(WarmupTest, MB_31450) {
    MB_31450(false);
}

// Test fixture for Durability-related Warmup tests.
class DurabilityWarmupTest : public DurabilityKVBucketTest {
protected:
    // Test that a pending SyncWrite/Delete not yet committed is correctly
    // warmed up when the bucket restarts.
    void testPendingSyncWrite(vbucket_state_t vbState,
                              const std::vector<std::string>& keys,
                              DocumentState docState);

    // Test that a pending SyncWrite/Delete which was committed is correctly
    // warmed up when the bucket restarts (as a Committed item).
    void testCommittedSyncWrite(vbucket_state_t vbState,
                                const std::vector<std::string>& keys,
                                DocumentState docState);

    // Test that a committed mutation followed by a pending SyncWrite to the
    // same key is correctly warmed up when the bucket restarts.
    void testCommittedAndPendingSyncWrite(vbucket_state_t vbState,
                                          DocumentState docState);

    // Helper method - fetches the given Item via engine->get(); issuing a
    // BG fetch and second get() if necessary.
    GetValue getItemFetchFromDiskIfNeeded(const DocKey& key,
                                          DocumentState docState);

    // Helper method - fetches the Item from disk by evicting the item then
    // running a bgfetch
    GetValue getItemFromDisk(const DocKey& key, DocumentState docState);

    enum class Resolution : uint8_t { Commit, Abort };

    /**
     * Test that when we complete a Prepare the correct HCS is persisted into
     * the local document.
     */
    void testHCSPersistedAndLoadedIntoVBState();

    /**
     * Test that we persist the checkpoint type correctly when we persist a
     * snapshot range.
     */
    void testCheckpointTypePersistedAndLoadedIntoVBState(CheckpointType type);

    void testFullyPersistedSnapshotSetsHPS(CheckpointType type);

    void testPartiallyPersistedSnapshotDoesNotSetHPS(CheckpointType type);

    /**
     * Test that a vbucket which was promoted from replica to active
     * mid-snapshot warms up the correct HPS value from the persisted state.
     */
    void testPromotedReplicaMidSnapshotHPS(CheckpointType type);

    /**
     * Test that a vbucket which was promoted from replica to active after
     * fully persisting a snapshot warms up the correct HPS value from the
     * persisted state.
     */
    void testPromotedReplicaCompleteSnapshotHPS(CheckpointType type);

    class PrePostStateChecker {
    public:
        explicit PrePostStateChecker(VBucketPtr vb);

        // Not copyable, only movable.
        PrePostStateChecker(const PrePostStateChecker&) = delete;
        PrePostStateChecker& operator=(const PrePostStateChecker&) = delete;
        PrePostStateChecker(PrePostStateChecker&&) = default;
        PrePostStateChecker& operator=(PrePostStateChecker&&) = default;

        ~PrePostStateChecker();

        void setVBucket(VBucketPtr vb) {
            this->vb = vb;
        }

        // Checker can be disabled if the test is doing something special, e.g.
        // driving the ADM directly
        void disable() {
            vb.reset();
        }

    private:
        VBucketPtr vb;
        int64_t preHPS = 0;
        int64_t preHCS = 0;
    };

    PrePostStateChecker resetEngineAndWarmup();

    void storePrepare(const std::string& key, uint64_t seqno, uint64_t cas);

    void storeMutation(const std::string& key, uint64_t seqno, uint64_t cas);

    void setupForPrepareWarmup(VBucket& vb, const StoredDocKey& key);
};

DurabilityWarmupTest::PrePostStateChecker::PrePostStateChecker(VBucketPtr vb) {
    EXPECT_TRUE(vb);
    preHPS = vb->getHighPreparedSeqno();
    preHCS = vb->getHighCompletedSeqno();
}

DurabilityWarmupTest::PrePostStateChecker::~PrePostStateChecker() {
    if (!vb) {
        return;
    }

    EXPECT_EQ(preHPS, vb->getHighPreparedSeqno())
            << "PrePostStateChecker: Found that post warmup the HPS does not "
               "match the pre-warmup value";
    EXPECT_EQ(preHCS, vb->getHighCompletedSeqno())
            << "PrePostStateChecker: Found that post warmup the HCS does not "
               "match the pre-warmup value";
}

DurabilityWarmupTest::PrePostStateChecker
DurabilityWarmupTest::resetEngineAndWarmup() {
    PrePostStateChecker checker(engine->getVBucket(vbid));
    DurabilityKVBucketTest::resetEngineAndWarmup();
    checker.setVBucket(engine->getVBucket(vbid));
    return checker;
}

void DurabilityWarmupTest::storePrepare(const std::string& key,
                                        uint64_t seqno,
                                        uint64_t cas) {
    auto vb = engine->getKVBucket()->getVBucket(vbid);

    auto tracked = vb->getDurabilityMonitor().getNumTracked();

    auto prepareKey = makeStoredDocKey(key);
    using namespace cb::durability;
    auto prepare = makePendingItem(
            prepareKey, "value", {Level::Majority, Timeout::Infinity()});
    prepare->setVBucketId(vbid);
    prepare->setBySeqno(seqno);
    prepare->setCas(cas);
    EXPECT_EQ(cb::engine_errc::success, store->prepare(*prepare, cookie));

    EXPECT_EQ(tracked + 1, vb->getDurabilityMonitor().getNumTracked());
}

void DurabilityWarmupTest::storeMutation(const std::string& key,
                                         uint64_t mutationSeqno,
                                         uint64_t cas) {
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    auto mutationKey = makeStoredDocKey(key);
    auto mutation = makeCommittedItem(mutationKey, "value");
    mutation->setBySeqno(mutationSeqno);
    mutation->setCas(cas);
    uint64_t* seqno = nullptr;
    EXPECT_EQ(cb::engine_errc::success,
              store->setWithMeta(*mutation,
                                 0,
                                 seqno,
                                 cookie,
                                 {vbucket_state_replica},
                                 CheckConflicts::No,
                                 true,
                                 GenerateBySeqno::No,
                                 GenerateCas::No,
                                 nullptr));
    EXPECT_EQ(mutationSeqno, vb->getHighSeqno());
}

GetValue DurabilityWarmupTest::getItemFetchFromDiskIfNeeded(
        const DocKey& key, DocumentState docState) {
    const auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto gv = engine->getKVBucket()->get(key, vbid, cookie, options);
    if (docState == DocumentState::Deleted) {
        // Need an extra bgFetch to get a deleted item.
        EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
        runBGFetcherTask();
        gv = engine->getKVBucket()->get(key, vbid, cookie, options);
    }
    return gv;
}

GetValue DurabilityWarmupTest::getItemFromDisk(const DocKey& key,
                                               DocumentState docState) {
    // Evict the key if it exists so that we read from disk. Hit the vBucket
    // level function (rather than the KVBucket level one) to ensure that this
    // works for replica vBuckets
    const char* msg;
    auto vb = store->getVBucket(vbid);
    vb->evictKey(&msg, vb->lockCollections(key));

    // And get. Again, hit the vBucket level function rather than the KVBucket
    // one to run the get against both active and replica vBuckets.
    const auto options =
            static_cast<get_options_t>(QUEUE_BG_FETCH | GET_DELETED_VALUE);
    auto gv = vb->getInternal(cookie,
                              *engine,
                              options,
                              VBucket::GetKeyOnly::No,
                              vb->lockCollections(key),
                              vb->getState() == vbucket_state_active
                                      ? ForGetReplicaOp::No
                                      : ForGetReplicaOp::Yes);

    EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
    runBGFetcherTask();
    gv = vb->getInternal(cookie,
                         *engine,
                         options,
                         VBucket::GetKeyOnly::No,
                         vb->lockCollections(key),
                         vb->getState() == vbucket_state_active
                                 ? ForGetReplicaOp::No
                                 : ForGetReplicaOp::Yes);
    return gv;
}

void DurabilityWarmupTest::testPendingSyncWrite(
        vbucket_state_t vbState,
        const std::vector<std::string>& keys,
        DocumentState docState) {
    // Store the given pending SyncWrites/Deletes (without committing) and then
    // restart

    auto vb = engine->getVBucket(vbid);
    auto numTracked = vb->getDurabilityMonitor().getNumTracked();

    for (const auto& k : keys) {
        // Previous runs could have left the VB into a non-active state - must
        // be active to perform set().
        if (vb->getState() != vbucket_state_active) {
            setVBucketToActiveWithValidTopology();
        }

        const auto key = makeStoredDocKey(k);
        auto item = makePendingItem(key, "pending_value");
        if (docState == DocumentState::Deleted) {
            item->setDeleted(DeleteSource::Explicit);
        }
        ASSERT_EQ(cb::engine_errc::sync_write_pending,
                  store->set(*item, cookie));
        flush_vbucket_to_disk(vbid);

        // Set the state that we want to test
        if (vbState != vbucket_state_active) {
            setVBucketStateAndRunPersistTask(vbid, vbState);
        }

        // About to destroy engine; reset vb shared_ptr.
        vb.reset();
        resetEngineAndWarmup();
        vb = engine->getVBucket(vbid);

        // Check that attempts to read this key via frontend are blocked.
        auto gv = store->get(key, vbid, cookie, {});
        EXPECT_EQ(cb::engine_errc::sync_write_re_commit_in_progress,
                  gv.getStatus());

        // Check that the item is still pending with the correct CAS.
        auto handle = vb->lockCollections(item->getKey());
        auto prepared = vb->fetchPreparedValue(handle);
        EXPECT_TRUE(prepared.storedValue);
        EXPECT_TRUE(prepared.storedValue->isPending());
        EXPECT_EQ(item->isDeleted(), prepared.storedValue->isDeleted());
        EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());

        // DurabilityMonitor be tracking the prepare.
        EXPECT_EQ(++numTracked, vb->getDurabilityMonitor().getNumTracked());

        EXPECT_EQ(numTracked,
                  store->getEPEngine().getEpStats().warmedUpPrepares);
        EXPECT_EQ(numTracked,
                  store->getEPEngine()
                          .getEpStats()
                          .warmupItemsVisitedWhilstLoadingPrepares);
    }
}

TEST_P(DurabilityWarmupTest, ActivePendingSyncWrite) {
    testPendingSyncWrite(vbucket_state_active,
                         {"key1", "key2", "key3"},
                         DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActivePendingSyncDelete) {
    testPendingSyncWrite(vbucket_state_active,
                         {"key1", "key2", "key3"},
                         DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaPendingSyncWrite) {
    testPendingSyncWrite(vbucket_state_replica,
                         {"key1", "key2", "key3"},
                         DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaPendingSyncDelete) {
    testPendingSyncWrite(vbucket_state_replica,
                         {"key1", "key2", "key3"},
                         DocumentState::Deleted);
}
void DurabilityWarmupTest::testCommittedSyncWrite(
        vbucket_state_t vbState,
        const std::vector<std::string>& keys,
        DocumentState docState) {
    if (isRocksDB()) {
        return; // Skipping for MB-36546
    }

    // Prepare
    testPendingSyncWrite(vbState, keys, docState);

    auto vb = engine->getVBucket(vbid);
    auto numTracked = vb->getDurabilityMonitor().getNumTracked();
    ASSERT_EQ(keys.size(), numTracked);

    if (vbState != vbucket_state_active) {
        vb->notifyPassiveDMOfSnapEndReceived(vb->getHighSeqno());
    }

    auto prepareSeqno = 1;
    for (const auto& k : keys) {
        // Commit
        const auto key = makeStoredDocKey(k);
        if (vbState == vbucket_state_active) {
            // Commit on active is driven by the ADM so we need to drive our
            // commit via seqno ack
            EXPECT_EQ(cb::engine_errc::success,
                      vb->seqnoAcknowledged(folly::SharedMutex::ReadHolder(
                                                    vb->getStateLock()),
                                            "replica",
                                            prepareSeqno++));
            vb->processResolvedSyncWrites();
        } else {
            // Commit on non-active is driven by VBucket::commit
            vb->commit(key, prepareSeqno++, {}, vb->lockCollections(key));
        }

        flush_vbucket_to_disk(vbid, 1);

        if (vbState != vbucket_state_active) {
            setVBucketStateAndRunPersistTask(vbid, vbState);
        }

        // Read from disk to ensure that the result is comparable with the read
        // after warmup (as we store commits on disk as mutations)
        const auto expectedItem = getItemFromDisk(key, docState);
        ASSERT_EQ(cb::engine_errc::success, expectedItem.getStatus());

        // About to destroy engine; reset vb shared_ptr.
        vb.reset();
        resetEngineAndWarmup();
        vb = engine->getVBucket(vbid);

        // Check the item state. Committed will be CommittedViaMutation rather
        // than CommittedViaPrepare as commits are stored as mutations on disk.
        GetValue gv = getItemFromDisk(key, docState);
        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        EXPECT_EQ(CommittedState::CommittedViaMutation,
                  gv.item->getCommitted());
        EXPECT_EQ(*expectedItem.item, *gv.item);

        // DurabilityMonitor should be empty as no outstanding prepares.
        EXPECT_EQ(--numTracked, vb->getDurabilityMonitor().getNumTracked());

        EXPECT_EQ(numTracked,
                  store->getEPEngine().getEpStats().warmedUpPrepares);
        EXPECT_EQ(numTracked,
                  store->getEPEngine()
                          .getEpStats()
                          .warmupItemsVisitedWhilstLoadingPrepares);
    }
}

TEST_P(DurabilityWarmupTest, ActiveCommittedSyncWrite) {
    testCommittedSyncWrite(vbucket_state_active,
                           {"key1", "key2", "key3"},
                           DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedSyncDelete) {
    testCommittedSyncWrite(vbucket_state_active,
                           {"key1", "key2", "key3"},
                           DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedSyncWrite) {
    testCommittedSyncWrite(vbucket_state_replica,
                           {"key1", "key2", "key3"},
                           DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedSyncDelete) {
    testCommittedSyncWrite(vbucket_state_replica,
                           {"key1", "key2", "key3"},
                           DocumentState::Deleted);
}

void DurabilityWarmupTest::testCommittedAndPendingSyncWrite(
        vbucket_state_t vbState, DocumentState docState) {
    // Store committed mutation followed by a pending SyncWrite (without
    // committing) and then restart.
    auto key = makeStoredDocKey("key");
    auto committedItem = store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    if (docState == DocumentState::Deleted) {
        item->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    flush_vbucket_to_disk(vbid, 2);

    if (vbState != vbucket_state_active) {
        setVBucketStateAndRunPersistTask(vbid, vbState);
    }
    resetEngineAndWarmup();
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(2,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Should load two items into memory - both committed and the pending value.
    // Check the original committed value is inaccessible due to the pending
    // needing to be re-committed.
    auto vb = engine->getVBucket(vbid);
    // @TODO: RocksDB currently only has an estimated item count in
    // full-eviction, so it fails this check. Skip if RocksDB && full_eviction.
    if ((!isRocksDB()) || std::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(1, vb->getNumTotalItems());
    }
    EXPECT_EQ(1, vb->ht.getNumPreparedSyncWrites());

    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(cb::engine_errc::sync_write_re_commit_in_progress,
              gv.getStatus());

    // Check that the item is still pending.
    {
        auto handle = vb->lockCollections(item->getKey());
        auto prepared = vb->fetchPreparedValue(handle);
        EXPECT_TRUE(prepared.storedValue);
        EXPECT_TRUE(prepared.storedValue->isPending());
        EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());
        EXPECT_EQ("B", prepared.storedValue->getValue()->to_s());
    }

    // DurabilityMonitor be tracking the prepare.
    EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

    // Abort the prepare so we can validate the previous Committed value
    // is present, readable and the same it was before warmup.
    {
        auto handle = vb->lockCollections(item->getKey());
        ASSERT_EQ(cb::engine_errc::success,
                  vb->abort(key, item->getBySeqno(), {}, handle));
    }
    gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ(committedItem, *gv.item);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedAndPendingSyncWrite) {
    testCommittedAndPendingSyncWrite(vbucket_state_active,
                                     DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ActiveCommittedAndPendingSyncDelete) {
    testCommittedAndPendingSyncWrite(vbucket_state_active,
                                     DocumentState::Deleted);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedAndPendingSyncWrite) {
    testCommittedAndPendingSyncWrite(vbucket_state_replica,
                                     DocumentState::Alive);
}

TEST_P(DurabilityWarmupTest, ReplicaCommittedAndPendingSyncDelete) {
    testCommittedAndPendingSyncWrite(vbucket_state_replica,
                                     DocumentState::Deleted);
}

// Negative test - check that a prepared SyncWrite which has been Aborted
// does _not_ restore the old, prepared SyncWrite after warmup.
TEST_P(DurabilityWarmupTest, AbortedSyncWritePrepareIsNotLoaded) {
    // Commit an initial value 'A', then prepare and then abort a SyncWrite of
    // "B".
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid, 2);

    { // scoping vb - is invalid once resetEngineAndWarmup() is called.
        auto vb = engine->getVBucket(vbid);
        EXPECT_EQ(1, vb->getNumItems());
        // Force an abort
        vb->processDurabilityTimeout(std::chrono::steady_clock::now() +
                                     std::chrono::seconds(1000));
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 1);
        EXPECT_EQ(1, vb->getNumItems());
    }
    resetEngineAndWarmup();
    EXPECT_EQ(0, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(0,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Should load one item into memory - committed value.
    auto vb = engine->getVBucket(vbid);
    // @TODO: RocksDB currently only has an estimated item count in
    // full-eviction, so it fails this check. Skip if RocksDB && full_eviction.
    if (!isRocksDB() || std::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(1, vb->getNumItems());
    }
    EXPECT_EQ(0, vb->ht.getNumPreparedSyncWrites());
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(cb::engine_errc::success, gv.getStatus());
    EXPECT_EQ("A", gv.item->getValue()->to_s());

    // Check there's no pending item
    auto handle = vb->lockCollections(item->getKey());
    auto prepared = vb->fetchPreparedValue(handle);
    EXPECT_FALSE(prepared.storedValue);

    // DurabilityMonitor should be empty.
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
}

// Test that not having a replication topology stored on disk (i.e. pre v6.5
// file) is correctly handled during warmup.
TEST_P(DurabilityWarmupTest, ReplicationTopologyMissing) {
    // Store an item, then make the VB appear old ready for warmup
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    // Remove the replicationTopology and re-persist.
    auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    auto vbstate = *kvstore->getCachedVBucketState(vbid);
    vbstate.transition.replicationTopology.clear();
    kvstore->snapshotVBucket(vbid, vbstate);

    resetEngineAndWarmup();
    EXPECT_EQ(0, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(0,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    // Check topology is empty.
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(nlohmann::json().dump(), vb->getReplicationTopology().dump());
}

// Test that replication topology is correctly loaded from disk during warmup.
TEST_P(DurabilityWarmupTest, ReplicationTopologyLoaded) {
    // Change the replication topology to a specific value (different from
    // normal test SetUp method).
    const auto topology = nlohmann::json::array(
            {{"other_active", "other_replica", "other_replica2"}});
    setVBucketToActiveWithValidTopology(topology);

    resetEngineAndWarmup();

    // Check topology has been correctly loaded from disk.
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(topology.dump(), vb->getReplicationTopology().dump());
}

void DurabilityWarmupTest::setupForPrepareWarmup(VBucket& vb,
                                                 const StoredDocKey& key) {
    // Change the replication topology to a specific value (different from
    // normal test SetUp method).
    const auto topology = nlohmann::json::array({{"active"}});
    setVBucketToActiveWithValidTopology(topology);
    auto item = makePendingItem(key, "do");
    { // collections read-lock scope
        auto cHandle = vb.lockCollections(item->getKey());
        EXPECT_TRUE(cHandle.valid());
        // Use vb level set so that the commit doesn't yet happen, we want to
        // simulate the prepare, but not commit landing on disk
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb.set(*item, cookie, *engine, {}, cHandle));
    }

    // Need at least one committed item on disk for warmup to run the
    // LoadingData / LoadingKVPairs step, which some tests need to check for.
    store_item(vbid, makeStoredDocKey("dummy"), "A");

    flush_vbucket_to_disk(vbid, 2);
}

// Test that if we 'crashed' whilst committing, that the warmup will re-commit
TEST_P(DurabilityWarmupTest, WarmupCommit) {
    auto vb = store->getVBucket(vbid);
    auto key = makeStoredDocKey("key");
    setupForPrepareWarmup(*vb, key);

    // Now warmup, we've stored the prepare but never made it to commit
    // Because we bypassed KVBucket::set the HPS/HCS will be incorrect and fail
    // the pre/post warmup checker, so disable the checker for this test.
    vb.reset();
    resetEngineAndWarmup().disable();
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(1,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);

    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    vb->processResolvedSyncWrites();

    auto sv = vb->ht.findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isCommitted());
}

// Verify that if a SyncWrite is re-committed during warmup; if the re-commit
// occurs before warmup has completed, newly-committed document is warmed-up
// and then Persistance Callback runs, this is handled correctly.
// Regression test for MB-41658.
TEST_P(DurabilityWarmupTest, WarmupCommitRaceWithPersistence) {
    if (isNexus()) {
        // The various callbacks cause us to deadlock on the Nexus vbucket lock
        GTEST_SKIP();
    }

    // Setup: Create and flush a prepare to disk.
    auto vb = store->getVBucket(vbid);
    auto key = makeStoredDocKey("key");
    setupForPrepareWarmup(*vb, key);

    // Setup: Restart engine and begin warmup. This is a single-threaded
    // test so we will run each phase in this main thread; using testing hooks
    // to ensure steps are interleaved as we require:
    // [1] Warmup loads prepare from disk.
    // [2] ActiveDM completes prepare, adding Committed SyncWrite to HashTable
    //     (as dirty).
    // [3] Persistence runs, writing the Commit to disk - but crucially
    //     *doesn't* get as far as executing PersistenceCallback.
    // [4] Warmup advances to value load phase (LoadingKVPairs / LoadingData
    //     depending on eviction mode), and loads the Commit from [3] into
    //     HashTable (setting it clean).
    // [5] Persistence Callback runs, encounters a clean item in HT for
    //     item just persisted - BUG.
    vb.reset();
    resetEngineAndEnableWarmup();
    // Increase early warmup finish item percentage to 200% - this is needed
    // to avoid warmup finishing early before loading our new committed
    // syncWrite (if we leave at default 100% it will stop as soon as it's
    // warmed up the original 1 item it identified earlier in warmup).
    // In production this isn't needed as data is loaded for many different
    // vBuckets; but to simplify here we just stick with a single VB.
    engine->getEpStats().warmupNumReadCap = 2.0;

    // Batons used to synchronise the various setup phases:
    folly::Baton<true> syncWriteCommittedBaton;
    folly::Baton<true> syncWritePersistedBaton;
    folly::Baton<true> warmupDoneBaton;

    // Setup for [1, 2, 4]: use the warmup transition hook to trigger
    // commit of all resolved sync writes once they have been loaded from disk
    // during warmup. (This would normally be done by DurabilityCompletionTask
    // but we are running single-threaded here).
    store->getWarmup()->stateTransitionHook =
            [this,
             &key,
             &syncWriteCommittedBaton,
             &syncWritePersistedBaton,
             &warmupDoneBaton](const WarmupState::State& state) {
                if (state == WarmupState::State::LoadingData ||
                    state == WarmupState::State::LoadingKVPairs) {
                    // Reached data load phase of warmup; prepares have already
                    // been loaded. [2]: Have ActiveDM commit this prepare.
                    auto vb = store->getVBucket(vbid);
                    ASSERT_TRUE(vb);

                    // Sanity check - the prepare should have been warmed up.
                    const auto* sv = vb->ht.findForWrite(key).storedValue;
                    ASSERT_TRUE(sv);
                    ASSERT_TRUE(sv->isPending());
                    vb->processResolvedSyncWrites();

                    // After running ActiveDM; item should be committed and
                    // dirty in HT.
                    sv = vb->ht.findForRead(key).storedValue;
                    ASSERT_TRUE(sv);
                    EXPECT_TRUE(sv->isCommitted());
                    EXPECT_TRUE(sv->isDirty());

                    // Wake up flusher in separate thread.
                    syncWriteCommittedBaton.post();

                    // [4]: Block data load until flusher has written Commit to
                    // disk.
                    syncWritePersistedBaton.wait();
                }
                // [5]: Once data load has completed, allow persistence callback
                // to run.
                if (state == WarmupState::State::Done) {
                    warmupDoneBaton.post();
                }
            };

    // Setup for [3]: Wait for [2]; then run flusher in background thread to
    // write commit to disk.
    folly::Baton<true> flusherCompletedBaton;
    std::thread flusherThread(
            [this, &syncWriteCommittedBaton, &flusherCompletedBaton]() {
                // Block thread until SyncWrite has been committed during
                // warmup.
                syncWriteCommittedBaton.wait();
                // Run flusher so Commit can be written to disk before
                // State::LoadingKVPairs / State::LoadingData. It will be inside
                // this call the persistence callback throws with the bug.
                flush_vbucket_to_disk(vbid, 1);

                // Notify main thread persistence is completed.
                flusherCompletedBaton.post();
            });

    // Setup for [5]: Use PostFlushHook to unblock warmup so it can run _before_
    // PersistenceCallback.
    store->getRWUnderlying(vbid)->setPostFlushHook(
            [this, &key, &syncWritePersistedBaton, &warmupDoneBaton] {
                // The hook is executed on background writer after we have
                // flushed to disk but before we call back into the
                // PersistenceCallback. Unblock warmup value load
                syncWritePersistedBaton.post();

                // Wait until warmup has completed; to ensure that
                // PersistenceCallback executes once Committed item has been
                // loaded from disk into HT.
                warmupDoneBaton.wait();
            });

    // GO! Start running the Warmup tasks which are on Reader threads.
    runReadersUntilWarmedUp();

    // Block until persistence is complete; in original MB the bug would have
    // thrown in persistence thread by now.
    flusherCompletedBaton.wait();

    // Postcondition: verify the Committed item is now marked as clean.
    auto sv = store->getVBucket(vbid)->ht.findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    EXPECT_TRUE(sv->isCommitted());
    EXPECT_FALSE(sv->isDirty());

    flusherThread.join();
}

void DurabilityWarmupTest::testCheckpointTypePersistedAndLoadedIntoVBState(
        CheckpointType type) {
    auto vb = store->getVBucket(vbid);
    vb->checkpointManager->createSnapshot(
            1 /*snapStart*/, 1 /*snapEnd*/, 0 /*HCS*/, type, 0);

    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "do");
    item->setBySeqno(1);
    { // collections read-lock scope
        auto cHandle = vb->lockCollections(item->getKey());
        EXPECT_TRUE(cHandle.valid());
        // Use vb level set so that the commit doesn't yet happen, we want to
        // simulate the prepare, but not commit landing on disk
        EXPECT_EQ(cb::engine_errc::sync_write_pending,
                  vb->set(*item, cookie, *engine, {}, cHandle));
    }

    flushVBucketToDiskIfPersistent(vbid, 1);
    vb.reset();
    resetEngineAndWarmup();

    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(type,
              store->getRWUnderlying(vbid)
                      ->getCachedVBucketState(vbid)
                      ->checkpointType);
}

void DurabilityWarmupTest::testFullyPersistedSnapshotSetsHPS(
        CheckpointType cpType) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    { // Scope for VBPtr (invalid after reset and warmup)
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // Receive a snapshot
        // 1        2
        // Prepare, Mutation
        vb->checkpointManager->createSnapshot(
                1 /*snapStart*/,
                2 /*snapEnd*/,
                (cpType == CheckpointType::Disk ? std::make_optional(1)
                                                : std::nullopt) /*HCS*/,
                cpType,
                0);
        ASSERT_EQ(cpType == CheckpointType::Disk,
                  vb->isReceivingDiskSnapshot());

        // 1) Receive the prepare
        storePrepare("prepare", 1, 1);

        // 2) Unrelated mutation at seqno 2
        storeMutation("mutation", 2, 2);

        // Notify snap end to correct the in-memory HPS
        vb->notifyPassiveDMOfSnapEndReceived(2);

        // 3) Flush and shutdown
        flushVBucketToDiskIfPersistent(vbid, 2);
        EXPECT_EQ(cpType,
                  store->getRWUnderlying(vbid)
                          ->getCachedVBucketState(vbid)
                          ->checkpointType);
    }

    // HCS will be different post warmup as we don't have a precise value during
    // disk snapshots on replica (it will be 0 pre-reset because we have not
    // seen the completion for the prepare).
    // @TODO We could correct this by passing the HCS into the PDM when we pass
    // it into the CheckpointManager.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // Check that the HPS loaded from disk matches the expected value
    // disk -> HPS set to snap_end
    // memory -> HPS set to seqno of last prepare
    EXPECT_EQ(cpType == CheckpointType::Disk ? 2 : 1,
              vb->getDurabilityMonitor().getHighPreparedSeqno());
}

void DurabilityWarmupTest::testPartiallyPersistedSnapshotDoesNotSetHPS(
        CheckpointType cpType) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    { // Scope for VBPtr (invalid after reset and warmup)
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // Receive a partial snapshot
        // 1        2         3
        // Prepare, Mutation, NOT RECEIVED
        const auto hcs = (cpType == CheckpointType::Disk)
                                 ? std::make_optional(1)
                                 : std::nullopt;
        vb->checkpointManager->createSnapshot(
                1 /*snapStart*/, 3 /*snapEnd*/, hcs, cpType, 0);
        ASSERT_EQ(cpType == CheckpointType::Disk,
                  vb->isReceivingDiskSnapshot());

        // 1) Receive the prepare
        storePrepare("prepare", 1, 1);

        // 2) Unrelated mutation at seqno 2
        storeMutation("mutation", 2, 2);

        // 3) Flush and shutdown WITHOUT receiving the last item/notifying the
        // PDM of snap end
        flushVBucketToDiskIfPersistent(vbid, 2);
        EXPECT_EQ(cpType,
                  store->getRWUnderlying(vbid)
                          ->getCachedVBucketState(vbid)
                          ->checkpointType);
    }

    // HCS will be different post warmup as we don't have a precise value during
    // disk snapshots on replica (it will be 0 pre-reset because we have not
    // seen the completion for the prepare).
    // @TODO We could correct this by passing the HCS into the PDM when we pass
    // it into the CheckpointManager.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // HPS loaded from disk should not have moved, snapshot is incomplete
    EXPECT_EQ(0, vb->getDurabilityMonitor().getHighPreparedSeqno());
}

void DurabilityWarmupTest::testPromotedReplicaMidSnapshotHPS(
        CheckpointType type) {
    // start as replica, and reach a state where the on disk hps != pps
    testPartiallyPersistedSnapshotDoesNotSetHPS(type);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // HCS will be different post warmup as we don't have a precise value during
    // disk snapshots on replica (it will be 0 pre-reset because we have not
    // seen the completion for the prepare).
    // @TODO We could correct this by passing the HCS into the PDM when we pass
    // it into the CheckpointManager.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);

    store = engine->getKVBucket();
    auto* rwUnderlying = store->getRWUnderlying(vbid);
    const auto* persistedVbState = rwUnderlying->getCachedVBucketState(vbid);
    auto pps = persistedVbState->persistedPreparedSeqno;
    auto hps = persistedVbState->highPreparedSeqno;

    ASSERT_EQ(0, hps);
    ASSERT_EQ(1, pps);

    // the active should warmup hps as the high prepared seqno.
    // normally on an active hps==pps, as every flush batch persists
    // an entire snapshot. However, if the vb was previously a replica,
    // hps != pps may be found on disk.
    EXPECT_EQ(hps, vb->getDurabilityMonitor().getHighPreparedSeqno());
}

void DurabilityWarmupTest::testPromotedReplicaCompleteSnapshotHPS(
        CheckpointType type) {
    // start as replica, and reach a state where the on disk hps != pps
    testFullyPersistedSnapshotSetsHPS(type);

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // HCS will be different post warmup as we don't have a precise value during
    // disk snapshots on replica (it will be 0 pre-reset because we have not
    // seen the completion for the prepare).
    // @TODO We could correct this by passing the HCS into the PDM when we pass
    // it into the CheckpointManager.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);

    store = engine->getKVBucket();
    auto* rwUnderlying = store->getRWUnderlying(vbid);
    const auto* persistedVbState = rwUnderlying->getCachedVBucketState(vbid);
    auto pps = persistedVbState->persistedPreparedSeqno;
    auto hps = persistedVbState->highPreparedSeqno;

    // Disk snapshots advance the HPS to the snapshot end
    // to accomodate dedupe & purging
    auto snapEnd = 2;
    ASSERT_EQ(type == CheckpointType::Disk ? snapEnd : 1, hps);
    ASSERT_EQ(1, pps);

    // the active should warmup hps as the high prepared seqno.
    // normally on an active hps==pps, as every flush batch persists
    // an entire snapshot. However, if the vb was previously a replica,
    // hps != pps may be found on disk.
    EXPECT_EQ(hps, vb->getDurabilityMonitor().getHighPreparedSeqno());
}

TEST_P(DurabilityWarmupTest, TestCheckpointTypePersistedMemory) {
    testCheckpointTypePersistedAndLoadedIntoVBState(CheckpointType::Memory);
}

TEST_P(DurabilityWarmupTest, TestCheckpointTypePersistedDisk) {
    testCheckpointTypePersistedAndLoadedIntoVBState(CheckpointType::Disk);
}

void DurabilityWarmupTest::testHCSPersistedAndLoadedIntoVBState() {
    // Queue a Prepare
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(key, "value");
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie));

    // Check the Prepared
    const int64_t preparedSeqno = 1;
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    const auto* sv = vb->ht.findForWrite(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isPending());
    ASSERT_EQ(preparedSeqno, sv->getBySeqno());

    // Persist the Prepare and vbstate.
    flush_vbucket_to_disk(vbid);
    auto hps1 = engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno();
    vb.reset();
    resetEngineAndWarmup();

    // Check hps matches the pre-warmup value
    EXPECT_EQ(hps1,
              engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno());

    auto checkHCS = [this](int64_t hcs) -> void {
        auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
        auto vbstate = *kvstore->getCachedVBucketState(vbid);
        ASSERT_EQ(hcs, vbstate.persistedCompletedSeqno);
    };

    // HCS still 0 in vbstate
    checkHCS(0);

    // Complete the Prepare
    vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(cb::engine_errc::success,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      preparedSeqno));
    vb->processResolvedSyncWrites();

    sv = vb->ht.findForRead(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isCommitted());
    ASSERT_GT(sv->getBySeqno(), preparedSeqno);

    // Persist the Commit/Abort and vbstate.
    flush_vbucket_to_disk(vbid);
    checkHCS(preparedSeqno);

    vb.reset();
    resetEngineAndWarmup();

    // HCS must have been loaded from vbstate from disk
    checkHCS(preparedSeqno);
    EXPECT_EQ(preparedSeqno,
              engine->getKVBucket()->getVBucket(vbid)->getHighCompletedSeqno());
    EXPECT_EQ(preparedSeqno,
              engine->getKVBucket()->getVBucket(vbid)->getHighPreparedSeqno());
}

TEST_P(DurabilityWarmupTest, HCSPersistedAndLoadedIntoVBState_Commit) {
    testHCSPersistedAndLoadedIntoVBState();
}

TEST_P(DurabilityWarmupTest, testHPSPersistedAndLoadedIntoVBState) {
    // Queue a Prepare
    auto key = makeStoredDocKey("key");
    auto prepare = makePendingItem(key, "value");
    ASSERT_EQ(cb::engine_errc::sync_write_pending,
              store->set(*prepare, cookie));

    // Not flushed yet
    auto* kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    auto vbstate = *kvstore->getCachedVBucketState(vbid);
    ASSERT_EQ(0, vbstate.persistedPreparedSeqno);
    ASSERT_EQ(0, vbstate.onDiskPrepares);
    ASSERT_EQ(0, vbstate.getOnDiskPrepareBytes());

    // Check the Prepared
    const int64_t preparedSeqno = 1;
    auto vb = store->getVBucket(vbid);
    ASSERT_TRUE(vb);
    const auto* sv = vb->ht.findForWrite(key).storedValue;
    ASSERT_TRUE(sv);
    ASSERT_TRUE(sv->isPending());
    ASSERT_EQ(preparedSeqno, sv->getBySeqno());

    // Persist the Prepare and vbstate.
    flush_vbucket_to_disk(vbid);

    // HPS and prepare counter incremented
    vbstate = *kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(preparedSeqno, vbstate.persistedPreparedSeqno);

    // @TODO: RocksDB currently does not track the prepare count
    // Magma does not track the prepare count
    if (!isRocksDB() && !isMagma()) {
        EXPECT_EQ(1, vbstate.onDiskPrepares);
        // Hard to predict the size of the prepare on-disk, given it will
        // be compressed by couchstore. For simplicity just check it's non-zero.
        EXPECT_GT(vbstate.getOnDiskPrepareBytes(), 0);
    }

    // Warmup
    vb.reset();
    resetEngineAndWarmup();

    kvstore = engine->getKVBucket()->getRWUnderlying(vbid);
    vbstate = *kvstore->getCachedVBucketState(vbid);
    EXPECT_EQ(preparedSeqno, vbstate.persistedPreparedSeqno);

    // @TODO: RocksDB currently does not track the prepare count
    // Magma does not track the prepare count
    if (!isRocksDB() && !isMagma()) {
        EXPECT_EQ(1, vbstate.onDiskPrepares);
        EXPECT_GT(vbstate.getOnDiskPrepareBytes(), 0);
    }
}

// Test that when setting a vbucket to dead after warmup, when at least one
// Prepared SyncWrite is still pending, that notification ignores the null
// cookie from a warmed up SyncWrite.
TEST_P(DurabilityWarmupTest, SetStateDeadWithWarmedUpPrepare) {
    // Setup: Store a pending SyncWrite/Delete (without committing) and then
    // restart.
    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "pending_value");
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);
    resetEngineAndWarmup();

    // Sanity check - should have the SyncWrite after warmup and not committed.
    auto vb = engine->getVBucket(vbid);
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(cb::engine_errc::sync_write_re_commit_in_progress,
              gv.getStatus());

    // Test: Set state to dead. Should skip notification for the warmed-up
    // Prepare (as it has no cookie) when task is run.
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_dead));

    // No task scheduled because no prepare has a cookie (so cannot be notified)
    EXPECT_EQ(0,
              task_executor->getLpTaskQ()[NONIO_TASK_IDX]->getReadyQueueSize());
}

// Test actually covers an issue seen in MB-34956, the issue was just the lack
// of more complete warmup support which is added by MB-34910, in this test
// we check that even after some sync-writes have completed/committed we can
// still warmup and handle latent seqnoAcks, i.e. 0 prepares on disk but we have
// non zero HCS/HPS.
TEST_P(DurabilityWarmupTest, CommittedWithAckAfterWarmup) {
    auto key = makeStoredDocKey("okey");
    auto item = makePendingItem(key, "dokey");
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                1);
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 1);
    }
    resetEngineAndWarmup();
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                1);
    }
}

// MB-35192: EPBucket::flushVBucket calls
// rwUnderlying->prepareForDeduplication(items); Which may reorder the items
// before they are written to disk. Test to ensure the persisted HPS and HCS are
// set to the highest value found in the items that are about to be flushed.
TEST_P(DurabilityWarmupTest, WarmUpHPSAndHCSWithNonSeqnoSortedItems) {
    auto key = makeStoredDocKey("okey");

    // These items will be sorted by key by prepareForDeduplication
    // ordering them a -> b, the opposite order to their seqnos.
    auto itemB = makePendingItem(makeStoredDocKey("b"), "value");
    auto itemA = makePendingItem(makeStoredDocKey("a"), "value");
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*itemB, cookie));
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*itemA, cookie));
    SCOPED_TRACE("A");
    flush_vbucket_to_disk(vbid, 2);
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                2);
        vb->processResolvedSyncWrites();

        flush_vbucket_to_disk(vbid, 2);
    }
    SCOPED_TRACE("B");
    resetEngineAndWarmup();
    {
        auto vb = engine->getVBucket(vbid);
        vb->seqnoAcknowledged(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                "replica",
                2);
    }
}

// Manipulate a replicaVB as if it is receiving from an active (calling correct
// replica methods) and test the VB warms up.
TEST_P(DurabilityWarmupTest, ReplicaVBucket) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    auto key = makeStoredDocKey("okey");
    auto item = makePendingItem(key, "dokey");
    item->setCas(1);
    item->setBySeqno(1);
    item->setPendingSyncWrite({cb::durability::Level::Majority,
                               cb::durability::Timeout::Infinity()});

    auto vb = engine->getVBucket(vbid);

    // Drive a replica just like DCP does
    // Send two snapshots, 1 prepare and 1 commit

    // snap 1
    vb->checkpointManager->createSnapshot(
            1, 1, {} /*HCS*/, CheckpointType::Memory, 0);
    ASSERT_EQ(cb::engine_errc::success, store->prepare(*item, cookie));
    flush_vbucket_to_disk(vbid);
    vb->notifyPassiveDMOfSnapEndReceived(1);

    // snap 2
    vb->checkpointManager->createSnapshot(
            2, 2, {} /*HCS*/, CheckpointType::Memory, 0);
    EXPECT_EQ(cb::engine_errc::success,
              vb->commit(key, 1, 2, vb->lockCollections(key)));
    flush_vbucket_to_disk(vbid, 1);
    vb->notifyPassiveDMOfSnapEndReceived(2);

    vb.reset();

    // Warmup and allow the pre/post checker to test the state
    resetEngineAndWarmup();
}

/**
 * Test that we do not move the PCS when we receive an abort as part of a disk
 * snapshot. If we were to do so then we would prevent the warmup of any
 * prepares beforehand. This is okay in the general case as, if we went down, we
 * would resume our disk snapshot and eventually receive the completions. If we
 * were to be promoted in this state, whilst we would have lost data already,
 * we could warmup the prepares and eventually recommit them which means that we
 * be consistent up to the point of the Abort.
 */
TEST_P(DurabilityWarmupTest, AbortDoesNotMovePCSInDiskSnapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    { // Scope for VBPtr (invalid after reset and warmup)
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // We're going to pretend to be a replica because it's easier than
        // setting up a consumer. We want to be receiving a partial disk
        // snapshot from seqno 1 to 5 where seqno 1 is a prepare, seqno 2
        // unrelated, and seqno 4 is an abort for a different prepare.
        // HCS is not flushed as the snapshot is incomplete, even though we
        // flush the abort.
        vb->checkpointManager->createSnapshot(1 /*snapStart*/,
                                              5 /*snapEnd*/,
                                              3 /*HCS*/,
                                              CheckpointType::Disk,
                                              0);
        ASSERT_TRUE(vb->isReceivingDiskSnapshot());

        // 1) Receive the prepare
        auto prepareKey = makeStoredDocKey("prepare");
        using namespace cb::durability;
        auto prepare = makePendingItem(
                prepareKey,
                "value",
                Requirements{Level::Majority, Timeout::Infinity()});
        prepare->setVBucketId(vbid);
        prepare->setCas(1);
        prepare->setBySeqno(1);
        EXPECT_EQ(cb::engine_errc::success, store->prepare(*prepare, cookie));

        // 2) Receive the abort
        auto abortKey = makeStoredDocKey("abort");
        vb->abort(abortKey, 3, 4, vb->lockCollections(abortKey));
        EXPECT_EQ(4, vb->getHighSeqno());

        // 3) Flush and shutdown to simulate a partial snapshot.
        flushVBucketToDiskIfPersistent(vbid, 2);
        EXPECT_EQ(0,
                  store->getRWUnderlying(vbid)
                          ->getCachedVBucketState(vbid)
                          ->persistedCompletedSeqno);
    }

    resetEngineAndWarmup();

    EXPECT_EQ(0,
              store->getRWUnderlying(vbid)
                      ->getCachedVBucketState(vbid)
                      ->persistedCompletedSeqno);

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // We should have loaded the prepare at seqno 1.
    EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);

    // We visit 2 items as our snapshot is incomplete
    EXPECT_EQ(2,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);
}

// MB-35768: Test that having a replication topology stored on disk under
// which durability is impossible (e.g. [active, <null>], and a Pending
// // SyncWrite doesn't crash during warmup (when re-populating ActiveDM).
TEST_P(DurabilityWarmupTest, ImpossibleTopology) {
    // Store a prepared SyncWrite.
    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "value");
    ASSERT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);

    // Change topology to one with a missing replica (e.g. simulating a node
    // failed over).
    // Remove one node from the topology replicationTopology and re-persist.
    auto topology = nlohmann::json::array({{"active", nullptr}});
    setVBucketStateAndRunPersistTask(
            vbid, vbucket_state_active, {{"topology", topology}});

    // Test: This previously triggered a gsl::fail_fast exception due to an
    // Expects() failure.
    ASSERT_NO_THROW(resetEngineAndWarmup());
    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);

    // Sanity check - "impossible" topology was loaded.
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    EXPECT_EQ(topology.dump(), vb->getReplicationTopology().dump());
}

/**
 * Test that we do warm up from the PCS to the high seqno when we have an
 * incomplete disk snapshot to ensure that we warm up completions for prepares
 * that we cannot track due to de-dupe. In this case we will have a simple
 * disk snapshot with 1 prepare, 1 commit for the prepare, followed by 1
 * unrelated mutation that will not be received.
 */
TEST_P(DurabilityWarmupTest, IncompleteDiskSnapshotWarmsUpToHighSeqno) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    { // Scope for VBPtr (invalid after reset and warmup)
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // We're going to pretend to be a replica because it's easier than
        // setting up a consumer. We want to be receiving a partial disk
        // snapshot from seqno 1 to 3 where seqno 1 is a prepare, seqno 2 the
        // commit for the prepare, and seqno 3 is an unrelated mutation. HCS is
        // not flushed as the snapshot is incomplete.
        vb->checkpointManager->createSnapshot(1 /*snapStart*/,
                                              3 /*snapEnd*/,
                                              1 /*HCS*/,
                                              CheckpointType::Disk,
                                              3);
        ASSERT_TRUE(vb->isReceivingDiskSnapshot());

        // 1) Receive the prepare
        auto prepareKey = makeStoredDocKey("prepare");
        using namespace cb::durability;
        auto prepare = makePendingItem(
                prepareKey,
                "value",
                Requirements{Level::Majority, Timeout::Infinity()});
        prepare->setVBucketId(vbid);
        prepare->setCas(1);
        prepare->setBySeqno(1);
        EXPECT_EQ(cb::engine_errc::success, store->prepare(*prepare, cookie));

        EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

        // 2) Receive the commit (as we are in a disk snapshot this is actually
        // sent by the active as a mutation which hits setWithMeta on a replica)
        auto completion = makeCommittedItem(prepareKey, "value");
        completion->setBySeqno(2);
        completion->setCas(prepare->getCas());
        uint64_t* seqno = nullptr;
        EXPECT_EQ(cb::engine_errc::success,
                  store->setWithMeta(*completion,
                                     0,
                                     seqno,
                                     cookie,
                                     {vbucket_state_replica},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::No,
                                     GenerateCas::No,
                                     nullptr));
        EXPECT_EQ(2, vb->getHighSeqno());
        // Commit is visible
        EXPECT_EQ(2, vb->checkpointManager->getMaxVisibleSeqno());

        // 3) Flush and shutdown to simulate a partial snapshot.
        flushVBucketToDiskIfPersistent(vbid, 2);
    }

    // HCS will go backwards (to 0) so we can't make the normal checks here;
    // this is because we flush it on snapshot end in the flusher and we have
    // not received the snapshot end. This is expected.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // We should not have loaded the prepare at seqno 1.
    EXPECT_EQ(0, vb->getDurabilityMonitor().getNumTracked());
    EXPECT_EQ(0, vb->getDurabilityMonitor().getHighPreparedSeqno());
    EXPECT_EQ(0, vb->getDurabilityMonitor().getHighCompletedSeqno());

    // Check our warmup stats, should have visited both the prepare and the
    // logical commit
    EXPECT_EQ(0, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(2,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);
}

/**
 * Test that we use our normal warmup optimization when we have just persisted a
 * complete disk snapshot. To test this we want to ensure that the PCS is not
 * equal to the PPS.
 */
TEST_P(DurabilityWarmupTest, CompleteDiskSnapshotWarmsUpPCStoPPS) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    { // Scope for VBPtr (invalid after reset and warmup)
        auto vb = engine->getKVBucket()->getVBucket(vbid);

        // We'll say that our HCS is 2 by flushing it as part of a Disk snapshot
        // and write a prepare at seqno 3 and an unrelated mutation at seqno 4
        // to complete the snapshot.
        vb->checkpointManager->createSnapshot(1 /*snapStart*/,
                                              4 /*snapEnd*/,
                                              2 /*HCS*/,
                                              CheckpointType::Disk,
                                              4);
        ASSERT_TRUE(vb->isReceivingDiskSnapshot());

        // 1) Receive the prepare
        auto prepareKey = makeStoredDocKey("prepare");
        using namespace cb::durability;
        auto prepare = makePendingItem(
                prepareKey,
                "value",
                Requirements{Level::Majority, Timeout::Infinity()});
        prepare->setVBucketId(vbid);
        prepare->setCas(3);
        prepare->setBySeqno(3);
        EXPECT_EQ(cb::engine_errc::success, store->prepare(*prepare, cookie));

        EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());

        // 2) Unrelated mutation at seqno 4
        auto mutationKey = makeStoredDocKey("unrelated");
        auto mutation = makeCommittedItem(mutationKey, "value");
        mutation->setBySeqno(4);
        mutation->setCas(4);
        uint64_t* seqno = nullptr;
        EXPECT_EQ(cb::engine_errc::success,
                  store->setWithMeta(*mutation,
                                     0,
                                     seqno,
                                     cookie,
                                     {vbucket_state_replica},
                                     CheckConflicts::No,
                                     true,
                                     GenerateBySeqno::No,
                                     GenerateCas::No,
                                     nullptr));
        EXPECT_EQ(4, vb->getHighSeqno());
        // Commit is visible
        EXPECT_EQ(4, vb->checkpointManager->getMaxVisibleSeqno());
        // Notify snap end to correct the HPS
        vb->notifyPassiveDMOfSnapEndReceived(4);

        // 3) Flush and shutdown to simulate a partial snapshot. We should only
        // flush the Disk checkpoints
        flushVBucketToDiskIfPersistent(vbid, 2);
        EXPECT_EQ(CheckpointType::Disk,
                  store->getRWUnderlying(vbid)
                          ->getCachedVBucketState(vbid)
                          ->checkpointType);
    }

    // HCS will be different post warmup as we don't have a precise value during
    // disk snapshots on replica (it will be 0 pre-reset because we have not
    // seen the completion for the prepare).
    // @TODO We could correct this by passing the HCS into the PDM when we pass
    // it into the CheckpointManager.
    resetEngineAndWarmup().disable();

    auto vb = engine->getKVBucket()->getVBucket(vbid);
    // We should have loaded only the final prepare but we should not have
    // scanned the entire snapshot
    EXPECT_EQ(1, vb->getDurabilityMonitor().getNumTracked());
    EXPECT_EQ(2, vb->getDurabilityMonitor().getHighCompletedSeqno());
    EXPECT_EQ(4, vb->getDurabilityMonitor().getHighPreparedSeqno());
    const auto& pdm = static_cast<const PassiveDurabilityMonitor&>(
            vb->getDurabilityMonitor());
    EXPECT_EQ(3, pdm.getHighestTrackedSeqno());

    EXPECT_EQ(1, store->getEPEngine().getEpStats().warmedUpPrepares);
    EXPECT_EQ(1,
              store->getEPEngine()
                      .getEpStats()
                      .warmupItemsVisitedWhilstLoadingPrepares);
}

/**
 * Test that the on disk HPS is set to the snap_end of a disk snapshot once
 * fully persisted
 */
TEST_P(DurabilityWarmupTest, CompleteDiskSnapshotSetsOnDiskHPSCorrectly) {
    testFullyPersistedSnapshotSetsHPS(CheckpointType::Disk);
}

/**
 * Test that the on disk HPS is set to the highest prepare seqno of a memory
 * snapshot once fully persisted
 */
TEST_P(DurabilityWarmupTest, CompleteMemorySnapshotSetsOnDiskHPSCorrectly) {
    testFullyPersistedSnapshotSetsHPS(CheckpointType::Memory);
}

/**
 * Test that the on disk HPS is set to the snap_end of a disk snapshot once
 * fully persisted
 */
TEST_P(DurabilityWarmupTest, IncompleteDiskSnapshotDoesNotSetOnDiskHPS) {
    testPartiallyPersistedSnapshotDoesNotSetHPS(CheckpointType::Disk);
}

/**
 * Test that the on disk HPS is set to the highest prepare seqno of a memory
 * snapshot once fully persisted
 */
TEST_P(DurabilityWarmupTest, IncompleteMemorySnapshotDoesNotSetOnDiskHPS) {
    testPartiallyPersistedSnapshotDoesNotSetHPS(CheckpointType::Memory);
}

TEST_P(DurabilityWarmupTest, promotedReplicaPartialSnapshotHPS_Memory) {
    testPromotedReplicaMidSnapshotHPS(CheckpointType::Memory);
}

TEST_P(DurabilityWarmupTest, promotedReplicaPartialSnapshotHPS_Disk) {
    testPromotedReplicaMidSnapshotHPS(CheckpointType::Disk);
}

TEST_P(DurabilityWarmupTest, promotedReplicaCompleteSnapshotHPS_Memory) {
    testPromotedReplicaCompleteSnapshotHPS(CheckpointType::Memory);
}

TEST_P(DurabilityWarmupTest, promotedReplicaCompleteSnapshotHPS_Disk) {
    testPromotedReplicaCompleteSnapshotHPS(CheckpointType::Disk);
}

INSTANTIATE_TEST_SUITE_P(CouchstoreFullOrValue,
                         DurabilityWarmupTest,
                         STParameterizedBucketTest::couchstoreConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

#ifdef EP_USE_MAGMA
INSTANTIATE_TEST_SUITE_P(MagmaFullOrValue,
                         DurabilityWarmupTest,
                         STParameterizedBucketTest::magmaConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        NexusFullOrValue,
        DurabilityWarmupTest,
        STParameterizedBucketTest::nexusCouchstoreMagmaConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
#endif

#ifdef EP_USE_ROCKSDB
INSTANTIATE_TEST_SUITE_P(RocksFullOrValue,
                         DurabilityWarmupTest,
                         STParameterizedBucketTest::rocksDbConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
#endif

class MB_34718_WarmupTest : public STParameterizedBucketTest {};

// In MB-34718 a GET arrived during warmup on a full-eviction bucket. The GET
// was processed and found an item which had expired. The expiry path queued
// a delete which was flushed. In the document count callbacks, we processed
// the delete and subtracted 1 from the collection count. All of this happened
// before warmup had read the collection counts from disk, so the counter goes
// negative and throws an exception. The test performs those steps seen in the
// MB and demonstrates how changes in warmup prevent this situation, the VB
// is not visible until it is fully initialised by warmup.
TEST_P(MB_34718_WarmupTest, getTest) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active, {});

    // Store a key and trigger a warmup
    auto key = makeStoredDocKey("key");
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    store_item(vbid, key, "meh", ep_real_time() + 3600);
    flush_vbucket_to_disk(vbid);
    resetEngineAndEnableWarmup();

    // Now run the reader tasks to the stage of interest, we will run the test
    // once we have ran the new warmup stage which puts the fully initialised VB
    // into the vbMap, before warmup has reached that stage we expect the GET to
    // faile with NMVB.
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    bool keepRunningReaderTasks = true;
    while (keepRunningReaderTasks) {
        runNextTask(readerQueue);
        CheckedExecutor executor(task_executor, readerQueue);
        keepRunningReaderTasks = executor.getCurrentTask()->getTaskId() !=
                                 TaskId::WarmupPopulateVBucketMap;

        auto gv = store->get(key, vbid, cookie, options);
        ASSERT_EQ(cb::engine_errc::not_my_vbucket, gv.getStatus());

        executor.runCurrentTask();
        executor.completeCurrentTask();
    }

    auto vb = engine->getVBucket(vbid);
    ASSERT_TRUE(vb);
    EXPECT_EQ(1, vb->lockCollections().getItemCount(CollectionID::Default));

    // - Full eviction a get is allowed and it can expire documents during the
    //   final stages of warmup.
    // - Value eviction will fail until all K/V are loaded and warmup completes
    if (fullEviction()) {
        // FE can read the item count before loading items
        EXPECT_EQ(1, vb->getNumItems());
        TimeTraveller rick(4800);
        auto gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::would_block, gv.getStatus());
        runBGFetcherTask();

        // Expect expired (key_noent)
        gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

        // Prior to the MB being resolved, this would trigger a negative counter
        // exception as we tried to decrement the collection counter which is
        // 0 because warmup hadn't loaded the counts
        flush_vbucket_to_disk(vbid);

        // Finish warmup so we don't hang TearDown
        runReadersUntilWarmedUp();
    } else {
        // Value eviction, expect no key whilst warming up
        auto gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

        runReadersUntilWarmedUp();

        // VE: Can only read the item count once items are loaded
        EXPECT_EQ(1, vb->getNumItems());

        gv = store->get(key, vbid, cookie, options);

        EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
        TimeTraveller morty(4800);

        // And expired
        gv = store->get(key, vbid, cookie, options);
        EXPECT_EQ(cb::engine_errc::no_such_key, gv.getStatus());

        flush_vbucket_to_disk(vbid);
    }
    EXPECT_EQ(0, vb->lockCollections().getItemCount(CollectionID::Default));
    EXPECT_EQ(0, vb->getNumItems());
}

// Perform the sequence of operations which lead to MB-35326, a snapshot range
// exception. When the issue is fixed this test will pass
TEST_F(WarmupTest, MB_35326) {
    // 1) Write an item to an active vbucket and flush it.
    //    vb state on disk will have a range of {1,1}
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);

    // 2) Mark the vbucket as dead and persist the state
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_dead);

    // 3) Warmup - the dead vbucket will be skipped by warmup but KVStore has
    //    loaded the state into cachedVBStates
    resetEngineAndWarmup();

    // 4) Now active creation, this results in a new VBucket object with default
    //    state, for this issue the snapshot range of {0,0}
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // 5) Store an item and flush, this would crash because we combine the in
    //    memory range {0,0} with the on disk range {1,1}, the crash occurs
    //    as the new range is {1, 0} and start:1 cannot be greater than end:0.
    store_item(vbid, key, "value");

    flush_vbucket_to_disk(vbid);
}

class WarmupDiskTest : public SingleThreadedKVBucketTest {
public:
    boost::filesystem::path getDataDir() {
        auto* kvstore = engine->getKVBucket()->getOneRWUnderlying();
        const auto dbname = kvstore->getConfig().getDBName();
        return boost::filesystem::current_path() / dbname;
    }

    boost::filesystem::path getDataFile() {
        const boost::filesystem::path dir(getDataDir());
        for (const auto& file :
             boost::filesystem::recursive_directory_iterator(dir)) {
            if (file.path().has_filename() &&
                file.path().filename().string().find("stats.json") !=
                        std::string::npos) {
                continue;
            }
            return file.path();
        }
        throw std::runtime_error("failed to find any data files");
    };

    void deleteDataFile() {
        auto dataFile = getDataFile();
        EXPECT_TRUE(boost::filesystem::remove(dataFile));
        ASSERT_FALSE(boost::filesystem::is_regular_file(dataFile));
    };

    void makeFileReadOnly() {
        auto dataFile = getDataFile();
        boost::filesystem::permissions(
                dataFile,
                boost::filesystem::others_read | boost::filesystem::owner_read);
    }

    void deleteStatsFile() {
        auto statsFile = (getDataDir() / "stats.json");
        EXPECT_TRUE(boost::filesystem::remove(statsFile));
        ASSERT_FALSE(boost::filesystem::is_regular_file(statsFile));
    }

    TaskQueue& getReaderQueue() {
        return *task_executor->getLpTaskQ()[READER_TASK_IDX];
    }

    KVBucket* getKVBucket() {
        return engine->getKVBucket();
    }

    void sendEnableTrafficRequest(
            cb::engine_errc expectedStatus = cb::engine_errc::failed) {
        cb::mcbp::Request request;
        request.setMagic(cb::mcbp::Magic::ClientRequest);
        request.setOpcode(cb::mcbp::ClientOpcode::EnableTraffic);
        EXPECT_EQ(expectedStatus,
                  engine->handleTrafficControlCmd(
                          cookie, request, dummyRespHandler));
    }

private:
    AddResponseFn dummyRespHandler = [](std::string_view key,
                                        std::string_view extras,
                                        std::string_view body,
                                        protocol_binary_datatype_t datatype,
                                        cb::mcbp::Status status,
                                        uint64_t cas,
                                        const void* void_cookie) -> bool {
        return true;
    };
};

/**
 * Test to check that if a data file deleted between Warmup::initialize() and
 * Warmup::loadCollectionStatsForShard() that we fail warmup
 */
TEST_F(WarmupDiskTest, noDataFileCollectionCountsTest) {
    // Create a vbucket on disk
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Set up engine so its ready to warm up from disk
    resetEngineAndEnableWarmup();

    bool runDiskFailure = true;
    // run through the stages of warmup
    while (getKVBucket()->isWarmupLoadingData()) {
        // if check this is the state of warmup that we want to fail
        // only run the disruption the for the first task though as we
        // have two shards
        if (runDiskFailure &&
            WarmupState::State::LoadingCollectionCounts ==
                    getKVBucket()->getWarmup()->getWarmupState()) {
            // delete the datafile on disk
            deleteDataFile();
            runDiskFailure = false;
        }
        try {
            CheckedExecutor executor(task_executor, getReaderQueue());
            // run the task
            executor.runCurrentTask();
            executor.completeCurrentTask();
        } catch (const std::logic_error& e) {
            // CheckedExecutor should fail after LoadingCollectionCounts failed
            // to open data file
            EXPECT_FALSE(runDiskFailure);
            EXPECT_EQ("CheckedExecutor failed fetchNextTask",
                      std::string(e.what()));
            break;
        }
    }
    EXPECT_EQ(WarmupState::State::LoadingCollectionCounts,
              getKVBucket()->getWarmup()->getWarmupState());
}

/**
 * Test to check that if a data file deleted between Warmup::initialize() and
 * Warmup::loadPreparedSyncWrites() that we fail warmup
 */
TEST_F(WarmupDiskTest, diskFailureBeforeLoadPrepares) {
    // Create a vbucket on disk and a prepare
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    store_item(vbid,
               makeStoredDocKey("key"),
               "value",
               0,
               {cb::engine_errc::sync_write_pending},
               PROTOCOL_BINARY_RAW_BYTES,
               cb::durability::Requirements{});
    flush_vbucket_to_disk(vbid);
    // Set up engine so its ready to warm up from disk
    resetEngineAndEnableWarmup();

    bool runDiskFailure = true;
    // run through the stages of warmup
    while (getKVBucket()->isWarmupLoadingData()) {
        // if check this is the state of warmup that we want to fail
        // only run the disruption the for the first task though as we
        // have two shards
        if (runDiskFailure &&
            WarmupState::State::LoadPreparedSyncWrites ==
            getKVBucket()->getWarmup()->getWarmupState()) {
            // delete the datafile on disk
            deleteDataFile();
            runDiskFailure = false;
        }
        try {
            CheckedExecutor executor(task_executor, getReaderQueue());
            // run the task
            executor.runCurrentTask();
            executor.completeCurrentTask();
        } catch (const std::logic_error& e) {
            // CheckedExecutor should fail after LoadingCollectionCounts failed
            // to open data file
            EXPECT_FALSE(runDiskFailure);
            EXPECT_EQ("CheckedExecutor failed fetchNextTask",
                      std::string(e.what()));
            break;
        }
    }
    EXPECT_EQ(WarmupState::State::LoadPreparedSyncWrites,
              getKVBucket()->getWarmup()->getWarmupState());
}

/**
 * Test to check that if a data has been made read only Warmup::initialize() and
 * Warmup::populateVBucketMap() we don't enable traffic.
 */
TEST_F(WarmupDiskTest, readOnlyDataFileSetVbucketStateTest) {
    // Create a vbucket on disk and add a key to read later
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid, makeStoredDocKey("key"), "value");
    flush_vbucket_to_disk(vbid);
    // Set up engine so its ready to warm up from disk
    resetEngineAndEnableWarmup();
    // make sure create a new entry in the failover table so that we need
    // to persist new vbucket state.
    deleteStatsFile();

    bool runDiskFailure = true;
    // run through the stages of warmup
    while (getKVBucket()->isWarmupLoadingData()) {
        // if check this is the state of warmup that we want to fail
        // only run the disruption the for the first task though as we
        // have two shards
        if (runDiskFailure &&
            WarmupState::State::PopulateVBucketMap ==
                    getKVBucket()->getWarmup()->getWarmupState()) {
            // delete the datafile on disk
            makeFileReadOnly();
            runDiskFailure = false;
        }
        CheckedExecutor executor(task_executor, getReaderQueue());
        // run the task
        executor.runCurrentTask();
        executor.completeCurrentTask();
    }
    // Check that we finished warmuo
    EXPECT_EQ(WarmupState::State::Done,
              getKVBucket()->getWarmup()->getWarmupState());
    EXPECT_FALSE(getKVBucket()->isWarmupLoadingData());
    // Ensure we don't enable traffic
    EXPECT_TRUE(getKVBucket()->hasWarmupSetVbucketStateFailed());
    sendEnableTrafficRequest();
    // Check that we can read the file we had written before warmup and the file
    // becoming read only
    auto [status, item] = engine->get(
            *cookie, makeStoredDocKey("key"), vbid, DocStateFilter::Alive);
    EXPECT_EQ(cb::engine_errc::success, status);
    ASSERT_TRUE(item);
    EXPECT_EQ("value", item->getValueView());
}

TEST_F(WarmupTest, DontStartFlushersUntilPopulateVBucketMap) {
    // Create a vbucket on disk and add a key to read later
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    store_item(vbid, makeStoredDocKey("key"), "value");
    flush_vbucket_to_disk(vbid);

    // Reset without starting the warmup task
    resetEngine();

    // Normal setup would call KVBucket::intialize() to create warmup task and
    // before this
    store->initialize();

    // Flusher will be in initializing and we won't have scheduled a task
    auto bucket = static_cast<EPBucket*>(store);
    auto* flusher = bucket->getFlusher(vbid);
    EXPECT_EQ("initializing", std::string(flusher->stateName()));

    auto& writerQueue = *task_executor->getLpTaskQ()[WRITER_TASK_IDX];
    EXPECT_EQ(0, writerQueue.getFutureQueueSize());

    // Warmup - run past the PopulateVBucketMap step which is the one that
    // we care about
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    auto* warmup = engine->getKVBucket()->getWarmup();
    ASSERT_TRUE(warmup);

    while (warmup->getWarmupState() != WarmupState::State::CheckForAccessLog) {
        runNextTask(readerQueue);
    }

    // 2 shards so 2 flushers/tasks
    EXPECT_EQ(2, writerQueue.getFutureQueueSize());
    EXPECT_EQ("initializing", std::string(flusher->stateName()));

    // Run them and the flushers should be good to go
    runNextTask(writerQueue);
    runNextTask(writerQueue);

    EXPECT_EQ("running", std::string(flusher->stateName()));

    // Finish warmup or the test gets stuck
    runReadersUntilWarmedUp();
}

TEST_F(WarmupTest, OnlyShutdownPersistsForceShutdownStat) {
    // Hook to run
    std::function<void()> stopDestructionEarly = [this]() {
        // Run StatSnap manually
        auto statSnap = StatSnap(engine.get());
        statSnap.run();

        // throw so we exit the destroy method early
        throw std::runtime_error("Stop destruction");
    };
    engine->epDestroyFailureHook = stopDestructionEarly;

    // Gotta create our vb
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    //  Check that our first failover entry is 0
    const auto initFailoverEntry = *getLatestFailoverTableEntry();
    ASSERT_EQ(0, initFailoverEntry.by_seqno);

    // Start an unclean shutdown
    engine->epDestroyFailureHook = stopDestructionEarly;
    try {
        engine->destroy(false);
        FAIL() << "We should have thrown so we didn't run destroy() in "
                  "full";
    } catch (const std::runtime_error& e) {
        // catch the exception thrown by stopDestructionEarly() so we clean
        // up the thread nicely and check it was the exception we where
        // expecting
        EXPECT_EQ(std::string_view{"Stop destruction"}, e.what());
    }

    // Tidy up for warmup
    engine->epDestroyFailureHook = []() -> void {};
    tidyUpAbortedShutdown();
    initialiseAndWarmupFully();

    // If StatSnap had run and persisted "ep_force_shutdown"="false" during
    // the shutdown then the failover table entries here would be the same
    const auto failoverEntryAfterKill = *getLatestFailoverTableEntry();
    EXPECT_NE(initFailoverEntry, failoverEntryAfterKill);
}

TEST_F(WarmupTest, MB_45756_CrashDuringEPDestruction) {
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);
    std::function<void()> stopDestructionEarly = []() {
        // throw so we exit the destroy method early
        throw std::runtime_error("Stop destruction");
    };

    // 1. Create a vbucket on disk that we can warm up from.
    //    also place a doc on disk so the seqno is non zero
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    ASSERT_EQ(0, engine->getVBucket(vbid)->getHighSeqno());
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "value");
    flush_vbucket_to_disk(vbid);
    ASSERT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());

    // 2. Check that our first failover entry is 0
    const auto initFailoverEntry = *getLatestFailoverTableEntry();
    ASSERT_EQ(0, initFailoverEntry.by_seqno);

    // 3. Shutdown cleanly and re-warmup. Check that the failover entry hasn't
    //    changed and that our high seqno is 1 so we've warmed up with the doc
    //    and that we can get it.
    resetEngineAndWarmup("", false);
    const auto failoverEntryCleanShutdown = *getLatestFailoverTableEntry();
    EXPECT_EQ(initFailoverEntry, failoverEntryCleanShutdown);
    EXPECT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());
    auto gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    // 4. Perform a forced shutdown and re-warmup. Check that the failover entry
    //    has changed and that our high seqno is 1 so we've warmed up with the
    //    doc and that we can get it.
    resetEngineAndWarmup("", true);
    const auto failoverEntryForcedShutdown = *getLatestFailoverTableEntry();
    EXPECT_NE(initFailoverEntry, failoverEntryForcedShutdown);
    EXPECT_EQ(1, failoverEntryForcedShutdown.by_seqno);
    EXPECT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());
    gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());

    // 5. This stage is to "simulate" ep-engine being SIGKILLed during shutdown
    //    Set the hook to ensure that destroy() will throw half way though
    //    we should have not shutdown cleanly. Then manually clean up the mess
    //    the throw has caused.
    engine->epDestroyFailureHook = stopDestructionEarly;
    try {
        engine->destroy(false);
        FAIL() << "We should have thrown so we didn't run destroy() in "
                  "full";
    } catch (const std::runtime_error& e) {
        // catch the exception thrown by stopDestructionEarly() so we clean
        // up the thread nicely and check it was the exception we where
        // expecting
        EXPECT_EQ(std::string_view{"Stop destruction"}, e.what());
    }
    // clear the early exit hook
    engine->epDestroyFailureHook = []() -> void {};
    tidyUpAbortedShutdown();

    // 6. Init the engine and warm backup. We shouldn't have written to
    //    stats.json that we shutdown cleanly so we should generate a new
    //    failover table uuid, but the entry should be still at seqno 1.
    //    Also check that we warmed up correctly but fetching the doc that
    //    should still be on disk
    initialiseAndWarmupFully();
    const auto failoverEntryAfterKill = *getLatestFailoverTableEntry();
    EXPECT_NE(initFailoverEntry, failoverEntryAfterKill);
    EXPECT_NE(failoverEntryCleanShutdown, failoverEntryAfterKill);
    EXPECT_NE(failoverEntryForcedShutdown, failoverEntryAfterKill);
    EXPECT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());
    EXPECT_EQ(1, failoverEntryForcedShutdown.by_seqno);
    gv = store->get(key, vbid, cookie, options);
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
}

TEST_F(WarmupTest, CrashWarmupAfterInitialize) {
    // Create a vbucket on disk and add a key to read later
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    // Write a doc to disk so we can see the failover entry seqno change
    store_item(vbid, makeStoredDocKey("key"), "value");
    flush_vbucket_to_disk(vbid);
    auto initFailoverEntry = *getLatestFailoverTableEntry();
    ASSERT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());
    ASSERT_EQ(0, initFailoverEntry.by_seqno);

    {
        // Set up engine so its ready to warm up from disk
        resetEngineAndEnableWarmup();
        auto kvBucket = engine->getKVBucket();
        auto warmup = kvBucket->getWarmup();
        // 1. Warmup stopping just before WarmupState::State::CreateVBuckets
        //    We should set 'ep_force_shutdown=true' in stats.log during
        //    WarmupState::State::initialize.
        do {
            CheckedExecutor executor(
                    task_executor,
                    *task_executor->getLpTaskQ()[READER_TASK_IDX]);
            // run the task
            executor.runCurrentTask();
            executor.completeCurrentTask();
        } while (kvBucket->isWarmupLoadingData() &&
                 WarmupState::State::CreateVBuckets !=
                         warmup->getWarmupState());
        ASSERT_EQ(WarmupState::State::CreateVBuckets, warmup->getWarmupState());
        // 2. Tear down the Engine without calling destroyInner() to simulate
        //    the process being SIGKILLed during warmup
        tidyUpAbortedShutdown();
    }

    // 3. Init the Engine and warmup full, we should notice that
    //    'ep_force_shutdown' is set to 'true' in stats.log and generate a new
    //    failover table entry.
    initialiseAndWarmupFully();
    // 4. Verify that we did indeed generate a new failover table entry
    auto failoverEntry = *getLatestFailoverTableEntry();
    EXPECT_NE(initFailoverEntry, failoverEntry);
    EXPECT_EQ(1, engine->getVBucket(vbid)->getHighSeqno());
    EXPECT_EQ(1, failoverEntry.by_seqno);
}

// MB-48373
// This originally manifested as a scan error during the (warmup) backill of a
// full eviction bucket. The scan error cropped up because a DCP consumer came
// along during the warmup and the connected active told a replica vBucket to
// rollback. This rollback was to 0 and as such the vBucket was deleted and
// recreated. The file then didn't exist on disk when the (warmup) backfill
// came to scan this vBucket. The fix is/was to just prevent the connection
// of consumers until we complete all of the warmup tasks. We do this by
// checking that warmup has entired the Done state before allowing consumer
// connections.
TEST_F(WarmupTest, ConsumerDuringWarmup) {
    // Write to two vBuckets on two different shards (we have 2 by default) so
    // that we can have two tasks that load data which allows us to check
    // at the same point in MB-48373 that a DCP connection can't be created
    setVBucketToActiveWithValidTopology();
    store_item(vbid, makeStoredDocKey("key"), "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    setVBucketStateAndRunPersistTask(Vbid(1), vbucket_state_active);
    store_item(Vbid(1), makeStoredDocKey("key1"), "value");
    flushVBucketToDiskIfPersistent(Vbid(1), 1);

    resetEngineAndEnableWarmup();
    auto* warmupPtr = store->getWarmup();
    auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
    while (store->isWarmupLoadingData()) {
        if (warmupPtr->getWarmupState() == WarmupState::State::LoadingData) {
            break;
        }
        runNextTask(readerQueue);
    }

    // We've run until LoadingData (for the first shard)
    EXPECT_EQ(warmupPtr->getWarmupState(), WarmupState::State::LoadingData);

    // Consumer should fail - not finished warmup
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              engine->dcpOpen(cookie,
                              /*opaque:unused*/ {},
                              /*seqno:unused*/ {},
                              0 /*flags - consumer*/,
                              "consumer",
                              {}));

    // Run for next shard, this is where the data loading was originally
    // completed in MB-48373
    runNextTask(readerQueue);
    EXPECT_EQ(warmupPtr->getWarmupState(), WarmupState::State::LoadingData);

    // ns_server gets stats to determine when to set up DcpConsumers. The stat
    // ep_warmup_thread encapsulates the state of warmup threads and returns a
    // value of either "running" or "complete". Check here that it is "running"
    // as we will temp_fail a DcpConsumer creation.
    {
        bool threadStatAdded;
        auto dummyAddStats = [&threadStatAdded](std::string_view key,
                                                std::string_view value,
                                                const void*) {
            if (key == "ep_warmup_thread") {
                EXPECT_EQ("running", value);
                threadStatAdded = true;
            }
        };
        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_stats(*cookie, "warmup", {}, dummyAddStats));
        EXPECT_TRUE(threadStatAdded);
    }

    // Still fails, not finished warmup
    EXPECT_EQ(cb::engine_errc::temporary_failure,
              engine->dcpOpen(cookie,
                              /*opaque:unused*/ {},
                              /*seqno:unused*/ {},
                              0 /*flags - consumer*/,
                              "consumer",
                              {}));

    // Move to Done now
    runNextTask(readerQueue);
    EXPECT_EQ(warmupPtr->getWarmupState(), WarmupState::State::Done);

    // Have to run again in the Done state to mark things as complete and stop
    // finish warmup
    runNextTask(readerQueue);

    // Now that all warmup threads have complete, ep_warmup_thread should return
    // "complete" indicating to ns_server that they can now create a DcpConsumer
    {
        bool threadStatAdded;
        auto dummyAddStats = [&threadStatAdded](std::string_view key,
                                                std::string_view value,
                                                const void*) {
            if (key == "ep_warmup_thread") {
                EXPECT_EQ("complete", value);
                threadStatAdded = true;
            }
        };
        EXPECT_EQ(cb::engine_errc::success,
                  engine->get_stats(*cookie, "warmup", {}, dummyAddStats));
        EXPECT_TRUE(threadStatAdded);
    }

    // Opening the connection should now work
    EXPECT_EQ(cb::engine_errc::success,
              engine->dcpOpen(cookie,
                              /*opaque:unused*/ {},
                              /*seqno:unused*/ {},
                              0 /*flags - consumer*/,
                              "consumer",
                              {}));
}

INSTANTIATE_TEST_SUITE_P(FullOrValue,
                         MB_34718_WarmupTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

/**
 * Tests which verify that warmup is aborted if any disk failures occur.
 */
class WarmupAbortedOnDiskError : public WarmupTest {
public:
    enum class InjectErrorFunc { InitScanContext, Scan };

protected:
    // Verify that disk errors encountered during warmup are correctly detected
    // and propagated and trigger warmup to be aborted, for errors occuring in
    // initScanContext or scan() for the given warmup state.
    void testWarmupAbortedOnDiskError(WarmupState::State injectState,
                                      InjectErrorFunc injectFunc,
                                      std::string extraConfig = "") {
        // Setup - store one committed item so we have data to warmup and one
        // prepare for testing warmup of prepared sync writes.
        setVBucketToActiveWithValidTopology();
        store_item(vbid, makeStoredDocKey("key1"), "value");
        auto item = makePendingItem(makeStoredDocKey("key2"), "value2");
        uint64_t cas;
        ASSERT_EQ(cb::engine_errc::would_block,
                  engine->storeInner(
                          cookie, *item, cas, StoreSemantics::Set, false));
        flush_vbucket_to_disk(vbid, 2);

        // Setup: Restart engine and begin warmup. This is a single-threaded
        // test so we will run each phase in this main thread; using testing
        // hooks to inject disk errors as required:
        // 1. Advance to phase keyDumpforShard
        // 2. Replace kvstore with a MockStore, and configure to make
        //    initScanContext fail
        resetEngineAndEnableWarmup(extraConfig);

        store->getWarmup()->stateTransitionHook = [this,
                                                   injectState,
                                                   injectFunc](
                                                          const auto& state) {
            using namespace ::testing;
            if (state == injectState) {
                // Need to replace RW kvstore
                auto& mockKVStore =
                        MockKVStore::replaceRWKVStoreWithMock(*store, 0);
                switch (injectFunc) {
                case InjectErrorFunc::InitScanContext:
                    // Configure the mock to return an empty scan context.
                    EXPECT_CALL(mockKVStore,
                                initBySeqnoScanContext(_, _, _, _, _, _, _, _))
                            .WillOnce(Return(ByMove(
                                    std::unique_ptr<BySeqnoScanContext>())));
                    break;
                case InjectErrorFunc::Scan:
                    // First expect an initBySeqnoScanContext call, but that can
                    // perform default action in MockKVStore (succeed).
                    EXPECT_CALL(mockKVStore,
                                initBySeqnoScanContext(_, _, _, _, _, _, _, _))
                            .Times(1);
                    EXPECT_CALL(mockKVStore, scan(An<BySeqnoScanContext&>()))
                            .WillOnce(Return(scan_failed));
                    break;
                }

                if (injectState == WarmupState::State::LoadPreparedSyncWrites) {
                    // This also calls getCachedVBucketState() - which we
                    // can simply rely on default action in MockKVStore to
                    // forward to real.
                    EXPECT_CALL(mockKVStore, getCachedVBucketState(_)).Times(1);
                }
            }
        };

        // Test: Advance warmup until no more tasks remain.
        auto& readerQueue = *task_executor->getLpTaskQ()[READER_TASK_IDX];
        CheckedExecutor executor(task_executor, readerQueue);
        do {
            executor.runCurrentTask();
            executor.completeCurrentTask();
            executor.updateCurrentTime();
        } while (readerQueue.fetchNextTask(executor));

        // Verify error was correctly handled - warmup should not have completed
        // (essentially bucket stops and doesn't proceed).
        auto kvBucket = engine->getKVBucket();
        auto warmup = kvBucket->getWarmup();
        EXPECT_TRUE(kvBucket->isWarmupLoadingData());
        EXPECT_EQ(injectState, warmup->getWarmupState());

        // Clean up (ensures that original KVStore is in place for bucket
        // tear-down.
        MockKVStore::restoreOriginalRWKVStore(*store);
    }
};

TEST_F(WarmupAbortedOnDiskError, InitScanContext_LoadPreparedSyncWrites) {
    testWarmupAbortedOnDiskError(WarmupState::State::LoadPreparedSyncWrites,
                                 InjectErrorFunc::InitScanContext);
}

TEST_F(WarmupAbortedOnDiskError, InitScanContext_KeyDump) {
    testWarmupAbortedOnDiskError(WarmupState::State::KeyDump,
                                 InjectErrorFunc::InitScanContext);
}

TEST_F(WarmupAbortedOnDiskError, InitScanContext_LoadingData) {
    testWarmupAbortedOnDiskError(WarmupState::State::LoadingData,
                                 InjectErrorFunc::InitScanContext);
}

TEST_F(WarmupAbortedOnDiskError, InitScanContext_LoadingKVPairs) {
    // Loading KVPairs is only applicable to full eviction
    testWarmupAbortedOnDiskError(WarmupState::State::LoadingKVPairs,
                                 InjectErrorFunc::InitScanContext,
                                 "item_eviction_policy=full_eviction");
}

TEST_F(WarmupAbortedOnDiskError, Scan_LoadPreparedSyncWrites) {
    testWarmupAbortedOnDiskError(WarmupState::State::LoadPreparedSyncWrites,
                                 InjectErrorFunc::Scan);
}

TEST_F(WarmupAbortedOnDiskError, Scan_KeyDump) {
    testWarmupAbortedOnDiskError(WarmupState::State::KeyDump,
                                 InjectErrorFunc::Scan);
}

TEST_F(WarmupAbortedOnDiskError, Scan_LoadingData) {
    testWarmupAbortedOnDiskError(WarmupState::State::LoadingData,
                                 InjectErrorFunc::Scan);
}

TEST_F(WarmupAbortedOnDiskError, Scan_LoadingKVPairs) {
    // Loading KVPairs is only applicable to full eviction
    testWarmupAbortedOnDiskError(WarmupState::State::LoadingKVPairs,
                                 InjectErrorFunc::Scan,
                                 "item_eviction_policy=full_eviction");
}

TEST_F(WarmupTest, WarmupStateRace) {
    // Create a vbucket on disk
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Reset without starting the warmup task
    resetEngine();

    // Normal setup would call KVBucket::intialize() to create warmup task and
    // before this
    store->initialize();

    // Warmup - run past the PopulateVBucketMap step which is the one that
    // we care about
    auto* warmup = engine->getKVBucket()->getWarmup();
    ASSERT_TRUE(warmup);
    warmup->setWarmupStateTransitionHook([&]() { warmup->stop(); });

    // We'll throw here if we race
    runReadersUntilWarmedUp();
}
