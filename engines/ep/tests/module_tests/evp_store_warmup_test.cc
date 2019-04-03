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

#include "../mock/mock_dcp.h"
#include "../mock/mock_dcp_producer.h"
#include "../mock/mock_ep_bucket.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "dcp/response.h"
#include "ep_time.h"
#include "evp_store_single_threaded_test.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"
#include "vbucket_state.h"
#include "warmup.h"

class WarmupTest : public SingleThreadedKVBucketTest {};

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

    std::string valueData(
            "{\"product\": \"car\",\"price\": \"100\"},"
            "{\"product\": \"bus\",\"price\": \"1000\"},"
            "{\"product\": \"Train\",\"price\": \"100000\"}");

    // Store an item, then make the VB appear old ready for warmup
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
                                         {},
                                         TransferVB::No,
                                         cookie));
        executor.runCurrentTask();
    }

    EXPECT_GT(get_number_of_mock_cookie_io_notifications(cookie),
              notifications);
    EXPECT_NE(nullptr, store->getVBuckets().getBucket(vbid));
    EXPECT_EQ(ENGINE_SUCCESS,
              store->setVBucketState(vbid, vbucket_state_active));

    // finish warmup so the test can exit
    while (engine->getKVBucket()->isWarmingUp()) {
        CheckedExecutor executor(task_executor, readerQueue);
        executor.runCurrentTask();
    }
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

        // Now step the producer to transfer the delete/tombstone.
        EXPECT_EQ(ENGINE_SUCCESS, producer->stepWithBorderGuard(producers));
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
 *    ENGINE_TMPFAIL as the vbucket is not warmed up)
 * 4. Send a DCP deletion for the document to the vbucket (this should fail with
 *    ENGINE_DISCONNECT as there shouldn't be a DCP connection open)
 * 5. Try and flush the vbucket, nothing should be flushed as the deletion
 *    should have failed
 * 6. Finish warming up the vbucket
 * 7. repeat steps 3, 4 and 5 which should now return ENGINE_SUCCESS and
 *    the item should be deleted from disk as we are know fully warmed up.
 */
TEST_F(WarmupTest, MB_32577) {
    std::string keyName("key");
    std::string value("value");
    nlohmann::json metaStateChange{};
    uint32_t zeroFlags{0};

    // create an active vbucket
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    // Store an item to the vbucket
    store_item(vbid, makeStoredDocKey(keyName), value);
    // Change the type of vbucket to a replica
    store->setVBucketState(
            vbid, vbucket_state_replica, metaStateChange, TransferVB::Yes);
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
    while (store->isWarmingUp()) {
        if (warmupPtr->getWarmupState() ==
            WarmupState::State::LoadingCollectionCounts) {
            break;
        }
        runNextTask(readerQueue);
    }

    // Try and set a DCP connection, this should return ENGINE_TMPFAIL
    // as were in warm up but without the fix for MB-32577 this will return
    // ENGINE_SUCCESS
    EXPECT_EQ(ENGINE_TMPFAIL,
              engine->open(cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // create a stream to send the delete request so we can send vbucket on the
    // consumer
    EXPECT_EQ(ENGINE_DISCONNECT, engine->add_stream(cookie, 0, vbid, 0));

    // create snapshot so we can delete the document
    EXPECT_EQ(ENGINE_DISCONNECT,
              engine->snapshot_marker(cookie,
                                      /*opaque*/ 1,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 100,
                                      zeroFlags));

    // create a DocKey object for the delete request
    const DocKey docKey{reinterpret_cast<const uint8_t*>(keyName.data()),
                        keyName.size(),
                        DocKeyEncodesCollectionId::No};
    // Try and delete the doc
    EXPECT_EQ(ENGINE_DISCONNECT,
              engine->deletion(cookie,
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
    // Get the engine to flush the delete to disk, this will cause an
    // underflow exception if the deletion request was successful
    EXPECT_NO_THROW(dynamic_cast<EPBucket&>(*store).flushVBucket(vbid));

    // finish warmup so we can check the number of items in the engine
    while (store->isWarmingUp()) {
        runNextTask(readerQueue);
    }

    // We shouldn't have deleted the doc and thus it should still be there
    EXPECT_EQ(1,
              store->getVBucket(vbid)->lockCollections().getItemCount(
                      CollectionID::Default));

    // Try and set a DCP connection, this should return ENGINE_SUCCESS
    // as we have now warmed up.
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->open(cookie, 0, 0 /*seqno*/, zeroFlags, "test_consumer"));

    // create a stream to send the delete request so we can send vbucket on the
    // consumer
    EXPECT_EQ(ENGINE_SUCCESS, engine->add_stream(cookie, 0, vbid, 0));

    // create snapshot so we can delete the document
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->snapshot_marker(cookie,
                                      /*opaque*/ 1,
                                      vbid,
                                      /*start_seqno*/ 0,
                                      /*end_seqno*/ 100,
                                      zeroFlags));

    // Try and delete the doc
    EXPECT_EQ(ENGINE_SUCCESS,
              engine->deletion(cookie,
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

// Test fixture for Durability-related Warmup tests.
class DurabilityWarmupTest : public STParameterizedBucketTest {
protected:
    void SetUp() {
        STParameterizedBucketTest::SetUp();
        // Add an initial replication topology so we can accept SyncWrites.
        setVBucketToActiveWithValidTopology();
    }

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }
};

// Test that a pending SyncWrite not yet committed is correctly warmed up
// when the bucket restarts.
TEST_P(DurabilityWarmupTest, PendingSyncWrite) {
    // Store a pending SyncWrite (without committing) and then restart
    auto item = makePendingItem(makeStoredDocKey("pendingSW"), "pending_value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);

    resetEngineAndWarmup();

    // Check that the item is still pending with the correct CAS.
    auto vb = engine->getVBucket(vbid);
    auto handle = vb->lockCollections(item->getKey());
    auto prepared = vb->fetchPreparedValue(handle);
    EXPECT_TRUE(prepared.storedValue);
    EXPECT_TRUE(prepared.storedValue->isPending());
    EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());
}

// Test that a pending SyncWrite which was committed is correctly warmed up
// when the bucket restarts (as a Committed item).
TEST_P(DurabilityWarmupTest, CommittedSyncWrite) {
    // Store a pending SyncWrite (without committing) and then restart
    auto key = makeStoredDocKey("key");
    auto item = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid);

    { // scoping vb - is invalid once resetEngineAndWarmup() is called.
        auto vb = engine->getVBucket(vbid);
        vb->commit(key, item->getBySeqno(), {}, vb->lockCollections(key));

        flush_vbucket_to_disk(vbid, 1);
    }
    resetEngineAndWarmup();

    // Check that the item is CommittedviaPrepare.
    auto vb = engine->getVBucket(vbid);
    auto handle = vb->lockCollections(item->getKey());
    auto res = vb->fetchValidValue(
            WantsDeleted::No, TrackReference::No, QueueExpired::No, handle);
    EXPECT_TRUE(res.storedValue);
    EXPECT_EQ(CommittedState::CommittedViaPrepare,
              res.storedValue->getCommitted());
}

// Test that a committed mutation followed by a pending SyncWrite to the same
// key is correctly warmed up when the bucket restarts.
TEST_P(DurabilityWarmupTest, CommittedAndPendingSyncWrite) {
    // Store committed mutation followed by a pending SyncWrite (without
    // committing) and then restart.
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));

    flush_vbucket_to_disk(vbid, 2);

    resetEngineAndWarmup();

    // Should load two items into memory - both committed and the pending value.
    // Check the original committed value is still readable.
    auto vb = engine->getVBucket(vbid);
    EXPECT_EQ(1, vb->getNumItems());
    EXPECT_EQ(1, vb->ht.getNumPreparedSyncWrites());
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ("A", gv.item->getValue()->to_s());

    // Check that the item is still pending, with correct CAS.
    auto handle = vb->lockCollections(item->getKey());
    auto prepared = vb->fetchPreparedValue(handle);
    EXPECT_TRUE(prepared.storedValue);
    EXPECT_TRUE(prepared.storedValue->isPending());
    EXPECT_EQ(item->getCas(), prepared.storedValue->getCas());
}

// Negative test - check that a prepared SyncWrite which has been Aborted
// does _not_ restore the old, prepared SyncWrite after warmup.
TEST_P(DurabilityWarmupTest, AbortedSyncWritePrepareIsNotLoaded) {
    // Commit an initial value 'A', then prepare and then abort a SyncWrite of
    // "B".
    auto key = makeStoredDocKey("key");
    store_item(vbid, key, "A");
    auto item = makePendingItem(key, "B");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    flush_vbucket_to_disk(vbid, 2);

    { // scoping vb - is invalid once resetEngineAndWarmup() is called.
        auto vb = engine->getVBucket(vbid);
        vb->abort(key, item->getBySeqno(), {}, vb->lockCollections(key));

        flush_vbucket_to_disk(vbid, 1);
    }
    resetEngineAndWarmup();

    // Should load one item into memory - committed value.
    auto vb = engine->getVBucket(vbid);
    // @TODO: RocksDB currently only has an estimated item count in
    // full-eviction, so it fails this check. Skip if RocksDB && full_eviction.
    if ((std::get<0>(GetParam()).find("Rocksdb") == std::string::npos) ||
        std::get<0>(GetParam()) == "value_only") {
        EXPECT_EQ(1, vb->getNumItems());
    }
    EXPECT_EQ(0, vb->ht.getNumPreparedSyncWrites());
    auto gv = store->get(key, vbid, cookie, {});
    ASSERT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ("A", gv.item->getValue()->to_s());

    // Check there's no pending item
    auto handle = vb->lockCollections(item->getKey());
    auto prepared = vb->fetchPreparedValue(handle);
    EXPECT_FALSE(prepared.storedValue);
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
    auto vbstate = *kvstore->getVBucketState(vbid);
    vbstate.replicationTopology.clear();
    kvstore->snapshotVBucket(
            vbid, vbstate, VBStatePersist::VBSTATE_PERSIST_WITH_COMMIT);

    resetEngineAndWarmup();

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

INSTANTIATE_TEST_CASE_P(
        FullOrValue,
        DurabilityWarmupTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTestPrintName());
