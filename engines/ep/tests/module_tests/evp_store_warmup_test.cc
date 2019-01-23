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
#include "programs/engine_testapp/mock_server.h"
#include "test_helpers.h"

#include <libcouchstore/couch_db.h>

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
