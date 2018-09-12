/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * Tests for Collection functionality in EPStore.
 */
#include "bgfetcher.h"
#include "dcp/dcpconnmap.h"
#include "failover-table.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"

#include <functional>
#include <thread>

extern uint8_t dcp_last_op;
extern std::string dcp_last_key;
extern uint32_t dcp_last_flags;
extern CollectionID dcp_last_collection_id;

class CollectionsDcpTest : public SingleThreadedKVBucketTest {
public:
    CollectionsDcpTest()
        : cookieC(create_mock_cookie()), cookieP(create_mock_cookie()) {
        mock_set_collections_support(cookieP, true);
        mock_set_collections_support(cookieC, true);
    }

    // Setup a producer/consumer ready for the test
    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        createDcpObjects({{nullptr, 0}} /*collections on, but no filter*/);
    }
    std::string getManifest(uint16_t vb) const {
        return store->getVBucket(vb)
                ->getShard()
                ->getRWUnderlying()
                ->getCollectionsManifest(vbid);
    }

    void createDcpStream() {
        uint64_t rollbackSeqno;
        ASSERT_EQ(ENGINE_SUCCESS,
                  producer->streamRequest(
                          0, // flags
                          1, // opaque
                          vbid,
                          0, // start_seqno
                          ~0ull, // end_seqno
                          0, // vbucket_uuid,
                          0, // snap_start_seqno,
                          0, // snap_end_seqno,
                          &rollbackSeqno,
                          &CollectionsDcpTest::dcpAddFailoverLog));
    }

    void createDcpConsumer() {
        CollectionsDcpTest::consumer = std::make_shared<MockDcpConsumer>(
                *engine, cookieC, "test_consumer");
        store->setVBucketState(replicaVB, vbucket_state_replica, false);
        ASSERT_EQ(ENGINE_SUCCESS,
                  consumer->addStream(/*opaque*/ 0,
                                      replicaVB,
                                      /*flags*/ 0));
        // Setup a snapshot on the consumer
        ASSERT_EQ(ENGINE_SUCCESS,
                  consumer->snapshotMarker(/*opaque*/ 1,
                                           /*vbucket*/ replicaVB,
                                           /*start_seqno*/ 0,
                                           /*end_seqno*/ 100,
                                           /*flags*/ 0));
    }

    void createDcpObjects(boost::optional<cb::const_char_buffer> collections) {
        createDcpConsumer();
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookieP, collections, IncludeDeleteTime::No);
        // Patch our local callback into the handlers
        producers->system_event = &CollectionsDcpTest::sendSystemEvent;
        createDcpStream();
    }

    void TearDown() override {
        teardown();
        SingleThreadedKVBucketTest::TearDown();
    }

    void teardown() {
        destroy_mock_cookie(cookieC);
        destroy_mock_cookie(cookieP);
        consumer->closeAllStreams();
        consumer->cancelTask();
        producer->closeAllStreams();
        producer->cancelCheckpointCreatorTask();
        producer.reset();
        consumer.reset();
    }

    void runCheckpointProcessor() {
        SingleThreadedKVBucketTest::runCheckpointProcessor(*producer,
                                                           *producers);
    }

    void notifyAndStepToCheckpoint(
            cb::mcbp::ClientOpcode expectedOp =
                    cb::mcbp::ClientOpcode::DcpSnapshotMarker,
            bool fromMemory = true) {
        // Call parent class function with our producer
        SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(
                *producer, *producers, expectedOp, fromMemory);
    }

    void testDcpCreateDelete(int expectedCreates,
                             int expectedDeletes,
                             int expectedMutations,
                             bool fromMemory = true);

    void resetEngineAndWarmup(std::string new_config = "") {
        teardown();
        SingleThreadedKVBucketTest::resetEngineAndWarmup(new_config);
        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        cookieC = create_mock_cookie();
        cookieP = create_mock_cookie();
    }

    static const uint16_t replicaVB{1};
    static std::shared_ptr<MockDcpConsumer> consumer;
    static mcbp::systemevent::id dcp_last_system_event;

    /*
     * DCP callback method to push SystemEvents on to the consumer
     */
    static ENGINE_ERROR_CODE sendSystemEvent(gsl::not_null<const void*> cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             mcbp::systemevent::id event,
                                             uint64_t bySeqno,
                                             cb::const_byte_buffer key,
                                             cb::const_byte_buffer eventData) {
        (void)cookie;
        (void)vbucket; // ignored as we are connecting VBn to VBn+1
        dcp_last_op = PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT;
        dcp_last_system_event = event;

        if (event == mcbp::systemevent::id::CreateCollection ||
            event == mcbp::systemevent::id::DeleteCollection) {
            dcp_last_collection_id =
                    reinterpret_cast<const Collections::SystemEventDcpData*>(
                            eventData.data())
                            ->cid.to_host();
        }

        // Using the ::size directly in the EXPECT is failing to link on
        // OSX build, but copying the value works.
        const auto expectedSize = Collections::SystemEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        return consumer->systemEvent(
                opaque, replicaVB, event, bySeqno, key, eventData);
    }

    static ENGINE_ERROR_CODE dcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    const void* cookieC;
    const void* cookieP;
    std::unique_ptr<dcp_message_producers> producers;
    std::shared_ptr<MockDcpProducer> producer;
};

std::shared_ptr<MockDcpConsumer> CollectionsDcpTest::consumer;
mcbp::systemevent::id CollectionsDcpTest::dcp_last_system_event;

TEST_F(CollectionsDcpTest, test_dcp_consumer) {
    const void* cookie = create_mock_cookie();

    auto consumer =
            std::make_shared<MockDcpConsumer>(*engine, cookie, "test_consumer");

    store->setVBucketState(vbid, vbucket_state_replica, false);
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // Create meat with uid 4 as if it came from manifest uid cafef00d
    std::string collection = "meat";
    CollectionID cid = CollectionEntry::meat.getId();
    Collections::uid_t manifestUid = 0xcafef00d;
    Collections::SystemEventDcpData eventData{htonll(manifestUid),
                                              cid.to_network()};

    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(/*opaque*/ 1,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 100,
                                       /*flags*/ 0));

    VBucketPtr vb = store->getVBucket(vbid);

    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Call the consumer function for handling DCP events
    // create the meat collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      /*opaque*/ 1,
                      vbid,
                      mcbp::systemevent::id::CreateCollection,
                      /*seqno*/ 1,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&eventData),
                       Collections::SystemEventDcpData::size}));

    // We can now access the collection
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));
    EXPECT_TRUE(vb->lockCollections().isCollectionOpen(CollectionEntry::meat));
    EXPECT_EQ(0xcafef00d, vb->lockCollections().getManifestUid());

    // Call the consumer function for handling DCP events
    // delete the meat collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      /*opaque*/ 1,
                      vbid,
                      mcbp::systemevent::id::DeleteCollection,
                      /*seqno*/ 2,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&eventData),
                       Collections::SystemEventDcpData::size}));

    // It's gone!
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    consumer->closeAllStreams();
    destroy_mock_cookie(cookie);
    consumer->cancelTask();
}

/*
 * test_dcp connects a producer and consumer to test that collections created
 * on the producer are transferred to the consumer
 *
 * The test replicates VBn to VBn+1
 */
TEST_F(CollectionsDcpTest, test_dcp) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add a collection, then remove it. This adds events into the CP which
    // we'll manually replicate with calls to step
    CollectionsManifest cm(CollectionEntry::meat);
    vb->updateFromManifest({cm});

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // 1. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // remove meat
    vb->updateFromManifest({cm.remove(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // 3. Replica now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Now step the producer, no more collection events
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

TEST_F(CollectionsDcpTest, mb30893_dcp_partial_updates) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add 3 collections in one update
    CollectionsManifest cm;
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy)
                                    .add(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // Now step the producer to transfer the collection creation(s)
    // each collection-creation, closed the checkpoint (hence the extra steps)
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);

    // And now the new manifest-UID is exposed
    // The cm will have uid 3 + 1 (for the addition of the default scope)
    EXPECT_EQ(4, replica->lockCollections().getManifestUid());

    // Remove two
    vb->updateFromManifest(
            {cm.remove(CollectionEntry::fruit).remove(CollectionEntry::dairy)});

    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(4, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(6, replica->lockCollections().getManifestUid());

    // Add and remove
    vb->updateFromManifest(
            {cm.add(CollectionEntry::dairy2).remove(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(6, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, dcp_last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(8, replica->lockCollections().getManifestUid());
}

void CollectionsDcpTest::testDcpCreateDelete(int expectedCreates,
                                             int expectedDeletes,
                                             int expectedMutations,
                                             bool fromMemory) {
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              fromMemory);

    int creates = 0, deletes = 0, mutations = 0;
    // step until done
    while (producer->step(producers.get()) == ENGINE_SUCCESS) {
        if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT) {
            switch (dcp_last_system_event) {
            case mcbp::systemevent::id::CreateCollection:
                creates++;
                break;
            case mcbp::systemevent::id::DeleteCollection:
                deletes++;
                break;
            default:
                throw std::logic_error(
                        "CollectionsDcpTest::testDcpCreateDelete unknown "
                        "event:" +
                        std::to_string(int(dcp_last_system_event)));
            }
        } else if (dcp_last_op == PROTOCOL_BINARY_CMD_DCP_MUTATION) {
            mutations++;
        }
    }

    EXPECT_EQ(expectedCreates, creates);
    EXPECT_EQ(expectedDeletes, deletes);
    EXPECT_EQ(expectedMutations, mutations);

    // Finally check that the active and replica have the same manifest, our
    // BeginDeleteCollection should of contained enough information to form
    // an equivalent manifest
    EXPECT_EQ(getManifest(vbid), getManifest(vbid + 1));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, test_dcp_create_delete) {
    const int items = 3;
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy & fruit
        CollectionsManifest cm(CollectionEntry::fruit);
        vb->updateFromManifest({cm.add(CollectionEntry::dairy)});

        // Mutate dairy
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Mutate fruit
        for (int ii = 0; ii < items; ii++) {
            std::string key = "fruit:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::fruit}, "value");
        }

        // Delete dairy
        vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

        // Persist everything ready for warmup and check.
        // Flusher will merge create/delete and we only flush the delete
        flush_vbucket_to_disk(0, (2 * items) + 2);

        // We will see create fruit/dairy and delete dairy (from another CP)
        // In-memory stream will also see all 2*items mutations (ordered with
        // create
        // and delete)
        testDcpCreateDelete(2, 1, (2 * items));
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}}); // from disk

    // Streamed from disk, one create (create of fruit) and items of fruit
    testDcpCreateDelete(1, 0, items, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::fruit));
}

// Test that a backfill stream is consistent over failure.
// 1) do some collection changes and flush then delete a collection but do not
//    flush the delete
// 2) DCP stream from 0
// 3) reset/warmup
// 4) DCP stream from 0 - we must get the same items as step 2.
// Previously, a backfill would perform isLogicallyDeleted using the in-memory
// Vbucket manifest, which loses changes if we fail before flushing
// (so 4 would receive items which 2 did not send). The test ensures that the
//  stream is consistent, because it should be doing backfill isLogically checks
// against the persisted collections state.
TEST_F(CollectionsDcpTest, test_dcp_create_delete_warmup) {
    const int items = 3;
    CollectionsManifest cm;
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy & fruit
        vb->updateFromManifest(
                {cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy)});

        // Mutate dairy
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Mutate fruit
        for (int ii = 0; ii < items; ii++) {
            std::string key = "fruit:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::fruit}, "value");
        }

        // Flush the creates and the items, but do not flush the next delete
        flush_vbucket_to_disk(0, (2 * items) + 2);
    }

    // To force the first full backfill, kill the engine
    resetEngineAndWarmup();

    // Now delete the dairy collection
    {
        store->getVBucket(vbid)->updateFromManifest(
                {cm.remove(CollectionEntry::dairy)});

        // Front-end will be stopping dairy mutations...
        EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
                CollectionEntry::fruit));
        EXPECT_FALSE(
                store->getVBucket(vbid)->lockCollections().isCollectionOpen(
                        CollectionEntry::dairy));
    }

    createDcpObjects({{nullptr, 0}}); // from disk

    // Now read from DCP. We will see:
    // * 2 creates (from the disk backfill)
    // * 2*items, basically all items from all collections
    testDcpCreateDelete(2, 0, (2 * items), false);

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}}); // from disk

    // Now read from DCP. We will see:
    // * 2 creates (from the disk backfill)
    // * 2*items, basically all items from all collections
    // The important part is we see the same creates/items as before the warmup
    testDcpCreateDelete(2, 0, (2 * items), false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::fruit));
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::dairy));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        CollectionsManifest cm(CollectionEntry::dairy);
        vb->updateFromManifest({cm});

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Delete dairy
        vb->updateFromManifest({cm.remove(CollectionEntry::dairy)});

        // Create dairy (new uid)
        vb->updateFromManifest({cm.add(CollectionEntry::dairy2)});

        // Persist everything ready for warmup and check.
        // Flush the items + 1 delete (dairy) and 1 create (dairy2)
        flush_vbucket_to_disk(0, items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete(2, 1, items);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the 2x create events or the intermediate
    // delete. So check DCP sends only 1 collection create.
    testDcpCreateDelete(1, 0, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::dairy2.getId()));
}

// Test that a create/delete/create don't dedup
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create2) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        CollectionsManifest cm(CollectionEntry::dairy);
        vb->updateFromManifest({cm});

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Delete dairy/create dairy in *one* update
        vb->updateFromManifest({cm.remove(CollectionEntry::dairy)
                                        .add(CollectionEntry::dairy2)});

        // Persist everything ready for warmup and check.
        // Flush the items + 1 delete (dairy) and 1 create (dairy2)
        flush_vbucket_to_disk(0, items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete(2, 1, items);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the first create or delete
    testDcpCreateDelete(1, 0, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::dairy2.getId()));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, MB_26455) {
    const int items = 3;
    uint32_t m = 7;

    {
        auto vb = store->getVBucket(vbid);

        for (uint32_t n = 2; n < m; n++) {
            CollectionsManifest cm;
            vb->updateFromManifest({cm});

            // add fruit (new generation)
            vb->updateFromManifest(
                    {cm.add(CollectionEntry::Entry{CollectionName::fruit, n})});

            // Mutate fruit
            for (int ii = 0; ii < items; ii++) {
                std::string key = "fruit:" + std::to_string(ii);
                store_item(vbid, StoredDocKey{key, n}, "value");
            }

            // expect create_collection + items
            flush_vbucket_to_disk(vbid, 1 + items);

            if (n < m - 1) {
                // Drop fruit, except for the last 'generation'
                vb->updateFromManifest({cm.remove(CollectionEntry::fruit)});

                flush_vbucket_to_disk(vbid, 1);
            }
        }
    }

    resetEngineAndWarmup();

    // Stream again!
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, one create (create of fruit) and items of fruit
    testDcpCreateDelete(1, 0, items, false /*fromMemory*/);

    EXPECT_TRUE(
            store->getVBucket(vbid)->lockCollections().isCollectionOpen(m - 1));
}

class CollectionsFilteredDcpErrorTest : public SingleThreadedKVBucketTest {
public:
    CollectionsFilteredDcpErrorTest() : cookieP(create_mock_cookie()) {
    }
    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }

    void TearDown() override {
        destroy_mock_cookie(cookieP);
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

protected:
    std::shared_ptr<MockDcpProducer> producer;
    const void* cookieP;
};

TEST_F(CollectionsFilteredDcpErrorTest, error1) {
    // Set some collections
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});

    std::string filter = R"({"collections":["8"]})";
    // Can't create a filter for unknown collections
    try {
        MockDcpProducer mock(*engine,
                             cookieP,
                             "test_producer",
                             0,
                             cb::const_char_buffer{filter},
                             false /*startTask*/);
        FAIL() << "Expected an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::unknown_collection, e.code());
    }
}

class CollectionsFilteredDcpTest : public CollectionsDcpTest {
public:
    CollectionsFilteredDcpTest() : CollectionsDcpTest() {
    }

    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        producers = std::make_unique<MockDcpMessageProducers>(engine.get());
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

TEST_F(CollectionsFilteredDcpTest, filtering) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level (filters are
    // worked out from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});
    // Setup filtered DCP for CID 6 (dairy)
    createDcpObjects({{R"({"collections":["6"]})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(CollectionEntry::dairy.getId(), dcp_last_collection_id);

    // Store collection documents
    std::array<std::string, 2> expectedKeys = {{"dairy:one", "dairy:two"}};
    store_item(vbid, StoredDocKey{"meat:one", CollectionEntry::meat}, "value");
    store_item(vbid,
               StoredDocKey{expectedKeys[0], CollectionEntry::dairy},
               "value");
    store_item(vbid, StoredDocKey{"meat:two", CollectionEntry::meat}, "value");
    store_item(vbid,
               StoredDocKey{expectedKeys[1], CollectionEntry::dairy},
               "value");
    store_item(
            vbid, StoredDocKey{"meat:three", CollectionEntry::meat}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer keys, only two keys are expected as all "meat"
    // keys are filtered
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(), dcp_last_collection_id);
        EXPECT_EQ(key, dcp_last_key);
    }
    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    flush_vbucket_to_disk(vbid, 7);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    // In order to create a filter, a manifest needs to be set
    store->setCollections({cm});

    createDcpObjects({{R"({"collections":["6"]})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations in the dairy collection
    testDcpCreateDelete(1, 0, 2, false);
}

// Check that when filtering is on, we don't send snapshots for fully filtered
// snapshots
TEST_F(CollectionsFilteredDcpTest, MB_24572) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level (filters are
    // worked out from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});
    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["6"]})"}});

    // Store collection documents
    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"meat::three", CollectionEntry::meat}, "value");

    notifyAndStepToCheckpoint();

    // SystemEvent createCollection for dairy is expected
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT, dcp_last_op);
    EXPECT_EQ(CollectionEntry::dairy.getId(), dcp_last_collection_id);

    // And no more for this stream - no meat
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    // and new mutations?
    store_item(
            vbid, StoredDocKey{"meat::one1", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"meat::two2", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"meat::three3", CollectionEntry::meat}, "value");
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid);
}

TEST_F(CollectionsFilteredDcpTest, default_only) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level (filters are
    // worked out from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});

    // Setup DCP
    createDcpObjects({/*no collections*/});

    // Store collection documents and one default collection document
    store_item(vbid, StoredDocKey{"meat:one", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"dairy:one", CollectionEntry::dairy}, "value");
    store_item(
            vbid, StoredDocKey{"anykey", CollectionEntry::defaultC}, "value");
    store_item(
            vbid, StoredDocKey{"dairy:two", CollectionEntry::dairy}, "value");
    store_item(
            vbid, StoredDocKey{"meat:three", CollectionEntry::meat}, "value");

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now step into the items of which we expect to see only anykey
    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(PROTOCOL_BINARY_CMD_DCP_MUTATION, dcp_last_op);
    EXPECT_EQ("anykey", dcp_last_key);

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

TEST_F(CollectionsFilteredDcpTest, stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["2"]})"}});

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteCollection

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    store->setCollections({cm.remove(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the close stream
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

/**
 * Test that creation of a stream that resulted in an empty filter, because
 * all the collection have been deleted, just goes to stream end.
 */
TEST_F(CollectionsFilteredDcpTest, empty_filter_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    producer = createDcpProducer(
            cookieP, {{R"({"collections":["2"]})"}}, IncludeDeleteTime::No);
    createDcpConsumer();

    // Perform a delete of meat
    store->setCollections({cm.remove(CollectionEntry::meat)});

    createDcpStream();

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    EXPECT_TRUE(vb0Stream->isActive());

    // Expect a stream end marker and no data
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(END_STREAM_FILTER_EMPTY, dcp_last_flags);

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

/**
 * Test that creation of a stream that resulted in an empty filter, because
 * all the collection have been deleted, just goes to stream end.
 * This test uses a filter with name:uid
 */
TEST_F(CollectionsFilteredDcpTest, empty_filter_stream_closes2) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    // specific collection uid
    producer = createDcpProducer(
            cookieP, {{R"({"collections":["2"]})"}}, IncludeDeleteTime::No);
    createDcpConsumer();

    // Perform a delete of meat
    store->setCollections({cm.remove(CollectionEntry::meat)});

    createDcpStream();

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    EXPECT_TRUE(vb0Stream->isActive());

    // Expect a stream end marker and no data
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(END_STREAM_FILTER_EMPTY, dcp_last_flags);

    // Done... collection deletion of meat:1 has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

TEST_F(CollectionsFilteredDcpTest, legacy_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level (filters are worked out
    // from the bucket manifest)
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    // Setup legacy DCP, it only receives default collection mutation/deletion
    // and should self-close if the default collection were to be deleted
    createDcpObjects({});

    auto vb0Stream = producer->findStream(0);
    ASSERT_NE(nullptr, vb0Stream.get());

    // No keys have been written and no event can be sent, so expect nothing
    // after kicking the stream into life
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid);

    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of $default
    store->setCollections({cm.remove(CollectionEntry::defaultC)});

    // Expect a stream end marker
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(END_STREAM_OK, dcp_last_flags);

    // Done... collection deletion of default has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}
