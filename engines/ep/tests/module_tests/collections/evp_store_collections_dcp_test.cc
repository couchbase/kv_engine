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
#include "collections/manager.h"
#include "dcp/dcpconnmap.h"
#include "failover-table.h"
#include "kvstore.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/test_manifest.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"

#include <functional>
#include <thread>

TEST_P(CollectionsDcpParameterizedTest, test_dcp_consumer) {
    store->setVBucketState(vbid, vbucket_state_replica, false);
    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // Create meat with uid 4 as if it came from manifest uid cafef00d
    std::string collection = "meat";
    CollectionID cid = CollectionEntry::meat.getId();
    ScopeID sid = ScopeEntry::shop1.getId();
    Collections::ManifestUid manifestUid = 0xcafef00d;
    Collections::CreateEventData createEventData{
            manifestUid, sid, cid, "noname", {/*no ttl*/}};
    Collections::CreateEventDcpData createEventDcpData{createEventData};
    Collections::DropEventData dropEventData{manifestUid, sid, cid};
    Collections::DropEventDcpData dropEventDcpData{dropEventData};

    ASSERT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(/*opaque*/ 2,
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
                      /*opaque*/ 2,
                      vbid,
                      mcbp::systemevent::id::CreateCollection,
                      /*seqno*/ 1,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&createEventDcpData),
                       Collections::CreateEventDcpData::size}));

    // We can now access the collection
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));
    EXPECT_TRUE(vb->lockCollections().isCollectionOpen(CollectionEntry::meat));
    EXPECT_EQ(0xcafef00d, vb->lockCollections().getManifestUid());

    // Call the consumer function for handling DCP events
    // delete the meat collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      /*opaque*/ 2,
                      vbid,
                      mcbp::systemevent::id::DeleteCollection,
                      /*seqno*/ 2,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&dropEventDcpData),
                       Collections::DropEventDcpData::size}));

    // It's gone!
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));
}

/*
 * test_dcp connects a producer and consumer to test that collections created
 * on the producer are transferred to the consumer
 *
 * The test replicates VBn to VBn+1
 */
TEST_P(CollectionsDcpParameterizedTest, test_dcp) {
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
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionName::meat, producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionUid::meat, producers->last_collection_id);

    // 2. Replica now knows the collection
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

// Test that DCP works when a TTL is enabled on a collection
TEST_P(CollectionsDcpParameterizedTest, test_dcp_with_ttl) {
    auto checkDcp = [](MockDcpProducer* p,
                       CollectionsDcpTestProducers* dcpCallBacks) {
        // Now step the producer to transfer the collection creation and
        // validate
        // the data we would transfer to the consumer
        EXPECT_EQ(ENGINE_SUCCESS, p->step(dcpCallBacks));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  dcpCallBacks->last_op);
        EXPECT_EQ(CollectionName::meat, dcpCallBacks->last_key);
        EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
                  dcpCallBacks->last_system_event);
        EXPECT_EQ(CollectionUid::meat, dcpCallBacks->last_collection_id);

        // Assert version1, i.e. a TTL is encoded
        ASSERT_EQ(mcbp::systemevent::version::version1,
                  dcpCallBacks->last_system_event_version);

        auto eventData = reinterpret_cast<
                const Collections::CreateWithMaxTtlEventDcpData*>(
                dcpCallBacks->last_system_event_data.data());
        EXPECT_EQ(100, ntohl(eventData->maxTtl));
    };
    {
        VBucketPtr vb = store->getVBucket(vbid);

        // Add a collection, then remove it. This adds events into the CP which
        // we'll manually replicate with calls to step
        CollectionsManifest cm;
        cm.add(CollectionEntry::meat, std::chrono::seconds(100) /*maxttl*/);
        vb->updateFromManifest({cm});

        notifyAndStepToCheckpoint();

        VBucketPtr replica = store->getVBucket(replicaVB);

        // 1. Replica does not know about meat
        EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"meat:bacon", CollectionEntry::meat}));

        checkDcp(producer.get(), producers.get());

        // Finally validate the TTL comes back after a restart
        flushVBucketToDiskIfPersistent(Vbid(0), 1);
    }

    // Ensure the DCP stream has to hit disk/seqlist for backfill
    ensureDcpWillBackfill();

    createDcpObjects({{nullptr, 0}}); // from disk
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false /*backfill from disk*/);

    checkDcp(producer.get(), producers.get());
}

/*
 * test_dcp connects a producer and consumer to test that collections created
 * on the producer are transferred to the consumer when not in the default scope
 *
 * The test replicates VBn to VBn+1
 */
TEST_P(CollectionsDcpParameterizedTest, test_dcp_non_default_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add a scope+collection, then remove then later remove the collection.
    // The test will step DCP and check the replica is updated.
    CollectionsManifest cm;
    cm.setUid(88); // set non-zero for better validation

    cm.add(ScopeEntry::shop1);
    vb->updateFromManifest({cm});
    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_system_event, mcbp::systemevent::id::CreateScope);
    EXPECT_EQ(producers->last_scope_id, ScopeEntry::shop1.uid);
    EXPECT_EQ(producers->last_key, ScopeEntry::shop1.name);

    // MB-32124: check the manifest uid was transferred
    EXPECT_EQ(88, producers->last_collection_manifest_uid);

    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    vb->updateFromManifest({cm});

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));


    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.uid);
    EXPECT_EQ(producers->last_key, CollectionEntry::meat.name);

    // 2. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // remove meat
    vb->updateFromManifest(
            {cm.remove(CollectionEntry::meat, ScopeEntry::shop1)});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // 3. Replica now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // 4. Check that the scopeID was replicated properly
    EXPECT_TRUE(replica->lockCollections().doesKeyBelongToScope(
            StoredDocKey{"meat:bacon", CollectionEntry::meat},
            ScopeEntry::shop1));

    // Now step the producer, no more collection events
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

TEST_P(CollectionsDcpParameterizedTest, mb30893_dcp_partial_updates) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add 3 collections in one update
    CollectionsManifest cm;
    vb->updateFromManifest({cm.add(CollectionEntry::fruit)
                                    .add(CollectionEntry::dairy)
                                    .add(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    // MB-31463: Adding the following handleSlowStream gives coverage for this
    // MB, without the fix the entire test fails as the stream cannot retrieve
    // data from the checkpoint. Only do this for persistent buckets as
    // ephemeral operates differently in-terms of the transition back to
    // in-memory.
    if (persistent()) {
        auto stream = producer->findStream(vbid);
        ASSERT_TRUE(stream);
        auto* as = static_cast<ActiveStream*>(stream.get());
        as->handleSlowStream();
    }

    VBucketPtr replica = store->getVBucket(replicaVB);

    // Now step the producer to transfer the collection creation(s)
    // each collection-creation, closed the checkpoint (hence the extra steps)
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());
    EXPECT_EQ("dairy", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ("meat", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    // And now the new manifest-UID is exposed
    // The cm will have uid 3 + 1 (for the addition of the default scope)
    EXPECT_EQ(4, replica->lockCollections().getManifestUid());

    // Remove two
    vb->updateFromManifest(
            {cm.remove(CollectionEntry::fruit).remove(CollectionEntry::dairy)});

    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(4, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(6, replica->lockCollections().getManifestUid());

    // Add and remove
    vb->updateFromManifest(
            {cm.add(CollectionEntry::dairy2).remove(CollectionEntry::meat)});

    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(6, replica->lockCollections().getManifestUid());

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(8, replica->lockCollections().getManifestUid());
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
        flush_vbucket_to_disk(Vbid(0), (2 * items) + 2);

        // We will see create fruit/dairy and delete dairy (from another CP)
        // In-memory stream will also see all 2*items mutations (ordered with
        // create
        // and delete)
        testDcpCreateDelete({CollectionEntry::fruit, CollectionEntry::dairy},
                            {CollectionEntry::dairy},
                            (2 * items));
    }
    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}}); // from disk

    // Streamed from disk, one create (create of fruit) and items of fruit
    // And the tombstone of dairy
    testDcpCreateDelete(
            {CollectionEntry::fruit}, {CollectionEntry::dairy}, items, false);

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
        flush_vbucket_to_disk(Vbid(0), (2 * items) + 2);
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
    testDcpCreateDelete({CollectionEntry::fruit, CollectionEntry::dairy},
                        {},
                        (2 * items),
                        false);

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}}); // from disk

    // Now read from DCP. We will see:
    // * 2 creates (from the disk backfill)
    // * 2*items, basically all items from all collections
    // The important part is we see the same creates/items as before the warmup
    testDcpCreateDelete({CollectionEntry::fruit, CollectionEntry::dairy},
                        {},
                        (2 * items),
                        false);

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
        flush_vbucket_to_disk(Vbid(0), items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the 2x create events or the intermediate
    // delete. So check DCP sends only 1 collection create (of dairy2) and the
    // tombstone of dairy
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

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
        flush_vbucket_to_disk(Vbid(0), items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the first create (the first delete
    // exists as a tombstone)
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::dairy2.getId()));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, MB_26455) {
    const int items = 3;
    uint32_t m = 7;
    std::vector<CollectionEntry::Entry> dropped;

    {
        auto vb = store->getVBucket(vbid);

        for (uint32_t n = 2; n < m; n++) {
            CollectionsManifest cm;
            vb->updateFromManifest({cm});

            // add fruit (new generation), + 10 to use valid collection range
            vb->updateFromManifest({cm.add(
                    CollectionEntry::Entry{CollectionName::fruit, n + 10})});

            // Mutate fruit
            for (int ii = 0; ii < items; ii++) {
                std::string key = "fruit:" + std::to_string(ii);
                store_item(vbid, StoredDocKey{key, n + 10}, "value");
            }

            // expect create_collection + items
            flush_vbucket_to_disk(vbid, 1 + items);

            if (n < m - 1) {
                dropped.push_back(
                        CollectionEntry::Entry{CollectionName::fruit, n + 10});
                // Drop fruit, except for the last 'generation'
                vb->updateFromManifest({cm.remove(dropped.back())});

                flush_vbucket_to_disk(vbid, 1);
            }
        }
    }

    resetEngineAndWarmup();

    // Stream again!
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, one create (create of fruit) and items of fruit and
    // every delete (tombstones)
    testDcpCreateDelete(
            {CollectionEntry::Entry{CollectionName::fruit, (m - 1) + 10}},
            dropped,
            items,
            false /*fromMemory*/);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            (m - 1) + 10));
}

TEST_P(CollectionsDcpParameterizedTest, collections_manifest_is_ahead) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest({cm});
    store_item(vbid, makeStoredDocKey("k", CollectionEntry::dairy), "v");

    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    try {
        createDcpStream({{R"({"uid":"9"})"}});
        FAIL() << "Expected stream creation to throw";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::collections_manifest_is_ahead, e.code());
    }

    createDcpStream({{R"({"uid":"3"})"}});
    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::fruit.getId());
    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);
    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
}

// Test that create and delete (full deletion) keeps the collection drop marker
// as a tombstone
TEST_F(CollectionsDcpTest, test_dcp_create_delete_erase) {
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
        flush_vbucket_to_disk(Vbid(0), items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);

        runCompaction();
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the first create or delete
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().isCollectionOpen(
            CollectionEntry::dairy2.getId()));
}

// Test that following create scope/collection and drop, the tombstones left
// are replicated to the replica and then finally test that the replica can
// itself replicate those events
TEST_F(CollectionsDcpTest, tombstone_replication) {
    // Firstly, create and drop a scope with collections and flush it all
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // add scope and collection to scope
        CollectionsManifest cm;
        cm.add(ScopeEntry::shop1);
        cm.add(CollectionEntry::fruit, ScopeEntry::shop1);
        vb->updateFromManifest({cm});
        flush_vbucket_to_disk(Vbid(0), 2);

        // remove the scope (and the collection)
        cm.remove(ScopeEntry::shop1);
        vb->updateFromManifest({cm});
        flush_vbucket_to_disk(Vbid(0), 2);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    testDcpCreateDelete(
            {}, {CollectionEntry::fruit}, 0, false, {}, {ScopeEntry::shop1});

    // We've just sent the events from vb0 to vb1, flush vb1
    flush_vbucket_to_disk(Vbid(1), 2);

    // Next reset ready to stream back vb1
    resetEngineAndWarmup();

    // Now manually create, we don't want a consumer
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);
    producers->consumer = nullptr;

    // We want to stream vbid(1)
    createDcpStream({{nullptr, 0}}, Vbid(1));

    // Expect the same data back from vb1 as when we streamed vb0
    testDcpCreateDelete(
            {}, {CollectionEntry::fruit}, 0, false, {}, {ScopeEntry::shop1});
}

// Test that backfilled streams, which drop logically deleted items returns
// a snapshot that is consistent with the dropped collection.
// We need to be sure that in the case we are dropping items the client isn't
// exposed to a snapshot that doesn't include the drop event
void CollectionsDcpTest::tombstone_snapshots_test(bool forceWarmup) {
    uint64_t uuid = 0;
    uint64_t highSeqno = 0;
    {
        VBucketPtr vb = store->getVBucket(vbid);

        // Add two collections and items to default, fruit and dairy
        // We will end by dropping fruit
        CollectionsManifest cm;
        cm.add(CollectionEntry::fruit);
        cm.add(CollectionEntry::dairy);
        vb->updateFromManifest({cm});
        store_item(vbid, StoredDocKey{"d_k1", CollectionEntry::defaultC}, "v");
        store_item(vbid, StoredDocKey{"d_k2", CollectionEntry::defaultC}, "v");
        flushVBucketToDiskIfPersistent(Vbid(0), 4);

        store_item(vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "v");
        store_item(vbid, StoredDocKey{"dairy", CollectionEntry::dairy}, "v");
        store_item(vbid, StoredDocKey{"k2", CollectionEntry::fruit}, "v");

        vb->updateFromManifest({cm.remove(CollectionEntry::fruit)});
        flushVBucketToDiskIfPersistent(Vbid(0), 4);

        uuid = vb->failovers->getLatestUUID();
        highSeqno = vb->getHighSeqno();
    }

    // Now the DCP client, we will step the creation events and one of the
    // default collection items, then disconnect and force backfill

    notifyAndStepToCheckpoint();

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::fruit.getId());
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);

    // Create collection created a new snapshot
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);

    // Create collection created a new snapshot
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    // Record the snapshot we're processing
    auto ss = producers->last_snap_start_seqno;
    auto se = producers->last_snap_end_seqno;

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ(producers->last_collection_id, CollectionID::Default);
    auto startSeq =
            producers->last_byseqno; // Record the startSeq for the new stream

    // Simulate a disconnect, based on test input
    if (forceWarmup) {
        // We want to force backfill, so wipe out everything
        resetEngineAndWarmup();
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookieP, IncludeDeleteTime::No);
        producers->consumer = nullptr;
    } else {
        // Just force stream disconnect
        producer.reset();
    }

    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);
    producers->consumer = nullptr;

    uint64_t rollbackSeqno = 0;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      startSeq,
                                      ~0ull, // end_seqno
                                      uuid,
                                      ss,
                                      se,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{nullptr, 0}}));

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              !forceWarmup);

    // The next validation is split to show the different sequences we expect
    if (forceWarmup) {
        // Critical: The snapshot end must equal the collection drop we queued
        EXPECT_EQ(highSeqno, producers->last_snap_end_seqno);

        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "d_k2");
        EXPECT_EQ(producers->last_collection_id, CollectionID::Default);

        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "dairy");
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::dairy.getId());

        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());
        EXPECT_EQ(producers->last_system_event,
                  mcbp::systemevent::id::DeleteCollection);
        EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
    } else {
        // We expect to see everything upto the drop of fruit
        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "d_k2");
        EXPECT_EQ(producers->last_collection_id, CollectionID::Default);

        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "k1");
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());

        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "dairy");
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::dairy.getId());

        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(producers->last_key, "k2");
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());

        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());
        EXPECT_EQ(producers->last_system_event,
                  mcbp::systemevent::id::DeleteCollection);
        EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
    }
}

TEST_F(CollectionsDcpTest, tombstone_snapshots_disconnect_backfill) {
    tombstone_snapshots_test(true);
}

TEST_P(CollectionsDcpParameterizedTest, tombstone_snapshots_disconnect_memory) {
    tombstone_snapshots_test(false);
}

// Test that we apply the latest manifest when we promote a vBucket from
// replica to active. We don't expect ns_server to send the manifest again so
// we need apply it on promotion
TEST_P(CollectionsDcpParameterizedTest, vb_promotion_update_manifest) {
    // Add fruit to the manifest
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);

    // Get the manager through KVBucketTest so we can get a non-const
    // version and update it to include fruit. This should update the active
    // but NOT the replica.
    KVBucketTest::getCollectionsManager().update(*store,
                                                 cb::const_char_buffer(cm));
    auto active = store->getVBucket(vbid);
    ASSERT_TRUE(active->lockCollections().exists(CollectionEntry::fruit));
    auto replica = store->getVBucket(replicaVB);
    ASSERT_FALSE(replica->lockCollections().exists(CollectionEntry::fruit));

    // Active is now aware of fruit, proving that we updated the manifest,
    // but replica is not. Change the state of replica to active to update
    // the replica manifest.
    store->setVBucketState(replicaVB, vbucket_state_active, false);
    EXPECT_TRUE(replica->lockCollections().exists(CollectionEntry::fruit));
}

class CollectionsFilteredDcpErrorTest : public SingleThreadedKVBucketTest {
public:
    CollectionsFilteredDcpErrorTest() : cookieP(create_mock_cookie()) {
    }
    void SetUp() override {
        config_string +=
                "collections_enabled=true;dcp_noop_mandatory_for_v5_"
                "features=false";
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

class CollectionsFilteredDcpTest : public CollectionsDcpTest {
public:
    CollectionsFilteredDcpTest() : CollectionsDcpTest() {
    }

    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active, false);
    }
};

TEST_F(CollectionsFilteredDcpTest, filtering) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level and a delete of
    // _default (see MB-32131)
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)
                                   .add(CollectionEntry::dairy)
                                   .remove(CollectionEntry::defaultC)});

    // Setup filtered DCP for CID 12/0xc (dairy)
    createDcpObjects({{R"({"collections":["c"]})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ("dairy", producers->last_key);

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

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer keys, only two keys are expected as all "meat"
    // keys are filtered
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    flush_vbucket_to_disk(vbid, 8);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"collections":["c"]})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations in the dairy collection
    testDcpCreateDelete({CollectionEntry::dairy}, {}, 2, false);
}

TEST_F(CollectionsFilteredDcpTest, filtering_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat)
                     .add(ScopeEntry::shop1)
                     .add(CollectionEntry::dairy, ScopeEntry::shop1)});
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    EXPECT_EQ(ScopeEntry::shop1.name, producers->last_key);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // SystemEvent createCollection dairy in shop1
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ(CollectionEntry::dairy.name, producers->last_key);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

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

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer the dairy mutations, we do not expect meat
    // mutations as it is in the default scope
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    flush_vbucket_to_disk(vbid, 8);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations in the dairy collection
    // 1x scope drop
    testDcpCreateDelete(
            {CollectionEntry::dairy}, {}, 2, false, {ScopeEntry::shop1});
}

TEST_F(CollectionsFilteredDcpTest, filtering_grow_scope_from_empty) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Create the scope but don't add any collections to it
    CollectionsManifest cm;
    store->setCollections({cm.add(ScopeEntry::shop1)});
    // Setup filtered DCP for SID 8 (shop1)
    try {
        createDcpObjects({{R"({"scope":"8"})"}});
    } catch (...) {
        FAIL() << "Error creating stream with empty scope";
    }

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now lets add a collection to the shop1 scope
    store->setCollections({cm.add(CollectionEntry::dairy, ScopeEntry::shop1)});

    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    // And no more
    ASSERT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    // Lets add some stuff to dairy
    std::array<std::string, 2> expectedKeys = {{"dairy:one", "dairy:two"}};
    store_item(vbid,
               StoredDocKey{expectedKeys[0], CollectionEntry::dairy},
               "value");
    store_item(vbid,
               StoredDocKey{expectedKeys[1], CollectionEntry::dairy},
               "value");

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer the dairy mutations
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }

    // We flush a collection create + 4 mutations
    flush_vbucket_to_disk(vbid, 4);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations - 2x in dairy
    // 1x scope drop
    testDcpCreateDelete(
            {CollectionEntry::dairy}, {}, 2, false, {ScopeEntry::shop1});
}

TEST_F(CollectionsFilteredDcpTest, filtering_grow_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy(shop1) via the bucket level
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat)
                     .add(ScopeEntry::shop1)
                     .add(CollectionEntry::dairy, ScopeEntry::shop1)});
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Add a new collection to the scope that we are filtering
    store->setCollections(
            {cm.add(CollectionEntry::vegetable, ScopeEntry::shop1)});
    notifyAndStepToCheckpoint();

    // Check we got the system event
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    ASSERT_EQ(CollectionEntry::vegetable.getId(),
              producers->last_collection_id);

    // Store collection documents
    std::array<std::string, 2> expectedKeys = {
            {"vegetable:one", "vegetable:two"}};
    store_item(vbid, StoredDocKey{"meat:four", CollectionEntry::meat}, "value");
    store_item(vbid,
               StoredDocKey{expectedKeys[0], CollectionEntry::vegetable},
               "value");
    store_item(vbid, StoredDocKey{"meat:five", CollectionEntry::meat}, "value");
    store_item(vbid,
               StoredDocKey{expectedKeys[1], CollectionEntry::vegetable},
               "value");
    store_item(vbid, StoredDocKey{"meat:six", CollectionEntry::meat}, "value");

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer vegetable mutations, we do not expect meat
    // mutations as meat is in the default scope
    for (auto& key : expectedKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    flush_vbucket_to_disk(vbid, 9);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 2x create - create of dairy, create of vegetable
    // 2x mutations - 2x in vegetable
    // 1x scope drop
    testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::vegetable},
                        {},
                        2,
                        false,
                        {ScopeEntry::shop1});
}

TEST_F(CollectionsFilteredDcpTest, filtering_shrink_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy(shop1)/vegetable(shop1) via the bucket
    // level
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat)
                     .add(ScopeEntry::shop1)
                     .add(CollectionEntry::dairy, ScopeEntry::shop1)
                     .add(CollectionEntry::vegetable, ScopeEntry::shop1)});
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    ASSERT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // Check the collection create events are correct
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    ASSERT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    ASSERT_EQ(CollectionEntry::vegetable.getId(),
              producers->last_collection_id);

    // Store collection documents
    std::array<std::string, 2> dairyKeys = {{"dairy:one", "dairy:two"}};
    std::array<std::string, 2> vegetableKeys = {
            {"vegetable:one", "vegetable:two"}};
    store_item(vbid, StoredDocKey{"meat:one", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{dairyKeys[0], CollectionEntry::dairy}, "value");
    store_item(
            vbid, StoredDocKey{dairyKeys[1], CollectionEntry::dairy}, "value");
    store_item(vbid, StoredDocKey{"meat:two", CollectionEntry::meat}, "value");
    store_item(vbid,
               StoredDocKey{vegetableKeys[0], CollectionEntry::vegetable},
               "value");
    store_item(vbid,
               StoredDocKey{vegetableKeys[1], CollectionEntry::vegetable},
               "value");
    store_item(
            vbid, StoredDocKey{"meat:three", CollectionEntry::meat}, "value");

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer dairy and vegetable mutations, do not expect
    // meat mutations as meat is in the default scope
    for (auto& key : dairyKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }

    for (auto& key : vegetableKeys) {
        EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    // Now delete the dairy collection
    store->setCollections(
            {cm.remove(CollectionEntry::dairy, ScopeEntry::shop1)});
    notifyAndStepToCheckpoint();

    // Check we got the system event
    ASSERT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    ASSERT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    // Store more meat documents
    store_item(vbid, StoredDocKey{"meat:four", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat:five", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat:six", CollectionEntry::meat}, "value");

    // No new items (dairy is now filtered)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));

    flush_vbucket_to_disk(vbid, 14);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 1x create - create of vegetable
    // 2x mutations - 2x mutations in vegetable
    // 1x delete - the tombstone of dairy
    // 1x scope create - the create of shop1
    testDcpCreateDelete({CollectionEntry::vegetable},
                        {CollectionEntry::dairy},
                        2,
                        false,
                        {ScopeEntry::shop1});
}

// Created for MB-32360
// Note this test does not resume a DCP stream, but a new stream from 0 hits
// the same issue, the filter doesn't know the collection was part of the scope
TEST_F(CollectionsFilteredDcpTest, collection_tombstone_on_scope_filter) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy(shop1)/vegetable(shop1) via the bucket
    // level
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat)
                     .add(ScopeEntry::shop1)
                     .add(CollectionEntry::dairy, ScopeEntry::shop1)
                     .add(CollectionEntry::vegetable, ScopeEntry::shop1)});

    store_item(vbid, StoredDocKey{"dairy1", CollectionEntry::dairy}, "value");
    store_item(vbid, StoredDocKey{"dairy2", CollectionEntry::dairy}, "value");

    // Now delete the dairy collection
    store->setCollections(
            {cm.remove(CollectionEntry::dairy, ScopeEntry::shop1)});

    // Flush it all out
    flush_vbucket_to_disk(vbid, 6);

    // Force collection purge. This is the crucial trigger behind MB-32360
    // Compaction runs and drops all items of dairy, when compaction gets to the
    // dairy deletion system event it triggers 'completeDelete', this is what
    // removes the dairy collection from KV meta-data and allowed the new DCP
    // stream to fail to replicate the collection-drop(dairy) event
    runCompaction();

    vb.reset();

    // Force warmup, it's not required for the MB, but makes the test simpler
    // to progress the DCP tasks
    resetEngineAndWarmup();

    // Now stream back from disk and check filtering
    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 1x create - create of vegetable
    // 2x mutations - 2x mutations in vegetable
    // 1x delete - the tombstone of dairy
    // 1x scope create - the create of shop1
    testDcpCreateDelete({CollectionEntry::vegetable},
                        {CollectionEntry::dairy},
                        0,
                        false,
                        {ScopeEntry::shop1});
}

// Check that when filtering is on, we don't send snapshots for fully filtered
// snapshots
TEST_P(CollectionsDcpParameterizedTest, MB_24572) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level
    CollectionsManifest cm;
    store->setCollections(
            {cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});
    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["c"]})"}});

    // Store collection documents
    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"meat::three", CollectionEntry::meat}, "value");

    notifyAndStepToCheckpoint();

    // SystemEvent createCollection for dairy is expected
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

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

TEST_P(CollectionsDcpParameterizedTest, default_only) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level
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

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now step into the items of which we expect to see only anykey
    notifyAndStepToCheckpoint();

    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("anykey", producers->last_key);

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

TEST_P(CollectionsDcpParameterizedTest, stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["8"]})"}});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteCollection

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(ENGINE_SUCCESS, producer->step(producers.get()));

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of meat via the bucket level
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

TEST_P(CollectionsDcpParameterizedTest, stream_closes_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    store->setCollections({cm});

    // Setup filtered DCP
    createDcpObjects({{R"({"scope":"8"})"}});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteScope

    // Now step the producer to transfer the scope creation
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    // Create scope and the collection are in one update, only the final change
    // gets the manifest ID of 3, so create scope is 0
    EXPECT_EQ(0, producers->last_collection_manifest_uid);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ(CollectionEntry::meat.name, producers->last_key);
    // Final change of the update, moves to the new UID of 3
    EXPECT_EQ(3, producers->last_collection_manifest_uid);

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Remove the scope
    store->setCollections({cm.remove(ScopeEntry::shop1)});

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    // Drop scope triggers 2 changes, the drop collection doesn't expose the
    // new manifest yet.
    EXPECT_EQ(3, producers->last_collection_manifest_uid);

    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));

    // Now step the producer to transfer the scope deletion
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::DropScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    // Now we are fully at the new manifest
    EXPECT_EQ(4, producers->last_collection_manifest_uid);

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the stream end message
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpStreamEnd));
    EXPECT_EQ(end_stream_status_t::END_STREAM_FILTER_EMPTY,
              producers->last_flags);

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

/**
 * Test that you cannot create a filter for closed collections
 */
TEST_P(CollectionsDcpParameterizedTest, empty_filter_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    producer = createDcpProducer(cookieP, IncludeDeleteTime::No);
    createDcpConsumer();

    // Perform a delete of meat
    store->setCollections({cm.remove(CollectionEntry::meat)});

    uint64_t rollbackSeqno;
    try {
        producer->streamRequest(0, // flags
                                1, // opaque
                                vbid,
                                0, // start_seqno
                                ~0ull, // end_seqno
                                0, // vbucket_uuid,
                                0, // snap_start_seqno,
                                0, // snap_end_seqno,
                                &rollbackSeqno,
                                &CollectionsDcpTest::dcpAddFailoverLog,
                                {{R"({"collections":["8"]})"}});
        FAIL() << "Expected an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(ENGINE_UNKNOWN_COLLECTION,
                  ENGINE_ERROR_CODE(e.code().value()));
    }
}

TEST_P(CollectionsDcpParameterizedTest, legacy_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    store->setCollections({cm.add(CollectionEntry::meat)});

    // Make cookie look like a non-collection client
    mock_set_collections_support(cookieP, false);
    mock_set_collections_support(cookieC, false);
    // Setup legacy DCP, it only receives default collection mutation/deletion
    // and should self-close if the default collection were to be deleted
    createDcpObjects({});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // No keys have been written and no event can be sent, so expect nothing
    // after kicking the stream into life
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::Invalid);

    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of $default
    store->setCollections({cm.remove(CollectionEntry::defaultC)});

    // Expect a stream end marker
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(END_STREAM_OK, producers->last_flags);

    // Done... collection deletion of default has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(ENGINE_EWOULDBLOCK, producer->step(producers.get()));
}

static auto allConfigValues = ::testing::Values(
        std::make_tuple(std::string("ephemeral"), std::string("auto_delete")),
        std::make_tuple(std::string("ephemeral"), std::string("fail_new_data")),
        std::make_tuple(std::string("persistent"), std::string{}));

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_CASE_P(CollectionsDcpEphemeralOrPersistent,
                        CollectionsDcpParameterizedTest,
                        allConfigValues, );
