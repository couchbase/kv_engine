/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * Tests for Collection functionality in EPStore.
 */
#include "bgfetcher.h"
#include "checkpoint_manager.h"
#include "collections/collection_persisted_stats.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "dcp/response.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "ep_time.h"
#include "ephemeral_vb.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "kvstore/couch-kvstore/couch-kvstore-metadata.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_global_task.h"
#include "tests/mock/mock_stream.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/dcp_utils.h"
#include "tests/module_tests/evp_store_test.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/vbucket_utils.h"

#include <engines/ep/src/collections/collections_types.h>
#include <engines/ep/src/ephemeral_tombstone_purger.h>
#include <engines/ep/tests/ep_test_apis.h>
#include <utilities/test_manifest.h>

#include <functional>
#include <thread>

TEST_P(CollectionsDcpParameterizedTest, test_dcp_consumer) {
    store->setVBucketState(vbid, vbucket_state_replica);
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    // Create meat with uid 4 as if it came from manifest uid cafef00d
    std::string collection = "meat";
    CollectionID cid = CollectionEntry::meat.getId();
    ScopeID sid = ScopeEntry::shop1.getId();
    Collections::ManifestUid manifestUid(0xcafef00d);
    Collections::CreateEventData createEventData{
            manifestUid, {sid, cid, collection, {/*no ttl*/}}};
    Collections::CreateEventDcpData createEventDcpData{createEventData};
    Collections::DropEventData dropEventData{manifestUid, sid, cid};
    Collections::DropEventDcpData dropEventDcpData{dropEventData};

    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 2,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 100,
                                       /*flags*/ 0,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    VBucketPtr vb = store->getVBucket(vbid);

    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Call the consumer function for handling DCP events
    // create the meat collection
    EXPECT_EQ(cb::engine_errc::success,
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
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::meat));
    EXPECT_EQ(0xcafef00d, vb->lockCollections().getManifestUid());

    // Lets put an item in it
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      /*opaque*/ 2,
                      StoredDocKey{"meat:bacon", CollectionEntry::meat},
                      cb::const_byte_buffer(),
                      /*priv_bytes*/ 0,
                      PROTOCOL_BINARY_DATATYPE_JSON,
                      /*cas*/ 0,
                      vbid,
                      /*flags*/ 0,
                      /*bySeqno*/ 2,
                      /*revSeqno*/ 0,
                      /*expTime*/ 0,
                      /*lock_time*/ 0,
                      /*meta*/ cb::const_byte_buffer(),
                      /*nru*/ 0));

    // Now check that the DCP consumer has updated the in memory high seqno
    // counter for this item
    EXPECT_EQ(2, vb->lockCollections().getHighSeqno(CollectionEntry::meat));

    // Call the consumer function for handling DCP events
    // delete the meat collection
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      /*opaque*/ 2,
                      vbid,
                      mcbp::systemevent::id::DeleteCollection,
                      /*seqno*/ 3,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&dropEventDcpData),
                       Collections::DropEventDcpData::size}));

    // It's gone!
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));
}

/**
 * Test that we are sending the manifest uid when resuming a stream
 */
TEST_F(CollectionsDcpTest, stream_request_uid) {
    // We shouldn't have tried to create a filtered producer
    EXPECT_EQ("", producers->last_collection_filter);

    // Create meat with uid 4 as if it came from manifest uid cafef00d
    std::string collection = "meat";
    CollectionID cid = CollectionEntry::meat.getId();
    ScopeID sid = ScopeEntry::shop1.getId();
    Collections::ManifestUid manifestUid(0xcafef00d);
    Collections::CreateEventData createEventData{
            manifestUid, {sid, cid, collection, {/*no ttl*/}}};
    Collections::CreateEventDcpData eventDcpData{createEventData};

    VBucketPtr vb = store->getVBucket(replicaVB);

    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    uint32_t opaque = 1;
    uint32_t seqno = 1;

    // Setup a snapshot on the consumer
    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 1,
                                       /*vbucket*/ replicaVB,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 100,
                                       /*flags*/ 0,
                                       /*highCompletedSeqno*/ {},
                                       /*maxVisibleSeqno*/ {}));

    // Call the consumer function for handling DCP events
    // create the meat collection
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      opaque,
                      replicaVB,
                      mcbp::systemevent::id::CreateCollection,
                      seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(collection.data()),
                       collection.size()},
                      {reinterpret_cast<const uint8_t*>(&eventDcpData),
                       Collections::CreateEventDcpData::size}));

    // We can now access the collection
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));
    EXPECT_TRUE(vb->lockCollections().exists(CollectionEntry::meat));
    EXPECT_EQ(0xcafef00d, vb->lockCollections().getManifestUid());

    consumer->closeAllStreams();

    // When we add a stream back we should send the latest manifest uid
    EXPECT_EQ(cb::engine_errc::success,
              consumer->addStream(opaque, replicaVB, 0));

    while (consumer->step(*producers) == cb::engine_errc::success) {
        handleProducerResponseIfStepBlocked(*consumer, *producers);
    }

    // And we've passed the correct filter on to the producer
    EXPECT_EQ("{\"uid\":\"cafef00d\"}", producers->last_collection_filter);
}

/**
 * This test was originally created to cover what happens when a stream-request
 * from the "future" occurs, in collection terms the client claims to know about
 * a manifest from the future. No longer though does KV do a 'tmpfail' error
 * when this happens, it's flawed in the case of quorum loss and could lead to
 * indefinite denial to the client. KV now allows the stream-request and uses
 * normal rollback to guide the client to the correct start-seqno, this test
 * now covers that.
 */
TEST_F(CollectionsDcpTest, failover_after_drop_collection) {
    Vbid vbid0{vbid};
    Vbid vbid1{replicaVB};

    // This test begins with an active (vb:0) and replica (vb:1) vbucket and
    // uses the CollectionsDcpTest 'producers' so step transfers system-events
    // from active to replica.
    //
    // The test begins and sets our scenario:
    //
    // 1) Creates two collections and expect they exist in both vbuckets.
    // 2) Drop one of the collections from vb:0 only.
    auto vb0 = store->getVBucket(vbid0);

    CollectionsManifest cm(CollectionEntry::meat);
    // Update the bucket::manifest (which will apply changes to the active VB)
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    flushVBucketToDiskIfPersistent(vbid0, 2);
    notifyAndStepToCheckpoint();

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);

    EXPECT_TRUE(vb0->lockCollections().exists(CollectionEntry::meat));
    EXPECT_TRUE(vb0->lockCollections().exists(CollectionEntry::fruit));
    auto vb1 = store->getVBucket(vbid1);
    EXPECT_TRUE(vb1->lockCollections().exists(CollectionEntry::meat));
    EXPECT_FALSE(vb1->lockCollections().exists(CollectionEntry::fruit));

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_TRUE(vb1->lockCollections().exists(CollectionEntry::fruit));

    // Store and transfer items 0 to make rollback testing cover more ground.
    auto key = StoredDocKey{"k1", CollectionEntry::fruit};
    store_item(vbid0, key, "v1");
    store_item(vbid0, StoredDocKey{"k2", CollectionEntry::fruit}, "v2");
    flushVBucketToDiskIfPersistent(vbid0, 2);
    notifyAndStepToCheckpoint();
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);

    // flush of vb:1 is required so that it later directs rollback to not 0.
    flushVBucketToDiskIfPersistent(vbid1, 4);

    // Now drop the collection
    vb0->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
    flushVBucketToDiskIfPersistent(vbid0, 1);

    // vb:0 is at seqno:3 and has 1 collection
    // vb:1 is at seqno:2 and has 2 collections
    //
    // Next the vbuckets will switch states for the next phase of the test.
    // Here vb:0 will consume DCP from vb:1 (i.e. vb:0 is now a replica).
    // This means that vb:0 will do a stream-request with a start-seqno {3}
    // which is ahead of vb:1 {2}

    // Kill the producer, then set up a consumer from the active to the
    // replica to loosely simulate a failover
    notifyAndStepToCheckpoint();
    producer->closeAllStreams();
    producer->cancelCheckpointCreatorTask();
    consumer->closeAllStreams();
    consumer->cancelTask();

    // Grab the vb:1 uuid for the stream-request
    failover_entry_t entry = vb1->failovers->getLatestEntry();
    auto vbucket_uuid = entry.vb_uuid;

    // Switch the states
    store->setVBucketState(vbid0, vbucket_state_replica, {});
    store->setVBucketState(vbid1, vbucket_state_active, {});

    // Some required setup
    consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookieP, "test_consumer");
    ASSERT_EQ(cb::engine_errc::success, consumer->addStream(0, vbid0, 0));
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieC, IncludeDeleteTime::No);

    uint64_t rollbackSeqno = 0;

    // Now we ask vb:1 for a stream from 5 and expect to be told to rollback to
    // 4 - i.e. dropped collection is now back in play.
    EXPECT_EQ(5, vb0->getHighSeqno());
    EXPECT_EQ(cb::engine_errc::rollback,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid1,
                                      vb0->getHighSeqno(), // start_seqno
                                      ~0ull, // end_seqno
                                      vbucket_uuid, // vbucket_uuid,
                                      vb0->getHighSeqno(), // snap_start_seqno,
                                      vb0->getHighSeqno(), // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {}));

    // Seqno 4 is for the high-seqno of fruit
    EXPECT_EQ(rollbackSeqno, 4);
    EXPECT_FALSE(vb0->lockCollections().exists(CollectionEntry::fruit));
    GetValue gv = store->get(key, vbid0, cookie, {});
    EXPECT_EQ(cb::engine_errc::unknown_collection, gv.getStatus());

    EXPECT_EQ(TaskStatus::Complete, store->rollback(vbid0, rollbackSeqno));
    EXPECT_EQ(rollbackSeqno, vb0->getHighSeqno());
    EXPECT_TRUE(vb0->lockCollections().exists(CollectionEntry::fruit));
    gv = store->getReplica(key, vbid0, cookie, {});
    EXPECT_EQ(cb::engine_errc::success, gv.getStatus());
}

// MB_38019 saw that if a replica gets ahead of the local node, and is
// switched to active, we would effectively roll it back, however that would
// trigger a monotonic exception
TEST_F(CollectionsDcpTest, MB_38019) {
    VBucketPtr active = store->getVBucket(vbid);

    // setCollections will update active node
    CollectionsManifest cm(CollectionEntry::fruit);
    setCollections(cookie, cm);

    VBucketPtr replica = store->getVBucket(replicaVB);

    auto uid = cm.getUid();
    // Drive the replica as if DCP was pushing changes, first a snapshot must
    // be created, then create collections, first we will match the active
    // node, then go ahead by two extra changes.
    replica->checkpointManager->createSnapshot(
            1, 3, 0, CheckpointType::Memory, 3);
    replica->replicaCreateCollection(Collections::ManifestUid(uid),
                                     {ScopeID::Default, CollectionEntry::fruit},
                                     "fruit",
                                     {},
                                     1);
    replica->replicaCreateCollection(Collections::ManifestUid(++uid),
                                     {ScopeID::Default, CollectionEntry::meat},
                                     "meat",
                                     {},
                                     2);
    replica->replicaCreateCollection(Collections::ManifestUid(++uid),
                                     {ScopeID::Default, CollectionEntry::dairy},
                                     "dairy",
                                     {},
                                     3);

    // Would of seen a monotonic exception
    EXPECT_NO_THROW(
            store->setVBucketState(replicaVB, vbucket_state_active, {}));

    // Finally apply the changes the replica saw, but via setCollections, the
    // replica should be ignoring the downlevel manifests
    cm.add(CollectionEntry::meat);
    EXPECT_EQ(cb::engine_errc::success, setCollections(cookie, cm));

    cm.add(CollectionEntry::dairy);
    EXPECT_EQ(cb::engine_errc::success, setCollections(cookie, cm));
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
    vb->updateFromManifest(makeManifest(cm));

    // @todo MB-26334: persistent buckets don't track the system event counts
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionName::meat, producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionUid::meat, producers->last_collection_id);

    // 2. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // remove meat
    vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::meat)));

    // The delete collection event still exists
    if (!persistent()) {
        EXPECT_EQ(1, vb->getNumSystemItems());
    }

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));

    // 3. Replica now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Now step the producer, no more collection events
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

// Test that DCP works when a TTL is enabled on a collection
TEST_P(CollectionsDcpParameterizedTest, test_dcp_with_ttl) {
    auto checkDcp = [](MockDcpProducer* p,
                       CollectionsDcpTestProducers& dcpCallBacks) {
        // Now step the producer to transfer the collection creation and
        // validate
        // the data we would transfer to the consumer
        EXPECT_EQ(cb::engine_errc::success, p->step(dcpCallBacks));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, dcpCallBacks.last_op);
        EXPECT_EQ(CollectionName::meat, dcpCallBacks.last_key);
        EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
                  dcpCallBacks.last_system_event);
        EXPECT_EQ(CollectionUid::meat, dcpCallBacks.last_collection_id);

        // Assert version1, i.e. a TTL is encoded
        ASSERT_EQ(mcbp::systemevent::version::version1,
                  dcpCallBacks.last_system_event_version);

        auto eventData = reinterpret_cast<
                const Collections::CreateWithMaxTtlEventDcpData*>(
                dcpCallBacks.last_system_event_data.data());
        EXPECT_EQ(100, ntohl(eventData->maxTtl));
    };
    {
        VBucketPtr vb = store->getVBucket(vbid);

        // Add a collection, then remove it. This adds events into the CP which
        // we'll manually replicate with calls to step
        CollectionsManifest cm;
        cm.add(CollectionEntry::meat, std::chrono::seconds(100) /*maxttl*/);
        vb->updateFromManifest(makeManifest(cm));

        notifyAndStepToCheckpoint();

        VBucketPtr replica = store->getVBucket(replicaVB);

        // 1. Replica does not know about meat
        EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
                StoredDocKey{"meat:bacon", CollectionEntry::meat}));

        checkDcp(producer.get(), *producers);

        // Finally validate the TTL comes back after a restart
        flushVBucketToDiskIfPersistent(Vbid(0), 1);
    }

    // Ensure the DCP stream has to hit disk/seqlist for backfill
    ensureDcpWillBackfill();

    createDcpObjects({{nullptr, 0}}); // from disk
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false /*in-memory = false*/);

    checkDcp(producer.get(), *producers);
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
    vb->updateFromManifest(makeManifest(cm));
    notifyAndStepToCheckpoint();
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_system_event, mcbp::systemevent::id::CreateScope);
    EXPECT_EQ(producers->last_scope_id, ScopeEntry::shop1.uid);
    EXPECT_EQ(producers->last_key, ScopeEntry::shop1.name);

    // MB-32124: check the manifest uid was transferred
    EXPECT_EQ(89, producers->last_collection_manifest_uid);

    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    vb->updateFromManifest(makeManifest(cm));

    notifyAndStepToCheckpoint();

    VBucketPtr replica = store->getVBucket(replicaVB);

    // 1. Replica does not know about meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.uid);
    EXPECT_EQ(producers->last_scope_id, ScopeEntry::shop1.uid);
    EXPECT_EQ(producers->last_key, CollectionEntry::meat.name);

    // 2. Replica now knows the collection
    EXPECT_TRUE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    EXPECT_EQ(0, vb->getManifest().lock().getDataSize(ScopeEntry::shop1));
    EXPECT_EQ(0, replica->getManifest().lock().getDataSize(ScopeEntry::shop1));

    // Both active and replica updated the data sizes for persistent buckets
    if (persistent()) {
        // Flush, the event and item are stored and dataSize is updated
        flushVBucketToDiskIfPersistent(vbid, 2);
        flushVBucketToDiskIfPersistent(replicaVB, 2);

        EXPECT_NE(0, vb->getManifest().lock().getDataSize(ScopeEntry::shop1));
        EXPECT_EQ(vb->getManifest().lock().getDataSize(ScopeEntry::shop1),
                  replica->getManifest().lock().getDataSize(ScopeEntry::shop1));
    } else {
        // Nothing for ephemeral
        EXPECT_EQ(0, vb->getManifest().lock().getDataSize(ScopeEntry::shop1));
        EXPECT_EQ(0,
                  replica->getManifest().lock().getDataSize(ScopeEntry::shop1));
    }

    // remove meat
    vb->updateFromManifest(
            makeManifest(cm.remove(CollectionEntry::meat, ScopeEntry::shop1)));

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));

    // 3. Check that the scopeID was replicated properly
    EXPECT_TRUE(replica->lockCollections().isScopeValid(ScopeEntry::shop1));

    // 4. Replica now blocking access to meat
    EXPECT_FALSE(replica->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"meat:bacon", CollectionEntry::meat}));

    // Now step the producer, no more collection events
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, mb30893_dcp_partial_updates) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add 3 collections in one update
    CollectionsManifest cm;
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::fruit)
                                                .add(CollectionEntry::dairy)
                                                .add(CollectionEntry::meat)));

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
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(0, replica->lockCollections().getManifestUid());
    EXPECT_EQ("dairy", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ("meat", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    // And now the new manifest-UID is exposed
    // The cm will have uid 2 + 1 (for the addition of the default scope)
    EXPECT_EQ(3, replica->lockCollections().getManifestUid());

    // Remove two
    vb->updateFromManifest(makeManifest(
            cm.remove(CollectionEntry::fruit).remove(CollectionEntry::dairy)));

    notifyAndStepToCheckpoint();

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(3, replica->lockCollections().getManifestUid());

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(5, replica->lockCollections().getManifestUid());

    // Add and remove
    vb->updateFromManifest(makeManifest(
            cm.add(CollectionEntry::dairy2).remove(CollectionEntry::meat)));

    notifyAndStepToCheckpoint();

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(5, replica->lockCollections().getManifestUid());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(7, replica->lockCollections().getManifestUid());
}

// Test that a create/delete don't dedup
TEST_P(CollectionsDcpParameterizedTest, test_dcp_create_delete) {
    const int items = 3;
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy & fruit
        CollectionsManifest cm(CollectionEntry::fruit);
        vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::dairy)));

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

        flushVBucketToDiskIfPersistent(vbid, (2 * items) + 2);

        // Delete dairy
        vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

        // Persist everything ready for warmup and check.
        flushVBucketToDiskIfPersistent(vbid, 1);

        // We will see create fruit/dairy and delete dairy (from another CP)
        // In-memory stream will also see all 2*items mutations (ordered with
        // create
        // and delete)
        {
            SCOPED_TRACE("DCP 1");
            testDcpCreateDelete(
                    {CollectionEntry::fruit, CollectionEntry::dairy},
                    {CollectionEntry::dairy},
                    (2 * items));
        }
    }
    ensureDcpWillBackfill();

    // Test against a different VB as our old replica will have data
    replicaVB++;
    createDcpObjects({{nullptr, 0}}); // from disk

    // Streamed from disk, one create (create of fruit) and items of fruit
    // And the tombstone of dairy
    {
        SCOPED_TRACE("DCP 2");
        testDcpCreateDelete({CollectionEntry::fruit},
                            {CollectionEntry::dairy},
                            items,
                            false);
    }

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
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
        vb->updateFromManifest(makeManifest(
                cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy)));

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
                makeManifest(cm.remove(CollectionEntry::dairy)));

        // Front-end will be stopping dairy mutations...
        EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
                CollectionEntry::fruit));
        EXPECT_FALSE(store->getVBucket(vbid)->lockCollections().exists(
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

    // Test against a different VB as our old replica will have data
    replicaVB++;
    createDcpObjects({{nullptr, 0}}); // from disk

    // Now read from DCP. We will see:
    // * 2 creates (from the disk backfill)
    // * 2*items, basically all items from all collections
    // The important part is we see the same creates/items as before the warmup
    testDcpCreateDelete({CollectionEntry::fruit, CollectionEntry::dairy},
                        {},
                        (2 * items),
                        false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::fruit));
    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::dairy));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        CollectionsManifest cm(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Delete dairy
        vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::dairy)));

        // Create dairy (new uid)
        vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::dairy2)));

        // Persist everything ready for warmup and check.
        // Flush the items + 1 delete (dairy) and 1 create (dairy2)
        flush_vbucket_to_disk(Vbid(0), items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);
    }

    resetEngineAndWarmup();

    // Test against a different VB as our old replica will have data
    replicaVB++;
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the 2x create events or the intermediate
    // delete. So check DCP sends only 1 collection create (of dairy2) and the
    // tombstone of dairy
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::dairy2.getId()));
}

// Test that a create/delete/create don't dedup
TEST_F(CollectionsDcpTest, test_dcp_create_delete_create2) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        CollectionsManifest cm(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Delete dairy/create dairy in *one* update
        vb->updateFromManifest(
                makeManifest(cm.remove(CollectionEntry::dairy)
                                     .add(CollectionEntry::dairy2)));

        // Persist everything ready for warmup and check.
        // Flush the items + 1 delete (dairy) and 1 create (dairy2)
        flush_vbucket_to_disk(Vbid(0), items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);
    }

    resetEngineAndWarmup();

    // Test against a different VB as our old replica will have data
    replicaVB++;
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the first create (the first delete
    // exists as a tombstone)
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
            CollectionEntry::dairy2.getId()));
}

// Test that a create/delete don't dedup (collections creates new checkpoints)
TEST_F(CollectionsDcpTest, MB_26455) {
    const int items = 3;
    uint32_t m = 7;
    std::vector<CollectionEntry::Entry> dropped;

    {
        auto vb = store->getVBucket(vbid);

        CollectionsManifest cm;
        vb->updateFromManifest(makeManifest(cm));
        for (uint32_t n = 2; n < m; n++) {

            // add fruit (new generation), + 10 to use valid collection range
            vb->updateFromManifest(makeManifest(cm.add(
                    CollectionEntry::Entry{CollectionName::fruit, n + 10})));

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
                vb->updateFromManifest(makeManifest(cm.remove(dropped.back())));

                flush_vbucket_to_disk(vbid, 1);
            }
        }
    }

    resetEngineAndWarmup();

    // Stream again!
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, one create (create of fruit) and items of fruit and
    // every delete (tombstones)
    {
        SCOPED_TRACE("");
        testDcpCreateDelete(
                {CollectionEntry::Entry{CollectionName::fruit, (m - 1) + 10}},
                dropped,
                items,
                false /*fromMemory*/);
    }
    EXPECT_TRUE(
            store->getVBucket(vbid)->lockCollections().exists((m - 1) + 10));
}

// Test that create and delete (full deletion) keeps the collection drop marker
// as a tombstone
TEST_F(CollectionsDcpTest, test_dcp_create_delete_erase) {
    {
        VBucketPtr vb = store->getVBucket(vbid);
        // Create dairy
        CollectionsManifest cm(CollectionEntry::dairy);
        vb->updateFromManifest(makeManifest(cm));

        // Mutate dairy
        const int items = 3;
        for (int ii = 0; ii < items; ii++) {
            std::string key = "dairy:" + std::to_string(ii);
            store_item(
                    vbid, StoredDocKey{key, CollectionEntry::dairy}, "value");
        }

        // Delete dairy/create dairy in *one* update
        vb->updateFromManifest(
                makeManifest(cm.remove(CollectionEntry::dairy)
                                     .add(CollectionEntry::dairy2)));

        // Persist everything ready for warmup and check.
        // Flush the items + 1 delete (dairy) and 1 create (dairy2)
        flush_vbucket_to_disk(vbid, items + 2);

        // However DCP - Should see 2x create dairy and 1x delete dairy
        testDcpCreateDelete({CollectionEntry::dairy, CollectionEntry::dairy2},
                            {CollectionEntry::dairy},
                            items);

        runCompaction(vbid);
    }

    resetEngineAndWarmup();

    // Test against a different VB as our old replica will have data
    replicaVB++;
    createDcpObjects({{nullptr, 0}});

    // Streamed from disk, we won't see the first create or delete
    testDcpCreateDelete(
            {CollectionEntry::dairy2}, {CollectionEntry::dairy}, 0, false);

    EXPECT_TRUE(store->getVBucket(vbid)->lockCollections().exists(
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
        vb->updateFromManifest(makeManifest(cm));
        flush_vbucket_to_disk(vbid, 2);

        // remove the scope (and the collection)
        cm.remove(ScopeEntry::shop1);
        vb->updateFromManifest(makeManifest(cm));
        flush_vbucket_to_disk(vbid, 2);
    }

    resetEngineAndWarmup();

    createDcpObjects({{nullptr, 0}});

    testDcpCreateDelete(
            {}, {CollectionEntry::fruit}, 0, false, {}, {ScopeEntry::shop1});

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

// Drop the default collection, replicate it and expect it gone in all relevant
// locations.
TEST_P(CollectionsDcpParameterizedTest, test_dcp_drop_default) {
    VBucketPtr vb = store->getVBucket(vbid);

    CollectionsManifest cm;
    cm.remove(CollectionEntry::defaultC);
    vb->updateFromManifest(makeManifest(cm));

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionID::Default, producers->last_collection_id);

    EXPECT_FALSE(vb->lockCollections().exists(CollectionID::Default));
    EXPECT_FALSE(store->getVBucket(replicaVB)->lockCollections().exists(
            CollectionID::Default));

    flushVBucketToDiskIfPersistent(vbid, 1);
    flushVBucketToDiskIfPersistent(replicaVB, 1);

    if (persistent()) {
        auto [statusA, manifestA] =
                vb->getShard()->getRWUnderlying()->getCollectionsManifest(vbid);
        ASSERT_TRUE(statusA);

        VBucketPtr vbr = store->getVBucket(replicaVB);
        auto [statusR, manifestR] =
                vbr->getShard()->getRWUnderlying()->getCollectionsManifest(
                        replicaVB);
        ASSERT_TRUE(statusR);

        EXPECT_EQ(0, manifestA.collections.size());
        EXPECT_EQ(1, manifestA.scopes.size());
        EXPECT_TRUE(manifestA.droppedCollectionsExist);
        EXPECT_EQ(manifestA, manifestR);

        auto droppedA =
                vb->getShard()->getRWUnderlying()->getDroppedCollections(vbid);
        auto droppedR =
                vbr->getShard()->getRWUnderlying()->getDroppedCollections(
                        replicaVB);
        EXPECT_EQ(droppedA, droppedR);
    }
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
        vb->updateFromManifest(makeManifest(cm));
        store_item(vbid, StoredDocKey{"d_k1", CollectionEntry::defaultC}, "v");
        store_item(vbid, StoredDocKey{"d_k2", CollectionEntry::defaultC}, "v");
        flushVBucketToDiskIfPersistent(Vbid(0), 4);

        store_item(vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "v");
        store_item(vbid, StoredDocKey{"dairy", CollectionEntry::dairy}, "v");
        store_item(vbid, StoredDocKey{"k2", CollectionEntry::fruit}, "v");

        vb->updateFromManifest(makeManifest(cm.remove(CollectionEntry::fruit)));
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

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    // Record the snapshot we're processing
    auto ss = producers->last_snap_start_seqno;
    auto se = producers->last_snap_end_seqno;
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);

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
    ASSERT_EQ(cb::engine_errc::success,
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
        EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
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

        // Drop collection in a new snapshot
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());
        EXPECT_EQ(producers->last_system_event,
                  mcbp::systemevent::id::DeleteCollection);
        EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
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
void CollectionsDcpParameterizedTest::testVBPromotionUpdateManifest() {
    // Add fruit to the manifest
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);

    // This will update the active but NOT the replica.
    setCollections(cookie, cm);
    auto active = store->getVBucket(vbid);
    ASSERT_TRUE(active->lockCollections().exists(CollectionEntry::fruit));
    auto replica = store->getVBucket(replicaVB);
    ASSERT_FALSE(replica->lockCollections().exists(CollectionEntry::fruit));

    // Active is now aware of fruit, proving that we updated the manifest,
    // but replica is not. Change the state of replica to active to update
    // the replica manifest.
    store->setVBucketState(replicaVB, vbucket_state_active);
    EXPECT_TRUE(replica->lockCollections().exists(CollectionEntry::fruit));
}

TEST_P(CollectionsDcpParameterizedTest, vb_promotion_update_manifest_replica) {
    testVBPromotionUpdateManifest();
}

TEST_P(CollectionsDcpParameterizedTest, vb_promotion_update_manifest_pending) {
    store->setVBucketState(replicaVB, vbucket_state_pending);
    testVBPromotionUpdateManifest();
}

TEST_P(CollectionsDcpParameterizedTest, vb_promotion_update_manifest_dead) {
    store->setVBucketState(replicaVB, vbucket_state_dead);
    testVBPromotionUpdateManifest();
}

class CollectionsFilteredDcpErrorTest : public SingleThreadedKVBucketTest {
public:
    CollectionsFilteredDcpErrorTest()
        : cookieP(create_mock_cookie(engine.get())) {
    }
    void SetUp() override {
        config_string +=
                "collections_enabled=true;dcp_noop_mandatory_for_v5_"
                "features=false";
        SingleThreadedKVBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }

    void TearDown() override {
        destroy_mock_cookie(cookieP);
        producer.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

protected:
    std::shared_ptr<MockDcpProducer> producer;
    CookieIface* cookieP;
};

class CollectionsFilteredDcpTest : public CollectionsDcpTest {
public:
    CollectionsFilteredDcpTest() : CollectionsDcpTest() {
    }

    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        producers = std::make_unique<CollectionsDcpTestProducers>();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }
};

TEST_F(CollectionsFilteredDcpTest, filtering) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level and a delete of
    // _default (see MB-32131)
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::meat)
                           .add(CollectionEntry::dairy)
                           .remove(CollectionEntry::defaultC));

    // Setup filtered DCP for CID 12/0xc (dairy)
    createDcpObjects({{R"({"collections":["c"]})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createCollection
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
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
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    flush_vbucket_to_disk(vbid, 8);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"collections":["c"]})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations in the dairy collection
    {
        SCOPED_TRACE("");
        testDcpCreateDelete({CollectionEntry::dairy},
                            {},
                            2,
                            false,
                            {},
                            {},
                            /*compareManifests*/ false);
    }
}

TEST_F(CollectionsFilteredDcpTest, filtering_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::meat)
                           .add(ScopeEntry::shop1)
                           .add(CollectionEntry::dairy, ScopeEntry::shop1));
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    EXPECT_EQ(ScopeEntry::shop1.name, producers->last_key);

    // Uid is 0 because we only set the manifest uid for the last SystemEvent
    // generated in the manifest update. In this case this is for the meat
    // collection.
    EXPECT_EQ(0, producers->last_collection_manifest_uid);

    // SystemEvent createCollection dairy in shop1
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ(CollectionEntry::dairy.name, producers->last_key);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    // Uid is 0 because we only set the manifest uid for the last SystemEvent
    // generated in the manifest update. In this case this is for the meat
    // collection.
    EXPECT_EQ(0, producers->last_collection_manifest_uid);

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
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    flush_vbucket_to_disk(vbid, 8);

    vb.reset();

    // Now stream back from disk and check filtering
    resetEngineAndWarmup();

    createDcpObjects({{R"({"scope":"8"})"}});

    // Streamed from disk
    // 1x create - create of dairy
    // 2x mutations in the dairy collection
    // 1x scope drop
    testDcpCreateDelete({CollectionEntry::dairy},
                        {},
                        2,
                        false,
                        {ScopeEntry::shop1},
                        {},
                        /*compareManifests*/ false);
}

TEST_F(CollectionsFilteredDcpTest, filtering_grow_scope_from_empty) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Create the scope but don't add any collections to it
    CollectionsManifest cm;
    setCollections(cookie, cm.add(ScopeEntry::shop1));
    // Setup filtered DCP for SID 8 (shop1)
    try {
        createDcpObjects({{R"({"scope":"8"})"}});
    } catch (...) {
        FAIL() << "Error creating stream with empty scope";
    }

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now lets add a collection to the shop1 scope
    setCollections(cookie, cm.add(CollectionEntry::dairy, ScopeEntry::shop1));

    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    // SystemEvent createCollection
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    // And no more
    ASSERT_EQ(cb::engine_errc::would_block, producer->step(*producers));

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
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
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
    setCollections(cookie,
                   cm.add(CollectionEntry::meat)
                           .add(ScopeEntry::shop1)
                           .add(CollectionEntry::dairy, ScopeEntry::shop1));
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    // SystemEvent createCollection
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Add a new collection to the scope that we are filtering
    setCollections(cookie,
                   cm.add(CollectionEntry::vegetable, ScopeEntry::shop1));
    notifyAndStepToCheckpoint();

    // Check we got the system event
    ASSERT_EQ(cb::engine_errc::success, producer->step(*producers));
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
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }
    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

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
                        {ScopeEntry::shop1},
                        {},
                        /*compareManifests*/ false);
}

TEST_F(CollectionsFilteredDcpTest, filtering_shrink_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy(shop1)/vegetable(shop1) via the bucket
    // level
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::meat)
                           .add(ScopeEntry::shop1)
                           .add(CollectionEntry::dairy, ScopeEntry::shop1)
                           .add(CollectionEntry::vegetable, ScopeEntry::shop1));
    // Setup filtered DCP for SID 8 (shop1)
    createDcpObjects({{R"({"scope":"8"})"}});
    notifyAndStepToCheckpoint();

    // SystemEvent createScope
    ASSERT_EQ(cb::engine_errc::success, producer->step(*producers));
    ASSERT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    ASSERT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);

    // Check the collection create events are correct
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    ASSERT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
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
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::dairy.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }

    for (auto& key : vegetableKeys) {
        EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::vegetable.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(key, producers->last_key);
    }

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

    // Now delete the dairy collection
    setCollections(cookie,
                   cm.remove(CollectionEntry::dairy, ScopeEntry::shop1));
    notifyAndStepToCheckpoint();

    // Check we got the system event
    ASSERT_EQ(cb::engine_errc::success, producer->step(*producers));
    ASSERT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    ASSERT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    // Store more meat documents
    store_item(vbid, StoredDocKey{"meat:four", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat:five", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat:six", CollectionEntry::meat}, "value");

    // No new items (dairy is now filtered)
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

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
                        {ScopeEntry::shop1},
                        {},
                        /*compareManifests*/ false);
}

// Created for MB-32360
// Note this test does not resume a DCP stream, but a new stream from 0 hits
// the same issue, the filter doesn't know the collection was part of the scope
TEST_F(CollectionsFilteredDcpTest, collection_tombstone_on_scope_filter) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy(shop1)/vegetable(shop1) via the bucket
    // level
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::meat)
                           .add(ScopeEntry::shop1)
                           .add(CollectionEntry::dairy, ScopeEntry::shop1)
                           .add(CollectionEntry::vegetable, ScopeEntry::shop1));

    store_item(vbid, StoredDocKey{"dairy1", CollectionEntry::dairy}, "value");
    store_item(vbid, StoredDocKey{"dairy2", CollectionEntry::dairy}, "value");

    // Now delete the dairy collection
    setCollections(cookie,
                   cm.remove(CollectionEntry::dairy, ScopeEntry::shop1));

    // Flush it all out
    flush_vbucket_to_disk(vbid, 6);

    // Force collection purge. This is the crucial trigger behind MB-32360
    // Compaction runs and drops all items of dairy, when compaction gets to the
    // dairy deletion system event it triggers 'completeDelete', this is what
    // removes the dairy collection from KV meta-data and allowed the new DCP
    // stream to fail to replicate the collection-drop(dairy) event
    runCompaction(vbid);

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
                        {ScopeEntry::shop1},
                        {},
                        /*compareManifests*/ false);
}

// Test that we can stream-resume an interrupted snapshot where the rest of the
// snapshot is filtered away.
TEST_F(CollectionsFilteredDcpTest, MB_47009) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Create two collections
    CollectionsManifest cm;
    setCollections(
            cookie,
            cm.add(CollectionEntry::vegetable).add(CollectionEntry::fruit));
    flush_vbucket_to_disk(vbid, 2);

    // Write some items, sequenced though so that vegetable is fruit
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::vegetable}, "value");
    store_item(vbid, StoredDocKey{"k2", CollectionEntry::vegetable}, "value");
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "value");
    store_item(vbid, StoredDocKey{"k2", CollectionEntry::fruit}, "value");
    flush_vbucket_to_disk(vbid, 4);

    // Now DCP stream the vegetable collection only, but request as if we were
    // interrupted from the initial backfill, but have all of the collection.
    // I.e. as if our backfill looked like
    // snap{0, 6}
    // create vegetable{1}
    // mutation{3, k1}
    // mutation{4, k2}
    // seqno advance{6}
    // but we were interrupted after receiving seqno 4

    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    // Out stream request starts @4 but sets the snapshot as {0:6}
    uint64_t rollbackSeqno;
    ASSERT_EQ(cb::engine_errc::success,
              producer->streamRequest(
                      0,
                      1, // opaque
                      vbid,
                      4, // start_seqno
                      ~0ull, // end_seqno
                      vb->failovers->getLatestEntry().vb_uuid, // vbucket_uuid,
                      0, // snap_start_seqno,
                      6, // snap_end_seqno,
                      &rollbackSeqno,
                      [](const std::vector<vbucket_failover_t>&) {
                          return cb::engine_errc::success;
                      },
                      R"({"collections":["a"]})"));

    // Drive the stream and expect a seqno-advance to move us to the end of
    // the snapshot
    notifyAndStepToCheckpoint();
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced, producers->last_op);
    EXPECT_EQ(6, producers->last_byseqno);

    auto stream = producer->findStream(vbid);
    EXPECT_EQ(6, stream->getLastReadSeqno());

    // Then we can continue with other snapshots
    store_item(vbid, StoredDocKey{"k3", CollectionEntry::vegetable}, "value");
    store_item(vbid, StoredDocKey{"k4", CollectionEntry::vegetable}, "value");
    store_item(vbid, StoredDocKey{"k3", CollectionEntry::fruit}, "value");
    flush_vbucket_to_disk(vbid, 3);
    notifyAndStepToCheckpoint();
    EXPECT_EQ(7, producers->last_snap_start_seqno);
    EXPECT_EQ(8, producers->last_snap_end_seqno);

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ(7, producers->last_byseqno);
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ(8, producers->last_byseqno);
    // 9 is filtered out, but the stream read it
    EXPECT_EQ(9, stream->getLastReadSeqno());
}

// Test that a filtered stream-request is denied if the producer has sync-writes
// enabled
TEST_F(CollectionsFilteredDcpTest, MB_47009_deny_sync_writes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Create two collections
    CollectionsManifest cm;
    setCollections(
            cookie,
            cm.add(CollectionEntry::vegetable).add(CollectionEntry::fruit));
    flush_vbucket_to_disk(vbid, 2);

    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    EXPECT_EQ(cb::engine_errc::success,
              producer->control(0 /*opaque*/, "enable_sync_writes", "true"));

    uint64_t rollbackSeqno;
    EXPECT_EQ(cb::engine_errc::not_supported,
              producer->streamRequest(
                      0,
                      1, // opaque
                      vbid,
                      0, // start_seqno
                      ~0ull, // end_seqno
                      vb->failovers->getLatestEntry().vb_uuid, // vbucket_uuid,
                      0, // snap_start_seqno,
                      0, // snap_end_seqno,
                      &rollbackSeqno,
                      [](const std::vector<vbucket_failover_t>&) {
                          return cb::engine_errc::success;
                      },
                      R"({"collections":["a"]})"));
}

// Check that when filtering is on, we don't send snapshots for fully filtered
// snapshots
TEST_P(CollectionsDcpParameterizedTest, MB_24572) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat/dairy via the bucket level
    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));
    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["c"]})"}});

    // Store collection documents
    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "value");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "value");
    store_item(
            vbid, StoredDocKey{"meat::three", CollectionEntry::meat}, "value");

    notifyAndStepToCheckpoint();

    // SystemEvent createCollection for dairy is expected
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);

    // And no more for this stream - no meat
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));

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
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

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

    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("anykey", producers->last_key);

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::meat));

    // Setup filtered DCP
    createDcpObjects({{R"({"collections":["8"]})"}});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteCollection

    // Now step the producer to transfer the collection creation
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Perform a delete of meat via the bucket level
    setCollections(cookie, cm.remove(CollectionEntry::meat));

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the close stream
    EXPECT_EQ(cb::engine_errc::success, producer->step(*producers));

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, stream_closes_scope) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    cm.add(ScopeEntry::shop1);
    cm.add(CollectionEntry::meat, ScopeEntry::shop1);
    setCollections(cookie, cm);

    // Setup filtered DCP
    createDcpObjects({{R"({"scope":"8"})"}});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();

    // Now step DCP to transfer system events. We expect that the stream will
    // close once we transfer DeleteScope

    // Now step the producer to transfer the scope creation
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::CreateScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    // Create scope and the collection are in one update, only the final change
    // gets the manifest ID of 3, so create scope is 0
    EXPECT_EQ(0, producers->last_collection_manifest_uid);

    // SystemEvent createCollection
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ(CollectionEntry::meat.name, producers->last_key);
    // Final change of the update, moves to the new UID of 2
    EXPECT_EQ(2, producers->last_collection_manifest_uid);

    // Not dead yet...
    EXPECT_TRUE(vb0Stream->isActive());

    // Remove the scope
    setCollections(cookie, cm.remove(ScopeEntry::shop1));

    notifyAndStepToCheckpoint();

    // Now step the producer to transfer the collection deletion
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    // Drop scope triggers 2 changes, the drop collection doesn't expose the
    // new manifest yet.
    EXPECT_EQ(2, producers->last_collection_manifest_uid);

    // Now step the producer to transfer the scope deletion
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::DropScope, producers->last_system_event);
    EXPECT_EQ(ScopeEntry::shop1.getId(), producers->last_scope_id);
    // Now we are fully at the new manifest
    EXPECT_EQ(3, producers->last_collection_manifest_uid);

    // Done... collection deletion of meat has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // Now step the producer to transfer the stream end message
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpStreamEnd));
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::FilterEmpty,
              producers->last_end_status);

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

/**
 * Test that you cannot create a filter for closed collections
 */
TEST_P(CollectionsDcpParameterizedTest, empty_filter_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::meat));

    producer = createDcpProducer(cookieP, IncludeDeleteTime::No);
    createDcpConsumer();

    // Perform a delete of meat
    setCollections(cookie, cm.remove(CollectionEntry::meat));

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
        EXPECT_EQ(cb::engine_errc::unknown_collection,
                  cb::engine_errc(e.code().value()));
    }
}

TEST_P(CollectionsDcpParameterizedTest, legacy_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Perform a create of meat via the bucket level
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::meat));

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
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
    setCollections(cookie, cm.remove(CollectionEntry::defaultC));

    // Expect a stream end marker
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Ok, producers->last_end_status);

    // Done... collection deletion of default has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

// Because there is never an explicit create event for the default collection
// we can never purge it's tombstone.
TEST_P(CollectionsDcpParameterizedTest, DefaultCollectionDropped) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add 1 item so default is not empty, so purge runs and we can check the
    // tombstone remains
    store_item(vbid, StoredDocKey{"key", CollectionEntry::defaultC}, "value");

    // 1) Drop the default collection
    // 2) Add a new collection so that the drop event is not the high-seq
    CollectionsManifest cm;
    setCollections(cookie, cm.remove(CollectionEntry::defaultC));
    setCollections(cookie, cm.add(CollectionEntry::meat));
    flushVBucketToDiskIfPersistent(vbid, 3);

    TimeTraveller bill(
            engine->getConfiguration().getPersistentMetadataPurgeAge() + 1);
    runCollectionsEraser(vbid);

    // Clear everything from CP manager so DCP backfills - here we are streaming
    // the active VB which we have just ran tombstone purging on.
    vb->checkpointManager->clear();

    createDcpObjects(std::make_optional(std::string_view{}));

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false /*from-memory... false backfill*/);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::defaultC.getId(), producers->last_collection_id);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);

    flushVBucketToDiskIfPersistent(replicaVB, 2);

    TimeTraveller ted(
            engine->getConfiguration().getPersistentMetadataPurgeAge() + 1);
    // No items are transferred - the default collection is empty and no purge
    // is triggered.

    // Clear everything from CP manager so DCP backfills
    store->getVBucket(replicaVB)->checkpointManager->clear();
    producers->consumer = nullptr; // effectively stops faux 'replication'
    createDcpStream({std::string{}}, replicaVB, cb::engine_errc::success, 0);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpNoop);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false /*from-memory... false backfill*/);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::defaultC.getId(), producers->last_collection_id);
    EXPECT_EQ(producers->last_vbucket, replicaVB);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ(producers->last_vbucket, replicaVB);
}

class CollectionsDcpCloseAfterLosingPrivs
    : public CollectionsDcpParameterizedTest {
public:
    void SetUp() override {
        MockCookie::setCheckPrivilegeFunction({});
        mock_set_privilege_context_revision(0);
        CollectionsDcpParameterizedTest::SetUp();
    }
    void TearDown() override {
        MockCookie::setCheckPrivilegeFunction({});
        mock_set_privilege_context_revision(0);
        CollectionsDcpParameterizedTest::TearDown();
    }

    void setNoAccess(CollectionID noaccess) {
        MockCookie::setCheckPrivilegeFunction(
                [noaccess](const CookieIface&,
                           cb::rbac::Privilege priv,
                           std::optional<ScopeID> sid,
                           std::optional<CollectionID> cid)
                        -> cb::rbac::PrivilegeAccess {
                    if (cid && *cid == noaccess) {
                        return cb::rbac::PrivilegeAccessFail;
                    }
                    return cb::rbac::PrivilegeAccessOk;
                });
        mock_set_privilege_context_revision(1);
    }
};

TEST_P(CollectionsDcpCloseAfterLosingPrivs, collection_stream) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    createDcpObjects({{R"({"collections":["9"]})"}});
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    notifyAndStepToCheckpoint();
    // SystemEvent createCollection
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ(CollectionEntry::fruit.name, producers->last_key);

    // Lose access.
    setNoAccess(CollectionEntry::fruit.getId());

    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "green");

    // Stream goes straight to end
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::LostPrivileges,
              producers->last_end_status);

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
    // Done... loss of privs has closed stream
    EXPECT_FALSE(vb0Stream->isActive());
}

TEST_P(CollectionsDcpCloseAfterLosingPrivs, collection_stream_from_backfill) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    store_item(vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "green");
    store_item(vbid, StoredDocKey{"grape", CollectionEntry::fruit}, "red");
    flushVBucketToDiskIfPersistent(vbid, 3);
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());
    ensureDcpWillBackfill();

    // Stream request and then drop access
    createDcpObjects({{R"({"collections":["9"]})"}});

    setNoAccess(CollectionEntry::fruit.getId());

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd,
                              false /*in-memory = false*/);

    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::LostPrivileges,
              producers->last_end_status);
}

TEST_P(CollectionsDcpCloseAfterLosingPrivs, legacy_stream_closes) {
    VBucketPtr vb = store->getVBucket(vbid);

    store_item(vbid, StoredDocKey{"k", CollectionID::Default}, "v");

    // Make cookie look like a non-collection client
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(false);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(false);
    // Setup legacy DCP, it only receives default collection mutation/deletion
    // and should self-close if the default collection were to be deleted
    createDcpObjects({});

    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    EXPECT_TRUE(vb0Stream->isActive());

    setNoAccess(CollectionID::Default);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::Ok, producers->last_end_status);

    // Done... collection deletion of default has closed the stream
    EXPECT_FALSE(vb0Stream->isActive());

    // And no more
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, no_seqno_advanced_from_memory) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));
    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");

    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 4);
    }

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    // should be no more ops
    EXPECT_EQ(cb::engine_errc(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, no_seqno_advanced_from_memory_replica) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");

    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 4);
    }

    store->setVBucketState(vbid, vbucket_state_replica);
    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    // should be no more ops
    EXPECT_EQ(cb::engine_errc(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest,
       seqno_advanced_backfill_from_empty_disk_snapshot) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");

    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 4);
    }

    ensureDcpWillBackfill();

    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_byseqno.load(), 4);
}

class MB48010CollectionsDCPParamTest : public CollectionsDcpParameterizedTest {
public:
    void SetUp() override;
    /**
     * Allow for variation of the test, the test writes mutations 1 to 4.
     * The presence of 3 and 4 on the stream allows for different examples of
     * the monotonic violation.
     * @param partOfStream if true, mutation 3 will be written to the dairy
     *        collection and thus 'transmitted' on the stream.
     */
    void writeMutation3(bool partOfStream = false);
};

/**
 * Setup to test MB-48010 in which we test that a collection filtered stream
 * sends appropriate seqno advances if we are streaming from a replica that
 * had a partial disk checkpoint at the time of backfill creation that had been
 * partially expelled. We are also test that we send seqno advances as required
 * when a:
 *
 * 1) snapshot end with item in collection
 * 1) snapshot ends with different collection
 * 2) snapshot ends with prepare
 * 3) snapshot ends with prepare in different collection
 */
void MB48010CollectionsDCPParamTest::SetUp() {
    CollectionsDcpParameterizedTest::SetUp();

    // Destroys all, we don't want DCP until later (unless we're ephemeral and
    // have to jump through some hoops to get a backfill stream going...)
    if (isPersistent()) {
        resetEngineAndWarmup();
    }

    // Test requires a replica vbucket so we do the merging of disk/memry
    store->setVBucketState(vbid, vbucket_state_replica);
    VBucketPtr vb = store->getVBucket(vbid);

    // 1) Create a snapshot which we will populate as if DCP is sending messages
    // This snapshot will cover 0 to 5
    // It begins by creating two collections and receiving one mutation
    // All of that is flushed and then expel is used to discard in-memory items
    vb->checkpointManager->createSnapshot(0, 4, 0, CheckpointType::Disk, 4);

    uint64_t uid = 0;
    vb->replicaCreateCollection(Collections::ManifestUid(uid),
                                {ScopeID::Default, CollectionEntry::dairy},
                                "dairy",
                                {},
                                1);
    vb->replicaCreateCollection(Collections::ManifestUid(++uid),
                                {ScopeID::Default, CollectionEntry::fruit},
                                "fruit",
                                {},
                                2);

    // 2 collections written
    flushVBucketToDiskIfPersistent(vbid, 2);

    if (ephemeral()) {
        // Step the stream for the first two items, this is going to ensure that
        // expel can run by having a cursor in the checkpoint (if we had none
        // then we'd skip expel in favour of dropping the checkpoint).
        runCheckpointProcessor();
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                      cb::engine_errc::success);
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                      cb::engine_errc::success);
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                      cb::engine_errc::success);
    }

    // IMPORTANT: Run expel so that some flushed items are removed from memory
    // The DCP stream has to run a backfill for the snapshot
    auto expel = vb->checkpointManager->expelUnreferencedCheckpointItems();
    EXPECT_NE(0, expel.count);

    if (ephemeral()) {
        // Nuke the dcp stuff without restart - which would break the test for
        // ephemeral as the items would go away.
        teardown();
        cookieC = create_mock_cookie();
        cookieP = create_mock_cookie();
    }

    // Stream the dairy collection only
    createDcpObjects({{R"({"collections":["c"]})"}});
    producers->consumer = nullptr; // don't need the consumer
    auto vb0Stream = producer->findStream(Vbid(0));
    ASSERT_NE(nullptr, vb0Stream.get());

    // Now we drive the stream and finish the snapshot
    // First chunk of data comes from backfilling
    EXPECT_TRUE(vb0Stream->isBackfilling());
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(4, producers->last_snap_end_seqno);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);

    // At this point ActiveStream::readyQ is empty - the buggy code would use
    // that as part of the condition for sending a seqno-advance, yet it has
    // no bearing on the snapshot
    EXPECT_EQ(cb::engine_errc::would_block,
              producer->stepWithBorderGuard(*producers));
}

void MB48010CollectionsDCPParamTest::writeMutation3(bool partOfStream) {
    auto cid = partOfStream ? CollectionEntry::dairy.getId()
                            : CollectionEntry::fruit.getId();
    auto key = makeStoredDocKey("m3", cid);
    writeDocToReplica(vbid, key, 3, false);
    notifyAndRunToCheckpoint(*producer, *producers, true);
    if (partOfStream) {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                      cb::engine_errc::success);
    }
    // Nothing on stream
    EXPECT_EQ(cb::engine_errc::would_block,
              producer->stepWithBorderGuard(*producers));
}

TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_mutation) {
    writeMutation3();

    // Mutation 4 is also for the stream and is processed in a different run
    // of the checkpoint processor task
    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::dairy.getId()),
                      4,
                      false);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ(4, producers->last_byseqno);
}

TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_non_collection_mutation) {
    writeMutation3();

    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::fruit.getId()),
                      4,
                      false);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(4, producers->last_byseqno);
}

TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_prepare) {
    writeMutation3();

    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::dairy.getId()),
                      4,
                      true);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(4, producers->last_byseqno);
}

TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_non_collection_prepare) {
    writeMutation3();

    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::fruit.getId()),
                      4,
                      true);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(4, producers->last_byseqno);
}

// Another variant of MB48010.
// Mutation is part of the stream and incorrectly triggered a seqno-advance
// Mutation 4 then violated the monotonicity of the stream (due to the seqno
// advance which moved the stream to the end of the snapshot).
TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_mutation_v2) {
    writeMutation3(true);
    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::dairy.getId()),
                      4,
                      false);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ(4, producers->last_byseqno);
}

TEST_P(MB48010CollectionsDCPParamTest,
       replica_merged_snapshot_ends_on_non_collection_mutation_v2) {
    writeMutation3(true);
    writeDocToReplica(vbid,
                      makeStoredDocKey("m4", CollectionEntry::fruit.getId()),
                      4,
                      false);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(4, producers->last_byseqno);
}

TEST_P(CollectionsDcpParameterizedTest,
       seqno_advanced_backfill_from_empty_disk_snapshot_replica_due_deleted) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    StoredDocKey keyOne{"meat::one", CollectionEntry::meat};
    StoredDocKey keyTwo{"meat::two", CollectionEntry::meat};
    StoredDocKey keyThree{"dairy::one", CollectionEntry::dairy};
    store_item(vbid, keyOne, "pork");
    store_item(vbid, keyTwo, "beef");

    // 2 collections + 2 mutations
    flushVBucketToDiskIfPersistent(vbid, 4);

    delete_item(vbid, keyOne);
    delete_item(vbid, keyTwo);
    store_item(vbid, keyThree, "cheese");

    // 2 deletes
    flushVBucketToDiskIfPersistent(vbid, 3);

    // Purge tombstones
    if (persistent()) {
        runCompaction(vbid, 0, true);
    } else {
        auto* evb = dynamic_cast<EphemeralVBucket*>(vb.get());
        EphemeralVBucket::HTTombstonePurger purger(0);
        purger.setCurrentVBucket(*evb);
        evb->ht.visit(purger);
        EXPECT_EQ(purger.getVisitedCount(), 5);
        EXPECT_EQ(purger.getNumItemsMarkedStale(), 2);
        EXPECT_EQ(evb->purgeStaleItems(), 2);
    }

    store->setVBucketState(vbid, vbucket_state_replica);
    ensureDcpWillBackfill();

    // filter only CollectionEntry::meat
    createDcpObjects({{R"({"collections":["8"]})"}});

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_byseqno.load(), 7);
}

TEST_P(CollectionsDcpParameterizedTest,
       seqno_advanced_backfill_from_disk_snapshot) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(
            vbid, StoredDocKey{"dairy::one", CollectionEntry::dairy}, "milk");
    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");

    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 5);
    }

    ensureDcpWillBackfill();

    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_byseqno.load(), 5);
}

TEST_P(CollectionsDcpParameterizedTest,
       seqno_advanced_backfill_from_disk_snapshot_replica) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(
            vbid, StoredDocKey{"dairy::one", CollectionEntry::dairy}, "milk");
    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");

    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 5);
    }
    store->setVBucketState(vbid, vbucket_state_replica);
    ensureDcpWillBackfill();

    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}},
                     false,
                     DCP_ADD_STREAM_FLAG_DISKONLY);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_byseqno.load(), 5);
}

TEST_P(CollectionsDcpParameterizedTest,
       no_seqno_advanced_from_disk_to_memory_sync_rep) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(
            vbid, StoredDocKey{"dairy::one", CollectionEntry::dairy}, "dairy");
    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 4);
    }

    EXPECT_EQ(4, vb->getHighSeqno());

    ensureDcpWillBackfill();
    createDcpObjects("", false, 0, true);
    store_item(
            vbid, StoredDocKey{"dairy::two", CollectionEntry::dairy}, "dairy");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");
    store_item(vbid,
               StoredDocKey{"dairy::three", CollectionEntry::dairy},
               "dairy123");

    EXPECT_EQ(7, vb->getHighSeqno());

    // Push items into the ActiveStream::readyQ
    if (persistent()) {
        notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                                  false);
    } else {
        BackfillManager& bfm = producer->getBFM();
        ASSERT_EQ(1, bfm.getNumBackfills());
        while (bfm.backfill() != backfill_finished) {
        }
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                      cb::engine_errc::success);
    }

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    EXPECT_EQ(producers->last_key, "meat::one");
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_key, "dairy::one");

    // Persistent bucket has not persisted some mutations, so it gets another
    // marker for the a following Memory snapshot.
    // While Ephemeral just sends everything in the Backfill snapshot.
    if (persistent()) {
        notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    }

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_key, "dairy::two");
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    EXPECT_EQ(producers->last_key, "meat::two");
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    EXPECT_EQ(producers->last_key, "dairy::three");

    // should be no more ops
    EXPECT_EQ(cb::engine_errc(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_P(CollectionsDcpParameterizedTest, seqno_advanced_from_disk_to_memory) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie,
                   cm.add(CollectionEntry::meat).add(CollectionEntry::dairy));

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(
            vbid, StoredDocKey{"dairy::one", CollectionEntry::dairy}, "dairy");
    // 2 collections + 2 mutations
    if (persistent()) {
        flush_vbucket_to_disk(vbid, 4);
    }

    EXPECT_EQ(4, vb->getHighSeqno());

    ensureDcpWillBackfill();
    // filter only CollectionEntry::meat
    // Note: The stream will not support SyncRepl
    createDcpObjects({{R"({"collections":["8"]})"}});
    store_item(
            vbid, StoredDocKey{"dairy::two", CollectionEntry::dairy}, "dairy");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");
    store_item(vbid,
               StoredDocKey{"dairy::three", CollectionEntry::dairy},
               "dairy123");

    EXPECT_EQ(7, vb->getHighSeqno());

    // Push items into the ActiveStream::readyQ
    if (persistent()) {
        notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                                  false);
    } else {
        BackfillManager& bfm = producer->getBFM();
        ASSERT_EQ(1, bfm.getNumBackfills());
        while (bfm.backfill() != backfill_finished) {
        }
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                      cb::engine_errc::success);
    }

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    EXPECT_EQ(producers->last_key, "meat::one");

    // Persistent bucket has not persisted some mutations, so it gets a
    // SeqnoAdvance at the end of the backfill + another marker for the a
    // following Memory snapshot.
    if (persistent()) {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                      cb::engine_errc::success);
        EXPECT_EQ(producers->last_byseqno.load(), 4);

        notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    }

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::meat.getId());
    EXPECT_EQ(producers->last_key, "meat::two");

    if (ephemeral()) {
        // Ephemeral sends everything in the Backfill snapshot, so SeqnoAdvance
        // is at the end of that.
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                      cb::engine_errc::success);
        EXPECT_EQ(producers->last_byseqno.load(), 7);
    }

    // should be no more ops
    EXPECT_EQ(cb::engine_errc(cb::engine_errc::would_block),
              producer->step(*producers));
}

class CollectionsDcpPersistentOnly : public CollectionsDcpParameterizedTest {
public:
    void resurrectionTest(bool dropAtEnd, bool updateItemPath, bool deleteItem);
    void resurrectionStatsTest(bool reproduceUnderflow, bool updateItemDropped);
};

// Observed in MB-39864, the data we store in _local had a collection with
// id x in the open collection list twice, leading to failure of warmup. The
// issue occurred because a collection was recreated (cluster rolled back
// manifest state), KV flushed a drop{c1}/create{c1} together and the flusher
// de-duped the drop{c1} away so the meta-data became 'corrupt'.
// This test re-creates the events which lead to a underflow in stats and
// metadata corruption that lead to warmup failing.
void CollectionsDcpPersistentOnly::resurrectionTest(bool dropAtEnd,
                                                    bool updateItemPath,
                                                    bool deleteItem) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the fruit collection
    CollectionEntry::Entry target = CollectionEntry::fruit;
    CollectionsManifest cm(target);
    setCollections(cookie, cm);
    auto key = makeStoredDocKey("orange", target);
    // Put a key in for the original 'fruit'
    store_item(vbid, key, "yum");

    // Transfer the evant and item active to replica
    notifyAndStepToCheckpoint();
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, target.getId());
    EXPECT_EQ(producers->last_system_event,
              mcbp::systemevent::id::CreateCollection);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, target.getId());
    EXPECT_EQ(producers->last_key, "orange");

    // Flush both vbuckets
    flushVBucketToDiskIfPersistent(vbid, 2);
    flushVBucketToDiskIfPersistent(replicaVB, 2);

    // Now in 1 flush drop and add fruit, this simulates the manifest dropping
    // the collection, but going backwards/forwards 'resurrecting' the
    // collection. KV needs to be robust against such events.
    cm.remove(target);
    setCollections(cookie, cm);
    target.name += "a";
    cm.add(target);
    setCollections(cookie, cm);
    cm.remove(target);
    setCollections(cookie, cm);
    target.name += "b";
    cm.add(target);
    setCollections(cookie, cm);

    // Store one key in the last generation of the target collection, this
    // allows stats to be tested at the end.
    StoredDocKey lastGenKey;
    if (updateItemPath) {
        lastGenKey = makeStoredDocKey("orange", target);
    } else {
        lastGenKey = makeStoredDocKey("pear", target);
    }

    store_item(vbid,
               lastGenKey,
               "shaped",
               0 /*exptime*/,
               {cb::engine_errc::success},
               PROTOCOL_BINARY_DATATYPE_JSON,
               {},
               deleteItem);

    if (dropAtEnd) {
        cm.remove(target);
        setCollections(cookie, cm);
    }

    auto stepDelete = [this] {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                      cb::engine_errc::success);
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                      cb::engine_errc::success);
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());
        EXPECT_EQ(producers->last_system_event,
                  mcbp::systemevent::id::DeleteCollection);
    };
    auto stepCreate = [this] {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                      cb::engine_errc::success);
        stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                      cb::engine_errc::success);
        EXPECT_EQ(producers->last_collection_id,
                  CollectionEntry::fruit.getId());
        EXPECT_EQ(producers->last_system_event,
                  mcbp::systemevent::id::CreateCollection);
    };

    notifyAndRunToCheckpoint(*producer, *producers);

    stepDelete();
    stepCreate();
    stepDelete();
    stepCreate();

    auto messageType = cb::mcbp::ClientOpcode::DcpMutation;
    if (deleteItem) {
        messageType = cb::mcbp::ClientOpcode::DcpDeletion;
    }

    stepAndExpect(messageType, cb::engine_errc::success);

    if (dropAtEnd) {
        stepDelete();
    }

    // Many events were generated but only 1 is counted as flushed (because of
    // flusher de-dup). The second item flushed is the 'pear' key
    auto flusherDedupe = !store->getOneROUnderlying()
                                  ->getStorageProperties()
                                  .hasAutomaticDeduplication();
    auto expected = 1 + 1;
    if (!flusherDedupe) {
        expected += 3;

        if (dropAtEnd) {
            expected++;
        }
    }
    flushVBucketToDiskIfPersistent(vbid, expected);
    flushVBucketToDiskIfPersistent(replicaVB, expected);

    // With or without the de-dup bug, we cannot read the key from the dropped
    // collection because currently in-memory VB::Manifest has the correct
    // view of collections - and our key isn't part of any 'new' generation of
    // 'fruit'
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue gv = store->get(key, vbid, cookie, options);

    if (gv.getStatus() == cb::engine_errc::would_block) {
        runBGFetcherTask();
        gv = store->get(key, vbid, cookie, options);
    }

    auto expectedGetStatus = cb::engine_errc::no_such_key;
    if (dropAtEnd) {
        expectedGetStatus = cb::engine_errc::unknown_collection;
    } else if (updateItemPath && !deleteItem) {
        expectedGetStatus = cb::engine_errc::success;
    }

    EXPECT_EQ(expectedGetStatus, gv.getStatus());

    VBucketPtr rvb = store->getVBucket(replicaVB);
    EXPECT_EQ(rvb->getHighSeqno(), vb->getHighSeqno());
    // Now read back the persisted manifest(s) and validate that the fruit
    // collection exists.
    auto checkManifest = [dropAtEnd, &vb, &target](const auto& result) {
        auto status = result.first;
        auto m = result.second;

        ASSERT_TRUE(status);

        auto openSize = dropAtEnd ? 1 : 2;
        EXPECT_EQ(openSize, m.collections.size());
        EXPECT_TRUE(m.droppedCollectionsExist); // Always at least 1 drop

        for (const auto& entry : m.collections) {
            if (entry.metaData.cid == CollectionID::Default) {
                EXPECT_EQ(0, entry.startSeqno);
                EXPECT_EQ(ScopeID::Default, uint32_t(entry.metaData.sid));
                EXPECT_EQ("_default", entry.metaData.name);
            } else {
                EXPECT_EQ(2, openSize);
                EXPECT_EQ(target.getId(), entry.metaData.cid);
                EXPECT_EQ(ScopeID::Default, uint32_t(entry.metaData.sid));
                EXPECT_EQ(target.name, entry.metaData.name);
                EXPECT_EQ(vb->getHighSeqno() - 1, entry.startSeqno);
            }
        }
    };
    checkManifest(
            vb->getShard()->getRWUnderlying()->getCollectionsManifest(vbid));
    checkManifest(rvb->getShard()->getRWUnderlying()->getCollectionsManifest(
            replicaVB));

    // Now read back the persisted drop collection data and validate
    auto checkDropped =
            [dropAtEnd,
             &vb,
             &target](const std::pair<
                      bool,
                      std::vector<Collections::KVStore::DroppedCollection>>&
                              passed) {
                const auto& [status, dropped] = passed;

                EXPECT_TRUE(status);
                ASSERT_EQ(1, dropped.size());
                auto& entry = dropped[0];
                EXPECT_EQ(target.getId(), entry.collectionId);
                // In this test the first generation of target is always seqno:1
                EXPECT_EQ(1, entry.startSeqno);
                if (dropAtEnd) {
                    EXPECT_EQ(vb->getHighSeqno(), entry.endSeqno);
                } else {
                    EXPECT_EQ(vb->getHighSeqno() - 2, entry.endSeqno);
                }
            };

    checkDropped(
            vb->getShard()->getRWUnderlying()->getDroppedCollections(vbid));
    checkDropped(rvb->getShard()->getRWUnderlying()->getDroppedCollections(
            replicaVB));

    // Check value in VB before erasure
    expected = 2;
    if (updateItemPath) {
        expected = 1;
    }

    if (deleteItem) {
        expected--;
    }

    ASSERT_EQ(expected, vb->getNumItems());
    runEraser();

    expected = 1;
    if (deleteItem) {
        expected--;
    }

    auto checkKVS = [dropAtEnd, expected, &target, &cm](KVStoreIface& kvs,
                                                        Vbid id) {
        auto [status, dropped] = kvs.getDroppedCollections(id);
        ASSERT_TRUE(status);
        EXPECT_TRUE(dropped.empty());
        auto fileHandle = kvs.makeFileHandle(id);
        EXPECT_TRUE(fileHandle);

        if (dropAtEnd) {
            auto [status, stats] = kvs.getCollectionStats(id, target);
            EXPECT_EQ(KVStore::GetCollectionStatsStatus::NotFound, status);
            EXPECT_EQ(0, stats.itemCount);
            EXPECT_EQ(0, stats.highSeqno);
            EXPECT_EQ(0, stats.diskSize);
        } else {
            auto [status, stats] = kvs.getCollectionStats(id, target);
            EXPECT_EQ(KVStore::GetCollectionStatsStatus::Success, status);
            EXPECT_EQ(expected, stats.itemCount);
            EXPECT_EQ(7, stats.highSeqno);
        }

        auto uid = kvs.getCollectionsManifestUid(*fileHandle);
        EXPECT_TRUE(uid.has_value());
        EXPECT_EQ(cm.getUid(), uid.value());
    };

    auto* activeKVS = vb->getShard()->getRWUnderlying();
    ASSERT_TRUE(activeKVS);
    {
        SCOPED_TRACE("Active");
        checkKVS(*activeKVS, vbid);
    }

    auto* replicaKVS = rvb->getShard()->getRWUnderlying();
    ASSERT_TRUE(replicaKVS);
    {
        SCOPED_TRACE("Replica");
        checkKVS(*replicaKVS, replicaVB);
    }

    if (dropAtEnd) {
        EXPECT_EQ(0, vb->getNumItems());
    } else {
        EXPECT_EQ(expected, vb->getNumItems());
    }
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id) {
    resurrectionTest(false, false, false);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_update) {
    resurrectionTest(false, true, false);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_end_dropped) {
    resurrectionTest(true, false, false);
}

TEST_P(CollectionsDcpPersistentOnly,
       create_drop_create_same_id_end_dropped_update) {
    resurrectionTest(true, true, false);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_delete) {
    resurrectionTest(false, false, true);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_update_delete) {
    resurrectionTest(false, true, true);
}

TEST_P(CollectionsDcpPersistentOnly,
       create_drop_create_same_id_end_dropped_delete) {
    resurrectionTest(true, false, true);
}

TEST_P(CollectionsDcpPersistentOnly,
       create_drop_create_same_id_end_dropped_update_delete) {
    resurrectionTest(true, true, true);
}

void CollectionsDcpPersistentOnly::resurrectionStatsTest(
        bool reproduceUnderflow, bool updateItemDropped) {
    VBucketPtr vb = store->getVBucket(vbid);

    // Add the target collection
    CollectionEntry::Entry target = CollectionEntry::fruit;
    CollectionsManifest cm(target);
    setCollections(cookie, cm);
    auto key1 = makeStoredDocKey("orange", target);
    // Put a key in for the original 'fruit'
    store_item(vbid, key1, "yum1");
    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(2, vb->getPersistenceSeqno());
    EXPECT_EQ(2, vb->getHighSeqno());

    auto stats = vb->getManifest().lock(target.getId()).getPersistedStats();

    // Sizes are manually verified from dbdump and other manual checks
    // 57 for the value, 14 for the key and 18 for the v1 metadata
    // 14 for the value, 7 for the key and 18 for the v1 metadata
    size_t systemeventSize =
            57 + 14 + MetaData::getMetaDataSize(MetaData::Version::V1);
    size_t itemSize =
            14 + key1.size() + MetaData::getMetaDataSize(MetaData::Version::V1);
    if (isMagma()) {
        // magma doesn't account the same bits and bytes
        systemeventSize = 56;
        itemSize = 4;
    }
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(systemeventSize + itemSize, stats.diskSize);
    EXPECT_EQ(2, stats.highSeqno);

    // Next store a new item, drop the collection, create the collection and
    // flush. Before fixes for MB-39864 the 'apple' item, which belongs to the
    // first generation of the collection was accounted against the new
    // generation, the new collection would get an item count of 1, but you
    // cannot read back apple!
    auto key2 = makeStoredDocKey("apple", target);

    auto expectedToFlush = 1;

    auto flusherDedupe = !store->getOneROUnderlying()
                                  ->getStorageProperties()
                                  .hasAutomaticDeduplication();
    if (!flusherDedupe) {
        expectedToFlush++;
    }

    auto highSeqno = vb->getHighSeqno();
    if (!reproduceUnderflow) {
        expectedToFlush++;
        highSeqno++;
        store_item(vbid, key2, "yum1");
    }

    if (updateItemDropped) {
        expectedToFlush++;
        highSeqno++;
        store_item(vbid, key1, "yum2");
    }

    // remove
    cm.remove(target);
    highSeqno++;
    setCollections(cookie, cm);

    cm.add(target);
    setCollections(cookie, cm);
    highSeqno++;
    flushVBucketToDiskIfPersistent(vbid, expectedToFlush);

    stats = vb->getManifest().lock(target.getId()).getPersistedStats();
    // In both test variations the new collection has no items but some usage of
    // disk (system event is counted). Note 57 manually verified from dbdump
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(systemeventSize, stats.diskSize);
    EXPECT_EQ(highSeqno, stats.highSeqno);

    // Finally we should be able to mutate/delete the key we stored at the start
    // of the test. MB-39864 showed that this sequence of events could lead
    // underflow exceptions. The issue was that the store was treated as an
    // update, so we didn't increment the item count (so collection has 0 items)
    // the delete then triggers underflow.
    store_item(vbid, key1, "yummy");
    highSeqno++;
    flushVBucketToDiskIfPersistent(vbid, 1);
    stats = vb->getManifest().lock(target.getId()).getPersistedStats();

    // Note 15 manually verified from dbdump and is the item usage
    itemSize =
            15 + key1.size() + MetaData::getMetaDataSize(MetaData::Version::V1);
    if (isMagma()) {
        // magma doesn't account the same bits and bytes
        itemSize = 5;
    }
    EXPECT_EQ(1, stats.itemCount);
    EXPECT_EQ(systemeventSize + itemSize, stats.diskSize);
    EXPECT_EQ(highSeqno, stats.highSeqno);

    auto kvstore = store->getRWUnderlying(vbid);
    auto [status, diskStats] =
            kvstore->getCollectionStats(vbid, target.getId());
    ASSERT_EQ(status, KVStore::GetCollectionStatsStatus::Success);
    EXPECT_EQ(highSeqno, diskStats.highSeqno);
    EXPECT_EQ(systemeventSize + itemSize, diskStats.diskSize);
    EXPECT_EQ(1, diskStats.itemCount);

    delete_item(vbid, key1);
    itemSize = key1.size() + MetaData::getMetaDataSize(MetaData::Version::V1);
    if (isMagma()) {
        // magma doesn't account the remaining key/meta of the tombstone
        itemSize = 0;
    }
    flushVBucketToDiskIfPersistent(vbid, 1);
    stats = vb->getManifest().lock(target.getId()).getPersistedStats();
    EXPECT_EQ(0, stats.itemCount);
    EXPECT_EQ(systemeventSize + itemSize, stats.diskSize);
    highSeqno++;
    EXPECT_EQ(highSeqno, stats.highSeqno);

    std::tie(status, diskStats) =
            kvstore->getCollectionStats(vbid, target.getId());
    EXPECT_EQ(highSeqno, diskStats.highSeqno);
    EXPECT_EQ(systemeventSize + itemSize, diskStats.diskSize);
    EXPECT_EQ(0, diskStats.itemCount);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_stats) {
    resurrectionStatsTest(false /*reproduceUnferflow*/, false /*updateItem*/);
}

// reproduce the underflow seen in MB-39864
TEST_P(CollectionsDcpPersistentOnly,
       create_drop_create_same_id_stats_undeflow) {
    resurrectionStatsTest(true /*reproduceUnferflow*/, false /*updateItem*/);
}

TEST_P(CollectionsDcpPersistentOnly, create_drop_create_same_id_stats_update) {
    resurrectionStatsTest(false /*reproduceUnferflow*/, true /*updateItem*/);
}

TEST_P(CollectionsDcpPersistentOnly,
       create_drop_create_same_id_stats_underflow_update) {
    resurrectionStatsTest(true /*reproduceUnferflow*/, true /*updateItem*/);
}

// Demonstrate we can detect a 'split' in the collection state (i.e. cluster
// partition). The only expected split is the following scenario.
// 1) A majority of the cluster decides to drop a collection
// 2) The drop is replicated to a node not in this majority
// 3) The majority of nodes are removed, the dropped collection comes back
// This test simulates that single vbucket, a replica is told to drop
// a collection, which then comes back.
TEST_P(CollectionsDcpParameterizedTest, replica_active_state_diverge) {
    // Set the manifest with the fruit collection
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);

    // Make the vb replica
    store->setVBucketState(vbid, vbucket_state_replica);

    // Now drive changes as a replica, and drop fruit
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0, vbid, /*flags*/ 0));

    std::string collection = "fruit";
    CollectionID cid = CollectionEntry::fruit.getId();
    Collections::ManifestUid manifestUid(cm.getUid());
    Collections::DropEventData dropEventData{
            manifestUid, ScopeID::Default, cid};
    Collections::DropEventDcpData dropEventDcpData{dropEventData};

    ASSERT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(/*opaque*/ 2,
                                       vbid,
                                       /*start_seqno*/ 0,
                                       /*end_seqno*/ 100,
                                       /*flags*/ 0,
                                       /*HCS*/ {},
                                       /*maxVisibleSeqno*/ {}));

    VBucketPtr vb = store->getVBucket(vbid);

    // Call the consumer function for handling DCP events
    // drop the fruit collection
    EXPECT_EQ(cb::engine_errc::success,
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

    // Collection gone
    EXPECT_FALSE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"apple", CollectionEntry::fruit}));

    // cluster manager is going to 'boost' the manifest-ID during quorum loss
    // some fixed jump - 1000 discussed, so 1000 it is.
    cm.updateUid(cm.getUid() + 1000);
    setCollections(cookie, cm);

    // Change state and test that the vbucket is set to equal the current node
    EXPECT_EQ(cb::engine_errc::success,
              store->setVBucketState(vbid, vbucket_state_active));

    // Collection still exists
    EXPECT_TRUE(vb->lockCollections().doesKeyContainValidCollection(
            StoredDocKey{"apple", CollectionEntry::fruit}));

}

// Test for MB-47753. In this case a filtered DCP stream has its cursor dropped.
// Before the cursor drop, the client is up-to-date with their filtered
// collection, and last received a seqno-advance to move them to the end of
// the snapshot. After cursor dropping before the fix for this issue a new
// backfill was scheduled, that returns nothing and forces a new seqno-advance.
// With the fix in place, the second backfill doesn't occur and no duplicate
// message is seen.
TEST_F(CollectionsFilteredDcpTest, MB_47753) {
    // Only valid for persistent
    if (!isPersistent()) {
        return;
    }

    // Store an item to a new collection and create a stream (which backfills)
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm;
    setCollections(cookie, cm.add(CollectionEntry::fruit));
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::fruit}, "v1");
    flush_vbucket_to_disk(vbid, 2);

    ensureDcpWillBackfill();

    createDcpObjects({{R"({"collections":["9"]})"}});

    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto* as = static_cast<ActiveStream*>(stream.get());

    // Manually drive the backfill so stream can drop cursor whilst running
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    EXPECT_TRUE(as->isBackfilling());

    // Store a new item and flush it, to a different collection, clear the
    // checkpoint so we cannot get a cursor for this item.
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::defaultC}, "v1");
    flush_vbucket_to_disk(vbid, 1);
    vb->checkpointManager->clear();

    // Drop the streams cursor and run the first backfill (this backfill
    // is the one which was scheduled as part of stream creation).
    as->handleSlowStream();

    // backfill:create()
    runNextTask(lpAuxioQ);

    // backfill:scan()
    runNextTask(lpAuxioQ);

    // backfill:finished()
    runNextTask(lpAuxioQ);

    // The initial snapshot is sent to the client
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(0, producers->last_snap_start_seqno);
    EXPECT_EQ(3, producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);
    EXPECT_EQ(1, producers->last_byseqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
    EXPECT_EQ(2, producers->last_byseqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    EXPECT_EQ(3, producers->last_byseqno);

    // And this stream produces no more, prior to fixing MB-47753 a second
    // backfill produced an empty snapshot
    EXPECT_TRUE(as->isInMemory());
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(*producers));
}

// Test cases which run for persistent and ephemeral buckets
INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         CollectionsDcpParameterizedTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         CollectionsDcpCloseAfterLosingPrivs,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         CollectionsDcpPersistentOnly,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsDcpEphemeralOrPersistent,
                         MB48010CollectionsDCPParamTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
