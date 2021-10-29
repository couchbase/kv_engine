/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "tests/module_tests/collections/collections_dcp_test.h"
#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "ep_bucket.h"
#include "ephemeral_bucket.h"
#include "ephemeral_vb.h"

#include "kv_bucket.h"
#include "kvstore/kvstore.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_conn_map.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

#include <utilities/test_manifest.h>

CollectionsDcpTest::CollectionsDcpTest()
    : cookieC(create_mock_cookie(engine.get())),
      cookieP(create_mock_cookie(engine.get())) {
    cookie_to_mock_cookie(cookieP)->setCollectionsSupport(true);
    cookie_to_mock_cookie(cookieC)->setCollectionsSupport(true);
    replicaVB = Vbid(1);
}

// Setup a producer/consumer ready for the test
void CollectionsDcpTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
    internalSetUp();
}

void CollectionsDcpTest::internalSetUp() {
    // Start vbucket as active to allow us to store items directly to it.
    store->setVBucketState(vbid, vbucket_state_active);
    producers = std::make_unique<CollectionsDcpTestProducers>();
    createDcpObjects(std::make_optional(
            std::string_view{}) /*collections on, but no filter*/);
}

Collections::KVStore::Manifest CollectionsDcpTest::getPersistedManifest(
        Vbid vb) const {
    auto [status, persistedManifest] = store->getVBucket(vb)
                                               ->getShard()
                                               ->getRWUnderlying()
                                               ->getCollectionsManifest(vb);
    EXPECT_TRUE(status);
    return persistedManifest;
}

void CollectionsDcpTest::createDcpStream(
        std::optional<std::string_view> collections,
        Vbid id,
        cb::engine_errc expectedError,
        uint32_t flags) {
    uint64_t rollbackSeqno;
    ASSERT_EQ(cb::engine_errc(expectedError),
              producer->streamRequest(
                      flags,
                      1, // opaque
                      id,
                      0, // start_seqno
                      ~0ull, // end_seqno
                      0, // vbucket_uuid,
                      0, // snap_start_seqno,
                      0, // snap_end_seqno,
                      &rollbackSeqno,
                      [](const std::vector<vbucket_failover_t>&) {
                          return cb::engine_errc::success;
                      },
                      collections));
}

void CollectionsDcpTest::createDcpConsumer() {
    // Nuke the old consumer (if it exists) to ensure that we remove any
    // streams from the vbToConns map or we will end up firing assertions
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    if (consumer) {
        mockConnMap.removeConn(consumer->getCookie());
        engine->releaseCookie(cookieC);
        cookieC = create_mock_cookie();
    }

    consumer = std::make_shared<MockDcpConsumer>(
            *engine, cookieC, "test_consumer");
    mockConnMap.addConn(cookieC, consumer);

    store->setVBucketState(replicaVB, vbucket_state_replica);
    ASSERT_EQ(cb::engine_errc::success,
              consumer->addStream(/*opaque*/ 0,
                                  replicaVB,
                                  /*flags*/ 0));
}

void CollectionsDcpTest::createDcpObjects(
        std::optional<std::string_view> collections,
        bool enableOutOfOrderSnapshots,
        uint32_t flags,
        bool enableSyncRep) {
    createDcpConsumer();
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    // Give the producers object access to the consumer and vbid of replica
    producers->consumer = consumer.get();
    producers->replicaVB = replicaVB;

    if (enableOutOfOrderSnapshots) {
        // The CollectionsDcpProducer by default tries to pass messages to the
        // replica which won't work with OSO. No consumer = no replication
        producers->consumer = nullptr;
        producer->enableOutOfOrderSnapshots();
    }

    if (enableSyncRep) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(1, "enable_sync_writes", "true"));
        EXPECT_EQ(cb::engine_errc::success,
                  producer->control(1, "consumer_name", "mock_replication"));
    }

    createDcpStream(collections, vbid, cb::engine_errc::success, flags);
}

void CollectionsDcpTest::TearDown() {
    teardown();
    SingleThreadedKVBucketTest::TearDown();
}

void CollectionsDcpTest::teardown() {
    if (consumer) {
        consumer->closeAllStreams();
        consumer->cancelTask();
    }
    if (producer) {
        producer->closeAllStreams();
        producer->cancelCheckpointCreatorTask();
    }
    auto& mockConnMap = static_cast<MockDcpConnMap&>(engine->getDcpConnMap());
    mockConnMap.removeConn(cookieC);
    mockConnMap.removeConn(cookieP);
    destroy_mock_cookie(cookieC);
    destroy_mock_cookie(cookieP);
    producer.reset();
    consumer.reset();
}

void CollectionsDcpTest::runCheckpointProcessor() {
    SingleThreadedKVBucketTest::runCheckpointProcessor(*producer, *producers);
}

void CollectionsDcpTest::notifyAndStepToCheckpoint(
        cb::mcbp::ClientOpcode expectedOp, bool fromMemory) {
    // Call parent class function with our producer
    SingleThreadedKVBucketTest::notifyAndStepToCheckpoint(
            *producer, *producers, expectedOp, fromMemory);
}

void CollectionsDcpTest::stepAndExpect(cb::mcbp::ClientOpcode opcode,
                                       cb::engine_errc err) {
    EXPECT_EQ(cb::engine_errc(err),
              producer->stepAndExpect(*producers, opcode));
}

void CollectionsDcpTest::testDcpCreateDelete(
        const std::vector<CollectionEntry::Entry>& expectedCreates,
        const std::vector<CollectionEntry::Entry>& expectedDeletes,
        int expectedMutations,
        bool fromMemory,
        const std::vector<ScopeEntry::Entry>& expectedScopeCreates,
        const std::vector<ScopeEntry::Entry>& expectedScopeDrops,
        bool compareManifests) {
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              fromMemory);

    // Expect nothing outstanding before we start
    flushVBucketToDiskIfPersistent(replicaVB, 0);
    int mutations = 0;

    auto createItr = expectedCreates.begin();
    auto deleteItr = expectedDeletes.begin();
    auto scopeCreateItr = expectedScopeCreates.begin();
    auto scopeDropItr = expectedScopeDrops.begin();

    // step until done
    while (producer->step(*producers) == cb::engine_errc::success) {
        if (producers->last_op == cb::mcbp::ClientOpcode::DcpSystemEvent) {
            switch (producers->last_system_event) {
            case mcbp::systemevent::id::CreateCollection:
                ASSERT_NE(createItr, expectedCreates.end())
                        << "Found a create collection, but expected vector is "
                           "now at the end";
                EXPECT_EQ((*createItr).name, producers->last_key);
                EXPECT_EQ((*createItr).uid, producers->last_collection_id);

                createItr++;
                break;
            case mcbp::systemevent::id::DeleteCollection:
                ASSERT_NE(deleteItr, expectedDeletes.end())
                        << "Found a drop collection, but expected vector is "
                           "now at the end";

                EXPECT_EQ((*deleteItr).uid, producers->last_collection_id);
                deleteItr++;
                break;
            case mcbp::systemevent::id::CreateScope:
                ASSERT_NE(scopeCreateItr, expectedScopeCreates.end())
                        << "Found a create scope, but expected vector is "
                           "now at the end";

                EXPECT_EQ((*scopeCreateItr).name, producers->last_key);
                EXPECT_EQ((*scopeCreateItr).uid, producers->last_scope_id);

                scopeCreateItr++;
                break;
            case mcbp::systemevent::id::DropScope:
                ASSERT_NE(scopeDropItr, expectedScopeDrops.end())
                        << "Found a drop scope, but expected vector is "
                           "now at the end";
                EXPECT_EQ((*scopeDropItr).uid, producers->last_scope_id);
                scopeDropItr++;
                break;
            default:
                throw std::logic_error(
                        "CollectionsDcpTest::testDcpCreateDelete unknown "
                        "event:" +
                        std::to_string(int(producers->last_system_event)));
            }
        } else if (producers->last_op == cb::mcbp::ClientOpcode::DcpMutation) {
            mutations++;
        }
    }

    EXPECT_EQ(createItr, expectedCreates.end());
    EXPECT_EQ(deleteItr, expectedDeletes.end());
    EXPECT_EQ(expectedMutations, mutations);

    if (isPersistent()) {
        // Flush everything. If we don't then we can't compare persisted
        // manifests. Don't know how much we will have to flush as the flusher
        // will de-dupe creates and deletes of collections in the same batch
        const auto res =
                dynamic_cast<EPBucket&>(*store).flushVBucket(replicaVB);
        EXPECT_EQ(EPBucket::MoreAvailable::No, res.moreAvailable);

        // Finally check that the active and replica have the same manifest, our
        // BeginDeleteCollection should of contained enough information to form
        // an equivalent manifest
        auto m1 = getPersistedManifest(vbid);
        auto m2 = getPersistedManifest(replicaVB);

        // Manifest uid should always be less than or equal to the correct
        // active manifest uid. We may have filtered a system event with a
        // manifest uid update.
        EXPECT_GE(m1.manifestUid, m2.manifestUid);

        // We may not want to compare the actual manifests if we are testing
        // filtered DCP
        if (compareManifests) {
            // For the tests that compare manifests entirely, we should also
            // always have the correct uid.
            EXPECT_EQ(m1.manifestUid, m2.manifestUid);
            EXPECT_EQ(m1.collections, m2.collections);
            EXPECT_EQ(m1.scopes, m2.scopes);
        }
    }
}

void CollectionsDcpTest::resetEngineAndWarmup(std::string new_config) {
    teardown();
    SingleThreadedKVBucketTest::resetEngineAndWarmup(new_config);
    producers = std::make_unique<CollectionsDcpTestProducers>();
    cookieC = create_mock_cookie(engine.get());
    cookieP = create_mock_cookie(engine.get());
}

void CollectionsDcpTest::ensureDcpWillBackfill() {
    // Wipe the checkpoint out of the 'source' VB, so any DCP work has to
    // go back to backfilling (i.e. cannot resume from checkpoint manager)
    VBucketPtr vb = store->getVBucket(vbid);
    vb->checkpointManager->clear();

    // Move DCP to a new vbucket so that we can replay history from 0
    // without having to wind back vbid(1)
    replicaVB++;
}

void CollectionsDcpTest::runEraser() {
    {
        SCOPED_TRACE("CollectionsDcpTest::runEraser - active");
        runCollectionsEraser(vbid);
    }

    // Only run on the replica for persistent buckets as the ephemeral task
    // will iterate over the vbuckets, so has already hit vbid and replicaVB
    // in the first runEraser above
    if (isPersistent()) {
        SCOPED_TRACE("CollectionsDcpTest::runEraser - replica");
        runCollectionsEraser(replicaVB);
    }
}

cb::engine_errc CollectionsDcpTest::dcpAddFailoverLog(
        const std::vector<vbucket_failover_t>&) {
    return cb::engine_errc::success;
}

void CollectionsDcpTest::createScopeOnConsumer(Vbid id,
                                               uint32_t opaque,
                                               Collections::ManifestUid muid,
                                               const ScopeEntry::Entry& entry,
                                               uint64_t seqno) {
    Collections::CreateScopeEventData createEventData{
            muid, {entry.getId(), entry.name}};
    Collections::CreateScopeEventDcpData createEventDcpData{createEventData};
    // Call the consumer function for handling DCP events
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      opaque,
                      id,
                      mcbp::systemevent::id::CreateScope,
                      /*seqno*/ seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(entry.name.data()),
                       entry.name.size()},
                      {reinterpret_cast<const uint8_t*>(&createEventDcpData),
                       Collections::CreateScopeEventDcpData::size}));
}

void CollectionsDcpTest::createCollectionOnConsumer(
        Vbid id,
        uint32_t opaque,
        Collections::ManifestUid muid,
        ScopeID sid,
        const CollectionEntry::Entry& entry,
        uint64_t seqno) {
    Collections::CreateEventData createEventData{
            muid, {sid, entry.getId(), entry.name, {/*no ttl*/}}};
    Collections::CreateEventDcpData createEventDcpData{createEventData};
    // Call the consumer function for handling DCP events
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      opaque,
                      id,
                      mcbp::systemevent::id::CreateCollection,
                      /*seqno*/ seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(entry.name.data()),
                       entry.name.size()},
                      {reinterpret_cast<const uint8_t*>(&createEventDcpData),
                       Collections::CreateEventDcpData::size}));
}
