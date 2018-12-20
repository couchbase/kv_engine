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

#include "tests/module_tests/collections/collections_dcp_test.h"
#include "checkpoint_manager.h"
#include "programs/engine_testapp/mock_server.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"

CollectionsDcpTest::CollectionsDcpTest()
    : cookieC(create_mock_cookie()), cookieP(create_mock_cookie()) {
    mock_set_collections_support(cookieP, true);
    mock_set_collections_support(cookieC, true);
    replicaVB = Vbid(1);
}

// Setup a producer/consumer ready for the test
void CollectionsDcpTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
    internalSetUp();
}

void CollectionsDcpTest::internalSetUp() {
    // Start vbucket as active to allow us to store items directly to it.
    store->setVBucketState(vbid, vbucket_state_active, false);
    producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
    createDcpObjects({{}} /*collections on, but no filter*/);
}

Collections::VB::PersistedManifest CollectionsDcpTest::getManifest(
        Vbid vb) const {
    return store->getVBucket(vb)
            ->getShard()
            ->getRWUnderlying()
            ->getCollectionsManifest(vbid);
}

void CollectionsDcpTest::createDcpStream(
        boost::optional<cb::const_char_buffer> collections,
        Vbid id,
        cb::engine_errc expectedError) {
    uint64_t rollbackSeqno;
    ASSERT_EQ(ENGINE_ERROR_CODE(expectedError),
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      id,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      collections));
}

void CollectionsDcpTest::createDcpConsumer() {
    consumer = std::make_shared<MockDcpConsumer>(
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

void CollectionsDcpTest::createDcpObjects(
        boost::optional<cb::const_char_buffer> collections) {
    createDcpConsumer();
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);
    // Give the producers object access to the consumer and vbid of replica
    producers->consumer = consumer.get();
    producers->replicaVB = replicaVB;

    createDcpStream(collections);
}

void CollectionsDcpTest::TearDown() {
    teardown();
    SingleThreadedKVBucketTest::TearDown();
}

void CollectionsDcpTest::teardown() {
    destroy_mock_cookie(cookieC);
    destroy_mock_cookie(cookieP);
    if (consumer) {
        consumer->closeAllStreams();
        consumer->cancelTask();
    }
    if (producer) {
        producer->closeAllStreams();
        producer->cancelCheckpointCreatorTask();
    }
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
    EXPECT_EQ(ENGINE_ERROR_CODE(err),
              producer->stepAndExpect(producers.get(), opcode));
}

void CollectionsDcpTest::testDcpCreateDelete(
        const std::vector<CollectionEntry::Entry>& expectedCreates,
        const std::vector<CollectionEntry::Entry>& expectedDeletes,
        int expectedMutations,
        bool fromMemory,
        const std::vector<ScopeEntry::Entry>& expectedScopeCreates,
        const std::vector<ScopeEntry::Entry>& expectedScopeDrops) {
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              fromMemory);

    int mutations = 0;

    auto createItr = expectedCreates.begin();
    auto deleteItr = expectedDeletes.begin();
    auto scopeCreateItr = expectedScopeCreates.begin();
    auto scopeDropItr = expectedScopeDrops.begin();

    // step until done
    while (producer->step(producers.get()) == ENGINE_SUCCESS) {
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

    // Finally check that the active and replica have the same manifest, our
    // BeginDeleteCollection should of contained enough information to form
    // an equivalent manifest
    EXPECT_EQ(getManifest(vbid), getManifest(Vbid(vbid.get() + 1)));
}

void CollectionsDcpTest::resetEngineAndWarmup(std::string new_config) {
    teardown();
    SingleThreadedKVBucketTest::resetEngineAndWarmup(new_config);
    producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
    cookieC = create_mock_cookie();
    cookieP = create_mock_cookie();
}

void CollectionsDcpTest::ensureDcpWillBackfill() {
    // Wipe the checkpoint out of the 'source' VB, so any DCP work has to
    // go back to backfilling (i.e. cannot resume from checkpoint manager)
    VBucketPtr vb = store->getVBucket(vbid);
    bool newCp = true;
    vb->checkpointManager->removeClosedUnrefCheckpoints(*vb, newCp);

    // Move DCP to a new vbucket so that we can replay history from 0
    // without having to wind back vbid(1)
    replicaVB = Vbid(2);
}

/*
 * DCP callback method to push SystemEvents on to the consumer
 */
ENGINE_ERROR_CODE CollectionsDcpTestProducers::system_event(
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData,
        cb::mcbp::DcpStreamId sid) {
    (void)vbucket; // ignored as we are connecting VBn to VBn+1
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSystemEvent;
    last_system_event = event;
    // Validate the provided parameters
    cb::mcbp::request::DcpSystemEventPayload extras(bySeqno, event, version);
    EXPECT_TRUE(extras.isValidVersion());
    EXPECT_TRUE(extras.isValidEvent());
    last_system_event_data.insert(
            last_system_event_data.begin(), eventData.begin(), eventData.end());
    last_system_event_version = version;
    last_stream_id = sid;

    switch (event) {
    case mcbp::systemevent::id::CreateCollection: {
        last_collection_id =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        eventData.data())
                        ->cid.to_host();
        if (version == mcbp::systemevent::version::version0) {
            // Using the ::size directly in the EXPECT is failing to link on
            // OSX build, but copying the value works.
            const auto expectedSize = Collections::CreateEventDcpData::size;
            EXPECT_EQ(expectedSize, eventData.size());
            const auto* ev =
                    reinterpret_cast<const Collections::CreateEventDcpData*>(
                            eventData.data());
            last_collection_id = ev->cid.to_host();
            last_scope_id = ev->sid.to_host();
            last_collection_manifest_uid = ev->manifestUid.to_host();
        } else {
            const auto expectedSize =
                    Collections::CreateWithMaxTtlEventDcpData::size;
            EXPECT_EQ(expectedSize, eventData.size());
            const auto* ev = reinterpret_cast<
                    const Collections::CreateWithMaxTtlEventDcpData*>(
                    eventData.data());
            last_collection_id = ev->cid.to_host();
            last_scope_id = ev->sid.to_host();
            last_collection_manifest_uid = ev->manifestUid.to_host();
        }

        last_key.assign(reinterpret_cast<const char*>(key.data()), key.size());
        break;
    }
    case mcbp::systemevent::id::DeleteCollection: {
        const auto* ev = reinterpret_cast<const Collections::DropEventDcpData*>(
                eventData.data());
        last_collection_id = ev->cid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();
        // Using the ::size directly in the EXPECT is failing to link on
        // OSX build, but copying the value works.
        const auto expectedSize = Collections::DropEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        EXPECT_EQ(nullptr, key.data());
        break;
    }
    case mcbp::systemevent::id::CreateScope: {
        const auto* ev =
                reinterpret_cast<const Collections::CreateScopeEventDcpData*>(
                        eventData.data());
        last_scope_id = ev->sid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();

        const auto expectedSize = Collections::CreateScopeEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        last_key.assign(reinterpret_cast<const char*>(key.data()), key.size());
        break;
    }
    case mcbp::systemevent::id::DropScope: {
        const auto* ev =
                reinterpret_cast<const Collections::DropScopeEventDcpData*>(
                        eventData.data());
        last_scope_id = ev->sid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();

        const auto expectedSize = Collections::DropScopeEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        EXPECT_EQ(nullptr, key.data());
        break;
    }
    default:
        EXPECT_TRUE(false) << "Unsupported event " << int(event);
    }

    if (consumer) {
        auto rv = consumer->systemEvent(
                opaque, replicaVB, event, bySeqno, version, key, eventData);
        EXPECT_EQ(ENGINE_SUCCESS, rv)
                << "Failure to push system-event onto the consumer";
        return rv;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE CollectionsDcpTest::dcpAddFailoverLog(
        vbucket_failover_t* entry,
        size_t nentries,
        gsl::not_null<const void*> cookie) {
    return ENGINE_SUCCESS;
}
