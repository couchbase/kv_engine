/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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
#include "tests/module_tests/collections/evp_store_durability_collections_dcp_test.h"

#include "dcp/response.h"
#include "durability/active_durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_stream.h"
#include "tests/module_tests/test_helpers.h"
#include "tests/module_tests/vbucket_utils.h"
#include <utilities/test_manifest.h>

#include <engines/ep/tests/ep_test_apis.h>

void CollectionsSyncWriteParamTest::SetUp() {
    CollectionsDcpParameterizedTest::SetUp();

    producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
    producers->replicaVB = replicaVB;
    producers->consumer = consumer.get();

    // Need to recreate our stream with SyncRepl enabled
    producer->closeAllStreams();
    producer->setSyncReplication(SyncReplication::SyncReplication);

    uint64_t rollbackSeqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      0, // vbucket_uuid,
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      std::make_optional(
                                              std::string_view{})
                      /*collections on,
                       * but no filter*/));

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
}

void CollectionsSyncWriteParamTest::TearDown() {
    CollectionsDcpTest::TearDown();
}

TEST_P(CollectionsSyncWriteParamTest,
       seqno_advanced_one_mutation_plus_pending) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(
            cookie,
            std::string{
                    cm.add(CollectionEntry::meat).add(CollectionEntry::dairy)});
    // filter only CollectionEntry::dairy
    createDcpObjects({{R"({"collections":["c"]})"}});

    store_item(vbid, StoredDocKey{"meat::one", CollectionEntry::meat}, "pork");
    store_item(vbid, StoredDocKey{"meat::two", CollectionEntry::meat}, "beef");
    store_item(vbid,
               StoredDocKey{"dairy::three", CollectionEntry::dairy},
               "cheese");
    store_item(
            vbid,
            StoredDocKey{"dairy::four", CollectionEntry::dairy},
            std::string("milk"),
            0,
            {cb::engine_errc::sync_write_pending},
            PROTOCOL_BINARY_RAW_BYTES,
            cb::durability::Requirements(cb::durability::Level::Majority,
                                         cb::durability::Timeout::Infinity()));

    // 2 collections + 3 mutations
    flushVBucketToDiskIfPersistent(vbid, 6);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(producers->last_collection_id, CollectionEntry::dairy.getId());
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
}

TEST_P(CollectionsSyncWriteParamTest, drop_collection_with_pending_write) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie, std::string{cm.add(CollectionEntry::dairy)});

    store_item(vbid, StoredDocKey{"milk", CollectionEntry::defaultC}, "milk");

    auto item = makePendingItem(
            StoredDocKey{"cream", CollectionEntry::defaultC}, "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    item = makePendingItem(StoredDocKey{"cream", CollectionEntry::dairy},
                           "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    // 1 collections + 1 mutation + 2 pending
    flushVBucketToDiskIfPersistent(vbid, 4);

    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));
    if (persistent()) {
        flush_vbucket_to_disk(replicaVB, 4);
    }

    setCollections(cookie, std::string{cm.remove(CollectionEntry::dairy)});
    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));

    flushVBucketToDiskIfPersistent(vbid, 1);
    flushVBucketToDiskIfPersistent(replicaVB, 1);

    auto replica = store->getVBucket(replicaVB);
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vb);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*replica);

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(4, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Now process the drop on all vbuckets
    if (isPersistent()) {
        runCompaction(vbid);
        runCompaction(replicaVB);
    } else {
        // Only need to run on vbid for ephemeral because the task will visit
        // both active/replica
        runEraser(vbid);
    }

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(4, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(4, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
}

failover_entry_t CollectionsSyncWriteParamTest::
        testCompleteDifferentPrepareOnActiveBeforeReplicaDropSetUp() {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm{};
    setCollections(cookie, std::string{cm.add(CollectionEntry::dairy)});

    auto item = makePendingItem(StoredDocKey{"cream", CollectionEntry::dairy},
                                "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    item = makePendingItem(makeStoredDocKey("keyToCommit"), "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    // 1 collections + 2 pending
    flushVBucketToDiskIfPersistent(vbid, 3);

    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));
    flushVBucketToDiskIfPersistent(replicaVB, 3);

    setCollections(cookie, std::string{cm.remove(CollectionEntry::dairy)});

    // Get DCP ready, but don't step the drop event yet
    notifyAndStepToCheckpoint();

    flushVBucketToDiskIfPersistent(vbid, 1);

    auto replica = store->getVBucket(replicaVB);
    auto& adm = VBucketTestIntrospector::public_getActiveDM(*vb);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*replica);

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Now process the drop but only on the active (note replica hasn't yet
    // received the drop, this is mainly because the ephemeral purger will
    // process all vbuckets and cause failure if our pdm EXPECTS)
    if (isPersistent()) {
        runCompaction(vbid);
    } else {
        runEraser(vbid);
    }

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(3, adm.getHighPreparedSeqno());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Commit on the active and add an extra prepare
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged(
                      folly::SharedMutex::ReadHolder(vb->getStateLock()),
                      "replica",
                      3 /*prepareSeqno*/));

    vb->processResolvedSyncWrites();

    item = makePendingItem(makeStoredDocKey("prepareToMoveHPS"), "value");
    EXPECT_EQ(ENGINE_SYNC_WRITE_PENDING, store->set(*item, cookie));

    flushVBucketToDiskIfPersistent(vbid, 2);

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(6, adm.getHighPreparedSeqno());
    EXPECT_EQ(3, adm.getHighCompletedSeqno());

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());

    // Now transfer the drop event to the replica
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    flushVBucketToDiskIfPersistent(replicaVB, 1);

    EXPECT_EQ(2, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(1, pdm.getNumDroppedCollections());

    return vb->failovers->getLatestEntry();
}

TEST_P(CollectionsSyncWriteParamTest,
       CompleteDifferentPrepareOnActiveBeforeReplicaDropsCollection) {
    testCompleteDifferentPrepareOnActiveBeforeReplicaDropSetUp();

    auto replica = store->getVBucket(replicaVB);
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*replica);

    // Stream commit and a new prepare to the replica. We should remove two
    // prepares (the one for the dropped collection and the prepare for the
    // commit).
    notifyAndStepToCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpCommit));

    // This commit drops the droppedCollection as the seqno is higher than the
    // end seqno of the droppedCollection
    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(6, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    // Finally run the compaction/tombstone purge which drops the collection
    // keys
    if (isPersistent()) {
        runCompaction(replicaVB);
    } else {
        runEraser(replicaVB);
    }

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(6, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());
}

/**
 * Test that if we warm up with a dropped collection we have not yet erased we
 * do not load break the PDM state. This should be covered by the
 * bySeqnoScanCallbacks which should prevent us from warming up any keys of
 * dropped collections.
 */
TEST_P(CollectionsSyncWriteParamTest,
       CompleteDifferentPrepareOnActiveBeforeReplicaDropsCollectionWarmupInMiddle) {
    // This test requires a warmup so doesn't work for ephemeral
    if (!isPersistent()) {
        return;
    }

    // Need our failoverEntry to set up the dcp connection again
    failover_entry_t failoverEntry =
            testCompleteDifferentPrepareOnActiveBeforeReplicaDropSetUp();

    resetEngineAndWarmup();

    auto replica = store->getVBucket(replicaVB);
    ASSERT_TRUE(replica);
    EXPECT_EQ(4, replica->getHighSeqno());
    auto& pdm = VBucketTestIntrospector::public_getPassiveDM(*replica);

    // The warmup will not load the prepares for the dropped collection or
    // the droppedCollections map
    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    // Set up our DCP stream and finish streaming to the replica to check
    // that everything is working as intended
    CollectionsDcpTest::internalSetUp();
    producer->closeStream(0, vbid);

    // Swap to the collection dcp producers which support mutations and prepares
    producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
    producers->replicaVB = replicaVB;
    producers->consumer = consumer.get();

    // Need to recreate our stream with SyncRepl enabled
    // producer->closeAllStreams();
    producer->setSyncReplication(SyncReplication::SyncReplication);

    uint64_t rollbackSeqno;
    ASSERT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0, // flags
                                      1, // opaque
                                      vbid,
                                      4, // start_seqno
                                      ~0ull, // end_seqno
                                      failoverEntry.vb_uuid, // vbucket_uuid,
                                      4, // snap_start_seqno,
                                      7, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &dcpAddFailoverLog,
                                      std::make_optional(
                                              std::string_view{})
                      /*collections on,
                       * but no filter*/));

    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // And stream to the replica - this mutation is actually the commit but we
    // are sending it as a Disk snapshot so it is turned into a mutation
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                              false /*disk*/);
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpMutation));

    // This logical commit removes the prepare
    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    // Next prepare streams normally
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpPrepare));

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    // Eraser makes no changes when it drops the collection
    runEraser(replicaVB);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(3, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());

    // And our flush moves the HPS (because this is a Disk snapshot)
    flushVBucketToDiskIfPersistent(replicaVB, 2);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(6, pdm.getHighPreparedSeqno());
    EXPECT_EQ(3, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getNumDroppedCollections());
}

INSTANTIATE_TEST_SUITE_P(
        CollectionsDcpEphemeralOrPersistent,
        CollectionsSyncWriteParamTest,
        STParameterizedBucketTest::ephAndCouchstoreConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
