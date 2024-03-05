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

#include "checkpoint_manager.h"
#include "dcp/backfill_by_seqno_disk.h"
#include "item.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <spdlog/fmt/fmt.h>

class CollectionsOSODcpTest : public CollectionsDcpParameterizedTest {
public:
    CollectionsOSODcpTest() : CollectionsDcpParameterizedTest() {
    }

    void SetUp() override {
        // Disable OSO backfill auto-selection to simplify most of the
        // functional tests - set to always.
        if (!config_string.empty()) {
            config_string += ";";
        }
        config_string += "dcp_oso_backfill=enabled";

        CollectionsDcpParameterizedTest::SetUp();
        producers = std::make_unique<CollectionsDcpTestProducers>();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }

    void testTwoCollections(bool backfillWillPause,
                            OutOfOrderSnapshots osoMode,
                            uint64_t highSeqno);

    void MB_43700(CollectionID cid);

    void emptyDiskSnapshot(OutOfOrderSnapshots osoMode);
};

std::pair<CollectionsManifest, uint64_t>
CollectionsDcpTest::setupTwoCollections(bool endOnTarget) {
    CollectionsManifest cm(CollectionEntry::fruit);
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flush_vbucket_to_disk(vbid, 2);
    return {cm, writeTwoCollectios(endOnTarget)};
}

uint64_t CollectionsDcpTest::writeTwoCollectios(bool endOnTarget) {
    // Interleave the writes to two collections, this is linked to expectations
    // in CollectionsOSODcpTest test harness
    store_item(vbid, makeStoredDocKey("b", CollectionEntry::fruit), "q");
    store_item(vbid, makeStoredDocKey("b", CollectionEntry::vegetable), "q");
    store_item(vbid, makeStoredDocKey("d", CollectionEntry::fruit), "a");
    store_item(vbid, makeStoredDocKey("d", CollectionEntry::vegetable), "q");
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::fruit), "w");
    store_item(vbid, makeStoredDocKey("a", CollectionEntry::vegetable), "q");

    if (endOnTarget) {
        store_item(vbid, makeStoredDocKey("c", CollectionEntry::fruit), "y");
        store_item(
                vbid, makeStoredDocKey("c", CollectionEntry::vegetable), "q");
    } else {
        store_item(
                vbid, makeStoredDocKey("c", CollectionEntry::vegetable), "q");
        store_item(vbid, makeStoredDocKey("c", CollectionEntry::fruit), "y");
    }
    flush_vbucket_to_disk(vbid, 8);
    return store->getVBucket(vbid)->getHighSeqno();
}

// Run through how we expect OSO to work, this is a minimal test which will
// use the default collection
TEST_P(CollectionsOSODcpTest, basic) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");
    store_item(vbid, makeStoredDocKey("a"), "w");
    store_item(vbid, makeStoredDocKey("c"), "y");
    flush_vbucket_to_disk(vbid, 4);

    std::array<cb::mcbp::DcpAddStreamFlag, 2> flags = {
            {cb::mcbp::DcpAddStreamFlag::None,
             cb::mcbp::DcpAddStreamFlag::DiskOnly}};

    for (auto flag : flags) {
        // Reset so we have to stream from backfill
        resetEngineAndWarmup();

        // Filter on default collection (this will request from seqno:0)
        createDcpObjects(
                {{R"({"collections":["0"]})"}}, OutOfOrderSnapshots::Yes, flag);

        // We have a single filter, expect the backfill to be OSO
        runBackfill();

        // OSO snapshots are never really used in KV to KV replication, but this
        // test is using KV to KV test code, hence we need to set a snapshot so
        // that any transferred items don't trigger a snapshot exception.
        consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

        // Manually step the producer and inspect all callbacks
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
        EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
                  producers->last_oso_snapshot_flags);

        // We don't expect a collection create, this is the default collection
        // which clients assume exists unless deleted.
        std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};
        for (auto& k : keys) {
            // Now we get the mutations, they aren't guaranteed to be in seqno
            // order, but we know that for now they will be in key order.
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
            EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
            EXPECT_EQ(k, producers->last_key);
        }

        // Now we get the end message
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
        EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
                  producers->last_oso_snapshot_flags);

        if (flag == cb::mcbp::DcpAddStreamFlag::DiskOnly) {
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers->last_op);
        }
    }
}

void CollectionsOSODcpTest::emptyDiskSnapshot(OutOfOrderSnapshots osoMode) {
    // Put something on disk otherwise to expose the issue
    store_item(vbid, makeStoredDocKey("c"), "y");
    flush_vbucket_to_disk(vbid, 1);

    std::array<cb::mcbp::DcpAddStreamFlag, 2> flags = {
            {cb::mcbp::DcpAddStreamFlag::None,
             cb::mcbp::DcpAddStreamFlag::DiskOnly}};

    for (auto flag : flags) {
        // Reset so we have to stream from backfill
        resetEngineAndWarmup();
        VBucketPtr vb = store->getVBucket(vbid);
        // Create a collection so we can get a stream, but don't flush it
        CollectionsManifest cm(CollectionEntry::fruit);
        vb->updateFromManifest(
                folly::SharedMutex::ReadHolder(vb->getStateLock()),
                makeManifest(cm));

        // Filter on fruit collection (this will request from seqno:0)
        createDcpObjects({{R"({"collections":["9"]})"}}, osoMode, flag);

        // We have a single filter, expect the backfill to be OSO
        runBackfill();

        // OSO snapshots are never really used in KV to KV replication, but this
        // test is using KV to KV test code, hence we need to set a snapshot so
        // that any transferred items don't trigger a snapshot exception.
        consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

        // Manually step the producer and inspect all callbacks
        // We currently send OSO start/end with no data
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
        EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
                  producers->last_oso_snapshot_flags);

        if (osoMode == OutOfOrderSnapshots::YesWithSeqnoAdvanced) {
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                      producers->last_op);
        }

        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
        EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
                  producers->last_oso_snapshot_flags);

        if (flag == cb::mcbp::DcpAddStreamFlag::DiskOnly) {
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers->last_op);
        }
    }
}

TEST_P(CollectionsOSODcpTest, emptyDiskSnapshot_MB_49847) {
    emptyDiskSnapshot(OutOfOrderSnapshots::Yes);
}
TEST_P(CollectionsOSODcpTest, emptyDiskSnapshot_MB_49847_withSeqAdvanced) {
    emptyDiskSnapshot(OutOfOrderSnapshots::YesWithSeqnoAdvanced);
}

// MB-49542: Confirm that an oso backfill does not register a cursor if the
// associated stream is dead.
TEST_P(CollectionsOSODcpTest, NoCursorRegisteredForDeadStream) {
    // Write a couple of items to disk, to attempt to backfill
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");

    flush_vbucket_to_disk(vbid, 2);
    // Reset so we have to stream from backfill
    ensureDcpWillBackfill();

    // set up stream, registers cursor
    createDcpObjects({{R"({"collections":["0"]})"}},
                     OutOfOrderSnapshots::Yes,
                     cb::mcbp::DcpAddStreamFlag::None);

    auto& cm = *store->getVBucket(vbid)->checkpointManager;
    auto initialCursors = cm.getNumCursors();
    // disconnect will end all the streams
    producer->setDisconnect();

    // stream cursor was removed
    ASSERT_EQ(cm.getNumCursors(), initialCursors - 1);

    // step the backfill tasks, may attempt to register a cursor
    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create() -> fails to register cursor, state change to done
    runNextTask(lpAuxioQ);
    // run backfill:done()
    runNextTask(lpAuxioQ);

    // backfill did not successfully register a cursor, the stream was dead.
    // MB-49542: would fail as a cursor is registered and is "leaked"
    // as the stream has already transitioned to dead, which is the last time
    // the stream would remove the cursor.
    EXPECT_EQ(cm.getNumCursors(), initialCursors - 1);
}

void CollectionsOSODcpTest::testTwoCollections(bool backfillWillPause,
                                               OutOfOrderSnapshots osoMode,
                                               uint64_t highSeqno) {
    ASSERT_NE(OutOfOrderSnapshots::No, osoMode);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    // Filter on vegetable collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["a"]})"}}, OutOfOrderSnapshots::Yes);

    if (backfillWillPause) {
        producer->setBackfillBufferSize(1);
    }

    producer->setOutOfOrderSnapshots(osoMode);

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // see comment in CollectionsOSODcpTest.basic
    consumer->snapshotMarker(1, replicaVB, 0, highSeqno, 0, 0, highSeqno);

    auto step = [this, &backfillWillPause]() {
        auto result = producer->stepWithBorderGuard(*producers);
        if (backfillWillPause) {
            // backfill paused, step does nothing
            EXPECT_EQ(cb::engine_errc::would_block, result);
            auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
            runNextTask(lpAuxioQ);
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
        } else {
            EXPECT_EQ(cb::engine_errc::success, result);
        }
    };

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    EXPECT_EQ("vegetable", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    uint64_t txHighSeqno = 0;
    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};
    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        step();
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
        txHighSeqno = std::max(txHighSeqno, producers->last_byseqno.load());
    }

    // Backfill will now have completed
    step();

    if (osoMode == OutOfOrderSnapshots::YesWithSeqnoAdvanced &&
        txHighSeqno != highSeqno) {
        // Expect a SeqnoAdvanced that advances us to the highest seqno in
        // the snapshot (which is a different collection item
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced, producers->last_op);
        EXPECT_EQ(highSeqno, producers->last_byseqno);

        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
    }

    // Now we get the end message
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);
}

TEST_P(CollectionsOSODcpTest, two_collections) {
    testTwoCollections(
            false, OutOfOrderSnapshots::Yes, setupTwoCollections().second);
}

TEST_P(CollectionsOSODcpTest, two_collections_backfill_pause) {
    testTwoCollections(
            true, OutOfOrderSnapshots::Yes, setupTwoCollections().second);
}

TEST_P(CollectionsOSODcpTest, two_collections_with_seqno_advanced) {
    testTwoCollections(false,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections().second);
}

TEST_P(CollectionsOSODcpTest,
       two_collections_backfill_pause_with_seqno_advanced) {
    testTwoCollections(true,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections().second);
}

// The next two tests will run with OSO+SeqnoAdvanced but the target collection
// is the highest seqno, so no SeqnoAdvanced is sent in the OSO snapshot
TEST_P(CollectionsOSODcpTest, two_collections_with_seqno_advanced_skipped) {
    testTwoCollections(false,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections(true).second);
}

TEST_P(CollectionsOSODcpTest,
       two_collections_backfill_pause_with_seqno_advanced_skipped) {
    testTwoCollections(true,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections(true).second);
}

TEST_P(CollectionsOSODcpTest, dropped_collection) {
    auto setup = setupTwoCollections();

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    // Filter on vegetable collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["a"]})"}}, OutOfOrderSnapshots::Yes);

    // The drop is deliberately placed here, after we've permitted the stream
    // request to vegetable, yet before the stream schedules a backfill. So the
    // stream should only return a dropped vegetable event and no vegetable
    // items in the OSO snapshot
    VBucketPtr vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            folly::SharedMutex::ReadHolder(vb->getStateLock()),
            makeManifest(setup.first.remove(CollectionEntry::vegetable)));
    flush_vbucket_to_disk(vbid, 1);

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // see comment in CollectionsOSODcpTest.basic
    consumer->snapshotMarker(
            1, replicaVB, 0, setup.second + 1, 0, 0, setup.second + 1);

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(CollectionUid::vegetable, producers->last_collection_id);
    EXPECT_EQ(mcbp::systemevent::id::DeleteCollection,
              producers->last_system_event);

    // ... end
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);
}

// Test that we can transition to in memory and continue.
TEST_P(CollectionsOSODcpTest, transition_to_memory) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "b-value");
    store_item(vbid, makeStoredDocKey("a"), "a-value");
    store_item(vbid, makeStoredDocKey("c"), "c-value");
    flush_vbucket_to_disk(vbid, 3);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    createDcpObjects({{R"({"collections":["0"]})"}}, OutOfOrderSnapshots::Yes);

    // Some in-memory only item
    store_item(vbid, makeStoredDocKey("d"), "d-value");
    store_item(vbid, makeStoredDocKey("e"), "e-value");
    store_item(vbid, makeStoredDocKey("f"), "f-value");

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // OSO snapshots are never really used in KV to KV replication, but this
    // test is using KV to KV test code, hence we need to set a snapshot so
    // that any transferred items don't trigger a snapshot exception.
    consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    std::array<std::pair<std::string, uint64_t>, 3> keys = {
            {{"a", 2}, {"b", 1}, {"c", 3}}};
    for (const auto& [k, s] : keys) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(s, producers->last_byseqno);
    }

    // Now we get the end message
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    notifyAndStepToCheckpoint();

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("d", producers->last_key);
    EXPECT_EQ(4, producers->last_byseqno);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));

    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("e", producers->last_key);
    EXPECT_EQ(5, producers->last_byseqno);
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));

    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("f", producers->last_key);
    EXPECT_EQ(6, producers->last_byseqno);
}

// Test that we can transition to in memory and continue (issue raised by
// MB-38999)
TEST_P(CollectionsOSODcpTest, transition_to_memory_MB_38999) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "b-value");
    store_item(vbid, makeStoredDocKey("a"), "a-value");
    store_item(vbid, makeStoredDocKey("c"), "c-value");
    flush_vbucket_to_disk(vbid, 3);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    createDcpObjects({{R"({"collections":["0"]})"}}, OutOfOrderSnapshots::Yes);

    // Now write the 4th item and flush it
    store_item(vbid, makeStoredDocKey("d"), "d-value");
    flush_vbucket_to_disk(vbid, 1);

    // Now write the 5th item, not flushed
    store_item(vbid, makeStoredDocKey("e"), "e-value");

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // OSO snapshots are never really used in KV to KV replication, but this
    // test is using KV to KV test code, hence we need to set a snapshot so
    // that any transferred items don't trigger a snapshot exception.
    consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    std::array<std::pair<std::string, uint64_t>, 4> keys = {
            {{"a", 2}, {"b", 1}, {"c", 3}, {"d", 4}}};
    for (const auto& [k, s] : keys) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(s, producers->last_byseqno);
    }

    // Now we get the end message
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    notifyAndStepToCheckpoint();

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
    EXPECT_EQ("e", producers->last_key);
    EXPECT_EQ(5, producers->last_byseqno);
}

// OSO + StreamID enabled
TEST_P(CollectionsOSODcpTest, basic_with_stream_id) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");
    store_item(vbid, makeStoredDocKey("a"), "w");
    store_item(vbid, makeStoredDocKey("c"), "y");
    flush_vbucket_to_disk(vbid, 4);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    // Create DCP producer and stream.
    producer = SingleThreadedKVBucketTest::createDcpProducer(
            cookieP, IncludeDeleteTime::No);

    producers->consumer = nullptr;
    producers->replicaVB = replicaVB;
    producer->enableMultipleStreamRequests();
    producer->setOutOfOrderSnapshots(OutOfOrderSnapshots::Yes);

    createDcpStream({{R"({"sid":88, "collections":["0"]})"}});

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    const size_t snapshotSz = sizeof(cb::mcbp::Request) +
                              sizeof(cb::mcbp::request::DcpOsoSnapshotPayload) +
                              sizeof(cb::mcbp::DcpStreamIdFrameInfo);

    size_t outstanding = snapshotSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    // We don't expect a collection create, this is the default collection which
    // clients assume exists unless deleted.
    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};
    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        const size_t mutationSz =
                sizeof(cb::mcbp::Request) +
                sizeof(cb::mcbp::request::DcpMutationPayload) +
                sizeof(cb::mcbp::DcpStreamIdFrameInfo) + 2 /*key*/ +
                1 /*value*/;
        outstanding += mutationSz;
        EXPECT_EQ(outstanding, producer->getBytesOutstanding());
    }

    // Now we get the end message
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);

    outstanding += snapshotSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());
}

// Run through the issue of MB-43700, we were generating the end key of the OSO
// rage incorrectly which showed up when the collection-ID was certain value
void CollectionsOSODcpTest::MB_43700(CollectionID cid) {
    store_item(vbid, makeStoredDocKey("b", cid), cid.to_string() + "b");
    store_item(vbid, makeStoredDocKey("d", cid), cid.to_string() + "d");
    store_item(vbid, makeStoredDocKey("a", cid), cid.to_string() + "a");
    store_item(vbid, makeStoredDocKey("c", cid), cid.to_string() + "c");
    flush_vbucket_to_disk(vbid, 4);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    nlohmann::json filter;
    filter["collections"] = {fmt::format("{:x}", uint32_t(cid))};
    createDcpObjects(filter.dump(), OutOfOrderSnapshots::Yes);

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSystemEvent, producers->last_op);
    EXPECT_EQ(cid, producers->last_collection_id);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};
    for (auto& k : keys) {
        // Now we get the mutations, they aren't guaranteed to be in seqno
        // order, but we know that for now they will be in key order.
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(cid, producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(cid.to_string() + k, producers->last_value);
    }

    // Now we get the end message
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpOsoSnapshot, producers->last_op);
    EXPECT_EQ(uint32_t(cb::mcbp::request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);
}

TEST_P(CollectionsOSODcpTest, MB_43700) {
    // Bug was raised with ID 0xff, we shall test some other 'max' values
    std::array<CollectionID, 3> collections = {{0xff, 0xffff, 0xffffffff}};
    CollectionsManifest cm;
    for (auto cid : collections) {
        cm.add(CollectionEntry::Entry{cid.to_string(), cid});
        setCollections(cookie, cm);
        flush_vbucket_to_disk(vbid, 1);
        MB_43700(cid);
    }
}

TEST_P(CollectionsOSODcpTest, cursor_dropped) {
    CollectionsManifest cm(CollectionEntry::fruit);
    VBucketPtr vb = store->getVBucket(vbid);
    setCollections(cookie, cm.add(CollectionEntry::vegetable));
    flushVBucketToDiskIfPersistent(vbid, 2);
    // Put data into 2 collections, we will stream the first data which has the
    // lower high-seqno
    ASSERT_TRUE(store_items(
            4, vbid, StoredDocKey{"apple", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 4);
    ASSERT_TRUE(store_items(4,
                            vbid,
                            StoredDocKey{"turnip", CollectionEntry::vegetable},
                            "nice"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    ensureDcpWillBackfill();

    createDcpObjects({{R"({"collections":["9"]})"}}, OutOfOrderSnapshots::Yes);

    // Backfill scheduled, we will manually run and cursor drop in between
    // create/scan

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    // backfill:create()
    runNextTask(lpAuxioQ);

    auto firstBackfillEnd = vb->getHighSeqno();

    // More keys, flushed to the snapshot we don't have open
    ASSERT_TRUE(store_items(
            4, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    // and kick all of those out of memory
    ensureDcpWillBackfill();

    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);
    auto* as = static_cast<ActiveStream*>(stream.get());
    as->handleSlowStream();

    // backfill:scan() -> complete
    runNextTask(lpAuxioQ);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpOsoSnapshot);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent);

    std::array<std::pair<std::string, uint64_t>, 4> keys = {
            {{"apple0", 3}, {"apple1", 4}, {"apple2", 5}, {"apple3", 6}}};
    for (const auto& [k, s] : keys) {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::fruit.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(s, producers->last_byseqno);
    }

    // end OSO
    stepAndExpect(cb::mcbp::ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(firstBackfillEnd, as->getLastReadSeqno());

    // MB-52956 noted that the backfill would start too early, meaning we may
    // scan a range that is larger than required. Here we skip driving things
    // via auxio and instead now grab the backfill manager so we can inspect
    // the backfill in detail
    auto uniquePtr = producer->public_dequeueNextBackfill();
    ASSERT_TRUE(uniquePtr);
    auto* backfill = dynamic_cast<DCPBackfillBySeqnoDisk*>(uniquePtr.get());
    ASSERT_TRUE(backfill);
    EXPECT_EQ(backfill->getStartSeqno(), firstBackfillEnd + 1);
    EXPECT_EQ(backfill_finished, backfill->run());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    EXPECT_EQ(firstBackfillEnd + 1, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    keys = {{{"orange0", 11},
             {"orange1", 12},
             {"orange2", 13},
             {"orange3", 14}}};
    for (const auto& [k, s] : keys) {
        stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation);
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionEntry::fruit.getId(),
                  producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(s, producers->last_byseqno);
    }
}

// Similar to basic, except we disable OSO at the bucket level and check
// this correctly falls back to seqno
TEST_P(CollectionsOSODcpTest, fallbackToSeqnoIfOsoDisabled) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");
    store_item(vbid, makeStoredDocKey("a"), "w");
    store_item(vbid, makeStoredDocKey("c"), "y");
    flush_vbucket_to_disk(vbid, 4);

    // Reset so we have to stream from backfill
    resetEngineAndWarmup();

    // Disable OSO at the bucket level.
    engine->getConfiguration().setDcpOsoBackfill("disabled");

    // Filter on default collection (this will request from seqno:0)
    createDcpObjects(
            {{R"({"collections":["0"]})"}}, OutOfOrderSnapshots::Yes, {});

    // We have a single filter, expect the backfill to be OSO
    runBackfill();

    // OSO snapshots are never really used in KV to KV replication, but this
    // test is using KV to KV test code, hence we need to set a snapshot so
    // that any transferred items don't trigger a snapshot exception.
    consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

    // Manually step the producer check we get a non-OSO snapshot.
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);
}

// OSO doesn't support ephemeral - this one test checks it falls back to normal
// snapshots
class CollectionsOSOEphemeralTest : public CollectionsDcpParameterizedTest {
public:
};

// Run through how we expect OSO to work, this is a minimal test which will
// use the default collection
TEST_P(CollectionsOSOEphemeralTest, basic) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");
    store_item(vbid, makeStoredDocKey("a"), "w");
    store_item(vbid, makeStoredDocKey("c"), "y");

    ensureDcpWillBackfill();

    // Filter on default collection (this will request from seqno:0)
    createDcpObjects({{R"({"collections":["0"]})"}}, OutOfOrderSnapshots::Yes);

    runBackfill();

    // OSO snapshots are never really used in KV to KV replication, but this
    // test is using KV to KV test code, hence we need to set a snapshot so
    // that any transferred items don't trigger a snapshot exception.
    consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);

    // Manually step the producer and inspect all callbacks
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));
    EXPECT_EQ(cb::mcbp::ClientOpcode::DcpSnapshotMarker, producers->last_op);

    std::array<std::pair<std::string, uint64_t>, 4> keys = {
            {{"b", 1}, {"d", 2}, {"a", 3}, {"c", 4}}};
    for (const auto& [k, s] : keys) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepWithBorderGuard(*producers));
        EXPECT_EQ(cb::mcbp::ClientOpcode::DcpMutation, producers->last_op);
        EXPECT_EQ(CollectionID::Default, producers->last_collection_id);
        EXPECT_EQ(k, producers->last_key);
        EXPECT_EQ(s, producers->last_byseqno);
    }
}

// Test fixture for Collections DCP tests for dcp_oso_backfill=auto - is
// OSO or seqno correctly selected based on which is expected to be fastest.
class CollectionsOSODcpAutoSelectTest : public CollectionsOSODcpTest {
protected:
    // Helper method - tests that two collections which have number of items
    // of the given ratio of the total bucket are streamed via OSO and
    // seqno backfill respectively, when populated with items of the given
    // size.
    void testDcpOsoBackfillAutomaticMode(double osoCollectionRatio,
                                         double seqnoCollectionRatio,
                                         size_t valueSize) {
        // Need to change dcp_oso_backfill mode back to "auto" for this test.
        engine->getConfiguration().setDcpOsoBackfill("auto");

        // Setup 3 collections:
        //  - 'fruit' is sized to osoCollectionRatio of all items in the
        //     vBucket, and is expected to be streamed via OSO backfill.
        //  - 'vegetable' is sized to seqnoCollectionRatio of all items in the
        //     vBucket, and is expected to be streamed by seqno backfill.
        //  -  default contains all the other items (such that fruit and veg
        //     have their desired percentages).
        CollectionsManifest cm(CollectionEntry::fruit);
        setCollections(cookie, cm.add(CollectionEntry::vegetable));
        flush_vbucket_to_disk(vbid, 2);

        const auto totalItems = 1000;
        const size_t fruitItems = std::round(osoCollectionRatio * totalItems);
        ASSERT_GT(fruitItems, 0)
                << "Cannot populate 'fruit' collection at ratio "
                << osoCollectionRatio << "with only " << totalItems
                << " items total";
        const size_t vegetableItems =
                std::round(seqnoCollectionRatio * totalItems);
        ASSERT_GT(vegetableItems, 0)
                << "Cannot populate 'fruit' collection at ratio "
                << seqnoCollectionRatio << "with only " << totalItems
                << " items total";

        storeItems(CollectionEntry::fruit,
                   fruitItems,
                   cb::engine_errc::success,
                   valueSize);
        storeItems(CollectionEntry::vegetable,
                   vegetableItems,
                   cb::engine_errc::success,
                   valueSize);
        storeItems(CollectionEntry::defaultC,
                   totalItems - fruitItems - vegetableItems);
        flush_vbucket_to_disk(vbid, totalItems);
        ensureDcpWillBackfill();

        // For each of fruit and vegetable; stream via DCP. Fruit should be
        // streamed via OSO as it is small enough; vegetable should be streamed
        // via seqno as it is too large.
        using cb::mcbp::ClientOpcode;
        struct Test {
            CollectionID id;
            ClientOpcode marker;
        };
        for (auto [collection, marker] :
             {Test{CollectionEntry::fruit, ClientOpcode::DcpOsoSnapshot},
              Test{CollectionEntry::vegetable,
                   ClientOpcode::DcpSnapshotMarker}}) {
            createDcpObjects(makeStreamRequestValue({collection}),
                             OutOfOrderSnapshots::Yes,
                             {});
            runBackfill();
            runBackfill();

            // OSO snapshots are never really used in KV to KV replication, but
            // this test is using KV to KV test code, hence we need to set a
            // snapshot so that any transferred items don't trigger a snapshot
            // exception.
            consumer->snapshotMarker(1, replicaVB, 0, 4, 0, 0, 4);
            // Manually step the producer and check the snapshot type.
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(marker, producers->last_op)
                    << "For collection:" << collection.to_string();
        }
    }
};

// For a collection where the average item size is "small" (less than 256B),
// check that OSO is only used when the collection requested is smaller than
// 0.5% of the total (v)Bucket item count.
//
TEST_P(CollectionsOSODcpAutoSelectTest, SmallItems) {
    testDcpOsoBackfillAutomaticMode(0.004, 0.006, 200);
}

// Test that runtime changes to dcp_oso_backfill_small_value_ratio are reflected
// in backfill behaviour.
//
TEST_P(CollectionsOSODcpAutoSelectTest, SmallItemsDynamic) {
    // Increase dcp_oso_backfill_small_value_ratio from 0.5% to 2% and
    // confirm change is reflected when oso vs seqno selection.
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setDcpParam(
                      "dcp_oso_backfill_small_value_ratio", "0.02", msg));
    testDcpOsoBackfillAutomaticMode(0.01, 0.03, 200);
}

// For a collection where the average item size is "large" (greater than 256B),
// check that OSO is only used when the collection requested is smaller than
// 4% of the total (v)Bucket item count.
//
TEST_P(CollectionsOSODcpAutoSelectTest, LargeItems) {
    testDcpOsoBackfillAutomaticMode(0.03, 0.05, 300);
}

// Test that runtime changes to dcp_oso_backfill_large_value_ratio are reflected
// in backfill behaviour.
//
TEST_P(CollectionsOSODcpAutoSelectTest, LargeItemsDynamic) {
    // Increase dcp_oso_backfill_large_value_ratio from 4% to 10% and
    // confirm change is reflected when oso vs seqno selection.
    std::string msg;
    ASSERT_EQ(cb::engine_errc::success,
              engine->setDcpParam(
                      "dcp_oso_backfill_large_value_ratio", "0.1", msg));
    testDcpOsoBackfillAutomaticMode(0.09, 0.11, 300);
}

// Test that runtime changes to dcp_oso_backfill_small_item_size_threshold are
// reflected in backfill behaviour.
//
TEST_P(CollectionsOSODcpAutoSelectTest, SmallThresholdDynamic) {
    // Increase dcp_oso_backfill_small_item_size_threshold from 256B to 400B,
    // confirm change is reflected when oso vs seqno selection.
    std::string msg;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->setDcpParam(
                    "dcp_oso_backfill_small_item_size_threshold", "400", msg));
    // Now 300B is considered "small", we should only see OSO backfill if
    // collection is less than 0.5% of items.
    testDcpOsoBackfillAutomaticMode(0.004, 0.006, 300);
}

class CollectionsOSOMultiTest : public CollectionsOSODcpTest {
public:
    void SetUp() override {
        CollectionsOSODcpTest::SetUp();
        std::string msg;
        ASSERT_EQ(cb::engine_errc::success,
                  engine->setDcpParam(
                          "dcp_oso_max_collections_per_backfill", "100", msg));
    }
};

// The test uses three collections and filters for two and expects the two
// filtered collections to be sent in the OSO snapshot
TEST_P(CollectionsOSOMultiTest, multi) {
    using namespace cb::mcbp;

    CollectionsManifest cm;
    setCollections(cookie,
                   cm.add(CollectionEntry::vegetable)
                           .add(CollectionEntry::fruit)
                           .add(CollectionEntry::dairy));
    flush_vbucket_to_disk(vbid, 3);

    // 3 collections
    std::array<CollectionID, 3> collections = {CollectionUid::fruit,
                                               CollectionUid::dairy,
                                               CollectionUid::vegetable};
    // 4 keys
    std::array<std::string, 4> keys = {{"a", "b", "c", "d"}};

    // combine!
    for (auto cid : collections) {
        for (const auto& key : keys) {
            store_item(vbid, makeStoredDocKey(key, cid), "value" + key);
        }
        flush_vbucket_to_disk(vbid, keys.size());
    }

    ensureDcpWillBackfill();

    // filter on collections 1 and 2 of the 3 that have been written to.
    nlohmann::json filter = {{"collections",
                              {collections.at(1).to_string(false),
                               collections.at(2).to_string(false)}}};
    createDcpObjects(filter.dump(), OutOfOrderSnapshots::Yes, {});

    runBackfill();

    stepAndExpect(ClientOpcode::DcpOsoSnapshot);
    EXPECT_EQ(uint32_t(request::DcpOsoSnapshotFlags::Start),
              producers->last_oso_snapshot_flags);

    std::unordered_set<std::string> keys1, keys2;

    // This test is written to not assume any ordering of the snapshot.
    // This loop will collect all keys and check they are for the correct
    // collections and the correct keys
    while (producer->stepWithBorderGuard(*producers) ==
                   cb::engine_errc::success &&
           producers->last_op != ClientOpcode::DcpOsoSnapshot) {
        EXPECT_THAT(producers->last_op,
                    testing::AnyOf(ClientOpcode::DcpSystemEvent,
                                   ClientOpcode::DcpMutation));
        EXPECT_THAT(producers->last_collection_id,
                    testing::AnyOf(collections.at(1), collections.at(2)));

        if (producers->last_op == ClientOpcode::DcpMutation) {
            EXPECT_THAT(producers->last_key, testing::AnyOfArray(keys));

            if (producers->last_collection_id == collections.at(1)) {
                keys1.emplace(producers->last_key);
            } else {
                keys2.emplace(producers->last_key);
            }
        }
    }

    EXPECT_EQ(uint32_t(request::DcpOsoSnapshotFlags::End),
              producers->last_oso_snapshot_flags);
    // All Keys from each collection must of been observed
    EXPECT_EQ(keys.size(), keys1.size());
    EXPECT_EQ(keys.size(), keys2.size());
}

INSTANTIATE_TEST_SUITE_P(CollectionsOSOEphemeralTests,
                         CollectionsOSOEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsOSODcpTests,
                         CollectionsOSODcpTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsOSODcpAutoSelectTests,
                         CollectionsOSODcpAutoSelectTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(CollectionsOSOMultiTests,
                         CollectionsOSOMultiTest,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);