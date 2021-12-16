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
#include "item.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <spdlog/fmt/fmt.h>

class CollectionsOSODcpTest : public CollectionsDcpTest {
public:
    CollectionsOSODcpTest() : CollectionsDcpTest() {
    }

    void SetUp() override {
        config_string += "collections_enabled=true";
        SingleThreadedKVBucketTest::SetUp();
        producers = std::make_unique<CollectionsDcpTestProducers>();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
    }

    void testTwoCollections(bool backfillWillPause,
                            OutOfOrderSnapshots osoMode,
                            uint64_t highSeqno);

    void MB_43700(CollectionID cid);

    void emptyDiskSnapshot(OutOfOrderSnapshots osoMode);

    /**
     *  @param endOnTarget true and the last item written will be for the target
     *         collection
     */
    std::pair<CollectionsManifest, uint64_t> setupTwoCollections(
            bool endOnTarget = false);
};

std::pair<CollectionsManifest, uint64_t>
CollectionsOSODcpTest::setupTwoCollections(bool endOnTarget) {
    VBucketPtr vb = store->getVBucket(vbid);
    CollectionsManifest cm(CollectionEntry::fruit);
    vb->updateFromManifest(makeManifest(cm.add(CollectionEntry::vegetable)));

    // Interleave the writes to two collections and then OSO backfill one
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
    flush_vbucket_to_disk(vbid, 10); // 8 keys + 2 events
    return {cm, 10};
}

// Run through how we expect OSO to work, this is a minimal test which will
// use the default collection
TEST_F(CollectionsOSODcpTest, basic) {
    // Write to default collection and deliberately not in lexicographical order
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");
    store_item(vbid, makeStoredDocKey("a"), "w");
    store_item(vbid, makeStoredDocKey("c"), "y");
    flush_vbucket_to_disk(vbid, 4);

    std::array<uint32_t, 2> flags = {{0, DCP_ADD_STREAM_FLAG_DISKONLY}};

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

        if (flag == DCP_ADD_STREAM_FLAG_DISKONLY) {
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

    std::array<uint32_t, 2> flags = {{0, DCP_ADD_STREAM_FLAG_DISKONLY}};

    for (auto flag : flags) {
        // Reset so we have to stream from backfill
        resetEngineAndWarmup();
        VBucketPtr vb = store->getVBucket(vbid);
        // Create a collection so we can get a stream, but don't flush it
        CollectionsManifest cm(CollectionEntry::fruit);
        vb->updateFromManifest(makeManifest(cm));

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

        if (flag == DCP_ADD_STREAM_FLAG_DISKONLY) {
            EXPECT_EQ(cb::engine_errc::success,
                      producer->stepWithBorderGuard(*producers));
            EXPECT_EQ(cb::mcbp::ClientOpcode::DcpStreamEnd, producers->last_op);
        }
    }
}

TEST_F(CollectionsOSODcpTest, emptyDiskSnapshot_MB_49847) {
    emptyDiskSnapshot(OutOfOrderSnapshots::Yes);
}
TEST_F(CollectionsOSODcpTest, emptyDiskSnapshot_MB_49847_withSeqAdvanced) {
    emptyDiskSnapshot(OutOfOrderSnapshots::YesWithSeqnoAdvanced);
}

// MB-49542: Confirm that an oso backfill does not register a cursor if the
// associated stream is dead.
TEST_F(CollectionsOSODcpTest, NoCursorRegisteredForDeadStream) {
    // Write a couple of items to disk, to attempt to backfill
    store_item(vbid, makeStoredDocKey("b"), "q");
    store_item(vbid, makeStoredDocKey("d"), "a");

    flush_vbucket_to_disk(vbid, 2);
    // Reset so we have to stream from backfill
    ensureDcpWillBackfill();

    // set up stream, registers cursor
    createDcpObjects({{R"({"collections":["0"]})"}},
                     OutOfOrderSnapshots::Yes,
                     0 /*flags*/);

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
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepWithBorderGuard(*producers));

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

TEST_F(CollectionsOSODcpTest, two_collections) {
    testTwoCollections(
            false, OutOfOrderSnapshots::Yes, setupTwoCollections().second);
}

TEST_F(CollectionsOSODcpTest, two_collections_backfill_pause) {
    testTwoCollections(
            true, OutOfOrderSnapshots::Yes, setupTwoCollections().second);
}

TEST_F(CollectionsOSODcpTest, two_collections_with_seqno_advanced) {
    testTwoCollections(false,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections().second);
}

TEST_F(CollectionsOSODcpTest,
       two_collections_backfill_pause_with_seqno_advanced) {
    testTwoCollections(true,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections().second);
}

// The next two tests will run with OSO+SeqnoAdvanced but the target collection
// is the highest seqno, so no SeqnoAdvanced is sent in the OSO snapshot
TEST_F(CollectionsOSODcpTest, two_collections_with_seqno_advanced_skipped) {
    testTwoCollections(false,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections(true).second);
}

TEST_F(CollectionsOSODcpTest,
       two_collections_backfill_pause_with_seqno_advanced_skipped) {
    testTwoCollections(true,
                       OutOfOrderSnapshots::YesWithSeqnoAdvanced,
                       setupTwoCollections(true).second);
}

TEST_F(CollectionsOSODcpTest, dropped_collection) {
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
TEST_F(CollectionsOSODcpTest, transition_to_memory) {
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
TEST_F(CollectionsOSODcpTest, transition_to_memory_MB_38999) {
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
TEST_F(CollectionsOSODcpTest, basic_with_stream_id) {
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

TEST_F(CollectionsOSODcpTest, MB_43700) {
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

// OSO doesn't support ephemeral - this one test checks it falls back to normal
// snapshots
class CollectionsOSOEphemeralTest : public CollectionsDcpParameterizedTest {
public:
    std::pair<CollectionsManifest, uint64_t> setupTwoCollections();
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

INSTANTIATE_TEST_SUITE_P(CollectionsOSOEphemeralTests,
                         CollectionsOSOEphemeralTest,
                         STParameterizedBucketTest::ephConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);
