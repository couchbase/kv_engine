/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "checkpoint_manager.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/active_stream_checkpoint_processor_task.h"
#include "dcp/backfill-manager.h"
#include "dcp/backfill_by_seqno.h"
#include "dcp/response.h"
#include "ep_bucket.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <programs/engine_testapp/mock_server.h>
#include <utilities/test_manifest.h>

class CollectionsDcpStreamsTest : public CollectionsDcpTest,
                                  public STParameterizedBucketTest {
public:
    CollectionsDcpStreamsTest() : CollectionsDcpTest() {
    }
    // Create producer without any streams.
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
        producers = std::make_unique<CollectionsDcpTestProducers>();
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookieP, IncludeDeleteTime::No);
        CollectionsDcpTest::consumer = std::make_shared<MockDcpConsumer>(
                *engine, cookieC, "test_consumer");
    }

    void close_stream_by_id_test(bool enableStreamEnd, bool manyStreams);
};

TEST_P(CollectionsDcpStreamsTest, request_validation) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);

    // Cannot do this without enabling the feature
    createDcpStream({{R"({"collections":["9"], "sid":99})"}},
                    vbid,
                    cb::engine_errc::dcp_streamid_invalid);

    // sid of 0 is not allowed, this error has to be caught at this level
    // (ep_engine.cc catches in the full stack)
    try {
        createDcpStream({{R"({"collections":["9"], "sid":0})"}},
                        vbid,
                        cb::engine_errc::dcp_streamid_invalid);
        FAIL() << "Expected an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::dcp_streamid_invalid,
                  cb::engine_errc(e.code().value()));
    }

    producer->enableMultipleStreamRequests();

    // Now caller must provide the sid
    createDcpStream({{R"({"collections":["9"]})"}},
                    vbid,
                    cb::engine_errc::dcp_streamid_invalid);

    // sid of 0 is still not allowed
    try {
        createDcpStream({{R"({"collections":["9"], "sid":0})"}},
                        vbid,
                        cb::engine_errc::dcp_streamid_invalid);
        FAIL() << "Expected an exception";
    } catch (const cb::engine_error& e) {
        EXPECT_EQ(cb::engine_errc::dcp_streamid_invalid,
                  cb::engine_errc(e.code().value()));
    }
}

// A collection enabled stream should be notified for some sync-write activity
// so that a suitable seqno-advance message can be transmitted. For MB-56148
// aborts were not waking a collection enabled (but sync-write disabled)
// producer leaving an indexing client behind the high-seqno.
TEST_P(CollectionsDcpStreamsTest, NonSyncWriteStreamNotify) {
    // Setup the active vbucket so sync-writes can be created.
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    VBucketPtr vb = store->getVBucket(vbid);

    // Manually force creation of the checkpoint processor task so we can drive
    // the test via runNextTask
    producer->createCheckpointProcessorTask();
    producer->scheduleCheckpointProcessorTask();

    // Create a new DCP stream for the entire bucket (collections are enabled
    // but sync-writes are not)
    uint64_t rollbackSeqno{0};
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      0, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      0, // snap_start_seqno,
                                      0, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      std::string_view{}));
    EXPECT_EQ(0, rollbackSeqno);

    // Consume a notification generated from the streamRequest
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookieP));

    // poke the producer as if the frontend executor has awoken
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));

    // MB-56148 reproducible with any collection, stick to default collection.
    StoredDocKey key{"key", CollectionID::Default};

    // Test requires prepare(key), abort(key)
    auto item = makePendingItem(key, "value");
    EXPECT_EQ(cb::engine_errc::sync_write_pending, store->set(*item, cookie));

    // The prepare will not notify
    EXPECT_FALSE(mock_cookie_notified(cookieP))
            << "Prepare not expected to notify";
    {
        std::shared_lock rlh(vb->getStateLock());
        EXPECT_EQ(cb::engine_errc::success,
                  vb->abort(rlh,
                            key,
                            item->getBySeqno(),
                            {},
                            vb->lockCollections(key)));
    }
    // With the fix the abort will notify
    EXPECT_TRUE(mock_cookie_notified(cookieP));
    // Prior to MB-56148 the test would hang here as no notify occurs.
    EXPECT_EQ(cb::engine_errc::success, mock_waitfor_cookie(cookieP));

    // Now that cookie was notified it can step, expect nothing as the task gets
    // scheduled.
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));

    // The abort will have scheduled a task to process the checkpoint.
    auto& nonIOQueue = *task_executor->getLpTaskQ(TaskType::NonIO);
    runNextTask(nonIOQueue,
                "Process checkpoint(s) for DCP producer test_producer");

    // And we get a snapshot - seqno advance replaces the abort.
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSnapshotMarker,
                  cb::engine_errc::success);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);

    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    // should be no more ops
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest, streamRequestNoRollbackSeqnoAdvanced) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 5);
    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    ASSERT_TRUE(store_items(
            4, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    delete_item(vbid, StoredDocKey{"Beef1", CollectionEntry::meat});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Beef5", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    purgeTombstonesBefore(vb->getHighSeqno());

    EXPECT_EQ(7, streamSeqno);
    EXPECT_EQ(13, vb->getHighSeqno());
    EXPECT_EQ(12, vb->getPurgeSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["9"]})"}}));
    EXPECT_EQ(0, rollbackSeqno);

    // The DCP client started the stream at the collection high-seqno, they have
    // all the data, there's nothing to send, not even a seqno advance.
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest, streamRequestNoRollbackNoSeqnoAdvanced) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_item(vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"orange1", CollectionEntry::fruit});
    flushVBucketToDiskIfPersistent(vbid, 1);

    purgeTombstonesBefore(vb->getHighSeqno());

    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(6, vb->getHighSeqno());
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"Apple1", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["9"]})"}}));
    EXPECT_EQ(0, rollbackSeqno);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(streamSeqno, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    // Write another item for the stream.
    store_item(vbid, StoredDocKey{"Apple2", CollectionEntry::fruit}, "nice");
    EXPECT_EQ(8, vb->getHighSeqno());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno() - 1, producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("Apple1", producers->last_key);

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, true);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("Apple2", producers->last_key);

    // should be no more ops
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest,
       streamRequestNoRollbackPurgeBetweenRequestAndSnapshot) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            5, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 5);

    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(7, streamSeqno);
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    ASSERT_TRUE(store_items(
            4, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 4);

    delete_item(vbid, StoredDocKey{"Beef1", CollectionEntry::meat});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Chicken", CollectionEntry::meat}, "soup");
    flushVBucketToDiskIfPersistent(vbid, 1);

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    // stream fruit (collection 9)
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["9"]})"}}));
    EXPECT_EQ(0, rollbackSeqno);

    purgeTombstonesBefore(vb->getHighSeqno());

    EXPECT_EQ(13, vb->getHighSeqno());

    // Client has all data for the collection, so nothing is produced yet and
    // is not instructed to rollback
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest, streamRequestNoRollbackMultiCollection) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            2, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto streamSeqno = vb->getHighSeqno();

    store_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(
            vbid, StoredDocKey{"KeyTwo", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    purgeTombstonesBefore(vb->getHighSeqno());

    EXPECT_EQ(6, streamSeqno);
    EXPECT_EQ(9, vb->getHighSeqno());
    EXPECT_EQ(8, vb->getPurgeSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["8","9"]})"}}));
    EXPECT_EQ(0, rollbackSeqno);

    // A backfill is scheduled, but as the startSeqno of the backfill (7) is
    // higher than all collection data on disk, there's no need to scan. DCP
    // produces nothing yet, the client is up-to-date. Must run the backfill
    // to completion
    runBackfill();
    // nothing from step
    EXPECT_EQ(cb::engine_errc::would_block,
              producer->stepWithBorderGuard(*producers));

    // Store data and the stream wakes up
    store_item(vbid, StoredDocKey{"Apple", CollectionEntry::fruit}, "value");
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, true);
    EXPECT_EQ(streamSeqno, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("Apple", producers->last_key);

    // should be no more ops
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest, streamRequestRollbackMultiCollection) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()),
            Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    ASSERT_TRUE(store_items(
            2, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice"));
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto streamSeqno = vb->getHighSeqno();

    store_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Lamb", CollectionEntry::meat}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    purgeTombstonesBefore(vb->getHighSeqno());

    EXPECT_EQ(6, streamSeqno);
    EXPECT_EQ(9, vb->getHighSeqno());
    EXPECT_EQ(8, vb->getPurgeSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(cb::engine_errc::rollback,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["8","9"]})"}}));
    EXPECT_EQ(0, rollbackSeqno);

    streamSeqno = rollbackSeqno;
    EXPECT_EQ(cb::engine_errc::success,
              producer->streamRequest(cb::mcbp::DcpAddStreamFlag::None,
                                      1, // opaque
                                      vbid,
                                      streamSeqno, // start_seqno
                                      ~0ull, // end_seqno
                                      vb->failovers->getLatestUUID(),
                                      streamSeqno, // snap_start_seqno,
                                      streamSeqno, // snap_end_seqno,
                                      &rollbackSeqno,
                                      &CollectionsDcpTest::dcpAddFailoverLog,
                                      {{R"({"collections":["8","9"]})"}}));

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(streamSeqno, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ("meat", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);

    uint64_t expectedSeqno{3};
    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(expectedSeqno++, producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("orange0", producers->last_key);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(expectedSeqno++, producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("orange1", producers->last_key);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(expectedSeqno++, producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ("Beef0", producers->last_key);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(expectedSeqno++, producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ("Beef1", producers->last_key);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::meat.getId(), producers->last_collection_id);
    EXPECT_EQ("Lamb", producers->last_key);

    // should be no more ops
    EXPECT_EQ(cb::engine_errc::would_block, producer->step(false, *producers));
}

TEST_P(CollectionsDcpStreamsTest, close_stream_validation1) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":99})"}}, vbid);

    EXPECT_EQ(cb::engine_errc::dcp_streamid_invalid,
              producer->closeStream(0, vbid, {}));
}

TEST_P(CollectionsDcpStreamsTest, close_stream_validation2) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);

    createDcpStream({{R"({"collections":["9"]})"}}, vbid);

    EXPECT_EQ(cb::engine_errc::dcp_streamid_invalid,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(99)));
}

TEST_P(CollectionsDcpStreamsTest, end_stream_for_state_change) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":42})"}}, vbid);
    createDcpStream({{R"({"collections":["0"], "sid":1984})"}}, vbid);

    store->setVBucketState(vbid, vbucket_state_replica);
    producer->closeStreamDueToVbStateChange(
            vbid, vbucket_state_replica, nullptr);

    // And mutations
    size_t endSize = sizeof(cb::mcbp::Request) +
                     sizeof(cb::mcbp::request::DcpStreamEndPayload) +
                     sizeof(cb::mcbp::DcpStreamIdFrameInfo);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamId(1984), producers->last_stream_id);
    EXPECT_EQ(endSize, producer->getBytesOutstanding());

    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamId(42), producers->last_stream_id);
    EXPECT_EQ(endSize * 2, producer->getBytesOutstanding());
}

// Core test method for close with a stream ID.
// Allows a number of cases to be covered where we have many streams mapped to
// the vbid and stream_end message enabled/disabled.
void CollectionsDcpStreamsTest::close_stream_by_id_test(bool enableStreamEnd,
                                                        bool manyStreams) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);
    if (enableStreamEnd) {
        producer->enableStreamEndOnClientStreamClose();
    }
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":99})"}}, vbid);

    if (manyStreams) {
        createDcpStream({{R"({"collections":["9", "0"], "sid":101})"}}, vbid);
        createDcpStream({{R"({"collections":["0"], "sid":555})"}}, vbid);
    }

    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(99)));
    {
        auto rv = producer->findStream(vbid, cb::mcbp::DcpStreamId(99));
        // enableStreamEnd:true we must still find the stream in the producer
        EXPECT_EQ(enableStreamEnd, rv.first != nullptr);
        // manyStreams:true we always expect to find streams
        // manyStreams:false only expect streams if enableStreamEnd:true
        EXPECT_EQ(enableStreamEnd | manyStreams, rv.second);
    }

    if (enableStreamEnd) {
        EXPECT_EQ(cb::engine_errc::success,
                  producer->stepAndExpect(
                          *producers, cb::mcbp::ClientOpcode::DcpStreamEnd));
        EXPECT_EQ(cb::mcbp::DcpStreamId(99), producers->last_stream_id);
    }

    {
        auto rv = producer->findStream(vbid, cb::mcbp::DcpStreamId(99));
        // Always expect at this point to not find stream for sid 99
        EXPECT_FALSE(rv.first);
        // However the vb may still have streams for manyStreams test
        EXPECT_EQ(manyStreams, rv.second);
    }

    // We should still have an element in vbToConns if we have many streams
    if (manyStreams) {
        auto& connmap = engine->getDcpConnMap();
        EXPECT_TRUE(connmap.vbConnectionExists(producer.get(), vbid));
    }
}

TEST_P(CollectionsDcpStreamsTest, close_stream_by_id_with_end_stream) {
    close_stream_by_id_test(true, false);
}

TEST_P(CollectionsDcpStreamsTest, close_stream_by_id) {
    close_stream_by_id_test(false, false);
}

TEST_P(CollectionsDcpStreamsTest,
       close_stream_by_id_with_end_stream_and_many_streams) {
    close_stream_by_id_test(true, true);
}

TEST_P(CollectionsDcpStreamsTest, close_stream_by_id_and_many_streams) {
    close_stream_by_id_test(false, true);
}

TEST_P(CollectionsDcpStreamsTest,
       close_stream_by_id_any_many_streams_removes_connhandler_for_last) {
    close_stream_by_id_test(false, true);

    // Close another of the streams
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(101)));

    // Still got 1 alive stream so the vbConn should still exist
    auto& connmap = engine->getDcpConnMap();
    EXPECT_TRUE(connmap.vbConnectionExists(producer.get(), vbid));

    // Close last stream
    EXPECT_EQ(cb::engine_errc::success,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(555)));

    // Should no longer have the ConnHandler in vbToConns
    EXPECT_FALSE(connmap.vbConnectionExists(producer.get(), vbid));
}

TEST_P(CollectionsDcpStreamsTest, two_streams) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);
    store_item(vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");

    producer->enableMultipleStreamRequests();

    // Two streams on the same collection
    createDcpStream({{R"({"sid":88, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":32, "collections":["9"]})"}});

    // Calling this will swallow the first snapshot marker
    notifyAndStepToCheckpoint();

    // snapshot marker + 3 bytes of stream-id in the buffer log. Manually
    // calculate the expected size. Header + extras + frame-info (streamid)
    size_t snapshotSz = sizeof(cb::mcbp::Request) +
                        sizeof(cb::mcbp::request::DcpSnapshotMarkerV1Payload) +
                        sizeof(cb::mcbp::DcpStreamIdFrameInfo);

    size_t outstanding = snapshotSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    // Step and get the second snapshot marker (for second stream)
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      *producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);

    outstanding += snapshotSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(32), producers->last_stream_id);

    size_t systemEventSz = sizeof(cb::mcbp::Request) +
                           sizeof(cb::mcbp::request::DcpSystemEventPayload) +
                           sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                           producers->last_system_event_data.size() +
                           sizeof("fruit") - 1; // adjust for \0
    outstanding += systemEventSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    producers->clear_dcp_data();
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);
    outstanding += systemEventSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    // And mutations
    size_t mutSize = sizeof(cb::mcbp::Request) +
                     sizeof(cb::mcbp::request::DcpMutationPayload) +
                     sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                     (sizeof("orange") - 1) + (sizeof("nice") - 1) +
                     1 /*collection-id*/;
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(cb::mcbp::DcpStreamId(32), producers->last_stream_id);
    outstanding += mutSize;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);
    outstanding += mutSize;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());
}

TEST_P(CollectionsDcpStreamsTest, two_streams_different) {
    if (!isPersistent()) {
        // This was originally mis-categorized as to be fixed with
        // MB-62963. With the fix for MB-62963, the test still fails, since the
        // test below doesn't need a backfill. Opened MB-65365 to investigate
        // why the test fails for ephemeral buckets.
        GTEST_SKIP();
    }
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    auto vb = store->getVBucket(vbid);
    setCollections(cookie, cm);

    producer->enableMultipleStreamRequests();

    // Two streams on different collections
    createDcpStream({{R"({"sid":101, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":2018, "collections":["c"]})"}});

    // Calling this will swallow the first snapshot marker
    notifyAndStepToCheckpoint();

    // Step and get the second snapshot marker (for second stream)
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(
                      *producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(101), producers->last_stream_id);

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ("dairy", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(2018), producers->last_stream_id);

    producers->clear_dcp_data();
    EXPECT_EQ(cb::engine_errc::success,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::BeginCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(101), producers->last_stream_id);
}

/**
 * MB-41092: Test to ensure that we scheduled compaction after a pending vbucket
 * after the following steps occur:
 * 1. disk snapshot marker received
 * 2. collection creation system event received for disk snapshot
 * 3. mutation for the collection received for disk snapshot
 * 4. memory snapshot marker received
 * 5. collection drop system event received for memory snapshot.
 * 6. Flush of items for steps 2, 3 and 5
 *
 * As before we MB-41092 we wouldn't schedule compaction as
 * Flush::countNonEmptyDroppedCollections() would be given the persisted seqno
 * instead of the high seqno of the collection. Which in this case the persisted
 * seqno would be 0 as we've not flushed anything in the collection at the point
 * we've enqueued the collection drop system event.
 */
TEST_P(CollectionsDcpStreamsTest,
       MB_41092_Ensure_compaction_scheduled_dropped_collection) {
    if (!isPersistent()) {
        GTEST_SKIP();
    }
    // set the vbucket to the pending state
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_pending);
    auto vbucketPtr = store->getVBucket(replicaVB);

    // enable V7 DCP status codes to make it easier to debug if there is no
    // stream or opaque miss match
    consumer->enableV7DcpStatus();
    const uint32_t opaque = 1;
    consumer->addStream(opaque, replicaVB, cb::mcbp::DcpAddStreamFlag::None);

    // create a disk snapshot of range {1 ... 3}
    uint64_t snapStart = 1;
    uint64_t snapEnd = 3;
    // set flags for disk snapshot and checkpoint range
    auto flags =
            DcpSnapshotMarkerFlag::Disk | DcpSnapshotMarkerFlag::Checkpoint;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       replicaVB,
                                       snapStart,
                                       snapEnd,
                                       flags,
                                       {},
                                       {},
                                       {},
                                       {}));
    // double check no items on disk
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());
    // set seqno at the begging of snapshot
    auto seqno = snapStart;
    // create a DCP collection creation event
    Collections::ManifestUid manifestUid;
    Collections::CreateEventDcpData createEventDcpData{
            {manifestUid,
             {ScopeEntry::defaultS.getId(),
              CollectionEntry::dairy.getId(),
              CollectionEntry::dairy.name,
              {},
              CanDeduplicate::Yes,
              Collections::Metered::Yes,
              Collections::ManifestUid{}}}};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      opaque,
                      replicaVB,
                      mcbp::systemevent::id::BeginCollection,
                      seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(
                               CollectionEntry::dairy.name.data()),
                       CollectionEntry::dairy.name.size()},
                      {reinterpret_cast<const uint8_t*>(&createEventDcpData),
                       Collections::CreateEventDcpData::size}));
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());

    // Store a document in the collection
    EXPECT_EQ(cb::engine_errc::success,
              consumer->mutation(
                      opaque,
                      StoredDocKey("key", CollectionEntry::dairy.getId()),
                      {},
                      0,
                      123,
                      replicaVB,
                      0,
                      ++seqno,
                      0,
                      0,
                      0,
                      {},
                      0));
    // Ensure on disk item count is 0 as we've not flushed anything
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());
    ++seqno;
    // Update snap range for next snapshot marker to contain the collection
    // drop SystemEvent
    snapStart = seqno;
    snapEnd = snapStart + 1;
    // set flags for memory snapshot and checkpoint range
    flags = DcpSnapshotMarkerFlag::Memory | DcpSnapshotMarkerFlag::Checkpoint;
    EXPECT_EQ(cb::engine_errc::success,
              consumer->snapshotMarker(opaque,
                                       replicaVB,
                                       snapStart,
                                       snapEnd,
                                       flags,
                                       {},
                                       {},
                                       {},
                                       {}));

    // create collection drop system event and apply it to the pending vbucket
    Collections::DropEventDcpData dropEventDcpData{
            {Collections::ManifestUid{manifestUid++},
             ScopeEntry::defaultS.getId(),
             CollectionEntry::dairy.getId(),
             false}};
    EXPECT_EQ(cb::engine_errc::success,
              consumer->systemEvent(
                      opaque,
                      replicaVB,
                      mcbp::systemevent::id::EndCollection,
                      ++seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(
                               CollectionEntry::dairy.name.data()),
                       CollectionEntry::dairy.name.size()},
                      {reinterpret_cast<const uint8_t*>(&dropEventDcpData),
                       Collections::DropEventDcpData::size}));
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());

    // Flush collection create, mutation and collection drop events to disk
    flushVBucketToDiskIfPersistent(replicaVB, 3);
    // ensure that our on disk doc count is 1 for the mutation as we've not
    // yet erased the collection on disk. But with the fix for MB-41092 we
    // should have scheduled compaction to do this, as before we wouldn't.
    EXPECT_EQ(1, vbucketPtr->getNumTotalItems());

    // ensure we've schedule compaction after the collection drop and that after
    // compaction is run the on disk doc count is 0
    runCollectionsEraser(replicaVB);
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());
}

INSTANTIATE_TEST_SUITE_P(CollectionsDcpStreamsTests,
                         CollectionsDcpStreamsTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);

class CollectionsDcpStreamsTestWithDurationZero
    : public CollectionsDcpStreamsTest {
public:
    void SetUp() override {
        config_string += "dcp_producer_processor_run_duration_us=0";
        CollectionsDcpStreamsTest::SetUp();
    }
};

TEST_P(CollectionsDcpStreamsTestWithDurationZero, multi_stream_time_limited) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    setCollections(cookie, cm);

    store_item(vbid, StoredDocKey{"k", CollectionEntry::fruit}, "v");
    store_item(vbid, StoredDocKey{"k", CollectionEntry::dairy}, "v");

    producer->enableMultipleStreamRequests();

    createDcpStream({{R"({"sid":101, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":2018, "collections":["c"]})"}});
    createDcpStream({{R"({"sid":218, "collections":["c"]})"}});

    EXPECT_EQ(0, producer->getStreamAggStats().readyQueueMemory);

    // With the test config setting duration to 0, multiple steps are needed
    // to get all stream data moved into the readyQ
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());

    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());

    // Now call run on the snapshot task to move checkpoint into DCP
    // stream - only 1 stream can be processed with the time limit.
    producer->getCheckpointSnapshotTask()->run();

    auto memory = producer->getStreamAggStats().readyQueueMemory;
    EXPECT_NE(0, memory);

    // 1 stream was processed leaving 2
    EXPECT_EQ(2, producer->getCheckpointSnapshotTask()->getStreamsSize());

    producer->getCheckpointSnapshotTask()->run();
    // 1 stream was processed leaving 2
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->getStreamsSize());

    auto memory2 = producer->getStreamAggStats().readyQueueMemory;
    EXPECT_GT(memory2, memory);
    memory = memory2;

    producer->getCheckpointSnapshotTask()->run();

    // 1 stream was processed leaving 0
    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());

    memory2 = producer->getStreamAggStats().readyQueueMemory;
    EXPECT_GT(memory2, memory);
    memory = memory2;

    producer->getCheckpointSnapshotTask()->run();
    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());
    memory2 = producer->getStreamAggStats().readyQueueMemory;
    EXPECT_EQ(memory2, memory);
}

INSTANTIATE_TEST_SUITE_P(CollectionsDcpStreamsTestsWithDurationZero,
                         CollectionsDcpStreamsTestWithDurationZero,
                         STParameterizedBucketTest::persistentConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

class CollectionsDcpStreamsTestWithHugeDuration
    : public CollectionsDcpStreamsTest {
public:
    void SetUp() override {
        using namespace std::chrono;
        config_string +=
                "dcp_producer_processor_run_duration_us=" +
                std::to_string(duration_cast<microseconds>(hours(48)).count());
        CollectionsDcpStreamsTest::SetUp();
    }
};

// Test all streams are processed in one run (if time allows)
TEST_P(CollectionsDcpStreamsTestWithHugeDuration, multi_stream) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    setCollections(cookie, cm);

    store_item(vbid, StoredDocKey{"k", CollectionEntry::fruit}, "v");
    store_item(vbid, StoredDocKey{"k", CollectionEntry::dairy}, "v");

    producer->enableMultipleStreamRequests();

    createDcpStream({{R"({"sid":101, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":2018, "collections":["c"]})"}});
    createDcpStream({{R"({"sid":218, "collections":["c"]})"}});

    EXPECT_EQ(0, producer->getStreamAggStats().readyQueueMemory);

    // With the test config setting duration to 0, multiple steps are needed
    // to get all stream data moved into the readyQ
    EXPECT_EQ(1, producer->getCheckpointSnapshotTask()->queueSize());
    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());

    // Now call run on the snapshot task to move checkpoints into streams
    producer->getCheckpointSnapshotTask()->run();

    // Validate memory is increased from zero and that the streams container is
    // empty.
    auto memory = producer->getStreamAggStats().readyQueueMemory;
    EXPECT_NE(0, memory);
    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());

    // Calling run again to check for no oddity, e.g. readyQueueMemory remains
    // the same.
    producer->getCheckpointSnapshotTask()->run();
    EXPECT_EQ(0, producer->getCheckpointSnapshotTask()->getStreamsSize());
    EXPECT_EQ(producer->getStreamAggStats().readyQueueMemory, memory);
}

TEST_P(CollectionsDcpStreamsTest, OptimiseStartSeqno) {
    CollectionsManifest cm;
    // Move high-seq so new collection begins from > 0
    store_item(vbid, StoredDocKey{"k1", CollectionEntry::defaultC}, "v");
    store_item(vbid, StoredDocKey{"k2", CollectionEntry::defaultC}, "v");
    cm.add(CollectionEntry::fruit);
    setCollections(cookie, cm);
    flushVBucketToDiskIfPersistent(vbid, 3);
    auto vb = store->getVBucket(vbid);

    ensureDcpWillBackfill();

    // fruit collection
    createDcpStream({{R"({"collections":["9"]})"}});

    // Inspect backfill has starts with fruit
    auto& bfm = producer->getBFM();
    ASSERT_EQ(1, bfm.getNumBackfills());
    auto stream = producer->findStream(vbid);
    ASSERT_TRUE(stream);

    const auto& bf = dynamic_cast<const DCPBackfillBySeqno&>(
            bfm.getBackfill(stream->getBackfillUID()));
    auto seq = vb->getManifest().lock(CollectionEntry::fruit).getStartSeqno();
    ASSERT_GT(seq, 1);
    EXPECT_EQ(bf.getStartSeqno(), seq);
}

INSTANTIATE_TEST_SUITE_P(CollectionsDcpStreamsTestsWithHugeDuration,
                         CollectionsDcpStreamsTestWithHugeDuration,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
