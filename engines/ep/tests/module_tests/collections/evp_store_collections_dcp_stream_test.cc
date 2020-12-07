/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "checkpoint_manager.h"
#include "collections/manifest.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/response.h"
#include "failover-table.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/collections_test_helpers.h"
#include <utilities/test_manifest.h>

class CollectionsDcpStreamsTest : public CollectionsDcpTest {
public:
    CollectionsDcpStreamsTest() : CollectionsDcpTest() {
    }
    // Create producer without any streams.
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
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

TEST_F(CollectionsDcpStreamsTest, request_validation) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

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

TEST_F(CollectionsDcpStreamsTest, streamRequestNoRollbackSeqnoAdvanced) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(
            5, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 5);
    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    store_items(4, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 4);

    delete_item(vbid, StoredDocKey{"Beef1", CollectionEntry::meat});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Beef5", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    runCompaction(vbid, 0, true);

    EXPECT_EQ(7, streamSeqno);
    EXPECT_EQ(13, vb->getHighSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0,
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

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    // should be no more ops
    EXPECT_EQ(ENGINE_ERROR_CODE(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_F(CollectionsDcpStreamsTest, streamRequestNoRollbackNoSeqnoAdvanced) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_item(vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"orange1", CollectionEntry::fruit});
    flushVBucketToDiskIfPersistent(vbid, 1);

    runCompaction(vbid, 0, true);

    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(6, vb->getHighSeqno());
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    store_item(vbid, StoredDocKey{"Apple1", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 1);

    ensureDcpWillBackfill();

    store_item(vbid, StoredDocKey{"Apple2", CollectionEntry::fruit}, "nice");
    // flushVBucketToDiskIfPersistent(vbid, 1);

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0,
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
    EXPECT_EQ(vb->getHighSeqno() - 1, producers->last_snap_end_seqno);

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
    EXPECT_EQ(ENGINE_ERROR_CODE(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_F(CollectionsDcpStreamsTest,
       streamRequestNoRollbackPurgeBetweenRequestAndSnapshot) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(
            5, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 5);

    auto streamSeqno = vb->getHighSeqno();
    EXPECT_EQ(7, streamSeqno);
    EXPECT_EQ(streamSeqno,
              vb->getManifest().lock().getHighSeqno(CollectionEntry::fruit));

    store_items(4, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 4);

    delete_item(vbid, StoredDocKey{"Beef1", CollectionEntry::meat});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Chicken", CollectionEntry::meat}, "soup");
    flushVBucketToDiskIfPersistent(vbid, 1);

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0,
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

    runCompaction(vbid, 0, true);

    EXPECT_EQ(13, vb->getHighSeqno());

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(streamSeqno, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);

    // should be no more ops
    EXPECT_EQ(ENGINE_ERROR_CODE(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_F(CollectionsDcpStreamsTest, streamRequestNoRollbackMultiCollection) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(2, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto streamSeqno = vb->getHighSeqno();

    store_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(
            vbid, StoredDocKey{"KeyTwo", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    runCompaction(vbid, 0, true);

    EXPECT_EQ(6, streamSeqno);
    EXPECT_EQ(9, vb->getHighSeqno());
    EXPECT_EQ(8, vb->getPurgeSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0,
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

    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, false);
    EXPECT_EQ(streamSeqno, producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);

    store_item(vbid, StoredDocKey{"Apple", CollectionEntry::fruit}, "value");
    notifyAndStepToCheckpoint(cb::mcbp::ClientOpcode::DcpSnapshotMarker, true);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_start_seqno);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_snap_end_seqno);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpMutation,
                  cb::engine_errc::success);
    EXPECT_EQ(vb->getHighSeqno(), producers->last_byseqno);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("Apple", producers->last_key);

    // should be no more ops
    EXPECT_EQ(ENGINE_ERROR_CODE(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_F(CollectionsDcpStreamsTest, streamRequestRollbackMultiCollection) {
    CollectionsManifest cm(CollectionEntry::meat);
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(Collections::Manifest{std::string{cm}});
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(
            2, vbid, StoredDocKey{"orange", CollectionEntry::fruit}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    store_items(2, vbid, StoredDocKey{"Beef", CollectionEntry::meat}, "nice");
    flushVBucketToDiskIfPersistent(vbid, 2);

    auto streamSeqno = vb->getHighSeqno();

    store_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    delete_item(vbid, StoredDocKey{"Key", CollectionEntry::defaultC});
    flushVBucketToDiskIfPersistent(vbid, 1);

    store_item(vbid, StoredDocKey{"Lamb", CollectionEntry::meat}, "value");
    flushVBucketToDiskIfPersistent(vbid, 1);

    runCompaction(vbid, 0, true);

    EXPECT_EQ(6, streamSeqno);
    EXPECT_EQ(9, vb->getHighSeqno());
    EXPECT_EQ(8, vb->getPurgeSeqno());

    ensureDcpWillBackfill();

    uint64_t rollbackSeqno{0};
    EXPECT_EQ(ENGINE_ROLLBACK,
              producer->streamRequest(0,
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
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->streamRequest(0,
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
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpSystemEvent,
                  cb::engine_errc::success);
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
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
    EXPECT_EQ(ENGINE_ERROR_CODE(cb::engine_errc::would_block),
              producer->step(*producers));
}

TEST_F(CollectionsDcpStreamsTest, close_stream_validation1) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":99})"}}, vbid);

    EXPECT_EQ(ENGINE_DCP_STREAMID_INVALID, producer->closeStream(0, vbid, {}));
}

TEST_F(CollectionsDcpStreamsTest, close_stream_validation2) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    createDcpStream({{R"({"collections":["9"]})"}}, vbid);

    EXPECT_EQ(ENGINE_DCP_STREAMID_INVALID,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(99)));
}

TEST_F(CollectionsDcpStreamsTest, end_stream_for_state_change) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":42})"}}, vbid);
    createDcpStream({{R"({"collections":["0"], "sid":1984})"}}, vbid);

    store->setVBucketState(vbid, vbucket_state_replica);
    producer->closeStreamDueToVbStateChange(
            vbid, vbucket_state_replica, nullptr);

    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamId(1984), producers->last_stream_id);
    stepAndExpect(cb::mcbp::ClientOpcode::DcpStreamEnd);
    EXPECT_EQ(cb::mcbp::DcpStreamId(42), producers->last_stream_id);
}

// Core test method for close with a stream ID.
// Allows a number of cases to be covered where we have many streams mapped to
// the vbid and stream_end message enabled/disabled.
void CollectionsDcpStreamsTest::close_stream_by_id_test(bool enableStreamEnd,
                                                        bool manyStreams) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
    if (enableStreamEnd) {
        producer->enableStreamEndOnClientStreamClose();
    }
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":99})"}}, vbid);

    if (manyStreams) {
        createDcpStream({{R"({"collections":["9", "0"], "sid":101})"}}, vbid);
        createDcpStream({{R"({"collections":["0"], "sid":555})"}}, vbid);
    }

    EXPECT_EQ(ENGINE_SUCCESS,
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
        EXPECT_EQ(ENGINE_SUCCESS,
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

TEST_F(CollectionsDcpStreamsTest, close_stream_by_id_with_end_stream) {
    close_stream_by_id_test(true, false);
}

TEST_F(CollectionsDcpStreamsTest, close_stream_by_id) {
    close_stream_by_id_test(false, false);
}

TEST_F(CollectionsDcpStreamsTest,
       close_stream_by_id_with_end_stream_and_many_streams) {
    close_stream_by_id_test(true, true);
}

TEST_F(CollectionsDcpStreamsTest, close_stream_by_id_and_many_streams) {
    close_stream_by_id_test(false, true);
}

TEST_F(CollectionsDcpStreamsTest,
       close_stream_by_id_any_many_streams_removes_connhandler_for_last) {
    close_stream_by_id_test(false, true);

    // Close another of the streams
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(101)));

    // Still got 1 alive stream so the vbConn should still exist
    auto& connmap = engine->getDcpConnMap();
    EXPECT_TRUE(connmap.vbConnectionExists(producer.get(), vbid));

    // Close last stream
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(555)));

    // Should no longer have the ConnHandler in vbToConns
    EXPECT_FALSE(connmap.vbConnectionExists(producer.get(), vbid));
}

TEST_F(CollectionsDcpStreamsTest, two_streams) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));
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
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(
                      *producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);

    outstanding += snapshotSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(32), producers->last_stream_id);

    size_t systemEventSz = sizeof(cb::mcbp::Request) +
                           sizeof(cb::mcbp::request::DcpSystemEventPayload) +
                           sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                           sizeof(Collections::CreateEventDcpData) +
                           sizeof("fruit") - 1; // adjust for \0
    outstanding += systemEventSz;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    producers->clear_dcp_data();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
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
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(cb::mcbp::DcpStreamId(32), producers->last_stream_id);
    outstanding += mutSize;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());

    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpMutation));
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);
    outstanding += mutSize;
    EXPECT_EQ(outstanding, producer->getBytesOutstanding());
}

TEST_F(CollectionsDcpStreamsTest, two_streams_different) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest(makeManifest(cm));

    producer->enableMultipleStreamRequests();

    // Two streams on different collections
    createDcpStream({{R"({"sid":101, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":2018, "collections":["c"]})"}});

    // Calling this will swallow the first snapshot marker
    notifyAndStepToCheckpoint();

    // Step and get the second snapshot marker (for second stream)
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(
                      *producers, cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(101), producers->last_stream_id);

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ("dairy", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(2018), producers->last_stream_id);

    producers->clear_dcp_data();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(*producers,
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
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
TEST_F(CollectionsDcpStreamsTest,
       MB_41092_Ensure_compaction_scheduled_dropped_collection) {
    // set the vbucket to the pending state
    setVBucketStateAndRunPersistTask(replicaVB, vbucket_state_pending);
    auto vbucketPtr = store->getVBucket(replicaVB);

    // enable V7 DCP status codes to make it easier to debug if there is no
    // stream or opaque miss match
    consumer->enableV7DcpStatus();
    const uint32_t opaque = 1;
    consumer->addStream(opaque, replicaVB, 0);

    // create a disk snapshot of range {1 ... 3}
    uint64_t snapStart = 1;
    uint64_t snapEnd = 3;
    // set flags for disk snapshot and checkpoint range
    uint32_t flags = dcp_marker_flag_t::MARKER_FLAG_DISK |
                     dcp_marker_flag_t::MARKER_FLAG_CHK;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, replicaVB, snapStart, snapEnd, flags, {}, {}));
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
              {}}}};
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      opaque,
                      replicaVB,
                      mcbp::systemevent::id::CreateCollection,
                      seqno,
                      mcbp::systemevent::version::version0,
                      {reinterpret_cast<const uint8_t*>(
                               CollectionEntry::dairy.name.data()),
                       CollectionEntry::dairy.name.size()},
                      {reinterpret_cast<const uint8_t*>(&createEventDcpData),
                       Collections::CreateEventDcpData::size}));
    EXPECT_EQ(0, vbucketPtr->getNumTotalItems());

    // Store a document in the collection
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->mutation(
                      opaque,
                      StoredDocKey("key", CollectionEntry::dairy.getId()),
                      {},
                      0,
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

    // Update snap range for next snapshot marker to contain the collection
    // drop SystemEvent
    snapStart = seqno;
    snapEnd = snapStart + 1;
    // set flags for memory snapshot and checkpoint range
    flags = dcp_marker_flag_t::MARKER_FLAG_MEMORY |
            dcp_marker_flag_t::MARKER_FLAG_CHK;
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->snapshotMarker(
                      opaque, replicaVB, snapStart, snapEnd, flags, {}, {}));

    // create collection drop system event and apply it to the pending vbucket
    Collections::DropEventDcpData dropEventDcpData{
            {Collections::ManifestUid{manifestUid++},
             ScopeEntry::defaultS.getId(),
             CollectionEntry::dairy.getId()}};
    EXPECT_EQ(ENGINE_SUCCESS,
              consumer->systemEvent(
                      opaque,
                      replicaVB,
                      mcbp::systemevent::id::DeleteCollection,
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