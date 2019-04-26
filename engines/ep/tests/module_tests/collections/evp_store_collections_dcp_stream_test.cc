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

#include "dcp/response.h"
#include "kv_bucket.h"
#include "tests/mock/mock_dcp.h"
#include "tests/mock/mock_dcp_consumer.h"
#include "tests/mock/mock_dcp_producer.h"
#include "tests/mock/mock_synchronous_ep_engine.h"
#include "tests/module_tests/collections/collections_dcp_test.h"
#include "tests/module_tests/collections/test_manifest.h"

class CollectionsDcpStreamsTest : public CollectionsDcpTest {
public:
    CollectionsDcpStreamsTest() : CollectionsDcpTest() {
    }
    // Create producer without any streams.
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
        // Start vbucket as active to allow us to store items directly to it.
        store->setVBucketState(vbid, vbucket_state_active);
        producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
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
    vb->updateFromManifest({cm});

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

TEST_F(CollectionsDcpStreamsTest, close_stream_validation1) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest({cm});
    producer->enableMultipleStreamRequests();
    createDcpStream({{R"({"collections":["9"], "sid":99})"}}, vbid);

    EXPECT_EQ(ENGINE_DCP_STREAMID_INVALID, producer->closeStream(0, vbid, {}));
}

TEST_F(CollectionsDcpStreamsTest, close_stream_validation2) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest({cm});

    createDcpStream({{R"({"collections":["9"]})"}}, vbid);

    EXPECT_EQ(ENGINE_DCP_STREAMID_INVALID,
              producer->closeStream(0, vbid, cb::mcbp::DcpStreamId(99)));
}

// Core test method for close with a stream ID.
// Allows a number of cases to be covered where we have many streams mapped to
// the vbid and stream_end message enabled/disabled.
void CollectionsDcpStreamsTest::close_stream_by_id_test(bool enableStreamEnd,
                                                        bool manyStreams) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest({cm});
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
        EXPECT_EQ(
                ENGINE_SUCCESS,
                producer->stepAndExpect(producers.get(),
                                        cb::mcbp::ClientOpcode::DcpStreamEnd));
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
    vb->updateFromManifest({cm});

    producer->enableMultipleStreamRequests();

    // Two streams on the same collection
    createDcpStream({{R"({"sid":88, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":32, "collections":["9"]})"}});

    // Calling this will swallow the first snapshot marker
    notifyAndStepToCheckpoint();

    // Step and get the second snapshot marker (for second stream)
    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(32), producers->last_stream_id);

    producers->clear_dcp_data();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(88), producers->last_stream_id);
}

TEST_F(CollectionsDcpStreamsTest, two_streams_different) {
    CollectionsManifest cm;
    cm.add(CollectionEntry::fruit).add(CollectionEntry::dairy);
    auto vb = store->getVBucket(vbid);
    vb->updateFromManifest({cm});

    producer->enableMultipleStreamRequests();

    // Two streams on different collections
    createDcpStream({{R"({"sid":101, "collections":["9"]})"}});
    createDcpStream({{R"({"sid":2018, "collections":["c"]})"}});

    // Calling this will swallow the first snapshot marker
    notifyAndStepToCheckpoint();

    // Step and get the second snapshot marker (for second stream)
    EXPECT_EQ(
            ENGINE_SUCCESS,
            producer->stepAndExpect(producers.get(),
                                    cb::mcbp::ClientOpcode::DcpSnapshotMarker));
    EXPECT_EQ(cb::mcbp::DcpStreamId(101), producers->last_stream_id);

    // The producer will send the create fruit event twice, once per stream!
    // SystemEvent createCollection
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::dairy.getId(), producers->last_collection_id);
    EXPECT_EQ("dairy", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(2018), producers->last_stream_id);

    producers->clear_dcp_data();
    EXPECT_EQ(ENGINE_SUCCESS,
              producer->stepAndExpect(producers.get(),
                                      cb::mcbp::ClientOpcode::DcpSystemEvent));
    EXPECT_EQ(CollectionEntry::fruit.getId(), producers->last_collection_id);
    EXPECT_EQ("fruit", producers->last_key);
    EXPECT_EQ(mcbp::systemevent::id::CreateCollection,
              producers->last_system_event);
    EXPECT_EQ(cb::mcbp::DcpStreamId(101), producers->last_stream_id);
}
