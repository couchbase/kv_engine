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
        store->setVBucketState(vbid, vbucket_state_active, false);
        producers = std::make_unique<CollectionsDcpTestProducers>(engine.get());
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookieP, IncludeDeleteTime::No);
        CollectionsDcpTest::consumer = std::make_shared<MockDcpConsumer>(
                *engine, cookieC, "test_consumer");
    }
};

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