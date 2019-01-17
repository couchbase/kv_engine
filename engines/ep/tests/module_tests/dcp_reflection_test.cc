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

/*
 * Unit tests for DCP which connecting a DCP Producer to a DCP Consumer.
 */

#include <memcached/protocol_binary.h>
#include <programs/engine_testapp/mock_server.h>
#include <tests/mock/mock_dcp_consumer.h>
#include <tests/mock/mock_dcp_producer.h>
#include <tests/mock/mock_stream.h>
#include <tests/mock/mock_synchronous_ep_engine.h>

#include "checkpoint_manager.h"
#include "dcp/response.h"
#include "evp_store_single_threaded_test.h"
#include "test_helpers.h"

/**
 * Test fixture which creates two ep-engine (bucket) instances, using one
 * as a source for DCP replication and the second as the destination.
 */
class DCPLoopbackStreamTest : public SingleThreadedKVBucketTest {
protected:
    void SetUp() override {
        SingleThreadedKVBucketTest::SetUp();
    }

    ENGINE_ERROR_CODE getInternalHelper(const DocKey& key) {
        return getInternal(key,
                           vbid,
                           cookie,
                           vbucket_state_t::vbucket_state_active,
                           get_options_t::NONE)
                .getStatus();
    }

    void setupProducer(EnableExpiryOutput enableExpiryOutput,
                       uint32_t exp_time = 0) {
        // Setup the source (active) Bucket.
        EXPECT_EQ(ENGINE_SUCCESS,
                  engine->getKVBucket()->setVBucketState(vbid,
                                                         vbucket_state_active));

        // Add some items to the source Bucket.
        auto key1 = makeStoredDocKey("key1");
        auto key2 = makeStoredDocKey("key2");
        auto key3 = makeStoredDocKey("key3");
        store_item(vbid, key1, "value", exp_time);
        store_item(vbid, key2, "value", exp_time);
        store_item(vbid, key3, "value", exp_time);

        // Create the Dcp producer.
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookie,
                IncludeDeleteTime::No);
        producer->scheduleCheckpointProcessorTask();

        // Setup conditions for expirations
        auto expectedGetOutcome = ENGINE_SUCCESS;
        if (enableExpiryOutput == EnableExpiryOutput::Yes) {
            producer->setDCPExpiry(true);
            expectedGetOutcome = ENGINE_KEY_ENOENT;
        }
        TimeTraveller t(1080);
        // Trigger expiries on a get, or just check that the key exists
        EXPECT_EQ(expectedGetOutcome, getInternalHelper(key1));
        EXPECT_EQ(expectedGetOutcome, getInternalHelper(key2));
        EXPECT_EQ(expectedGetOutcome, getInternalHelper(key3));

        auto& sourceVb = *engine->getVBucket(vbid);
        producer->mockActiveStreamRequest(consumerStream->getFlags(),
                                          consumerStream->getOpaque(),
                                          sourceVb,
                                          consumerStream->getStartSeqno(),
                                          consumerStream->getEndSeqno(),
                                          consumerStream->getVBucketUUID(),
                                          consumerStream->getSnapStartSeqno(),
                                          consumerStream->getSnapEndSeqno());
        producerStream = dynamic_cast<MockActiveStream*>(
                producer->findStream(vbid).get());

        ASSERT_EQ(2, sourceVb.checkpointManager->getNumOfCursors())
                << "Should have both persistence and DCP producer cursor on "
                   "source "
                   "VB";

        // Creating a producer will schedule one
        // ActiveStreamCheckpointProcessorTask
        // that task though sleeps forever, so won't run until woken.
        ASSERT_EQ(1, getLpAuxQ()->getFutureQueueSize());
    }

    void setupConsumer(uint32_t flags) {
        // In addition to the initial engine which is created; we also need
        // to create a second bucket instance for the destination (replica)
        // vBucket.
        std::string config = config_string;
        if (config.size() > 0) {
            config += ";";
        }
        config += "dbname=" + std::string(test_dbname) + "-replica";
        replicaEngine = SynchronousEPEngine::build(config);

        // Setup destination (replica) Bucket.
        EXPECT_EQ(ENGINE_SUCCESS,
                  replicaEngine->getKVBucket()->setVBucketState(
                          vbid, vbucket_state_replica));

        // Setup the consumer.
        consumer = std::make_shared<MockDcpConsumer>(
                *replicaEngine, cookie, "test_consumer");
        EXPECT_EQ(ENGINE_SUCCESS,
                  consumer->addStream(
                          /*opaque*/ 0, vbid, flags));
        consumerStream = consumer->getVbucketStream(vbid).get();

        // Need to discard the first message from the consumerStream (the
        // StreamRequest), as we'll manually set that up in the producer.
        {
            std::unique_ptr<DcpResponse> streamRequest(consumerStream->next());
            EXPECT_NE(nullptr, streamRequest);
            EXPECT_EQ(DcpResponse::Event::StreamReq, streamRequest->getEvent());
        }
    }

    void TearDown() override {
        producer->cancelCheckpointCreatorTask();
        producer->closeAllStreams();
        producer.reset();

        consumer->closeAllStreams();
        consumer.reset();
        shutdownAndPurgeTasks(replicaEngine.get());
        destroy_mock_cookie(cookie);
        cookie = nullptr;
        replicaEngine.reset();
        SingleThreadedKVBucketTest::TearDown();
    }

    TaskQueue* getLpAuxQ() const {
        auto* task_executor = reinterpret_cast<SingleThreadedExecutorPool*>(
                ExecutorPool::get());
        return task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
    }

    std::unique_ptr<DcpResponse> getNextProducerMsg(MockActiveStream* stream) {
        std::unique_ptr<DcpResponse> producerMsg(stream->next());
        if (!producerMsg) {
            EXPECT_EQ(1, getLpAuxQ()->getFutureQueueSize())
                    << "Expected to have ActiveStreamCheckpointProcessorTask "
                       "in future queue after null producerMsg";
            stream->nextCheckpointItemTask();
            EXPECT_GT(stream->getItemsRemaining(), 0)
                    << "Expected some items ready after calling "
                       "nextCheckpointItemTask()";
            return getNextProducerMsg(stream);
        }
        return producerMsg;
    }

    void readNextConsumerMsgAndSendToProducer(ActiveStream& producerStream,
                                              PassiveStream& consumerStream);

    void takeoverTest(EnableExpiryOutput enableExpiryOutput);

    std::unique_ptr<SynchronousEPEngine> replicaEngine;
    std::shared_ptr<MockDcpConsumer> consumer;
    // Non-owning ptr to consumer stream (owned by consumer).
    PassiveStream* consumerStream;

    std::shared_ptr<MockDcpProducer> producer;

    // Non-owning ptr to producer stream (owned by producer).
    MockActiveStream* producerStream;
};

void DCPLoopbackStreamTest::readNextConsumerMsgAndSendToProducer(
        ActiveStream& producerStream, PassiveStream& consumerStream) {
    std::unique_ptr<DcpResponse> consumerMsg(consumerStream.next());

    // Pass the consumer's message to the producer.
    if (consumerMsg) {
        switch (consumerMsg->getEvent()) {
        case DcpResponse::Event::SnapshotMarker:
            producerStream.snapshotMarkerAckReceived();
            break;
        case DcpResponse::Event::SetVbucket:
            producerStream.setVBucketStateAckRecieved();
            break;
        default:
            FAIL();
        }
    }
}

/**
 * Test the behaviour of a Takeover stream between a DcpProducer and
 * DcpConsumer.
 *
 * Creates a Producer and Consumer; along with a single Active -> Passive
 * stream, then makes a streamRequest (simulating what ns_server normally does).
 * Then loops; reading messages from the producer and passing them to the
 * consumer, and reading responses from the consumer and passing to the
 * producer. Test finishes when the PassiveStream is set to Dead - at that point
 * the vBucket should be active on the destination; and dead on the source.
 */
void DCPLoopbackStreamTest::takeoverTest(
        EnableExpiryOutput enableExpiryOutput) {
    uint32_t exp_time = 0;
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        exp_time = time(NULL) + 256;
    }

    // Note: the order matters.
    //     First, we setup the Consumer with the given flags and we discard the
    //     StreamRequest message from the Consumer::readyQ.
    //     Then, we simulate the Producer receiving the StreamRequest just
    //     by creating the Producer with the Consumer's flags
    setupConsumer(DCP_ADD_STREAM_FLAG_TAKEOVER);
    setupProducer(enableExpiryOutput, exp_time);

    // Both streams created. Check state is as expected.
    ASSERT_TRUE(producerStream->isTakeoverSend())
            << "Producer stream state should have transitioned to "
               "TakeoverSend";

    while (true) {
        auto producerMsg = getNextProducerMsg(producerStream);
        ASSERT_TRUE(producerMsg);

        // Cannot pass mutation/deletion directly to the consumer as the object
        // is different
        if (producerMsg->getEvent() == DcpResponse::Event::Mutation ||
            producerMsg->getEvent() == DcpResponse::Event::Deletion ||
            producerMsg->getEvent() == DcpResponse::Event::Expiration) {
            producerMsg = std::make_unique<MutationConsumerMessage>(
                    *static_cast<MutationResponse*>(producerMsg.get()));
        }

        // Pass the message onto the consumer.
        EXPECT_EQ(ENGINE_SUCCESS,
                  consumerStream->messageReceived(std::move(producerMsg)));

        // Get the next message from the consumer; and pass to the producer.
        readNextConsumerMsgAndSendToProducer(*producerStream, *consumerStream);

        // Check consumer stream state - drop reflecting messages when
        // stream goes dead.
        if (!consumerStream->isActive()) {
            break;
        }
    }

    auto* sourceVb = engine->getVBucket(vbid).get();
    EXPECT_EQ(vbucket_state_dead, sourceVb->getState())
            << "Expected producer vBucket to be dead once stream "
               "transitions to dead.";

    auto* destVb = replicaEngine->getVBucket(vbid).get();
    EXPECT_EQ(vbucket_state_active, destVb->getState())
            << "Expected consumer vBucket to be active once stream "
               "transitions to dead.";

    // Check final state of items
    auto num_left = 3, expired = 0;
    auto expectedOutcome = ENGINE_SUCCESS;
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        num_left = 0, expired = 3;
        expectedOutcome = ENGINE_KEY_ENOENT;
    }
    EXPECT_EQ(num_left, sourceVb->getNumItems());
    EXPECT_EQ(num_left, destVb->getNumItems());
    EXPECT_EQ(expired, sourceVb->numExpiredItems);
    // numExpiredItems is a stat for recording how many items have been flipped
    // from active to expired on a vbucket, so does not get transferred during
    // a takeover.

    auto key1 = makeStoredDocKey("key1");
    EXPECT_EQ(expectedOutcome, getInternalHelper(key1));
}

TEST_F(DCPLoopbackStreamTest, Takeover) {
    takeoverTest(EnableExpiryOutput::No);
}

TEST_F(DCPLoopbackStreamTest, TakeoverWithExpiry) {
    takeoverTest(EnableExpiryOutput::Yes);
}
