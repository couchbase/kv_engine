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
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>
#include <tests/mock/mock_checkpoint_manager.h>
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

        // Paranoia - remove any previous replica disk files.
        try {
            cb::io::rmrf(std::string(test_dbname) + "-replica");
        } catch (std::system_error& e) {
            if (e.code() != std::error_code(ENOENT, std::system_category())) {
                throw e;
            }
        }

        ASSERT_EQ(ENGINE_SUCCESS,
                  engine->getKVBucket()->setVBucketState(
                          vbid,
                          vbucket_state_active,
                          {{"topology",
                            nlohmann::json::array({{"active", "replica"}})}}));
    }

    ENGINE_ERROR_CODE getInternalHelper(const DocKey& key) {
        return getInternal(key,
                           vbid,
                           cookie,
                           vbucket_state_t::vbucket_state_active,
                           get_options_t::NONE)
                .getStatus();
    }

    void setupProducer(
            EnableExpiryOutput enableExpiryOutput = EnableExpiryOutput::Yes,
            SyncReplication syncReplication = SyncReplication::Yes) {
        // Create the Dcp producer.
        producer = SingleThreadedKVBucketTest::createDcpProducer(
                cookie,
                IncludeDeleteTime::No);
        producer->scheduleCheckpointProcessorTask();
        if (enableExpiryOutput == EnableExpiryOutput::Yes) {
            producer->setDCPExpiry(true);
        }
        producer->setSyncReplication(syncReplication == SyncReplication::Yes);

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

        ASSERT_EQ(2,
                  static_cast<MockCheckpointManager*>(
                          sourceVb.checkpointManager.get())
                          ->getNumOfCursors())
                << "Should have both persistence and DCP producer cursor on "
                   "source VB";

        // Creating a producer will schedule one
        // ActiveStreamCheckpointProcessorTask, and potentially a Backfill task.
        // The ActiveStreamCheckpointProcessorTask task though sleeps forever,
        // so won't run until woken.
        ASSERT_GE(getLpAuxQ()->getFutureQueueSize(), 1);
    }

    void setupConsumer(uint32_t flags = 0) {
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
        flushReplicaIfPersistent();

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

    ENGINE_ERROR_CODE storePrepare(std::string key) {
        auto docKey = makeStoredDocKey(key);
        using namespace cb::durability;
        auto reqs = Requirements(Level::Majority, Timeout::Infinity());
        return store->set(*makePendingItem(docKey, {}, reqs), cookie);
    }

    ENGINE_ERROR_CODE storeCommit(std::string key) {
        auto docKey = makeStoredDocKey(key);
        auto vb = engine->getVBucket(vbid);
        return vb->commit(docKey, 1, {}, vb->lockCollections(docKey));
    }

    ENGINE_ERROR_CODE storeSet(std::string key) {
        auto docKey = makeStoredDocKey(key);
        return store->set(*makeCommittedItem(docKey, {}), cookie);
    }

    /**
     * Flush all outstanding items to disk on the replica (if persistent).
     */
    void flushReplicaIfPersistent() {
        if (replicaEngine->getConfiguration().getBucketType() == "persistent") {
            auto& replicaKVB = *replicaEngine->getKVBucket();
            dynamic_cast<EPBucket&>(replicaKVB).flushVBucket(vbid);
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
            EXPECT_GE(getLpAuxQ()->getFutureQueueSize(), 1)
                    << "Expected to have at least "
                       "ActiveStreamCheckpointProcessorTask "
                       "in future queue after null producerMsg";

            // Run the next waiting task to populate the streams' items.
            CheckedExecutor executor(task_executor, *getLpAuxQ());
            executor.runCurrentTask();
            executor.completeCurrentTask();
            if (!stream->getItemsRemaining()) {
                return {};
            }
            return getNextProducerMsg(stream);
        }

        // Cannot pass mutation/deletion directly to the consumer as the object
        // is different
        if (producerMsg->getEvent() == DcpResponse::Event::Mutation ||
            producerMsg->getEvent() == DcpResponse::Event::Deletion ||
            producerMsg->getEvent() == DcpResponse::Event::Expiration ||
            producerMsg->getEvent() == DcpResponse::Event::Prepare) {
            producerMsg = std::make_unique<MutationConsumerMessage>(
                    *static_cast<MutationResponse*>(producerMsg.get()));
        }

        return producerMsg;
    }

    void readNextConsumerMsgAndSendToProducer(ActiveStream& producerStream,
                                              PassiveStream& consumerStream);

    void takeoverTest(EnableExpiryOutput enableExpiryOutput);

    /**
     * Test the behaviour of switching betweeen Disk and Memory phases of a DCP
     * stream, where Prepared SyncWrites to the same key appear in each of the
     * Disk and Memory snapshots. This _should_ be permitted, but MB-35001
     * highlight and issue where there different prepares were put into the same
     * Checkpoint on the replica, which isn't permitted.
     *
     * Consider the following scenario of items on disk and in memory
     * (checkpoint manager):
     *
     *  Disk:
     *      1:PRE(a), 2:CMT(a), 3:SET(b)
     *
     *  Memory:
     *                          3:CKPT_START
     *                          3:SET(b),     4:PRE(a), 5:SET(c)
     *
     * (items 1..2 were in a removed checkpoint and no longer in-memory.)
     *
     * An ep-engine replica attempting to stream all of this (0..infinity) will
     * result in a backfill of items 1..3, with a checkpoint cursor being placed
     * at seqno:4. Note this isn't the start of the Checkpoint (which is 3) and
     * hence not pointing at a checkpoint_start item.
     *
     * As such when this is streamed over DCP (up to seqno:4) the consumer will
     * see:
     *
     *     SNAPSHOT_MARKER(start=1, end=3, flags=DISK|CKPT)
     *     1:PRE(a)
     *     2:CMT(a)
     *     3:SET(b)
     *     SNAPSHOT_MARKER(start=4, end=5, flags=MEM)
     *     4:PRE(a),
     *     [[[missing seqno 5]]
     *
     * If the consumer puts all of these mutations in the same Checkpoint, then
     * it will result in duplicate PRE(a) items (which breaks Checkpoint
     * invariant).
     *
     * @param flags Flags to use when creating the ADD_STREAM request.
     */
    void testBackfillAndInMemoryDuplicatePrepares(uint32_t flags);

    SynchronousEPEngineUniquePtr replicaEngine;
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

    // Add some items to the source Bucket.
    std::vector<StoredDocKey> keys;
    keys.push_back(makeStoredDocKey("key1"));
    keys.push_back(makeStoredDocKey("key2"));
    keys.push_back(makeStoredDocKey("key3"));
    for (const auto& key : keys) {
        store_item(vbid, key, "value", exp_time);
    }

    // Setup conditions for expirations
    auto expectedGetOutcome = ENGINE_SUCCESS;
    if (enableExpiryOutput == EnableExpiryOutput::Yes) {
        expectedGetOutcome = ENGINE_KEY_ENOENT;
    }
    TimeTraveller t(1080);
    // Trigger expiries on a get, or just check that the key exists
    for (const auto& key : keys) {
        EXPECT_EQ(expectedGetOutcome, getInternalHelper(key));
    }

    // Note: the order matters.
    //     First, we setup the Consumer with the given flags and we discard the
    //     StreamRequest message from the Consumer::readyQ.
    //     Then, we simulate the Producer receiving the StreamRequest just
    //     by creating the Producer with the Consumer's flags
    setupConsumer(DCP_ADD_STREAM_FLAG_TAKEOVER);
    setupProducer(enableExpiryOutput);

    // Both streams created. Check state is as expected.
    ASSERT_TRUE(producerStream->isTakeoverSend())
            << "Producer stream state should have transitioned to "
               "TakeoverSend";

    while (true) {
        auto producerMsg = getNextProducerMsg(producerStream);
        ASSERT_TRUE(producerMsg);

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

void DCPLoopbackStreamTest::testBackfillAndInMemoryDuplicatePrepares(
        uint32_t flags) {
    // First checkpoint 1..2: PRE(a), CMT(a)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, storePrepare("a"));
    EXPECT_EQ(ENGINE_SUCCESS, storeCommit("a"));

    // Second checkpoint 3..5: SET(b), PRE(a), SET(c)
    auto vb = engine->getVBucket(vbid);
    vb->checkpointManager->createNewCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS, storeSet("b"));

    // Flush up to seqno:3 to disk.
    flushVBucketToDiskIfPersistent(vbid, 3);

    // Add 4:PRE(a), 5:SET(c)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, storePrepare("a"));
    EXPECT_EQ(ENGINE_SUCCESS, storeSet("c"));

    // Remove the first checkpoint (to force a DCP backfill).
    bool newCkpt = false;
    ASSERT_EQ(2,
              vb->checkpointManager->removeClosedUnrefCheckpoints(
                      *vb, newCkpt, 1));
    ASSERT_FALSE(newCkpt);
    /* State is now:
     *  Disk:
     *      1:PRE(a), 2:CMT(a), 3:SET(b)
     *
     *  Memory:
     *                          3:CKPT_START
     *                          3:SET(b),     4:PRE(a), 5:SET(c)
     */

    // Setup: Create DCP producer and consumer connections.
    setupConsumer(flags);
    setupProducer();

    // Test: Transfer 6 messages between Producer and Consumer
    // (SNAP_MARKER, PRE, CMT, SET), (SNAP_MARKER, PRE), with a flush after the
    // first 4.
    for (int i = 0; i < 4; i++) {
        EXPECT_EQ(ENGINE_SUCCESS,
                  consumerStream->messageReceived(
                          getNextProducerMsg(producerStream)));
    }
    flushReplicaIfPersistent();

    // Transfer 2 more messages (SNAP_MARKER, PRE)
    EXPECT_EQ(ENGINE_SUCCESS,
              consumerStream->messageReceived(
                      getNextProducerMsg(producerStream)));
    EXPECT_EQ(ENGINE_SUCCESS,
              consumerStream->messageReceived(
                      getNextProducerMsg(producerStream)));
    flushReplicaIfPersistent();
}

TEST_F(DCPLoopbackStreamTest, BackfillAndInMemoryDuplicatePrepares) {
    testBackfillAndInMemoryDuplicatePrepares(0);
}

TEST_F(DCPLoopbackStreamTest, BackfillAndInMemoryDuplicatePreparesTakeover) {
    // Variant with takeover stream, which has a different memory-based state.
    testBackfillAndInMemoryDuplicatePrepares(DCP_ADD_STREAM_FLAG_TAKEOVER);
}

/*
 * Test a similar scenario to testBackfillAndInMemoryDuplicatePrepares(), except
 * here we start in In-Memory and transition to backfilling via cursor dropping.
 *
 * The test scenario is such that there is a duplicate Prepare (same key) in
 * the initial In-Memory and then the Backfill snapshot.
 */
TEST_F(DCPLoopbackStreamTest, InMemoryAndBackfillDuplicatePrepares) {
    // First checkpoint 1..2:
    //     1:PRE(a)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, storePrepare("a"));

    // Setup: Create DCP connections; and stream the first 2 items (SNAP, 1:PRE)
    setupConsumer();
    setupProducer();
    auto msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, msg->getEvent());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));
    msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::Prepare, msg->getEvent());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    //     2:CMT(a)
    EXPECT_EQ(ENGINE_SUCCESS, storeCommit("a"));

    // Create second checkpoint 3..4: 3:SET(b)
    auto vb = engine->getVBucket(vbid);
    vb->checkpointManager->createNewCheckpoint();
    //     3:SET(b)
    EXPECT_EQ(ENGINE_SUCCESS, storeSet("b"));

    // Flush up to seqno:3 to disk.
    flushVBucketToDiskIfPersistent(vbid, 3);

    //     4:PRE(a)
    //     5:SET(c)
    EXPECT_EQ(ENGINE_EWOULDBLOCK, storePrepare("a"));
    EXPECT_EQ(ENGINE_SUCCESS, storeSet("c"));

    // Trigger cursor dropping; then remove (now unreferenced) first checkpoint.
    ASSERT_TRUE(producer->handleSlowStream(
            vbid, producerStream->getCursor().lock().get()));
    bool newCkpt = false;
    ASSERT_EQ(2,
              vb->checkpointManager->removeClosedUnrefCheckpoints(
                      *vb, newCkpt, 1));
    ASSERT_FALSE(newCkpt);

    /* State is now:
     *  Disk:
     *      1:PRE(a), 2:CMT(a),   3:SET(b)
     *
     *  Memory:
     *     [1:PRE(a), 2:CMT(a)]  [3:CKPT_START
     *                            3:SET(b),     4:PRE(a), 5:SET(c)
     *
     *                ^
     *                DCP Cursor
     */

    // Test: Transfer next 2 messages from Producer to Consumer which
    // should be from backfill (after cursor dropping):
    // SNAP_MARKER (disk), 2:CMT
    msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, msg->getEvent());
    auto* marker = dynamic_cast<SnapshotMarker*>(msg.get());
    EXPECT_EQ(2, marker->getStartSeqno());
    EXPECT_EQ(3, marker->getEndSeqno());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    msg = getNextProducerMsg(producerStream);
    // Note: This was originally a Commit but because it has come from disk
    // it's sent as a Mutation (as backfill in general doesn't know if consumer
    // recieved the prior prepare so must send as Mutation).
    EXPECT_EQ(DcpResponse::Event::Mutation, msg->getEvent());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::Mutation, msg->getEvent());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    // Transfer 2 memory messages - should be:
    // SNAP_MARKER (mem), 4:PRE
    msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::SnapshotMarker, msg->getEvent());
    marker = dynamic_cast<SnapshotMarker*>(msg.get());
    EXPECT_EQ(4, marker->getStartSeqno());
    EXPECT_EQ(5, marker->getEndSeqno());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    msg = getNextProducerMsg(producerStream);
    EXPECT_EQ(DcpResponse::Event::Prepare, msg->getEvent());
    EXPECT_EQ(ENGINE_SUCCESS, consumerStream->messageReceived(std::move(msg)));

    flushReplicaIfPersistent();
}
