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

// Indexes for the engines we will use in the tests, a single array allows test
// code to locate the engine for the Node
using Node = int;
static Node Node0 = 0;
static Node Node1 = 1;
static Node Node2 = 2;
static Node Node3 = 3;

static TaskQueue* getLpAuxQ() {
    auto* task_executor =
            reinterpret_cast<SingleThreadedExecutorPool*>(ExecutorPool::get());
    return task_executor->getLpTaskQ()[AUXIO_TASK_IDX];
}

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
        // Always stash KVBucketTest::engine in engines as Node0
        engines[Node0] = engine.get();

        // Always create Node1
        createNode(Node1, vbucket_state_replica);
    }

    ENGINE_ERROR_CODE getInternalHelper(const DocKey& key) {
        return getInternal(key,
                           vbid,
                           cookie,
                           vbucket_state_t::vbucket_state_active,
                           get_options_t::NONE)
                .getStatus();
    }

    void createNode(Node node, vbucket_state_t vbState) {
        ASSERT_NE(Node0, node) << "Cannot re-create Node0";
        ASSERT_LE(node, Node3) << "Out of bounds for Node" << node;

        std::string config = config_string;
        if (config.size() > 0) {
            config += ";";
        }
        config += "dbname=" + std::string(test_dbname) + "-node_" +
                  std::to_string(node);
        extraEngines.push_back(SynchronousEPEngine::build(config));
        engines[node] = extraEngines.back().get();

        // Setup one vbucket in the requested state
        EXPECT_EQ(ENGINE_SUCCESS,
                  engines[node]->getKVBucket()->setVBucketState(vbid, vbState));
        flushNodeIfPersistent(node);
    }

    /**
     * DcpRoute connects nodes together and provides methods for joining
     * the streams and "sending" messages. A route can be destroyed as well
     * for simulation of connection failures
     */
    class DcpRoute {
    public:
        DcpRoute(Vbid vbid,
                 EventuallyPersistentEngine* producerNode,
                 std::shared_ptr<MockDcpProducer> producer,
                 std::shared_ptr<MockDcpConsumer> consumer)
            : vbid(vbid),
              producerNode(producerNode),
              producer(producer),
              consumer(consumer) {
        }

        ~DcpRoute() {
            destroy();
        }

        void destroy();

        std::pair<cb::engine_errc, uint64_t> doStreamRequest(int flags = 0);
        std::unique_ptr<DcpResponse> getNextProducerMsg(ActiveStream* stream);

        void transferMessage();
        void transferMessage(DcpResponse::Event expectedEvent);

        void transferMutation(const StoredDocKey& expectedKey,
                              uint64_t expectedSeqno);

        void transferSnapshotMarker(uint64_t expectedStart,
                                    uint64_t expectedEnd,
                                    uint32_t expectedFlags);

        void transferResponseMessage();

        std::pair<ActiveStream*, PassiveStream*> getStreams();

        Vbid vbid;
        EventuallyPersistentEngine* producerNode;
        std::shared_ptr<MockDcpProducer> producer;
        std::shared_ptr<MockDcpConsumer> consumer;
    };

    // Create a route between two nodes, result in the creation of a DCP
    // producer and consumer object
    DcpRoute createDcpRoute(
            Node producerNode,
            Node consumerNode,
            EnableExpiryOutput producerExpiryOutput = EnableExpiryOutput::Yes) {
        EXPECT_TRUE(engines[producerNode])
                << " createDcpRoute: No engine for producer Node"
                << producerNode;
        EXPECT_TRUE(engines[consumerNode])
                << "createDcpRoute: No engine for consumer Node"
                << consumerNode;
        return {vbid,
                engines[producerNode],
                createDcpProducer(
                        producerNode, consumerNode, producerExpiryOutput),
                createDcpConsumer(producerNode, consumerNode)};
    }

    static ENGINE_ERROR_CODE fakeDcpAddFailoverLog(
            vbucket_failover_t* entry,
            size_t nentries,
            gsl::not_null<const void*> cookie) {
        return ENGINE_SUCCESS;
    }

    std::shared_ptr<MockDcpProducer> createDcpProducer(
            Node producerNode,
            Node consumerNode,
            EnableExpiryOutput enableExpiryOutput = EnableExpiryOutput::Yes,
            SyncReplication syncReplication = SyncReplication::Yes) {
        EXPECT_TRUE(engines[producerNode])
                << "createDcpProducer: No engine for Node" << producerNode;

        int flags = cb::mcbp::request::DcpOpenPayload::IncludeXattrs |
                    cb::mcbp::request::DcpOpenPayload::IncludeDeleteTimes;
        auto producer = std::make_shared<MockDcpProducer>(
                *engines[producerNode],
                create_mock_cookie(),
                "Node" + std::to_string(producerNode) + " to Node" +
                        std::to_string(consumerNode),
                flags,
                false /*startTask*/);

        // Create the task object, but don't schedule
        producer->createCheckpointProcessorTask();

        // Need to enable NOOP for XATTRS (and collections).
        producer->setNoopEnabled(true);

        producer->scheduleCheckpointProcessorTask();
        if (enableExpiryOutput == EnableExpiryOutput::Yes) {
            producer->setDCPExpiry(true);
        }

        producer->setSyncReplication(syncReplication == SyncReplication::Yes);

        return producer;
    }

    std::shared_ptr<MockDcpConsumer> createDcpConsumer(Node producerNode,
                                                       Node consumerNode) {
        EXPECT_TRUE(engines[consumerNode])
                << "createDcpConsumer: No engine for Node" << consumerNode;
        auto mockConsumer = std::make_shared<MockDcpConsumer>(
                *engines[consumerNode],
                create_mock_cookie(),
                "Node" + std::to_string(consumerNode) + " from Node" +
                        std::to_string(producerNode));

        return mockConsumer;
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

    ENGINE_ERROR_CODE storeSet(const DocKey& docKey) {
        return store->set(*makeCommittedItem(docKey, {}), cookie);
    }

    /**
     * Flush all outstanding items to disk on the desired node (if persistent)
     */
    void flushNodeIfPersistent(Node node = Node0) {
        ASSERT_TRUE(engines[node])
                << "flushNodeIfPersistent: No engine for Node" << node;
        if (engines[node]->getConfiguration().getBucketType() == "persistent") {
            auto& replicaKVB = *engines[node]->getKVBucket();
            dynamic_cast<EPBucket&>(replicaKVB).flushVBucket(vbid);
        }
    }

    void TearDown() override {
        for (auto& e : extraEngines) {
            shutdownAndPurgeTasks(e.get());
        }

        destroy_mock_cookie(cookie);
        cookie = nullptr;

        extraEngines.clear();

        SingleThreadedKVBucketTest::TearDown();
    }

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

    // engines is 'map' from Node to an engine pointer, currently Node0 is the
    // engine created by the parent class and Node1 are created by this
    // class. Node1 is always created by SetUp and additional nodes created on
    // demand
    std::array<SynchronousEPEngine*, 4> engines;

    // Owned pointers to the other engines, created on demand by tests
    std::vector<SynchronousEPEngineUniquePtr> extraEngines;
};

void DCPLoopbackStreamTest::DcpRoute::destroy() {
    if (producer && consumer) {
        producer->cancelCheckpointCreatorTask();
        producer->closeAllStreams();
        consumer->closeAllStreams();
        destroy_mock_cookie(producer->getCookie());
        producer.reset();
        destroy_mock_cookie(consumer->getCookie());
        consumer.reset();
    } else {
        // don't expect consumer or producer, both or nothing
        ASSERT_FALSE(producer);
        ASSERT_FALSE(consumer);
    }
}

std::unique_ptr<DcpResponse>
DCPLoopbackStreamTest::DcpRoute::getNextProducerMsg(ActiveStream* stream) {
    std::unique_ptr<DcpResponse> producerMsg(stream->next());
    if (!producerMsg) {
        EXPECT_GE(getLpAuxQ()->getFutureQueueSize(), 1)
                << "Expected to have at least "
                   "ActiveStreamCheckpointProcessorTask "
                   "in future queue after null producerMsg";

        // Run the next waiting task to populate the streams' items.
        CheckedExecutor executor(ExecutorPool::get(), *getLpAuxQ());
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

std::pair<ActiveStream*, PassiveStream*>
DCPLoopbackStreamTest::DcpRoute::getStreams() {
    auto* pStream =
            dynamic_cast<ActiveStream*>(producer->findStream(vbid).get());
    auto* cStream = consumer->getVbucketStream(vbid).get();
    EXPECT_TRUE(pStream);
    EXPECT_TRUE(cStream);
    return {pStream, cStream};
}

void DCPLoopbackStreamTest::DcpRoute::transferMessage() {
    auto streams = getStreams();
    auto msg = getNextProducerMsg(streams.first);
    ASSERT_TRUE(msg);
    EXPECT_EQ(ENGINE_SUCCESS, streams.second->messageReceived(std::move(msg)));
}

void DCPLoopbackStreamTest::DcpRoute::transferMessage(
        DcpResponse::Event expectedEvent) {
    auto streams = getStreams();
    auto msg = getNextProducerMsg(streams.first);
    ASSERT_TRUE(msg);
    EXPECT_EQ(expectedEvent, msg->getEvent()) << *msg;
    EXPECT_EQ(ENGINE_SUCCESS, streams.second->messageReceived(std::move(msg)));
}

void DCPLoopbackStreamTest::DcpRoute::transferMutation(
        const StoredDocKey& expectedKey, uint64_t expectedSeqno) {
    auto streams = getStreams();
    auto msg = getNextProducerMsg(streams.first);
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::Mutation, msg->getEvent());
    ASSERT_TRUE(msg->getBySeqno()) << "optional seqno has no value";
    EXPECT_EQ(expectedSeqno, msg->getBySeqno().get());
    auto* mutation = static_cast<MutationResponse*>(msg.get());
    EXPECT_EQ(expectedKey, mutation->getItem()->getKey());
    EXPECT_EQ(ENGINE_SUCCESS, streams.second->messageReceived(std::move(msg)));
}

void DCPLoopbackStreamTest::DcpRoute::transferSnapshotMarker(
        uint64_t expectedStart, uint64_t expectedEnd, uint32_t expectedFlags) {
    auto streams = getStreams();
    auto msg = getNextProducerMsg(streams.first);
    ASSERT_TRUE(msg);
    ASSERT_EQ(DcpResponse::Event::SnapshotMarker, msg->getEvent()) << *msg;
    auto* marker = static_cast<SnapshotMarker*>(msg.get());
    EXPECT_EQ(expectedStart, marker->getStartSeqno());
    EXPECT_EQ(expectedEnd, marker->getEndSeqno());
    EXPECT_EQ(expectedFlags, marker->getFlags());
    EXPECT_EQ(ENGINE_SUCCESS, streams.second->messageReceived(std::move(msg)));
}

void DCPLoopbackStreamTest::DcpRoute::transferResponseMessage() {
    auto streams = getStreams();
    std::unique_ptr<DcpResponse> consumerMsg(streams.second->next());

    // Pass the consumer's message to the producer.
    if (consumerMsg) {
        switch (consumerMsg->getEvent()) {
        case DcpResponse::Event::SnapshotMarker:
            streams.first->snapshotMarkerAckReceived();
            break;
        case DcpResponse::Event::SetVbucket:
            streams.first->setVBucketStateAckRecieved();
            break;
        default:
            FAIL() << *consumerMsg;
        }
    }
}

std::pair<cb::engine_errc, uint64_t>
DCPLoopbackStreamTest::DcpRoute::doStreamRequest(int flags) {
    // Do the add_stream
    EXPECT_EQ(ENGINE_SUCCESS, consumer->addStream(/*opaque*/ 0, vbid, flags));
    auto streamRequest = consumer->getVbucketStream(vbid)->next();
    EXPECT_TRUE(streamRequest);
    EXPECT_EQ(DcpResponse::Event::StreamReq, streamRequest->getEvent());
    StreamRequest* sr = static_cast<StreamRequest*>(streamRequest.get());
    // Create an active stream against the producing node
    uint64_t rollbackSeqno = 0;
    auto error = producer->streamRequest(sr->getFlags(),
                                         sr->getOpaque(),
                                         vbid,
                                         sr->getStartSeqno(),
                                         sr->getEndSeqno(),
                                         sr->getVBucketUUID(),
                                         sr->getSnapStartSeqno(),
                                         sr->getSnapEndSeqno(),
                                         &rollbackSeqno,
                                         fakeDcpAddFailoverLog,
                                         {});
    if (error == ENGINE_SUCCESS) {
        auto producerVb = producerNode->getVBucket(vbid);
        EXPECT_GE(static_cast<MockCheckpointManager*>(
                          producerVb->checkpointManager.get())
                          ->getNumOfCursors(),
                  2)
                << "Should have both persistence and DCP producer cursor on "
                   "producer VB";
        EXPECT_GE(getLpAuxQ()->getFutureQueueSize(), 1);
        // Finally the stream-request response sends the failover table back
        // to the consumer... simulate that
        auto failoverLog = producerVb->failovers->getFailoverLog();
        std::vector<vbucket_failover_t> networkFailoverLog;
        for (const auto entry : failoverLog) {
            networkFailoverLog.push_back(
                    {htonll(entry.uuid), htonll(entry.seqno)});
        }
        consumer->public_streamAccepted(
                sr->getOpaque(),
                cb::mcbp::Status::Success,
                reinterpret_cast<const uint8_t*>(networkFailoverLog.data()),
                networkFailoverLog.size() * sizeof(vbucket_failover_t));

        auto addStreamResp = consumer->getVbucketStream(vbid)->next();
        EXPECT_EQ(DcpResponse::Event::AddStream, addStreamResp->getEvent());
    }
    return {cb::to_engine_errc(error), rollbackSeqno};
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
    auto route0_1 = createDcpRoute(Node0, Node1, enableExpiryOutput);
    EXPECT_EQ(cb::engine_errc::success,
              route0_1.doStreamRequest(DCP_ADD_STREAM_FLAG_TAKEOVER).first);
    auto* producerStream = static_cast<ActiveStream*>(
            route0_1.producer->findStream(vbid).get());
    ASSERT_TRUE(producerStream);

    // Both streams created. Check state is as expected.
    ASSERT_TRUE(producerStream->isTakeoverSend())
            << "Producer stream state should have transitioned to "
               "TakeoverSend";

    auto* consumerStream = route0_1.consumer->getVbucketStream(vbid).get();
    while (true) {
        // We expect an producer->consumer message that will trigger a response
        route0_1.transferMessage();
        route0_1.transferResponseMessage();

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

    auto* destVb = engines[Node1]->getVBucket(vbid).get();
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
    auto route0_1 = createDcpRoute(Node0, Node1);
    EXPECT_EQ(cb::engine_errc::success, route0_1.doStreamRequest(flags).first);

    // Test: Transfer 6 messages between Producer and Consumer
    // (SNAP_MARKER, PRE, CMT, SET), (SNAP_MARKER, PRE), with a flush after the
    // first 4.
    route0_1.transferSnapshotMarker(0, 3, MARKER_FLAG_CHK | MARKER_FLAG_DISK);
    route0_1.transferMessage(DcpResponse::Event::Prepare);
    route0_1.transferMutation(makeStoredDocKey("a"), 2);
    route0_1.transferMutation(makeStoredDocKey("b"), 3);

    flushNodeIfPersistent(Node1);

    // Transfer 2 more messages (SNAP_MARKER, PRE)
    int takeover = flags & DCP_ADD_STREAM_FLAG_TAKEOVER ? MARKER_FLAG_ACK : 0;
    route0_1.transferSnapshotMarker(
            4, 5, MARKER_FLAG_CHK | MARKER_FLAG_MEMORY | takeover);
    route0_1.transferMessage(DcpResponse::Event::Prepare);

    flushNodeIfPersistent(Node1);
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
    auto route0_1 = createDcpRoute(Node0, Node1);
    EXPECT_EQ(cb::engine_errc::success, route0_1.doStreamRequest().first);
    route0_1.transferMessage(DcpResponse::Event::SnapshotMarker);
    route0_1.transferMessage(DcpResponse::Event::Prepare);

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
    auto* pStream = static_cast<ActiveStream*>(
            route0_1.producer->findStream(vbid).get());
    ASSERT_TRUE(route0_1.producer->handleSlowStream(
            vbid, pStream->getCursor().lock().get()));
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
    route0_1.transferSnapshotMarker(2, 3, MARKER_FLAG_DISK | MARKER_FLAG_CHK);
    // Note: This was originally a Commit but because it has come from disk
    // it's sent as a Mutation (as backfill in general doesn't know if consumer
    // received the prior prepare so must send as Mutation).
    route0_1.transferMutation(makeStoredDocKey("a"), 2);
    route0_1.transferMutation(makeStoredDocKey("b"), 3);

    // Transfer 2 memory messages - should be:
    // SNAP_MARKER (mem), 4:PRE
    route0_1.transferSnapshotMarker(4, 5, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route0_1.transferMessage(DcpResponse::Event::Prepare);

    flushNodeIfPersistent(Node1);
}

// This test is validating that a replica which recevies a partial disk snapshot
// is rolled back to before that partial snapshot during failover. Prior to this
// test it was not clear if DCP would incorrectly resume the replica from beyond
// the partial snapshot start point, however the test proved that the way that
// KV calculates a disk snapshot marker is what ensures the consumer's post
// failover stream request to be rejected.
TEST_F(DCPLoopbackStreamTest, MultiReplicaPartialSnapshot) {
    // The keys we will use
    auto k1 = makeStoredDocKey("k1");
    auto k2 = makeStoredDocKey("k2");
    auto k3 = makeStoredDocKey("k3");
    auto k4 = makeStoredDocKey("k4");
    auto k5 = makeStoredDocKey("k5");

    // setup Node2 so we have two replicas
    createNode(Node2, vbucket_state_replica);

    auto route0_1 = createDcpRoute(Node0, Node1);
    auto route0_2 = createDcpRoute(Node0, Node2);
    EXPECT_EQ(cb::engine_errc::success, route0_1.doStreamRequest().first);
    EXPECT_EQ(cb::engine_errc::success, route0_2.doStreamRequest().first);

    // Setup the active, first move the active away from seqno 0 with a couple
    // of keys, we don't really care about these in this test
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k1));
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k2));
    flushVBucketToDiskIfPersistent(vbid, 2);
    // These go everywhere...
    route0_1.transferSnapshotMarker(0, 2, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route0_1.transferMutation(k1, 1);
    route0_1.transferMutation(k2, 2);
    route0_2.transferSnapshotMarker(0, 2, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route0_2.transferMutation(k1, 1);
    route0_2.transferMutation(k2, 2);

    // Now setup the interesting operations, and build the replicas as we go.
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k3));
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k4));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // And replicate the snapshot to replica on Node1
    route0_1.transferSnapshotMarker(3, 4, MARKER_FLAG_MEMORY);
    route0_1.transferMutation(k3, 3);
    route0_1.transferMutation(k4, 4);
    flushNodeIfPersistent(Node1);

    // Simulate disconnect of route0_2 Node0->Node2
    route0_2.destroy();

    auto vb = engines[Node0]->getVBucket(vbid);
    // Next snapshot, *important* k3 is set again and in a new checkpoint
    vb->checkpointManager->createNewCheckpoint();
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k5));
    EXPECT_EQ(ENGINE_SUCCESS, storeSet(k3));
    flushVBucketToDiskIfPersistent(vbid, 2);

    // And replicate a partial snapshot to the replica on Node1
    route0_1.transferSnapshotMarker(5, 6, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route0_1.transferMutation(k5, 5);
    // k3@6 doesn't transfer
    flushNodeIfPersistent(Node1);

    // brute force... ensure in-memory is now purged so our new stream backfills
    vb->checkpointManager->clear(vbucket_state_active);

    // Now reconnect Node0/Node2
    auto route0_2_new = createDcpRoute(Node0, Node2);
    EXPECT_EQ(cb::engine_errc::success, route0_2_new.doStreamRequest().first);
    runBackfill();

    // Now transfer the disk snapshot, again partial, leave the last key.
    // NOTE: This is the important snapshot which ensures our consumer later
    // stream-requests and rolls back to before this partial snapshot. What
    // is special about disk snapshots is the start-seqno is the stream-request
    // start seqno. Only disk snapshots would do that, in-memory snapshots
    // always set the marker.start to be the first seqno the ActiveStream pushes
    // to the readyQueue regardless of what the stream-request start-seqno was.
    route0_2_new.transferSnapshotMarker(
            2, 6, MARKER_FLAG_DISK | MARKER_FLAG_CHK);
    route0_2_new.transferMutation(k4, 4); // transfer k4
    flushNodeIfPersistent(Node2);
    route0_2_new.transferMutation(k5, 5); // transfer k5
    flushNodeIfPersistent(Node2);
    // but not k3@6

    // DISASTER. NODE0 dies...
    // DCP crashes
    route0_1.destroy();
    route0_2_new.destroy();
    // NODE1 promoted
    EXPECT_EQ(ENGINE_SUCCESS,
              engines[Node1]->getKVBucket()->setVBucketState(
                      vbid, vbucket_state_active));

    flushNodeIfPersistent(Node1);

    // New topology
    // NODE1 active -> NODE2, NODE3
    createNode(Node3, vbucket_state_replica); // bring node3 into the test
    auto route1_2 = createDcpRoute(Node1, Node2);
    auto route1_3 = createDcpRoute(Node1, Node3);
    auto rollback = route1_2.doStreamRequest();
    EXPECT_EQ(cb::engine_errc::rollback, rollback.first);

    // The existing replica which connects to Node1 has to go back to seqno:2
    // and must rebuild the partial snapshot again
    EXPECT_EQ(2, rollback.second);

    // The new node joins successfully and builds a replica from 0
    EXPECT_EQ(cb::engine_errc::success, route1_3.doStreamRequest().first);
    route1_3.transferSnapshotMarker(0, 4, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route1_3.transferMutation(k1, 1);
    route1_3.transferMutation(k2, 2);
    route1_3.transferMutation(k3, 3);
    route1_3.transferMutation(k4, 4);
    route1_3.transferSnapshotMarker(5, 5, MARKER_FLAG_MEMORY | MARKER_FLAG_CHK);
    route1_3.transferMutation(k5, 5);
}