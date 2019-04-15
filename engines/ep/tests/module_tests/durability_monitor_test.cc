/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "durability_monitor_test.h"

#include "checkpoint_manager.h"
#include "test_helpers.h"

#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityMonitorTest::addSyncWrite(int64_t seqno,
                                         cb::durability::Requirements req) {
    auto numTracked = monitor->public_getNumTracked();
    auto item = Item(makeStoredDocKey("key" + std::to_string(seqno)),
                     0 /*flags*/,
                     0 /*exp*/,
                     "value",
                     5 /*valueSize*/,
                     PROTOCOL_BINARY_RAW_BYTES,
                     0 /*cas*/,
                     seqno);
    using namespace cb::durability;
    item.setPendingSyncWrite(req);
    // Note: necessary for non-auto-generated seqno
    vb->checkpointManager->createSnapshot(seqno, seqno);
    // Note: need to go through VBucket::processSet to set the given bySeqno
    ASSERT_EQ(MutationStatus::WasClean, processSet(item));
    ASSERT_EQ(numTracked + 1, monitor->public_getNumTracked());
}

size_t DurabilityMonitorTest::addSyncWrites(int64_t seqnoStart,
                                            int64_t seqnoEnd,
                                            cb::durability::Requirements req) {
    size_t expectedNumTracked = monitor->public_getNumTracked();
    size_t added = 0;
    for (auto seqno = seqnoStart; seqno <= seqnoEnd; seqno++) {
        addSyncWrite(seqno, req);
        added++;
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    }
    return added;
}

size_t DurabilityMonitorTest::addSyncWrites(const std::vector<int64_t>& seqnos,
                                            cb::durability::Requirements req) {
    if (seqnos.empty()) {
        throw std::logic_error(
                "DurabilityMonitorTest::addSyncWrites: seqnos list is empty");
    }
    size_t expectedNumTracked = monitor->public_getNumTracked();
    size_t added = 0;
    for (auto seqno : seqnos) {
        addSyncWrite(seqno, req);
        added++;
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    }
    return added;
}

MutationStatus DurabilityMonitorTest::processSet(Item& item) {
    auto htRes = vb->ht.findForWrite(item.getKey());
    VBQueueItemCtx ctx;
    ctx.genBySeqno = GenerateBySeqno::No;
    ctx.durability =
            DurabilityItemCtx{item.getDurabilityReqs(), /*cookie*/ nullptr};
    return vb
            ->processSet(htRes.lock,
                         htRes.storedValue,
                         item,
                         item.getCas(),
                         true /*allow_existing*/,
                         false /*has_metadata*/,
                         ctx,
                         {/*no predicate*/})
            .first;
}

void DurabilityMonitorTest::assertNodeTracking(const std::string& node,
                                               uint64_t lastWriteSeqno,
                                               uint64_t lastAckSeqno) const {
    ASSERT_EQ(lastWriteSeqno, monitor->public_getNodeWriteSeqno(node));
    ASSERT_EQ(lastAckSeqno, monitor->public_getNodeAckSeqno(node));
}

void DurabilityMonitorTest::testLocalAck(int64_t ackSeqno,
                                         size_t expectedNumTracked,
                                         int64_t expectedLastWriteSeqno,
                                         int64_t expectedLastAckSeqno) {
    vb->setPersistenceSeqno(ackSeqno);
    monitor->notifyLocalPersistence();
    EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNodeTracking(
                active, expectedLastWriteSeqno, expectedLastAckSeqno);
    }
}

void DurabilityMonitorTest::testSeqnoAckReceived(
        const std::string& replica,
        int64_t ackSeqno,
        size_t expectedNumTracked,
        int64_t expectedLastWriteSeqno,
        int64_t expectedLastAckSeqno) const {
    EXPECT_NO_THROW(monitor->seqnoAckReceived(replica, ackSeqno));
    EXPECT_EQ(expectedNumTracked, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNodeTracking(
                replica, expectedLastWriteSeqno, expectedLastAckSeqno);
    }
}

bool DurabilityMonitorTest::testDurabilityPossible(
        const nlohmann::json::array_t& topology,
        queued_item& item,
        uint8_t expectedFirstChainSize,
        uint8_t expectedFirstChainMajority) {
    auto expectAddSyncWriteImpossible = [this, &item]() -> void {
        try {
            monitor->addSyncWrite(nullptr /*cookie*/, item);
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("Impossible") !=
                        std::string::npos);
            return;
        }
        FAIL();
    };

    monitor->public_setReplicationTopology(topology);
    EXPECT_EQ(expectedFirstChainSize, monitor->public_getFirstChainSize());
    EXPECT_EQ(expectedFirstChainMajority,
              monitor->public_getFirstChainMajority());

    if (expectedFirstChainSize < expectedFirstChainMajority) {
        expectAddSyncWriteImpossible();
        EXPECT_EQ(0, monitor->public_getNumTracked());
        return false;
    }

    EXPECT_NO_THROW(monitor->addSyncWrite(nullptr /*cookie*/, item));
    // @todo: There is an open issue (MB-33395) that requires that we wipe
    //     out the tracked container before resetting the replication chain
    //     (note some tests call this function multiple times).
    //     We can remove this step as soon as MB-33395 is fixed.
    // Note: This is also an implicit check on the number of tracked items.
    EXPECT_EQ(1 /*numRemoved*/, monitor->public_wipeTracked());
    return true;
}

TEST_F(DurabilityMonitorTest, AddSyncWrite) {
    EXPECT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedSmallerThanLastAcked) {
    addSyncWrites({1, 2} /*seqnos*/);

    const int64_t ackSeqno = 1;
    SCOPED_TRACE("");
    testLocalAck(ackSeqno,
                 2 /*expectedNumTracked*/,
                 1 /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
    // This call removes s:1
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         1 /*expectedNumTracked*/,
                         1 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);

    try {
        monitor->seqnoAckReceived(replica1, 0 /*preparedSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("Monotonic") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedEqualPending) {
    int64_t seqnoStart = 1;
    int64_t seqnoEnd = 3;

    auto numItems = addSyncWrites(seqnoStart, seqnoEnd);
    ASSERT_EQ(3, numItems);
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    for (int64_t ackSeqno = seqnoStart; ackSeqno <= seqnoEnd; ackSeqno++) {
        SCOPED_TRACE("");
        testLocalAck(ackSeqno,
                     numItems /*expectedNumTracked*/,
                     ackSeqno /*expectedLastWriteSeqno*/,
                     ackSeqno /*expectedLastAckSeqno*/);
        testSeqnoAckReceived(replica1,
                             ackSeqno,
                             --numItems /*expectedNumTracked*/,
                             ackSeqno /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/);
    }
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_ContinuousSeqnos) {
    ASSERT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 2;
    SCOPED_TRACE("");
    testLocalAck(ackSeqno,
                 3 /*expectedNumTracked*/,
                 ackSeqno /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         1 /*expectedNumTracked*/,
                         ackSeqno /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedGreaterThanPending_SparseSeqnos) {
    ASSERT_EQ(3, addSyncWrites({1, 3, 5} /*seqnos*/));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 4;
    SCOPED_TRACE("");
    testLocalAck(ackSeqno,
                 3 /*expectedNumTracked*/,
                 3 /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         1 /*expectedNumTracked*/,
                         3 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_ContinuousSeqnos) {
    ASSERT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 4;
    SCOPED_TRACE("");
    testLocalAck(ackSeqno,
                 3 /*expectedNumTracked*/,
                 3 /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         0 /*expectedNumTracked*/,
                         3 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);
}

TEST_F(DurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_SparseSeqnos) {
    ASSERT_EQ(3, addSyncWrites({1, 3, 5} /*seqnos*/));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 10;
    SCOPED_TRACE("");
    testLocalAck(ackSeqno,
                 3 /*expectedNumTracked*/,
                 5 /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         0 /*expectedNumTracked*/,
                         5 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);
}

// @todo: Refactor test suite and expand test cases
TEST_F(DurabilityMonitorTest, SeqnoAckReceived_PersistToMajority) {
    ASSERT_EQ(3,
              addSyncWrites({1, 3, 5} /*seqnos*/,
                            {cb::durability::Level::PersistToMajority,
                             0 /*timeout*/}));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 10;

    // Receive a seqno-ack greater than the last tracked seqno
    EXPECT_EQ(ENGINE_SUCCESS, monitor->seqnoAckReceived(replica1, ackSeqno));

    // Check that we have not committed as the active has not ack'ed the
    // persisted seqno
    EXPECT_EQ(3, monitor->public_getNumTracked());

    // Check that the tracking for Replica has been updated correctly
    assertNodeTracking(
            replica1, 5 /*lastWriteSeqno*/, ackSeqno /*lastAckSeqno*/);

    // Check that the tracking for Active has not moved yet
    assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);

    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence
    vb->setPersistenceSeqno(ackSeqno);
    monitor->notifyLocalPersistence();

    // Check that we committed and removed all SyncWrites
    EXPECT_EQ(0, monitor->public_getNumTracked());

    // Check that the tracking for Active has been updated correctly
    assertNodeTracking(active, 5 /*lastWriteSeqno*/, ackSeqno /*lastAckSeqno*/);
}

TEST_F(DurabilityMonitorTest, SetTopology_NotAnArray) {
    try {
        monitor->setReplicationTopology(nlohmann::json::object());
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is not an array") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_Empty) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array());
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is empty") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_FirstChainEmpty) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array({{}}));
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("FirstChain cannot be empty") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_FirstChainUndefinedActive) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array({{nullptr}}));
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(
                std::string(e.what()).find(
                        "first node in chain (active) cannot be undefined") !=
                std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_TooManyNodesInChain) {
    try {
        monitor->setReplicationTopology(nlohmann::json::array(
                {{"active", "replica1", "replica2", "replica3", "replica4"}}));
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Too many nodes in chain") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SetTopology_NodeDuplicateIncChain) {
    try {
        monitor->setReplicationTopology(
                nlohmann::json::array({{"node1", "node1"}}));
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Duplicate node") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

// @todo: Extend to disk-seqno
TEST_F(DurabilityMonitorTest, SeqnoAckReceived_MultipleReplica) {
    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}})));
    ASSERT_EQ(4, monitor->public_getFirstChainSize());

    const int64_t seqno = 1;
    addSyncWrite(seqno);

    // Nothing ack'ed yet
    for (const auto& node : {active, replica1, replica2, replica3}) {
        SCOPED_TRACE("");
        assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
    // Nothing committed
    EXPECT_EQ(1, monitor->public_getNumTracked());

    {
        SCOPED_TRACE("");
        // active acks; nothing committed yet
        testLocalAck(seqno /*ackSeqno*/,
                     1 /*expectedNumTracked*/,
                     seqno /*expectedLastWriteSeqno*/,
                     seqno /*expectedLastAckSeqno*/);

        // replica2 acks; nothing committed yet
        testSeqnoAckReceived(replica2,
                             seqno /*ackSeqno*/,
                             1 /*expectedNumTracked*/,
                             seqno /*expectedLastWriteSeqno*/,
                             seqno /*expectedLastAckSeqno*/);

        // replica3 acks; requirements verified, committed
        testSeqnoAckReceived(replica3,
                             seqno /*ackSeqno*/,
                             0 /*expectedNumTracked*/,
                             seqno /*expectedLastWriteSeqno*/,
                             seqno /*expectedLastAckSeqno*/);

        // replica1 has not ack'ed yet
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
}

TEST_F(DurabilityMonitorTest, NeverExpireIfTimeoutNotSet) {
    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica1}})));
    ASSERT_EQ(2, monitor->public_getFirstChainSize());

    // Note: Timeout=0 (i.e., no timeout) in default Durability Requirements
    ASSERT_EQ(1, addSyncWrites({1} /*seqno*/));
    EXPECT_EQ(1, monitor->public_getNumTracked());

    // Never expire, neither after 1 year !
    const auto year = std::chrono::hours(24 * 365);
    monitor->processTimeout(std::chrono::steady_clock::now() + year);

    // Not expired, still tracked
    EXPECT_EQ(1, monitor->public_getNumTracked());
}

TEST_F(DurabilityMonitorTest, ProcessTimeout) {
    ASSERT_NO_THROW(monitor->setReplicationTopology(
            nlohmann::json::array({{active, replica1}})));
    ASSERT_EQ(2, monitor->public_getFirstChainSize());

    auto assertNoAck = [this]() -> void {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    };

    /*
     * 1 SyncWrite
     */

    const auto level = cb::durability::Level::Majority;

    ASSERT_EQ(1, addSyncWrites({1} /*seqno*/, {level, 1 /*timeout*/}));
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(1000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    /*
     * Multiple SyncWrites, ordered by timeout
     */

    ASSERT_EQ(1, addSyncWrites({101} /*seqno*/, {level, 1 /*timeout*/}));
    ASSERT_EQ(1, addSyncWrites({102} /*seqno*/, {level, 10}));
    ASSERT_EQ(1, addSyncWrites({103} /*seqno*/, {level, 20}));
    EXPECT_EQ(3, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(10000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    /*
     * Multiple SyncWrites, not ordered by timeout
     */

    ASSERT_EQ(1, addSyncWrites({201} /*seqno*/, {level, 20 /*timeout*/}));
    ASSERT_EQ(1, addSyncWrites({202} /*seqno*/, {level, 1}));
    ASSERT_EQ(1, addSyncWrites({203} /*seqno*/, {level, 50000}));
    EXPECT_EQ(3, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(10000));

    EXPECT_EQ(1, monitor->public_getNumTracked());
    const auto tracked = monitor->public_getTrackedSeqnos();
    EXPECT_TRUE(tracked.find(201) == tracked.end());
    EXPECT_TRUE(tracked.find(202) == tracked.end());
    EXPECT_TRUE(tracked.find(203) != tracked.end());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }

    monitor->processTimeout(std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(100000));

    EXPECT_EQ(0, monitor->public_getNumTracked());
    {
        SCOPED_TRACE("");
        assertNoAck();
    }
}

TEST_F(DurabilityMonitorTest, MajorityAndPersistActive) {
    ASSERT_EQ(3,
              addSyncWrites({1, 3, 5} /*seqnos*/,
                            {cb::durability::Level::MajorityAndPersistOnMaster,
                             0 /*timeout*/}));
    {
        SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    int64_t ackSeqno = 10;

    SCOPED_TRACE("");
    // Replica acks all; nothing ack'ed on active though, so nothing committed
    testSeqnoAckReceived(replica1,
                         ackSeqno,
                         3 /*expectedNumTracked*/,
                         5 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/);
    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence. Then: all satisfied, all committed.
    testLocalAck(ackSeqno /*ackSeqno*/,
                 0 /*expectedNumTracked*/,
                 5 /*expectedLastWriteSeqno*/,
                 ackSeqno /*expectedLastAckSeqno*/);
}

/*
 * The following tests check that the DurabilityMonitor enforces the
 * durability-impossible semantic. I.e., DM enforces durability-impossible when
 * the caller tries to enqueue a SyncWrite but the topology state prevents
 * Requirements from being satisfied.
 *
 * Note: Not covered in the following as already covered in dedicated tests:
 * - FirstChain cannot be empty
 * - Active node in chain cannot be undefined
 *
 * @todo: Extend durability-impossible tests to SecondChain when supported
 */

TEST_F(DurabilityMonitorTest, DurabilityImpossible_NoReplica) {
    auto item = makePendingItem(makeStoredDocKey("key"), "value");
    item->setBySeqno(1);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible({{"active"}},
                                           item,
                                           1 /*expectedFirstChainSize*/,
                                           1 /*expectedFirstChainMajority*/));
    }
}

TEST_F(DurabilityMonitorTest, DurabilityImpossible_1Replica) {
    auto item = makePendingItem(makeStoredDocKey("key"), "value");

    item->setBySeqno(1);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica"}},
                                           item,
                                           2 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    item->setBySeqno(2);
    {
        SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible({{"active", nullptr}},
                                            item,
                                            1 /*expectedFirstChainSize*/,
                                            2 /*expectedFirstChainMajority*/));
    }
}

TEST_F(DurabilityMonitorTest, DurabilityImpossible_2Replicas) {
    auto item = makePendingItem(makeStoredDocKey("key"), "value");

    item->setBySeqno(1);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica1", "replica2"}},
                                           item,
                                           3 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    item->setBySeqno(2);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica1", nullptr}},
                                           item,
                                           2 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    item->setBySeqno(3);
    {
        SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible({{"active", nullptr, nullptr}},
                                            item,
                                            1 /*expectedFirstChainSize*/,
                                            2 /*expectedFirstChainMajority*/));
    }
}

TEST_F(DurabilityMonitorTest, DurabilityImpossible_3Replicas) {
    // Note: In this test, playing with Undefined nodes in different and
    // non-consecutive positions.
    // Not sure if ns_server can set something like that (e.g. {A, u, R2, u}),
    // but better that we cover the most general case.
    auto item = makePendingItem(makeStoredDocKey("key"), "value");

    item->setBySeqno(1);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{"active", "replica1", "replica2", "replica3"}},
                item,
                4 /*expectedFirstChainSize*/,
                3 /*expectedFirstChainMajority*/));
    }

    item->setBySeqno(2);
    {
        SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{"active", "replica1", nullptr, "replica3"}},
                item,
                3 /*expectedFirstChainSize*/,
                3 /*expectedFirstChainMajority*/));
    }

    item->setBySeqno(3);
    {
        SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible(
                {{"active", nullptr, nullptr, "replica3"}},
                item,
                2 /*expectedFirstChainSize*/,
                3 /*expectedFirstChainMajority*/));
    }

    // No need to increase bySeqno here, as at the previous call s:3 must be
    // rejected (durability is impossible when chainSize<chainMajority).
    // Also, if for some reason the durability-impossible logic is broken
    // (i.e., s:3 is successfully queued at the previous step rather than being
    // rejected) then the following step fails as well, as trying to
    // re-queueing s:3 will break seqno-invariant.
    {
        SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{"active", nullptr, nullptr, nullptr}},
                                       item,
                                       1 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/));
    }
}
