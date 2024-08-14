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

#include "durability_monitor_test.h"

#include "../mock/mock_synchronous_ep_engine.h"
#include "../mock/mock_taskable.h"
#include "checkpoint_manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "durability/active_durability_monitor.h"
#include "durability/sync_write.h"
#include "item.h"
#include "vbucket_queue_item_ctx.h"
#include "vbucket_utils.h"

#include <programs/engine_testapp/mock_cookie.h>
#include <utilities/test_manifest.h>

void ActiveDurabilityMonitorTest::SetUp() {
    // MB-34453: Change sync_writes_max_allowed_replicas back to total
    // possible replicas given we want to still test with all replicas.
    setup(3, true);
}

void ActiveDurabilityMonitorTest::setup(int maxAllowedReplicas,
                                        bool setupTopology) {
    config_string += "sync_writes_max_allowed_replicas=" +
                     std::to_string(maxAllowedReplicas);

    STParameterizedBucketTest::SetUp();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    vb = store->getVBuckets().getBucket(vbid).get();
    ASSERT_TRUE(vb);

    monitor = &VBucketTestIntrospector::public_getActiveDM(*vb);
    ASSERT_TRUE(monitor);

    // The ActiveDM must not track anything before the Replication Chain is set.
    auto& adm = getActiveDM();
    ASSERT_EQ(0, adm.getFirstChainSize());
    ASSERT_EQ(0, adm.getFirstChainMajority());

    if (setupTopology) {
        adm.setReplicationTopology(nlohmann::json::array({{active, replica1}}));
        ASSERT_EQ(2, adm.getFirstChainSize());
        ASSERT_EQ(2, adm.getFirstChainMajority());
    }
}

void ActiveDurabilityMonitorTest::TearDown() {
    // Calling the ostream operator will iterate over the entire ADM and may
    // catch any 'corruption' issues. The underlying issue for MB-35661 was
    // already triggered by some existing tests but never detected.
    std::stringstream ss;
    ss << getActiveDM();
    STParameterizedBucketTest::TearDown();
}

ActiveDurabilityMonitor& ActiveDurabilityMonitorTest::getActiveDM() const {
    return dynamic_cast<ActiveDurabilityMonitor&>(*monitor);
}

void DurabilityMonitorTest::addSyncWrite(int64_t seqno,
                                         cb::durability::Requirements req) {
    auto current = vb->getHighSeqno();
    ASSERT_LE(current, seqno);
    auto item = Item(makeStoredDocKey("key" + std::to_string(seqno)),
                     0 /*flags*/,
                     0 /*exp*/,
                     "value",
                     5 /*valueSize*/,
                     PROTOCOL_BINARY_RAW_BYTES,
                     0 /*cas*/,
                     seqno);

    // Note: necessary for non-auto-generated seqno. Instead of hooking into
    // processSet (we can't because we call checkForCommit in VBucket::set) we
    // set a specific seqno by storing items up to that point.
    for (auto i = current; i < seqno - 1; i++) {
        auto key = makeStoredDocKey("key");
        store_item(vbid, key, "value");
    }

    // Note: need to go through VBucket::set make sure we call
    // ADM::checkForCommit
    item.setPendingSyncWrite(req);
    ASSERT_EQ(cb::engine_errc::sync_write_pending, set(item));

    vb->processResolvedSyncWrites();
}

void PassiveDurabilityMonitorTest::addSyncWrite(
        int64_t seqno, cb::durability::Requirements req) {
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
    vb->checkpointManager->createSnapshot(
            seqno, seqno, {} /*HCS*/, CheckpointType::Memory, 0 /*MVS*/);
    processSet(item);
}

void DurabilityMonitorTest::addSyncDelete(int64_t seqno,
                                          cb::durability::Requirements req) {
    ASSERT_GT(seqno, 1);
    auto current = vb->getHighSeqno();
    ASSERT_LE(current, seqno);
    auto item = Item(makeStoredDocKey("key" + std::to_string(seqno)),
                     0 /*flags*/,
                     0 /*exp*/,
                     "value",
                     5 /*valueSize*/,
                     PROTOCOL_BINARY_RAW_BYTES,
                     0 /*cas*/,
                     current + 1);
    ASSERT_EQ(cb::engine_errc::success, set(item));
    uint64_t cas = item.getCas();
    current = vb->getHighSeqno();

    // Note: necessary for non-auto-generated seqno. Instead of hooking into
    // processSet (we can't because we call checkForCommit in VBucket::set) we
    // set a specific seqno by storing items up to that point.
    for (auto i = current; i < seqno - 1; i++) {
        auto key = makeStoredDocKey("key");
        store_item(vbid, key, "value");
    }

    {
        std::shared_lock rlh(vb->getStateLock());
        mutation_descr_t mutation_descr;
        ASSERT_EQ(cb::engine_errc::sync_write_pending,
                  vb->deleteItem(rlh,
                                 cas,
                                 cookie,
                                 *engine,
                                 req,
                                 nullptr,
                                 mutation_descr,
                                 vb->lockCollections(item.getKey())));
    }
    vb->notifyActiveDMOfLocalSyncWrite();
    vb->processResolvedSyncWrites();
}

size_t ActiveDurabilityMonitorTest::addSyncWrites(
        int64_t seqnoStart,
        int64_t seqnoEnd,
        cb::durability::Requirements req) {
    size_t expectedNumTracked = monitor->getNumTracked();
    size_t added = 0;
    for (auto seqno = seqnoStart; seqno <= seqnoEnd; seqno++) {
        addSyncWrite(seqno, req);
        added++;
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, monitor->getNumTracked());
    }
    return added;
}

void DurabilityMonitorTest::addSyncWrites(const std::vector<int64_t>& seqnos,
                                          cb::durability::Requirements req) {
    for (auto seqno : seqnos) {
        addSyncWrite(seqno, req);
    }
}

MutationStatus DurabilityMonitorTest::processSet(Item& item) {
    VBQueueItemCtx ctx{CanDeduplicate::Yes};
    ctx.genBySeqno = GenerateBySeqno::No;
    ctx.durability =
            DurabilityItemCtx{item.getDurabilityReqs(), /*cookie*/ nullptr};

    auto htRes = vb->ht.findForUpdate(item.getKey());
    auto* v = htRes.selectSVToModify(item);

    return vb
            ->processSet(htRes,
                         v,
                         item,
                         item.getCas(),
                         true /*allow_existing*/,
                         false /*has_metadata*/,
                         ctx,
                         {/*no predicate*/})
            .first;
}

cb::engine_errc DurabilityMonitorTest::set(Item& item) {
    cb::engine_errc result;
    {
        std::shared_lock rlh(vb->getStateLock());
        result = vb->set(rlh,
                         item,
                         cookie,
                         *engine,
                         {},
                         vb->lockCollections(item.getKey()));
    }
    vb->notifyActiveDMOfLocalSyncWrite();
    return result;
}

void ActiveDurabilityMonitorTest::assertNodeTracking(
        const std::string& node,
        uint64_t lastWriteSeqno,
        uint64_t lastAckSeqno) const {
    auto& adm = getActiveDM();
    ASSERT_EQ(lastWriteSeqno, adm.getNodeWriteSeqno(node));
    ASSERT_EQ(lastAckSeqno, adm.getNodeAckSeqno(node));
}

void ActiveDurabilityMonitorTest::assertHPSAndHCS(
        const int64_t expectedHPS, const int64_t expectedHCS) const {
    ASSERT_EQ(expectedHPS, monitor->getHighPreparedSeqno());
    ASSERT_EQ(expectedHCS, monitor->getHighCompletedSeqno());
}

void ActiveDurabilityMonitorTest::testLocalAck(int64_t ackSeqno,
                                               size_t expectedNumTracked,
                                               int64_t expectedLastWriteSeqno,
                                               int64_t expectedLastAckSeqno) {
    vb->setPersistenceSeqno(ackSeqno);
    monitor->notifyLocalPersistence();
    EXPECT_EQ(expectedNumTracked, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(
                active, expectedLastWriteSeqno, expectedLastAckSeqno);
    }
}

void ActiveDurabilityMonitorTest::testSeqnoAckReceived(
        const std::string& replica,
        int64_t ackSeqno,
        int64_t expectedLastWriteSeqno,
        int64_t expectedLastAckSeqno,
        size_t expectedNumTracked,
        int64_t expectedHPS,
        int64_t expectedHCS) const {
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica, ackSeqno));
    vb->processResolvedSyncWrites();
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(
                replica, expectedLastWriteSeqno, expectedLastAckSeqno);
        assertNumTrackedAndHPSAndHCS(
                expectedNumTracked, expectedHPS, expectedHCS);
    }
}

queued_item ActiveDurabilityMonitorTest::makePendingWithSeqno(
        int64_t seqno, cb::durability::Level level) {
    auto item = makePendingItem(makeStoredDocKey("key"),
                                "value",
                                cb::durability::Requirements(level, {}));
    item->setBySeqno(seqno);
    return item;
}

bool ActiveDurabilityMonitorTest::testDurabilityPossible(
        const nlohmann::json::array_t& topology,
        queued_item item,
        uint8_t expectedFirstChainSize,
        uint8_t expectedFirstChainMajority,
        uint8_t expectedSecondChainSize,
        uint8_t expectedSecondChainMajority) {
    auto& adm = getActiveDM();
    auto expectAddSyncWriteImpossible = [&adm, &item]() -> void {
        try {
            adm.addSyncWrite(nullptr /*cookie*/, item);
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(std::string(e.what()).find("Impossible") !=
                        std::string::npos);
            return;
        }
        FAIL();
    };

    adm.setReplicationTopology(topology);
    EXPECT_EQ(expectedFirstChainSize, adm.getFirstChainSize());
    EXPECT_EQ(expectedFirstChainMajority, adm.getFirstChainMajority());
    EXPECT_EQ(expectedSecondChainSize, adm.getSecondChainSize());
    EXPECT_EQ(expectedSecondChainMajority, adm.getSecondChainMajority());

    if (expectedFirstChainSize < expectedFirstChainMajority ||
        expectedSecondChainSize < expectedSecondChainMajority) {
        expectAddSyncWriteImpossible();
        EXPECT_EQ(0, monitor->getNumTracked());
        return false;
    }

    adm.addSyncWrite(nullptr /*cookie*/, item);
    return true;
}

void ActiveDurabilityMonitorTest::testChainEmpty(
        const nlohmann::json::array_t& topology,
        DurabilityMonitor::ReplicationChainName chainName) {
    try {
        getActiveDM().setReplicationTopology(topology);
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find(to_string(chainName) +
                                               " chain cannot be empty") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

void ActiveDurabilityMonitorTest::testChainUndefinedActive(
        const nlohmann::json::array_t& topology,
        DurabilityMonitor::ReplicationChainName chainName) {
    try {
        getActiveDM().setReplicationTopology(topology);
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find(
                            "first node in " + to_string(chainName) +
                            " chain (active) "
                            "cannot be undefined") != std::string::npos);
        return;
    }
    FAIL();
}

void ActiveDurabilityMonitorTest::testChainTooManyNodes(
        const nlohmann::json::array_t& topology,
        DurabilityMonitor::ReplicationChainName chainName) {
    try {
        getActiveDM().setReplicationTopology(topology);
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Too many nodes in " +
                                               to_string(chainName) +
                                               " chain") != std::string::npos);
        return;
    }
    FAIL();
}

void ActiveDurabilityMonitorTest::testChainDuplicateNode(
        const nlohmann::json::array_t& topology) {
    try {
        getActiveDM().setReplicationTopology(topology);
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Duplicate node") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

void ActiveDurabilityMonitorTest::testSeqnoAckSmallerThanLastAck() {
    DurabilityMonitorTest::addSyncWrites({1, 2} /*seqnos*/);
    ASSERT_EQ(2, monitor->getNumTracked());

    // This call removes s:1
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         2 /*expectedHPS*/,
                         1 /*expectedHCS*/);

    // MB-35096: The active needs to be resilient to non-monotonic acking
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica1, 0 /*preparedSeqno*/));
}

void ActiveDurabilityMonitorPersistentTest::testSeqnoAckPersistToMajority(
        const std::vector<std::string>& nodesToAck) {
    DurabilityMonitorTest::addSyncWrites(
            {1, 3, 5} /*seqnos*/,
            {cb::durability::Level::PersistToMajority, {}});
    EXPECT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    {
        CB_SCOPED_TRACE("");
        for (const auto& node : nodesToAck) {
            assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        }
    }

    const int64_t ackSeqno = 5;

    for (const auto& node : nodesToAck) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(node,
                             ackSeqno /*ackSeqno*/,
                             5 /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/,
                             3 /*expectedNumTracked*/,
                             0 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    testLocalAck(ackSeqno,
                 0 /*expectedNumTracked*/,
                 5 /*expectedLastWriteSeqno*/,
                 0 /*expectedLastAckSeqno*/);
}

void ActiveDurabilityMonitorPersistentTest::
        testSeqnoAckMajorityAndPersistOnMaster(
                const std::vector<std::string>& nodesToAck) {
    DurabilityMonitorTest::addSyncWrites(
            {1, 3, 5} /*seqnos*/,
            {cb::durability::Level::MajorityAndPersistOnMaster, {}});
    ASSERT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
    // Nothing ack'ed yet by Replicas
    for (const auto& node : nodesToAck) {
        CB_SCOPED_TRACE("");
        assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 5;

    // Ack all replicas, nothing should be committed
    for (const auto& node : nodesToAck) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(node,
                             ackSeqno,
                             5 /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/,
                             3 /*expectedNumTracked*/,
                             0 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    CB_SCOPED_TRACE("");

    // Simulate the Flusher that notifies the local DurabilityMonitor after
    // persistence. Then: all satisfied, all committed.
    testLocalAck(ackSeqno /*ackSeqno*/,
                 0 /*expectedNumTracked*/,
                 5 /*expectedLastWriteSeqno*/,
                 0 /*expectedLastAckSeqno*/);
}

void ActiveDurabilityMonitorTest::testSeqnoAckUnknownNode(
        const std::string& nodeToAck,
        const std::vector<std::string>& unchangedNodes) {
    DurabilityMonitorTest::addSyncWrite(1);
    ASSERT_EQ(1, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 1 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
    // Nothing ack'ed yet
    for (const auto& node : unchangedNodes) {
        CB_SCOPED_TRACE("");
        assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(nodeToAck, 1));

    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 1 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
    // Nothing ack'ed yet
    for (const auto& node : unchangedNodes) {
        CB_SCOPED_TRACE("");
        assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
    EXPECT_EQ(1, monitor->getNumTracked());
}

void ActiveDurabilityMonitorTest::testRepeatedSeqnoAck(int64_t firstAck,
                                                       int64_t secondAck) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2}}));
    auto numTracked = addSyncWrites(1 /*seqnoStart*/, 1 /*seqnoEnd*/);

    ASSERT_EQ(1, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(1, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    testSeqnoAckReceived(replica1,
                         firstAck /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         firstAck /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);

    testSeqnoAckReceived(replica1,
                         secondAck /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         std::max(firstAck, secondAck) /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
}

void ActiveDurabilityMonitorTest::setSeqnoAckReceivedPostProcessHook(
        std::function<void()> func) {
    getActiveDM().seqnoAckReceivedPostProcessHook = func;
}

TEST_P(ActiveDurabilityMonitorTest, AddSyncWrite) {
    EXPECT_EQ(3, addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/));
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedUnknownNode) {
    testSeqnoAckUnknownNode(replica2, {replica1});
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedUnknownNodeTwoChains) {
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    testSeqnoAckUnknownNode(replica3, {replica1, replica2});
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedSmallerThanLastAcked) {
    testSeqnoAckSmallerThanLastAck();
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedSmallerThanLastAckedSecondChain) {
    getActiveDM().setReplicationTopology({{active}, {active, replica1}});
    testSeqnoAckSmallerThanLastAck();
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedEqualPending) {
    // Note: Topology set to {{active, replica1}} in test setup

    int64_t seqnoStart = 1;
    int64_t seqnoEnd = 3;
    auto numItems = addSyncWrites(seqnoStart, seqnoEnd);
    ASSERT_EQ(3, numItems);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    for (int64_t ackSeqno = seqnoStart; ackSeqno <= seqnoEnd; ackSeqno++) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             ackSeqno,
                             ackSeqno /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/,
                             --numItems /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             ackSeqno /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedEqualPendingTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    int64_t seqnoStart = 1;
    int64_t seqnoEnd = 3;
    auto numItems = addSyncWrites(seqnoStart, seqnoEnd);
    ASSERT_EQ(3, numItems);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica2, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    // At every ack, FirstChain is satisfied but SecondChain is not
    for (int64_t ackSeqno = seqnoStart; ackSeqno <= seqnoEnd; ackSeqno++) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             ackSeqno,
                             ackSeqno /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/,
                             numItems /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    // At every ack, both chains are satisfied
    for (int64_t ackSeqno = seqnoStart; ackSeqno <= seqnoEnd; ackSeqno++) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica2,
                             ackSeqno,
                             ackSeqno /*expectedLastWriteSeqno*/,
                             ackSeqno /*expectedLastAckSeqno*/,
                             --numItems /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             ackSeqno /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_ContinuousSeqnos) {
    // Note: Topology set to {{active, replica1}} in test setup

    auto numItems = addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/);
    ASSERT_EQ(3, numItems);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             2 /*ackSeqno*/,
                             2 /*expectedLastWriteSeqno*/,
                             2 /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             2 /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_ContinuousSeqnosTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    auto numItems = addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/);
    ASSERT_EQ(3, numItems);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             2 /*ackSeqno*/,
                             2 /*expectedLastWriteSeqno*/,
                             2 /*expectedLastAckSeqno*/,
                             3 /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica2,
                             2 /*ackSeqno*/,
                             2 /*expectedLastWriteSeqno*/,
                             2 /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             3 /*expectedHPS*/,
                             2 /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_SparseSeqnos) {
    // Note: Topology set to {{active, replica1}} in test setup

    DurabilityMonitorTest::addSyncWrites({1, 3, 5} /*seqnos*/);
    auto numTracked = monitor->getNumTracked();
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(5, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             4 /*ackSeqno*/,
                             3 /*expectedLastWriteSeqno*/,
                             4 /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             5 /*expectedHPS*/,
                             3 /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanPending_SparseSeqnosTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    DurabilityMonitorTest::addSyncWrites({1, 3, 5} /*seqnos*/);
    auto numTracked = monitor->getNumTracked();
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(5, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica2, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             4 /*ackSeqno*/,
                             3 /*expectedLastWriteSeqno*/,
                             4 /*expectedLastAckSeqno*/,
                             numTracked /*expectedNumTracked*/,
                             5 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica2,
                             4 /*ackSeqno*/,
                             3 /*expectedLastWriteSeqno*/,
                             4 /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             5 /*expectedHPS*/,
                             3 /*expectedHCS*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_ContinuousSeqnos) {
    auto numTracked = addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/);
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    // MB-35096: We now expect the adm to handle seqno acks beyond
    // the last tracked write seqno
    testSeqnoAckReceived(replica1,
                         4 /*ackSeqno*/,
                         3 /*expectedLastWriteSeqno*/,
                         4 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         3 /*expectedHPS*/,
                         3 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_ContinuousSeqnosTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    auto numTracked = addSyncWrites(1 /*seqnoStart*/, 3 /*seqnoEnd*/);
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(3, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica2, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    testSeqnoAckReceived(replica1,
                         4 /*ackSeqno*/,
                         3 /*expectedLastWriteSeqno*/,
                         4 /*expectedLastAckSeqno*/,
                         3 /*expectedNumTracked*/,
                         3 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    testSeqnoAckReceived(replica2,
                         5 /*ackSeqno*/,
                         3 /*expectedLastWriteSeqno*/,
                         5 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         3 /*expectedHPS*/,
                         3 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_SparseSeqnos) {
    DurabilityMonitorTest::addSyncWrites({1, 3, 5} /*seqnos*/);
    auto numTracked = monitor->getNumTracked();
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(5, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    testSeqnoAckReceived(replica1,
                         10 /*ackSeqno*/,
                         5 /*expectedLastWriteSeqno*/,
                         10 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         5 /*expectedHPS*/,
                         5 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceivedGreaterThanLastTracked_SparseSeqnosTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    DurabilityMonitorTest::addSyncWrites({1, 3, 5} /*seqnos*/);
    auto numTracked = monitor->getNumTracked();
    ASSERT_EQ(3, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(5, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica2, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    testSeqnoAckReceived(replica1,
                         10 /*ackSeqno*/,
                         5 /*expectedLastWriteSeqno*/,
                         10 /*expectedLastAckSeqno*/,
                         3 /*expectedNumTracked*/,
                         5 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    testSeqnoAckReceived(replica2,
                         9 /*ackSeqno*/,
                         5 /*expectedLastWriteSeqno*/,
                         9 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         5 /*expectedHPS*/,
                         5 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckWithNoTrackedWrites) {
    auto numTracked = monitor->getNumTracked();
    ASSERT_EQ(0, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(0, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    testSeqnoAckReceived(replica1,
                         4 /*ackSeqno*/,
                         0 /*expectedLastWriteSeqno*/,
                         4 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         0 /*expectedHPS*/,
                         0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckTwice_Higher) {
    testRepeatedSeqnoAck(1, 4);
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckTwice_eq) {
    testRepeatedSeqnoAck(1, 1);
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckTwice_Lower) {
    testRepeatedSeqnoAck(4, 1);
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckTwiceDoesNotIncreaseAckCountTwice) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}}));
    auto numTracked = addSyncWrites(1 /*seqnoStart*/, 1 /*seqnoEnd*/);

    ASSERT_EQ(1, numTracked);
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(1, 0);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    // one replica ack + active does not satisfy majority of 4
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    // The same replica might ack again at the end of a disk snapshot
    // Should still be tracking the write as it should *not* be complete.
    // (majority still not satisfied, we need a *different* replica to ack
    // for majority to be reached)
    testSeqnoAckReceived(replica1,
                         4 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         4 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    // reach majority
    testSeqnoAckReceived(replica2,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceived_PersistToSecondChainNewActive) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {replica2, replica1}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    DurabilityMonitorTest::addSyncWrites({1, 3, 5} /*seqnos*/);
    EXPECT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 5 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica2, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    const int64_t ackSeqno = 5;

    CB_SCOPED_TRACE("");
    // active has already acked, replica 1 now acks which gives us majority on
    // both fist and second chain. However, we still need the ack from our new
    // active (replica2)
    testSeqnoAckReceived(replica1,
                         ackSeqno /*ackSeqno*/,
                         5 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/,
                         3 /*expectedNumTracked*/,
                         5 /*expectedHPS*/,
                         0 /*expectedHCS*/);
    testSeqnoAckReceived(replica2,
                         ackSeqno /*ackSeqno*/,
                         5 /*expectedLastWriteSeqno*/,
                         ackSeqno /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         5 /*expectedHPS*/,
                         5 /*expectedHCS*/);
}

// MB-34628: Test that if two seqnoAckReceived calls are made concurrently
// (from different Replicas) and if each one results in one item being
// Committed, then we correctly order the VB::commit() calls.
// Original bug was while the toCommit list updates were correctly serialised,
// the calls to VBucket::commit() were not, which could result in items being
// added to CheckpointManager in a different order to which they actually
// occurred.
TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceivedConcurrentDataRace) {
    // Setup: Prepare topology and prepared SyncWrites such that there are 2
    // Prepares in flight.
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2}}));
    DurabilityMonitorTest::addSyncWrites({1, 2} /*seqnos*/);

    // Setup: Register a cursor at end of checkpoint to be able to validate
    // subsequent commits.
    auto* ckptMgr = vb->checkpointManager.get();
    auto cursor = ckptMgr->registerCursorBySeqno(
            "test", vb->getHighSeqno(), CheckpointCursor::Droppable::Yes);

    // Test: the first seqnoAckRecieved(1) (from replica A) commits
    // SyncWrite(1), and the second seqnoAckReceived(2) from replica B commits
    // the second. Use hooks in seqnoAckReceived to control the precise
    // ordering.
    int callCount = 0;
    setSeqnoAckReceivedPostProcessHook([this, &adm, &callCount]() {
        callCount++;
        if (callCount == 1) {
            // Trigger the seqnoAckReceived in the middle of the first one to
            // trigger the race.
            adm.seqnoAckReceived(replica2, 2);
        }
    });
    adm.seqnoAckReceived(replica1, 1);
    vb->processResolvedSyncWrites();

    // Check: Should be zero tracked after the two (concurrent)
    // seqnoAckReceived() calls.
    assertNumTrackedAndHPSAndHCS(0, 2, 2);

    // Check: Commits in checkpoint should be in same order as prepares.
    std::vector<queued_item> items;
    ckptMgr->getNextItemsForDcp(*cursor.takeCursor().lock(), items);
    ASSERT_EQ(2, items.size());
    EXPECT_TRUE(items[0]->isCommitted());
    EXPECT_EQ(makeStoredDocKey("key1"), items[0]->getKey());
    EXPECT_TRUE(items[1]->isCommitted());
    EXPECT_EQ(makeStoredDocKey("key2"), items[1]->getKey());
}

TEST_P(ActiveDurabilityMonitorTest,
       CommitTopologyWithSyncWriteInCompletedQueue) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json({{active, replica1, replica2}}));
    DurabilityMonitorTest::addSyncWrites({1});

    notifyPersistence(1, 1, 0);

    setSeqnoAckReceivedPostProcessHook([this, &adm]() {
        adm.setReplicationTopology(nlohmann::json::array({{active, replica1}}));
        this->vb->processResolvedSyncWrites();
    });

    adm.seqnoAckReceived("replica1", 1);
}

TEST_P(ActiveDurabilityMonitorTest,
       MovingSyncWriteToResolvedQueueShouldInvalidateChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json({{active, replica1, replica2}}));
    DurabilityMonitorTest::addSyncWrites({1});

    // Should be enough to commit
    notifyPersistence(1, 1, 0);
    adm.seqnoAckReceived("replica1", 1);

    // Not resolved yet but in the resolved queue, means we won't try to
    // update the chains
    ASSERT_EQ(0, adm.getNumTracked());

    // Recreate the chain to invalidate pointers to the first one
    adm.setReplicationTopology(nlohmann::json({{active, replica1, replica3}}));

    // Don't care about the resulting cookies
    adm.prepareTransitionAwayFromActive();

    // Before the fix we would segfault here
    std::stringstream ss;
    ss << adm;
}

// @todo: Refactor test suite and expand test cases
TEST_P(ActiveDurabilityMonitorPersistentTest,
       SeqnoAckReceived_PersistToMajority) {
    testSeqnoAckPersistToMajority({replica1});
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       SeqnoAckReceived_PersistToMajorityTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    testSeqnoAckPersistToMajority({replica1, replica2});
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_NotAnArray) {
    try {
        getActiveDM().setReplicationTopology(nlohmann::json::object());
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is not an array") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_Empty) {
    try {
        getActiveDM().setReplicationTopology(nlohmann::json::array());
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Topology is empty") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_FirstChainEmpty) {
    testChainEmpty(nlohmann::json::array({{}}),
                   DurabilityMonitor::ReplicationChainName::First);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_SecondChainEmpty) {
    // It's valid to not have a second chain, but we shouldn't have an empty
    // second chain
    testChainEmpty(nlohmann::json::array(
                           {{active, replica1}, nlohmann::json::array()}),
                   DurabilityMonitor::ReplicationChainName::Second);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_TooManyChains) {
    try {
        getActiveDM().setReplicationTopology(nlohmann::json::array(
                {{active, replica1}, {active, replica2}, {active, replica3}}));
    } catch (const std::invalid_argument& e) {
        EXPECT_TRUE(std::string(e.what()).find("Too many chains specified") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_FirstChainUndefinedActive) {
    testChainUndefinedActive(nlohmann::json::array({{nullptr}}),
                             DurabilityMonitor::ReplicationChainName::First);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_SecondChainUndefinedActive) {
    testChainUndefinedActive(
            nlohmann::json::array({{active, replica1}, {nullptr}}),
            DurabilityMonitor::ReplicationChainName::Second);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_TooManyNodesInFirstChain) {
    testChainTooManyNodes(nlohmann::json::array({{"active",
                                                  "replica1",
                                                  "replica2",
                                                  "replica3",
                                                  "replica4"}}),
                          DurabilityMonitor::ReplicationChainName::First);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_TooManyNodesInSecondChain) {
    testChainTooManyNodes(
            nlohmann::json::array(
                    {{active, replica1},
                     {active, replica1, replica2, replica3, "replica4"}}),
            DurabilityMonitor::ReplicationChainName::Second);
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_NodeDuplicateInFirstChain) {
    testChainDuplicateNode(nlohmann::json::array({{"node1", "node1"}}));
}

TEST_P(ActiveDurabilityMonitorTest, SetTopology_NodeDuplicateInSecondChain) {
    testChainDuplicateNode(
            nlohmann::json::array({{active, replica1}, {active, active}}));
}

TEST_P(ActiveDurabilityMonitorTest, Fallback_CannotCompleteInMajority) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active, replica1}}));
    DurabilityMonitorTest::addSyncWrites({1});

    // Check that neither strategy allows the write to be completed.
    adm.setAndProcessCommitStrategy(
            DurabilityMonitor::CommitStrategy::MajorityAck);
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);
    adm.setAndProcessCommitStrategy(
            DurabilityMonitor::CommitStrategy::MajorityAck);

    // Check that the write has not been resolved.
    ASSERT_EQ(1, adm.getNumTracked());
}

// MajorityAckFallbackToMasterAckOnly allows durability when no replica is present.
TEST_P(ActiveDurabilityMonitorTest, Fallback_DurabilityPossible) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active, nullptr}}));

    adm.setAndProcessCommitStrategy(
            DurabilityMonitor::CommitStrategy::MajorityAck);
    EXPECT_FALSE(adm.isDurabilityPossible());

    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);
    EXPECT_TRUE(adm.isDurabilityPossible());
}

// MajorityAckFallbackToMasterAckOnly commits immediately when no replica is present
// (durability impossibe in MajorityAck).
TEST_P(ActiveDurabilityMonitorTest, Fallback_CommitWhenMajorityOne) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active, nullptr}}));
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);

    DurabilityMonitorTest::addSyncWrites({1, 2});

    // Check that the writes have been committed without replica ack.
    EXPECT_EQ(2, adm.getNumCommittedNotDurable());
}

// MajorityAckFallbackToMasterAckOnly does not commit when persist level is used.
TEST_P(ActiveDurabilityMonitorTest, Fallback_PersistWithFallback) {
    if (!persistent()) {
        // Persistence level not valid for ephemeral
        GTEST_SKIP();
    }

    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active, nullptr}}));
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);

    using namespace cb::durability;
    DurabilityMonitorTest::addSyncWrites(
            {1, 2},
            Requirements(Level::MajorityAndPersistOnMaster,
                         Timeout::Infinity()));

    // Check that the writes have been resolved with the new strategy.
    EXPECT_EQ(2, adm.getNumTracked());
}

// MajorityAckFallbackToMasterAckOnly allows unresolved writes to commit when the
// topology becomes active-only.
TEST_P(ActiveDurabilityMonitorTest, Fallback_CompleteOnTopologyChange) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active, replica1}}));
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);

    DurabilityMonitorTest::addSyncWrites({1});

    // Check that the writes have not resolved, due to majority > 1.
    EXPECT_EQ(1, adm.getNumTracked());

    adm.setReplicationTopology(nlohmann::json::array({{active, nullptr}}));

    // Check that the writes have been resolved after the failover.
    EXPECT_EQ(0, adm.getNumTracked());

    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()));

    EXPECT_EQ(1, adm.getNumCommittedNotDurable());
}

// When we have two replication chains, we should perform the write durably
// where possible. When the first chain support majority ack, we should wait for
// the replica to ack the write. When the second chain doesn't support majority
// ack, we should satisfy the write once the first chain is satisfied.
TEST_P(ActiveDurabilityMonitorTest, Fallback_OneReplicaInFirstChain) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, nullptr}}));
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);

    DurabilityMonitorTest::addSyncWrites({1});

    // Check that the writes have not resolved, because the firstChain requires
    // replica ack.
    EXPECT_EQ(1, adm.getNumTracked());

    adm.seqnoAckReceived(replica1, 1);
    vb->processResolvedSyncWrites();

    EXPECT_EQ(1, adm.getNumCommittedNotDurable());
}

TEST_P(ActiveDurabilityMonitorTest, Fallback_OneReplicaInSecondChain) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, nullptr}, {active, replica1}}));
    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);

    DurabilityMonitorTest::addSyncWrites({1});

    // Check that the writes have not resolved, because the secondChain requires
    // replica ack.
    EXPECT_EQ(1, adm.getNumTracked());

    adm.seqnoAckReceived(replica1, 1);
    vb->processResolvedSyncWrites();

    EXPECT_EQ(1, adm.getNumCommittedNotDurable());
}

TEST_P(ActiveDurabilityMonitorTest, SeqnoAckReceived_MultipleReplicas) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}})));
    ASSERT_EQ(4, adm.getFirstChainSize());

    const int64_t preparedSeqno = 1;
    addSyncWrite(preparedSeqno);
    ASSERT_EQ(1, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(preparedSeqno /*HPS*/, 0);
        for (const auto& node : {replica1, replica2, replica3}) {
            assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        }
    }

    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica2,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    // Majority reached
    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica3,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             0 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             1 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceived_MultipleReplicasTwoChains) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3},
                                   {active, replica1, replica2, replica4}})));
    ASSERT_EQ(4, adm.getFirstChainSize());
    ASSERT_EQ(4, adm.getSecondChainSize());

    const int64_t preparedSeqno = 1;
    addSyncWrite(preparedSeqno);
    ASSERT_EQ(1, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(preparedSeqno /*HPS*/, 0);
        for (const auto& node : {replica1, replica2, replica3, replica4}) {
            assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        }
    }

    /*
     * replica2 ack
     * replica3 ack -> majority satisfied in FirstChain
     */
    for (const auto& node : {replica2, replica3}) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(node,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    // This satisfies majority in SecondChain, so Prepare is committed
    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica4,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             0 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             1 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SeqnoAckReceived_MultipleReplicasTwoChainsDisjoint) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3},
                                   {active, replica4, replica5, replica6}})));
    ASSERT_EQ(4, adm.getFirstChainSize());
    ASSERT_EQ(4, adm.getSecondChainSize());

    const int64_t preparedSeqno = 1;
    addSyncWrite(preparedSeqno);
    ASSERT_EQ(1, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertHPSAndHCS(preparedSeqno /*HPS*/, 0);
        for (const auto& node :
             {replica1, replica2, replica3, replica4, replica5, replica6}) {
            assertNodeTracking(node, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        }
    }

    /*
     * replica2 ack
     * replica3 ack -> majority satisfied in FirstChain
     * replica4 ack
     */
    for (const auto& node : {replica2, replica3, replica4}) {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(node,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    // This satisfies majority in SecondChain, so Prepare is committed
    {
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica5,
                             preparedSeqno /*ackSeqno*/,
                             preparedSeqno /*expectedLastWriteSeqno*/,
                             preparedSeqno /*expectedLastAckSeqno*/,
                             0 /*expectedNumTracked*/,
                             preparedSeqno /*expectedHPS*/,
                             1 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica6, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }
}

TEST_P(ActiveDurabilityMonitorTest, NeverExpireIfTimeoutNotSet) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}})));
    ASSERT_EQ(2, adm.getFirstChainSize());

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements(Level::Majority, Timeout::Infinity()));
    EXPECT_EQ(1, monitor->getNumTracked());

    // Never expire, neither after 1 year !
    const auto year = std::chrono::hours(24 * 365);
    simulateTimeoutCheck(adm, std::chrono::steady_clock::now() + year);

    // Not expired, still tracked
    EXPECT_EQ(1, monitor->getNumTracked());
}

void ActiveDurabilityMonitorTest::simulateTimeoutCheck(
        ActiveDurabilityMonitor& adm,
        std::chrono::steady_clock::time_point now) const {
    adm.processTimeout(now);
    adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock()));
}

TEST_P(ActiveDurabilityMonitorTest, ProcessTimeout) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}})));
    ASSERT_EQ(2, adm.getFirstChainSize());

    /*
     * 1 SyncWrite
     */

    const auto level = cb::durability::Level::Majority;

    addSyncWrite(1 /*seqno*/, {level, cb::durability::Timeout(1)});
    EXPECT_EQ(1, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 1 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    simulateTimeoutCheck(
            adm,
            std::chrono::steady_clock::now() + std::chrono::milliseconds(1000));

    EXPECT_EQ(0, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        // @todo: Here and below, the next check means that after abort the
        //     tracking for a node (the Active in this case) is like:
        //     - Position::it -> points to Container::end()
        //     - Position::lastWriteSeqno -> stays set to the seqno of the
        //         aborted Prepare
        // Is that what we want?
        assertNodeTracking(active, 1 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    /*
     * Multiple SyncWrites, ordered by timeout
     */

    addSyncWrite(101 /*seqno*/, {level, cb::durability::Timeout(1)});
    addSyncWrite(102 /*seqno*/, {level, cb::durability::Timeout(10)});
    addSyncWrite(103 /*seqno*/, {level, cb::durability::Timeout(20)});
    ASSERT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 103 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    simulateTimeoutCheck(adm,
                         std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(10000));

    EXPECT_EQ(0, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 103 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    /*
     * Multiple SyncWrites, not ordered by timeout
     */

    addSyncWrite(201 /*seqno*/, {level, cb::durability::Timeout(20)});
    addSyncWrite(202 /*seqno*/, {level, cb::durability::Timeout(1)});
    addSyncWrite(203 /*seqno*/, {level, cb::durability::Timeout(50000)});
    ASSERT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 203 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    simulateTimeoutCheck(adm,
                         std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(10000));

    EXPECT_EQ(1, monitor->getNumTracked());
    auto tracked = adm.getTrackedSeqnos();
    EXPECT_TRUE(tracked.find(201) == tracked.end());
    EXPECT_TRUE(tracked.find(202) == tracked.end());
    EXPECT_TRUE(tracked.find(203) != tracked.end());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 203 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    simulateTimeoutCheck(adm,
                         std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(100000));

    EXPECT_EQ(0, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 203 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    /*
     * Multiple SyncWrites, in reverse timeout order.
     * Note that we must complete SyncWrites in order, thus even if a later
     * SyncWrite has timed out we must wait for the earlier one to complete.
     */
    addSyncWrite(301 /*seqno*/, {level, cb::durability::Timeout(20000)});
    addSyncWrite(302 /*seqno*/, {level, cb::durability::Timeout(10000)});
    addSyncWrite(303 /*seqno*/, {level, cb::durability::Timeout(1)});
    ASSERT_EQ(3, monitor->getNumTracked());
    {
        CB_SCOPED_TRACE("");
        assertNodeTracking(active, 303 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
        assertNodeTracking(replica1, 0 /*lastWriteSeqno*/, 0 /*lastAckSeqno*/);
    }

    simulateTimeoutCheck(
            adm,
            std::chrono::steady_clock::now() + std::chrono::milliseconds(5000));

    // A second processTimeout (now up to 15s later). Still shouldn't time
    // anything out as would break In-Order completion.
    simulateTimeoutCheck(adm,
                         std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(15000));
    EXPECT_EQ(3, monitor->getNumTracked());

    // Only when the first item reaches it's timeout can we process all of them.
    simulateTimeoutCheck(adm,
                         std::chrono::steady_clock::now() +
                                 std::chrono::milliseconds(30000));
    EXPECT_EQ(0, monitor->getNumTracked());
}

TEST_P(ActiveDurabilityMonitorPersistentTest, MajorityAndPersistOnMaster) {
    testSeqnoAckMajorityAndPersistOnMaster({replica1});
}

TEST_P(ActiveDurabilityMonitorPersistentTest, MajorityAndPersistOnMasterTwoChains) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));
    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getSecondChainSize());

    testSeqnoAckMajorityAndPersistOnMaster({replica1, replica2});
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       PersistToMajority_EnsurePersistAtActive) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2}})));
    ASSERT_EQ(3, adm.getFirstChainSize());

    testSeqnoAckPersistToMajority({replica1, replica2});
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       PersistToMajority_EnsurePersistAtActiveTwoChains) {
    auto& adm = getActiveDM();
    ASSERT_NO_THROW(adm.setReplicationTopology(nlohmann::json::array(
            {{active, replica1, replica2}, {active, replica3, replica4}})));
    ASSERT_EQ(3, adm.getFirstChainSize());
    ASSERT_EQ(3, adm.getSecondChainSize());

    testSeqnoAckPersistToMajority({replica1, replica2, replica3, replica4});
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

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_NoReplica) {
    EXPECT_TRUE(testDurabilityPossible(
            {{"active"}},
            makePendingWithSeqno(1, cb::durability::Level::PersistToMajority),
            1 /*expectedFirstChainSize*/,
            1 /*expectedFirstChainMajority*/));
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_TwoChains_NoReplica) {
    EXPECT_TRUE(testDurabilityPossible(
            {{active}, {replica1}},
            makePendingWithSeqno(1, cb::durability::Level::PersistToMajority),
            1 /*expectedFirstChainSize*/,
            1 /*expectedFirstChainMajority*/,
            1 /*expectedSecondChainSize*/,
            1 /*expectedSecondChainMajority*/));
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_1Replica) {
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica"}},
                                           makePendingWithSeqno(1),
                                           2 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible({{"active", nullptr}},
                                            makePendingWithSeqno(2),
                                            1 /*expectedFirstChainSize*/,
                                            2 /*expectedFirstChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_TwoChains_1Replica) {
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(
                testDurabilityPossible({{active, replica1}, {active, replica2}},
                                       makePendingWithSeqno(1),
                                       2 /*expectedFirstChainSize*/,
                                       2 /*expectedFirstChainMajority*/,
                                       2 /*expectedSecondChainSize*/,
                                       2 /*expectedSecondChainMajority*/));
    }

    auto item = makePendingWithSeqno(2);
    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, nullptr}, {active, replica2}},
                                       item,
                                       1 /*expectedFirstChainSize*/,
                                       2 /*expectedFirstChainMajority*/,
                                       2 /*expectedSecondChainSize*/,
                                       2 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, replica1}, {active, nullptr}},
                                       item,
                                       2 /*expectedFirstChainSize*/,
                                       2 /*expectedFirstChainMajority*/,
                                       1 /*expectedSecondChainSize*/,
                                       2 /*expectedSecondChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_2Replicas) {
    {
        CB_SCOPED_TRACE("");
        auto item = makePendingWithSeqno(1);
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica1", "replica2"}},
                                           item,
                                           3 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        auto item = makePendingWithSeqno(2);
        EXPECT_TRUE(testDurabilityPossible({{"active", "replica1", nullptr}},
                                           item,
                                           2 /*expectedFirstChainSize*/,
                                           2 /*expectedFirstChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        auto item = makePendingWithSeqno(3);
        EXPECT_FALSE(testDurabilityPossible({{"active", nullptr, nullptr}},
                                            item,
                                            1 /*expectedFirstChainSize*/,
                                            2 /*expectedFirstChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_TwoChains_2Replicas) {
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{active, replica1, replica2}, {active, replica3, replica4}},
                makePendingWithSeqno(1),
                3 /*expectedFirstChainSize*/,
                2 /*expectedFirstChainMajority*/,
                3 /*expectedSecondChainSize*/,
                2 /*expectedSecondChainMajority*/));
    }

    auto item = makePendingWithSeqno(2);
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{active, replica1, nullptr}, {active, replica3, replica4}},
                item,
                2 /*expectedFirstChainSize*/,
                2 /*expectedFirstChainMajority*/,
                3 /*expectedSecondChainSize*/,
                2 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible(
                {{active, nullptr, nullptr}, {active, replica3, replica4}},
                item,
                1 /*expectedFirstChainSize*/,
                2 /*expectedFirstChainMajority*/,
                3 /*expectedSecondChainSize*/,
                2 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{active, replica1, replica2}, {active, replica3, nullptr}},
                makePendingWithSeqno(4),
                3 /*expectedFirstChainSize*/,
                2 /*expectedFirstChainMajority*/,
                2 /*expectedSecondChainSize*/,
                2 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(testDurabilityPossible(
                {{active, replica1, replica2}, {active, nullptr, nullptr}},
                makePendingWithSeqno(5),
                3 /*expectedFirstChainSize*/,
                2 /*expectedFirstChainMajority*/,
                1 /*expectedSecondChainSize*/,
                2 /*expectedSecondChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_3Replicas) {
    // Note: In this test, playing with Undefined nodes in different and
    // non-consecutive positions.
    // Not sure if ns_server can set something like that (e.g. {A, u, R2, u}),
    // but better that we cover the most general case.
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{"active", "replica1", "replica2", "replica3"}},
                makePendingWithSeqno(1),
                4 /*expectedFirstChainSize*/,
                3 /*expectedFirstChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(testDurabilityPossible(
                {{"active", "replica1", nullptr, "replica3"}},
                makePendingWithSeqno(2),
                3 /*expectedFirstChainSize*/,
                3 /*expectedFirstChainMajority*/));
    }

    auto item = makePendingWithSeqno(3);
    {
        CB_SCOPED_TRACE("");
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
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{"active", nullptr, nullptr, nullptr}},
                                       item,
                                       1 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, DurabilityImpossible_TwoChains_3Replicas) {
    // Note: In this test, playing with Undefined nodes in different and
    // non-consecutive positions.
    // Not sure if ns_server can set something like that (e.g. {A, u, R2, u}),
    // but better that we cover the most general case.
    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(
                testDurabilityPossible({{active, replica1, replica2, replica3},
                                        {active, replica4, replica5, replica6}},
                                       makePendingWithSeqno(1),
                                       4 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       4 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(
                testDurabilityPossible({{active, replica1, nullptr, replica3},
                                        {active, replica4, replica5, replica6}},
                                       makePendingWithSeqno(2),
                                       3 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       4 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    auto item3 = makePendingWithSeqno(3);
    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, nullptr, nullptr, replica3},
                                        {active, replica3, replica5, replica6}},
                                       item3,
                                       2 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       4 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, nullptr, nullptr, nullptr},
                                        {active, replica4, replica5, replica6}},
                                       item3,
                                       1 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       4 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_TRUE(
                testDurabilityPossible({{active, replica1, replica2, replica3},
                                        {active, replica4, nullptr, replica6}},
                                       item3,
                                       4 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       3 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    auto item4 = makePendingWithSeqno(4);
    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, replica1, replica2, replica3},
                                        {active, nullptr, nullptr, replica6}},
                                       item4,
                                       4 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       2 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }

    {
        CB_SCOPED_TRACE("");
        EXPECT_FALSE(
                testDurabilityPossible({{active, replica1, replica2, replica3},
                                        {active, nullptr, nullptr, nullptr}},
                                       item4,
                                       4 /*expectedFirstChainSize*/,
                                       3 /*expectedFirstChainMajority*/,
                                       1 /*expectedSecondChainSize*/,
                                       3 /*expectedSecondChainMajority*/));
    }
}

TEST_P(ActiveDurabilityMonitorTest, dropOnlyKey) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));

    addSyncWrite(1);
    ASSERT_EQ(1, adm.getNumTracked());
    ASSERT_EQ(0, adm.getHighCompletedSeqno());
    ASSERT_EQ(1, adm.getHighPreparedSeqno());

    adm.eraseSyncWrite(makeStoredDocKey("key1"), 1);

    EXPECT_EQ(0, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
    EXPECT_EQ(1, adm.getHighPreparedSeqno());

    // Should have notified the client
    auto* mockCookie = cookie_to_mock_cookie(cookie);
    ASSERT_TRUE(mockCookie);
    EXPECT_EQ(cb::engine_errc::sync_write_ambiguous,
              mock_waitfor_cookie(mockCookie));
}

void ActiveDurabilityMonitorTest::testDropFirstKey() {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));

    addSyncWrites(1, 2);
    ASSERT_EQ(2, adm.getNumTracked());
    ASSERT_EQ(0, adm.getHighCompletedSeqno());
    ASSERT_EQ(2, adm.getHighPreparedSeqno());

    adm.eraseSyncWrite(makeStoredDocKey("key1"), 1);

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
    EXPECT_EQ(2, adm.getHighPreparedSeqno());

    auto seqnos = adm.getTrackedSeqnos();
    ASSERT_EQ(1, seqnos.size());
    EXPECT_EQ(1, seqnos.count(2));

    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 2));
}

TEST_P(ActiveDurabilityMonitorTest, dropFirstKey) {
    testDropFirstKey();
}

void ActiveDurabilityMonitorTest::testDropLastKey() {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}}));

    addSyncWrites(1, 2);
    ASSERT_EQ(2, adm.getNumTracked());
    ASSERT_EQ(0, adm.getHighCompletedSeqno());
    ASSERT_EQ(2, adm.getHighPreparedSeqno());

    adm.eraseSyncWrite(makeStoredDocKey("key2"), 2);

    EXPECT_EQ(1, adm.getNumTracked());
    EXPECT_EQ(0, adm.getHighCompletedSeqno());
    EXPECT_EQ(2, adm.getHighPreparedSeqno());

    auto seqnos = adm.getTrackedSeqnos();
    ASSERT_EQ(1, seqnos.size());
    EXPECT_EQ(1, seqnos.count(1));
}

TEST_P(ActiveDurabilityMonitorTest, dropLastKey) {
    testDropLastKey();
}

TEST_P(ActiveDurabilityMonitorTest, dropFirstKeyAndAck) {
    testDropFirstKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 1));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 1));
}

TEST_P(ActiveDurabilityMonitorTest, dropFirstKeyAndAckNext) {
    testDropFirstKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 2));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 2));
}

TEST_P(ActiveDurabilityMonitorTest, dropLastKeyAndAck) {
    testDropLastKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 2));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 2));
}

TEST_P(ActiveDurabilityMonitorTest, dropLastKeyAndAckPrevious) {
    testDropLastKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 1));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 1));
}

TEST_P(ActiveDurabilityMonitorTest, dropFirstKeyAndCompleteSecond) {
    testDropFirstKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 2));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 2));

    vb->processResolvedSyncWrites();

    EXPECT_EQ(2, adm.getHighCompletedSeqno());
}

TEST_P(ActiveDurabilityMonitorTest, dropLastKeyAndCompleteFirst) {
    testDropLastKey();

    auto& adm = getActiveDM();
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica1, 2));
    EXPECT_NO_THROW(adm.seqnoAckReceived(replica2, 2));

    vb->processResolvedSyncWrites();

    EXPECT_EQ(1, adm.getHighCompletedSeqno());
}

/// Test fixture which does not set an initial topology for the ADM.
class ActiveDurabilityMonitorNullTopologyTest
    : public ActiveDurabilityMonitorTest {
public:
    void SetUp() override {
        setup(3, false);
    }
};

/// Check that we do not allow SyncWrites to complete before setting the
/// topology (topology is null immediately after switch to active).
TEST_P(ActiveDurabilityMonitorNullTopologyTest,
       Fallback_DurabilityImpossibleWithNullTopology) {
    auto& adm = getActiveDM();
    EXPECT_EQ(0, adm.getFirstChainSize());

    adm.setAndProcessCommitStrategy(
            DurabilityMonitor::CommitStrategy::MajorityAck);
    EXPECT_FALSE(adm.isDurabilityPossible());

    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);
    EXPECT_FALSE(adm.isDurabilityPossible());
}

/**
 * Test with the production limit of 2 replicas.
 */
class ActiveDurabilityMonitorWithReplicaLimitTest
    : public ActiveDurabilityMonitorTest {
public:
    void SetUp() override {
        setup(2, false);
    }
};

/// Check that enabling the fallback strategy does not allow SyncWrites with
/// unsupported number of replicas.
TEST_P(ActiveDurabilityMonitorWithReplicaLimitTest, Fallback_TooManyReplicas) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}}));

    adm.setAndProcessCommitStrategy(
            DurabilityMonitor::CommitStrategy::MajorityAck);
    EXPECT_FALSE(adm.isDurabilityPossible());

    adm.setAndProcessCommitStrategy(DurabilityMonitor::CommitStrategy::
                                            MajorityAckFallbackToMasterAckOnly);
    EXPECT_FALSE(adm.isDurabilityPossible());
}

void PassiveDurabilityMonitorTest::SetUp() {
    STParameterizedBucketTest::SetUp();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    vb = store->getVBuckets().getBucket(vbid).get();
    ASSERT_TRUE(vb);
    monitor = &VBucketTestIntrospector::public_getPassiveDM(*vb);
    ASSERT_TRUE(monitor);
}

void PassiveDurabilityMonitorTest::TearDown() {
    // Calling the ostream operator will iterate over the entire PDM and may
    // catch any 'corruption' issues in the iterators used.
    std::stringstream ss;
    ss << getPassiveDM();

    STParameterizedBucketTest::TearDown();
}

PassiveDurabilityMonitor& PassiveDurabilityMonitorTest::getPassiveDM() const {
    return dynamic_cast<PassiveDurabilityMonitor&>(*monitor);
}

TEST_P(PassiveDurabilityMonitorTest, AddSyncWrite) {
    ASSERT_EQ(0, monitor->getNumTracked());
    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    EXPECT_EQ(1, monitor->getNumTracked());
}

/// Check that attempting to add a SyncWrite to PDM with default timeout
/// fails (active should have set an explicit timeout).
TEST_P(PassiveDurabilityMonitorTest, AddSyncWriteDefaultTimeoutInvalid) {
    using namespace cb::durability;
    auto item = makePendingItem(makeStoredDocKey("key"),
                                "value",
                                Requirements{Level::Majority, Timeout()});
    EXPECT_THROW(VBucketTestIntrospector::public_getPassiveDM(*vb).addSyncWrite(
                         item),
                 std::invalid_argument);
}

void DurabilityMonitorTest::addSyncWrite(const std::vector<int64_t>& seqnos,
                                         const cb::durability::Level level,
                                         const int64_t expectedHPS,
                                         const int64_t expectedHCS) {
    const size_t expectedNumTracked = monitor->getNumTracked() + seqnos.size();
    // Use an appropriate timeout for the given type of DurabilityMonitor -
    // - active DM usually has non-infinite timeout (unless the SyncWrites
    // were grandfathered in from a PassiveDM).
    // - passive DM must have infinite timeout.
    const auto timeout = vb->getState() == vbucket_state_active
                                 ? cb::durability::Timeout(10)
                                 : cb::durability::Timeout::Infinity();
    addSyncWrites(seqnos, cb::durability::Requirements{level, timeout});
    assertNumTrackedAndHPSAndHCS(expectedNumTracked, expectedHPS, expectedHCS);
}

void DurabilityMonitorTest::assertNumTrackedAndHPSAndHCS(
        const size_t expectedNumTracked,
        const int64_t expectedHPS,
        const int64_t expectedHCS) const {
    ASSERT_EQ(expectedNumTracked, monitor->getNumTracked());
    ASSERT_EQ(expectedHPS, monitor->getHighPreparedSeqno());
    ASSERT_EQ(expectedHCS, monitor->getHighCompletedSeqno());
}

void DurabilityMonitorTest::notifyPersistence(const int64_t persistedSeqno,
                                              const int64_t expectedHPS,
                                              const int64_t expectedHCS) {
    vb->setPersistenceSeqno(persistedSeqno);
    monitor->notifyLocalPersistence();
    ASSERT_EQ(expectedHPS, monitor->getHighPreparedSeqno());
    ASSERT_EQ(expectedHCS, monitor->getHighCompletedSeqno());
}

void DurabilityMonitorTest::replaceSeqnoAckCB(
        std::function<void(Vbid vbid, int64_t seqno)> cb) {
    vb->seqnoAckCb = cb;
}

TEST_P(PassiveDurabilityMonitorTest, HPS_Majority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite({1, 2, 3} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        0 /*expectedHPS*/,
                                        0 /*expectedHCS*/);

    notifySnapEndReceived(3 /*snapEnd*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    notifyPersistence(
            1000 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, HPS_Majority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 2, 3} /*seqnos*/,
                 cb::durability::Level::Majority,
                 3 /*expectedHPS*/,
                 0 /*expectedHCS*/);

    notifyPersistence(
            1000 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);
}

void PassiveDurabilityMonitorTest::testResolvePrepare(
        PassiveDurabilityMonitor::Resolution res) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    // PassiveDM doesn't track anything yet, no commit expected
    auto& pdm = getPassiveDM();
    auto thrown{false};
    try {
        pdm.completeSyncWrite(makeStoredDocKey("akey"), res, 1);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find(
                            "No tracked, but received " +
                            PassiveDurabilityMonitor::to_string(res) +
                            " for key") != std::string::npos);
        thrown = true;
    }
    if (!thrown) {
        FAIL();
    }

    const std::vector<int64_t> seqnos{1, 2, 3};
    DurabilityMonitorTest::addSyncWrite(seqnos,
                                        cb::durability::Level::Majority,
                                        0 /*expectHPS*/,
                                        0 /*expectedHCS*/);
    ASSERT_EQ(seqnos.size(), monitor->getNumTracked());

    notifySnapEndReceived(3 /*snapEnd*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    // A negative check first: we must enforce In-Order Commit at Active, so
    // Replica expects a commit for s:1 at this point.
    thrown = false;
    try {
        pdm.completeSyncWrite(makeStoredDocKey("key2"), res, 1);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find(
                            "received unexpected " +
                            PassiveDurabilityMonitor::to_string(res) +
                            " for key") != std::string::npos);
        thrown = true;
    }
    if (!thrown) {
        FAIL();
    }

    // Commit all Prepares now
    uint8_t numTracked = monitor->getNumTracked();
    for (const auto s : seqnos) {
        pdm.completeSyncWrite(
                makeStoredDocKey("key" + std::to_string(s)), res, s);
        EXPECT_EQ(--numTracked, monitor->getNumTracked());
    }
    EXPECT_EQ(0, monitor->getNumTracked());
}

void PassiveDurabilityMonitorTest::testResolvePrepareOutOfOrder(
        PassiveDurabilityMonitor::Resolution res) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    vb->checkpointManager->createSnapshot(
            1, 7, 1 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);

    // PassiveDM doesn't track anything yet, no commit expected
    auto& pdm = getPassiveDM();

    if (res == PassiveDurabilityMonitor::Resolution::Commit) {
        // aborts may legitimately be seen without a preceding prepare
        // due to dedupe.
        auto thrown{false};
        try {
            pdm.completeSyncWrite(makeStoredDocKey("akey"), res, 1);
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(std::string(e.what()).find(
                                "No tracked, but received " +
                                PassiveDurabilityMonitor::to_string(res) +
                                " for key") != std::string::npos);
            thrown = true;
        }
        if (!thrown) {
            FAIL();
        }
    }

    auto key1 = makeStoredDocKey("key1");
    auto key2 = makeStoredDocKey("key2");
    auto key3 = makeStoredDocKey("key3");
    auto req =
            cb::durability::Requirements{cb::durability::Level::Majority,
                                         cb::durability::Timeout::Infinity()};
    // Reuse the checkpoint manager by setting the seqno range to the same as
    // above
    vb->checkpointManager->createSnapshot(
            1, 7, 3 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);

    for (uint64_t seqno = 1; seqno < 4; seqno++) {
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

        processSet(item);
    }

    assertNumTrackedAndHPSAndHCS(3, 0, 0);

    // We must enforce In-Order Commit at Active, but Passive can accept
    // out of order Commits
    // All the prepares should still be tracked as the HPS should not advance
    // until snapend
    pdm.completeSyncWrite(key2, res, 2);
    assertNumTrackedAndHPSAndHCS(3, 0, 2);

    // Check key1 is still in the hashtable, was not affected by completing key2
    // OOO
    auto vb = engine->getVBucket(vbid);
    {
        auto findResult = vb->ht.findOnlyPrepared(key1);
        auto* sv = findResult.storedValue;
        EXPECT_TRUE(sv);
    }

    // complete prepare for key3
    pdm.completeSyncWrite(key3, res, 3);
    assertNumTrackedAndHPSAndHCS(3, 0, 3);

    // complete prepare for key1
    pdm.completeSyncWrite(key1, res, 1);
    assertNumTrackedAndHPSAndHCS(3, 0, 3);

    notifySnapEndReceived(6 /*snapEnd*/, 0 /*expectedHPS*/, 3 /*expectedHCS*/);

    ASSERT_EQ(3, pdm.getNumTracked());

    // Snapshot-end persisted: HPS moves, now we can remove all Prepares
    // the HPS will advance to the snapshotEnd
    notifyPersistence(
            6 /*persistedSeqno*/, 6 /*expectedHPS*/, 3 /*expectedHCS*/);

    ASSERT_EQ(0, pdm.getNumTracked());
}

TEST_P(PassiveDurabilityMonitorTest, Commit) {
    testResolvePrepare(PassiveDurabilityMonitor::Resolution::Commit);
}

TEST_P(PassiveDurabilityMonitorTest, Abort) {
    testResolvePrepare(PassiveDurabilityMonitor::Resolution::Abort);
}

TEST_P(PassiveDurabilityMonitorPersistentTest, CommitOutOfOrder) {
    testResolvePrepareOutOfOrder(PassiveDurabilityMonitor::Resolution::Commit);
}

TEST_P(PassiveDurabilityMonitorPersistentTest, AbortOutOfOrder) {
    testResolvePrepareOutOfOrder(PassiveDurabilityMonitor::Resolution::Abort);
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       AbortWithoutPrepareRefusedFromMemorySnap) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    vb->checkpointManager->createSnapshot(
            1, 1, {} /*HCS*/, CheckpointType::Memory, 0 /*MVS*/);

    // PassiveDM doesn't track anything yet, no commit expected
    auto& pdm = getPassiveDM();

    auto thrown{false};
    try {
        pdm.completeSyncWrite(makeStoredDocKey("akey"),
                              PassiveDurabilityMonitor::Resolution::Abort,
                              1);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(
                std::string(e.what()).find(
                        "No tracked, but received " +
                        PassiveDurabilityMonitor::to_string(
                                PassiveDurabilityMonitor::Resolution::Abort) +
                        " for key") != std::string::npos);
        thrown = true;
    }
    if (!thrown) {
        FAIL();
    }
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       AbortWithoutPrepareAcceptedFromDiskSnap) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    vb->checkpointManager->createSnapshot(
            1, 1, 1 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);

    // PassiveDM doesn't track anything yet, no commit expected
    auto& pdm = getPassiveDM();

    EXPECT_NO_THROW(
            pdm.completeSyncWrite(makeStoredDocKey("akey"),
                                  PassiveDurabilityMonitor::Resolution::Abort,
                                  1));
}

void PassiveDurabilityMonitorTest::testRemoveCompletedOnlyIfLocallySatisfied(
        PassiveDurabilityMonitor::Resolution res) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0);

    // Add Majority to tracking
    DurabilityMonitorTest::addSyncWrite({1} /*seqno*/,
                                        cb::durability::Level::Majority,
                                        0 /*expectedHPS*/,
                                        0 /*expectedHCS*/);
    const auto& pdm = getPassiveDM();
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(1, pdm.getNumTracked());

    // Note: for simulating a real scenario, we have to receive the snapshot-end
    // mutation before receiving a Commit/Abort for the Prepare.
    // That is because at Active we ensure no-dedup by avoiding Prepare and
    // Commit/Abort (for the same key) in the same snapshot.

    // snapshot-end received -> HPS moves
    notifySnapEndReceived(1 /*snap-end*/, 1 /*expectedHPS*/, 0 /*expectedHCS*/);
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(1, pdm.getNumTracked());

    // Commit: HCP moves -> we can remove the Prepare from tracking
    getPassiveDM().completeSyncWrite(makeStoredDocKey("key1"), res, 1);
    ASSERT_EQ(1, pdm.getHighPreparedSeqno());
    ASSERT_EQ(1, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getNumTracked());

    if (!persistent()) {
        // PersistToMajority not valid for ephemeral
        return;
    }

    // Add PersistToMajority + Majority to tracking
    DurabilityMonitorTest::addSyncWrite(
            {2} /*seqno*/,
            cb::durability::Level::PersistToMajority,
            1 /*expectHPS*/,
            1 /*expectedHCS*/);
    ASSERT_EQ(1, pdm.getHighCompletedSeqno());
    ASSERT_EQ(1, pdm.getNumTracked());
    DurabilityMonitorTest::addSyncWrite({3} /*seqno*/,
                                        cb::durability::Level::Majority,
                                        1 /*expectHPS*/,
                                        1 /*expectedHCS*/);
    ASSERT_EQ(1, pdm.getHighCompletedSeqno());
    ASSERT_EQ(2, pdm.getNumTracked());

    // Snapshot-end received: HPS doesn't move as we have not persisted the
    // complete snapshot yet (ie, we cannot move the durability-fence
    // represented by the PersistToMajority seqno:2).
    notifySnapEndReceived(3 /*snap-end*/, 1 /*expectedHPS*/, 1 /*expectedHCS*/);
    ASSERT_EQ(1, pdm.getHighCompletedSeqno());
    ASSERT_EQ(2, pdm.getNumTracked());

    // Commit all, still 2 tracked as the Prepares are completed before the HPS
    // moves
    getPassiveDM().completeSyncWrite(makeStoredDocKey("key2"), res, 2);
    ASSERT_EQ(1, pdm.getHighPreparedSeqno());
    ASSERT_EQ(2, pdm.getHighCompletedSeqno());
    ASSERT_EQ(2, pdm.getNumTracked());
    getPassiveDM().completeSyncWrite(makeStoredDocKey("key3"), res, 3);
    ASSERT_EQ(1, pdm.getHighPreparedSeqno());
    ASSERT_EQ(3, pdm.getHighCompletedSeqno());
    ASSERT_EQ(2, pdm.getNumTracked());

    // Snapshot-end persisted: HPS moves, now we can remove all Prepares
    notifyPersistence(
            3 /*persistedSeqno*/, 3 /*expectedHPS*/, 3 /*expectedHCS*/);
    ASSERT_EQ(0, pdm.getNumTracked());
}

TEST_P(PassiveDurabilityMonitorTest, RemoveCommittedOnlyIfLocallySatisfied) {
    testRemoveCompletedOnlyIfLocallySatisfied(
            PassiveDurabilityMonitor::Resolution::Commit);
}

TEST_P(PassiveDurabilityMonitorTest, RemoveAbortedOnlyIfLocallySatisfied) {
    testRemoveCompletedOnlyIfLocallySatisfied(
            PassiveDurabilityMonitor::Resolution::Abort);
}

TEST_P(PassiveDurabilityMonitorPersistentTest, AckLatestPersistedSnapshot) {
    /* Once a full snapshot containing a PersistToMajority Prepare is persisted,
     * we should seqnoAck at that snapshot's end seqno even if a newer snapshot
     * has been received since. Simply acking when persistence passes the latest
     * snapshot end could lead to seqnoAck being delayed indefinitely by new
     * snapshots "shifting the goalpost". With a constant disk write queue size,
     * persistence might never pass the lastest snapshotEnd.
     */

    auto& pdm = getPassiveDM();

    // Tracking the actual seqnoAcks is a bit "belt and braces" because the HPS
    // is enough information, but it seems worth explicitly confirming we are
    // acking at the point we expect.
    std::queue<uint64_t> seqnoAcks{};

    DurabilityMonitorTest::replaceSeqnoAckCB(
            [&seqnoAcks](Vbid vbid, int64_t seqno) { seqnoAcks.push(seqno); });

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    // SNAP 1
    DurabilityMonitorTest::addSyncWrite(
            {1, 2, 3} /*seqnos*/,
            cb::durability::Level::PersistToMajority,
            0 /*expectedHPS*/,
            0 /*expectedHCS*/);

    // End snapshot, but not yet persisted
    notifySnapEndReceived(3 /*snapEnd*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(3, pdm.getNumTracked());

    // SNAP 2
    DurabilityMonitorTest::addSyncWrite(
            {4, 5, 6} /*seqnos*/,
            cb::durability::Level::PersistToMajority,
            0 /*expectedHPS*/,
            0 /*expectedHCS*/);

    // New snapshot received, first snapshot *still* not persisted
    notifySnapEndReceived(6 /*snapEnd*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(6, pdm.getNumTracked());

    // Persist first snapshot
    notifyPersistence(
            3 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    EXPECT_EQ(6, pdm.getNumTracked());
    EXPECT_EQ(3, seqnoAcks.front());
    seqnoAcks.pop();

    // SNAP 3
    DurabilityMonitorTest::addSyncWrite({7, 8, 9} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        3 /*expectedHPS*/,
                                        0 /*expectedHCS*/);

    notifySnapEndReceived(9 /*snapEnd*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    EXPECT_EQ(9, pdm.getNumTracked());
    EXPECT_TRUE(seqnoAcks.empty());
    notifyPersistence(
            6 /*persistedSeqno*/, 9 /*expectedHPS*/, 0 /*expectedHCS*/);

    // As snap 3 contains only Majority level, once the snap 2 is persisted
    // snap 3 end can be acked because snap 3 is entirely satisfied in memory
    EXPECT_EQ(9, seqnoAcks.front());
    seqnoAcks.pop();

    // nothing has been completed
    EXPECT_EQ(9, pdm.getNumTracked());

    for (uint64_t seqno = 1; seqno < 10; seqno++) {
        pdm.completeSyncWrite(makeStoredDocKey("key" + std::to_string(seqno)),
                              PassiveDurabilityMonitor::Resolution::Commit,
                              seqno);
    }

    EXPECT_EQ(0, pdm.getNumTracked());
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       DiskSnapshotsAreAckedOnlyAtSnapEnd) {
    /* Prepares received as part of disk snapshots should not be seqnoAcked
     * until the entire snapshot has been received -  Majority/PersistToMaster
     * level might have deduped PersistToMajority Prepares so they cannot be
     * acked until we have a consistent state.
     */

    auto& pdm = getPassiveDM();

    std::queue<uint64_t> seqnoAcks{};

    DurabilityMonitorTest::replaceSeqnoAckCB(
            [&seqnoAcks](Vbid vbid, int64_t seqno) { seqnoAcks.push(seqno); });

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite({1, 2, 3} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        0 /*expectedHPS*/,
                                        0 /*expectedHCS*/);

    // End snapshot, but not yet persisted
    vb->checkpointManager->createSnapshot(
            1, 3, 0 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);
    notifySnapEndReceived(3 /*snapEnd*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(3, pdm.getNumTracked());

    EXPECT_TRUE(seqnoAcks.empty());

    // Partially persist snapshot
    notifyPersistence(
            2 /*persistedSeqno*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(3, pdm.getNumTracked());

    // Shouldn't ack yet
    EXPECT_TRUE(seqnoAcks.empty());

    // Persist full snapshot
    notifyPersistence(
            3 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    // nothing has been completed, track everything still
    EXPECT_EQ(3, pdm.getNumTracked());

    // Should have acked
    EXPECT_EQ(3, seqnoAcks.front());
    seqnoAcks.pop();

    for (uint64_t seqno = 1; seqno < 4; seqno++) {
        pdm.completeSyncWrite(makeStoredDocKey("key" + std::to_string(seqno)),
                              PassiveDurabilityMonitor::Resolution::Commit,
                              seqno);
    }

    EXPECT_EQ(0, pdm.getNumTracked());
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       DiskSnapshotsAckSnapEndSeqnoPrepares) {
    /* Disk snapshots may be deduped, in some cases the replica might not
     * receive a prepare in the snapshot (deduping + commits sent as mutations)
     * BUT the PDM must still ack *in case* there are prepares it did not see
     */

    auto& pdm = getPassiveDM();

    std::queue<uint64_t> seqnoAcks{};

    DurabilityMonitorTest::replaceSeqnoAckCB(
            [&seqnoAcks](Vbid vbid, int64_t seqno) { seqnoAcks.push(seqno); });

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite({1} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        0 /*expectedHPS*/,
                                        0 /*expectedHCS*/);

    // End disk snapshot, but not yet persisted, no prepares tracked
    vb->checkpointManager->createSnapshot(
            1, 3, 0 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);
    notifySnapEndReceived(3 /*snapEnd*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(1, pdm.getNumTracked());

    EXPECT_TRUE(seqnoAcks.empty());

    // Partially persist snapshot
    notifyPersistence(
            2 /*persistedSeqno*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);

    // Shouldn't ack yet
    EXPECT_TRUE(seqnoAcks.empty());

    // Persist full snapshot
    notifyPersistence(
            3 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    // nothing has been completed, track everything still
    EXPECT_EQ(1, pdm.getNumTracked());

    // Should have acked
    EXPECT_EQ(3, seqnoAcks.front());
    seqnoAcks.pop();

    pdm.completeSyncWrite(makeStoredDocKey("key1"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          1);

    EXPECT_EQ(0, pdm.getNumTracked());
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       DiskSnapshotsAckSnapEndSeqnoNoPrepares) {
    /* Disk snapshots may be deduped, in some cases the replica might not
     * receive a prepare in the snapshot (deduping + commits sent as mutations)
     * BUT the PDM must still ack *in case* there are prepares it did not see.
     *
     * Check that old pre
     */

    auto& pdm = getPassiveDM();

    std::queue<uint64_t> seqnoAcks{};

    DurabilityMonitorTest::replaceSeqnoAckCB(
            [&seqnoAcks](Vbid vbid, int64_t seqno) { seqnoAcks.push(seqno); });

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    // NOT adding any prepares

    // End disk snapshot, but not yet persisted, no prepares tracked
    vb->checkpointManager->createSnapshot(
            1, 3, 0 /*HCS*/, CheckpointType::Disk, 0 /*MVS*/);
    notifySnapEndReceived(3 /*snapEnd*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    EXPECT_EQ(0, pdm.getNumTracked());

    EXPECT_TRUE(seqnoAcks.empty());

    // Partially persist snapshot
    notifyPersistence(
            2 /*persistedSeqno*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);

    // Shouldn't ack yet
    EXPECT_TRUE(seqnoAcks.empty());

    // Persist full snapshot
    notifyPersistence(
            3 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    // Should have acked
    EXPECT_EQ(3, seqnoAcks.front());
    seqnoAcks.pop();
}

void PassiveDurabilityMonitorTest::notifySnapEndReceived(int64_t snapEnd,
                                                         int64_t expectedHPS,
                                                         int64_t expectedHCS) {
    getPassiveDM().notifySnapshotEndReceived(snapEnd);
    ASSERT_EQ(expectedHPS, monitor->getHighPreparedSeqno());
    ASSERT_EQ(expectedHCS, monitor->getHighCompletedSeqno());
}

TEST_P(PassiveDurabilityMonitorPersistentTest, HPS_MajorityAndPersistOnMaster) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite(
            {1, 2, 3} /*seqnos*/,
            cb::durability::Level::MajorityAndPersistOnMaster,
            0 /*expectedHPS*/,
            0 /*expectedHCS*/);

    notifySnapEndReceived(
            500 /*snapEnd*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    notifyPersistence(
            1000 /*persistedSeqno*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorPersistentTest, HPS_MajorityAndPersistOnMaster) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    const std::vector<int64_t> seqnos{1, 2, 3};
    addSyncWrite(seqnos,
                 cb::durability::Level::MajorityAndPersistOnMaster,
                 0 /*expectedHPS*/,
                 0 /*expectedHCS*/);

    for (const auto s : seqnos) {
        notifyPersistence(
                s /*persistedSeqno*/, s /*expectedHPS*/, 0 /*expectedHCS*/);
    }
}

TEST_P(PassiveDurabilityMonitorPersistentTest, HPS_PersistToMajority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    const std::vector<int64_t> seqnos{1, 2, 3};
    DurabilityMonitorTest::addSyncWrite(
            seqnos,
            cb::durability::Level::PersistToMajority,
            0 /*expectHPS*/,
            0 /*expectedHCS*/);

    // All Prepares persisted but snapshot-end not received (so, not persisted),
    // so replica cannot ack yet.
    for (const auto s : seqnos) {
        notifyPersistence(
                s /*persistedSeqno*/, 0 /*expectedHPS*/, 0 /*expectedHCS*/);
    }

    // Snapshot-end received and all Prepares persisted, but we have not
    // persisted the complete snapshot, so PersistToMajority cannot be ack'ed
    // yet.
    const int64_t snapEnd = 1000;
    notifySnapEndReceived(snapEnd, 0 /*expectedHPS*/, 0 /*expectedHCS*/);

    // The flusher persists the entire snapshot (and over), PersistToMajority
    // are locally-satisfied now, the HPS can move on to them.
    notifyPersistence(snapEnd + 10 /*persistedSeqno*/,
                      3 /*expectedHPS*/,
                      0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorPersistentTest, HPS_PersistToMajority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    const std::vector<int64_t> seqnos{1, 2, 3};
    addSyncWrite(seqnos,
                 cb::durability::Level::PersistToMajority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    for (const auto s : seqnos) {
        notifyPersistence(
                s /*persistedSeqno*/, s /*expectedHPS*/, 0 /*expectedHCS*/);
    }
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       HPS_MajorityAndPersistOnMaster_Majority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite(
            {1, 2, 3} /*seqnos*/,
            cb::durability::Level::MajorityAndPersistOnMaster,
            0 /*expectHPS*/,
            0 /*expectedHCS*/);

    notifySnapEndReceived(3 /*snapEnd*/, 3 /*expectedHPS*/, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite({4, 10, 21} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        3 /*expectHPS*/,
                                        0 /*expectedHCS*/);

    notifySnapEndReceived(
            100 /*snapEnd*/, 21 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       HPS_MajorityAndPersistOnMaster_Majority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 2, 3} /*seqnos*/,
                 cb::durability::Level::MajorityAndPersistOnMaster,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    addSyncWrite({4, 10, 21} /*seqnos*/,
                 cb::durability::Level::Majority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    notifyPersistence(
            3 /*persistedSeqno*/, 21 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       HPS_Majority_MajorityAndPersistOnMaster) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite({1, 7, 1000} /*seqnos*/,
                                        cb::durability::Level::Majority,
                                        0 /*expectHPS*/,
                                        0 /*expectedHCS*/);

    notifySnapEndReceived(
            1002 /*snapEnd*/, 1000 /*expectedHPS*/, 0 /*expectedHCS*/);

    DurabilityMonitorTest::addSyncWrite(
            {1004, 1010, 2021} /*seqnos*/,
            cb::durability::Level::MajorityAndPersistOnMaster,
            1000 /*expectHPS*/,
            0 /*expectedHCS*/);

    notifySnapEndReceived(
            3000 /*snapEnd*/, 2021 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       HPS_Majority_MajorityAndPersistOnMaster) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 7, 1000} /*seqnos*/,
                 cb::durability::Level::Majority,
                 1000 /*expectHPS*/,
                 0 /*expectedHCS*/);

    const std::vector<int64_t> seqnos{1004, 1010, 2021};
    addSyncWrite(seqnos,
                 cb::durability::Level::MajorityAndPersistOnMaster,
                 1000 /*expectHPS*/,
                 0 /*expectedHCS*/);

    for (const auto s : seqnos) {
        notifyPersistence(
                s /*persistedSeqno*/, s /*expectedHPS*/, 0 /*expectedHCS*/);
    }
}

void PassiveDurabilityMonitorTest::testHPS_PersistToMajorityIsDurabilityFence(
        cb::durability::Level testedLevel) {
    ASSERT_TRUE(testedLevel == cb::durability::Level::Majority ||
                testedLevel ==
                        cb::durability::Level::MajorityAndPersistOnMaster);

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    // We receive a snapshot[1, 50] with a mix of PersistToMajority (first) and
    // testedLevel Prepares
    DurabilityMonitorTest::addSyncWrite(
            {1, 2, 3} /*seqnos*/,
            cb::durability::Level::PersistToMajority,
            0 /*expectHPS*/,
            0 /*expectedHCS*/);

    // Note: We are persisting in the middle of a snapshot, which can happen
    // at Replica.
    // Check that persisting s:8 doesn't moves HPS, as we have not received
    // the complete snapshot yet.
    notifyPersistence(8 /*persistedSeqno*/, 0 /*expectHPS*/, 0 /*expectedHCS*/);

    // Receiving other Prepares in the same snapshot (Level!=PersistToMajority)
    DurabilityMonitorTest::addSyncWrite({10, 11, 21} /*seqnos*/,
                                        testedLevel,
                                        0 /*expectHPS*/,
                                        0 /*expectedHCS*/);

    // We receive the snap-end mutation, but the HPS can't move yet has we have
    // some PersistToMajority Prepares that:
    // 1) represents a durability-fence
    // 2) cannot be locally-satisfied yet because we have not persisted the
    //     complete snapshot
    const uint64_t snapEnd = 50;
    notifySnapEndReceived(snapEnd, 0 /*expectedHPS*/, 0 /*expectedHCS*/);

    // The HPS can move to the latest Prepare now:
    // 1) We have persisted (even over) the complete snapshot
    // 2) PersistToMajority represented a durability-fence that has been removed
    //     now, so the HPS can move to covering all the following Majority or
    //     MajorityAndPersistOnMaster Prepares in the snapshot
    notifyPersistence(snapEnd + 10 /*persistedSeqno*/,
                      21 /*expectedHPS*/,
                      0 /*expectedHCS*/);
}

TEST_P(PassiveDurabilityMonitorPersistentTest, HPS_PersistToMajority_Majority) {
    testHPS_PersistToMajorityIsDurabilityFence(cb::durability::Level::Majority);
}

TEST_P(ActiveDurabilityMonitorPersistentTest, HPS_PersistToMajority_Majority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 2, 3} /*seqnos*/,
                 cb::durability::Level::PersistToMajority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    addSyncWrite({4, 10, 21} /*seqnos*/,
                 cb::durability::Level::Majority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    // Check that persisting s:2 moves HPS to 2 and not beyond, as s:3 is
    // Level::PersistToMajority (i.e., a durability-fence)
    notifyPersistence(2 /*persistedSeqno*/, 2 /*expectHPS*/, 0 /*expectedHCS*/);

    // Now, simulate persistence of s:4. HPS reaches the latest tracked as s:3
    // is the last durability-fence.
    notifyPersistence(
            4 /*persistedSeqno*/, 21 /*expectHPS*/, 0 /*expectedHCS*/);
}

void PassiveDurabilityMonitorTest::testHPS_LevelIsNotDurabilityFence(
        cb::durability::Level testedLevel) {
    ASSERT_TRUE(testedLevel == cb::durability::Level::Majority ||
                testedLevel ==
                        cb::durability::Level::MajorityAndPersistOnMaster);

    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    // We receive a snapshot[1, 3000] with a mix of Majority or
    // MajorityAndPersistOnMaster (first) and PersistToMajority Prepares.
    DurabilityMonitorTest::addSyncWrite({1, 999, 1001} /*seqnos*/,
                                        testedLevel,
                                        0 /*expectHPS*/,
                                        0 /*expectedHCS*/);
    DurabilityMonitorTest::addSyncWrite(
            {2000, 2010, 2021} /*seqnos*/,
            cb::durability::Level::PersistToMajority,
            0 /*expectHPS*/,
            0 /*expectedHCS*/);

    // Note: We are persisting in the middle of a snapshot, which can happen
    // at Replica.
    // Check that persisting s:2500 doesn't moves HPS, as at any Level we
    // require that the snap-end mutation is received
    notifyPersistence(
            2500 /*persistedSeqno*/, 0 /*expectHPS*/, 0 /*expectedHCS*/);

    // Other PersistToMajority received in the same snapshot.
    DurabilityMonitorTest::addSyncWrite(
            {2600, 2700} /*seqnos*/,
            cb::durability::Level::PersistToMajority,
            0 /*expectHPS*/,
            0 /*expectedHCS*/);

    // We receive the snap-end mutation. The HPS does move, but only up to
    // before the durability-fence (ie, the first non-locally-satisfied
    // PersistToMajority Prepare), as the we have not persisted the complete
    // snapshot yet.
    notifySnapEndReceived(
            3000 /*snapEnd*/, 1001 /*expectedHPS*/, 0 /*expectedHCS*/);

    // We receive another partial snapshot[3001, 3010] with only Majority or
    // MajorityAndPersistOnMaster Prepares. HPS doesn't move.
    DurabilityMonitorTest::addSyncWrite({3002} /*seqnos*/,
                                        testedLevel,
                                        1001 /*expectHPS*/,
                                        0 /*expectedHCS*/);

    // We persist a seqno beyond the snap-end mutation of the first snapshot,
    // and the HPS moves to the last Prepare in the first snapshot.
    // Note that the HPS doesn't move into the second snapshot as at any Level
    // we require that the complete snapshot is received before the HPS can move
    // into the snapshot.
    notifyPersistence(
            3005 /*persistedSeqno*/, 2700 /*expectHPS*/, 0 /*expectedHCS*/);

    // Second snapshot complete in memory, HPS moves to the latest Prepare
    notifySnapEndReceived(
            3010 /*snapEnd*/, 3002 /*expectedHPS*/, 0 /*expectedHCS*/);
}

TEST_P(PassiveDurabilityMonitorPersistentTest, HPS_Majority_PersistToMajority) {
    testHPS_LevelIsNotDurabilityFence(cb::durability::Level::Majority);
}

TEST_P(ActiveDurabilityMonitorPersistentTest, HPS_Majority_PersistToMajority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 999, 1001} /*seqnos*/,
                 cb::durability::Level::Majority,
                 1001 /*expectHPS*/,
                 0 /*expectedHCS*/);

    addSyncWrite({2000, 2010, 2021} /*seqnos*/,
                 cb::durability::Level::PersistToMajority,
                 1001 /*expectHPS*/,
                 0 /*expectedHCS*/);

    notifyPersistence(
            2010 /*persistedSeqno*/, 2010 /*expectHPS*/, 0 /*expectedHCS*/);

    // Now, simulate persistence of s:4. HPS reaches the latest tracked as s:3
    // is the last durability-fence.
    notifyPersistence(
            2021 /*persistedSeqno*/, 2021 /*expectHPS*/, 0 /*expectedHCS*/);
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       HPS_PersistToMajority_MajorityAndPersistOnMaster) {
    testHPS_PersistToMajorityIsDurabilityFence(
            cb::durability::Level::MajorityAndPersistOnMaster);
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       HPS_PersistToMajority_MajorityAndPersistOnMaster) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 2, 3} /*seqnos*/,
                 cb::durability::Level::PersistToMajority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    addSyncWrite({4, 10, 21} /*seqnos*/,
                 cb::durability::Level::MajorityAndPersistOnMaster,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    // Check that persisting s:2 moves HPS to 2 and not beyond, as s:3 is
    // Level::PersistToMajority (i.e., a durability-fence)
    notifyPersistence(2 /*persistedSeqno*/, 2 /*expectHPS*/, 0 /*expectedHCS*/);

    // Check that persisting s:4 moves HPS to 4 and not beyond, as s:4 is
    // Level::MajorityAndPersistOnMaster (i.e., a durability-fence on Active)
    notifyPersistence(4 /*persistedSeqno*/, 4 /*expectHPS*/, 0 /*expectedHCS*/);
}

TEST_P(PassiveDurabilityMonitorPersistentTest,
       HPS_MajorityAndPersistOnMaster_PersistToMajority) {
    testHPS_LevelIsNotDurabilityFence(
            cb::durability::Level::MajorityAndPersistOnMaster);
}

TEST_P(ActiveDurabilityMonitorPersistentTest,
       HPS_MajorityAndPersistOnMaster_PersistToMajority) {
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);

    addSyncWrite({1, 999, 1001} /*seqnos*/,
                 cb::durability::Level::MajorityAndPersistOnMaster,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    addSyncWrite({2000, 2010, 2021} /*seqnos*/,
                 cb::durability::Level::PersistToMajority,
                 0 /*expectHPS*/,
                 0 /*expectedHCS*/);

    notifyPersistence(
            2021 /*persistedSeqno*/, 2021 /*expectHPS*/, 0 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, NoReplicaSyncWrite) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active}}));
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);
    addSyncWrite(1 /*seqno*/);
    assertNumTrackedAndHPSAndHCS(0, 1, 1 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, NoReplicaSyncDelete) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(nlohmann::json::array({{active}}));
    assertNumTrackedAndHPSAndHCS(0, 0, 0 /*expectedHCS*/);
    addSyncDelete(2 /*seqno*/);
    assertNumTrackedAndHPSAndHCS(0, 2, 2 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       FirstChainNodeAckBeforeAndCommitOnTopologySet) {
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, nullptr}})));

    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // We ack a new "unknown" node
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica2, 1));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new node
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2}}));
    vb->processResolvedSyncWrites();

    // Should have committed
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
}

TEST_P(ActiveDurabilityMonitorTest, FirstChainNodeAckBeforeTopologySet) {
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, nullptr}})));

    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // We ack a new "unknown" node
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica3, 1));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new node
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}})));

    // Should not have committed
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Should commit on replica1 ack.
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, SecondChainNodeAckBeforeTopologySet) {
    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // We ack a new "unknown" node
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica2, 1));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new node
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}})));

    // Still can't commit because firstChain must also be satisfied
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Should commit on replica1 ack.
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest,
       SecondChainNodeAckBeforeTopologySetMultipleReplica) {
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // We ack a new "unknown" node
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica2, 1));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new nodes
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(nlohmann::json::array(
            {{active, replica1}, {active, replica2, replica3, replica4}})));

    // Still can't commit because firstChain must also be satisfied
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Still not satisfied second chain so replica1 ack should not commit
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    // Should commit on replica3 ack.
    testSeqnoAckReceived(replica3,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
}

TEST_P(ActiveDurabilityMonitorTest,
       SecondChainNodeAckBeforeTopologySetAlreadyCommitted) {
    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Should commit on replica1 ack.
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);

    // We ack a new "unknown" node, already committed.
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica2, 1));
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }

    // Add the secondChain with the new node
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}})));

    // Still committed
    EXPECT_EQ(0, getActiveDM().getNumTracked());
}

// Unexpected topology change, just ensuring that we don't crash or do something
// stupid. Ensuring that we don't break something if we attempt to ack twice if
// for some reason the unexpected ack is from a node that exists in both chains
// (we should not even attempt this but it should be a no-op if we do).
TEST_P(ActiveDurabilityMonitorTest, BothChainNodeAckBeforeTopologySet) {
    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // We ack a new "unknown" node
    EXPECT_NO_THROW(getActiveDM().seqnoAckReceived(replica2, 1));
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Replica2 in the firstChain is the unexpected part
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2},
                                   {active, replica1, replica2, replica3}})));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Should commit on replica1 ack
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
}

TEST_P(ActiveDurabilityMonitorTest, MaintainSyncWriteAckCount) {
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}}));
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new node
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3},
                                   {active, replica1, replica2, nullptr}}));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    {
        CB_SCOPED_TRACE("");
        // Should commit on replica2 ack as this should satisfy both chains
        testSeqnoAckReceived(replica2,
                             1 /*ackSeqno*/,
                             1 /*expectedLastWriteSeqno*/,
                             1 /*expectedLastAckSeqno*/,
                             0 /*expectedNumTracked*/,
                             1 /*expectedHPS*/,
                             1 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
}

TEST_P(ActiveDurabilityMonitorTest, MaintainSyncWriteAckCount_SecondChain) {
    // Add the secondChain with the new node
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3},
                                   {active, replica1, replica2, replica4}}));
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    testSeqnoAckReceived(replica4,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         1 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Add the secondChain with the new node
    getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3},
                                   {active, replica1, replica2, nullptr}}));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    {
        // Replica1 ack should not commit (neither chain satisfied)
        CB_SCOPED_TRACE("");
        testSeqnoAckReceived(replica1,
                             1 /*ackSeqno*/,
                             1 /*expectedLastWriteSeqno*/,
                             1 /*expectedLastAckSeqno*/,
                             1 /*expectedNumTracked*/,
                             1 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        // Should commit on replica2 ack as this should satisfy both chains
        testSeqnoAckReceived(replica2,
                             1 /*ackSeqno*/,
                             1 /*expectedLastWriteSeqno*/,
                             1 /*expectedLastAckSeqno*/,
                             0 /*expectedNumTracked*/,
                             1 /*expectedHPS*/,
                             1 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
}

TEST_P(ActiveDurabilityMonitorTest, HPSResetOnTopologyChange) {
    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Should commit on replica1 ack.
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         1 /*expectedHPS*/,
                         1 /*expectedHCS*/);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }

    // Add the secondChain with the new node
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, replica1}, {active, replica2}})));

    // HPS should still be 1.
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
}

/**
 * Failover scenario:
 *
 * We have 1 replica that we failover. Topology is changed from
 * {{active, replica1}} to {{active, undefined}}. In this case we should abort
 * any in-flight SyncWrites with a non-infinite and not throw assertions.
 */
TEST_P(ActiveDurabilityMonitorTest,
       DurabilityImpossibleTopologyChangeAbortsInFlightSyncWrites) {
    // To start, we have 1 chain with active and replica1
    addSyncWrite(1);
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Failover
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, nullptr}})));
    vb->processResolvedSyncWrites();

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 1, 1);
    }
    EXPECT_EQ(0, getActiveDM().getNumCommitted());
    EXPECT_EQ(1, getActiveDM().getNumAborted());
}

/**
 * Failover scenario:
 *
 * We have 1 replica that we failover. Topology is changed from
 * {{active, replica1}} to {{active, undefined}}. In this case we should not
 * abort any in-flight SyncWrites with an infinite timeout as this breaks
 * durability. We create SyncWrites with an infinite timeout at warmup and at
 * promotion from replica to active as we MUST commit these SyncWrites.
 */
TEST_P(ActiveDurabilityMonitorTest,
       DurabilityImpossibleTopologyChangeDoesNotAbortsInfiniteTimeoutSyncWrites) {
    // To start, we have 1 chain with active and replica1
    using namespace cb::durability;
    addSyncWrite(1, Requirements{Level::Majority, Timeout::Infinity()});
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }

    // Failover
    EXPECT_NO_THROW(getActiveDM().setReplicationTopology(
            nlohmann::json::array({{active, nullptr}})));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(1, 1, 0);
    }
    EXPECT_EQ(0, getActiveDM().getNumCommitted());
    EXPECT_EQ(0, getActiveDM().getNumAborted());
}

// MB-35190: SyncWrites with enough acks for a majority but WITHOUT an ack from
// the active should not be treated as satisfied
TEST_P(ActiveDurabilityMonitorPersistentTest,
       SyncWriteNotSatisfiedWithoutMaster) {
    auto& adm = getActiveDM();
    adm.setReplicationTopology(
            nlohmann::json::array({{active, replica1, replica2, replica3}}));
    using namespace cb::durability;

    // Add a PersistToMajority to "block" the active to prevent immediate acking
    addSyncWrite(1,
                 Requirements(Level::PersistToMajority,
                              cb::durability::Timeout(30000)));

    // This SW should not be satisfied by just replica acks
    addSyncWrite(2,
                 Requirements(Level::Majority, cb::durability::Timeout(30000)));
    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(2, 0, 0);
    }

    // Enough acks to reach majority, but the active has not persisted yet.
    for (const auto& replica : {replica1, replica2, replica3}) {
        testSeqnoAckReceived(replica,
                             2 /*ackSeqno*/,
                             2 /*expectedLastWriteSeqno*/,
                             2 /*expectedLastAckSeqno*/,
                             2 /*expectedNumTracked*/,
                             0 /*expectedHPS*/,
                             0 /*expectedHCS*/);
    }

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(2, 0, 0);
    }

    // Rather than delving into getting the actual SyncWrite and checking if
    // satisfied, recreate scenario from MB-35190. If a SyncWrite is moved to
    // the resolvedQueue because its timeout has passed it should be aborted by
    // processCompletedSyncWriteQueue. processCompletedSyncWriteQueue dispatches
    // based on sw->isSatisfied(). If SWs could be satisfied WITHOUT the active
    // acking, the active could timeout a given syncWrite, and then proceed to
    // commit it.
    simulateTimeoutCheck(
            adm, std::chrono::steady_clock::now() + std::chrono::seconds(31));

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 0, 2);
    }

    EXPECT_EQ(adm.getNumAborted(), 2);

    // Active finally persists, but it is too late
    vb->setPersistenceSeqno(2);
    monitor->notifyLocalPersistence();

    {
        CB_SCOPED_TRACE("");
        assertNumTrackedAndHPSAndHCS(0, 0, 2);
    }
}

TEST_P(PassiveDurabilityMonitorTest, SendSeqnoAckRace) {
    auto& pdm = getPassiveDM();
    ASSERT_EQ(0, pdm.getNumTracked());

    // Throws if not monotonic.
    Monotonic<int64_t, ThrowExceptionPolicy> ackedSeqno(0);

    // Set up our function hooks
    pdm.notifySnapEndSeqnoAckPreProcessHook = [this, &pdm]() {
        // We are outside the state lock at this point but have not yet called
        // vb->sendSeqnoAck. Simulate this race by notifying persistence which
        // should attempt to ack 2.
        vb->setPersistenceSeqno(2);
        pdm.notifyLocalPersistence();
    };

    // Test: Assert the monotonicity of the ack that we attempt to send
    VBucketTestIntrospector::setSeqnoAckCb(
            *vb, [&ackedSeqno](Vbid vbid, uint64_t hps) {
                ackedSeqno = static_cast<int64_t>(hps);
            });

    // Load two SyncWrites. For this test we want a snapshot end to attempt to
    // seqno ack with seqno 1 but be "descheduled" before it can and for a
    // persistence callback to ack with seqno 2 before the seqno ack for seqno 1
    // completes. This could happen with the following two prepares in a single
    // snapshot.
    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    addSyncWrite(2 /*seqno*/,
                 Requirements{Level::PersistToMajority, Timeout::Infinity()});
    ASSERT_EQ(2, pdm.getNumTracked());

    // Should only be able to ack seqno 1 as we require persistence to ack seqno
    // 2.
    pdm.notifySnapshotEndReceived(2);
    EXPECT_EQ(2, ackedSeqno);
}

TEST_P(PassiveDurabilityMonitorTest, dropKey) {
    auto& pdm = getPassiveDM();

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
            Requirements{Level::Majority, Timeout::Infinity()});
    ASSERT_EQ(1, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.eraseSyncWrite(makeStoredDocKey("key1"), 1);

    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());
}

void PassiveDurabilityMonitorTest::testDropFirstKey() {
    auto& pdm = getPassiveDM();

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    addSyncWrite(2 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.eraseSyncWrite(makeStoredDocKey("key1"), 1);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropFirstKey) {
    testDropFirstKey();
}

void PassiveDurabilityMonitorTest::testDropLastKey() {
    auto& pdm = getPassiveDM();

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    addSyncWrite(2 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.eraseSyncWrite(makeStoredDocKey("key2"), 2);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropLastKey) {
    testDropLastKey();
}

TEST_P(PassiveDurabilityMonitorTest, dropFirstKeyAndReceiveSnapEnd) {
    testDropFirstKey();

    auto& pdm = getPassiveDM();

    pdm.notifySnapshotEndReceived(3);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropLastKeyAndReceiveSnapEnd) {
    testDropLastKey();

    auto& pdm = getPassiveDM();

    pdm.notifySnapshotEndReceived(3);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(1, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropFirstKeyAndCompleteNext) {
    testDropFirstKey();

    auto& pdm = getPassiveDM();

    pdm.notifySnapshotEndReceived(3);
    pdm.completeSyncWrite(makeStoredDocKey("key2"), PassiveDurabilityMonitor::Resolution::Commit, 2);

    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropLastKeyAndCompletePrevious) {
    testDropLastKey();

    auto& pdm = getPassiveDM();

    pdm.notifySnapshotEndReceived(3);
    pdm.completeSyncWrite(makeStoredDocKey("key1"), PassiveDurabilityMonitor::Resolution::Commit, 1);

    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(1, pdm.getHighCompletedSeqno());
    EXPECT_EQ(1, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropFirstKeyAndCompleteNextDiskSnap) {
    vb->checkpointManager->createSnapshot(1, 2, 0, CheckpointType::Disk, 0);

    auto& pdm = getPassiveDM();
    using namespace cb::durability;
    uint64_t currentSeqno{1};

    auto item = makePendingItem(
            makeStoredDocKey("key" + std::to_string(currentSeqno)),
            "value",
            Requirements{Level::Majority, Timeout::Infinity()});
    item->setBySeqno(currentSeqno);
    processSet(*item);

    currentSeqno++;
    item = makePendingItem(
            makeStoredDocKey("key" + std::to_string(currentSeqno)),
            "value",
            Requirements{Level::Majority, Timeout::Infinity()});
    item->setBySeqno(currentSeqno);
    processSet(*item);

    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    vb->checkpointManager->createSnapshot(3, 4, 2, CheckpointType::Memory, 4);
    pdm.eraseSyncWrite(makeStoredDocKey("key1"), 1);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.notifySnapshotEndReceived(4);
    pdm.completeSyncWrite(makeStoredDocKey("key2"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          2);

    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(2, pdm.getHighCompletedSeqno());
    EXPECT_EQ(2, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropLastKeyAndCompletePreviousDiskSnap) {
    vb->checkpointManager->createSnapshot(1, 2, 0, CheckpointType::Disk, 0);

    auto& pdm = getPassiveDM();
    using namespace cb::durability;
    uint64_t currentSeqno{1};

    auto item = makePendingItem(
            makeStoredDocKey("key" + std::to_string(currentSeqno)),
            "value",
            Requirements{Level::Majority, Timeout::Infinity()});
    item->setBySeqno(currentSeqno);
    processSet(*item);

    currentSeqno++;
    item = makePendingItem(
            makeStoredDocKey("key" + std::to_string(currentSeqno)),
            "value",
            Requirements{Level::Majority, Timeout::Infinity()});
    item->setBySeqno(currentSeqno);
    processSet(*item);

    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    vb->checkpointManager->createSnapshot(3, 4, 2, CheckpointType::Memory, 3);
    pdm.eraseSyncWrite(makeStoredDocKey("key2"), 2);

    EXPECT_EQ(1, pdm.getNumTracked());
    EXPECT_EQ(0, pdm.getHighCompletedSeqno());
    EXPECT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.notifySnapshotEndReceived(4);
    pdm.completeSyncWrite(makeStoredDocKey("key1"), PassiveDurabilityMonitor::Resolution::Commit, 1);

    EXPECT_EQ(0, pdm.getNumTracked());
    EXPECT_EQ(1, pdm.getHighCompletedSeqno());
    EXPECT_EQ(1, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, dropFirstKeyAndCompleteOutOfOrder) {
    auto& pdm = getPassiveDM();

    // Replica needs to be in a Disk snapshot for allowing OoO
    vb->checkpointManager->createSnapshot(0, 4, 0, CheckpointType::Disk, 4);

    using namespace cb::durability;
    auto req = Requirements{Level::Majority, Timeout::Infinity()};
    for (const uint64_t seqno : {1, 2, 3}) {
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
        processSet(item);
    }

    ASSERT_EQ(3, vb->getHighSeqno());
    ASSERT_EQ(3, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());

    pdm.eraseSyncWrite(makeStoredDocKey("key3"), 3);

    EXPECT_TRUE(vb->isReceivingDiskSnapshot());
    pdm.completeSyncWrite(makeStoredDocKey("key2"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          2);

    ASSERT_EQ(2, pdm.getNumTracked());
    ASSERT_EQ(2, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());
}

TEST_P(PassiveDurabilityMonitorTest, DropMultipleCollections) {
    auto& pdm = getPassiveDM();

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});

    pdm.notifyDroppedCollection(CollectionID(8), 10);
    pdm.notifyDroppedCollection(CollectionID(9), 11);
    EXPECT_EQ(2, pdm.getNumDroppedCollections());

    addSyncWrite(12 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    EXPECT_EQ(2, pdm.getNumDroppedCollections());

    pdm.completeSyncWrite(makeStoredDocKey("key1"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          1);
    EXPECT_EQ(2, pdm.getNumDroppedCollections());

    pdm.notifySnapshotEndReceived(20);
    EXPECT_EQ(0, pdm.getNumDroppedCollections());
}

// This test isn't a valid scenario, we should not try to complete the same
// prepare twice, but we previously crashed here instead of throwing and tearing
// down the connection if this happened.
TEST_P(PassiveDurabilityMonitorTest, ThrowIfEndOnOrderedComplete) {
    auto& pdm = getPassiveDM();

    using namespace cb::durability;
    addSyncWrite(1 /*seqno*/,
                 Requirements{Level::Majority, Timeout::Infinity()});
    ASSERT_EQ(1, pdm.getNumTracked());
    ASSERT_EQ(0, pdm.getHighCompletedSeqno());
    ASSERT_EQ(0, pdm.getHighPreparedSeqno());
    pdm.completeSyncWrite(makeStoredDocKey("key1"),
                          PassiveDurabilityMonitor::Resolution::Commit,
                          1);

    pdm.notifyDroppedCollection(CollectionID(8), 10);
    EXPECT_THROW(
            pdm.completeSyncWrite(makeStoredDocKey("invalid"),
                                  PassiveDurabilityMonitor::Resolution::Commit,
                                  100),
            std::logic_error);
}

class ActiveDurabilityMonitorAbortTest
    : public ActiveDurabilityMonitorPersistentTest {
public:
    void SetUp() override {
        // Run with max of 2 replicas so that we will abort on topology change
        ActiveDurabilityMonitorTest::setup(2, true);
    }
};

// Perform topology change similar to that seen in MB-35661.
// Go from 1 chain (1 replica) to a second chain (where the active/replica swap)
// The second chain is also crucially larger than the max-replicas so that
// sync-writes get aborted.
TEST_P(ActiveDurabilityMonitorAbortTest, MB_35661) {
    DurabilityMonitorTest::addSyncWrites(
            {1, 2}, {cb::durability::Level::MajorityAndPersistOnMaster, {}});
    testSeqnoAckReceived(replica1,
                         1 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         1 /*expectedLastAckSeqno*/,
                         2 /*expectedNumTracked*/,
                         0 /*expectedHPS*/,
                         0 /*expectedHCS*/);

    getActiveDM().setReplicationTopology(nlohmann::json::array(
            {{active, replica1}, {replica1, active, replica2, replica3}}));

    testSeqnoAckReceived(replica1,
                         2 /*ackSeqno*/,
                         1 /*expectedLastWriteSeqno*/,
                         2 /*expectedLastAckSeqno*/,
                         0 /*expectedNumTracked*/,
                         0 /*expectedHPS*/,
                         2 /*expectedHCS*/);
}

void NoTopologyActiveDurabilityMonitorTest::SetUp() {
    STParameterizedBucketTest::SetUp();
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    vb = store->getVBuckets().getBucket(vbid).get();
    ASSERT_TRUE(vb);

    monitor = &VBucketTestIntrospector::public_getActiveDM(*vb);
    ASSERT_TRUE(monitor);

    // The ActiveDM must not track anything before the Replication Chain is set.
    auto& adm = getActiveDM();
    ASSERT_EQ(0, adm.getFirstChainSize());
    ASSERT_EQ(0, adm.getFirstChainMajority());
}

TEST_P(NoTopologyActiveDurabilityMonitorTest, SeqnoAckReceivedBeforeTopology) {
    {
        std::shared_lock vbStateLh(vb->getStateLock());
        // Prior to MB-37188 this would fail an Expects as the first chain
        // has not yet been set.
        // This should now queue the ack until the topology is set
        vb->seqnoAcknowledged(vbStateLh, replica1, 1 /*prepare seqno */);
    }

    auto& adm = getActiveDM();
    ASSERT_EQ(0, adm.getFirstChainSize());
    ASSERT_EQ(0, adm.getFirstChainMajority());

    // node not in chain, can't check lastAckSeqno

    adm.setReplicationTopology(nlohmann::json::array({{active, replica1}}));

    ASSERT_EQ(2, adm.getFirstChainSize());
    ASSERT_EQ(2, adm.getFirstChainMajority());

    // expect that the ack was performed now the topology is set
    EXPECT_EQ(1, adm.getNodeAckSeqno(replica1));
}

TEST_P(ActiveDurabilityMonitorTest, MB_41235_commit) {
    PassiveDurabilityMonitor pdm(*engine->getVBucket(vbid));
    CollectionsManifest cm{CollectionEntry::meat};
    setCollections(cookie, cm);

    queued_item item1{new Item(makeStoredDocKey("key1", CollectionEntry::meat),
                               0 /*flags*/,
                               0 /*exp*/,
                               "value",
                               5 /*valueSize*/,
                               PROTOCOL_BINARY_RAW_BYTES,
                               0 /*cas*/,
                               1)};
    using namespace cb::durability;
    item1->setPendingSyncWrite(
            Requirements{Level::Majority, Timeout::Infinity()});

    pdm.addSyncWrite(item1, {});

    cm.remove(CollectionEntry::meat);
    setCollections(cookie, cm);

    EPStats stats;
    ActiveDurabilityMonitor adm(
            stats, *vb, std::move(pdm), NoopSyncWriteTimeoutFactory(*vb));
    vb->setState(vbucket_state_active);
    ASSERT_EQ(vbucket_state_active, vb->getState());
    adm.setReplicationTopology(nlohmann::json::array({{"active", "replica1"}}));
    ASSERT_EQ(cb::engine_errc::success, adm.seqnoAckReceived("replica1", 1));

    adm.checkForCommit();
    EXPECT_NO_THROW(adm.processCompletedSyncWriteQueue(
            std::shared_lock<folly::SharedMutex>(vb->getStateLock())));
}

TEST(DurabilityMonitorTrackedWritesTest, emplace_and_erase) {
    MemoryTrackingAllocator<DurabilityMonitor::SyncWrite> trackingAllocator;
    DurabilityMonitorTrackedWrites<DurabilityMonitor::SyncWrite> container{
            trackingAllocator};
#ifdef WIN32
    // msvc implementation results in a non zero mem-used
    EXPECT_EQ(40, container.getTotalMemoryUsed());
    EXPECT_EQ(40, trackingAllocator.getBytesAllocated());
#else
    EXPECT_EQ(0, container.getTotalMemoryUsed());
#endif

    queued_item item1{new Item(makeStoredDocKey("key1", CollectionEntry::fruit),
                               0 /*flags*/,
                               0 /*exp*/,
                               "value",
                               5 /*valueSize*/,
                               PROTOCOL_BINARY_RAW_BYTES,
                               0 /*cas*/,
                               1)};

    container.emplace_back(item1);
    EXPECT_EQ(1, container.size());
    EXPECT_FALSE(container.empty());

    // container now includes node allocation etc... from the
    // MemoryTrackingAllocator, this test now avoids absolute comparisons of
    // memory usage
    // Expect the container to store more then 1 SyncWrite+the item size - there
    // should be 1 list node allocated
    EXPECT_GT(container.getTotalMemoryUsed(),
              sizeof(DurabilityMonitor::SyncWrite) + item1->size());
    container.erase(container.begin());
    EXPECT_EQ(0, container.size());
    EXPECT_TRUE(container.empty());

#ifdef WIN32
    // msvc implementation results in a non zero mem-used
    EXPECT_EQ(40, container.getTotalMemoryUsed());
    EXPECT_EQ(40, trackingAllocator.getBytesAllocated());
#else
    EXPECT_EQ(0, container.getTotalMemoryUsed());
#endif
}

TEST(DurabilityMonitorTrackedWritesTest, splice1) {
    MemoryTrackingAllocator<DurabilityMonitor::SyncWrite> trackingAllocator;
    DurabilityMonitorTrackedWrites<DurabilityMonitor::SyncWrite> container{
            trackingAllocator};
#ifdef WIN32
    // msvc implementation results in a non zero mem-used
    EXPECT_EQ(40, container.getTotalMemoryUsed());
    EXPECT_EQ(40, trackingAllocator.getBytesAllocated());
#else
    EXPECT_EQ(0, container.getTotalMemoryUsed());
#endif

    for (auto i : {1, 2, 3, 4}) {
        container.emplace_back(
                makeCommittedItem(makeStoredDocKey("key" + std::to_string(i)),
                                  std::string(i, 'v')));
    }

    {
        // Create a second container in this new scope so we can see it destruct
        // This is similar to some of the splice behaviour inside the ADM
        DurabilityMonitorTrackedWrites<DurabilityMonitor::SyncWrite> container2{
                container.get_allocator()};

        // This splice variant "moves" all from container to container2
        auto memory = container.getTotalMemoryUsed();
        container2.splice(container2.end(), container);
        EXPECT_EQ(memory, container2.getTotalMemoryUsed());
        EXPECT_EQ(4, container2.size());

        // container2 has the items, but the underlying node allocations are
        // still tracked by both containers
        EXPECT_GT(container2.getTotalMemoryUsed(),
                  container.getTotalMemoryUsed());

        EXPECT_LT(container.getMemorySize(),
                  container.get_allocator().getBytesAllocated());

        // container still retains memory, but held in the allocator
#ifdef WIN32
        size_t expected = 240;
#else
        size_t expected = 160;
#endif
        EXPECT_EQ(expected, container.getTotalMemoryUsed());
        EXPECT_EQ(expected, trackingAllocator.getBytesAllocated());
    }

#ifdef WIN32
    // msvc implementation results in a non zero mem-used
    EXPECT_EQ(40, container.getTotalMemoryUsed());
    EXPECT_EQ(40, trackingAllocator.getBytesAllocated());
#else
    // container2 now gone - the memory usage has gone down to 0
    EXPECT_EQ(0, container.getTotalMemoryUsed());
    EXPECT_EQ(0, container.get_allocator().getBytesAllocated());
#endif

    EXPECT_EQ(0, container.size());
    EXPECT_TRUE(container.empty());
}

TEST(DurabilityMonitorTrackedWritesTest, splice2) {
    MemoryTrackingAllocator<DurabilityMonitor::SyncWrite> trackingAllocator;
    DurabilityMonitorTrackedWrites<DurabilityMonitor::SyncWrite> container{
            trackingAllocator};
#ifdef WIN32
    // msvc implementation results in a non zero mem-used
    EXPECT_EQ(40, container.getTotalMemoryUsed());
    EXPECT_EQ(40, trackingAllocator.getBytesAllocated());
#else
    EXPECT_EQ(0, container.getTotalMemoryUsed());
#endif

    for (auto i : {1, 2, 3, 4}) {
        container.emplace_back(
                makeCommittedItem(makeStoredDocKey("key" + std::to_string(i)),
                                  std::string(i, 'v')));
    }
    auto itr = container.begin();
    std::advance(itr, 2);
    auto memoryToMove = itr->getSize();

    DurabilityMonitorTrackedWrites<DurabilityMonitor::SyncWrite> container2{
            container.get_allocator()};

    // This splice variant moves the element referenced by itr from container to
    // container2.
    auto memory = container.getTotalMemoryUsed();
    container2.splice(container2.end(), container, itr);
    // container2 should now have the SyncWrite + Item + a bit more
    EXPECT_GT(container2.getTotalMemoryUsed(), memoryToMove);
    EXPECT_EQ(1, container2.size());
    EXPECT_EQ(memory - memoryToMove, container.getTotalMemoryUsed());
    EXPECT_EQ(3, container.size());
}

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         ActiveDurabilityMonitorTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         ActiveDurabilityMonitorNullTopologyTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         ActiveDurabilityMonitorWithReplicaLimitTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllBucketTypes,
        ActiveDurabilityMonitorAbortTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllBucketTypes,
        ActiveDurabilityMonitorPersistentTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         NoTopologyActiveDurabilityMonitorTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(AllBucketTypes,
                         PassiveDurabilityMonitorTest,
                         STParameterizedBucketTest::allConfigValues(),
                         STParameterizedBucketTest::PrintToStringParamName);

INSTANTIATE_TEST_SUITE_P(
        AllBucketTypes,
        PassiveDurabilityMonitorPersistentTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);
