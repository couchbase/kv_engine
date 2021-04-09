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
#pragma once

#include "durability/durability_monitor.h"
#include "durability/passive_durability_monitor.h"
#include "evp_store_single_threaded_test.h"
#include "kv_bucket.h"
#include "test_helpers.h"

#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

class ActiveDurabilityMonitor;

class DurabilityMonitorTest : public STParameterizedBucketTest {
protected:
    /**
     * Add a SyncWrite for tracking. Adds a SyncWrite at a specific seqno by
     * adding non-SyncWrite items up until the given seqno. Uses VBucket::set.
     *
     * @param seqno the seqno to put a SyncWrite at
     * @param req The Durability Requirements
     */
    virtual void addSyncWrite(int64_t seqno,
                              cb::durability::Requirements req = {});

    /**
     * Add a SyncDelete for tracking. Adds a SyncDelete at a specific seqno by
     * adding non-SyncWrite/Delete items up until the given seqno. Uses
     * VBuclet::set.
     *
     * @param seqno the seqno to put a SyncWrite at
     * @param req The Durability Requirements
     */
    void addSyncDelete(int64_t seqno, cb::durability::Requirements req = {});

    /**
     * Adds the given mutations for tracking.
     *
     * @param seqnos the mutations to be added
     * @param req The Durability Requirements
     */
    void addSyncWrites(const std::vector<int64_t>& seqnos,
                       cb::durability::Requirements req = {});

    /**
     * Stores the given item via VBucket::processSet.
     * Useful for setting an exact provided bySeqno.
     *
     * @param item the item to be stored
     */
    MutationStatus processSet(Item& item);

    /**
     * Stores the given item via VBucket::set.
     *
     * @param item the item to be stored
     */
    cb::engine_errc set(Item& item);

    /**
     * Add the given SyncWrites for tracking and check that High Prepared Seqno
     *  and High Completed Seqno have been updated as expected.
     *
     * @param seqnos The mutations to be queued
     * @param level The Durability Level of the queued mutations
     * @param expectedHPS
     * @param expectedHCS
     */
    void addSyncWrite(const std::vector<int64_t>& seqnos,
                      const cb::durability::Level level,
                      const int64_t expectedHPS,
                      const int64_t expectedHCS);

    /**
     * Check the number of tracked Prepares, the HPS and the HCS in
     * DurabilityMonitor.
     *
     * @param expectedNumTracked
     * @param expectedHPS
     * @param expectedHCS
     */
    void assertNumTrackedAndHPSAndHCS(const size_t expectedNumTracked,
                                      const int64_t expectedHPS,
                                      const int64_t expectedHCS) const;

    /**
     * Notify the persistedSeqno to the DM and check that High Prepared Seqno
     * and High Completed Seqno have been updated as expected.
     *
     * @param persistedSeqno
     * @param expectedHPS
     * @param expectedHCS
     */
    void notifyPersistence(const int64_t persistedSeqno,
                           const int64_t expectedHPS,
                           const int64_t expectedHCS);

    void replaceSeqnoAckCB(std::function<void(Vbid vbid, int64_t seqno)>);

    // Owned by KVBucket
    VBucket* vb;

    // Owned by VBucket
    DurabilityMonitor* monitor;
};

/*
 * ActiveDurabilityMonitor test fixture
 */
class ActiveDurabilityMonitorTest : public DurabilityMonitorTest {
public:
    void SetUp() override;
    void setup(int maxAllowedReplicas);
    void TearDown() override;

protected:
    ActiveDurabilityMonitor& getActiveDM() const;

    /**
     * Adds a number of SyncWrites with seqno in [start, end].
     *
     * @param seqnoStart
     * @param seqnoEnd
     * @param req The Durability Requirements
     * @return the number of added SyncWrites
     */
    size_t addSyncWrites(int64_t seqnoStart,
                         int64_t seqnoEnd,
                         cb::durability::Requirements req = {});

    /**
     * Check the tracking for the given node
     *
     * @param node
     * @param lastWriteSeqno The highest SyncWrite seqno pointed by tracking
     * @param lastAckSeqno The last seqno acked by node
     */
    void assertNodeTracking(const std::string& node,
                            uint64_t lastWriteSeqno,
                            uint64_t lastAckSeqno) const;

    /**
     * Check the current High Prepared Seqno and High Completed Seqno on Active.
     *
     * @param expectedHPS
     * @param expectedHCS
     */
    void assertHPSAndHCS(const int64_t expectedHPS,
                         const int64_t expectedHCS) const;

    /**
     * Processes the (local) ack-seqno and checks expected stats.
     *
     * @param ackSeqno The seqno-ack to be processed
     * @param expectedNumTracked
     * @param expectedLastWriteSeqno
     * @param expectedLastAckSeqno
     */
    void testLocalAck(int64_t ackSeqno,
                      size_t expectedNumTracked,
                      int64_t expectedLastWriteSeqno,
                      int64_t expectedLastAckSeqno);

    /**
     * Processes the ack-seqno for replica and checks expected stats.
     *
     * @param replica The replica that "receives" the ack
     * @param ackSeqno The seqno-ack to be processed
     * @param expectedLastWriteSeqno The seqno tracked for the given replica
     * @param expectedLastAckSeqno The ack tracked for the given replica
     * @param expectedNumTracked
     * @param expectedHPS
     * @param expectedHCS
     */
    void testSeqnoAckReceived(const std::string& replica,
                              int64_t ackSeqno,
                              int64_t expectedLastWriteSeqno,
                              int64_t expectedLastAckSeqno,
                              size_t expectedNumTracked,
                              int64_t expectedHPS,
                              int64_t expectedHCS) const;

    /**
     * Check durability possible/impossible at DM::addSyncWrite under the
     * test conditions defined by input args.
     *
     * @param topology The replication topology
     * @param item The item being added to tracking
     * @param expectedFirstChainSize The expected number of defined nodes in
     *     first-chain
     * @param expectedFirstChainMajority The expected Majority for first-chain.
     *     Note that the computation of Majority accounts for both defined and
     *     undefined nodes in chain.
     * @param expectedSecondChainSize The expected number of defined nodes in
     *     second-chain
     * @param expectedFirstChainMajority The expected Majority for second-chain.
     *     Note that the computation of Majority accounts for both defined and
     *     undefined nodes in chain.
     * @return true if durability is possible, false otherwise
     */
    bool testDurabilityPossible(const nlohmann::json::array_t& topology,
                                queued_item& item,
                                uint8_t expectedFirstChainSize,
                                uint8_t expectedFirstChainMajority,
                                uint8_t expectedSecondChainSize = 0,
                                uint8_t expectedSecondChainMajority = 0);

    /**
     * Check that the expected chain empty exception is thrown for the given
     * topology
     *
     * @param topology The replication topology to apply
     * @param chainName Chain name prefix printed in the exception
     */
    void testChainEmpty(const nlohmann::json::array_t& topology,
                        DurabilityMonitor::ReplicationChainName chainName);

    /**
     * Check that the expected undefined active exception is thrown for the
     * given topology
     *
     * @param topology The replication topology to apply
     * @param chainName Chain name prefix printed in the exception
     */
    void testChainUndefinedActive(
            const nlohmann::json::array_t& topology,
            DurabilityMonitor::ReplicationChainName chainName);

    /**
     * Check that the expected too many nodes exception is thrown for the given
     * topology
     *
     * @param topology The replication topology to apply
     * @param chainName Chain name prefix printed in the exception
     */
    void testChainTooManyNodes(
            const nlohmann::json::array_t& topology,
            DurabilityMonitor::ReplicationChainName chainName);

    /**
     * Check that the expected duplicate node exception is thrown for the given
     * topology
     *
     * @param topology The replication topology to apply
     */
    void testChainDuplicateNode(const nlohmann::json::array_t& topology);

    /**
     * Check that the expected Monotonic exception is thrown when attempting to
     * ack a lower seqno than has been previously acked on node "replica1"
     */
    void testSeqnoAckSmallerThanLastAck();

    /**
     * Check that seqnoAckReceived does not throw and that a SyncWrite is not
     * committed when an unknown node acks. This is a valid case during
     * rebalance. Ns_server only sets the secondChain when a new replica has
     * caught up to the active so that SyncWrites are not blocked by rebalance
     * progress.
     *
     * @param nodeToAck the unknown node we should ack
     * @param unchangedNodes list of nodes that should remain unchanged
     */
    void testSeqnoAckUnknownNode(
            const std::string& nodeToAck,
            const std::vector<std::string>& unchangedNodes);

    /**
     * Check that repeated acks from a replica are tolerated
     *
     * Add a single syncWrite then ack twice from the same replica.
     */
    void testRepeatedSeqnoAck(int64_t firstAck, int64_t secondAck);

    /**
     * Set the seqnoAckReceivedPostProcessHook function within the ActiveDM to
     * the given function.
     */
    void setSeqnoAckReceivedPostProcessHook(std::function<void()> func);

    /**
     * Simulate a timeout event happening at the given time point.
     */
    void simulateTimeoutCheck(ActiveDurabilityMonitor& adm,
                              std::chrono::steady_clock::time_point now) const;

    /**
     * Test that we can drop the first key in trackedWrites
     */
    void testDropFirstKey();

    /**
     * Test that we can drop the last key in trackedWrites
     */
    void testDropLastKey();

    const std::string active = "active";
    const std::string replica1 = "replica1";
    const std::string replica2 = "replica2";
    const std::string replica3 = "replica3";
    const std::string replica4 = "replica4";
    const std::string replica5 = "replica5";
    const std::string replica6 = "replica6";
};

/*
 * ActiveDurabilityMonitor test fixture for tests that should only run against
 * persistent buckets
 */
class ActiveDurabilityMonitorPersistentTest
    : public ActiveDurabilityMonitorTest {
protected:
    /**
     * Check that the PersistToMajority SyncWrites at senqo 1, 3, and 5 are
     * committed only when the active acks persistence after acking all the
     * given nodes.
     *
     * @param nodesToAck list of nodes to ack
     */
    void testSeqnoAckPersistToMajority(
            const std::vector<std::string>& nodesToAck);

    /**
     * Check that the MajorityAndPersistOnMaster SyncWrites at seqno 1, 3, and 5
     * are committed only when the active acks persistence after acking all the
     * given nodes.
     *
     * @param nodesToAck list of nodes to ack
     */
    void testSeqnoAckMajorityAndPersistOnMaster(
            const std::vector<std::string>& nodesToAck);
};

/*
 * PassiveDurabilityMonitor test fixture
 */
class PassiveDurabilityMonitorTest : public DurabilityMonitorTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    PassiveDurabilityMonitor& getPassiveDM() const;

    /**
     * Add a SyncWrite for tracking. Adds a SyncWrite at a specific seqno by
     * using VBucket::processSet. Overrides the default implementation because
     * a set call will fail for replica/pending vBuckets (we check the activeDM
     * for durability requirements/checkForCommit).
     *
     * @param seqno the seqno to put a SyncWrite at
     * @param req The Durability Requirements
     * @return the error code from the underlying engine
     */
    void addSyncWrite(int64_t seqno,
                      cb::durability::Requirements req = {}) override;

    /**
     * Simulate and verify that:
     * 1) PDM queues some Prepares
     * 2) PDM is notified of resolution for the tracked Prepares
     * 3) The Prepares are removed from tracking
     *
     * @param res The type of resolution, Commit/Abort
     */
    void testResolvePrepare(PassiveDurabilityMonitor::Resolution res);

    /**
     * Simulate and verify that:
     * 1) The PDM can resolve Prepares in a different order than they
     *    were received IF they are received from a disk snapshot
     *
     * @param res The type of resolution, Commit/Abort
     */
    void testResolvePrepareOutOfOrder(PassiveDurabilityMonitor::Resolution res);

    /**
     * Notify the DM that the snapshot-end mutation has been received on the
     * PassiveStream and check that High Prepared Seqno  and the High Completed
     * Seqno have been updated as expected.
     *
     * @param snapEnd
     * @param expectedHPS
     * @param expectedHCS
     */
    void notifySnapEndReceived(int64_t snapEnd,
                               int64_t expectedHPS,
                               int64_t expectedHCS);

    /**
     * Checks that the HPS is updated correctly when (1) Level:PersistToMajority
     * writes are queued into the DM and then (2) Level Majority or
     * MajorityAndPersistOnMaster writes are queued.
     * Verifies that Level:PersistToMajority writes enforce a durability-fence.
     *
     * @param testedLevel The level tested, Majority/MajorityAndPersistOnMaster
     */
    void testHPS_PersistToMajorityIsDurabilityFence(
            cb::durability::Level testedLevel);

    /**
     * Checks that the HPS is updated correctly when (1) Level:Majority writes
     * are queued into the DM and then (2) Level:PersistToMajority writes are
     * queued. Verifies that Level Majority and MajorityAndPersistOnMaster
     * writes do *not* enforce a durability-fence.
     *
     * @param testedLevel The level tested, Majority/MajorityAndPersistOnMaster
     */
    void testHPS_LevelIsNotDurabilityFence(cb::durability::Level testedLevel);

    /**
     * Checks that the Prepares can be removed from the PassiveDM only if they
     * are locally-satisfied. Note that (differenlty from ActiveDM) in PassiveDM
     * a Prepare can be completed *before* the local HPS has covered it.
     *
     * @param res The type of resolution, Commit/Abort
     */
    void testRemoveCompletedOnlyIfLocallySatisfied(
            PassiveDurabilityMonitor::Resolution res);

    /**
     * Test that we can drop the first key in trackedWrites
     */
    void testDropFirstKey();

    /**
     * Test that we can drop the last key in trackedWrites
     */
    void testDropLastKey();
};

/*
 * PassiveDurabilityMonitor test fixture for tests that should only run against
 * persistent buckets
 */
class PassiveDurabilityMonitorPersistentTest
    : public PassiveDurabilityMonitorTest {};

class NoTopologyActiveDurabilityMonitorTest
    : public ActiveDurabilityMonitorTest {
public:
    void SetUp() override;
};