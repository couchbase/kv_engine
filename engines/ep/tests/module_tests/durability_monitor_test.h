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
#pragma once

#include "durability/durability_monitor.h"
#include "evp_store_single_threaded_test.h"
#include "test_helpers.h"

#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

class ActiveDurabilityMonitor;
class PassiveDurabilityMonitor;

class DurabilityMonitorTest : public STParameterizedBucketTest {
protected:
    /**
     * Add a SyncWrite for tracking.
     *
     * @param seqno
     * @param req The Durability Requirements
     * @return the error code from the underlying engine
     */
    void addSyncWrite(int64_t seqno, cb::durability::Requirements req = {});

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
     * Checks that the HPS is updated correctly when Level:Majority writes
     * are queued into the DM.
     * The same logic must apply for both ActiveDM and PassiveDM.
     */
    void testHPS_Majority();

    /**
     * Checks that the HPS is updated correctly when Level:PersistToMajority
     * writes are queued into the DM.
     * The same logic must apply for both ActiveDM and PassiveDM.
     */
    void testHPS_PersistToMajority();

    /**
     * Checks that the HPS is updated correctly when (1) Level:PersistToMajority
     * writes are queued into the DM and then (2) Level:Majority writes are
     * queued. Verifies that Level:PersistToMajority writes enforce a
     * durability-fence.
     * The same logic must apply for both ActiveDM and PassiveDM.
     */
    void testHPS_PersistToMajority_Majority();

    /**
     * Checks that the HPS is updated correctly when (1) Level:Majority writes
     * are queued into the DM and then (2) Level:PersistToMajority writes are
     * queued. Verifies that Level:Majority writes do *not* enforce a
     * durability-fence.
     * The same logic must apply for both ActiveDM and PassiveDM.
     */
    void testHPS_Majority_PersistToMajority();

    /**
     * Add the given SyncWrites for tracking and check that High Prepared Seqno
     * has been updated as expected.
     *
     * @param seqnos The mutations to be queued
     * @param level The Durability Level of the queued mutations
     * @param expectedHPS
     */
    void addSyncWriteAndCheckHPS(const std::vector<int64_t>& seqnos,
                                 cb::durability::Level level,
                                 int64_t expectedHPS);

    /**
     * Notify the persistedSeqno to the DM and check that High Prepared Seqno
     * has been updated as expected.
     *
     * @param persistedSeqno
     * @param expectedHPS
     */
    void notifyPersistenceAndCheckHPS(int64_t persistedSeqno,
                                      int64_t expectedHPS);

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
     * @param expectedNumTracked
     * @param expectedLastWriteSeqno
     * @param expectedLastAckSeqno
     */
    void testSeqnoAckReceived(const std::string& replica,
                              int64_t ackSeqno,
                              size_t expectedNumTracked,
                              int64_t expectedLastWriteSeqno,
                              int64_t expectedLastAckSeqno) const;

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
     * @return true if durability is possible, false otherwise
     */
    bool testDurabilityPossible(const nlohmann::json::array_t& topology,
                                queued_item& item,
                                uint8_t expectedFirstChainSize,
                                uint8_t expectedFirstChainMajority);

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
     * Check that the SyncWrites at seqno 1, 2, and 3 are committed on the
     * seqno acks at 1, 2, and 3 respectively.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrites
     */
    void testSeqnoAckEqualToPending(const std::vector<std::string>& nodesToAck);

    /**
     * Check that the SyncWrites at seqno 1 and 2 are committed when acking
     * seqno 2, but not a SyncWrite at seqno 3.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrites
     */
    void testSeqnoAckGreaterThanPendingContinuousSeqnos(
            const std::vector<std::string>& nodesToAck);

    /**
     * Check that the SyncWrites at senqo 1 and 3 are committed, but not the
     * SyncWrite at seqno 5 when acking seqno 4.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrites
     */
    void testSeqnoAckGreaterThanPendingSparseSeqnos(
            const std::vector<std::string>& nodesToAck);

    /**
     * Check that the SyncWrites at seqno 1 to 3 are committed when acking seqno
     * 4.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrites
     */
    void testSeqnoAckGreaterThanLastTrackedContinuousSeqnos(
            const std::vector<std::string>& nodesToAck);

    /**
     * Check that the SyncWrites at seqno 1, 3, and 5 are committed when acking
     * seqno 10.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrites
     */
    void testSeqnoAckGreaterThanLastTrackedSparseSeqnos(
            const std::vector<std::string>& nodesToAck);

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
     * Check that a SyncWrite is committed when the majority requirement is met
     * when their are multiple replicas.
     *
     * @param nodesToAck list of nodes to ack, the last node to ack in the list
     *                   should be the one to commit the SyncWrite
     * @param unchangedNodes list of nodes that do not ack, and should remain
     *                       unchanged throughout
     */
    void testSeqnoAckMultipleReplicas(
            const std::vector<std::string>& nodesToAck,
            const std::vector<std::string>& unchangedNodes);

    /**
     * Check that the MajorityAndPersistOnMaster SyncWrites at seqno 1, 3, and 5
     * are committed only when the active acks persistence after acking all the
     * given nodes.
     *
     * @param nodesToAck list of nodes to ack
     */
    void testSeqnoAckMajorityAndPersistOnMaster(
            const std::vector<std::string>& nodesToAck);

    const std::string active = "active";
    const std::string replica1 = "replica1";
    const std::string replica2 = "replica2";
    const std::string replica3 = "replica3";
};

/*
 * PassiveDurabilityMonitor test fixture
 */
class PassiveDurabilityMonitorTest : public DurabilityMonitorTest {
public:
    void SetUp() override;
    void TearDown() override;
};
