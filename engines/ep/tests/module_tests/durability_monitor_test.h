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

#include "evp_store_single_threaded_test.h"
#include "test_helpers.h"

#include <programs/engine_testapp/mock_server.h>

#include <folly/portability/GTest.h>

class ActiveDurabilityMonitor;
class PassiveDurabilityMonitor;

class DurabilityMonitorTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() override;
    void TearDown() override;

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
     * Stores the given item via VBucket::processSet.
     * Useful for setting an exact provided bySeqno.
     *
     * @param item the item to be stored
     */
    MutationStatus processSet(Item& item);

    // Owned by KVBucket
    VBucket* vb;
};

/*
 * ActiveDurabilityMonitor test fixture
 */
class ActiveDurabilityMonitorTest : public DurabilityMonitorTest {
public:
    void SetUp() override;
    void TearDown() override;

protected:
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
     * Adds the given mutations for tracking.
     *
     * @param seqnos the mutations to be added
     * @param req The Durability Requirements
     * @return the number of added SyncWrites
     */
    size_t addSyncWrites(const std::vector<int64_t>& seqnos,
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

    // Owned by VBucket
    ActiveDurabilityMonitor* monitor;

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

protected:
    // Owned by VBucket
    PassiveDurabilityMonitor* monitor;
};
