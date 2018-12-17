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
#include "../mock/mock_synchronous_ep_engine.h"

void DurabilityMonitorTest::addSyncWrites() {
    EXPECT_EQ(0, mgr->public_getNumTracked());
    size_t expectedNumTracked = 0;
    for (auto& item : itemStore) {
        queued_item qi(std::move(item));
        using namespace cb::durability;
        qi->setPendingSyncWrite(Requirements(Level::Majority, 0 /*timeout*/));
        EXPECT_EQ(ENGINE_SUCCESS, mgr->addSyncWrite(qi));
        expectedNumTracked++;
        EXPECT_EQ(expectedNumTracked, mgr->public_getNumTracked());
    }
}

TEST_F(DurabilityMonitorTest, AddSyncWrite) {
    addSyncWrites();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedNoTrackedSyncWrite) {
    try {
        mgr->seqnoAckReceived(replica, 1 /*memSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("No tracked SyncWrite") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedSmallerThanPending) {
    addSyncWrites();
    try {
        auto seqno = mgr->public_getReplicaMemorySeqno(replica);
        mgr->seqnoAckReceived(replica, seqno - 1 /*memSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find(
                            "Ack'ed seqno is behind pending seqno") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedEqualPending) {
    addSyncWrites();

    // No ack received yet
    EXPECT_EQ(0 /*memSeqno*/, mgr->public_getReplicaMemorySeqno(replica));

    size_t seqno = 1;
    for (; seqno <= numItems; seqno++) {
        EXPECT_NO_THROW(mgr->seqnoAckReceived(replica, seqno /*memSeqno*/));

        // Check that the DM'tracking advances by 1 at each cycle
        EXPECT_EQ(seqno, mgr->public_getReplicaMemorySeqno(replica));
    }

    // All ack'ed, no more pendings
    try {
        mgr->seqnoAckReceived(replica, seqno + 1 /*memSeqno*/);
    } catch (const std::logic_error& e) {
        EXPECT_TRUE(std::string(e.what()).find("No pending SyncWrite") !=
                    std::string::npos);
        return;
    }
    FAIL();
}

TEST_F(DurabilityMonitorTest, SeqnoAckReceivedGreaterThanPending) {
    addSyncWrites();
    ASSERT_GT(numItems, 1);
    EXPECT_EQ(ENGINE_ENOTSUP,
              mgr->seqnoAckReceived(replica, numItems /*memSeqno*/));
}
