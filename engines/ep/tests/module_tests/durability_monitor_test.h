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

#include "../mock/mock_durability_monitor.h"
#include "../mock/mock_synchronous_ep_engine.h"

#include <programs/engine_testapp/mock_server.h>

#include <gtest/gtest.h>

/*
 * DurabilityMonitor test fixture
 */
class DurabilityMonitorTest : public SingleThreadedKVBucketTest {
public:
    void SetUp() {
        SingleThreadedKVBucketTest::SetUp();
        setVBucketStateAndRunPersistTask(
                vbid,
                vbucket_state_active,
                {{"topology", nlohmann::json::array({{active, replica}})}});
        vb = store->getVBuckets().getBucket(vbid).get();
        // Note: MockDurabilityMonitor is used only for accessing the base
        //     class protected members, it doesn't change the base class layout
        monitor = reinterpret_cast<MockDurabilityMonitor*>(
                vb->durabilityMonitor.get());
        ASSERT_GT(monitor->public_getReplicationChainSize(), 0);
    }

    void TearDown() {
        SingleThreadedKVBucketTest::TearDown();
    }

protected:
    /**
     * Adds a SyncWrite for tracking.
     *
     * @param seqno
     * @param req The Durability Requirements
     * @return the error code from the underlying engine
     */
    void addSyncWrite(int64_t seqno, cb::durability::Requirements req = {});

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
     * Stores the given item via VBucket::processSet.
     * Useful for setting an exact provided bySeqno.
     *
     * @param item the item to be stored
     */
    MutationStatus processSet(Item& item);

    // Owned by KVBucket
    VBucket* vb;
    // Owned by VBucket
    MockDurabilityMonitor* monitor;

    const std::string active = "active";
    const std::string replica = "replica";
};
