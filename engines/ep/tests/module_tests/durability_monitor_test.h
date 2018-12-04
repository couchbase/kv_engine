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
        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
        auto& vb = *store->getVBuckets().getBucket(vbid);
        mgr = std::make_unique<MockDurabilityMonitor>(vb);
        ASSERT_EQ(ENGINE_SUCCESS, mgr->registerReplicationChain({replicaUUID}));

        // Populate the mock Store
        for (size_t seqno = 1; seqno <= numItems; seqno++) {
            itemStore.push_back(std::make_unique<Item>(
                    makeStoredDocKey("key" + std::to_string(seqno)),
                    0 /*flags*/,
                    0 /*exp*/,
                    "value",
                    5 /*valueSize*/,
                    PROTOCOL_BINARY_RAW_BYTES,
                    0 /*cas*/,
                    seqno));
        }

        // Populate the HastTable
        cookie = const_cast<void*>(create_mock_cookie());
        for (auto& item : itemStore) {
            auto collectionsRHandle = vb.lockCollections(item->getKey());
            ASSERT_TRUE(collectionsRHandle.valid());
            EXPECT_EQ(ENGINE_SUCCESS,
                      vb.add(*item, cookie, *engine, collectionsRHandle));
        }
        ASSERT_EQ(numItems, vb.ht.getNumItems());
    }

    void TearDown() {
        destroy_mock_cookie(cookie);
        mgr.reset();
        itemStore.clear();
        SingleThreadedKVBucketTest::TearDown();
    }

protected:
    void addSyncWrites();

    const std::string replicaUUID = "uuid1";
    std::unique_ptr<MockDurabilityMonitor> mgr;

    const size_t numItems = 3;
    // Mock engine store
    std::vector<std::unique_ptr<Item>> itemStore;
    void* cookie;
};
