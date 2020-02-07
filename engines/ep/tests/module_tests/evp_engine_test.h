/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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
 * Unit tests for the EventuallyPersistentEngine class.
 */

#pragma once

#include "../mock/mock_executor_pool.h"

#include <folly/portability/GTest.h>
#include <memcached/durability_spec.h>
#include <memcached/vbucket.h>

struct EngineIface;
class EventuallyPersistentEngine;

class Item;
using queued_item = SingleThreadedRCPtr<Item>;

class EventuallyPersistentEngineTest : virtual public ::testing::Test {
public:
    EventuallyPersistentEngineTest() : bucketType("persistent") {
    }

protected:
    void SetUp() override;

    void TearDown() override;

    /* Helper methods for tests */

    /* Stores an item into the given vbucket. */
    queued_item store_item(Vbid vbid,
                           const std::string& key,
                           const std::string& value);

    /**
     * Store a pending SyncWrite into the given vbucket.
     * @returns the item stored
     */
    queued_item store_pending_item(Vbid vbid,
                                   const std::string& key,
                                   const std::string& value,
                                   cb::durability::Requirements reqs = {
                                           cb::durability::Level::Majority, 0});

    /**
     * Store a pending SyncDelete into the given vbucket.
     * @returns the item stored
     */
    queued_item store_pending_delete(Vbid vbid,
                                     const std::string& key,
                                     cb::durability::Requirements reqs = {
                                             cb::durability::Level::Majority,
                                             0});

    /// Stored a committed SyncWrite into the given vbucket.
    void store_committed_item(Vbid vbid,
                              const std::string& key,
                              const std::string& value);

    std::string config_string;

    static const char test_dbname[];

    const Vbid vbid = Vbid(0);

    EngineIface* handle;
    EventuallyPersistentEngine* engine;
    std::string bucketType;

    /**
     * Maximum number vBuckets to create (reduced from normal production count
     * of 1024 to minimise test setup / teardown time).
     */
    int numVbuckets = 4;

    /**
     * Maximum number vBuckets to create (reduced from normal production count
     * of auto-selected from #CPUS to minimise test setup / teardown time).
     * '2' picked as is enough to still exercise KVShard logic (VBuckets
     * distributed across > 1 shard).
     */
    int numShards = 2;

    const void* cookie = nullptr;
};

/* Tests parameterised over ephemeral and persistent buckets
 *
 */
class SetParamTest : public EventuallyPersistentEngineTest,
                     public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        bucketType = GetParam();
        EventuallyPersistentEngineTest::SetUp();
    }
};

/*
 * EPEngine-level test fixture for Durability.
 */
class DurabilityTest : public EventuallyPersistentEngineTest,
                       public ::testing::WithParamInterface<std::string> {
    void SetUp() override {
        bucketType = GetParam();
        MockExecutorPool::replaceExecutorPoolWithMock();
        EventuallyPersistentEngineTest::SetUp();
    }
};
