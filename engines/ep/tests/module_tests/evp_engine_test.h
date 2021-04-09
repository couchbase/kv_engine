/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Unit tests for the EventuallyPersistentEngine class.
 */

#pragma once

#include "../mock/mock_executor_pool.h"

#include <folly/portability/GTest.h>
#include <memcached/durability_spec.h>
#include <memcached/vbucket.h>

namespace cb::tracing {
class Traceable;
}

struct EngineIface;
class EventuallyPersistentEngine;

class Item;
using queued_item = SingleThreadedRCPtr<Item>;

class EventuallyPersistentEngineTest : virtual public ::testing::Test {
public:
    EventuallyPersistentEngineTest();

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
                                           cb::durability::Level::Majority,
                                           cb::durability::Timeout(0)});

    /**
     * Store a pending SyncDelete into the given vbucket.
     * @returns the item stored
     */
    queued_item store_pending_delete(Vbid vbid,
                                     const std::string& key,
                                     cb::durability::Requirements reqs = {
                                             cb::durability::Level::Majority,
                                             cb::durability::Timeout(0)});

    /// Stored a committed SyncWrite into the given vbucket.
    void store_committed_item(Vbid vbid,
                              const std::string& key,
                              const std::string& value);

    std::string config_string;

    // path for the test's database files.
    const std::string test_dbname;

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

    cb::tracing::Traceable* cookie = nullptr;
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
