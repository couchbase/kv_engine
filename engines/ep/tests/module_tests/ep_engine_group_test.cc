/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include "ep_engine_group.h"
#include "kv_bucket_test.h"
#include <tests/mock/mock_synchronous_ep_engine.h>

struct MockBucketApi : public ServerBucketIface {
    EngineIface* associateBucketFilter{nullptr};
    std::optional<AssociatedBucketHandle> tryAssociateBucket(
            EngineIface* engine) const override {
        if (associateBucketFilter == engine) {
            return AssociatedBucketHandle(engine, [](auto&&) {});
        }
        return {};
    }
};

class EPEngineGroupTest : public KVBucketTest {
public:
    void SetUp() override {
        config_string += ";bucket_type=ephemeral";
        KVBucketTest::SetUp();
        // tryAssociateBucket will always succeed
        bucketApi.associateBucketFilter = engine.get();
        // Replace the serverApi with the testing one
        serverApi = *engine->getServerApi();
        serverApi.bucket = &bucketApi;
        engine->setServerApi(&serverApi);
    }

    MockBucketApi bucketApi;
    ServerApi serverApi;
};

TEST_F(EPEngineGroupTest, AddEngine) {
    EPEngineGroup g(bucketApi);
    g.add(*engine);

    EXPECT_EQ(1, g.getActive().size());
    EXPECT_THROW(g.add(*engine), std::invalid_argument);
    EXPECT_EQ(1, g.getActive().size());
}

TEST_F(EPEngineGroupTest, AddEngineThenFailAssociate) {
    EPEngineGroup g(bucketApi);
    g.add(*engine);

    // Make tryAssociateBucket fail.
    bucketApi.associateBucketFilter = nullptr;
    // Engine should not be returned.
    EXPECT_EQ(0, g.getActive().size());
    // Let tryAssociateBucket succeed.
    bucketApi.associateBucketFilter = engine.get();
    EXPECT_EQ(1, g.getActive().size());
}

TEST_F(EPEngineGroupTest, RemoveEngine) {
    EPEngineGroup g(bucketApi);
    g.add(*engine);

    EXPECT_EQ(1, g.getActive().size());
    g.remove(*engine);
    EXPECT_EQ(0, g.getActive().size());
    EXPECT_THROW(g.remove(*engine), std::invalid_argument);
}
