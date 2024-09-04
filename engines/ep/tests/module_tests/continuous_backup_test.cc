/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "tests/mock/mock_magma_kvstore.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"

class ContinousBackupTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        config_string +=
                "continuous_backup_enabled=true;"
                "continuous_backup_interval=1000";

        STParameterizedBucketTest::SetUp();
        // To allow tests to set where history begins, use MockMagmaKVStore
        replaceMagmaKVStore();
    }

    MockMagmaKVStore& getMockKVStore(Vbid vbid) {
        auto* kvstore = store->getRWUnderlying(vbid);
        return dynamic_cast<MockMagmaKVStore&>(*kvstore);
    }
};

TEST_P(ContinousBackupTest, Config) {
    using namespace std::chrono_literals;
    auto& store = getMockKVStore(vbid);

    EXPECT_EQ(store.isContinuousBackupEnabled(), true);
    EXPECT_EQ(store.getContinuousBackupInterval(), 1000s);

    engine->getConfiguration().setContinuousBackupEnabled(false);
    engine->getConfiguration().setContinuousBackupInterval(123);

    EXPECT_EQ(store.isContinuousBackupEnabled(), false);
    EXPECT_EQ(store.getContinuousBackupInterval(), 123s);
}

INSTANTIATE_TEST_SUITE_P(ContinousBackupTests,
                         ContinousBackupTest,
                         STParameterizedBucketTest::magmaBucket(),
                         STParameterizedBucketTest::PrintToStringParamName);