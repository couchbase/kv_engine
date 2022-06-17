/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "evp_store_single_threaded_test.h"

#include "../mock/mock_synchronous_ep_engine.h"

class BucketQuotaChangeTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();
    }

    size_t getCurrentBucketQuota() {
        return engine->getEpStats().getMaxDataSize();
    }

    void setBucketQuota(size_t newValue) {
        std::string msg;
        engine->setFlushParam("max_size", std::to_string(newValue), msg);
    }

    void checkQuota(size_t expected) {
        EXPECT_EQ(expected, engine->getConfiguration().getMaxSize());
        EXPECT_EQ(expected, getCurrentBucketQuota());
    }
};

TEST_P(BucketQuotaChangeTest, QuotaChangeEqual) {
    auto currentQuota = getCurrentBucketQuota();

    auto newQuota = currentQuota;
    setBucketQuota(newQuota);

    {
        SCOPED_TRACE("");
        checkQuota(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDown) {
    auto currentQuota = getCurrentBucketQuota();

    auto newQuota = currentQuota / 2;
    setBucketQuota(newQuota);

    {
        SCOPED_TRACE("");
        checkQuota(newQuota);
    }
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUp) {
    auto currentQuota = getCurrentBucketQuota();

    auto newQuota = currentQuota * 2;
    setBucketQuota(newQuota);

    {
        SCOPED_TRACE("");
        checkQuota(newQuota);
    }
}

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         BucketQuotaChangeTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
