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
#include "ep_engine.h"
#include "kv_bucket.h"

#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif

auto percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

class BucketQuotaChangeTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        STParameterizedBucketTest::SetUp();

        // Watermark percentages are the percentage of the current watermark
        // of the current quota
        initialMemLowWatPercent = engine->getEpStats().mem_low_wat_percent;
        initialMemHighWatPercent = engine->getEpStats().mem_high_wat_percent;
    }

    size_t getCurrentBucketQuota() {
        return engine->getEpStats().getMaxDataSize();
    }

    void setBucketQuota(size_t newValue) {
        std::string msg;
        engine->setFlushParam("max_size", std::to_string(newValue), msg);
    }

    void setLowWatermark(double percentage) {
        auto newValue = percentOf(getCurrentBucketQuota(), percentage);

        std::string msg;
        engine->setFlushParam("mem_low_wat", std::to_string(newValue), msg);

        initialMemLowWatPercent = percentage;
    }

    void setHighWatermark(double percentage) {
        auto newValue = percentOf(getCurrentBucketQuota(), percentage);

        std::string msg;
        engine->setFlushParam("mem_high_wat", std::to_string(newValue), msg);

        initialMemHighWatPercent = percentage;
    }

    void checkQuota(size_t expected) {
        EXPECT_EQ(expected, engine->getConfiguration().getMaxSize());
        EXPECT_EQ(expected, getCurrentBucketQuota());
    }

    void checkWatermarkValues(size_t quotaValue) {
        EXPECT_EQ(initialMemLowWatPercent,
                  engine->getEpStats().mem_low_wat_percent);
        EXPECT_EQ(percentOf(quotaValue, initialMemLowWatPercent),
                  engine->getConfiguration().getMemLowWat());

        EXPECT_EQ(initialMemHighWatPercent,
                  engine->getEpStats().mem_high_wat_percent);
        EXPECT_EQ(percentOf(quotaValue, initialMemHighWatPercent),
                  engine->getConfiguration().getMemHighWat());
    }

    void checkMaxRunningBackfills(size_t quotaValue) {
        EXPECT_EQ(engine->getDcpConnMap().getMaxRunningBackfillsForQuota(
                          quotaValue),
                  engine->getDcpConnMap().getMaxRunningBackfills());
    }

    void checkStorageEngineQuota(size_t quotaValue) {
#ifdef EP_USE_MAGMA
        if (STParameterizedBucketTest::isMagma()) {
            auto& magmaKVStoreConfig = static_cast<const MagmaKVStoreConfig&>(
                    engine->getKVBucket()->getOneRWUnderlying()->getConfig());

            size_t magmaQuota;
            engine->getKVBucket()->getOneRWUnderlying()->getStat("memory_quota",
                                                                 magmaQuota);

            EXPECT_EQ(
                    percentOf(quotaValue,
                              magmaKVStoreConfig.getMagmaMemQuotaRatio()),
                    magmaQuota * engine->getConfiguration().getMaxNumShards());
        }
#endif
    }

    void checkBucketQuotaAndRelatedValues(size_t quotaValue) {
        SCOPED_TRACE("");
        checkQuota(quotaValue);
        checkWatermarkValues(quotaValue);
        checkMaxRunningBackfills(quotaValue);
        checkStorageEngineQuota(quotaValue);
    }

    void testQuotaChangeUp() {
        auto currentQuota = getCurrentBucketQuota();

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota * 2;
        setBucketQuota(newQuota);

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    void testQuotaChangeDown() {
        auto currentQuota = getCurrentBucketQuota();

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(currentQuota);
        }

        auto newQuota = currentQuota / 2;
        setBucketQuota(newQuota);

        {
            SCOPED_TRACE("");
            checkBucketQuotaAndRelatedValues(newQuota);
        }
    }

    double initialMemLowWatPercent;
    double initialMemHighWatPercent;
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
    SCOPED_TRACE("");
    testQuotaChangeDown();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUp) {
    SCOPED_TRACE("");
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeUpNonDefaultWatermarks) {
    SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeUp();
}

TEST_P(BucketQuotaChangeTest, QuotaChangeDownNonDefaultWatermarks) {
    SCOPED_TRACE("");
    setLowWatermark(0.5);
    setHighWatermark(0.6);
    testQuotaChangeDown();
}

INSTANTIATE_TEST_SUITE_P(EphemeralOrPersistent,
                         BucketQuotaChangeTest,
                         STParameterizedBucketTest::allConfigValuesNoNexus(),
                         STParameterizedBucketTest::PrintToStringParamName);
