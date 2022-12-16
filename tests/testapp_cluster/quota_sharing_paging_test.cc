/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <fmt/format.h>
#include <folly/synchronization/Baton.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <future>

using namespace cb::test;

size_t operator""_MiB(unsigned long long mebibytes) {
    return mebibytes * 1024 * 1024;
}

size_t operator""_MiB(long double mebibytes) {
    return static_cast<size_t>(mebibytes * 1024 * 1024);
}

// Helper to calculate absolute difference between two size_t.
ssize_t absdiff(size_t x, size_t y) {
    return std::abs(gsl::narrow<ssize_t>(x) - gsl::narrow<ssize_t>(y));
}

class QSPagingTest : public ClusterTest {
public:
    QSPagingTest(int numBuckets) : numBuckets(numBuckets) {
    }

    static void SetUpTestCase() {
        cluster = Cluster::create(1);
    }

    static void TearDownTestCase() {
        cluster.reset();
    }

    void SetUp() override {
        for (int i = 0; i < numBuckets; i++) {
            auto bucket = cluster->createBucket(fmt::format("bucket{}", i),
                                                getBucketConfig());

            buckets.push_back(bucket);

            auto conn = bucket->getConnection(Vbid(0));
            conn->authenticate("@admin", "password", "PLAIN");
            conn->selectBucket(bucket->getName());
            conns.push_back(std::move(conn));
        }
    }

    void TearDown() override {
        for (auto bucket : buckets) {
            cluster->deleteBucket(bucket->getName());
        }

        buckets.clear();
        conns.clear();
    }

protected:
    virtual nlohmann::json getBucketConfig() {
        return bucketConfig;
    }

    MemStats getSharedMemStats() {
        MemStats acc;
        for (int i = 0; i < numBuckets; i++) {
            auto stats = ClusterTest::getMemStats(*conns[i]);
            acc.lower += stats.lower;
            acc.upper += stats.upper;
            acc.current += stats.current;
        }
        return acc;
    }

    MemStats getMemStats(int bucket) {
        return ClusterTest::getMemStats(*conns[bucket]);
    }

    void setFlushParam(int bucket,
                       const std::string& paramName,
                       const std::string& paramValue) {
        ClusterTest::setFlushParam(*conns[bucket], paramName, paramValue);
    }

    void setMemWatermarks(int bucket, size_t memLowWat, size_t memHighWat) {
        ClusterTest::setMemWatermarks(*conns[bucket], memLowWat, memHighWat);
    }

    int getNumPagerRuns(int bucket) {
        return conns[bucket]->stats("")["ep_num_pager_runs"].get<int>();
    }

    std::vector<std::pair<Vbid, std::string>> generateItems(
            int bucket, size_t minTotalSize) {
        const size_t initialMemUsed = getMemStats(bucket).current;
        const size_t targetMemUsed = initialMemUsed + minTotalSize;

        const intptr_t itemSize = 0.125_MiB;
        const auto itemValue = std::string(itemSize, 'x');

        std::vector<std::pair<Vbid, std::string>> docKeys;

        int key = 0;
        while (getMemStats(bucket).current <= targetMemUsed) {
            auto vb = key % maxVBuckets;
            auto docKey = "key_" + std::to_string(key);
            conns[bucket]->store(docKey, Vbid(vb), itemValue);
            docKeys.emplace_back(Vbid(vb), std::move(docKey));
            key++;
        }

        auto minExpectedItems = (targetMemUsed - initialMemUsed) / itemSize / 2;
        EXPECT_GT(key, minExpectedItems)
                << "Wrote too few items. Expected to write at least "
                << minExpectedItems << " items but wrote only " << key
                << " before memory usage increased by " << minTotalSize / 1._MiB
                << " MiB";

        return docKeys;
    }

    /**
     * Triggers the check for mem_used above high_wat.
     */
    void triggerItemPagerIfNecessary() {
        // The checkAndMaybeFreeMemory check is done whenever we store an item.
        conns[0]->store("temp_item_to_trigger_eviction", Vbid(0), "");
        conns[0]->remove("temp_item_to_trigger_eviction", Vbid(0));
    }

    /**
     * Helper use to wait for a memory stat related check to pass.
     *
     * Keeps calling triggerItemPagerIfNecessary() until the wait times out or
     * the callback returns true.
     */
    template <typename Cond>
    bool waitAndTriggerItemPagerIfNecessary(Cond&& c) {
        // Wait for the predicate to return true (e.g. memory usage to drop)
        for (int i = 0; i < 10; i++) {
            triggerItemPagerIfNecessary();
            if (c()) {
                return true;
            }
            std::this_thread::sleep_for(evictionTimeout / 10);
        }
        return c();
    }

    /**
     * We only check whether we've reached the low watermark on every vBucket
     * visit. This means that we can potentially be 1 byte over than, but
     * proceed to evict everything from the following vBucket if all items
     * are at the same MFU (as they are in some tests below).
     */
    size_t evictionErrorMargin() {
        return conns[0]->stats("memory")["ep_max_size"].get<size_t>() /
               numBuckets;
    }

    // A single-node cluster.
    static std::unique_ptr<cb::test::Cluster> cluster;

    const int numBuckets;
    const int maxVBuckets{24};
    const std::chrono::milliseconds evictionTimeout{1000};
    // cross_bucket_ht_quota_sharing is not a default configuration for
    // any type of deployment (yet) so we have to enable it manually for
    // these tests. We don't care about replicas so we will disable
    // those here, and we need to set the freq_counter_increment_factor
    // to a lower value than the default to trigger the ItemFreqDecayer
    // more quickly in the tests that care about that. Setting it to 0
    // means that we always increment the frequency counter.
    const nlohmann::json bucketConfig{
            {"replicas", 0},
            {"max_vbuckets", maxVBuckets},
            {"cross_bucket_ht_quota_sharing", true},
            // Setting the increment factor to 0 means we always increment
            {"freq_counter_increment_factor", 0}};

    std::vector<std::shared_ptr<cb::test::Bucket>> buckets;
    // Connections to each bucket's vb:0.
    std::vector<std::unique_ptr<MemcachedConnection>> conns;
};

std::unique_ptr<cb::test::Cluster> QSPagingTest::cluster{nullptr};

/**
 * The 1 bucket test is parameteric, because we run it with quota sharing on
 * and off, as a sanity check.
 */
class OneBucketQSPagingTest : public QSPagingTest,
                              public ::testing::WithParamInterface<bool> {
public:
    OneBucketQSPagingTest() : QSPagingTest(1) {
    }

    nlohmann::json getBucketConfig() override {
        auto config = bucketConfig;
        config["cross_bucket_ht_quota_sharing"] = GetParam();
        return config;
    }
};

class TwoBucketQSPagingTest : public QSPagingTest {
public:
    TwoBucketQSPagingTest() : QSPagingTest(2) {
    }
};

/**
 * Test that eviction for a single bucket with quota sharing enabled works as
 * expected.
 */
TEST_P(OneBucketQSPagingTest, SingleBucketEvictionWorks) {
    const intptr_t testItemMemUsage = 15_MiB;

    setMemWatermarks(0, 20_MiB, 25_MiB);

    generateItems(0, testItemMemUsage);
    ASSERT_TRUE(getMemStats(0).isBelowLowWatermark());
    ASSERT_GT(getMemStats(0).current, 5_MiB);

    // Set the high watermark to a value that should cause items to be evicted.
    setMemWatermarks(0, 10_MiB, 12_MiB);
    // Items not immediately evicted -- memory condition not triggered.
    EXPECT_TRUE(getMemStats(0).isAboveHighWatermark())
            << "Expected bucket 0 to be above high watermark: "
            << getMemStats(0);

    EXPECT_TRUE(waitAndTriggerItemPagerIfNecessary([this]() {
        return !getMemStats(0).isAboveHighWatermark();
    })) << "Expected bucket 0 to be below high watermark: "
        << getMemStats(0);
    EXPECT_GT(getNumPagerRuns(0), 0);
}

INSTANTIATE_TEST_SUITE_P(QuotaSharingOnOff,
                         OneBucketQSPagingTest,
                         ::testing::Bool(),
                         ::testing::PrintToStringParamName());

/**
 * Start with 2 buckets, one of which has items in it and the other
 * has nothing in it. Check that items are evicted from bucket 0 such that
 * we reach the quota sharing low watermark.
 */
TEST_F(TwoBucketQSPagingTest, ItemsAreEvictedFromSingleBucket) {
    const intptr_t testItemMemUsage = 15_MiB;

    setMemWatermarks(0, 20_MiB, 25_MiB);

    generateItems(0, testItemMemUsage);
    ASSERT_TRUE(getMemStats(0).isBelowLowWatermark());
    ASSERT_GT(getMemStats(0).current, 5_MiB);

    // Set the high watermark to a value that should cause items to be evicted.
    setMemWatermarks(0, 10_MiB, 12_MiB);
    // Nothing to evict from bucket 1 and little contribution to sum(high_wat)
    setMemWatermarks(
            1, getMemStats(1).current, getMemStats(1).current + 0.1_MiB);
    ASSERT_TRUE(getSharedMemStats().isAboveHighWatermark())
            << "Expected quota-sharing group to be above high watermark: "
            << getSharedMemStats();

    EXPECT_TRUE(waitAndTriggerItemPagerIfNecessary([this]() {
        return !getSharedMemStats().isAboveHighWatermark();
    })) << "Expected quota-sharing group to be below high watermark: "
        << getSharedMemStats();
}

/**
 * Have a bucket "steal quota" from the other bucket and ensure that we don't
 * evict anything from that bucket.
 */
TEST_F(TwoBucketQSPagingTest, BucketsCanStealQuota) {
    setMemWatermarks(0, 20_MiB, 25_MiB);
    setMemWatermarks(1, 20_MiB, 25_MiB);

    // Have bucket 0 go over its individual quota, but sum(mem_used) <
    // sum(high_wat) so we shouldn't end up evicting anything in this case.
    generateItems(0, 30_MiB);
    generateItems(1, 10_MiB);

    ASSERT_TRUE(getMemStats(0).isAboveHighWatermark());
    ASSERT_TRUE(getMemStats(1).isBelowLowWatermark());

    ASSERT_GT(getMemStats(0).current, 30_MiB);
    ASSERT_GT(getMemStats(1).current, 10_MiB);

    // Make sure nothing is evicted even after waiting for the same amount
    // of time it takes us to evict items in other tests.
    auto expectedMemUsage = getMemStats(0).current;
    std::this_thread::sleep_for(evictionTimeout);
    EXPECT_NEAR(expectedMemUsage, getMemStats(0).current, 1_MiB);
}

/**
 * Fill 2 buckets with items, set their watermarks to different values, all
 * lower than sum(mem_used). Check that the mem_usage of the two buckets
 * end up similar after eviction runs.
 */
TEST_F(TwoBucketQSPagingTest, ItemsAreFairlyEvictedFromTwoBuckets) {
    const intptr_t testItemMemUsage = 15_MiB;

    setMemWatermarks(0, 20_MiB, 25_MiB);
    setMemWatermarks(1, 20_MiB, 25_MiB);

    generateItems(0, testItemMemUsage);
    generateItems(1, testItemMemUsage);

    ASSERT_TRUE(getMemStats(0).isBelowLowWatermark());
    ASSERT_TRUE(getMemStats(1).isBelowLowWatermark());

    ASSERT_GT(getMemStats(0).current, 15_MiB);
    ASSERT_GT(getMemStats(1).current, 15_MiB);

    // We have 15 MiB in bucket 0 and 15 MiB in bucket 1. Set the high wat of
    // both to 14 MiB to trigger eviction. Also, set the low watermark to 8 MiB
    // and 12 MiB, respectively, to test that:
    // a) eviction does not stop when an individual bucket's low_wat is reached
    // b) items are fairly evicted from both buckets
    // Expect to see bucket0:mem_usage ~= bucket1:mem_usage ~= 7.5 MiB,
    // since the MFU values in both buckets are the same and quota sharing
    // pager evict fairly from both buckets.
    setMemWatermarks(0, 8_MiB, 14_MiB);
    setMemWatermarks(1, 12_MiB, 14_MiB);
    ASSERT_TRUE(getSharedMemStats().isAboveHighWatermark());

    // Group < high_wat
    EXPECT_TRUE(waitAndTriggerItemPagerIfNecessary([this]() {
        return !getSharedMemStats().isAboveHighWatermark();
    })) << "Expected quota-sharing group to be below high watermark: "
        << getSharedMemStats();
    // Both buckets should be at ~7.5 MiB mem usage, regardless of their
    // watermarks.
    EXPECT_NEAR(getMemStats(0).current, 7.5_MiB, evictionErrorMargin());
    EXPECT_NEAR(getMemStats(1).current, 7.5_MiB, evictionErrorMargin());
    // Abs difference between buckets' mem usage should be < 2 MiB at this point
    EXPECT_NEAR(getMemStats(0).current,
                getMemStats(1).current,
                evictionErrorMargin() * 2)
            << "bucket0: " << getMemStats(0) << ", bucket1: " << getMemStats(1);
}

/**
 * Fill 2 buckets with items, make the items in one bucket hotter, cause
 * eviction to run, then assert that we evict from the less hot bucket.
 */
TEST_F(TwoBucketQSPagingTest, ItemsAreEvictedFromLessHotBucketsFirst) {
    const intptr_t testItemMemUsage = 20_MiB;

    setMemWatermarks(0, 20_MiB, 30_MiB);
    setMemWatermarks(1, 20_MiB, 30_MiB);

    auto hotBucketItems = generateItems(0, testItemMemUsage);
    auto coldBucketItems = generateItems(1, testItemMemUsage);

    // Make the items in bucket 0 hotter.
    for (const auto& [vbid, key] : hotBucketItems) {
        for (int i = 0; i < 2; i++) {
            conns[0]->get(key, vbid);
        }
    }

    ASSERT_TRUE(!getMemStats(0).isAboveHighWatermark());
    ASSERT_TRUE(!getMemStats(1).isAboveHighWatermark());

    auto hotBucketMemUsed = getMemStats(0).current;
    auto coldBucketMemUsed = getMemStats(1).current;
    ASSERT_GT(hotBucketMemUsed, 19_MiB);
    ASSERT_GT(coldBucketMemUsed, 19_MiB);

    // We have 15 MiB in bucket 0 and 15 MiB in bucket 1. Set the high wat of
    // both to 14 MiB to trigger eviction. What we should see is the mem_used
    // of the colder bucket (bucket 1) fall, due to eviction, but bucket 0 stay
    // the same.
    setMemWatermarks(0, 15_MiB, 18_MiB);
    setMemWatermarks(1, 15_MiB, 18_MiB);

    EXPECT_TRUE(waitAndTriggerItemPagerIfNecessary([this]() {
        return !getSharedMemStats().isAboveHighWatermark();
    })) << "Expected quota-sharing group to be below high watermark: "
        << getSharedMemStats();
    // And the hot bucket is untouched.
    EXPECT_NEAR(hotBucketMemUsed, getMemStats(0).current, 1_MiB)
            << "Expected hot bucket mem usage to remain unchanged.";
    // But the memory usage of the colder bucket has dropped.
    EXPECT_GT(absdiff(coldBucketMemUsed, getMemStats(1).current), 9_MiB)
            << "Expected cold bucket mem usage to drop.";
}

/**
 * Make sure we don't prevent buckets from being destroyed while eviction is
 * running.
 */
TEST_F(TwoBucketQSPagingTest, BucketsCanBeDestroyedWhileEvictionIsRunning) {
    auto startMemUsage = getMemStats(0).current;
    setMemWatermarks(0, startMemUsage, startMemUsage + 0.5_MiB);
    setMemWatermarks(1, startMemUsage, startMemUsage + 0.5_MiB);

    folly::Baton<> bucketCreated;
    auto f = std::async(std::launch::async, [this, &bucketCreated]() {
        auto tempBucket = cluster->createBucket("tempbucket", bucketConfig);
        // Setup a connection to the bucket
        auto tempBucketConn = tempBucket->getConnection(Vbid(0));
        tempBucketConn->authenticate("@admin", "password", "PLAIN");
        tempBucketConn->selectBucket(tempBucket->getName());
        ClusterTest::setMemWatermarks(
                *tempBucketConn,
                0,
                ClusterTest::getMemStats(*tempBucketConn).current + 0.5_MiB);
        // Signal the bucket has been created
        bucketCreated.post();

        while (tempBucketConn->stats("")["ep_num_pager_runs"].get<int>() < 1) {
            std::this_thread::yield();
        }

        cluster->deleteBucket("tempbucket");
        buckets.pop_back();
        // Stop eviction by increasing shared quota high_wat, allowing
        // generateItems below to complete.
        setMemWatermarks(0, 100_MiB, 100_MiB);
    });

    // Wait for the thread to create the test bucket
    bucketCreated.wait();
    // We will never reach 10 MiB mem usage because the high watermark is
    // too low, so we will be continuously evicting items.
    generateItems(1, 10_MiB);
    f.get();
}
