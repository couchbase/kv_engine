/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
#include "stats.h"

#include <folly/portability/GTest.h>
using namespace std::string_view_literals;

TEST(BucketTest, Reset) {
#if defined(__linux) && defined(__x86_64__)
    ASSERT_EQ(5808, sizeof(Bucket))
            << "Bucket size changed, the reset test must be updated with "
               "the new members";
#endif

    class MockBucket : public Bucket {
    public:
        void testReset() {
            throttle_gauge.increment(5);
            throttle_limit = 1;
            num_throttled = 1;
            throttle_wait_time = 1;
            num_commands = 1;
            num_commands_with_metered_units = 1;
            num_metered_dcp_messages = 1;
            num_rejected = 1;
            bucket_quota_exceeded = true;

            reset();
            EXPECT_EQ(std::numeric_limits<std::size_t>::max(), throttle_limit);
            EXPECT_EQ(0, num_throttled);
            EXPECT_EQ(0, throttle_wait_time);
            EXPECT_EQ(0, num_commands);
            EXPECT_EQ(0, num_commands_with_metered_units);
            EXPECT_EQ(0, num_metered_dcp_messages);
            EXPECT_EQ(0, num_rejected);
            EXPECT_EQ(0, throttle_gauge.getValue());
            EXPECT_FALSE(bucket_quota_exceeded);
        }
    } bucket;

    bucket.testReset();
}

class BucketManagerTest : public ::testing::Test, public BucketManager {
public:
    void SetUp() override {
        for (std::size_t ii = 1; ii < all_buckets.size(); ++ii) {
            all_buckets[ii].reset();
        }
    }

protected:
    void bucketTypeChangeListener(Bucket& bucket, BucketType type) override {
        bucketTypeChangeListenerFunc(bucket, type);
    }
    void bucketStateChangeListener(Bucket& bucket,
                                   Bucket::State state) override {
        bucketStateChangeListenerFunc(bucket, state);
    }

    std::function<void(Bucket& bucket, BucketType type)>
            bucketTypeChangeListenerFunc = [](Bucket&, BucketType) {};
    std::function<void(Bucket& bucket, Bucket::State state)>
            bucketStateChangeListenerFunc = [](Bucket&, Bucket::State) {};
};

TEST_F(BucketManagerTest, AllocateBucket) {
    // Allocate a bucket
    auto validateAllocatedBucket = [](std::string_view name, Bucket* bucket) {
        ASSERT_NE(nullptr, bucket);
        EXPECT_EQ(name, bucket->name);
        EXPECT_EQ(BucketType::Unknown, bucket->type);
        EXPECT_EQ(Bucket::State::Creating, bucket->state);
    };

    auto [err, bucket] = allocateBucket("foo");
    ASSERT_EQ(cb::engine_errc::success, err);
    validateAllocatedBucket("foo"sv, bucket);

    // If we try to allocate a new one we would get temporary failure
    // as we already have a call running
    {
        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::temporary_failure, err2);
        EXPECT_EQ(nullptr, bucket2);
    }

    // But if we progress the management command we would an error that it
    // already exists
    {
        bucket->management_operation_in_progress = false;
        bucket->state = Bucket::State::Ready;

        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::key_already_exists, err2);
        EXPECT_EQ(nullptr, bucket2);
    }

    // But if it is a ClusterConfigOnly bucket it should be allowed to
    // grab that slot
    {
        bucket->type = BucketType::ClusterConfigOnly;
        auto [err2, bucket2] = allocateBucket("foo");
        EXPECT_EQ(cb::engine_errc::success, err2);
        EXPECT_EQ(bucket, bucket2);
        // And the state should not have been changed
        EXPECT_EQ(Bucket::State::Ready, bucket->state);
    }

    // We should be able to create up to the max number of buckets:
    // Slot 0 == no bucket and slot 1 is what I created above
    for (std::size_t ii = 2; ii < all_buckets.size(); ++ii) {
        auto [e, b] = allocateBucket(std::to_string(ii));
        ASSERT_EQ(cb::engine_errc::success, e);
        validateAllocatedBucket(std::to_string(ii), b);
    }

    // The bucket array is full and trying to allocate a new slot should
    // fail
    auto [e, b] = allocateBucket("fail");
    ASSERT_EQ(cb::engine_errc::too_big, e);
    ASSERT_EQ(nullptr, b);
}

/// Verify that we set bucket transitions
TEST_F(BucketManagerTest, CreateBucket) {
    bool creating = false;
    bool initializing = false;
    bool ready = false;
    bucketStateChangeListenerFunc = [&creating, &initializing, &ready](
                                            Bucket& bucket,
                                            Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Stopping:
        case Bucket::State::Destroying:
            FAIL() << "Unexpected callback";
        case Bucket::State::Creating:
            creating = true;
            EXPECT_STREQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Unknown, bucket.type);
            EXPECT_EQ(Bucket::State::None, bucket.state);
            EXPECT_FALSE(bucket.hasEngine());
            EXPECT_EQ(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supportedFeatures.empty());
            EXPECT_TRUE(bucket.management_operation_in_progress);
            break;
        case Bucket::State::Initializing:
            initializing = true;
            EXPECT_STREQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::Unknown, bucket.type);
            EXPECT_EQ(Bucket::State::Creating, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supportedFeatures.empty());
            EXPECT_TRUE(bucket.management_operation_in_progress);
            break;
        case Bucket::State::Ready:
            ready = true;
            EXPECT_STREQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::NoBucket, bucket.type);
            EXPECT_EQ(Bucket::State::Initializing, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
            EXPECT_FALSE(bucket.management_operation_in_progress);
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::success, err);
    EXPECT_TRUE(creating) << "Expected callback for state Creating";
    EXPECT_TRUE(initializing) << "Expected callback for state Initializing";
    EXPECT_TRUE(ready) << "Expected callback for state Ready";
}

/// Verify the that everything is good to go before trying to set the bucket
/// type when promoting a cluster config only bucket to a real bucket
TEST_F(BucketManagerTest, PromoteClusterConfigOnlyBucket) {
    auto [e, b] = allocateBucket("mybucket");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, b);
    b->type = BucketType::ClusterConfigOnly;
    b->state = Bucket::State::Ready;
    b->management_operation_in_progress = false;

    bool typecallback = false;
    bucketTypeChangeListenerFunc = [&typecallback](Bucket& bucket,
                                                   BucketType type) {
        typecallback = true;
        EXPECT_STREQ("mybucket", bucket.name);
        EXPECT_EQ(Bucket::State::Ready, bucket.state);
        EXPECT_TRUE(bucket.hasEngine());
        EXPECT_NE(nullptr, bucket.getDcpIface());
        EXPECT_EQ(default_max_item_size, bucket.max_document_size);
        EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
        EXPECT_TRUE(bucket.management_operation_in_progress);
        EXPECT_EQ(BucketType::ClusterConfigOnly, bucket.type);
    };

    bool ready = false;
    bucketStateChangeListenerFunc = [&ready](Bucket& bucket,
                                             Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Stopping:
        case Bucket::State::Destroying:
        case Bucket::State::Creating:
        case Bucket::State::Initializing:
            FAIL() << "Unexpected callback";
            break;
        case Bucket::State::Ready:
            ready = true;
            EXPECT_STREQ("mybucket", bucket.name);
            EXPECT_EQ(BucketType::NoBucket, bucket.type);
            EXPECT_EQ(Bucket::State::Ready, bucket.state);
            EXPECT_TRUE(bucket.hasEngine());
            EXPECT_NE(nullptr, bucket.getDcpIface());
            EXPECT_EQ(default_max_item_size, bucket.max_document_size);
            EXPECT_TRUE(bucket.supports(cb::engine::Feature::Collections));
            EXPECT_FALSE(bucket.management_operation_in_progress);
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::success, err);
    EXPECT_TRUE(ready) << "Expected a callback for enter ready state";
    EXPECT_TRUE(typecallback) << "Expected callback for setting type";
}

/// Verify that we roll a failed bucket creation into a bucket deletion
TEST_F(BucketManagerTest, FailingPromoteClusterConfigOnlyBucket) {
    auto [e, b] = allocateBucket("mybucket");
    ASSERT_EQ(cb::engine_errc::success, e);
    ASSERT_NE(nullptr, b);
    b->type = BucketType::ClusterConfigOnly;
    b->state = Bucket::State::Ready;
    b->management_operation_in_progress = false;

    bucketTypeChangeListenerFunc = [](Bucket& bucket, BucketType type) {
        throw std::bad_alloc();
    };

    bool destroying = false;
    bucketStateChangeListenerFunc = [&destroying](Bucket& bucket,
                                                  Bucket::State state) {
        switch (state) {
        case Bucket::State::None:
        case Bucket::State::Stopping:
        case Bucket::State::Creating:
        case Bucket::State::Initializing:
        case Bucket::State::Ready:
            FAIL() << "Unexpected callback";
            break;
        case Bucket::State::Destroying:
            destroying = true;
            break;
        }
    };
    auto err = create(1, "mybucket", {}, BucketType::NoBucket);
    EXPECT_EQ(cb::engine_errc::not_stored, err);
    EXPECT_TRUE(destroying) << "Expected a state callback to destroying";
}
