/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "bucket_destroyer.h"
#include "bucket_manager.h"
#include "buckets.h"
#include "front_end_thread.h"
#include "resource_allocation_domain.h"
#include "settings.h"
#include "tests/mcbp/mcbp_mock_connection.h"
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>
#include <future>
#include <limits>

TEST(BucketTest, Reset) {
    // Attempt to spot when new members are added to the Bucket and the reset()
    // method and/or this test has not been updated.
    // The Bucket size varies depending on arch / platform ABI alignment rules,
    // check the main ones we run against.
    static constexpr size_t expectedBucketSize =
#if defined(__linux) && defined(__x86_64__)
            5792;
#elif defined(__APPLE__)
            5888;
#else
            0;
#endif
    if (expectedBucketSize) {
        ASSERT_EQ(expectedBucketSize, sizeof(Bucket))
                << "Bucket size changed, the reset test must be updated with "
                   "the new members";
    }

    class MockBucket : public Bucket {
    public:
        MockBucket(std::size_t idx) : Bucket(idx) {
        }
        void testReset() {
            throttle_gauge.increment(5);
            throttle_reserved = 1;
            throttle_hard_limit = 1;
            num_throttled = 1;
            num_commands = 1;
            num_commands_with_metered_units = 1;
            num_metered_dcp_messages = 1;
            num_rejected = 1;
            read_units_used = 1;
            write_units_used = 1;
            data_ingress_status = cb::mcbp::Status::Einval;
            pause_cancellation_source = folly::CancellationSource{};

            reset();
            EXPECT_EQ(0, throttle_reserved);
            EXPECT_EQ(std::numeric_limits<std::size_t>::max(),
                      throttle_hard_limit);
            EXPECT_EQ(0, num_throttled);
            EXPECT_EQ(0, num_commands);
            EXPECT_EQ(0, num_commands_with_metered_units);
            EXPECT_EQ(0, num_metered_dcp_messages);
            EXPECT_EQ(0, num_rejected);
            EXPECT_EQ(0, read_units_used);
            EXPECT_EQ(0, write_units_used);
            EXPECT_EQ(0, throttle_gauge.getValue());
            EXPECT_EQ(cb::mcbp::Status::Success, data_ingress_status);
            EXPECT_FALSE(pause_cancellation_source.canBeCancelled());
        }
    };
    MockBucket bucket(0);

    bucket.testReset();
}

/// A mock connection which allows the test to flip the "Unthrottled"
/// privilege without having to set up an entire privilege context.
class ThrottleMockConnection : public McbpMockConnection {
public:
    using McbpMockConnection::McbpMockConnection;

    void setSubjectToThrottling(bool value) {
        subject_to_throttling.store(value, std::memory_order_release);
    }
};

class BucketThrottleTest : public ::testing::Test, public Bucket {
public:
    BucketThrottleTest() : Bucket(1) {
    }

protected:
    static constexpr auto Unlimited = std::numeric_limits<std::size_t>::max();

    void SetUp() override {
        origThrottleEnabled = Settings::instance().isThrottleEnabled();
        origNodeCapacity = Settings::instance().getNodeCapacity();
        Settings::instance().setThrottleEnabled(true);
    }

    void TearDown() override {
        Settings::instance().setThrottleEnabled(origThrottleEnabled);
        Settings::instance().setNodeCapacity(origNodeCapacity);
        // Reset free pool as well
        BucketManager::instance().tick();
    }

    /// Set the size of the global (unassigned) resource pool to the provided
    /// limit, then consume the provided number of units from it.
    void setFreePool(std::size_t limit, std::size_t used) {
        Settings::instance().setNodeCapacity(limit);
        // tick() recalculates the free pool limit from the node capacity
        // (minus the sum of all bucket reservations) and resets its gauge.
        BucketManager::instance().tick();
        BucketManager::instance().consumedResources(used);
    }

    /// Configure the bucket with the provided limits and push its usage to
    /// `used` units.
    void setLimits(std::size_t reserved,
                   std::size_t hardLimit,
                   std::size_t used) {
        ASSERT_EQ(cb::engine_errc::success,
                  setThrottleLimits(reserved, hardLimit));
        throttle_gauge.increment(used);
    }

    FrontEndThread thread;
    ThrottleMockConnection connection{thread};

private:
    bool origThrottleEnabled = false;
    std::size_t origNodeCapacity = Unlimited;
};

/**
 * The bucket is above its reserved limit, below its (configured) hard limit,
 * and the global free pool is exhausted.
 *
 * Everything a bucket uses above its reservation has to be funded by the
 * free pool, so with an empty pool the command is throttled even though the
 * bucket is below its hard limit.
 */
TEST_F(BucketThrottleTest, AboveReservedBelowHardLimitWithExhaustedFreePool) {
    setFreePool(1000, 1000);
    ASSERT_FALSE(BucketManager::instance().isUnassignedResourcesAvailable(1))
            << "The free pool should be exhausted";

    setLimits(100, 500, 200);
    ASSERT_FALSE(throttle_gauge.isBelow(throttle_reserved, 1))
            << "The bucket should be above its reserved limit";
    ASSERT_TRUE(throttle_gauge.isBelow(throttle_hard_limit, 1))
            << "The bucket should be below its hard limit";

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_TRUE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::None, domain);
}

/// The same bucket as above, but with units left in the free pool: it may
/// continue, and the allocation comes out of the Global domain.
TEST_F(BucketThrottleTest, AboveReservedBelowHardLimitWithFreePoolAvailable) {
    setFreePool(1000, 500);
    ASSERT_TRUE(BucketManager::instance().isUnassignedResourcesAvailable(1));

    setLimits(100, 500, 200);

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_FALSE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::Global, domain);
}

/// A bucket without a hard limit behaves the same way once it is above its
/// reservation: an exhausted free pool means it gets throttled.
TEST_F(BucketThrottleTest, AboveReservedNoHardLimitWithExhaustedFreePool) {
    setFreePool(1000, 1000);
    ASSERT_FALSE(BucketManager::instance().isUnassignedResourcesAvailable(1));

    setLimits(100, Unlimited, 200);

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_TRUE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::None, domain);
}

/// A bucket without a hard limit which is above its reserved limit may run as
/// long as there are units left in the free pool.
TEST_F(BucketThrottleTest, AboveReservedNoHardLimitWithFreePoolAvailable) {
    setFreePool(1000, 500);
    ASSERT_TRUE(BucketManager::instance().isUnassignedResourcesAvailable(1));

    setLimits(100, Unlimited, 200);

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_FALSE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::Global, domain);
}

/// A bucket below its reserved limit is never throttled, even if the free
/// pool is exhausted.
TEST_F(BucketThrottleTest, BelowReservedWithExhaustedFreePool) {
    setFreePool(1000, 1000);
    ASSERT_FALSE(BucketManager::instance().isUnassignedResourcesAvailable(1));

    setLimits(100, 500, 50);

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_FALSE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::Bucket, domain);
}

/// The hard limit is a hard cap: once the bucket reaches it, it gets throttled
/// even when the free pool has plenty of units left.
TEST_F(BucketThrottleTest, AtHardLimitWithFreePoolAvailable) {
    setFreePool(1000, 0);
    ASSERT_TRUE(BucketManager::instance().isUnassignedResourcesAvailable(1));

    setLimits(100, 500, 500);
    ASSERT_FALSE(throttle_gauge.isBelow(throttle_hard_limit, 1));

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_TRUE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::None, domain);
}

/// Every Global allocation is charged to both gauges. Units used above
/// reservation should utilise the free pool but also consume the bucket's
/// units so the hard_limit can be reached
TEST_F(BucketThrottleTest, AboveReservedConsumesTheFreePool) {
    constexpr std::size_t freePool = 1000;
    setFreePool(freePool, 0);
    setLimits(100, 500, 200);

    ASSERT_TRUE(BucketManager::instance().isUnassignedResourcesAvailable(
            freePool - 1))
            << "The free pool should be unused";

    const auto [throttle, domain] = shouldThrottle(connection, 10);
    ASSERT_FALSE(throttle);
    ASSERT_EQ(ResourceAllocationDomain::Global, domain);

    consumedUnits(10, domain);
    EXPECT_EQ(210, throttle_gauge.getValue())
            << "The units should be accounted for in the bucket gauge";
    // The pair checks exactly 10 units were taken from free pool
    EXPECT_TRUE(BucketManager::instance().isUnassignedResourcesAvailable(
            freePool - 11));
    EXPECT_FALSE(BucketManager::instance().isUnassignedResourcesAvailable(
            freePool - 10))
            << "The 10 units should have been taken from the free pool";
}

/// Check below reservation is only taken from the bucket domain;
/// above it is consumed from the bucket and global domain
TEST_F(BucketThrottleTest, HardLimitIsReachableViaTheFreePool) {
    constexpr std::size_t reserved = 100;
    constexpr std::size_t hardLimit = 500;
    setFreePool(100000, 0);
    setLimits(reserved, hardLimit, 0);

    std::size_t fromBucket = 0;
    std::size_t fromPool = 0;
    bool throttled = false;
    for (std::size_t ii = 0; ii < 10 * hardLimit && !throttled; ++ii) {
        const auto [throttle, domain] = shouldThrottle(connection, 1);
        if (throttle) {
            EXPECT_EQ(ResourceAllocationDomain::None, domain);
            throttled = true;
            break;
        }
        if (domain == ResourceAllocationDomain::Bucket) {
            ++fromBucket;
        } else if (domain == ResourceAllocationDomain::Global) {
            ++fromPool;
        } else {
            FAIL() << "Unexpected domain for an allowed operation: " << domain;
        }
        consumedUnits(1, domain);
    }

    ASSERT_TRUE(throttled) << "The bucket should eventually be throttled";
    EXPECT_EQ(reserved - 1, fromBucket)
            << "The reservation should be spent from the Bucket domain";
    EXPECT_EQ(hardLimit - reserved, fromPool)
            << "The rest of the way to the hard limit should come from the "
               "free pool";
    EXPECT_EQ(hardLimit - 1, throttle_gauge.getValue())
            << "The bucket gauge must climb all the way to the hard limit";
}

/// Do not throttle if throttling is disabled
TEST_F(BucketThrottleTest, ThrottlingDisabled) {
    Settings::instance().setThrottleEnabled(false);
    setFreePool(1000, 1000);
    setLimits(100, 500, 500);

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_FALSE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::None, domain);
}

/// A connection holding the Unthrottled privilege bypasses all of the checks.
TEST_F(BucketThrottleTest, UnthrottledConnection) {
    setFreePool(1000, 1000);
    setLimits(100, 500, 500);
    connection.setSubjectToThrottling(false);
    ASSERT_TRUE(connection.isUnthrottled());

    const auto [throttle, domain] = shouldThrottle(connection, 1);
    EXPECT_FALSE(throttle);
    EXPECT_EQ(ResourceAllocationDomain::None, domain);
}
