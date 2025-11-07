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
#include "buckets.h"
#include "front_end_thread.h"
#include "memcached.h"
#include "tests/mcbp/mcbp_mock_connection.h"
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>
#include <future>

TEST(BucketTest, Reset) {
    // Attempt to spot when new members are added to the Bucket and the reset()
    // method and/or this test has not been updated.
    // The Bucket size varies depending on arch / platform ABI alignment rules,
    // check the main ones we run against.
    static constexpr size_t expectedBucketSize =
#if defined(__linux) && defined(__x86_64__)
            5696;
#elif defined(__APPLE__)
            5792;
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
        void testReset() {
            throttle_gauge.increment(5);
            throttle_reserved = 1;
            throttle_hard_limit = 1;
            num_throttled = 1;
            throttle_wait_time = 1;
            num_commands = 1;
            num_commands_with_metered_units = 1;
            num_metered_dcp_messages = 1;
            num_rejected = 1;
            data_ingress_status = cb::mcbp::Status::Einval;
            pause_cancellation_source = folly::CancellationSource{};

            reset();
            EXPECT_EQ(std::numeric_limits<std::size_t>::max(),
                      throttle_reserved);
            EXPECT_EQ(std::numeric_limits<std::size_t>::max(),
                      throttle_hard_limit);
            EXPECT_EQ(0, num_throttled);
            EXPECT_EQ(0, throttle_wait_time);
            EXPECT_EQ(0, num_commands);
            EXPECT_EQ(0, num_commands_with_metered_units);
            EXPECT_EQ(0, num_metered_dcp_messages);
            EXPECT_EQ(0, num_rejected);
            EXPECT_EQ(0, throttle_gauge.getValue());
            EXPECT_EQ(cb::mcbp::Status::Success, data_ingress_status);
            EXPECT_FALSE(pause_cancellation_source.canBeCancelled());
        }
    } bucket;

    bucket.testReset();
}
