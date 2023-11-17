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
#include "memcached.h"
#include "memcached/engine.h"
#include "engines/nobucket/nobucket_public.h"
#include "stats.h"
#include <folly/portability/GTest.h>

class BucketManagerTest : public ::testing::Test, public BucketManager {
};

class BucketDestroyerIntrospector {
public:
    static void setNextLogConnections(
            BucketDestroyer& destroyer,
            std::chrono::steady_clock::time_point tp) {
        destroyer.nextLogConnections = tp;
    }
};

// Destroy bucket with no clients.
TEST_F(BucketManagerTest, DestroyBucketTest) {
    auto& bucket = all_buckets.back();
    bucket.setEngine(create_no_bucket_instance());
    BucketDestroyer destroyer(bucket, "", false);
    EXPECT_EQ(cb::engine_errc::success, destroyer.drive());
}

// We shouldn't destroy a bucket with clients.
TEST_F(BucketManagerTest, DestroyBucketWaitsForClients) {
    auto& bucket = all_buckets.back();
    bucket.setEngine(create_no_bucket_instance());
    // Add a client and verify that we wait for it.
    bucket.clients++;

    using std::chrono::steady_clock;
    using namespace std::chrono_literals;
    BucketDestroyer destroyer(bucket, "", false);
    // Cannot complete the bucket delete as we've got a client. Run twice to
    // make sure the result is the same.
    EXPECT_EQ(cb::engine_errc::would_block, destroyer.drive());
    EXPECT_EQ(cb::engine_errc::would_block, destroyer.drive());

    // "Move" the connection log time, such that on the next run it has elapsed.
    BucketDestroyerIntrospector::setNextLogConnections(
            destroyer, steady_clock::now() - 1s);

    // We should still be unable to delete the bucket. Run twice to make sure
    // the result is the same.
    EXPECT_EQ(cb::engine_errc::would_block, destroyer.drive());
    EXPECT_EQ(cb::engine_errc::would_block, destroyer.drive());

    // We should succeed in deleting now that clients == 0.
    bucket.clients--;
    EXPECT_EQ(cb::engine_errc::success, destroyer.drive());
}
