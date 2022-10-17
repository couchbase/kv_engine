/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp_client_test.h"

#include <fmt/format.h>
#include <folly/portability/GMock.h>
#include <platform/timeutils.h>
#include <protocol/connection/client_mcbp_commands.h>

using namespace std::string_literals;

class PauseResumeTest : public TestappClientTest {
protected:
    nlohmann::json getBucketInformation(std::string_view bucket) {
        nlohmann::json stats;
        auto statName = fmt::format("bucket_details {}", bucket);
        adminConnection->stats(
                [&stats](auto k, auto v) { stats = nlohmann::json::parse(v); },
                statName);
        return stats;
    };

    bool waitUntilBucketStateIs(std::string_view bucket,
                                std::string_view stateName,
                                std::chrono::seconds deadline) {
        // Pause is non-blocking, need to wait for it to complete.
        return cb::waitForPredicateUntil(
                [&] {
                    return getBucketInformation(bucket)["state"] ==
                           std::string{stateName};
                },
                deadline);
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         PauseResumeTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(PauseResumeTest, Basic) {
    auto writeDoc = [](MemcachedConnection& conn) {
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    };
    // verify the state of the bucket
    EXPECT_EQ("ready", getBucketInformation(bucketName)["state"]);

    // store a document
    writeDoc(*userConnection);

    // Pause the bucket
    auto rsp = adminConnection->execute(BinprotPauseBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();

    // Pause is non-blocking, need to wait for it to complete.
    waitUntilBucketStateIs(bucketName, "paused", std::chrono::seconds{10});

    // Verify that it is in the expected state
    EXPECT_EQ("paused", getBucketInformation(bucketName)["state"]);

    // Verify that we can't store documents when we're in that state
    try {
        writeDoc(*userConnection);
        FAIL() << "Should not be able to store a document after pause";
    } catch (const ConnectionError& error) {
        FAIL() << "Connection should have been closed instead of an error: "
               << error.what();
    } catch (const std::runtime_error& e) {
        // Expected
    }

    // resume the bucket
    rsp = adminConnection->execute(BinprotResumeBucketCommand{bucketName});
    EXPECT_TRUE(rsp.isSuccess());

    // Verify that it is in the expected state
    EXPECT_EQ("ready", getBucketInformation(bucketName)["state"]);

    // succeed to store a document
    rebuildUserConnection(true);
    writeDoc(*userConnection);
}

/// Can Delete a bucket when paused.
TEST_P(PauseResumeTest, DeleteWhenPaused) {
    // Need a new bucket as we are going to delete it in the test (and
    // TearDownTestSuite expects the bucket to exist at the end of the test).
    auto testBucket = "pause_test_bucket"s;
    mcd_env->getTestBucket().setUpBucket(testBucket, "", *adminConnection);

    // Pause the bucket
    auto rsp = adminConnection->execute(BinprotPauseBucketCommand{testBucket});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();
    EXPECT_TRUE(waitUntilBucketStateIs(
            testBucket, "paused", std::chrono::seconds{10}))
            << getBucketInformation(testBucket)["state"];

    // Delete should succeed.
    adminConnection->deleteBucket(testBucket);
}

/// Cannot Pause a bucket when already paused.
TEST_P(PauseResumeTest, PauseFailsWhenPaused) {
    // Pause the bucket
    auto rsp = adminConnection->execute(BinprotPauseBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();
    EXPECT_EQ("paused", getBucketInformation(bucketName)["state"]);

    // Trying to pause again should fail.
    rsp = adminConnection->execute(BinprotPauseBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::BucketPaused, rsp.getStatus())
            << rsp.getDataView();
    EXPECT_EQ("paused", getBucketInformation(bucketName)["state"]);

    // Cleanup
    rsp = adminConnection->execute(BinprotResumeBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();
    rebuildUserConnection(true);
}

/// Cannot Resume if a Paused bucket has already been resumed.
TEST_P(PauseResumeTest, ResumeFailsWhenAlreadyResumed) {
    // Pause the bucket
    auto rsp = adminConnection->execute(BinprotPauseBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();

    // Resume the Bucket.
    rsp = adminConnection->execute(BinprotResumeBucketCommand{bucketName});
    ASSERT_TRUE(rsp.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();
    ASSERT_EQ("ready", getBucketInformation(bucketName)["state"]);
    rebuildUserConnection(true);

    // Attempting to resume again should fail.
    rsp = adminConnection->execute(BinprotResumeBucketCommand{bucketName});
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus())
            << rsp.getDataView();
    EXPECT_EQ("ready", getBucketInformation(bucketName)["state"]);
}

/// Cannot Resume unless the bucket is paused.
TEST_P(PauseResumeTest, ResumeFailsWhenNotPaused) {
    // Attempting to resume a bucket which has never been paused should fail.
    auto rsp = adminConnection->execute(BinprotResumeBucketCommand{bucketName});
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus())
            << rsp.getDataView();
    EXPECT_EQ("ready", getBucketInformation(bucketName)["state"]);
}
