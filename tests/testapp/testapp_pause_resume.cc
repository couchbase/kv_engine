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

#include <protocol/connection/client_mcbp_commands.h>

#include <folly/portability/GMock.h>

class PauseResumeTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         PauseResumeTest,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(PauseResumeTest, DISABLED_Basic) {
    auto writeDoc = [](MemcachedConnection& conn) {
        Document doc;
        doc.info.id = "mydoc";
        doc.value = "This is the value";
        conn.mutate(doc, Vbid{0}, MutationType::Set);
    };
    auto getBucketInformation = [this]() {
        nlohmann::json stats;
        adminConnection->stats(
                [&stats](auto k, auto v) { stats = nlohmann::json::parse(v); },
                "bucket_details " + bucketName);
        return stats;
    };

    // verify the state of the bucket
    EXPECT_EQ("ready", getBucketInformation()["state"]);

    // store a document
    writeDoc(*userConnection);

    // Pause the bucket
    auto rsp = adminConnection->execute(BinprotPauseBucketCommand{bucketName});
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus()) << rsp.getDataView();

    // Verify that it is in the expected state
    EXPECT_EQ("paused", getBucketInformation()["state"]);

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
    EXPECT_EQ("ready", getBucketInformation()["state"]);

    // succeed to store a document
    rebuildUserConnection(true);
    writeDoc(*userConnection);
}
