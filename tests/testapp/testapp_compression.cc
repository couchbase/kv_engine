/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"
#include "testapp_environment.h"
#include <protocol/mcbp/ewb_encode.h>
#include <cctype>
#include <limits>
#include <thread>

using namespace std::string_literals;

class CompressionTest : public TestappXattrClientTest {
protected:
    void waitForCompression() {
        bool compressed = false;
        using std::chrono::steady_clock;
        const auto timeout = steady_clock::now() + std::chrono::seconds{10};
        while (steady_clock::now() < timeout) {
            userConnection->stats(
                    [&compressed](auto& k, auto& v) {
                        if (k == "key_datatype"s) {
                            if (v.find("snappy") != std::string::npos) {
                                compressed = true;
                            }
                        }
                    },
                    fmt::format("key {} 0", name));
            if (compressed) {
                break;
            } else {
                // Don't busy-wait. back off and try again
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
        ASSERT_TRUE(compressed)
                << "Timed out waiting for the document to be compressed";
    }
};

INSTANTIATE_TEST_SUITE_P(
        TransportProtocols,
        CompressionTest,
        ::testing::Combine(::testing::Values(TransportProtocols::McbpSsl),
                           ::testing::Values(XattrSupport::Yes),
                           ::testing::Values(ClientJSONSupport::Yes),
                           ::testing::Values(ClientSnappySupport::Yes)),
        PrintToStringCombinedName());

/// Verify that increment works on a compressed document which contains extended
/// attributes
TEST_P(CompressionTest, MB57640) {
    TESTAPP_SKIP_FOR_OTHER_BUCKETS(BucketType::Couchbase);
    mcd_env->getTestBucket().setCompressionMode(
            *adminConnection, bucketName, "active");

    EXPECT_EQ(0, userConnection->increment(name, 1));

    const std::string xattr_key = "meta.value";
    std::string xattr_value;
    xattr_value.resize(4096, 'z');
    xattr_value.front() = '"';
    xattr_value.back() = '"';

    // Add an attribute shich should be big enough to the document
    // to be compressed
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictAdd);
        cmd.setKey(name);
        cmd.setPath(xattr_key);
        cmd.setValue(xattr_value);
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
        const auto resp = userConnection->execute(cmd);
        ASSERT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
    }

    // Wait for the document to become compressed
    waitForCompression();

    // Increment the counter
    EXPECT_EQ(1, userConnection->increment(name, 1));

    // Validate that the XATTRs is still intact
    {
        BinprotSubdocCommand cmd;
        cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
        cmd.setKey(name);
        cmd.setPath(xattr_key);
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        userConnection->sendCommand(cmd);
        BinprotSubdocResponse resp;
        userConnection->recvResponse(resp);
        ASSERT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
        EXPECT_EQ(xattr_value, resp.getValue());
    }
}
