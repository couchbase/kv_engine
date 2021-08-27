/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp.h"
#include "testapp_client_test.h"

/*
 * This test batch verifies that the interface array in the server may
 * be dynamically changed.
 */

class IoctlTest : public TestappClientTest {};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         IoctlTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(IoctlTest, IOCTL_Set) {
    // release_free_memory always returns OK, regardless of how much was
    // freed.
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet, "release_free_memory"});
    ASSERT_TRUE(rsp.isSuccess());
}

TEST_P(IoctlTest, IOCTL_Tracing) {
    // Disable trace so that we start from a known status
    adminConnection->ioctl_set("trace.stop", {});

    // Ensure that trace isn't running
    auto value = adminConnection->ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // Specify config
    const std::string config{
            "buffer-mode:ring;buffer-size:2000000;"
            "enabled-categories:*"};
    adminConnection->ioctl_set("trace.config", config);

    // Try to read it back and check that setting the config worked
    // Phosphor rebuilds the string and adds the disabled categories
    EXPECT_EQ(config + ";disabled-categories:",
              adminConnection->ioctl_get("trace.config"));

    // Start the trace
    adminConnection->ioctl_set("trace.start", {});

    // Ensure that it's running
    value = adminConnection->ioctl_get("trace.status");
    EXPECT_EQ("enabled", value);

    // Stop the tracing
    adminConnection->ioctl_set("trace.stop", {});

    // Ensure that it stopped
    value = adminConnection->ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // get the data
    auto uuid = adminConnection->ioctl_get("trace.dump.begin");

    const std::string chunk_key = "trace.dump.get?id=" + uuid;
    std::string dump = adminConnection->ioctl_get(chunk_key);
    adminConnection->ioctl_set("trace.dump.clear", uuid);

    // Difficult to tell what's been written to the buffer so just check
    // that it's valid JSON and that the traceEvents array is present
    auto json = nlohmann::json::parse(dump);
    EXPECT_TRUE(json["traceEvents"].is_array());
}
