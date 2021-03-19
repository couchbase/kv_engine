/*
 *     Copyright 2021 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
    auto& conn = getAdminConnection();
    auto rsp = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet, "release_free_memory"});
    ASSERT_TRUE(rsp.isSuccess());
}

TEST_P(IoctlTest, IOCTL_Tracing) {
    auto& conn = getAdminConnection();
    conn.authenticate("@admin", "password", "PLAIN");

    // Disable trace so that we start from a known status
    conn.ioctl_set("trace.stop", {});

    // Ensure that trace isn't running
    auto value = conn.ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // Specify config
    const std::string config{
            "buffer-mode:ring;buffer-size:2000000;"
            "enabled-categories:*"};
    conn.ioctl_set("trace.config", config);

    // Try to read it back and check that setting the config worked
    // Phosphor rebuilds the string and adds the disabled categories
    EXPECT_EQ(config + ";disabled-categories:", conn.ioctl_get("trace.config"));

    // Start the trace
    conn.ioctl_set("trace.start", {});

    // Ensure that it's running
    value = conn.ioctl_get("trace.status");
    EXPECT_EQ("enabled", value);

    // Stop the tracing
    conn.ioctl_set("trace.stop", {});

    // Ensure that it stopped
    value = conn.ioctl_get("trace.status");
    EXPECT_EQ("disabled", value);

    // get the data
    auto uuid = conn.ioctl_get("trace.dump.begin");

    const std::string chunk_key = "trace.dump.get?id=" + uuid;
    std::string dump = conn.ioctl_get(chunk_key);
    conn.ioctl_set("trace.dump.clear", uuid);

    // Difficult to tell what's been written to the buffer so just check
    // that it's valid JSON and that the traceEvents array is present
    auto json = nlohmann::json::parse(dump);
    EXPECT_TRUE(json["traceEvents"].is_array());
}
