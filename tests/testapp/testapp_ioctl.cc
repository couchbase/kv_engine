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

    ASSERT_TRUE(json.contains("clock_information"));
    ASSERT_TRUE(json["clock_information"].contains("ts"));
    EXPECT_TRUE(json["clock_information"]["ts"].is_number_float());
    EXPECT_TRUE(json["clock_information"].contains("system_clock"));
    EXPECT_TRUE(json["clock_information"]["system_clock"].is_string());
}

TEST_P(IoctlTest, IOCTL_ServerlessMaxConnectionsPerBucket) {
    // The command may only be used in serverless deployments
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlSet,
                                  "serverless.max_connections_per_bucket",
                                  "1000"});
    ASSERT_FALSE(rsp.isSuccess());
}

/// Helper: return the socket file descriptor of the current admin connection
/// by running "connections self" and extracting the "socket" field.
static size_t getAdminConnectionSocket(MemcachedConnection& conn) {
    auto stats = conn.stats("connections self");
    EXPECT_EQ(1, stats.size());
    return stats.front()["socket"].get<size_t>();
}

// -----------------------------------------------------------------------
// connection.rcvbufsize – GET
// -----------------------------------------------------------------------

TEST_P(IoctlTest, IOCTL_GetConnectionRcvBufSize_Success) {
    const auto socketId = getAdminConnectionSocket(*adminConnection);
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet,
            fmt::format("connection.rcvbufsize?id={}", socketId)});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();

    // The returned value must be a positive integer (receive buffer size).
    const auto value = std::string(rsp.getDataView());
    EXPECT_FALSE(value.empty());
    const auto bufSize = std::stoul(value);
    EXPECT_GT(bufSize, 0u);
}

TEST_P(IoctlTest, IOCTL_GetConnectionRcvBufSize_MissingId) {
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet, "connection.rcvbufsize"});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}

TEST_P(IoctlTest, IOCTL_GetConnectionRcvBufSize_InvalidId) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlGet,
                                  "connection.rcvbufsize?id=not-a-number"});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}

// -----------------------------------------------------------------------
// connection.rcvbufsize – SET
// -----------------------------------------------------------------------

TEST_P(IoctlTest, IOCTL_SetConnectionRcvBufSize_Success) {
    const auto socketId = getAdminConnectionSocket(*adminConnection);
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet,
            fmt::format("connection.rcvbufsize?id={}&size=65536", socketId)});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus() << " " << rsp.getDataView();

    // The returned value is the SO_RCVBUF reported by the OS after the set.
    const auto value = std::string(rsp.getDataView());
    EXPECT_FALSE(value.empty());
    const auto bufSize = std::stoul(value);
    EXPECT_GT(bufSize, 0u);
}

TEST_P(IoctlTest, IOCTL_SetConnectionRcvBufSize_MissingId) {
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet, "connection.rcvbufsize"});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}

TEST_P(IoctlTest, IOCTL_SetConnectionRcvBufSize_MissingSize) {
    const auto socketId = getAdminConnectionSocket(*adminConnection);
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet,
            fmt::format("connection.rcvbufsize?id={}", socketId)});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}

TEST_P(IoctlTest, IOCTL_SetConnectionRcvBufSize_InvalidId) {
    auto rsp = adminConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::IoctlSet,
                                  "connection.rcvbufsize?id=not-a-number"});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}

TEST_P(IoctlTest, IOCTL_SetConnectionRcvBufSize_InvalidSize) {
    const auto socketId = getAdminConnectionSocket(*adminConnection);
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlSet,
            fmt::format("connection.rcvbufsize?id={}&size=not-a-number",
                        socketId)});
    ASSERT_EQ(cb::mcbp::Status::Einval, rsp.getStatus()) << rsp.getDataView();
}
