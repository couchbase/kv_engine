/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <nlohmann/json.hpp>

// Test fixture for new MCBP miscellaneous commands
class MiscTest : public TestappClientTest {};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        MiscTest,
                        ::testing::Values(TransportProtocols::McbpPlain,
                                          TransportProtocols::McbpSsl),
                        ::testing::PrintToStringParamName());

TEST_P(MiscTest, GetFailoverLog) {
    TESTAPP_SKIP_IF_UNSUPPORTED(PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG);

    auto& connection = getConnection();

    // Test existing VBucket
    auto response = connection.getFailoverLog(0 /*vbid*/);
    auto header = response.getHeader().response;
    EXPECT_EQ(header.magic, PROTOCOL_BINARY_RES);
    EXPECT_EQ(header.opcode, PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG);
    EXPECT_EQ(header.keylen, 0);
    EXPECT_EQ(header.extlen, 0);
    EXPECT_EQ(header.datatype, 0);
    EXPECT_EQ(header.status, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    // Note: We expect 1 entry in the failover log, which is the entry created
    // at VBucket creation (8 bytes for UUID + 8 bytes for SEQNO)
    EXPECT_EQ(ntohl(header.bodylen), 0x10);
    EXPECT_EQ(header.cas, 0);
    EXPECT_EQ(response.getData().len, 0x10);

    // Test non-existing VBucket
    response = connection.getFailoverLog(1 /*vbid*/);
    header = response.getHeader().response;
    EXPECT_EQ(header.magic, PROTOCOL_BINARY_RES);
    EXPECT_EQ(header.opcode, PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG);
    EXPECT_EQ(header.keylen, 0);
    EXPECT_EQ(header.extlen, 0);
    EXPECT_EQ(header.datatype, 0);
    EXPECT_EQ(header.status, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    EXPECT_EQ(ntohl(header.bodylen), 0);
    EXPECT_EQ(header.cas, 0);
}

TEST_P(MiscTest, GetActiveUsers) {
    auto& conn = getConnection();
    auto clone1 = conn.clone();
    auto clone2 = conn.clone();
    auto clone3 = conn.clone();
    auto clone4 = conn.clone();

    clone1->authenticate("smith", "smithpassword", "PLAIN");
    clone2->authenticate("smith", "smithpassword", "PLAIN");
    clone3->authenticate("jones", "jonespassword", "PLAIN");
    clone4->authenticate("@admin", "password", "PLAIN");

    conn = getAdminConnection();
    BinprotCommand cmd;
    cmd.setOp(uint8_t(cb::mcbp::ClientOpcode::GetActiveExternalUsers));
    BinprotResponse resp;

    conn.executeCommand(cmd, resp);
    EXPECT_TRUE(resp.isSuccess());
    // We don't have any external users to test with for now.. will add
    // them after we've added support for external auth through our mock
    // server
    EXPECT_EQ("[]", resp.getDataString());
}

/**
 * Send the UpdateUserPermissions with a valid username and paylaod.
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, UpdateUserPermissionsSuccess) {
    const std::string rbac = R"(
{"johndoe" : {
  "domain" : "external",
  "buckets": {
    "default": ["Read","SimpleStats","Insert","Delete","Upsert"]
  },
  "privileges": []
}})";
    auto& conn = getAdminConnection();
    auto resp =
            conn.execute(BinprotUpdateUserPermissionsCommand{"johndoe", rbac});
    EXPECT_TRUE(resp.isSuccess());
}

/**
 * Send the UpdateUserPermissions with a valid username, but no payload
 * (this means remove).
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, UpdateUserPermissionsRemoveUser) {
    auto& conn = getAdminConnection();
    auto resp =
            conn.execute(BinprotUpdateUserPermissionsCommand{"johndoe", ""});
    EXPECT_TRUE(resp.isSuccess());
}

/**
 * Send the UpdateUserPermissions with a valid username, but invalid payload.
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, UpdateUserPermissionsInvalidPayload) {
    auto& conn = getAdminConnection();
    auto resp = conn.execute(
            BinprotUpdateUserPermissionsCommand{"johndoe", "bogus"});
    EXPECT_FALSE(resp.isSuccess());
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, resp.getStatus());
}
