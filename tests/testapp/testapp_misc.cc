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
    TESTAPP_SKIP_IF_UNSUPPORTED(cb::mcbp::ClientOpcode::GetFailoverLog);

    auto& connection = getConnection();

    // Test existing VBucket
    auto response = connection.getFailoverLog(Vbid(0));
    auto header = response.getResponse();
    EXPECT_EQ(cb::mcbp::Magic::ClientResponse, header.getMagic());
    EXPECT_EQ(cb::mcbp::ClientOpcode::GetFailoverLog, header.getClientOpcode());
    EXPECT_EQ(0, header.getKeylen());
    EXPECT_EQ(0, header.getExtlen());
    EXPECT_EQ(cb::mcbp::Datatype::Raw, header.getDatatype());
    EXPECT_EQ(header.getStatus(), cb::mcbp::Status::Success);
    // Note: We expect 1 entry in the failover log, which is the entry created
    // at VBucket creation (8 bytes for UUID + 8 bytes for SEQNO)
    EXPECT_EQ(0x10, header.getBodylen());
    EXPECT_EQ(0, header.getCas());
    EXPECT_EQ(response.getData().len, 0x10);

    // Test non-existing VBucket
    response = connection.getFailoverLog(Vbid(1));
    header = response.getResponse();
    EXPECT_EQ(cb::mcbp::Magic::ClientResponse, header.getMagic());
    EXPECT_EQ(cb::mcbp::ClientOpcode::GetFailoverLog, header.getClientOpcode());
    EXPECT_EQ(0, header.getKeylen());
    EXPECT_EQ(0, header.getExtlen());
    EXPECT_EQ(cb::mcbp::Datatype::Raw, header.getDatatype());
    EXPECT_EQ(header.getStatus(), cb::mcbp::Status::NotMyVbucket);
    EXPECT_EQ(0, header.getBodylen());
    EXPECT_EQ(0, header.getCas());
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
    auto resp = conn.execute(BinprotUpdateUserPermissionsCommand{rbac});
    EXPECT_TRUE(resp.isSuccess());
}

/**
 * Send the UpdateUserPermissions with a valid username, but no payload
 * (this means remove).
 *
 * Unfortunately there isn't a way to verify that the user was actually
 * updated as we can't fetch the updated entry.
 */
TEST_P(MiscTest, DISABLED_UpdateUserPermissionsRemoveUser) {
    auto& conn = getAdminConnection();
    auto resp = conn.execute(BinprotUpdateUserPermissionsCommand{""});
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
    auto resp = conn.execute(BinprotUpdateUserPermissionsCommand{"bogus"});
    EXPECT_FALSE(resp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

/**
 * Create a basic test to verify that the ioctl to fetch the database
 * works. Once we add support for modifying the RBAC database we'll
 * add tests to verify the content
 */
TEST_P(MiscTest, GetRbacDatabase) {
    auto& conn = getAdminConnection();
    auto response = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet, "rbac.db.dump?domain=external"});
    ASSERT_TRUE(response.isSuccess());
    ASSERT_FALSE(response.getDataString().empty());

    conn = getConnection();
    response = conn.execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::IoctlGet, "rbac.db.dump?domain=external"});
    ASSERT_FALSE(response.isSuccess());
    ASSERT_EQ(cb::mcbp::Status::Eaccess, response.getStatus());
}
