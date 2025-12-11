/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"
#include "platform/base64.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <mcbp/codec/frameinfo.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

#include <string>

using cb::mcbp::ClientOpcode;
using cb::mcbp::Status;

class JwtClusterTest : public cb::test::ClusterTest {
public:
    uint16_t registerToken(
            MemcachedConnection& connection,
            std::string_view bucket,
            nlohmann::json bucket_privileges,
            nlohmann::json user_privileges = nlohmann::json::array(),
            std::string domain = "external",
            std::string_view username = "jwt") {
        nlohmann::json rbac = nlohmann::json::object();
        rbac["buckets"][bucket]["privileges"] = std::move(bucket_privileges);
        rbac["privileges"] = std::move(user_privileges);
        rbac["domain"] = std::move(domain);

        auto builder = cb::test::AuthProviderService::getTokenBuilder(username);
        builder->addClaim("cb-rbac", cb::base64url::encode(rbac.dump()));
        builder->setExpiration(std::chrono::system_clock::now() +
                               std::chrono::minutes(1));

        nlohmann::json json = {{"id", token_id_counter},
                               {"token", builder->build()},
                               {"type", "JWT"}};
        BinprotGenericCommand cmd(
                ClientOpcode::RegisterAuthToken, {}, json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        auto rsp = connection.execute(cmd);
        if (!rsp.isSuccess()) {
            throw std::runtime_error("Failed to register token: " +
                                     rsp.getErrorContext());
        }
        return token_id_counter++;
    }

    void unregisterToken(MemcachedConnection& connection, uint16_t token) {
        nlohmann::json json = {{"id", token}};
        BinprotGenericCommand cmd(
                ClientOpcode::RegisterAuthToken, {}, json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        auto rsp = connection.execute(cmd);
        if (!rsp.isSuccess()) {
            throw std::runtime_error("Failed to unregister token: " +
                                     rsp.getErrorContext());
        }
    }

protected:
    uint16_t token_id_counter{0};
};

TEST_F(JwtClusterTest, TestPacketValidator_success) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "type":"JWT"})"_json;

    auto builder = cb::test::AuthProviderService::getTokenBuilder("jwt");
    builder->addClaim("cb-rbac", cb::base64url::encode(R"({
  "buckets": {
    "default": {
      "privileges": ["Read"]
    }
  },
  "privileges": [],
  "domain": "external"
})"));
    builder->setExpiration(std::chrono::system_clock::now() +
                           std::chrono::minutes(1));
    json["token"] = builder->build();

    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_TRUE(rsp.isSuccess());
}

TEST_F(JwtClusterTest, TestPacketValidator_NotHoldingImpersonate) {
    cluster->getAuthProviderService().upsertUser({"almighty", "bruce", R"(
{
    "buckets": {
      "*": [
        "all"
      ]
    },
    "privileges": [
      "SystemSettings",
      "Stats"
    ],
    "domain": "external"
  })"_json});

    auto conn = cluster->getBucket("default")->getConnection(Vbid{0});
    conn->authenticate("almighty", "bruce");
    auto json = R"({"id":0, "token":"test", "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
    cluster->getAuthProviderService().removeUser("almighty");
}

TEST_F(JwtClusterTest, TestPacketValidator_IncorrectDatatype) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "token":"test", "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("Datatype must be JSON", rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_MisingType) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "token":"test"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'type' which must be a string",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_TypeNotString) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "token":"test", "type":0})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'type' which must be a string",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_TypeNotJWT) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "token":"test", "type":"foo"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
    EXPECT_EQ("Only 'JWT' tokens is supported", rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_MisingId) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"token":"test", "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'id' which must be a number",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_IdNotNumeric) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":"foo", "token":"test", "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'id' which must be a number",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_MisingToken) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'token' which must be a string",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_TokenNotString) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":0, "token":0, "type":"JWT"})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("The provided JSON must contain 'token' which must be a string",
              rsp.getErrorContext());
}

TEST_F(JwtClusterTest, TestPacketValidator_RemoveToken) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});
    auto json = R"({"id":100})";
    BinprotGenericCommand cmd(ClientOpcode::RegisterAuthToken, {}, json);
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto rsp = conn->execute(cmd);
    ASSERT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
}

TEST_F(JwtClusterTest, TestWitnRegisteredToken) {
    auto conn = cluster->getBucket("default")->getAuthedConnection(Vbid{0});

    auto do_get = [&](std::optional<uint16_t> token_id) {
        BinprotGetCommand get_command("foo");
        if (token_id) {
            get_command.addFrameInfo(
                    cb::mcbp::request::ImpersonateWithTokenAuthIdFrameInfo{
                            *token_id});
        }
        return conn->execute(get_command);
    };

    // Verify that the user itself can run get

    auto rsp = do_get({});
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());

    const auto upsert_token_id =
            registerToken(*conn, "default", nlohmann::json::array({"Upsert"}));

    const auto readonly_token_id =
            registerToken(*conn, "default", nlohmann::json::array({"Read"}));

    /// The token with just upsert can't run get
    rsp = do_get(upsert_token_id);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << rsp.getErrorContext();

    /// The token with just read can run get
    rsp = do_get(readonly_token_id);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());

    unregisterToken(*conn, upsert_token_id);
    unregisterToken(*conn, readonly_token_id);

    // If I try to reference the removed token I get Einval
    rsp = do_get(upsert_token_id);
    EXPECT_FALSE(rsp.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    EXPECT_EQ("Unknown token auth id: no such key", rsp.getErrorContext());
}
