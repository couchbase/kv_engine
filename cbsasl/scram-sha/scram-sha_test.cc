/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "scram-sha.h"

#include <cbsasl/cbcrypto.h>
#include <cbsasl/password_database.h>
#include <cbsasl/pwfile.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>
#include <string_view>

using cb::crypto::Algorithm;
using cb::sasl::Error;
using cb::sasl::client::ClientContext;
using cb::sasl::server::ServerContext;
using namespace cb::sasl::mechanism::scram;
using namespace std::literals;

class ScramShaServerTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        using cb::sasl::pwdb::User;
        using cb::sasl::pwdb::UserFactory;

        auto passwordDatabase = std::make_unique<
                cb::sasl::pwdb::PasswordDatabase>(nlohmann::json{
                {"@@version@@", 2},
                {"username",
                 {{"scram-sha-1",
                   {{"hash", "VcEpRYqDQNvMxPJEadPOSag96hM="},
                    {"salt", "ZP8Dc+4BGCAMitu9B7qSbjaB2fs="},
                    {"iterations", 4096}}},
                  {"scram-sha-256",
                   {{"hash", "non+4eYQu187sSZYklHAAX6sxV5InTlbMQmKqY5yxvs="},
                    {"salt", "PxxruqJ4/CL3QR2fEH0ZDNqq56u8qf0eZm6YLK1aoTQ="},
                    {"iterations", 4096}}},
                  {"scram-sha-512",
                   {{"hash",
                     "IzJdxYFcBuwooTc30neYXSECygeWodXqcIOGefe51z9rsMIjFxLJJ0r0q"
                     "bbtDXBxH5gP50e8MRTb1uRKKy2dVg=="},
                    {"salt",
                     "PU0/"
                     "GpqaHx9+XsECIYGuGnN9o65two+"
                     "E5l3BAbisQn3smkGOIlL2OJ6hB8b5pfOETGG1v4X9/"
                     "xq8SjxsGdZ3xg=="},
                    {"iterations", 4096}}},
                  {"hash",
                   {{"algorithm", "argon2id"},
                    {"time", 3},
                    {"memory", 134217728},
                    {"parallelism", 1},
                    {"salt", "lEyxVOIleyw2vzrajlqAlA=="},
                    {"hash",
                     "k14HWFzYHGX2YhN1u/vI+W3m9dJSbu6SsKPEd4BQoJg="}}}}}});
        swap_password_database(std::move(passwordDatabase));
    }

    void getClientFirstMessage(ClientContext& client,
                               std::string_view expected) {
        const auto [error, data] = client.start();
        EXPECT_EQ(Error::OK, error);
        ASSERT_EQ(expected, data);
    }

    void getServerFirstMessage(ServerBackend& server,
                               std::string_view challenge,
                               std::string_view expected) {
        const auto [error, data] = server.start(challenge);
        EXPECT_EQ(Error::CONTINUE, error);
        EXPECT_EQ(expected, data);
    }

    void getClientFinalMessage(ClientContext& client,
                               std::string_view challenge,
                               std::string_view expected) {
        const auto [error, data] = client.step(challenge);
        EXPECT_EQ(Error::CONTINUE, error);
        EXPECT_EQ(expected, data);
    }
};

TEST_F(ScramShaServerTest, TestScramSha1) {
    ServerContext serverContext;
    auto server = std::make_unique<Sha1ServerBackend>(
            serverContext, []() { return "3rfcNHYJY1ZVvWVs7j"; });
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return "password"; },
                         "SCRAM-SHA1",
                         []() { return "fyko+d2lbbFgONRv9qkxdawL"; });
    std::string_view client_first_message =
            "n,,n=username,r=fyko+d2lbbFgONRv9qkxdawL"sv;
    std::string_view server_first_message =
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=ZP8Dc+4BGCAMitu9B7qSbjaB2fs=,i=4096"sv;
    std::string_view client_final_message =
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=BgDTjduRiG2rLn++YdlWSNmMCw8="sv;

    getClientFirstMessage(client, client_first_message);
    getServerFirstMessage(*server, client_first_message, server_first_message);
    getClientFinalMessage(client, server_first_message, client_final_message);

    const auto [error, data] = server->step(client_final_message);
    EXPECT_EQ(Error::OK, error);
    EXPECT_EQ("v=UfaW46lFMmrVCgnqdU1KxQVUNso="sv, data);
}

TEST_F(ScramShaServerTest, TestScramSha256) {
    ServerContext serverContext;
    auto server = std::make_unique<Sha256ServerBackend>(
            serverContext, []() { return "3rfcNHYJY1ZVvWVs7j"; });
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return "password"; },
                         "SCRAM-SHA256",
                         []() { return "fyko+d2lbbFgONRv9qkxdawL"; });

    std::string_view client_first_message =
            "n,,n=username,r=fyko+d2lbbFgONRv9qkxdawL"sv;
    std::string_view server_first_message =
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=PxxruqJ4/CL3QR2fEH0ZDNqq56u8qf0eZm6YLK1aoTQ=,i=4096"sv;
    std::string_view client_final_message =
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=zN1Vl3VZ03UhsHVOO2n4RHxT4js7dTn+/LcLT8Tq+JI="sv;
    getClientFirstMessage(client, client_first_message);
    getServerFirstMessage(*server, client_first_message, server_first_message);
    getClientFinalMessage(client, server_first_message, client_final_message);

    const auto [error, data] = server->step(client_final_message);
    EXPECT_EQ(Error::OK, error);
    EXPECT_EQ("v=LG5RRVHh+9OfBnOctbT+89mfY3Cph/3PSebVLY/PGsI="sv, data);
}

TEST_F(ScramShaServerTest, TestScramSha512) {
    ServerContext serverContext;
    auto server = std::make_unique<Sha512ServerBackend>(
            serverContext, []() { return "3rfcNHYJY1ZVvWVs7j"; });
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return "password"; },
                         "SCRAM-SHA512",
                         []() { return "fyko+d2lbbFgONRv9qkxdawL"; });
    std::string_view client_first_message =
            "n,,n=username,r=fyko+d2lbbFgONRv9qkxdawL"sv;
    std::string_view server_first_message =
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=PU0/GpqaHx9+XsECIYGuGnN9o65two+E5l3BAbisQn3smkGOIlL2OJ6hB8b5pfOETGG1v4X9/xq8SjxsGdZ3xg==,i=4096"sv;
    std::string_view client_final_message =
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=hw4VUyoNvrlZjhnfXS3EeWs8SQvAa2hVgyMlT3AuZIsLivqU8ZSFD5w9iT+nWHuxC4w4xkVTz4X9bqY0hx5EJQ=="sv;
    getClientFirstMessage(client, client_first_message);
    getServerFirstMessage(*server, client_first_message, server_first_message);
    getClientFinalMessage(client, server_first_message, client_final_message);

    const auto [error, data] = server->step(client_final_message);
    EXPECT_EQ(Error::OK, error);
    EXPECT_EQ(
            "v=s+JthevEfn6bno1LjNly0nJanZmL/o3Z7SbBPfWCUfd/Wdq+eXlZ5OqvnpuSJDlPl/rA92RzwNVhgif+pdPzkQ=="sv,
            data);
}

// The GS2 header must start with 'n' as we don't support channel binding
TEST_F(ScramShaServerTest, TestClientFirstMessageInvalidGs2Header) {
    ServerContext serverContext;
    Sha1ServerBackend server(serverContext);
    auto [error, data] = server.start("f,n=username,r=deadbeef"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST_F(ScramShaServerTest, TestClientFirstMessageNoUser) {
    ServerContext serverContext;
    Sha1ServerBackend server(serverContext);
    auto [error, data] = server.start("n,,r=deadbeef"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST_F(ScramShaServerTest, TestClientFirstMessageNoClientNonce) {
    ServerContext serverContext;
    Sha1ServerBackend server(serverContext);
    auto [error, data] = server.start("n,,n=username"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}
