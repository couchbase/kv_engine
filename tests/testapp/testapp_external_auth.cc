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

#include "auth_provider.h"
#include "testapp.h"
#include "testapp_client_test.h"

#include <mcbp/protocol/framebuilder.h>
#include <memory>

class TestappAuthProvider : public AuthProvider {
protected:
    std::pair<cb::sasl::Error, nlohmann::json> validatePassword(
            const std::string& username, const std::string& password) override {
        if (username != "osbourne") {
            if (username == "undefined") {
                return std::make_pair<cb::sasl::Error, nlohmann::json>(
                        cb::sasl::Error::NO_RBAC_PROFILE, {});
            }
            return std::make_pair<cb::sasl::Error, nlohmann::json>(
                    cb::sasl::Error::NO_USER, {});
        }

        if (password != "password") {
            return std::make_pair<cb::sasl::Error, nlohmann::json>(
                    cb::sasl::Error::PASSWORD_ERROR, {});
        }

        auto ret = nlohmann::json::parse(
                R"({"osbourne" : {
  "domain" : "external",
  "buckets": {
    "default": ["Read","SimpleStats","Insert","Delete","Upsert"]
  },
  "privileges": []
}})");

        return std::make_pair<cb::sasl::Error, nlohmann::json>(
                cb::sasl::Error::OK, std::move(ret));
    }
};

class ExternalAuthTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        cJSON_DeleteItemFromObject(memcached_cfg.get(),
                                   "external_auth_service");
        cJSON_AddTrueToObject(memcached_cfg.get(), "external_auth_service");
        reconfigure();

        auto& conn = getConnection();
        provider = conn.clone();
        // Register as RBAC provider
        provider->authenticate("@admin", "password", "PLAIN");
        provider->setDuplexSupport(true);
        BinprotResponse response;
        provider->executeCommand(BinprotAuthProviderCommand{}, response);
        ASSERT_TRUE(response.isSuccess());
    }

    void TearDown() override {
        provider.reset();
        cJSON_DeleteItemFromObject(memcached_cfg.get(),
                                   "external_auth_service");
        cJSON_AddTrueToObject(memcached_cfg.get(), "external_auth_service");
        reconfigure();
        TestappTest::TearDown();
    }

    void stepAuthProvider() {
        Frame frame;
        provider->recvFrame(frame);

        // Perform the authentication

        TestappAuthProvider authProvider;
        const auto payload = frame.getRequest()->getValue();
        auto auth_success = authProvider.process(std::string{
                reinterpret_cast<const char*>(payload.data()), payload.size()});

        uint32_t opaque = frame.getRequest()->getOpaque();
        frame.reset();
        frame.payload.resize(sizeof(cb::mcbp::Response) +
                             auth_success.second.size());

        using namespace cb::mcbp;
        ResponseBuilder builder({frame.payload.data(), frame.payload.size()});
        builder.setMagic(Magic::ServerResponse);
        builder.setDatatype(cb::mcbp::Datatype::JSON);
        builder.setOpcode(ServerOpcode::AuthRequest);
        builder.setOpaque(opaque);
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(auth_success.second.data()),
                 auth_success.second.size()});
        builder.setStatus(auth_success.first);
        provider->sendFrame(frame);
    }

    std::unique_ptr<MemcachedConnection> loginOsbourne() {
        auto ret = getConnection().clone();

        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
        saslAuthCommand.setMechanism("PLAIN");
        ret->sendCommand(saslAuthCommand);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        ret->recvResponse(response);
        if (!response.isSuccess()) {
            return {};
        }

        return ret;
    }

    /**
     * We've got multiple worker threads and if we try to disconnect a user
     * connected to one thread that thread may not be sheduled to run
     * immediately, so we may end up serving requests for another worker
     * thread.
     *
     * To work around this (and try to avoid spurious test failures) we'll
     * try to run the command a few times before we give up.
     *
     * @param content what we want the content of the users list to be
     */
    void waitForUserList(const std::string& content) {
        const auto start = std::chrono::steady_clock::now();
        do {
            auto& conn = getAdminConnection();
            auto resp = conn.execute(BinprotGetActiveUsersCommand{});
            if (resp.isSuccess() && content == resp.getDataString()) {
                return;
            }
        } while ((std::chrono::steady_clock::now() - start) <
                 std::chrono::seconds(2));

        auto& conn = getAdminConnection();
        auto resp = conn.execute(BinprotGetActiveUsersCommand{});
        FAIL() << "Timed out trying to get expected content [" << content
               << "] Current content is: " << resp.getDataString();
    }

    std::unique_ptr<MemcachedConnection> provider;
};

INSTANTIATE_TEST_CASE_P(TransportProtocols,
                        ExternalAuthTest,
                        ::testing::Values(TransportProtocols::McbpPlain),
                        ::testing::PrintToStringParamName());

TEST_P(ExternalAuthTest, TestExternalAuthWithNoExternalProvider) {
    // Drop the provider
    provider.reset();
    try {
        auto& conn = getConnection();
        conn.authenticate("osbourne", "password", "PLAIN");
        FAIL() << "Should not be able to authenticate with external user "
                  "without external auth service";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isTemporaryFailure());
        EXPECT_EQ("External auth service is down", error.getErrorContext());
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthSuccessful) {
    for (int ii = 0; ii < 10; ++ii) {
        auto& conn = getConnection();

        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
        saslAuthCommand.setMechanism("PLAIN");
        conn.sendCommand(saslAuthCommand);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        conn.recvResponse(response);
        EXPECT_TRUE(response.isSuccess());
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthUnknownUser) {
    for (int ii = 0; ii < 10; ++ii) {
        auto& conn = getConnection();

        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0foo\0password", 13});
        saslAuthCommand.setMechanism("PLAIN");
        conn.sendCommand(saslAuthCommand);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        conn.recvResponse(response);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::AuthError, response.getStatus());
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthIncorrectPasword) {
    for (int ii = 0; ii < 10; ++ii) {
        auto& conn = getConnection();

        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0osbourne\0bubba", 15});
        saslAuthCommand.setMechanism("PLAIN");
        conn.sendCommand(saslAuthCommand);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        conn.recvResponse(response);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::AuthError, response.getStatus());
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthNoRbacUser) {
    for (int ii = 0; ii < 10; ++ii) {
        auto& conn = getConnection();

        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0undefined\0bubba", 16});
        saslAuthCommand.setMechanism("PLAIN");
        conn.sendCommand(saslAuthCommand);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        conn.recvResponse(response);
        EXPECT_FALSE(response.isSuccess());
        EXPECT_EQ(cb::mcbp::Status::AuthError, response.getStatus());
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthServiceDying) {
    auto& conn = getConnection();

    BinprotSaslAuthCommand saslAuthCommand;
    saslAuthCommand.setChallenge({"\0undefined\0bubba", 16});
    saslAuthCommand.setMechanism("PLAIN");
    conn.sendCommand(saslAuthCommand);

    // kill the connection
    provider.reset();

    // Now read out the response from the client
    BinprotResponse response;
    conn.recvResponse(response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::Etmpfail, response.getStatus());
}

TEST_P(ExternalAuthTest, TestReloadRbacDbDontNukeExternalUsers) {
    // Do one authentication so that we know that the user is there
    auto& conn = getConnection();

    BinprotSaslAuthCommand saslAuthCommand;
    saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
    saslAuthCommand.setMechanism("PLAIN");
    conn.sendCommand(saslAuthCommand);

    stepAuthProvider();

    // Now read out the response from the client
    BinprotResponse response;
    conn.recvResponse(response);
    EXPECT_TRUE(response.isSuccess()) << "Failed to authenticate";

    // Now lets's reload the RBAC database
    conn = getAdminConnection();
    response = conn.execute(BinprotRbacRefreshCommand{});
    EXPECT_TRUE(response.isSuccess()) << "Failed to refresh DB";

    // Verify that the user is still there...
    response = conn.execute(BinprotGenericCommand{
            PROTOCOL_BINARY_CMD_IOCTL_GET, "rbac.db.dump?domain=external"});
    ASSERT_TRUE(response.isSuccess());
    auto json = nlohmann::json::parse(response.getDataString());
    EXPECT_EQ("external", json["osbourne"]["domain"])
            << response.getDataString();
}

TEST_P(ExternalAuthTest, GetActiveUsers) {
    // Log in a few "local" users
    auto& conn = getConnection();
    auto clone1 = conn.clone();
    auto clone2 = conn.clone();
    auto clone3 = conn.clone();
    auto clone4 = conn.clone();

    clone1->authenticate("smith", "smithpassword", "PLAIN");
    clone2->authenticate("smith", "smithpassword", "PLAIN");
    clone3->authenticate("jones", "jonespassword", "PLAIN");
    clone4->authenticate("@admin", "password", "PLAIN");

    // Log in 2 external ones
    auto osbourne1 = loginOsbourne();
    EXPECT_TRUE(osbourne1);

    auto osbourne2 = loginOsbourne();
    EXPECT_TRUE(osbourne2);

    waitForUserList(R"(["osbourne"])");

    // Log out one of the external users
    osbourne1.reset();

    waitForUserList(R"(["osbourne"])");

    // Log out the second external user
    osbourne2.reset();

    waitForUserList(R"([])");
}
