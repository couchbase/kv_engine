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
        provider->recvFrame(frame, false);

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
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, response.getStatus());
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
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, response.getStatus());
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
        EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, response.getStatus());
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
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_ETMPFAIL, response.getStatus());
}
