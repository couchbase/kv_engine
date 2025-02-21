/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "auth_provider.h"
#include "testapp.h"
#include "testapp_audit.h"
#include "testapp_client_test.h"

#include <auditd/couchbase_audit_events.h>
#include <mcbp/codec/frameinfo.h>
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

    std::pair<cb::sasl::Error, nlohmann::json> getUserEntry(
            const std::string& username) override {
        if (username == "satchel") {
            auto ret = nlohmann::json::parse(
                    R"({"satchel" : {
  "domain" : "external",
  "buckets": {
    "default": ["Read","Insert","Delete","Upsert"]
  },
  "privileges": []
}})");

            return std::make_pair<cb::sasl::Error, nlohmann::json>(
                    cb::sasl::Error::OK, std::move(ret));
        }

        return std::make_pair<cb::sasl::Error, nlohmann::json>(
                cb::sasl::Error::NO_RBAC_PROFILE, {});
    }
};

class ExternalAuthTest : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        memcached_cfg["external_auth_service"] = true;
        memcached_cfg["active_external_users_push_interval"] = "100 ms";
        reconfigure();

        auto& conn = getConnection();
        provider = conn.clone();
        // Register as RBAC provider
        provider->authenticate("@admin");
        provider->setDatatypeJson(true);
        provider->setDuplexSupport(true);
        const auto response = provider->execute(BinprotAuthProviderCommand{});
        ASSERT_TRUE(response.isSuccess());
    }

    void TearDown() override {
        provider.reset();
        memcached_cfg["external_auth_service"] = false;
        memcached_cfg["active_external_users_push_interval"] = "30 m";
        reconfigure();
        TestappTest::TearDown();
    }

    void stepAuthProvider(std::optional<std::chrono::microseconds> delay = {}) {
        Frame frame;
        do {
            provider->recvFrame(frame);
        } while (frame.getRequest()->getServerOpcode() ==
                 cb::mcbp::ServerOpcode::ActiveExternalUsers);
        // Perform the authentication

        TestappAuthProvider authProvider;
        const auto result = authProvider.process(*frame.getRequest());

        // Sleep after receiving the request but before sending the response
        if (delay && delay.value().count() != 0) {
            std::this_thread::sleep_for(*delay);
        }

        const auto opaque = frame.getRequest()->getOpaque();
        const auto opcode = frame.getRequest()->getServerOpcode();
        frame.reset();
        frame.payload.resize(sizeof(cb::mcbp::Response) + result.second.size());

        using namespace cb::mcbp;
        ResponseBuilder builder({frame.payload.data(), frame.payload.size()});
        builder.setMagic(Magic::ServerResponse);
        builder.setDatatype(cb::mcbp::Datatype::JSON);
        builder.setOpcode(opcode);
        builder.setOpaque(opaque);
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(result.second.data()),
                 result.second.size()});
        builder.setStatus(result.first);
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
     * The authentication provider should push the list of active users
     * every 20ms, so we should be able to pick the list up pretty fast.
     *
     * @param content what we want the content of the users list to be
     */
    void waitForUserList(const std::string& content) {
        while (true) {
            Frame frame;
            provider->recvFrame(frame);

            auto& request = *frame.getRequest();
            ASSERT_EQ(cb::mcbp::Magic::ServerRequest, request.getMagic());
            ASSERT_EQ(cb::mcbp::ServerOpcode::ActiveExternalUsers,
                      request.getServerOpcode());
            auto value = request.getValue();
            std::string users(reinterpret_cast<const char*>(value.data()),
                              value.size());
            if (users == content) {
                return;
            }
        }
    }

    std::unique_ptr<MemcachedConnection> provider;
};

class ExternalAuthSingleThreadTest : public ExternalAuthTest {
public:
    static void SetUpTestCase() {
        auto cfg = generate_config();
        cfg["threads"] = 1;
        cfg["external_auth_slow_duration"] = "100 ms";
        doSetUpTestCaseWithConfiguration(cfg);
    }
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         ExternalAuthSingleThreadTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         ExternalAuthTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(ExternalAuthTest, TestAllMechsOffered) {
    auto& conn = getConnection();
    auto rsp = conn.execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SaslListMechs});
    EXPECT_EQ("SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN", rsp.getDataView());
}

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
    auto auth = [this]() {
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
    };

    // Do one authentication so that we know that the user is there
    auth();

    // Now lets's reload the RBAC database
    auto response = adminConnection->execute(BinprotRbacRefreshCommand{});
    EXPECT_TRUE(response.isSuccess()) << "Failed to refresh DB";

    // Verify that the user is still there...
    auth();
}

TEST_P(ExternalAuthTest, GetActiveUsers) {
    // Log in a few "local" users
    auto& conn = getConnection();
    auto clone1 = conn.clone();
    auto clone2 = conn.clone();
    auto clone3 = conn.clone();
    auto clone4 = conn.clone();

    clone1->authenticate("smith");
    clone2->authenticate("smith");
    clone3->authenticate("jones");
    clone4->authenticate("@admin");

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

TEST_P(ExternalAuthTest, TestImpersonateExternalUser) {
    using cb::mcbp::request::ImpersonateUserFrameInfo;
    auto connection = adminConnection->clone();
    connection->authenticate("almighty");
    connection->executeInBucket(bucketName, [this](auto& c) {
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Noop);
        cmd.addFrameInfo(ImpersonateUserFrameInfo{"^satchel"});
        c.sendCommand(cmd);

        // Step the auth provider as we're expecting it to fetch the rbac
        // profile for satchel
        stepAuthProvider();

        BinprotResponse rsp;
        c.recvResponse(rsp);

        // The next time we call the op it should hit it in the cache..
        c.execute(cmd);

        // Now that we have the entry in cache, verify that we can
        // add privileges as part of impersonate
        BinprotGenericCommand stat(cb::mcbp::ClientOpcode::Stat);
        stat.addFrameInfo(ImpersonateUserFrameInfo{"^satchel"});
        rsp = c.execute(stat);
        EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
        stat.addFrameInfo(
                cb::mcbp::request::ImpersonateUserExtraPrivilegeFrameInfo{
                        "SimpleStats"});
        rsp = c.execute(stat);
        EXPECT_TRUE(rsp.isSuccess())
                << rsp.getStatus() << " " << rsp.getDataView();

        // A stat call would return multiple packets. We need to drain
        // all of them to avoid getting "out of sync" with later use of the
        // client
        while (!rsp.getKey().empty() || !rsp.getDataView().empty()) {
            c.recvResponse(rsp);
        }
    });
}

/**
 * Verify that the payload in the auth error include if LDAP is configured
 * or not. The payload should look like:
 *  {
 *      "error": {
 *         "ref": "4d58151c-b452-45c8-1d63-ff69a5dd7f44",
 *         "context" : "Authentication failed. This could be due ...."
 *      }
 *  }
 */
TEST_P(ExternalAuthTest, TestErrorIncludeLdapInfo) {
    auto& conn = getConnection();
    try {
        conn.authenticate("foo", "bar", "SCRAM-SHA512");
        FAIL() << "scram should not work";
    } catch (const ConnectionError& e) {
        const auto json = e.getErrorJsonContext();
        ASSERT_TRUE(json.contains("error") && json["error"].contains("context"))
                << json.dump(2);
        auto message = json["error"]["context"].get<std::string>();
        const std::string blueprint =
                "Authentication failed. This could be due to invalid "
                "credentials or if the user is an external user the "
                "external authentication service may not support the "
                "selected authentication mechanism.";
        EXPECT_EQ(blueprint, message);
    }
}

TEST_P(ExternalAuthTest, TestExternalAuthAudit) {
    auto logdir = mcd_env->getAuditLogDir();
    std::filesystem::remove_all(logdir);
    std::filesystem::create_directories(logdir);

    auto& json = mcd_env->getAuditConfig();
    json["auditd_enabled"] = true;
    json["event_states"]
        [std::to_string(MEMCACHED_AUDIT_AUTHENTICATION_FAILED)] = "enabled";
    AuditTest::reconfigureAudit();

    auto& conn = getConnection();

    BinprotSaslAuthCommand saslAuthCommand;
    saslAuthCommand.setChallenge({"\0TestExternalAuthAudit\0password", 31});
    saslAuthCommand.setMechanism("PLAIN");
    conn.sendCommand(saslAuthCommand);

    stepAuthProvider();

    // Now read out the response from the client
    BinprotResponse response;
    conn.recvResponse(response);
    EXPECT_FALSE(response.isSuccess());
    EXPECT_EQ(cb::mcbp::Status::AuthError, response.getStatus());

    // Audit is async.. so we need to wait for the audit to appear..
    bool found = false;
    AuditTest::iterate([&found](const nlohmann::json& entry) -> bool {
        if (entry.value("id", 0) == MEMCACHED_AUDIT_AUTHENTICATION_FAILED) {
            EXPECT_EQ("external",
                      entry["real_userid"]["domain"].get<std::string>());
            EXPECT_EQ("TestExternalAuthAudit",
                      entry["real_userid"]["user"].get<std::string>());
            found = true;
            return true;
        }
        return false;
    });

    EXPECT_TRUE(found) << "Timed out waiting for log entry to appear";
    found = false;
    json["auditd_enabled"] = false;
    AuditTest::reconfigureAudit();
}

TEST_P(ExternalAuthSingleThreadTest, TestCountersForExternalAuthentication) {
    auto conn = getConnection().clone();
    auto& adminConnection = getAdminConnection();
    adminConnection.selectBucket(bucketName);

    // Initialise local counters from stats
    auto authRequestsSent =
            getStat<size_t>(adminConnection, "", "auth_external_sent");
    auto authRequestsRecieved =
            getStat<size_t>(adminConnection, "", "auth_external_sent");

    for (int ii = 0; ii < 10; ++ii) {
        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
        saslAuthCommand.setMechanism("PLAIN");
        conn->sendCommand(saslAuthCommand);
        ++authRequestsSent;

        waitForStatToBe(
                adminConnection, "", "auth_external_sent", authRequestsSent);

        stepAuthProvider();

        // Now read out the response from the client
        BinprotResponse response;
        conn->recvResponse(response);
        ++authRequestsRecieved;

        EXPECT_TRUE(response.isSuccess());

        // Check if the correct stat has been updated
        EXPECT_EQ(
                authRequestsRecieved,
                getStat<size_t>(adminConnection, "", "auth_external_received"));
    }
}

TEST_P(ExternalAuthSingleThreadTest, TestSlowResponseFromAuthProvider) {
    BinprotSaslAuthCommand saslAuthCommand;
    auto& conn = getConnection();
    saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
    saslAuthCommand.setMechanism("PLAIN");
    conn.sendCommand(saslAuthCommand);

    // Delay 101ms to force slow response
    stepAuthProvider(std::chrono::milliseconds(101));

    // Now read out the response from the client
    BinprotResponse response;
    conn.recvResponse(response);

    EXPECT_TRUE(response.isSuccess());

    // Check for slow operation log message
    bool found = false;
    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (!found && std::chrono::steady_clock::now() < timeout) {
        mcd_env->iterateLogLines([&found](auto line) {
            if (line.find("Slow external user authentication") !=
                std::string::npos) {
                found = true;
                return false;
            }
            return true;
        });
        // Logging is async so back off and wait a short while
        if (!found) {
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
    }
    EXPECT_TRUE(found) << "Log message not found";
}

TEST_P(ExternalAuthSingleThreadTest, TestTimeOutRequestToAuthProvider) {
    // Test specific configuration
    memcached_cfg["external_auth_request_timeout"] = "100 ms";
    reconfigure();

    BinprotSaslAuthCommand saslAuthCommand;
    auto& conn = getConnection();
    saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
    saslAuthCommand.setMechanism("PLAIN");
    conn.sendCommand(saslAuthCommand);

    // Delay 101ms (timeout) + 20ms (max ExternalAuthManagerThread::run() will
    // sleep), due to potential race and intimittent failure increase to 130ms
    stepAuthProvider(std::chrono::milliseconds(130));

    // Now read out the response from the client
    BinprotResponse response;
    conn.recvResponse(response);

    // response will fail due to timeout
    EXPECT_FALSE(response.isSuccess());

    // Check for timeout request log message
    bool found = false;
    const auto timeout =
            std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (!found && std::chrono::steady_clock::now() < timeout) {
        mcd_env->iterateLogLines([&found](auto line) {
            if (line.find("Request timed out, external authentication manager "
                          "did not respond") != std::string::npos) {
                found = true;
                return false;
            }
            return true;
        });
        // Logging is async so back off and wait a short while
        if (!found) {
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
    }
    EXPECT_TRUE(found) << "Log message not found";
}

TEST_P(ExternalAuthSingleThreadTest, TestExposedExternalAuthTimings) {
    auto conn = getConnection().clone();
    auto& adminConnection = getAdminConnection();
    adminConnection.selectBucket(bucketName);

    // Initialise local counter from stats
    int numOfDurations = 0;
    try {
        auto authorizationHist = getStat<nlohmann::json>(
                adminConnection,
                "external-auth-timings",
                "auth-external-authorization-durations");
        numOfDurations = authorizationHist["total"];
    } catch (const nlohmann::json::type_error& e) {
        // Histogram is undefined, keep counter at 0
    }

    for (int ii = 0; ii < 10; ++ii) {
        BinprotSaslAuthCommand saslAuthCommand;
        saslAuthCommand.setChallenge({"\0osbourne\0password", 18});
        saslAuthCommand.setMechanism("PLAIN");
        conn->sendCommand(saslAuthCommand);

        stepAuthProvider();
        numOfDurations++;

        // Now read out the response from the client
        BinprotResponse response;
        conn->recvResponse(response);
        EXPECT_TRUE(response.isSuccess());

        // Check if histogram has additional entry
        auto authorizationHist = getStat<nlohmann::json>(
                adminConnection,
                "external-auth-timings",
                "auth-external-authorization-durations");
        EXPECT_EQ(numOfDurations, authorizationHist["total"]);
    }
}