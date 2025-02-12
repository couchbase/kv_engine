/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include "cbsasl/client.h"
#include "cbsasl/error.h"

#include <algorithm>
#include <filesystem>

using namespace std::string_literals;

class SaslTest : public TestappClientTest {
public:
    /**
     * Create a vector containing all of the supported mechanisms we
     * need to test.
     */
    SaslTest() {
        mechanisms.emplace_back("PLAIN");
        mechanisms.emplace_back("SCRAM-SHA1");
        mechanisms.emplace_back("SCRAM-SHA256");
        mechanisms.emplace_back("SCRAM-SHA512");
    }

    void SetUp() override {
        mcd_env->getTestBucket().createBucket(bucket1, {}, *adminConnection);
        mcd_env->getTestBucket().createBucket(bucket2, {}, *adminConnection);
    }

    void TearDown() override {
        // Delete any buckets created for the unit case.
        auto buckets = adminConnection->listBuckets();
        for (const auto& bucket : buckets) {
            if (bucket.starts_with("bucket")) {
                adminConnection->deleteBucket(bucket);
            }
        }
    }

protected:
    void testMixStartingFrom(const std::string& mechanism) {
        MemcachedConnection& conn = getConnection();

        for (const auto& mech : mechanisms) {
            conn.reconnect();
            conn.authenticate(bucket1, {}, mechanism);
            conn.authenticate(bucket2, {}, mech);
        }
    }

    void testIllegalLogin(const std::string& user, const std::string& mech) {
        MemcachedConnection& conn = getConnection();
        try {
            conn.authenticate(user, "wtf", mech);
            FAIL() << "incorrect authentication should fail for user \"" << user
                   << "\" with mech \"" << mech << "\"";
        } catch (const ConnectionError& e) {
            EXPECT_TRUE(e.isAuthError()) << e.what();
        }
        conn.reconnect();
    }

    void testUnknownUser(const std::string& mech) {
        testIllegalLogin("wtf", mech);
    }
    void testWrongPassword(const std::string& mech) {
        testIllegalLogin("@admin", mech);
    }

    void testPasswordExpired(const std::string& mech) {
        MemcachedConnection& conn = getConnection();
        try {
            conn.authenticate("expired", "expired", mech);
            FAIL() << "Authentication should fail!!!";
        } catch (const ConnectionError& e) {
            EXPECT_TRUE(e.isAuthError()) << e.what();
            EXPECT_EQ("Password expired", e.getErrorContext());
        }
    }

    /**
     * Update the list of supported authentication mechanisms
     *
     * @param mechanisms The new set of supported mechanisms
     * @param ssl Is the list for ssl connections or not
     */
    void setSupportedMechanisms(const std::string& mechanisms, bool ssl) {
        std::string key{"sasl_mechanisms"};
        if (ssl) {
            key.insert(0, "ssl_");
        }

        memcached_cfg[key] = mechanisms;
        reconfigure();
    }

    std::vector<std::string> mechanisms;
    const std::string bucket1{"bucket-1"};
    const std::string bucket2{"bucket-2"};
    const std::string bucket3{"bucket-3"};
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         SaslTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(SaslTest, SinglePLAIN) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, {}, "PLAIN");
}

TEST_P(SaslTest, SingleSCRAM_SHA1) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, {}, "SCRAM-SHA1");
}

TEST_P(SaslTest, SingleSCRAM_SHA256) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, {}, "SCRAM-SHA256");
}

TEST_P(SaslTest, SingleSCRAM_SHA512) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, {}, "SCRAM-SHA512");
}

TEST_P(SaslTest, UnknownUserPlain) {
    testUnknownUser("PLAIN");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA1) {
    testUnknownUser("SCRAM-SHA1");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA256) {
    testUnknownUser("SCRAM-SHA256");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA512) {
    testUnknownUser("SCRAM-SHA512");
}

TEST_P(SaslTest, IncorrectPlain) {
    testWrongPassword("PLAIN");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA1) {
    testWrongPassword("SCRAM-SHA1");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA256) {
    testWrongPassword("SCRAM-SHA256");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA512) {
    testWrongPassword("SCRAM-SHA512");
}

TEST_P(SaslTest, TestSaslMixFrom_PLAIN) {
    testMixStartingFrom("PLAIN");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA1) {
    testMixStartingFrom("SCRAM-SHA1");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA256) {
    testMixStartingFrom("SCRAM-SHA256");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA512) {
    testMixStartingFrom("SCRAM-SHA512");
}

TEST_P(SaslTest, TestDisablePLAIN) {
    auto& conn = getConnection();

    const auto before = conn.getSaslMechanisms();

    auto& c = connectionMap.getConnection(!conn.isSsl(), conn.getFamily());
    c.reconnect();

    const auto otherMechs = c.getSaslMechanisms();

    setSupportedMechanisms("SCRAM-SHA1", conn.isSsl());

    c.reconnect();
    conn.reconnect();

    // We should only support SCRAM-SHA1
    EXPECT_EQ("SCRAM-SHA1", conn.getSaslMechanisms());
    EXPECT_EQ(otherMechs, c.getSaslMechanisms());

    // It should not be possible to select any other mechanisms:
    for (const auto& mech : mechanisms) {
        // get a fresh connection
        conn.reconnect();
        if (mech == "SCRAM-SHA1") {
            // This should work
            conn.authenticate(bucket1, {}, mech);
        } else {
            // All other should fail
            try {
                conn.authenticate(bucket1, {}, mech);
                FAIL() << "Mechanism " << mech << " should be disabled";
            } catch (const ConnectionError& e) {
                EXPECT_TRUE(e.isAuthError());
            }
        }
    }

    // verify that we didn't change the setting for the other connection
    c.reconnect();
    // And PLAIN auth should work
    c.authenticate(bucket1, {}, "PLAIN");

    // Restore the sasl mechanisms
    setSupportedMechanisms(before, conn.isSsl());
}

TEST_P(SaslTest, StepWithoutStart) {
    MemcachedConnection& conn = getConnection();
    std::string username = "foobar";
    std::string password = "barbaz";
    std::string mech = "SCRAM-SHA1";
    cb::sasl::client::ClientContext client([username] { return username; },
                                           [password] { return password; },
                                           mech);
    const auto [status, challenge] = client.start();

    if (status != cb::sasl::Error::OK) {
        throw std::runtime_error(fmt::format(
                "cbsasl_client_start ({}): {}", client.getName(), status));
    }

    BinprotSaslStepCommand stepCommand;
    stepCommand.setMechanism(client.getName());
    stepCommand.setChallenge(challenge);
    auto response = conn.execute(stepCommand);

    EXPECT_FALSE(response.isSuccess());
}

TEST_P(SaslTest, IterationCountMayBeChanged) {
    std::optional<int> iteration_count;
    auto listener = [&iteration_count](char key, const std::string& value) {
        if (key == 'i') {
            iteration_count = stoi(value);
        }
    };

    auto clone = adminConnection->clone();
    clone->setScramPropertyListener(listener);

    auto tryFailedAuth = [&](const std::string& mech, int expected) {
        iteration_count.reset();
        try {
            clone->authenticate("nouser", "wrongpassword", mech);
            FAIL() << "Authentication should fail!!!";
        } catch (ConnectionError& error) {
            EXPECT_TRUE(error.isAuthError()) << error.what();
        }
        ASSERT_TRUE(iteration_count.has_value());
        EXPECT_EQ(expected, *iteration_count) << "When running " << mech;
    };

    memcached_cfg["scramsha_fallback_iteration_count"] = 5;
    reconfigure();

    tryFailedAuth("SCRAM-SHA512", 5);
    tryFailedAuth("SCRAM-SHA256", 5);
    tryFailedAuth("SCRAM-SHA1", 5);

    memcached_cfg["scramsha_fallback_iteration_count"] = 10;
    reconfigure();
    tryFailedAuth("SCRAM-SHA512", 10);
    tryFailedAuth("SCRAM-SHA256", 10);
    tryFailedAuth("SCRAM-SHA1", 10);
}

TEST_P(SaslTest, ExpiredPLAIN) {
    testPasswordExpired("PLAIN");
}

TEST_P(SaslTest, ExpiredSCRAM_SHA1) {
    testPasswordExpired("SCRAM-SHA1");
}

TEST_P(SaslTest, ExpiredSCRAM_SHA256) {
    testPasswordExpired("SCRAM-SHA256");
}

TEST_P(SaslTest, ExpiredSCRAM_SHA512) {
    testPasswordExpired("SCRAM-SHA512");
}

TEST_P(SaslTest, UnsupportedMechanismContainsContextInfo) {
    try {
        auto& conn = getConnection();
        if (!conn.isSsl()) {
            conn.authenticate("nouser", "wrongpassword", "OAUTHBEARER");
            FAIL() << "OAUTHBEARER require TLS";
        }
    } catch (const ConnectionError& e) {
        ASSERT_TRUE(e.isAuthError());
        EXPECT_EQ(
                "Unsupported mechanism. Must be one of: SCRAM-SHA512 "
                "SCRAM-SHA256 SCRAM-SHA1 PLAIN",
                e.getErrorContext());
    }
}
