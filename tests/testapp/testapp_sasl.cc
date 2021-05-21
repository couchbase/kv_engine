/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <cbcrypto/cbcrypto.h>
#include <algorithm>

class SaslTest : public TestappClientTest {
public:
    /**
     * Create a vector containing all of the supported mechanisms we
     * need to test.
     */
    SaslTest() {
        mechanisms.emplace_back("PLAIN");
        if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA1)) {
            mechanisms.emplace_back("SCRAM-SHA1");
        }

        if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA256)) {
            mechanisms.emplace_back("SCRAM-SHA256");
        }

        if (cb::crypto::isSupported(cb::crypto::Algorithm::SHA512)) {
            mechanisms.emplace_back("SCRAM-SHA512");
        }
    }

    void SetUp() override {
        auto& connection = getConnection();
        const auto mechs = connection.getSaslMechanisms();
        connection.authenticate("@admin", "password", mechs);
        ASSERT_NO_THROW(
                connection.createBucket(bucket1, "", BucketType::Memcached));
        ASSERT_NO_THROW(
                connection.createBucket(bucket2, "", BucketType::Memcached));
        ASSERT_NO_THROW(
                connection.createBucket(bucket3, "", BucketType::Couchbase));
        connection.reconnect();
    }

    void TearDown() override {
        auto& connection = getConnection();
        const auto mechs = connection.getSaslMechanisms();
        connection.authenticate("@admin", "password", mechs);
        ASSERT_NO_THROW(connection.deleteBucket(bucket1));
        ASSERT_NO_THROW(connection.deleteBucket(bucket2));
        ASSERT_NO_THROW(connection.deleteBucket(bucket3));
        connection.reconnect();
    }

protected:
    void testMixStartingFrom(const std::string& mechanism) {
        MemcachedConnection& conn = getConnection();

        for (const auto& mech : mechanisms) {
            conn.reconnect();
            conn.authenticate(bucket1, password1, mechanism);
            conn.authenticate(bucket2, password2, mech);
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

    bool isSupported(const std::string mechanism) {
        auto& conn = getConnection();
        const auto mechs = conn.getSaslMechanisms();
        if (mechs.find(mechanism) == std::string::npos) {
            std::cerr << "Skipping test due to missing server support for "
                      << mechanism << std::endl;
            return false;
        }
        return true;
    }

    std::vector<std::string> mechanisms;
    const std::string bucket1{"bucket-1"};
    const std::string password1{"1S|=,%#x1"};
    const std::string bucket2{"bucket-2"};
    const std::string password2{"secret"};
    const std::string bucket3{"bucket-3"};
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         SaslTest,
                         ::testing::Values(TransportProtocols::McbpPlain,
                                           TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(SaslTest, SinglePLAIN) {
    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, password1, "PLAIN");
}

TEST_P(SaslTest, SingleSCRAM_SHA1) {
    if (!isSupported("SCRAM-SHA1")) {
        return;
    }

    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, password1, "SCRAM-SHA1");
}

TEST_P(SaslTest, SingleSCRAM_SHA256) {
    if (!isSupported("SCRAM-SHA256")) {
        return;
    }

    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, password1, "SCRAM-SHA256");
}

TEST_P(SaslTest, SingleSCRAM_SHA512) {
    if (!isSupported("SCRAM-SHA512")) {
        return;
    }

    MemcachedConnection& conn = getConnection();
    conn.authenticate(bucket1, password1, "SCRAM-SHA512");
}

TEST_P(SaslTest, UnknownUserPlain) {
    testUnknownUser("PLAIN");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA1) {
    if (!isSupported("SCRAM-SHA1")) {
        return;
    }
    testUnknownUser("SCRAM-SHA1");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA256) {
    if (!isSupported("SCRAM-SHA256")) {
        return;
    }
    testUnknownUser("SCRAM-SHA256");
}

TEST_P(SaslTest, UnknownUserSCRAM_SHA512) {
    if (!isSupported("SCRAM-SHA512")) {
        return;
    }
    testUnknownUser("SCRAM-SHA512");
}

TEST_P(SaslTest, IncorrectPlain) {
    testWrongPassword("PLAIN");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA1) {
    if (!isSupported("SCRAM-SHA1")) {
        return;
    }
    testWrongPassword("SCRAM-SHA1");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA256) {
    if (!isSupported("SCRAM-SHA256")) {
        return;
    }

    testWrongPassword("SCRAM-SHA256");
}

TEST_P(SaslTest, IncorrectSCRAM_SHA512) {
    if (!isSupported("SCRAM-SHA512")) {
        return;
    }
    testWrongPassword("SCRAM-SHA512");
}

TEST_P(SaslTest, TestSaslMixFrom_PLAIN) {
    testMixStartingFrom("PLAIN");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA1) {
    if (!isSupported("SCRAM-SHA1")) {
        return;
    }
    testMixStartingFrom("SCRAM-SHA1");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA256) {
    if (!isSupported("SCRAM-SHA256")) {
        return;
    }
    testMixStartingFrom("SCRAM-SHA256");
}

TEST_P(SaslTest, TestSaslMixFrom_SCRAM_SHA512) {
    if (!isSupported("SCRAM-SHA512")) {
        return;
    }
    testMixStartingFrom("SCRAM-SHA512");
}

TEST_P(SaslTest, TestDisablePLAIN) {
    if (!isSupported("SCRAM-SHA1")) {
        return;
    }

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
            conn.authenticate(bucket1, password1, mech);
        } else {
            // All other should fail
            try {
                conn.authenticate(bucket1, password1, mech);
                FAIL() << "Mechanism " << mech << " should be disabled";
            } catch (const ConnectionError& e) {
                EXPECT_TRUE(e.isAuthError());
            }
        }
    }

    // verify that we didn't change the setting for the other connection
    c.reconnect();
    // And PLAIN auth should work
    c.authenticate(bucket1, password1, "PLAIN");

    // Restore the sasl mechanisms
    setSupportedMechanisms(before, conn.isSsl());
}

// Pretend we're a collection aware client
TEST_P(SaslTest, CollectionsConnectionSetup) {
    auto& conn = getConnection();

    // Hello
    BinprotHelloCommand helloCmd("Collections");
    helloCmd.enableFeature(cb::mcbp::Feature::Collections);
    helloCmd.enableFeature(cb::mcbp::Feature::XERROR);
    helloCmd.enableFeature(cb::mcbp::Feature::SELECT_BUCKET);
    const auto helloRsp = BinprotHelloResponse(conn.execute(helloCmd));
    ASSERT_TRUE(helloRsp.isSuccess());

    // Get err map
    const auto errMapRsp = conn.execute(BinprotGetErrorMapCommand{});

    // Get SASL mechs
    const auto mechs = conn.getSaslMechanisms();

    // Do a SASL auth
    EXPECT_NO_THROW(conn.authenticate(bucket3, password1, mechs));

    // Select the bucket
    EXPECT_NO_THROW(conn.selectBucket(bucket3));

    // Do a get
    BinprotGetCommand getCmd;
    getCmd.setOp(cb::mcbp::ClientOpcode::Get);
    getCmd.setKey(std::string{"\0key", 4});
    getCmd.setVBucket(Vbid(0));

    auto auto_retry_tmpfail = conn.getAutoRetryTmpfail();
    conn.setAutoRetryTmpfail(true);
    const auto getRsp = BinprotGetResponse(conn.execute(getCmd));
    conn.setAutoRetryTmpfail(auto_retry_tmpfail);
    EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, getRsp.getStatus());
}
