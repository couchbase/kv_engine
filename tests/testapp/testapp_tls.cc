/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"
#include "testapp_client_test.h"

#include <platform/compress.h>
#include <protocol/mcbp/ewb_encode.h>
#include <algorithm>

class TlsTests : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        tls = {{"private key", OBJECT_ROOT "/tests/cert/root/ca_root.key"},
               {"certificate chain",
                OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
               {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
               {"minimum version", "TLS 1"},
               {"cipher list",
                {{"TLS 1.2", "HIGH"},
                 {"TLS 1.3",
                  "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
                  "AES_"
                  "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
                  "SHA256"}}},
               {"cipher order", true},
               {"client cert auth", "disabled"}};
        reloadConfig();
        connection =
                connectionMap
                        .getConnection(true,
                                       mcd_env->haveIPv4() ? AF_INET : AF_INET6)
                        .clone();
    }

    void reloadConfig() {
        auto rsp = adminConnection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::Ifconfig, "tls", tls.dump()});
        ASSERT_TRUE(rsp.isSuccess()) << "Failed to set TLS properties: "
                                     << to_string(rsp.getStatus()) << std::endl
                                     << rsp.getDataString();
    }

    void setTlsMinimumSpec(const std::string& version) {
        tls["minimum version"] = version;
        reloadConfig();
    }

    void shouldPass(const std::string& version) {
        try {
            connection->setTlsProtocol(version);
            connection->reconnect();
            const auto rsp = connection->execute(BinprotGenericCommand(
                    cb::mcbp::ClientOpcode::SaslListMechs));
            ASSERT_TRUE(rsp.isSuccess()) << "Failed with version " << version;
        } catch (const std::exception& e) {
            FAIL() << "Failed with version \"" << version << "\": " << e.what();
        }
    }

    void shouldFail(const std::string& version) {
        try {
            connection->setTlsProtocol(version);
            connection->reconnect();
            FAIL() << "Should fail with " << version;
        } catch (const std::exception&) {
        }
    }

    nlohmann::json tls;
    std::unique_ptr<MemcachedConnection> connection;
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TlsTests,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(TlsTests, Minimum_Tls1) {
    setTlsMinimumSpec("TLS 1");

    shouldPass("tlsv1");
    shouldPass("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_1) {
    setTlsMinimumSpec("TLS 1.1");

    shouldFail("tlsv1");
    shouldPass("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_2) {
    setTlsMinimumSpec("TLS 1.2");

    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_3) {
    setTlsMinimumSpec("TLS 1.3");

    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, TLS12_Ciphers) {
    // Disable all TLS v1 ciphers
    tls["cipher list"]["TLS 1.2"] = "HIGH:!TLSv1";
    reloadConfig();

    // We should be able to pick one of the other ciphers
    shouldPass("tlsv1");
    shouldPass("tlsv1_1");
    shouldPass("tlsv1_2");

    // But all should fail if we set that we only want TLSv1 ciphers
    connection->setTls12Ciphers("TLSv1");
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");

    // TLS 1.1. did not introduce any new ciphers
    tls["cipher list"]["TLS 1.2"] = "HIGH:!TLSv1:!TLSv1.2";
    reloadConfig();
    connection->setTls12Ciphers("TLSv1.2");
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
}

TEST_P(TlsTests, TLS13_Ciphers) {
    tls["cipher list"]["TLS 1.3"] = "TLS_AES_256_GCM_SHA384";
    reloadConfig();

    shouldPass("tlsv1_3");

    connection->setTls13Ciphers("TLS_AES_128_GCM_SHA256");
    shouldFail("tlsv1_3");
}

// In the weird case you've configured the system to not accept any ciphers
// at all
TEST_P(TlsTests, No_Ciphers) {
    tls["cipher list"]["TLS 1.2"] = "";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
    shouldFail("tlsv1_3");
}

TEST_P(TlsTests, ECDHE_RSA_AES256_GCM_SHA384) {
    tls["cipher list"]["TLS 1.2"] = "ECDHE-RSA-AES256-GCM-SHA384";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();
    shouldPass("tlsv1_2");
}

TEST_P(TlsTests, DHE_RSA_AES256_SHA256) {
    tls["cipher list"]["TLS 1.2"] = "DHE-RSA-AES256-SHA256";
    tls["cipher list"]["TLS 1.3"] = "";
    reloadConfig();
    shouldPass("tlsv1_2");
}
