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
        memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "HIGH";
        memcached_cfg["ssl_cipher_list"]["tls 1.3"] =
                "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_"
                "128_GCM_SHA256";
        reloadConfig();
        setTlsMinimumSpec("tlsv1");
        connection = connectionMap.getConnection(true, AF_INET).clone();
    }

    void reloadConfig() {
        write_config_to_file(memcached_cfg.dump(2), config_file);
        auto& conn = prepare(connectionMap.getConnection(false, AF_INET));
        conn.authenticate("@admin", "password", "PLAIN");
        auto rsp = conn.execute(
                BinprotGenericCommand(cb::mcbp::ClientOpcode::ConfigReload));
        ASSERT_TRUE(rsp.isSuccess())
                << "Failed to reload config: " << rsp.getDataString();
    }

    void setTlsMinimumSpec(const std::string& version) {
        memcached_cfg["ssl_minimum_protocol"] = version;
        reloadConfig();
    }

    void shouldPass(const std::string& version) {
        try {
            connection->setTlsProtocol(version);
            connection->reconnect();
            const auto rsp = connection->execute(
                    BinprotGenericCommand(cb::mcbp::ClientOpcode::Noop));
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

    std::unique_ptr<MemcachedConnection> connection;
};

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         TlsTests,
                         ::testing::Values(TransportProtocols::McbpSsl),
                         ::testing::PrintToStringParamName());

TEST_P(TlsTests, Minimum_Tls1) {
    setTlsMinimumSpec("tlsv1");

    shouldPass("tlsv1");
    shouldPass("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_1) {
    setTlsMinimumSpec("tlsv1_1");

    shouldFail("tlsv1");
    shouldPass("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_2) {
    setTlsMinimumSpec("tlsv1_2");
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldPass("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, Minimum_Tls1_3) {
    setTlsMinimumSpec("tlsv1_3");
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
    shouldPass("tlsv1_3");
}

TEST_P(TlsTests, TLS12_Ciphers) {
    // Disable all TLS v1 ciphers
    memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "HIGH:!TLSv1";
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
    memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "HIGH:!TLSv1:!TLSv1.2";
    reloadConfig();
    connection->setTls12Ciphers("TLSv1.2");
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
}

TEST_P(TlsTests, TLS13_Ciphers) {
    memcached_cfg["ssl_cipher_list"]["tls 1.3"] = "TLS_AES_256_GCM_SHA384";
    reloadConfig();

    shouldPass("tlsv1_3");

    connection->setTls13Ciphers("TLS_AES_128_GCM_SHA256");
    shouldFail("tlsv1_3");
}

// In the weird case you've configured the system to not accept any ciphers
// at all
TEST_P(TlsTests, No_Ciphers) {
    memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "";
    memcached_cfg["ssl_cipher_list"]["tls 1.3"] = "";
    reloadConfig();
    shouldFail("tlsv1");
    shouldFail("tlsv1_1");
    shouldFail("tlsv1_2");
    shouldFail("tlsv1_3");
}

TEST_P(TlsTests, ECDHE_RSA_AES256_GCM_SHA384) {
    memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "ECDHE-RSA-AES256-GCM-SHA384";
    memcached_cfg["ssl_cipher_list"]["tls 1.3"] = "";
    reloadConfig();
    shouldPass("tlsv1_2");
}

TEST_P(TlsTests, DHE_RSA_AES256_SHA256) {
    memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "DHE-RSA-AES256-SHA256";
    memcached_cfg["ssl_cipher_list"]["tls 1.3"] = "";
    reloadConfig();
    shouldPass("tlsv1_2");
}
