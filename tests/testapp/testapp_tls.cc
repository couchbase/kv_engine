/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <platform/compress.h>
#include <platform/dirutils.h>
#include <protocol/mcbp/ewb_encode.h>
#include <algorithm>

class TlsTests : public TestappClientTest {
protected:
    void SetUp() override {
        TestappTest::SetUp();
        // MB-42607: Reduce BIO buffer size from default 8192 to 64 bytes, to
        // exercise SSL_ERROR_WANT_READ / SSL_ERROR_WANT_WRITE code paths.
        memcached_cfg["bio_drain_buffer_sz"] = 64;

        memcached_cfg["ssl_cipher_list"]["tls 1.2"] = "HIGH";
        memcached_cfg["ssl_cipher_list"]["tls 1.3"] =
                "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_"
                "128_GCM_SHA256";

        // MB-51498: Change to the larger certificate to force more cycles of
        // SSL_accept
        const std::string cwd = cb::io::getcwd();
        const std::string pem_path = cwd + CERTIFICATE_PATH("16k_testapp.pem");
        const std::string cert_path =
                cwd + CERTIFICATE_PATH("16k_testapp.cert");
        memcached_cfg["interfaces"][1]["ssl"] = {{"key", pem_path},
                                                 {"cert", cert_path}};

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

INSTANTIATE_TEST_CASE_P(TransportProtocols,
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
