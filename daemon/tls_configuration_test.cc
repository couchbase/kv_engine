/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "json_validator_test.h"
#include "tls_configuration.h"
#include <boost/filesystem/path.hpp>
#include <platform/base64.h>

/// Helper method to get one of the files we've got stored for
/// unit tests
static std::string getCertFile(const std::string& filename) {
    auto root = boost::filesystem::path(OBJECT_ROOT);
    return (root / "tests" / "cert" / "root" / filename).generic_string();
}

class TlsConfigurationFormatTest : public JsonValidatorTest {
public:
    TlsConfigurationFormatTest()
        : JsonValidatorTest({{"private key", getCertFile("ca_root.key")},
                             {"certificate chain", getCertFile("ca_root.cert")},
                             {"CA file", getCertFile("ca_root.cert")},
                             {"minimum version", "TLS 1.2"},
                             {"cipher list",
                              {{"TLS 1.2", "HIGH"},
                               {"TLS 1.3", "TLS_AES_256_GCM_SHA384"}}},
                             {"cipher order", true},
                             {"client cert auth", "mandatory"}}) {
    }

protected:
    void expectFail(const nlohmann::json& json) override {
        EXPECT_THROW(TlsConfiguration::validate(json), std::invalid_argument)
                << json.dump();
    }

    void expectSuccess(const nlohmann::json& json) override {
        try {
            TlsConfiguration::validate(json);
        } catch (const std::exception& e) {
            FAIL() << e.what() << " " << json.dump();
        }
    }
};

/// Start off by testing that we can parse a legal configuration. If this
/// fails all other tests will fail as we base upon this configuration.
TEST_F(TlsConfigurationFormatTest, Legal) {
    expectSuccess(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, PrivateKey) {
    acceptString("private key");
    legalSpec.erase((legalSpec.find("private key")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CertificateChain) {
    acceptString("certificate chain");
    legalSpec.erase((legalSpec.find("certificate chain")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CaFile) {
    acceptString("CA file");
    legalSpec["CA file"] = "foo";
    expectSuccess(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, MinimumTlsVersion) {
    acceptString("minimum version",
                 std::vector<std::string>{
                         {"TLS 1"}, {"TLS 1.1"}, {"TLS 1.2"}, {"TLS 1.3"}});
    legalSpec.erase((legalSpec.find("minimum version")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CipherOrder) {
    acceptBoolean("cipher order");
    legalSpec.erase((legalSpec.find("cipher order")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, ClientCertAuth) {
    acceptString(
            "client cert auth",
            std::vector<std::string>{{"mandatory"}, {"enabled"}, {"disabled"}});
    legalSpec.erase((legalSpec.find("client cert auth")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CipherList) {
    acceptObject("cipher list");
    legalSpec.erase((legalSpec.find("cipher list")));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CipherList_1_2) {
    legalSpec["cipher list"]["TLS 1.2"] = 1;
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.2"] = false;
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.2"] = nlohmann::json::array();
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.2"] = nlohmann::json::object();
    expectFail(legalSpec);
    auto iter = legalSpec.find("cipher list");
    iter->erase(iter->find("TLS 1.2"));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, CipherList_1_3) {
    legalSpec["cipher list"]["TLS 1.3"] = 1;
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.3"] = false;
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.3"] = nlohmann::json::array();
    expectFail(legalSpec);
    legalSpec["cipher list"]["TLS 1.3"] = nlohmann::json::object();
    expectFail(legalSpec);
    auto iter = legalSpec.find("cipher list");
    iter->erase(iter->find("TLS 1.3"));
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, UnknownCipherList) {
    legalSpec["cipher list"]["foo"] = "bar";
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, Password) {
    // The password must be base64 encoded so it'll fail if the string isn't
    // that.. So go ahead and add a single base64 encded string so we can
    // check that it must be a string
    acceptString("password",
                 std::vector<std::string>{{cb::base64::encode("bar", false)}});
}

TEST_F(TlsConfigurationFormatTest, PasswordNotBase64) {
    legalSpec["password"] = "..";
    expectFail(legalSpec);
}

TEST_F(TlsConfigurationFormatTest, UnknownKeys) {
    legalSpec["foo"] = "bar";
    expectFail(legalSpec);
}

class TlsConfigurationTest : public ::testing::Test {
public:
    TlsConfigurationTest()
        : legalSpec({{"private key", getCertFile("ca_root.key")},
                     {"certificate chain", getCertFile("ca_root.cert")},
                     {"CA file", getCertFile("ca_root.cert")},
                     {"minimum version", "TLS 1.2"},
                     {"cipher list",
                      {{"TLS 1.2", "HIGH"},
                       {"TLS 1.3", "TLS_AES_256_GCM_SHA384"}}},
                     {"cipher order", true},
                     {"client cert auth", "mandatory"}}) {
    }

protected:
    nlohmann::json legalSpec;
};

TEST_F(TlsConfigurationTest, Basic) {
    try {
        TlsConfiguration configuration(legalSpec);
        auto ssl = configuration.createClientSslHandle();
        ASSERT_TRUE(ssl);
        auto* ctx = SSL_get_SSL_CTX(ssl.get());
        ASSERT_NE(nullptr, ctx);
    } catch (const std::exception& e) {
        FAIL() << e.what() << std::endl << legalSpec.dump();
    }
}

TEST_F(TlsConfigurationTest, PasswordNotUsedIfFileNotEncrypted) {
    try {
        // We don't try to decode the password until it is actually used
        // Lets use something which isn't base64 encoded so that it would
        // fail to decode that if it ever was used
        legalSpec["password"] = ".";
        TlsConfiguration configuration(legalSpec);
    } catch (const std::exception& e) {
        FAIL() << e.what() << std::endl << legalSpec.dump();
    }
}

TEST_F(TlsConfigurationTest, PasswordProtectedKey) {
    try {
        legalSpec["password"] =
                cb::base64::encode("This is the passphrase", true);
        legalSpec["private key"] = getCertFile("ca_root_encrypted.key");
        TlsConfiguration configuration(legalSpec);
    } catch (const std::exception& e) {
        FAIL() << e.what() << std::endl << legalSpec.dump();
    }
}

TEST_F(TlsConfigurationTest, PasswordProtectedWrongKey) {
    try {
        legalSpec["password"] =
                cb::base64::encode("This isn't the passphrase", true);
        legalSpec["private key"] = getCertFile("ca_root_encrypted.key");
        TlsConfiguration configuration(legalSpec);
        FAIL() << "For some reason it accepted the wrong key";
    } catch (const CreateSslContextException& e) {
        EXPECT_EQ("SSL_CTX_use_PrivateKey_file", e.error["function"])
                << e.what();
    }
}

TEST_F(TlsConfigurationTest, MissingPrivateKey) {
    try {
        legalSpec["private key"] = getCertFile("testapp.pe");
        TlsConfiguration configuration(legalSpec);
        FAIL() << "Invalid file should be detected";
    } catch (const CreateSslContextException& e) {
        EXPECT_EQ("SSL_CTX_use_PrivateKey_file", e.error["function"]);
    }
}

TEST_F(TlsConfigurationTest, MissingCertificateChain) {
    try {
        legalSpec["certificate chain"] = getCertFile("testapp.pe");
        TlsConfiguration configuration(legalSpec);
        FAIL() << "Invalid file should be detected";
    } catch (const CreateSslContextException& e) {
        EXPECT_EQ("SSL_CTX_use_certificate_chain_file", e.error["function"]);
    }
}
