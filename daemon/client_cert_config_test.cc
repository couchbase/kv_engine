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
#include "client_cert_config.h"

#include <folly/portability/GTest.h>
#include <memcached/openssl.h>

#include <nlohmann/json.hpp>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <platform/dirutils.h>
#include <gsl/gsl>

using namespace cb::x509;

TEST(X509, ValidateStateDisabled) {
    // When the state is disabled we don't need any more parameters set
    auto config = ClientCertConfig::create(R"({"state": "disable"})"_json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Disabled, config->getMode());
}

TEST(X509, ValidateStateEnable) {
    auto config = ClientCertConfig::create(R"({"state": "enable", "path":
                                            "subject.cn"})"_json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Enabled, config->getMode());
}

TEST(X509, ValidateStateMandatory) {
    auto config = ClientCertConfig::create(R"({"state": "mandatory", "path":
"subject.cn"})"_json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Mandatory, config->getMode());
}

TEST(X509, ValidateInvalidStateValue) {
    EXPECT_THROW(ClientCertConfig::create(R"({"state": 5})"_json),
                 nlohmann::json::exception);
}

TEST(X509, ValidateMissingState) {
    EXPECT_THROW(ClientCertConfig::create(R"({})"_json),
                 nlohmann::json::exception);
}

TEST(X509, ParseValidSingleEntryFormat) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "path": "san.dnsname",
    "prefix": "www.",
     "delimiter": ".,;"
})"_json);
    EXPECT_TRUE(config);
    EXPECT_EQ(1, config->getNumMappings());
    const auto& entry = config->getMapping(0);
    EXPECT_EQ("san.dnsname", entry.path);
    EXPECT_EQ("www.", entry.prefix);
    EXPECT_EQ(".,;", entry.delimiter);
}

TEST(X509, ParseValidMultipleEntryFormat) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.dnsname",
            "prefix": "www.",
            "delimiter": ".,;"
        },
        {
            "path": "subject.cn",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"_json);
    EXPECT_TRUE(config);
    EXPECT_EQ(2, config->getNumMappings());

    // Make sure that the order is preserved
    {
        const auto& entry = config->getMapping(0);

        EXPECT_EQ("san.dnsname", entry.path);
        EXPECT_EQ("www.", entry.prefix);
        EXPECT_EQ(".,;", entry.delimiter);
    }

    {
        const auto& entry = config->getMapping(1);
        EXPECT_EQ("subject.cn", entry.path);
        EXPECT_TRUE(entry.prefix.empty());
        EXPECT_TRUE(entry.delimiter.empty());
    }
}

class SslParseCertTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        OPENSSL_init_ssl(0, NULL);
        SSL_load_error_strings();
        ERR_load_BIO_strings();
        OpenSSL_add_all_algorithms();

        std::string certPath{SOURCE_ROOT};
        certPath.append("/tests/cert/parse-test.pem");
        cb::io::sanitizePath(certPath);
        auto data = cb::io::loadFile(certPath);
        auto* certbio = BIO_new_mem_buf(
                const_cast<void*>(static_cast<const void*>(data.data())),
                gsl::narrow<int>(data.size()));
        cert.reset(PEM_read_bio_X509(certbio, NULL, 0, NULL));
        BIO_free(certbio);
        ASSERT_TRUE(cert.get()) << "Error in reading certificate file: "
                                << certPath;
    }

    static void TearDownTestCase() {
        // Release the certificate
        cert.reset();

        // Global OpenSSL cleanup:
        ENGINE_cleanup();
        CONF_modules_unload(1);
        ERR_free_strings();
        EVP_cleanup();
        CRYPTO_cleanup_all_ex_data();
    }

    static cb::openssl::unique_x509_ptr cert;
};

cb::openssl::unique_x509_ptr SslParseCertTest::cert;

/*
 * The certificate used in these tests contains the following entry
 * for the common name:
 *
 *     CN=testappname
 */

TEST_F(SslParseCertTest, TestCN_EntireField) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"_json);
    EXPECT_TRUE(config);
    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "testappname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefix) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": ""
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefixAndDelimiter) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "n"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "app");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndMultipleDelimiters) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "np"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "a");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndUnknownDelimiters) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "x"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_PrefixMissing) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "rand",
            "delimiter": "n"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::NoMatch);
}

/*
 * The certificate used in these tests contains the following entry
 * for the SAN:
 *
 *       URI:urn:li:testurl_1
 *       URI:email:testapp@example.com
 *       URI:couchbase://myuser@mycluster/mybucket
 *       DNS:MyServerName
 */

TEST_F(SslParseCertTest, TestSAN_URI_EntireField) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    // We should match the first entry
    ASSERT_EQ(statusPair.second, "urn:li:testurl_1");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithoutDelimiter) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "urn:",
            "delimiter": "_"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "li:testurl");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithDelimiter) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "urn:",
            "delimiter": ":"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "li");
}

TEST_F(SslParseCertTest, TestSAN_URI_MissingPrefix) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "uri",
            "delimiter": ":-_"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::NoMatch);
}

TEST_F(SslParseCertTest, TestSAN_URI_Email) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "email:",
            "delimiter": "@"
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "testapp");
}

TEST_F(SslParseCertTest, TestSAN_DNS_EntireField) {
    auto config = ClientCertConfig::create(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.dnsname",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "MyServerName");
}

TEST_F(SslParseCertTest, TestMatchForMultipleRules) {
    auto config = ClientCertConfig::create(R"({
  "state": "enable",
  "prefixes": [
    {
      "path": "subject.cn",
      "prefix": "NoMatch1",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch2",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch3",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch4",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch5",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch6",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch7",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch8",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch9",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch10",
      "delimiter": ""
    },
    {
      "path": "subject.cn",
      "prefix": "NoMatch11",
      "delimiter": ""
    },
    {
      "path": "san.dnsname",
      "prefix": "",
      "delimiter": ""
    }
  ]
})"_json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "MyServerName");
}
