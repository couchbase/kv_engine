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

#include "config.h"
#include "client_cert_config.h"

#include <cJSON_utils.h>
#include <gtest/gtest.h>
#include <memcached/openssl.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <platform/dirutils.h>
#include <platform/memorymap.h>

#include <string>

using namespace cb::x509;

TEST(X509, ValidateStateDisabled) {
    // When the state is disabled we don't need any more parameters set
    unique_cJSON_ptr json(cJSON_Parse(R"({"state": "disable"})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Disabled, config->getMode());
}

TEST(X509, ValidateStateEnable) {
    unique_cJSON_ptr json(
            cJSON_Parse(R"({"state": "enable", "path": "subject.cn"})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Enabled, config->getMode());
}

TEST(X509, ValidateStateMandatory) {
    unique_cJSON_ptr json(
            cJSON_Parse(R"({"state": "mandatory", "path": "subject.cn"})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);
    EXPECT_EQ(Mode::Mandatory, config->getMode());
}

TEST(X509, ValidateInvalidStateValue) {
    unique_cJSON_ptr json(cJSON_Parse(R"({"state": 5})"));
    EXPECT_TRUE(json);
    EXPECT_THROW(ClientCertConfig::create(*json), std::invalid_argument);
}

TEST(X509, ValidateMissingState) {
    unique_cJSON_ptr json(cJSON_Parse(R"({})"));
    EXPECT_TRUE(json);
    EXPECT_THROW(ClientCertConfig::create(*json), std::invalid_argument);
}

TEST(X509, ParseValidSingleEntryFormat) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "path": "san.dnsname",
    "prefix": "www.",
     "delimiter": ".,;"
})"));

    EXPECT_TRUE(json);

    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);
    EXPECT_EQ(1, config->getNumMappings());
    const auto& entry = config->getMapping(0);
    EXPECT_EQ("san.dnsname", entry.path);
    EXPECT_EQ("www.", entry.prefix);
    EXPECT_EQ(".,;", entry.delimiter);
}

TEST(X509, ParseValidMultipleEntryFormat) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
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
})"));

    EXPECT_TRUE(json);

    auto config = ClientCertConfig::create(*json);
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
#if OPENSSL_VERSION_NUMBER < 0x10100000L
        CRYPTO_malloc_init();
        SSL_library_init();
#else
        OPENSSL_init_ssl(0, NULL);
#endif
        SSL_load_error_strings();
        ERR_load_BIO_strings();
        OpenSSL_add_all_algorithms();

        std::string certPath{SOURCE_ROOT};
        certPath.append("/tests/cert/parse-test.pem");
#ifdef WIN32
        std::replace(certPath.begin(), certPath.end(), '/', '\\');
#endif

        cb::MemoryMappedFile map(certPath.c_str(),
                                 cb::MemoryMappedFile::Mode::RDONLY);
        map.open();
        auto* certbio = BIO_new_mem_buf(reinterpret_cast<char*>(map.getRoot()),
                                        map.getSize());
        cert.reset(PEM_read_bio_X509(certbio, NULL, 0, NULL));
        BIO_free(certbio);
        ASSERT_TRUE(cert.get()) << "Error in reading certificate file: "
                                << certPath;

        map.close();
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

#if OPENSSL_VERSION_NUMBER < 0x10100000L
        // per-thread cleanup:
        ERR_remove_state(0);

        // Newer versions of openssl (1.0.2a) have a the function
        // SSL_COMP_free_compression_methods() to perform this;
        // however we arn't that new...
        sk_SSL_COMP_free(SSL_COMP_get_compression_methods());
#endif
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
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);
    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "testappname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefix) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": ""
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefixAndDelimiter) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "n"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "app");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndMultipleDelimiters) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "np"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "a");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndUnknownDelimiters) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "test",
            "delimiter": "x"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_PrefixMissing) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "subject.cn",
            "prefix": "rand",
            "delimiter": "n"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
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
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    // We should match the first entry
    ASSERT_EQ(statusPair.second, "urn:li:testurl_1");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithoutDelimiter) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "urn:",
            "delimiter": "_"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "li:testurl");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithDelimiter) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "urn:",
            "delimiter": ":"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "li");
}

TEST_F(SslParseCertTest, TestSAN_URI_MissingPrefix) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "uri",
            "delimiter": ":-_"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());
    ASSERT_EQ(statusPair.first, cb::x509::Status::NoMatch);
}

TEST_F(SslParseCertTest, TestSAN_URI_Email) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.uri",
            "prefix": "email:",
            "delimiter": "@"
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "testapp");
}

TEST_F(SslParseCertTest, TestSAN_DNS_EntireField) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
    "state": "enable",
    "prefixes": [
        {
            "path": "san.dnsname",
            "prefix": "",
            "delimiter": ""
        }
    ]
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "MyServerName");
}

TEST_F(SslParseCertTest, TestMatchForMultipleRules) {
    unique_cJSON_ptr json(cJSON_Parse(R"({
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
})"));
    EXPECT_TRUE(json);
    auto config = ClientCertConfig::create(*json);
    EXPECT_TRUE(config);

    auto statusPair = config->lookupUser(cert.get());

    ASSERT_EQ(statusPair.first, cb::x509::Status::Success);
    ASSERT_EQ(statusPair.second, "MyServerName");
}
