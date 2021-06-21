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
#include "client_cert_config.h"

#include <folly/portability/GTest.h>
#include <memcached/openssl.h>

#include <gsl/gsl-lite.hpp>
#include <nlohmann/json.hpp>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <platform/dirutils.h>

using namespace cb::x509;

TEST(X509, ParseValidSingleEntryFormat) {
    auto config = ClientCertConfig::create(R"({
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
        std::string certPath{SOURCE_ROOT};
        certPath.append("/tests/cert/parse-test.pem");
        certPath = cb::io::sanitizePath(certPath);
        auto data = cb::io::loadFile(certPath);
        auto* certbio = BIO_new_mem_buf(
                const_cast<void*>(static_cast<const void*>(data.data())),
                gsl::narrow<int>(data.size()));
        cert.reset(PEM_read_bio_X509(certbio, nullptr, nullptr, nullptr));
        BIO_free(certbio);
        ASSERT_TRUE(cert.get()) << "Error in reading certificate file: "
                                << certPath;
    }

    static void TearDownTestCase() {
        // Release the certificate
        cert.reset();
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
