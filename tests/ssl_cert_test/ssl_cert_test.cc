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

#include <daemon/settings.h>
#include <gtest/gtest.h>
#include <memcached/openssl.h>
#include <openssl/engine.h>
#include <platform/dirutils.h>
#include <platform/memorymap.h>

#include <string>

class SslParseCertTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        CRYPTO_malloc_init();
        SSL_library_init();
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
        ASSERT_TRUE(cert.get())
                << "Error in reading certificate file: " << certPath;

        map.close();
    }

    static void TearDownTestCase() {
        // Release the certificate
        cert.reset();

        // Global OpenSSL cleanup:
        CRYPTO_set_locking_callback(NULL);
        CRYPTO_set_id_callback(NULL);
        ENGINE_cleanup();
        CONF_modules_unload(1);
        ERR_free_strings();
        EVP_cleanup();
        CRYPTO_cleanup_all_ex_data();

        // per-thread cleanup:
        ERR_remove_state(0);

        // Newer versions of openssl (1.0.2a) have a the function
        // SSL_COMP_free_compression_methods() to perform this;
        // however we arn't that new...
        sk_SSL_COMP_free(SSL_COMP_get_compression_methods());
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
    auto csu = CertUserFromSubject("cn", "", "");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "testappname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefix) {
    auto csu = CertUserFromSubject("cn", "test", "");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_WithPrefixAndDelimiter) {
    auto csu = CertUserFromSubject("cn", "test", "n");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "app");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndMultipleDelimiters) {
    auto csu = CertUserFromSubject("cn", "test", "np");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "a");
}

TEST_F(SslParseCertTest, TestCN_PrefixAndUnknownDelimiters) {
    auto csu = CertUserFromSubject("cn", "test", "x");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "appname");
}

TEST_F(SslParseCertTest, TestCN_PrefixMissing) {
    auto csu = CertUserFromSubject("cn", "rand", "n");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Error);
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
    auto csu = CertUserFromSAN("uri", "", "");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    // We should match the first entry
    ASSERT_EQ(statusPair.second, "urn:li:testurl_1");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithoutDelimiter) {
    auto csu = CertUserFromSAN("uri", "urn:", "_");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "li:testurl");
}

TEST_F(SslParseCertTest, TestSAN_URI_UrnWithDelimiter) {
    auto csu = CertUserFromSAN("uri", "urn:", ":");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "li");
}

TEST_F(SslParseCertTest, TestSAN_URI_MissingPrefix) {
    auto csu = CertUserFromSAN("uri", "uri", ":-_");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Error);
}

TEST_F(SslParseCertTest, TestSAN_URI_Email) {
    auto csu = CertUserFromSAN("uri", "email:", "@");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "testapp");
}

TEST_F(SslParseCertTest, TestSAN_DNS_EntireField) {
    auto csu = CertUserFromSAN("dnsname", "", "");
    auto statusPair = csu.getUser(cert.get());
    ASSERT_EQ(statusPair.first, ClientCertUser::Status::Success);
    ASSERT_EQ(statusPair.second, "MyServerName");
}
