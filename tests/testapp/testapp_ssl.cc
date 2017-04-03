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

#include <ctype.h>
#include <errno.h>
#include <evutil.h>
#include <fcntl.h>
#include <getopt.h>
#include <snappy-c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <string>

#include <gtest/gtest.h>
#include <platform/dirutils.h>
#include "config.h"

#include "memcached/openssl.h"
#include "testapp.h"
#include "utilities.h"
#include "utilities/protocol2text.h"

using std::string;
class SslCertTest : public TestappTest {
public:
    static void SetupTestCase() {
        initialize_openssl();
        unique_cJSON_ptr memcached_cfg;
        memcached_cfg.reset(generate_config(0));
        start_memcached_server(memcached_cfg.get());
        reconnect_to_server();
    }
    virtual void SetUp() {
        if ((ssl_ctx = SSL_CTX_new(SSLv23_client_method())) == NULL) {
            fprintf(stderr, "Failed to create openssl client contex\n");
        }
    }
    virtual void TearDown() {
        if (ssl_ctx) {
            SSL_CTX_free(ssl_ctx);
        }
        if (bio) {
            BIO_free_all(bio);
        }
    }
    static void TearDownTestCase() {
        stop_memcached_server();
    }
    bool setKey(string key);
    void set_client_cert_auth(string client_cert_auth, string path,
        string prefix, string delimiter="");
    void selectBucket(string bucketName);
    SSL_CTX* ssl_ctx = NULL;
    BIO* bio = nullptr;
};

void SslCertTest::set_client_cert_auth(string client_cert_auth, string path,
        string prefix, string delimiter) {
    unique_cJSON_ptr memcached_cfg;
    memcached_cfg.reset(generate_config(0));
    // Change the number of worker threads to one so we guarantee that
    // multiple connections are handled by a single worker.
    auto obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "path", path.c_str());
    cJSON_AddStringToObject(obj, "state", client_cert_auth.c_str());
    cJSON_AddStringToObject(obj, "prefix", prefix.c_str());
    cJSON_AddStringToObject(obj, "delimiter", delimiter.c_str());
    cJSON_AddItemToObject(memcached_cfg.get(), "client_cert_auth", obj);
    reconfigure(memcached_cfg);
}

bool SslCertTest::setKey(string key) {
    uint64_t value = 0xdeadbeefdeadcafe;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_storage_command(send.bytes, sizeof(send.bytes),
                                      PROTOCOL_BINARY_CMD_SET, key.c_str(),
                                      key.size(), &value, sizeof(value),
                                      0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                              PROTOCOL_BINARY_RESPONSE_SUCCESS);
    return true;
}

void SslCertTest::selectBucket(string bucketName) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                     PROTOCOL_BINARY_CMD_SELECT_BUCKET,
                     bucketName.c_str(), bucketName.size(),
                     nullptr, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  PROTOCOL_BINARY_CMD_SELECT_BUCKET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}


TEST_F(SslCertTest, LoginWhenDiabled) {
    set_client_cert_auth("enable", "", "");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    EXPECT_EQ(0,
              create_ssl_connection(
                      &ssl_ctx, &bio, "127.0.0.1", port_str, NULL, NULL, 1));
}

TEST_F(SslCertTest, LoginWhenMandatoryWithCert) {
    const std::string cwd = cb::io::getcwd();
    string env = "COUCHBASE_SSL_CLIENT_CERT_PATH=" + cwd + CERTIFICATE_PATH("");
    static char envvar[200];
    snprintf(envvar, sizeof(envvar), "%s", env.c_str());
    putenv(envvar);
    set_client_cert_auth("mandatory", "", "");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    EXPECT_EQ(0,
              create_ssl_connection(
                      &ssl_ctx, &bio, "127.0.0.1", port_str, NULL, NULL, 1));
}

TEST_F(SslCertTest, LoginWhenMandatoryWithoutCert) {
    const std::string cwd = cb::io::getcwd();
    // setting the ssl client path so that client connection succeed
    // in client_connection.cc
    string env = "COUCHBASE_SSL_CLIENT_CERT_PATH=" + cwd + CERTIFICATE_PATH("");
    static char envvar[200];
    snprintf(envvar, sizeof(envvar), "%s", env.c_str());
    putenv(envvar);
    set_client_cert_auth("mandatory", "", "");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    EXPECT_NE(0,
              create_ssl_connection(
                      &ssl_ctx, &bio, "127.0.0.1", port_str, NULL, NULL, 1));
    bio = nullptr;
    ssl_ctx = nullptr;
}

TEST_F(SslCertTest, LoginEnabledWithoutCert) {
    set_client_cert_auth("enable", "", "");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    EXPECT_EQ(0,
              create_ssl_connection(
                      &ssl_ctx, &bio, "127.0.0.1", port_str, NULL, NULL, 1));
}

TEST_F(SslCertTest, LoginEnabledWithCert) {
    const std::string cwd = cb::io::getcwd();
    string env = "COUCHBASE_SSL_CLIENT_CERT_PATH=" + cwd + CERTIFICATE_PATH("");
    static char envvar[200];
    snprintf(envvar, sizeof(envvar), "%s", env.c_str());
    putenv(envvar);
    set_client_cert_auth("enable", "", "");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    EXPECT_EQ(0,
              create_ssl_connection(
                      &ssl_ctx, &bio, "127.0.0.1", port_str, NULL, NULL, 1));
}

TEST_F(SslCertTest, CertAuthDisabledWithRbac) {
    const std::string cwd = cb::io::getcwd();
    string env = "COUCHBASE_SSL_CLIENT_CERT_PATH=" + cwd + CERTIFICATE_PATH("");
    static char envvar[200];
    snprintf(envvar, sizeof(envvar), "%s", env.c_str());
    putenv(envvar);
    set_client_cert_auth("disable", "subject.cn", "abc");
    char port_str[32];
    snprintf(port_str, 32, "%d", ssl_port);
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    sock_ssl = create_connect_ssl_socket(ssl_port, ssl_ctx);
    set_phase_ssl();
    ASSERT_EQ(true, setKey("testkey"));
    reset_bio_mem();
}

TEST_F(SslCertTest, CertAuthEnabledWithRbacAndPath) {
    const std::string cwd = cb::io::getcwd();
    // This will find user as Trond, which as write
    // permission for default bucket
    set_client_cert_auth("enable", "subject.cn", "", " ");
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    set_phase_ssl();
    sock_ssl = create_connect_ssl_socket(ssl_port, ssl_ctx);
    selectBucket("default");
    bool ret = setKey("test");
    ASSERT_EQ(ret, true);
    reset_bio_mem();
}

TEST_F(SslCertTest, CertAuthEnabledRbacUserNotFound) {
    const std::string cwd = cb::io::getcwd();
    char buf[100];
    // This will find user as Norbye, which is not a valid
    // user
    set_client_cert_auth("enable", "subject.cn", "Trond ", "");
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    set_phase_ssl();
    sock_ssl = create_connect_ssl_socket(ssl_port, ssl_ctx);
    // verify socket has been closed
    int rv = socket_recv(sock_ssl, buf, 1);
    ASSERT_EQ(rv, 0);
    reset_bio_mem();
}

TEST_F(SslCertTest, CertAuthEnabledRbacUserNotFoundWithoutPath) {
    const std::string cwd = cb::io::getcwd();
    reset_bio_mem();
    // This will find user as Norbye, which is not a valid
    // user
    set_client_cert_auth("enable", "", "", "");
    auto cert = cwd + CERTIFICATE_PATH("client.pem");
    auto key = cwd + CERTIFICATE_PATH("client.key");
    ASSERT_EQ(1,
              SSL_CTX_use_certificate_file(
                      ssl_ctx, cert.c_str(), SSL_FILETYPE_PEM));
    ASSERT_EQ(1,
              SSL_CTX_use_PrivateKey_file(
                      ssl_ctx, key.c_str(), SSL_FILETYPE_PEM));
    set_phase_ssl();
    sock_ssl = create_connect_ssl_socket(ssl_port, ssl_ctx);
    bool ret = setKey("test");
    ASSERT_EQ(ret, true);
    reset_bio_mem();
}
