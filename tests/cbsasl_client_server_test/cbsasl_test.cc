/*
 *     Copyright 2015 Couchbase, Inc.
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
#include <cbsasl/cbsasl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <array>
#include <cbsasl/cbcrypto.h>

const char* cbpwfile = "cbsasl_test.pw";

char envptr[256]{"ISASL_PWFILE=cbsasl_test.pw"};

struct my_sasl_ctx {
    std::string username;
    cbsasl_secret_t* secret;
    std::string nonce;
};

static int sasl_get_username(void* context, int id, const char** result,
                             unsigned int* len) {
    struct my_sasl_ctx* ctx = reinterpret_cast<my_sasl_ctx*>(context);
    if (!context || !result ||
        (id != CBSASL_CB_USER && id != CBSASL_CB_AUTHNAME)) {
        return CBSASL_BADPARAM;
    }

    *result = ctx->username.c_str();
    if (len) {
        *len = (unsigned int)ctx->username.length();
    }

    return CBSASL_OK;
}

static int sasl_get_password(cbsasl_conn_t* conn, void* context, int id,
                             cbsasl_secret_t** psecret) {
    struct my_sasl_ctx* ctx = reinterpret_cast<my_sasl_ctx*>(context);
    if (!conn || !psecret || id != CBSASL_CB_PASS || ctx == nullptr) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

static int sasl_get_cnonce(void* context, int id, const char** result,
                           unsigned int* len) {
    if (context == nullptr || id != CBSASL_CB_CNONCE || result == nullptr ||
        len == nullptr) {
        return CBSASL_BADPARAM;
    }

    struct my_sasl_ctx* ctx = reinterpret_cast<my_sasl_ctx*>(context);
    *result = ctx->nonce.data();
    *len = ctx->nonce.length();

    return CBSASL_OK;
}

static int client_log(void*, int, const char *message) {
    std::cerr << "C: " << message << std::endl;
    return CBSASL_OK;
}

static int server_log(void*, int, const char *message) {
    std::cerr << "S: " << message << std::endl;
    return CBSASL_OK;
}

class SaslClientServerTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        fprintf(fp, "mikewied mikepw \n");
        ASSERT_EQ(0, fclose(fp));

        putenv(envptr);
        ASSERT_EQ(CBSASL_OK, cbsasl_server_init(nullptr,
                                                "cbsasl_client_server_test"));
    }

    static void TearDownTestCase() {
        ASSERT_EQ(CBSASL_OK, cbsasl_server_term());
        ASSERT_EQ(0, remove(cbpwfile));
    }

    // You may set addNonce to true to have it use a fixed nonce
    // for debugging purposes
    void test_auth(const char* mech, bool addNonce = false) {
        struct my_sasl_ctx client_context;
        std::array<cbsasl_callback_t, 6> sasl_callbacks;
        int ii = 0;
        sasl_callbacks[ii].id = CBSASL_CB_USER;
        sasl_callbacks[ii].proc = (int (*)(void))&sasl_get_username;
        sasl_callbacks[ii++].context = &client_context;
        sasl_callbacks[ii].id = CBSASL_CB_AUTHNAME;
        sasl_callbacks[ii].proc = (int (*)(void))&sasl_get_username;
        sasl_callbacks[ii++].context = &client_context;
        sasl_callbacks[ii].id = CBSASL_CB_PASS;
        sasl_callbacks[ii].proc = (int (*)(void))&sasl_get_password;
        sasl_callbacks[ii++].context = &client_context;
        if (addNonce) {
            sasl_callbacks[ii].id = CBSASL_CB_CNONCE;
            sasl_callbacks[ii].proc = (int (*)(void))&sasl_get_cnonce;
            sasl_callbacks[ii++].context = &client_context;
        }
        sasl_callbacks[ii].id = CBSASL_CB_LOG;
        sasl_callbacks[ii].proc = (int (*)(void))&client_log;
        sasl_callbacks[ii++].context = &client_context;
        sasl_callbacks[ii].id = CBSASL_CB_LIST_END;
        sasl_callbacks[ii].proc = nullptr;
        sasl_callbacks[ii].context = nullptr;

        client_context.username = "mikewied";
        client_context.secret = (cbsasl_secret_t*)calloc(1, 100);
        memcpy(client_context.secret->data, "mikepw", 6);
        client_context.secret->len = 6;
        client_context.nonce = "fyko+d2lbbFgONRv9qkxdawL";

        cbsasl_conn_t* conn = nullptr;
        ASSERT_EQ(CBSASL_OK, cbsasl_client_new(nullptr, nullptr, nullptr,
                                               nullptr, sasl_callbacks.data(),
                                               0,
                                               &conn));
        client.reset(conn);
        conn = nullptr;

        const char* chosenmech;
        const char* data;
        unsigned int len;
        ASSERT_EQ(CBSASL_OK, cbsasl_client_start(client.get(), mech, nullptr,
                                                 &data, &len, &chosenmech));

        struct my_sasl_ctx server_context;
        server_context.nonce = "3rfcNHYJY1ZVvWVs7j";
        std::array<cbsasl_callback_t, 3> server_sasl_callback;

        ii = 0;
        if (addNonce) {
            server_sasl_callback[ii].id = CBSASL_CB_CNONCE;
            server_sasl_callback[ii].proc = (int (*)(void))&sasl_get_cnonce;
            server_sasl_callback[ii].context = &server_context;
            ++ii;
        }
        server_sasl_callback[ii].id = CBSASL_CB_LOG;
        server_sasl_callback[ii].proc = (int (*)(void))&server_log;
        server_sasl_callback[ii++].context = nullptr;
        server_sasl_callback[ii].id = CBSASL_CB_LIST_END;
        server_sasl_callback[ii].proc = nullptr;
        server_sasl_callback[ii].context = nullptr;

        ASSERT_EQ(CBSASL_OK,
                  cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                    server_sasl_callback.data(), 0, &conn));
        server.reset(conn);

        cbsasl_error_t err;
        const char* serverdata;
        unsigned int serverlen;
        err = cbsasl_server_start(server.get(), chosenmech, data, len,
                                  &serverdata, &serverlen);
        if (err == CBSASL_OK) {
            free(client_context.secret);
            return;
        }

        EXPECT_EQ(CBSASL_CONTINUE, err);
        if (serverlen > 0) {
            EXPECT_EQ(CBSASL_CONTINUE, cbsasl_client_step(client.get(),
                                                          serverdata,
                                                          serverlen,
                                                          nullptr, &data,
                                                          &len));
        }

        while ((err = cbsasl_server_step(server.get(), data, len,
                                         &serverdata, &serverlen)) ==
               CBSASL_CONTINUE) {

            EXPECT_EQ(CBSASL_CONTINUE, cbsasl_client_step(client.get(),
                                                          serverdata,
                                                          serverlen,
                                                          nullptr, &data,
                                                          &len));
        }

        EXPECT_EQ(CBSASL_OK, err);

        free(client_context.secret);
    }

protected:
    unique_cbsasl_conn_t server;
    unique_cbsasl_conn_t client;
};

TEST_F(SaslClientServerTest, PLAIN) {
    test_auth("PLAIN");
}

TEST_F(SaslClientServerTest, SCRAM_SHA1) {
    if (Couchbase::Crypto::isSupported(Couchbase::Crypto::Algorithm::SHA1)) {
        test_auth("SCRAM-SHA1");
    }
}

TEST_F(SaslClientServerTest, SCRAM_SHA256) {
    if (Couchbase::Crypto::isSupported(Couchbase::Crypto::Algorithm::SHA256)) {
        test_auth("SCRAM-SHA256");
    }
}

TEST_F(SaslClientServerTest, SCRAM_SHA512) {
    if (Couchbase::Crypto::isSupported(Couchbase::Crypto::Algorithm::SHA512)) {
        test_auth("SCRAM-SHA512");
    }
}

TEST_F(SaslClientServerTest, AutoSelectMechamism) {
    test_auth("(SCRAM-SHA512,SCRAM-SHA256,SCRAM-SHA1,CRAM-MD5,PLAIN)");
}
