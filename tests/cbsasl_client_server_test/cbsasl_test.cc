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
#include <cbsasl/cbsasl.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

cbsasl_conn_t* server;
cbsasl_conn_t* client;

const char* cbpwfile = "cbsasl_test.pw";

char envptr[256]{"ISASL_PWFILE=cbsasl_test.pw"};

struct my_sasl_ctx {
    std::string username;
    cbsasl_secret_t* secret;
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

    static void test_auth(const char* mech) {
        cbsasl_error_t err;
        const char* data;
        unsigned int len;
        const char* serverdata;
        unsigned int serverlen;
        const char* chosenmech;
        struct my_sasl_ctx context;
        cbsasl_callback_t sasl_callbacks[4];

        sasl_callbacks[0].id = CBSASL_CB_USER;
        sasl_callbacks[0].proc = (int (*)(void))&sasl_get_username;
        sasl_callbacks[0].context = &context;
        sasl_callbacks[1].id = CBSASL_CB_AUTHNAME;
        sasl_callbacks[1].proc = (int (*)(void))&sasl_get_username;
        sasl_callbacks[1].context = &context;
        sasl_callbacks[2].id = CBSASL_CB_PASS;
        sasl_callbacks[2].proc = (int (*)(void))&sasl_get_password;
        sasl_callbacks[2].context = &context;
        sasl_callbacks[3].id = CBSASL_CB_LIST_END;
        sasl_callbacks[3].proc = nullptr;
        sasl_callbacks[3].context = nullptr;

        context.username = "mikewied";
        context.secret = (cbsasl_secret_t*)calloc(1, 100);
        memcpy(context.secret->data, "mikepw", 6);
        context.secret->len = 6;

        ASSERT_EQ(CBSASL_OK, cbsasl_client_new(nullptr, nullptr, nullptr,
                                               nullptr, sasl_callbacks, 0,
                                               &client));

        ASSERT_EQ(CBSASL_OK, cbsasl_client_start(client, mech, nullptr,
                                                 &data, &len, &chosenmech));

        ASSERT_EQ(CBSASL_OK,
                  cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                    nullptr, 0, &server));

        err = cbsasl_server_start(server, chosenmech, data, len,
                                  (unsigned char**)&serverdata, &serverlen);
        if (err == CBSASL_OK) {
            free(context.secret);
            cbsasl_dispose(&client);
            cbsasl_dispose(&server);
            return;
        }

        EXPECT_EQ(CBSASL_CONTINUE, err);
        if (serverlen > 0) {
            EXPECT_EQ(CBSASL_CONTINUE, cbsasl_client_step(client, serverdata,
                                                          serverlen,
                                                          nullptr, &data,
                                                          &len));
        }

        while ((err = cbsasl_server_step(server, data, len,
                                         &serverdata, &serverlen)) ==
               CBSASL_CONTINUE) {

            EXPECT_EQ(CBSASL_CONTINUE, cbsasl_client_step(client, serverdata,
                                                          serverlen,
                                                          nullptr, &data,
                                                          &len));
        }

        EXPECT_EQ(CBSASL_OK, err);

        free(context.secret);
        cbsasl_dispose(&client);
        cbsasl_dispose(&server);
    }
};

TEST_F(SaslClientServerTest, PLAIN) {
    test_auth("PLAIN");
}

TEST_F(SaslClientServerTest, CRAM_MD5) {
    test_auth("CRAM-MD5");
}
