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
#include "cbsasl/pwfile.h"
#include "cbsasl/util.h"
#include <cbsasl/cbsasl.h>
#include "cbsasl/cbsasl_internal.h"

#include <algorithm>
#include <array>
#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char* cbpwfile = "sasl_server_test.pw";

char envptr[256]{"ISASL_PWFILE=sasl_server_test.pw"};

static std::string mechanisms;

static int sasl_getopt_callback(void*, const char*,
                                const char* option,
                                const char** result,
                                unsigned* len) {
    if (option == nullptr || result == nullptr || len == nullptr) {
        return CBSASL_BADPARAM;
    }

    if (strcmp(option, "sasl mechanisms") == 0) {
        *result = mechanisms.c_str();
        *len = mechanisms.length();
        return CBSASL_OK;
    }

    return CBSASL_FAIL;
}

class SaslServerTest : public ::testing::Test {
protected:
    void SetUp() {
        std::array<cbsasl_callback_t, 3> server_sasl_callback;
        int ii = 0;
        server_sasl_callback[ii].id = CBSASL_CB_GETOPT;
        server_sasl_callback[ii].proc = (int (*)(void))sasl_getopt_callback;
        server_sasl_callback[ii++].context = nullptr;
        server_sasl_callback[ii].id = CBSASL_CB_LIST_END;
        server_sasl_callback[ii].proc = nullptr;
        server_sasl_callback[ii].context = nullptr;

        ASSERT_EQ(CBSASL_OK, cbsasl_server_init(server_sasl_callback.data(),
                                                "cbsasl_server_test"));
        ASSERT_EQ(CBSASL_OK,
                  cbsasl_server_new(nullptr, nullptr, nullptr, nullptr, nullptr,
                                    nullptr, 0, &conn));
    }

    void TearDown() {
        cbsasl_dispose(&conn);
        ASSERT_EQ(CBSASL_OK, cbsasl_server_term());
    }

    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        fprintf(fp, "mikewied mikepw\ncseo cpw\njlim jpw\nnopass\n");
        ASSERT_EQ(0, fclose(fp));

        putenv(envptr);

#ifdef HAVE_PKCS5_PBKDF2_HMAC
        mechanisms.append("SCRAM-SHA512 SCRAM-SHA256 ");
#endif

#ifdef HAVE_PKCS5_PBKDF2_HMAC_SHA1
        mechanisms.append("SCRAM-SHA1 ");
#endif
        mechanisms.append("PLAIN");
    }

    static void TearDownTestCase() {
        ASSERT_EQ(0, remove(cbpwfile));
        free_user_ht();
    }

protected:
    cbsasl_conn_t* conn;
};

TEST_F(SaslServerTest, ListMechs) {
    const char* mechs = nullptr;
    unsigned len = 0;
    cbsasl_error_t err = cbsasl_listmech(conn, nullptr, nullptr, " ",
                                         nullptr, &mechs, &len, nullptr);
    ASSERT_EQ(CBSASL_OK, err);
    EXPECT_EQ(mechanisms, std::string(mechs, len));
}

TEST_F(SaslServerTest, ListMechsBadParam) {
    const char* mechs = nullptr;
    unsigned len = 0;
    cbsasl_error_t err = cbsasl_listmech(conn, nullptr, nullptr, ",",
                                         nullptr, nullptr, &len, nullptr);
    ASSERT_EQ(CBSASL_BADPARAM, err);

    err = cbsasl_listmech(nullptr, nullptr, nullptr, ",",
                          nullptr, &mechs, &len, nullptr);
    ASSERT_EQ(CBSASL_BADPARAM, err);
}

TEST_F(SaslServerTest, ListMechsSpecialized) {
    const char* mechs = nullptr;
    unsigned len = 0;
    int num;
    cbsasl_error_t err = cbsasl_listmech(conn, nullptr, "(", ",",
                                         ")", &mechs, &len, &num);
    ASSERT_EQ(CBSASL_OK, err);
    std::string mechlist(mechs, len);
    std::string expected("(" + mechanisms + ")");
    std::replace(expected.begin(), expected.end(), ' ', ',');
    EXPECT_EQ(expected, mechlist);
}

TEST_F(SaslServerTest, BadMech) {
    cbsasl_error_t err = cbsasl_server_start(conn, "bad_mech", nullptr, 0,
                                             nullptr, nullptr);
    ASSERT_EQ(CBSASL_NOMECH, err);
}

TEST_F(SaslServerTest, PlainCorrectPassword) {
    /* Normal behavior */
    const char* output = nullptr;
    unsigned outputlen = 0;
    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "\0mikewied\0mikepw", 16, &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    cb_free((void*)output);
}

TEST_F(SaslServerTest, PlainWrongPassword) {
    const char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "\0mikewied\0badpPW", 16, &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_PWERR, err);
    cb_free((void*)output);
}

TEST_F(SaslServerTest, PlainNoPassword) {
    const char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "\0nopass\0", 8,
                                             &output, &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    cb_free((void*)output);
}

TEST_F(SaslServerTest, PlainWithAuthzid) {
    const char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN",
                                             "funzid\0mikewied\0mikepw", 22,
                                             &output,
                                             &outputlen);
    ASSERT_EQ(CBSASL_OK, err);
    cb_free((void*)output);
}

TEST_F(SaslServerTest, PlainWithNoPwOrUsernameEndingNull) {
    const char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "funzid\0mikewied",
                                             15, &output, &outputlen);
    ASSERT_NE(CBSASL_OK, err);
    cb_free((void*)output);
}

TEST_F(SaslServerTest, PlainNoNullAtAll) {
    const char* output = nullptr;
    unsigned outputlen = 0;

    cbsasl_error_t err = cbsasl_server_start(conn, "PLAIN", "funzidmikewied",
                                             14, &output, &outputlen);
    ASSERT_NE(CBSASL_OK, err);
    cb_free((void*)output);
}

class SaslLimitMechServerTest : public SaslServerTest {
protected:
    void SetUp() {
        mechanisms = "PLAIN";
        SaslServerTest::SetUp();
    }
};

TEST_F(SaslLimitMechServerTest, TestDisableMechList) {
    const char* mechs = nullptr;
    unsigned len = 0;
    int num;
    cbsasl_error_t err = cbsasl_listmech(conn, nullptr, "(", ",",
                                         ")", &mechs, &len, &num);
    ASSERT_EQ(CBSASL_OK, err);
    std::string mechlist(mechs, len);
    EXPECT_EQ(std::string("(PLAIN)"), mechlist);
}
