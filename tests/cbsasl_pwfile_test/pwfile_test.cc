/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <gtest/gtest.h>

#include <platform/platform.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <cbsasl/cbsasl.h>

#include "cbsasl/pwfile.h"

const char* cbpwfile = "pwfile_test.pw";
char envptr[256]{"ISASL_PWFILE=pwfile_test.pw"};

const char* user1 = "mikewied";
const char* pass1 = "mikepw";
const char* user2 = "cseo";
const char* pass2 = "seopw";
const char* user3 = "jlim";
const char* pass3 = "limpw";

class PasswordFileTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        fprintf(fp, "%s %s\n%s %s\n%s %s\n",
                user1, pass1,
                user2, pass2,
                user3, pass3);
        ASSERT_EQ(0, fclose(fp));
        putenv(envptr);

        ASSERT_EQ(CBSASL_OK, load_user_db());
    }

    static void TearDownTestCase() {
        free_user_ht();
        ASSERT_EQ(0, remove(cbpwfile));
    }
};

TEST_F(PasswordFileTest, VerifyExpectedUsers) {
    const char* password;

    password = find_pw(user1);
    ASSERT_NE(nullptr, password);
    ASSERT_STREQ(pass1, password);

    password = find_pw(user2);
    ASSERT_NE(nullptr, password);
    ASSERT_STREQ(pass2, password);

    password = find_pw(user3);
    ASSERT_NE(nullptr, password);
    ASSERT_STREQ(pass3, password);
}

TEST_F(PasswordFileTest, VerifyUserNotFound) {
    EXPECT_EQ(nullptr, find_pw("nobody"));
}

TEST_F(PasswordFileTest, VerifyUsernameSuperset) {
    std::string user(user1);
    user.append(" ");
    EXPECT_EQ(nullptr, find_pw(user.c_str()));
}

TEST_F(PasswordFileTest, VerifyUsernameSubset) {
    std::string user(user1);
    user.resize(user.length() - 1);
    EXPECT_EQ(nullptr, find_pw(user.c_str()));
}
