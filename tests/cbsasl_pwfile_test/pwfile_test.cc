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

const std::string user1{"mikewied"};
const std::string pass1{"mikepw"};
const std::string user2{"cseo"};
const std::string pass2{"seopw"};
const std::string user3{"jlim"};
const std::string pass3{"limpw"};

class PasswordFileTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        FILE* fp = fopen(cbpwfile, "w");
        ASSERT_NE(nullptr, fp);

        fprintf(fp, "%s %s\n%s %s\n%s %s\n",
                user1.c_str(), pass1.c_str(),
                user2.c_str(), pass2.c_str(),
                user3.c_str(), pass3.c_str());
        ASSERT_EQ(0, fclose(fp));
        putenv(envptr);

        ASSERT_EQ(CBSASL_OK, load_user_db());
    }

    static void TearDownTestCase() {
        free_user_ht();
        ASSERT_EQ(0, remove(cbpwfile));
    }

    bool find_pw(const std::string& user, std::string& password) {
        Couchbase::User u;
        if (!find_user(user, u)) {
            return false;
        }

        if (!u.isDummy()) {
            try {
                const auto& meta = u.getPassword(Mechanism::PLAIN);
                password.assign(meta.getPassword());
                return true;
            } catch (...) { ;
            }
        }
        return false;
    }
};

TEST_F(PasswordFileTest, VerifyExpectedUsers) {
    std::string password;
    using namespace Couchbase::Crypto;

    ASSERT_TRUE(find_pw(user1, password));
    ASSERT_EQ(digest(Algorithm::SHA1, pass1), password);

    ASSERT_TRUE(find_pw(user2, password));
    ASSERT_EQ(digest(Algorithm::SHA1, pass2), password);

    ASSERT_TRUE(find_pw(user3, password));
    ASSERT_EQ(digest(Algorithm::SHA1, pass3), password);
}

TEST_F(PasswordFileTest, VerifyUserNotFound) {
    std::string password;
    EXPECT_FALSE(find_pw("nobody", password));
}

TEST_F(PasswordFileTest, VerifyUsernameSuperset) {
    std::string user(user1);
    user.append(" ");
    std::string password;
    EXPECT_FALSE(find_pw(user, password));
}

TEST_F(PasswordFileTest, VerifyUsernameSubset) {
    std::string user(user1);
    user.resize(user.length() - 1);
    std::string password;
    EXPECT_FALSE(find_pw(user, password));
}
