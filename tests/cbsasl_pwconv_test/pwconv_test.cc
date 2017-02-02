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

/**
 * This file contains tests to validate that cbasl_pwconv is able to
 * convert an "isasl-style" password file to a cbsasl style
 */
#include <gtest/gtest.h>

#include <cbsasl/pwconv.h>
#include <cbsasl/password_database.h>
#include <cbsasl/plain/check_password.h>

class PwconvTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        std::stringstream input;
        input << "\n" // Add a blank line
              << "\r\n" // and with carriage return
              << "#this is a comment line\n"
              << "trond trond password\n" // space in the middle
              << "mike mikepassword \n" // space at the end
              << "john  johnpassword\n" // space in the beginning
              << "james \n" // no password
              << "joe\n"; // no password

        std::stringstream output;
        cbsasl_pwconv(input, output);
        db.reset(new cb::sasl::PasswordDatabase(output.str(), false));
    }

    static std::unique_ptr<cb::sasl::PasswordDatabase> db;
};

std::unique_ptr<cb::sasl::PasswordDatabase> PwconvTest::db;

TEST_F(PwconvTest, VerifySpaceInTheMiddle) {
    auto trond = db->find("trond");
    EXPECT_FALSE(trond.isDummy());
    EXPECT_EQ(CBSASL_OK, cb::sasl::plain::check_password(trond, "trond password"));
    EXPECT_EQ(CBSASL_PWERR, cb::sasl::plain::check_password(trond, "password"));
}


TEST_F(PwconvTest, VerifySpaceAtTheEnd) {
    auto mike = db->find("mike");
    EXPECT_FALSE(mike.isDummy());
    EXPECT_EQ(CBSASL_OK, cb::sasl::plain::check_password(mike, "mikepassword "));
    EXPECT_EQ(CBSASL_PWERR, cb::sasl::plain::check_password(mike, "password"));
}

TEST_F(PwconvTest, VerifySpaceAtTheFront) {
    auto john = db->find("john");
    EXPECT_FALSE(john.isDummy());
    EXPECT_EQ(CBSASL_OK, cb::sasl::plain::check_password(john, " johnpassword"));
    EXPECT_EQ(CBSASL_PWERR, cb::sasl::plain::check_password(john, "password"));
}

TEST_F(PwconvTest, VerifyNoPassword) {
    auto james = db->find("james");
    EXPECT_FALSE(james.isDummy());
    EXPECT_EQ(CBSASL_OK, cb::sasl::plain::check_password(james, ""));
    EXPECT_EQ(CBSASL_PWERR, cb::sasl::plain::check_password(james, "password"));

    auto joe = db->find("joe");
    EXPECT_FALSE(joe.isDummy());
    EXPECT_EQ(CBSASL_OK, cb::sasl::plain::check_password(joe, ""));
    EXPECT_EQ(CBSASL_PWERR, cb::sasl::plain::check_password(joe, "password"));
}

TEST_F(PwconvTest, VerifyComment) {
    EXPECT_TRUE(db->find("#this").isDummy());
    EXPECT_TRUE(db->find("this").isDummy());
}

TEST_F(PwconvTest, VerifyUsernameSuperset) {
    EXPECT_TRUE(db->find("trond ").isDummy());
}

TEST_F(PwconvTest, VerifyUsernameSubset) {
    EXPECT_TRUE(db->find("tron").isDummy());
}
