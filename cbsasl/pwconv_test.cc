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

/**
 * This file contains tests to validate that cbasl_pwconv is able to
 * convert an "isasl-style" password file to a cbsasl style
 */
#include <folly/portability/GTest.h>
#include <memory>

#include "password_database.h"
#include "plain/check_password.h"

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
        cb::sasl::pwdb::convert(input, output);
        db = std::make_unique<cb::sasl::pwdb::PasswordDatabase>(output.str(),
                                                                false);
    }

    static std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> db;
};

std::unique_ptr<cb::sasl::pwdb::PasswordDatabase> PwconvTest::db;

TEST_F(PwconvTest, VerifySpaceInTheMiddle) {
    auto trond = db->find("trond");
    EXPECT_FALSE(trond.isDummy());
    EXPECT_EQ(
            cb::sasl::Error::OK,
            cb::sasl::plain::check_password(nullptr, trond, "trond password"));
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR,
              cb::sasl::plain::check_password(nullptr, trond, "password"));
}

TEST_F(PwconvTest, VerifySpaceAtTheEnd) {
    auto mike = db->find("mike");
    EXPECT_FALSE(mike.isDummy());
    EXPECT_EQ(cb::sasl::Error::OK,
              cb::sasl::plain::check_password(nullptr, mike, "mikepassword "));
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR,
              cb::sasl::plain::check_password(nullptr, mike, "password"));
}

TEST_F(PwconvTest, VerifySpaceAtTheFront) {
    auto john = db->find("john");
    EXPECT_FALSE(john.isDummy());
    EXPECT_EQ(cb::sasl::Error::OK,
              cb::sasl::plain::check_password(nullptr, john, " johnpassword"));
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR,
              cb::sasl::plain::check_password(nullptr, john, "password"));
}

TEST_F(PwconvTest, VerifyNoPassword) {
    auto james = db->find("james");
    EXPECT_FALSE(james.isDummy());
    EXPECT_EQ(cb::sasl::Error::OK,
              cb::sasl::plain::check_password(nullptr, james, ""));
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR,
              cb::sasl::plain::check_password(nullptr, james, "password"));

    auto joe = db->find("joe");
    EXPECT_FALSE(joe.isDummy());
    EXPECT_EQ(cb::sasl::Error::OK,
              cb::sasl::plain::check_password(nullptr, joe, ""));
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR,
              cb::sasl::plain::check_password(nullptr, joe, "password"));
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
