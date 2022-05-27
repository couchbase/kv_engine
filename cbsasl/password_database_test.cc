/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "cbcrypto.h"

#include <cbsasl/password_database.h>
#include <cbsasl/user.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json.hpp>

using cb::crypto::Algorithm;
using cb::sasl::pwdb::UserFactory;

class PasswordDatabaseTest : public ::testing::Test {
public:
    void SetUp() override {
        json["@@version@@"] = 2;
        json["trond"] = UserFactory::create("trond", "secret1").to_json();
        json["mike"] = UserFactory::create("mike", "secret2").to_json();
    }

    nlohmann::json json;
};

TEST_F(PasswordDatabaseTest, TestNormalInit) {
    const auto db = cb::sasl::pwdb::PasswordDatabase(json);
    EXPECT_FALSE(db.find("trond").isDummy());
    EXPECT_FALSE(db.find("mike").isDummy());
    EXPECT_TRUE(db.find("unknown").isDummy());
}

TEST_F(PasswordDatabaseTest, TestInvalidVersion) {
    json["@@version@@"] = 3;
    bool failed = false;
    try {
        cb::sasl::pwdb::PasswordDatabase{json};
    } catch (const std::exception& e) {
        EXPECT_STREQ("PasswordDatabase(): Unknown version: 3", e.what());
        failed = true;
    }
    EXPECT_TRUE(failed) << "Unknonw version should have been detected";
}
