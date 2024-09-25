/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <cbsasl/mechanism.h>
#include <cbsasl/password_database.h>
#include <cbsasl/pwfile.h>
#include <cbsasl/server.h>
#include <folly/portability/GTest.h>
#include <algorithm>

class SaslServerTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        using cb::sasl::pwdb::User;
        using cb::sasl::pwdb::UserFactory;

        auto passwordDatabase =
                std::make_unique<cb::sasl::pwdb::MutablePasswordDatabase>();
        passwordDatabase->upsert(UserFactory::create("mikewied", "mikepw"));
        passwordDatabase->upsert(UserFactory::create("nopass", ""));
        swap_password_database(std::move(passwordDatabase));
    }

    cb::sasl::server::ServerContext context;
};

TEST_F(SaslServerTest, ListMechs) {
    auto mechanisms = std::string_view{
            "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN OAUTHBEARER"};
    EXPECT_EQ(mechanisms, cb::sasl::server::listmech());
}

TEST_F(SaslServerTest, BadMech) {
    EXPECT_THROW(context.start("bad_mech", "", "foobar"),
                 cb::sasl::unknown_mechanism);
}

TEST_F(SaslServerTest, PlainCorrectPassword) {
    /* Normal behavior */
    auto data = context.start("PLAIN", "", {"\0mikewied\0mikepw", 16});
    EXPECT_EQ(cb::sasl::Error::OK, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, PlainWrongPassword) {
    auto data = context.start("PLAIN", "", {"\0mikewied\0badpPW", 16});
    EXPECT_EQ(cb::sasl::Error::PASSWORD_ERROR, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, PlainNoPassword) {
    auto data = context.start("PLAIN", "", {"\0nopass\0", 8});
    EXPECT_EQ(cb::sasl::Error::OK, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, PlainWithAuthzid) {
    auto data = context.start("PLAIN", "", {"funzid\0mikewied\0mikepw", 22});
    EXPECT_EQ(cb::sasl::Error::OK, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, PlainWithNoPwOrUsernameEndingNull) {
    auto data = context.start("PLAIN", "", {"funzid\0mikewied", 15});
    EXPECT_EQ(cb::sasl::Error::BAD_PARAM, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, PlainNoNullAtAll) {
    auto data = context.start("PLAIN", "", {"funzidmikewied", 14});
    EXPECT_EQ(cb::sasl::Error::BAD_PARAM, data.first);
    EXPECT_TRUE(data.second.empty());
}

TEST_F(SaslServerTest, CantPickUnsupportedMechanism) {
    EXPECT_THROW(
            context.start("PLAIN", "SCRAM-SHA512", {"\0mikewied\0mikepw", 16}),
            cb::sasl::unknown_mechanism);
}
