/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "pwfile.h"

#include <cbcrypto/digest.h>
#include <cbsasl/client.h>
#include <cbsasl/password_database.h>
#include <cbsasl/server.h>
#include <folly/portability/GTest.h>
#include <platform/uuid.h>
#include <memory>

using namespace std::string_view_literals;

class SaslClientServerTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        using cb::sasl::pwdb::User;
        using cb::sasl::pwdb::UserFactory;

        auto passwordDatabase =
                std::make_unique<cb::sasl::pwdb::MutablePasswordDatabase>();
        passwordDatabase->upsert(UserFactory::create(
                "mikewied",
                {{" mik epw "}, {"second_password"}},
                [](cb::crypto::Algorithm) { return true; },
                "pbkdf2-hmac-sha512"));
        passwordDatabase->upsert(UserFactory::create(
                "expired",
                "expired",
                [](cb::crypto::Algorithm) { return true; },
                "pbkdf2-hmac-sha512",
                1024UL));
        swap_password_database(std::move(passwordDatabase));
    }

    static void test_successful_auth(const std::string& mech,
                                     std::string_view password) {
        cb::sasl::client::ClientContext client(
                []() -> std::string { return std::string{"mikewied"}; },
                [&password]() -> std::string { return std::string{password}; },
                mech);

        auto client_data = client.start();
        ASSERT_EQ(cb::sasl::Error::OK, client_data.first);

        cb::sasl::server::ServerContext server;
        server.setValidateUserTokenFunction([&password](auto user, auto token) {
            using cb::sasl::Error;
            if (user == "mikewied") {
                return password == token ? Error::OK : Error::PASSWORD_ERROR;
            }
            return Error::NO_USER;
        });

        auto server_data = server.start(client.getName(),
                                        cb::sasl::server::listmech(),
                                        client_data.second);
        if (server_data.first == cb::sasl::Error::OK) {
            // Authentication success
            return;
        }

        ASSERT_EQ(cb::sasl::Error::CONTINUE, server_data.first);

        do {
            client_data = client.step(server_data.second);
            ASSERT_EQ(cb::sasl::Error::CONTINUE, client_data.first);
            server_data = server.step(client_data.second);
        } while (server_data.first == cb::sasl::Error::CONTINUE);

        ASSERT_EQ(cb::sasl::Error::OK, server_data.first);
        client_data = client.step(server_data.second);
        EXPECT_EQ(cb::sasl::Error::OK, client_data.first);
    }

    static void test_password_expired(const std::string& mech) {
        cb::sasl::client::ClientContext client(
                []() -> std::string { return "expired"; },
                []() -> std::string { return "expired"; },
                mech);

        auto client_data = client.start();
        ASSERT_EQ(cb::sasl::Error::OK, client_data.first);

        cb::sasl::server::ServerContext server;

        auto server_data = server.start(client.getName(),
                                        cb::sasl::server::listmech(),
                                        client_data.second);

        if (mech == "PLAIN") {
            EXPECT_EQ(cb::sasl::Error::PASSWORD_EXPIRED, server_data.first);
            return;
        }
        ASSERT_EQ(cb::sasl::Error::CONTINUE, server_data.first);

        do {
            client_data = client.step(server_data.second);
            ASSERT_EQ(cb::sasl::Error::CONTINUE, client_data.first);
            server_data = server.step(client_data.second);
        } while (server_data.first == cb::sasl::Error::CONTINUE);

        ASSERT_EQ(cb::sasl::Error::PASSWORD_EXPIRED, server_data.first);
    }
};

TEST_F(SaslClientServerTest, PLAIN) {
    test_successful_auth("PLAIN", " mik epw "sv);
}

TEST_F(SaslClientServerTest, OAUTHBEARER) {
    test_successful_auth("OAUTHBEARER", to_string(cb::uuid::random()));
}

TEST_F(SaslClientServerTest, SCRAM_SHA1) {
    test_successful_auth("SCRAM-SHA1", " mik epw "sv);
}

TEST_F(SaslClientServerTest, SCRAM_SHA256) {
    test_successful_auth("SCRAM-SHA256", " mik epw "sv);
}

TEST_F(SaslClientServerTest, SCRAM_SHA512) {
    test_successful_auth("SCRAM-SHA512", " mik epw "sv);
}

TEST_F(SaslClientServerTest, AutoSelectMechamism) {
    test_successful_auth(cb::sasl::server::listmech(), " mik epw "sv);
}

TEST_F(SaslClientServerTest, PLAIN_AlternativePassword) {
    test_successful_auth("PLAIN", "second_password"sv);
}

TEST_F(SaslClientServerTest, SCRAM_SHA1_AlternativePassword) {
    test_successful_auth("SCRAM-SHA1", "second_password"sv);
}

TEST_F(SaslClientServerTest, SCRAM_SHA256_AlternativePassword) {
    test_successful_auth("SCRAM-SHA256", "second_password"sv);
}

TEST_F(SaslClientServerTest, SCRAM_SHA512_AlternativePassword) {
    test_successful_auth("SCRAM-SHA512", "second_password"sv);
}

TEST_F(SaslClientServerTest, AutoSelectMechamism_AlternativePassword) {
    test_successful_auth(cb::sasl::server::listmech(), "second_password"sv);
}

TEST_F(SaslClientServerTest, PLAIN_PasswordExpired) {
    test_password_expired("PLAIN");
}

TEST_F(SaslClientServerTest, SCRAM_SHA1_PasswordExpired) {
    test_password_expired("SCRAM-SHA1");
}

TEST_F(SaslClientServerTest, SCRAM_SHA256_PasswordExpired) {
    test_password_expired("SCRAM-SHA256");
}

TEST_F(SaslClientServerTest, SCRAM_SHA512_PasswordExpired) {
    test_password_expired("SCRAM-SHA512");
}
