/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "plain.h"

#include "../cbcrypto.h"
#include <cbsasl/password_database.h>
#include <cbsasl/pwfile.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <string_view>

using namespace std::literals;
using cb::sasl::Error;
using cb::sasl::client::ClientContext;
using cb::sasl::mechanism::plain::authenticate;
using cb::sasl::server::ServerContext;
using PlainServer = cb::sasl::mechanism::plain::ServerBackend;
using cb::crypto::Algorithm;
using cb::sasl::pwdb::MutablePasswordDatabase;

class PlainServerTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        using cb::sasl::pwdb::User;
        using cb::sasl::pwdb::UserFactory;

        auto passwordDatabase = std::make_unique<MutablePasswordDatabase>();

        const std::unordered_map<std::string, std::string> users{
                {"nopassword", ""}, {"password", "secret"}};

        for (const auto& [u, p] : users) {
            passwordDatabase->upsert(UserFactory::create(u, p, [](auto alg) {
                switch (alg) {
                case Algorithm::SHA1:
                case Algorithm::SHA256:
                case Algorithm::SHA512:
                case Algorithm::Argon2id13:
                    return false;
                case Algorithm::DeprecatedPlain:
                    return true;
                }
                return false;
            }));
        }

#ifdef HAVE_LIBSODIUM
        passwordDatabase->upsert(
                UserFactory::create("argon2id", "password", [](auto alg) {
                    switch (alg) {
                    case Algorithm::SHA1:
                    case Algorithm::SHA256:
                    case Algorithm::SHA512:
                    case Algorithm::DeprecatedPlain:
                        return false;
                    case Algorithm::Argon2id13:
                        return true;
                    }
                    return false;
                }));
#endif
        swap_password_database(std::move(passwordDatabase));
    }

    void SetUp() override {
        backend = std::make_unique<PlainServer>(context);
    }
    ServerContext context;
    std::unique_ptr<PlainServer> backend;
};

TEST_F(PlainServerTest, GetName) {
    EXPECT_EQ("PLAIN", backend->getName());
}

TEST_F(PlainServerTest, NoChallengeProvidedChallenge) {
    const auto [error, data] = backend->start({});
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST_F(PlainServerTest, NoZeroTermsInChallenge) {
    const auto [error, data] = backend->start("asdf"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST_F(PlainServerTest, NoZeroTermAfterUsernameInChallenge) {
    const auto [error, data] = backend->start("\0asdf"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST_F(PlainServerTest, CorrectChallengeNoPassword) {
    const auto [error, data] = backend->start("\0nopassword\0"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::OK, error);
}

TEST_F(PlainServerTest, CorrectChallengeUnknownUser) {
    const auto [error, data] = backend->start("\0nouser\0"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::NO_USER, error);
}

TEST_F(PlainServerTest, CorrectChallengeWithPassword) {
    const auto [error, data] = backend->start("\0password\0secret"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::OK, error);
}

TEST_F(PlainServerTest, CorrectChallengeWrongPassword) {
    const auto [error, data] = backend->start("\0password\0invalid"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::PASSWORD_ERROR, error);
}

TEST_F(PlainServerTest, PlainAuthDontAllowStep) {
    bool detected = false;
    try {
        backend->step({});
    } catch (const std::logic_error& e) {
        EXPECT_NE(nullptr, strstr(e.what(), "Plain auth should not call step"));
        detected = true;
    }
    EXPECT_TRUE(detected) << "The exception was not thrown";
}

#ifdef HAVE_LIBSODIUM
TEST_F(PlainServerTest, CorrectChallengeWithPasswordForArgon2id) {
    const auto [error, data] = backend->start("\0argon2id\0password"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::OK, error);
}

TEST_F(PlainServerTest, CorrectChallengeWithWrongPasswordForArgon2id) {
    const auto [error, data] = backend->start("\0argon2id\0wrongpassword"sv);
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(Error::PASSWORD_ERROR, error);
}
#endif

TEST_F(PlainServerTest, Authenticate) {
    using cb::sasl::mechanism::plain::authenticate;
    EXPECT_EQ(Error::OK, authenticate("nopassword", ""));
    EXPECT_EQ(Error::OK, authenticate("password", "secret"));
    EXPECT_EQ(Error::NO_USER, authenticate("nouser", "secret"));
    EXPECT_EQ(Error::PASSWORD_ERROR, authenticate("nopassword", "secret"));
#ifdef HAVE_LIBSODIUM
    EXPECT_EQ(Error::OK, authenticate("argon2id", "password"));
#endif
}

TEST(PlainClientTest, TestName) {
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return "password"; },
                         "PLAIN");
    EXPECT_EQ("PLAIN", client.getName());
}

TEST(PlainClientTest, TestStart) {
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return "password"; },
                         "PLAIN");
    auto [error, challenge] = client.start();
    EXPECT_EQ(Error::OK, error);
    EXPECT_EQ("\0username\0password"sv, challenge);
}

TEST(PlainClientTest, TestStartNoPassword) {
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string { return ""; },
                         "PLAIN");
    EXPECT_EQ("PLAIN", client.getName());
    auto [error, challenge] = client.start();
    EXPECT_EQ(Error::OK, error);
    EXPECT_EQ("\0username\0"sv, challenge);
}

TEST(PlainClientTest, TestStartNoUser) {
    ClientContext client([]() -> std::string { return ""; },
                         []() -> std::string { return "password"; },
                         "PLAIN");
    EXPECT_EQ("PLAIN", client.getName());
    auto [error, challenge] = client.start();
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST(PlainClientTest, TestStartUserWithZeroTerm) {
    ClientContext client(
            []() -> std::string {
                return std::string{"user\0name", 9};
            },
            []() -> std::string { return "password"; },
            "PLAIN");
    auto [error, challenge] = client.start();
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST(PlainClientTest, TestStartPasswordWithZeroTerm) {
    ClientContext client([]() -> std::string { return "username"; },
                         []() -> std::string {
                             return std::string{"pass\0word", 9};
                         },
                         "PLAIN");
    auto [error, challenge] = client.start();
    EXPECT_EQ(Error::BAD_PARAM, error);
}

TEST(PlainClientTest, TestStepNotSupported) {
    ClientContext client({}, {}, "PLAIN");
    bool detected = false;
    try {
        client.step({});
    } catch (const std::logic_error& e) {
        EXPECT_NE(nullptr, strstr(e.what(), "Plain auth should not call step"));
        detected = true;
    }
    EXPECT_TRUE(detected) << "The exception was not thrown";
}
