/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "auth_provider.h"

#include <folly/portability/GTest.h>
#include <mcbp/protocol/status.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

class MockAuthProvider : public AuthProvider {
protected:
    std::pair<cb::sasl::Error, nlohmann::json> validatePassword(
            const std::string& username, const std::string& password) override {
        EXPECT_EQ("trond", username);
        EXPECT_EQ("foo", password);

        auto ret = nlohmann::json::parse(
                R"({
           "trond":{
              "buckets":{
                "default":["Read","SimpleStats","Insert","Delete","Upsert"]
              },
              "domain":"external",
              "privileges":[]
            }})");

        return std::make_pair<cb::sasl::Error, nlohmann::json>(
                cb::sasl::Error::OK, std::move(ret));
    }
    std::pair<cb::sasl::Error, nlohmann::json> getUserEntry(
            const std::string& username) override {
        if (username == "osbourne") {
            auto ret = nlohmann::json::parse(
                    R"({"osbourne" : {
  "domain" : "external",
  "buckets": {
    "default": ["Read","SimpleStats","Insert","Delete","Upsert"]
  },
  "privileges": []
}})");

            return std::make_pair<cb::sasl::Error, nlohmann::json>(
                    cb::sasl::Error::OK, std::move(ret));
        }

        return std::make_pair<cb::sasl::Error, nlohmann::json>(
                cb::sasl::Error::NO_RBAC_PROFILE, {});
    }
};

class AuthProviderTest : public ::testing::Test {
protected:
    MockAuthProvider provider;
};

TEST_F(AuthProviderTest, InvalidJson) {
    EXPECT_THROW(provider.processAuthnRequest(""), nlohmann::json::exception);
}

TEST_F(AuthProviderTest, NoMech) {
    try {
        provider.processAuthnRequest(
                R"({"challenge":"foo", "authentication-only":false})");
        FAIL() << "Mechanism must be specified";
    } catch (const nlohmann::json::exception& error) {
        EXPECT_STREQ(
                "[json.exception.out_of_range.403] key 'mechanism' not found",
                error.what());
    }
}

TEST_F(AuthProviderTest, UnsupportedMech) {
    const auto ret = provider.processAuthnRequest(
            R"({"mechanism":"SCRAM-SHA1", "challenge":"foo", "authentication-only":false})");
    ASSERT_EQ(cb::mcbp::Status::NotSupported, ret.first);
    const auto json = nlohmann::json::parse(ret.second);
    EXPECT_EQ("mechanism not supported", json["error"]["context"]);
}

TEST_F(AuthProviderTest, NoChallenge) {
    try {
        provider.processAuthnRequest(
                R"({"mechanism":"PLAIN", "authentication-only":false})");
        FAIL() << "Challenge must be specified";
    } catch (const nlohmann::json::exception& error) {
        EXPECT_STREQ(
                "[json.exception.out_of_range.403] key 'challenge' not found",
                error.what());
    }
}

TEST_F(AuthProviderTest, NoAuthenticationOnly) {
    try {
        provider.processAuthnRequest(
                R"({"mechanism":"PLAIN", "challenge":"foo"})");
        FAIL() << "authentication-only must be specified";
    } catch (const nlohmann::json::exception& error) {
        EXPECT_STREQ(
                "[json.exception.out_of_range.403] key 'authentication-only' "
                "not found",
                error.what());
    }
}

TEST_F(AuthProviderTest, PLAIN_SuccessfulAuth) {
    nlohmann::json json;
    json["mechanism"] = "PLAIN";
    json["challenge"] = cb::base64::encode({"\0trond\0foo", 10}, false);
    json["authentication-only"] = false;
    const auto ret = provider.processAuthnRequest(json.dump());
    EXPECT_EQ(cb::mcbp::Status::Success, ret.first);
    json = nlohmann::json::parse(ret.second);
    // @todo I should validate the correct layout of the RBAC section
    EXPECT_EQ("external", json["rbac"]["trond"]["domain"].get<std::string>());
}

TEST_F(AuthProviderTest, PLAIN_SuccessfulAuthOnly) {
    nlohmann::json json;
    json["mechanism"] = "PLAIN";
    json["challenge"] = cb::base64::encode({"\0trond\0foo", 10}, false);
    json["authentication-only"] = true;
    const auto ret = provider.processAuthnRequest(json.dump());
    EXPECT_EQ(cb::mcbp::Status::Success, ret.first);
    EXPECT_TRUE(nlohmann::json::parse(ret.second).empty());
}
