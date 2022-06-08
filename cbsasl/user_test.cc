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
#include <cbsasl/server.h>
#include <cbsasl/user.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <nlohmann/json.hpp>
#include <platform/base64.h>

using cb::crypto::Algorithm;
using cb::sasl::pwdb::User;

class PasswordMetaTest : public ::testing::Test {
public:
    void SetUp() override {
        sha1_blueprint["hash"] = "NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=";
        sha1_blueprint["salt"] = "iiU7hLv7l3yOoEgXusJvT2i1J2A=";
        sha1_blueprint["algorithm"] = "SHA-1";

        argon2id_blueprint["algorithm"] = "argon2id";
        argon2id_blueprint["hash"] =
                "fehlwuE2N30W336KzLvLJEe6Z32XhHURSP9W6ayhoDU=";
        argon2id_blueprint["memory"] = 134217728;
        argon2id_blueprint["parallelism"] = 1;
        argon2id_blueprint["salt"] = "wkhDB1DVlfeqZ10PnV4VJA==";
        argon2id_blueprint["time"] = 3;
    }

    nlohmann::json argon2id_blueprint;
    nlohmann::json sha1_blueprint;
};

TEST_F(PasswordMetaTest, TestNormalInit) {
    User::PasswordMetaData md(sha1_blueprint);
    EXPECT_EQ("iiU7hLv7l3yOoEgXusJvT2i1J2A=", md.getSalt());
    EXPECT_EQ("NP0b1Ji5jWG/ZV6hPzOIk3lmTmw=",
              Couchbase::Base64::encode(md.getPassword()));
    EXPECT_EQ("SHA-1", md.getAlgorithm());
}

TEST_F(PasswordMetaTest, UnknownLabel) {
    sha1_blueprint["extra"] = "foo";
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ(R"(PasswordMetaData(): Invalid attribute: "extra")",
                     e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestMissingHash) {
    sha1_blueprint.erase("hash");
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("PasswordMetaData(): hash must be specified", e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestMissingSalt) {
    sha1_blueprint.erase("salt");
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("PasswordMetaData(): salt must be specified", e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestMissingAlgorithm) {
    sha1_blueprint.erase("algorithm");
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("PasswordMetaData(): algorithm must be specified",
                     e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestInvalidDatatypeForHash) {
    sha1_blueprint["hash"] = 5;
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("PasswordMetaData(): hash must be a string", e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestInvalidDatatypeForSalt) {
    sha1_blueprint["salt"] = 5;
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("PasswordMetaData(): salt must be a string", e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestInvalidBase64EncodingForHash) {
    sha1_blueprint["hash"] = "!@#$%^&*";
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("Couchbase::base64::code2val Invalid input character",
                     e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, TestInvalidBase64EncodingForSalt) {
    sha1_blueprint["salt"] = "!@#$%^&*";
    bool failed = false;
    try {
        User::PasswordMetaData md(sha1_blueprint);
    } catch (const std::exception& e) {
        failed = true;
        EXPECT_STREQ("Couchbase::base64::code2val Invalid input character",
                     e.what());
    }
    EXPECT_TRUE(failed);
}

TEST_F(PasswordMetaTest, HashArgon2idEntry) {
    cb::sasl::pwdb::User::PasswordMetaData md(argon2id_blueprint);
}

TEST_F(PasswordMetaTest, AlgorithmMustBeValid) {
    // must be a string
    try {
        nlohmann::json json = argon2id_blueprint;
        json["algorithm"] = 5;
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): algorithm must be a string",
                     e.what());
    }

    // must be argon2id or SHA-1
    try {
        nlohmann::json json = argon2id_blueprint;
        json["algorithm"] = "whateverfancy";
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                R"(PasswordMetaData(): algorithm must be set to "argon2id" or "SHA-1")",
                e.what());
    }
}

TEST_F(PasswordMetaTest, HashArgon2idEntryMemoryCost) {
    // must be present
    try {
        nlohmann::json json = argon2id_blueprint;
        json.erase("memory");
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): argon2id requires memory to be set",
                     e.what());
    }

    // must be a number
    try {
        nlohmann::json json = argon2id_blueprint;
        json["memory"] = "whatever";
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): memory must be a number", e.what());
    }
}

TEST_F(PasswordMetaTest, HashArgon2idEntryTimeCost) {
    // must be present
    try {
        nlohmann::json json = argon2id_blueprint;
        json.erase("time");
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): argon2id requires time to be set",
                     e.what());
    }

    // must be a number
    try {
        nlohmann::json json = argon2id_blueprint;
        json["time"] = "whatever";
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): time must be a number", e.what());
    }
}

TEST_F(PasswordMetaTest, HashArgon2idEntryParallelCost) {
    // must be present
    try {
        nlohmann::json json = argon2id_blueprint;
        json.erase("parallelism");
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ(
                "PasswordMetaData(): argon2id requires parallelism to be set",
                e.what());
    }

    // must be a number
    try {
        nlohmann::json json = argon2id_blueprint;
        json["parallelism"] = "whatever";
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): parallelism must be a number",
                     e.what());
    }

    // must be set to 1
    try {
        nlohmann::json json = argon2id_blueprint;
        json["parallelism"] = 7;
        cb::sasl::pwdb::User::PasswordMetaData md(json);
        FAIL() << "Error should be detected";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("PasswordMetaData(): parallelism must be set to 1",
                     e.what());
    }
}

class UserTest : public ::testing::Test {
public:
    void SetUp() override {
        root = {{"scram-sha-1",
                 {{"server_key", "xlmIdzh5718/323JcFbA9oxz2N4="},
                  {"stored_key", "/xBUzh1cpAxZ6mClwSJpWISt8g4="},
                  {"salt", "iiU7hLv7l3yOoEgXusJvT2i1J2A="},
                  {"iterations", 10}}},
                {"scram-sha-256",
                 {{"server_key",
                   "AIBEwCV3hyZTM/sWiS2llpYmIJSrue6o3xOBSKqi7J4="},
                  {"stored_key",
                   "nenK/vukVml9UAqTyrfD5EziUKVL4HRLqJ9xB7LD7vk="},
                  {"salt", "i5Jn//LLM0245cscYnldCjM/HMC7Hj2U1HT6iXqCC0E="},
                  {"iterations", 10}}},
                {"scram-sha-512",
                 {{"server_key",
                   "XqUk1GEK2VEi+QNxCxJQNO6Bxm4j4zMPjKu/"
                   "SA2I+OSpeXZGt3J6BVGguDaQC2Zo7p2J6aqwHQNf0kDiYqG4VA=="},
                  {"stored_key",
                   "qA3MP+EMTSn8pfyYailyCs6maoAJWu1FGhVJYhlD+Dew9+"
                   "kt8XRs1ljQYnXprHzwRfKWIqHyOgQMVqWgVyVEHA=="},
                  {"salt",
                   "nUNk2ZbAZTabxboF+OBQws3zNJpxePtnuF8KwcylC3h/"
                   "NnQQ9FqU0YYohjJhvGRNbxjPTTSuYOgxBG4FMV1W3A=="},
                  {"iterations", 10}}},
                {"hash",
                 {{"algorithm", "argon2id"},
                  {"time", 3},
                  {"memory", 134217728},
                  {"parallelism", 1},
                  {"salt", "wkhDB1DVlfeqZ10PnV4VJA=="},
                  {"hash", "fehlwuE2N30W336KzLvLJEe6Z32XhHURSP9W6ayhoDU="}}}};
    }

    nlohmann::json root;
    const cb::UserData username{"myname"};
};

TEST_F(UserTest, TestNormalInit) {
    const User u(root, username);
    EXPECT_EQ(username.getRawValue(), u.getUsername().getRawValue());
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA512));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA256));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA1));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::Argon2id13));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::DeprecatedPlain));

    {
        auto& md = u.getScramMetaData(Algorithm::SHA512);
        EXPECT_EQ(10, md.iteration_count);
        EXPECT_EQ(
                "nUNk2ZbAZTabxboF+OBQws3zNJpxePtnuF8Kw"
                "cylC3h/NnQQ9FqU0YYohjJhvGRNbxjPTT"
                "SuYOgxBG4FMV1W3A==",
                md.salt);
        EXPECT_EQ(
                "XqUk1GEK2VEi+QNxCxJQNO6Bxm4j4zMPjKu/"
                "SA2I+OSpeXZGt3J6BVGguDaQC2Zo7p2J6aqwHQNf0kDiYqG4VA==",
                Couchbase::Base64::encode(md.server_key));
        EXPECT_EQ(
                "qA3MP+EMTSn8pfyYailyCs6maoAJWu1FGhVJYhlD+Dew9+"
                "kt8XRs1ljQYnXprHzwRfKWIqHyOgQMVqWgVyVEHA==",
                Couchbase::Base64::encode(md.stored_key));
    }

    {
        auto& md = u.getScramMetaData(Algorithm::SHA256);
        EXPECT_EQ(10, md.iteration_count);
        EXPECT_EQ("i5Jn//LLM0245cscYnldCjM/HMC7Hj2U1HT6iXqCC0E=", md.salt);
        EXPECT_EQ("AIBEwCV3hyZTM/sWiS2llpYmIJSrue6o3xOBSKqi7J4=",
                  Couchbase::Base64::encode(md.server_key));
        EXPECT_EQ("nenK/vukVml9UAqTyrfD5EziUKVL4HRLqJ9xB7LD7vk=",
                  Couchbase::Base64::encode(md.stored_key));
    }

    {
        auto& md = u.getScramMetaData(Algorithm::SHA1);
        EXPECT_EQ(10, md.iteration_count);
        EXPECT_EQ("iiU7hLv7l3yOoEgXusJvT2i1J2A=", md.salt);
        EXPECT_EQ("xlmIdzh5718/323JcFbA9oxz2N4=",
                  Couchbase::Base64::encode(md.server_key));
        EXPECT_EQ("/xBUzh1cpAxZ6mClwSJpWISt8g4=",
                  Couchbase::Base64::encode(md.stored_key));
    }

    {
        auto& md = u.getPaswordHash();
        EXPECT_EQ("wkhDB1DVlfeqZ10PnV4VJA==", md.getSalt());
        EXPECT_EQ("fehlwuE2N30W336KzLvLJEe6Z32XhHURSP9W6ayhoDU=",
                  Couchbase::Base64::encode(md.getPassword()));
        EXPECT_EQ("argon2id", md.getAlgorithm());
        EXPECT_EQ(3, md.getProperties()["time"]);
        EXPECT_EQ(1, md.getProperties()["parallelism"]);
        EXPECT_EQ(134217728, md.getProperties()["memory"]);
    }
}

TEST_F(UserTest, TestNoSha512) {
    root.erase("scram-sha-512");
    const User u(root, username);

    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::SHA512));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA256));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA1));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::Argon2id13));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::DeprecatedPlain));

    EXPECT_THROW(u.getScramMetaData(Algorithm::SHA512), std::invalid_argument);
}

TEST_F(UserTest, TestNoSha256) {
    root.erase("scram-sha-256");
    const User u(root, username);
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA512));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::SHA256));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA1));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::Argon2id13));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::DeprecatedPlain));
    EXPECT_THROW(u.getScramMetaData(Algorithm::SHA256), std::invalid_argument);
}

TEST_F(UserTest, TestNoSha1) {
    root.erase("scram-sha-1");
    const User u(root, username);
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA512));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::SHA256));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::SHA1));
    EXPECT_TRUE(u.isPasswordHashAvailable(Algorithm::Argon2id13));
    EXPECT_FALSE(u.isPasswordHashAvailable(Algorithm::DeprecatedPlain));
    EXPECT_THROW(u.getScramMetaData(Algorithm::SHA1), std::invalid_argument);
}

TEST_F(UserTest, TestNoHash) {
    root.erase("hash");
    bool failed = false;
    try {
        User{root, username};
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(R"(User("<ud>myname</ud>") must contain hash entry)",
                     e.what());
        failed = true;
    }
    EXPECT_TRUE(failed) << "An exception should have been thrown";
}

TEST_F(UserTest, InvalidAttribute) {
    root["gssapi"] = "foo";
    bool failed = false;
    try {
        User{root, username};
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(
                R"(User("<ud>myname</ud>"): Invalid attribute "gssapi" specified)",
                e.what());
        failed = true;
    }
    EXPECT_TRUE(failed) << "Invalid attribute not detected";
}

/**
 * Make sure that we generate the dummy salts the same way as ns_server does.
 *
 * The fallback salt and the resulting salt were reported back from the
 * ns_server team so we can verify that we generate the same salt by using
 * the same input data
 */
TEST(UserFactoryTest, CreateDummy) {
    using namespace cb::sasl;
    // set the fallback salt to something we know about ;)
    cb::sasl::server::set_scramsha_fallback_salt("WyulJ+YpKKZn+y9f");
    auto u = pwdb::UserFactory::createDummy("foobar", Algorithm::SHA512);
    EXPECT_TRUE(u.isDummy());
    auto meta = u.getScramMetaData(Algorithm::SHA512);
    EXPECT_EQ(
            "ZLBvongMC+gVSc8JsnCmK8CE+KJrCdS/8fT4cvb3IkJJGTgaGQ+HGuQaXKTN9829l/"
            "8eoUUpiI2Cyk/CRnULtw==",
            meta.salt);
}
