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

#include <folly/portability/GTest.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>

TEST(UserEntryTest, ParseLegalConfigOldFormat) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit", "Administrator"};
    json["trond"]["buckets"]["bucket1"] = {"Read", "Insert"};
    json["trond"]["buckets"]["bucket2"] = {"Read"};
    json["trond"]["domain"] = "external";

    cb::rbac::UserEntry ue("trond", *json.begin(), cb::rbac::Domain::External);
    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Audit)] = true;
        privs[int(cb::rbac::Privilege::Administrator)] = true;
        EXPECT_EQ(privs, ue.getPrivileges());
    }

    const auto& buckets = ue.getBuckets();
    EXPECT_EQ(2, buckets.size());
    auto it = buckets.find("bucket1");
    EXPECT_NE(buckets.cend(), it);

    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Read)] = true;
        privs[int(cb::rbac::Privilege::Insert)] = true;
        ASSERT_TRUE(it->second);
        EXPECT_EQ(privs, it->second->getPrivileges());
    }

    it = buckets.find("bucket2");
    EXPECT_NE(buckets.cend(), it);
    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Read)] = true;
        ASSERT_TRUE(it->second);
        EXPECT_EQ(privs, it->second->getPrivileges());
    }
}

TEST(UserEntryTest, DomainMustBeString) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit", "BucketManagement"};
    json["trond"]["buckets"]["bucket1"] = {"Read", "Insert"};
    json["trond"]["buckets"]["bucket2"] = {"Read"};
    json["trond"]["domain"] = 5;
    try {
        cb::rbac::UserEntry ue(
                "trond", *json.begin(), cb::rbac::Domain::External);
        FAIL() << "The entry must be a string";
    } catch (nlohmann::json::exception&) {
    }
}

TEST(UserEntryTest, PrivilegesIsOptional) {
    nlohmann::json json;
    json["trond"]["buckets"]["bucket1"] = {"Read", "Insert"};
    json["trond"]["buckets"]["bucket2"] = {"Read"};
    json["trond"]["domain"] = "local";
    cb::rbac::UserEntry ue("trond", *json.begin(), cb::rbac::Domain::Local);
}

TEST(UserEntryTest, BucketsIsOptional) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit", "Administrator"};
    cb::rbac::UserEntry ue("trond", *json.begin(), cb::rbac::Domain::Local);
}

TEST(UserEntryTest, DomainMustMatchExpected) {
    nlohmann::json json;
    json["trond"]["domain"] = "local";
    try {
        cb::rbac::UserEntry ue(
                "trond", *json.begin(), cb::rbac::Domain::External);
        FAIL() << "Should detect domain mismatch";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ("UserEntry::UserEntry: Invalid domain in this context",
                     error.what());
    }
}

TEST(UserEntryTest, InternalUsersMustBeLocal) {
    nlohmann::json json;
    json["@kv"]["domain"] = "local";
    cb::rbac::UserEntry local("@kv", *json.begin(), cb::rbac::Domain::Local);
    json["@kv"]["domain"] = "external";
    try {
        cb::rbac::UserEntry external(
                "@kv", *json.begin(), cb::rbac::Domain::External);
        FAIL() << "Internal users must be locally defined";
    } catch (const std::runtime_error&) {
    }
}

TEST(PrivilegeDatabaseTest, ParseLegalConfig) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit"};
    json["trond"]["buckets"]["mybucket"] = {"Read"};
    json["trond"]["domain"] = "external";
    cb::rbac::PrivilegeDatabase db(json, cb::rbac::Domain::External);

    // Looking up an existing user should not throw an exception
    (void)db.lookup("trond");
    try {
        (void)db.lookup("foo");
        FAIL() << "Trying to fetch a nonexisting user should throw exception";
    } catch (const cb::rbac::NoSuchUserException& exception) {
        EXPECT_STRCASEEQ("foo", exception.what());
    }
}

TEST(PrivilegeDatabaseTest, GenerationCounter) {
    cb::rbac::PrivilegeDatabase db1(nullptr, cb::rbac::Domain::Local);
    cb::rbac::PrivilegeDatabase db2(nullptr, cb::rbac::Domain::Local);
    EXPECT_GT(db2.generation, db1.generation);
}

TEST(PrivilegeDatabaseTest, to_json) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Administrator", "Audit"};
    json["trond"]["buckets"]["mybucket"]["privileges"] = {"Read", "Upsert"};
    json["trond"]["buckets"]["app"]["privileges"] = {"Delete"};
    json["trond"]["domain"] = "external";
    cb::rbac::PrivilegeDatabase db(json, cb::rbac::Domain::External);
    EXPECT_EQ(json.dump(2), db.to_json(cb::rbac::Domain::External).dump(2))
            << db.to_json(cb::rbac::Domain::External).dump(2);
}

/// Perform a sanity check on the Privilege that the following is true:
///
///   1. It is possible to map the privilege to a textual name
///   2. It is possible to map the textual name to the same privilege
///   3. The privilege may be put inside the PrivilegeMask
TEST(Privilege, sanity_check) {
    using namespace cb::rbac;
    PrivilegeMask mask;
    int highest = -1;

    // We've only defined a handfull of privileges, so loop with some
    // negative values and some higher so that we don't need to update
    // the test every time we add a new privilege
    constexpr int lower_test_limit = -10;
    constexpr int upper_test_limit = 1000;

    for (int ii = lower_test_limit; ii < upper_test_limit; ++ii) {
        auto priv = Privilege(ii);
        // The function is_legal_privilege use a switch on an enum class
        // which would cause a compile failure if you add a new value and
        // don't update the switch
        if (is_legal_privilege(priv)) {
            // Verify 1
            auto textual = to_string(priv);
            // Verify 2
            EXPECT_EQ(priv, to_privilege(textual));
            // Verify 3
            EXPECT_LT(ii, mask.size())
                    << textual << " is outside the privilege mask";
            if (highest > ii) {
                highest = ii;
            }
        }
    }

    EXPECT_LT(highest + 100, upper_test_limit)
            << "Please bump the upper test limit to ensure we test values "
               "outside the legal range";
}
