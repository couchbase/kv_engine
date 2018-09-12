/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <cJSON_utils.h>
#include <gtest/gtest.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>

TEST(UserEntryTest, ParseLegalConfig) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit", "BucketManagement"};
    json["trond"]["buckets"]["bucket1"] = {"Read", "Insert"};
    json["trond"]["buckets"]["bucket2"] = {"Read"};
    json["trond"]["domain"] = "external";

    unique_cJSON_ptr root(cJSON_Parse(json.dump().data()));
    cb::rbac::UserEntry ue(*root.get()->child);
    EXPECT_EQ(cb::sasl::Domain::External, ue.getDomain());

    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Audit)] = true;
        privs[int(cb::rbac::Privilege::BucketManagement)] = true;
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
        EXPECT_EQ(privs, it->second);
    }

    it = buckets.find("bucket2");
    EXPECT_NE(buckets.cend(), it);
    {
        cb::rbac::PrivilegeMask privs{};
        privs[int(cb::rbac::Privilege::Read)] = true;
        EXPECT_EQ(privs, it->second);
    }

    // The username does not start with @
    EXPECT_FALSE(ue.isInternal());
}

TEST(PrivilegeDatabaseTest, ParseLegalConfig) {
    nlohmann::json json;
    json["trond"]["privileges"] = {"Audit"};
    json["trond"]["buckets"]["mybucket"] = {"Read"};
    json["trond"]["domain"] = "external";
    unique_cJSON_ptr root(cJSON_Parse(json.dump().data()));
    cb::rbac::PrivilegeDatabase db(root.get());

    // Looking up an existing user should not throw an exception
    db.lookup("trond");
    try {
        db.lookup("foo");
        FAIL() << "Trying to fetch a nonexisting user should throw exception";
    } catch (const cb::rbac::NoSuchUserException& exception) {
        EXPECT_STRCASEEQ("foo", exception.what());
    }
}

TEST(PrivilegeDatabaseTest, GenerationCounter) {
    cb::rbac::PrivilegeDatabase db1(nullptr);
    cb::rbac::PrivilegeDatabase db2(nullptr);
    EXPECT_GT(db2.generation, db1.generation);
}
