/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include <folly/portability/GTest.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>

namespace cb::rbac {
class MockCollection : public Collection {
public:
    explicit MockCollection(const nlohmann::json& json) : Collection(json) {
    }

    void setMask(PrivilegeMask mask) {
        privilegeMask = mask;
    }

    PrivilegeMask getPrivileges() {
        return privilegeMask;
    }
};

TEST(CollectionTest, ParseLegalConfig) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges" : ["all"]})"));
    auto mask = collection.getPrivileges();
    ASSERT_TRUE(mask.any()) << "Expecting bits set";

    // we should have access to all bucket level privs
    for (size_t ii = 0; ii < mask.size(); ++ii) {
        auto priv = Privilege(ii);
        if (is_bucket_privilege(priv)) {
            EXPECT_EQ(PrivilegeAccess::Ok, collection.check(priv));
        } else {
            EXPECT_EQ(PrivilegeAccess::Fail, collection.check(priv));
        }
    }
}

class MockScope : public Scope {
public:
    explicit MockScope(const nlohmann::json& json) : Scope(json) {
    }

    PrivilegeMask& getPrivileges() {
        return privilegeMask;
    }
};

TEST(ScopeTest, ParseLegalConfigWithCollections) {
    MockScope scope(nlohmann::json::parse(R"(
{ "collections" : {
    "32" : {
      "privileges" : [ "Read" ]
    }
  }
})"));

    // No scope privileges present
    ASSERT_TRUE(scope.getPrivileges().none());

    // Check that we can access the collection!
    EXPECT_EQ(PrivilegeAccess::Ok, scope.check(Privilege::Read, 32));
    // but only with the desired access
    EXPECT_EQ(PrivilegeAccess::Fail, scope.check(Privilege::Upsert, 32));
    // but not an unknown collection
    EXPECT_EQ(PrivilegeAccess::Fail, scope.check(Privilege::Read, 33));
}

TEST(ScopeTest, ParseLegalConfigWithoutCollections) {
    MockScope scope(nlohmann::json::parse(R"({ "privileges" : [ "Read" ] })"));

    PrivilegeMask blueprint;
    blueprint[int(Privilege::Read)] = true;
    // No scope privileges present
    ASSERT_EQ(blueprint, scope.getPrivileges());

    for (int ii = 0; ii < 10; ++ii) {
        // Check that we can access all collections
        EXPECT_EQ(PrivilegeAccess::Ok, scope.check(Privilege::Read, ii));
        // but only with the desired access
        EXPECT_EQ(PrivilegeAccess::Fail, scope.check(Privilege::Upsert, ii));
    }
}

class MockBucket : public Bucket {
public:
    explicit MockBucket(const nlohmann::json& json) : Bucket(json) {
    }

    PrivilegeMask& getPrivileges() {
        return privilegeMask;
    }
};

TEST(BucketTest, ParseLegalConfigWithScopes) {
    // We don't need to add all of the various collection configuration
    // as we tested that in the Scope tests.. Use the simplest with
    // access to all collections within the scope
    MockBucket bucket(nlohmann::json::parse(R"(
{ "scopes" : {
    "32" : {
      "privileges" : [ "Read" ]
    }
  }
})"));

    // No bucket privileges present
    ASSERT_TRUE(bucket.getPrivileges().none());

    // Check that we can access the scope!
    EXPECT_EQ(PrivilegeAccess::Ok, bucket.check(Privilege::Read, 32, 23));
    // but only with the desired access
    EXPECT_EQ(PrivilegeAccess::Fail, bucket.check(Privilege::Upsert, 32, 23));
    // but not an unknown scope
    EXPECT_EQ(PrivilegeAccess::Fail, bucket.check(Privilege::Read, 33, 23));
}

TEST(BucketTest, ParseLegalConfigWithoutScopes) {
    // We don't need to add all of the various collection configuration
    // as we tested that in the Scope tests.. Use the simplest with
    // access to all collections within the scope
    MockBucket bucket(nlohmann::json::parse(R"({ "privileges" : [ "Read" ]})"));
    PrivilegeMask blueprint;
    blueprint[int(Privilege::Read)] = true;
    // No scope privileges present
    ASSERT_EQ(blueprint, bucket.getPrivileges());

    for (int ii = 0; ii < 10; ++ii) {
        // Check that we can access all scopes and collctions
        EXPECT_EQ(PrivilegeAccess::Ok, bucket.check(Privilege::Read, ii, ii));
        // but only with the desired access
        EXPECT_EQ(PrivilegeAccess::Fail,
                  bucket.check(Privilege::Upsert, ii, ii));
    }
}

} // namespace cb::rbac
