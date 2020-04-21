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
            EXPECT_TRUE(collection.check(priv).success());
        } else {
            EXPECT_TRUE(collection.check(priv).failed());
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
    EXPECT_TRUE(scope.check(Privilege::Read, 32, 0).success());
    // but only with the desired access
    EXPECT_TRUE(scope.check(Privilege::Upsert, 32, 0).failed());
    // but not an unknown collection
    EXPECT_TRUE(scope.check(Privilege::Read, 33, 0).failed());
}

TEST(ScopeTest, ParseLegalConfigWithoutCollections) {
    MockScope scope(nlohmann::json::parse(R"({ "privileges" : [ "Read" ] })"));

    PrivilegeMask blueprint;
    blueprint[int(Privilege::Read)] = true;
    // No scope privileges present
    ASSERT_EQ(blueprint, scope.getPrivileges());

    for (int ii = 0; ii < 10; ++ii) {
        // Check that we can access all collections
        EXPECT_TRUE(scope.check(Privilege::Read, ii, 0).success());
        // but only with the desired access
        EXPECT_TRUE(scope.check(Privilege::Upsert, ii, 0).failed());
    }
}

class MockBucket : public Bucket {
public:
    explicit MockBucket(const nlohmann::json& json) : Bucket(json) {
    }

    const PrivilegeMask& getPrivileges() const {
        return privilegeMask;
    }
    bool doesCollectionPrivilegeExists() const {
        return collectionPrivilegeExists;
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
    EXPECT_TRUE(bucket.check(Privilege::Read, 32, 23).success());
    // but only with the desired access
    EXPECT_TRUE(bucket.check(Privilege::Upsert, 32, 23).failed());
    // but not an unknown scope
    EXPECT_TRUE(bucket.check(Privilege::Read, 33, 23).failed());
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
        EXPECT_TRUE(bucket.check(Privilege::Read, ii, ii).success());
        // but only with the desired access
        EXPECT_TRUE(bucket.check(Privilege::Upsert, ii, ii).failed());
    }
}

class BucketPrivCollectionVisibility : public ::testing::Test {
public:
    // In this test fixture, the 'user' has Read for the bucket (as well as the
    // non-collection privilege audit). This means every Read check (bucket,
    // scope or collection) will succeed and non Read checks will plainly fail
    nlohmann::json bucketAccess = {{"privileges", {"Read", "Audit"}}};
    void test(std::optional<nlohmann::json> scope);
};

void BucketPrivCollectionVisibility::test(std::optional<nlohmann::json> scope) {
    if (scope) {
        bucketAccess["scopes"] = scope.value();
    }
    MockBucket bucket(bucketAccess);
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());

    // Can Read everything
    EXPECT_TRUE(bucket.check(Privilege::Read, {}, {}).success());
    // Can Read all in scope:32
    EXPECT_TRUE(bucket.check(Privilege::Read, 32, {}).success());
    // Can Read all in collection:32 (in scope:32)
    EXPECT_TRUE(bucket.check(Privilege::Read, 32, 32).success());

    // Now check for failure
    auto status = bucket.check(Privilege::Upsert, {}, {});
    // User definitely doesn't have Upsert for the bucket
    EXPECT_TRUE(status.failed());
    // Exact error says Fail
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Try check with scopes only
    for (int ii = 32; ii < 34; ++ii) {
        auto status = bucket.check(Privilege::Upsert, ii, {});
        // User definitely doesn't have Upsert for the scope
        EXPECT_TRUE(status.failed());
        // Exact error says Fail, which should translate to an access error
        // collections in scope cannot be upserted, but they're not 'invisible'
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());
    }

    // Try check with collections
    for (int ii = 32; ii < 34; ++ii) {
        auto status = bucket.check(Privilege::Upsert, ii, ii);
        // User definitely doesn't have Upsert for the collection
        EXPECT_TRUE(status.failed());
        // Exact error says Fail, which should translate to an access error
        // collections cannot be upserted, but they're not 'invisible'
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << ii;
    }
}

TEST_F(BucketPrivCollectionVisibility, bucketOnly) {
    // Run the test with bucket only privs (initialised to bucket:Read)
    test({});
}

TEST_F(BucketPrivCollectionVisibility, scope) {
    // Run the test, but below bucket add scope:32 with Delete privs
    test(nlohmann::json{{"32", {{"privileges", {"Delete"}}}}});
}

TEST_F(BucketPrivCollectionVisibility, scopeAndCollection) {
    // Run the test, but below bucket add scope:32,collection:32 with Delete
    // privs
    test(nlohmann::json{
            {"32", {{"collections", {{"32", {{"privileges", {"Delete"}}}}}}}}});
}

class ScopePrivCollectionVisibility : public ::testing::Test {
public:
    // In this test fixture, the 'user' has no collection privileges for the
    // bucket, they always have collection privileges @ scope 32. This means
    // they can Read everything under scope 32, any failure under scope 32 is
    // a plain failure, and any other failure (e.g. check under scope 33) can be
    // differentiated as Fail vs FailNoPrivileges
    nlohmann::json scopeAccess = {
            {"privileges", {"Audit"}},
            {"scopes", {{"32", {{"privileges", {"Read"}}}}}}};
    void test(std::optional<nlohmann::json> collection);
};

void ScopePrivCollectionVisibility::test(
        std::optional<nlohmann::json> collection) {
    if (collection) {
        scopeAccess["collections"] = collection.value();
    }
    MockBucket bucket(scopeAccess);
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());

    // Cannot Read the bucket
    auto status = bucket.check(Privilege::Read, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Can Read all in scope:32
    EXPECT_TRUE(bucket.check(Privilege::Read, 32, {}).success());
    // Can Read all in collection:32 (because it is in scope:32)
    EXPECT_TRUE(bucket.check(Privilege::Read, 32, 32).success());

    // User definitely doesn't have Upsert for the scope:32
    status = bucket.check(Privilege::Upsert, 32, {});
    EXPECT_TRUE(status.failed());
    // Exact error says Fail, which should translate to an access error
    // collections in scope:32 cannot be upserted, but they're not 'invisible'
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // User definitely doesn't have Upsert for the scope:33 (or any access)
    status = bucket.check(Privilege::Upsert, 33, {});
    EXPECT_TRUE(status.failed());
    // Failed but distinguishable from the other failure
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    // Try a couple of collections, 33 will never be mentioned in either test
    // variant, but should still be 'visible' because of scope 32 Read
    for (int cid : {32, 33}) {
        // Try checks with scope.collection
        status = bucket.check(Privilege::Upsert, 32, cid);
        // User definitely doesn't have Upsert for the scope/collection
        EXPECT_TRUE(status.failed());
        // Exact error says Fail, which should translate to an access error
        // collections in scope:32 cannot be upserted, but they're not
        // 'invisible'
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << cid;
    }

    // User definitely doesn't have Upsert for the scope:33 (or any access)
    status = bucket.check(Privilege::Upsert, 33, 32);
    EXPECT_TRUE(status.failed());
    // However user has no privileges at all for scope:33 so it should be
    // distinguishable from plain Fail
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());
}

TEST_F(ScopePrivCollectionVisibility, scopeOnly) {
    test({});
}

TEST_F(ScopePrivCollectionVisibility, collections) {
    test(nlohmann::json{{"32", {{"privileges", {"Delete"}}}}});
}

// case is scope{c1,c2}  privs scope{c1} lookup for c2

class CollectionPrivVisibility : public ::testing::Test {
public:
    // In this test fixture, the 'user' has no collection privileges for the
    // bucket or scope, they only have collection privileges at given
    // collections (depending on the test)
};

TEST_F(CollectionPrivVisibility, can_read_32_only) {
    nlohmann::json access = R"({
  "privileges": [
    "Audit"
  ],
  "scopes": {
    "32": {
      "collections": {
        "32": {
          "privileges": [
            "Read"
          ]
        }
      }
    }
  }
})"_json;

    // Read for collection 32 only
    // access["collections"] = {{"32", {{"privileges", {"Read"}}}}};
    MockBucket bucket(access);
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());

    // Cannot Read the bucket
    auto status = bucket.check(Privilege::Read, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Cannot Read a scope
    status = bucket.check(Privilege::Read, 32, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Cannot Read or Upsert collection 32.33 and it's not visible (no privs)
    status = bucket.check(Privilege::Read, 32, 33);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    status = bucket.check(Privilege::Upsert, 32, 33);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    // Can read 32,32
    status = bucket.check(Privilege::Read, 32, 32);
    EXPECT_TRUE(status.success());

    // Plain fail for Upsert 32.32
    status = bucket.check(Privilege::Upsert, 32, 32);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());
}

} // namespace cb::rbac
