/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
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
    "0x32a" : {
      "privileges" : [ "Read" ]
    }
  }
})"));

    // No scope privileges present
    ASSERT_TRUE(scope.getPrivileges().none());

    // Check that we can access the collection!
    EXPECT_TRUE(scope.check(Privilege::Read, 0x32a, 0).success());
    // but only with the desired access
    EXPECT_TRUE(scope.check(Privilege::Upsert, 0x32a, 0).failed());
    // but not an unknown collection
    EXPECT_TRUE(scope.check(Privilege::Read, 0x33, 0).failed());
}

TEST(ScopeTest, ParseIllegalConfigWithCollections) {
    try {
        MockScope scope(nlohmann::json::parse(R"(
{ "collections" : {
    "0x32a " : {
      "privileges" : [ "Read" ]
    }
  }
})"));
        FAIL() << "Should not accept trailing characters";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Scope::Scope(): Extra characters present for CID",
                     e.what());
    }
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
    "0x32a" : {
      "privileges" : [ "Read" ]
    }
  }
})"));

    // No bucket privileges present
    ASSERT_TRUE(bucket.getPrivileges().none());

    // Check that we can access the scope!
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32a, 0x23).success());
    // but only with the desired access
    EXPECT_TRUE(bucket.check(Privilege::Upsert, 0x32a, 0x23).failed());
    // but not an unknown scope
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x33, 0x23).failed());
}

TEST(ScopeTest, ParseIllegalConfigWithScopes) {
    try {
        MockBucket bucket(nlohmann::json::parse(R"(
{ "scopes" : {
    "0x32a " : {
      "privileges" : [ "Read" ]
    }
  }
})"));
        FAIL() << "Should not accept trailing characters";
    } catch (const std::invalid_argument& e) {
        EXPECT_STREQ("Bucket::Bucket(): Extra characters present for SID",
                     e.what());
    }
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
    // Can Read all in scope:0x32
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, {}).success());
    // Can Read all in collection:0x32 (in scope:0x32)
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, 0x32).success());

    // Now check for failure
    auto status = bucket.check(Privilege::Upsert, {}, {});
    // User definitely doesn't have Upsert for the bucket
    EXPECT_TRUE(status.failed());
    // Exact error says Fail
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Try check with scopes only
    for (int ii = 0x32; ii < 0x34; ++ii) {
        status = bucket.check(Privilege::Upsert, ii, {});
        // User definitely doesn't have Upsert for the scope
        EXPECT_TRUE(status.failed());
        // Exact error says Fail, which should translate to an access error
        // collections in scope cannot be upserted, but they're not 'invisible'
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());
    }

    // Try check with collections
    for (int ii = 0x32; ii < 0x34; ++ii) {
        status = bucket.check(Privilege::Upsert, ii, ii);
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
    // Run the test, but below bucket add scope:0x32 with Delete privs
    test(nlohmann::json{{"0x32", {{"privileges", {"Delete"}}}}});
}

TEST_F(BucketPrivCollectionVisibility, scopeAndCollection) {
    // Run the test, but below bucket add scope:0x32,collection:0x32 with Delete
    // privs
    test(nlohmann::json{
            {"0x32",
             {{"collections", {{"0x32", {{"privileges", {"Delete"}}}}}}}}});
}

class ScopePrivCollectionVisibility : public ::testing::Test {
public:
    // In this test fixture, the 'user' has no collection privileges for the
    // bucket, they always have collection privileges @ scope 0x32. This means
    // they can Read everything under scope 0x32, any failure under scope 0x32
    // is a plain failure, and any other failure (e.g. check under scope 0x33)
    // can be differentiated as Fail vs FailNoPrivileges
    nlohmann::json scopeAccess = {
            {"privileges", {"Audit"}},
            {"scopes", {{"0x32", {{"privileges", {"Read"}}}}}}};
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

    // Can Read all in scope:0x32
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, {}).success());
    // Can Read all in collection:0x32 (because it is in scope:0x32)
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, 0x32).success());

    // User definitely doesn't have Upsert for the scope:0x32
    status = bucket.check(Privilege::Upsert, 0x32, {});
    EXPECT_TRUE(status.failed());
    // Exact error says Fail, which should translate to an access error
    // collections in scope:0x32 cannot be upserted, but they're not 'invisible'
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // User definitely doesn't have Upsert for the scope:0x33 (or any access)
    status = bucket.check(Privilege::Upsert, 0x33, {});
    EXPECT_TRUE(status.failed());
    // Failed but distinguishable from the other failure
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    // Try a couple of collections, 0x33 will never be mentioned in either test
    // variant, but should still be 'visible' because of scope 0x32 Read
    for (int cid : {0x32, 0x33}) {
        // Try checks with scope.collection
        status = bucket.check(Privilege::Upsert, 0x32, cid);
        // User definitely doesn't have Upsert for the scope/collection
        EXPECT_TRUE(status.failed());
        // Exact error says Fail, which should translate to an access error
        // collections in scope:0x32 cannot be upserted, but they're not
        // 'invisible'
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << cid;
    }

    // User definitely doesn't have Upsert for the scope:0x33 (or any access)
    status = bucket.check(Privilege::Upsert, 0x33, 0x32);
    EXPECT_TRUE(status.failed());
    // However user has no privileges at all for scope:0x33 so it should be
    // distinguishable from plain Fail
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());
}

TEST_F(ScopePrivCollectionVisibility, scopeOnly) {
    test({});
}

TEST_F(ScopePrivCollectionVisibility, collections) {
    test(nlohmann::json{{"0x32", {{"privileges", {"Delete"}}}}});
}

// case is scope{c1,c2}  privs scope{c1} lookup for c2

class CollectionPrivVisibility : public ::testing::Test {
public:
    // In this test fixture, the 'user' has no collection privileges for the
    // bucket or scope, they only have collection privileges at given
    // collections (depending on the test)
};

TEST_F(CollectionPrivVisibility, can_read_0x32_only) {
    nlohmann::json access = R"({
  "privileges": [
    "Audit"
  ],
  "scopes": {
    "0x32": {
      "collections": {
        "0x32": {
          "privileges": [
            "Read"
          ]
        }
      }
    }
  }
})"_json;

    // Read for collection 0x32 only
    // access["collections"] = {{"0x32", {{"privileges", {"Read"}}}}};
    MockBucket bucket(access);
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());

    // Cannot Read the bucket
    auto status = bucket.check(Privilege::Read, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Cannot Read a scope
    status = bucket.check(Privilege::Read, 0x32, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Cannot Read or Upsert collection 0x32.0x33 and it's not visible (no
    // privs)
    status = bucket.check(Privilege::Read, 0x32, 0x33);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    status = bucket.check(Privilege::Upsert, 0x32, 0x33);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::FailNoPrivileges,
              status.getStatus());

    // Can read 0x32,0x32
    status = bucket.check(Privilege::Read, 0x32, 0x32);
    EXPECT_TRUE(status.success());

    // Plain fail for Upsert 0x32.0x32
    status = bucket.check(Privilege::Upsert, 0x32, 0x32);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());
}

} // namespace cb::rbac
