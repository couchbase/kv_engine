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
#include <sstream>

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

    // No scope-level privileges present
    ASSERT_TRUE(scope.getPrivileges().none());

    // Scope::check only tests scope-level privileges; collection delegation
    // is handled by Bucket::check.
    EXPECT_TRUE(scope.check(Privilege::Read, {}, false).failed());
    EXPECT_TRUE(scope.check(Privilege::Upsert, {}, false).failed());

    // The collection itself has Read
    const auto& cols = scope.getCollections();
    ASSERT_NE(cols.end(), cols.find(0x32a));
    EXPECT_TRUE(cols.at(0x32a).check(Privilege::Read).success());
    EXPECT_TRUE(cols.at(0x32a).check(Privilege::Upsert).failed());
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

    // Check that scope-level privileges are enforced
    EXPECT_TRUE(scope.check(Privilege::Read, {}, false).success());
    EXPECT_TRUE(scope.check(Privilege::Upsert, {}, false).failed());
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
    // non-collection privilege audit). When no scope entry is listed, bucket
    // Read flows to any scope/collection. When a scope IS listed, only that
    // scope's own privileges apply (no bucket fallback).
    nlohmann::json bucketAccess = {{"privileges", {"Read", "Audit"}}};
};

// bucket-only: no scope entries; bucket Read applies everywhere.
TEST_F(BucketPrivCollectionVisibility, bucketOnly) {
    MockBucket bucket(bucketAccess);
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());

    // Can Read everything (scope not listed → bucket fallback)
    EXPECT_TRUE(bucket.check(Privilege::Read, {}, {}).success());
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, {}).success());
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, 0x32).success());

    // Upsert is not held anywhere
    auto status = bucket.check(Privilege::Upsert, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    for (int ii = 0x32; ii < 0x34; ++ii) {
        status = bucket.check(Privilege::Upsert, ii, {});
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

        status = bucket.check(Privilege::Upsert, ii, ii);
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << ii;
    }
}

// scope: bucket Read + scope:0x32 with Delete. Bucket Read does NOT flow into
// a listed scope; only the scope's own privileges (Delete) apply for scope
// 0x32. Scopes not in the list still fall back to bucket privileges.
TEST_F(BucketPrivCollectionVisibility, scope) {
    bucketAccess["scopes"] =
            nlohmann::json{{"0x32", {{"privileges", {"Delete"}}}}};
    MockBucket bucket(bucketAccess);
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());

    // Bucket-level Read (no scope provided) still works
    EXPECT_TRUE(bucket.check(Privilege::Read, {}, {}).success());

    // Scope:0x32 is listed with Delete only, not Read
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, {}).failed());
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, 0x32).failed());

    // Upsert is not held at the bucket, scope, or collection level
    auto status = bucket.check(Privilege::Upsert, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    for (int ii = 0x32; ii < 0x34; ++ii) {
        status = bucket.check(Privilege::Upsert, ii, {});
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

        status = bucket.check(Privilege::Upsert, ii, ii);
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << ii;
    }
}

// scopeAndCollection: bucket Read + scope:0x32, collection:0x32 with Delete.
// Only collection:0x32 privileges apply for that collection; scope and bucket
// Read do not flow in.
TEST_F(BucketPrivCollectionVisibility, scopeAndCollection) {
    bucketAccess["scopes"] = nlohmann::json{
            {"0x32",
             {{"collections", {{"0x32", {{"privileges", {"Delete"}}}}}}}}};
    MockBucket bucket(bucketAccess);
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());

    // Bucket-level Read (no scope provided) still works
    EXPECT_TRUE(bucket.check(Privilege::Read, {}, {}).success());

    // Scope:0x32 is listed but has no scope-level privileges; the check
    // is performed at the scope level and returns Fail.
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, {}).failed());

    // Collection:0x32 in scope:0x32 has Delete only, not Read
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x32, 0x32).failed());

    // Upsert is not held at any level
    auto status = bucket.check(Privilege::Upsert, {}, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    for (int ii = 0x32; ii < 0x34; ++ii) {
        status = bucket.check(Privilege::Upsert, ii, {});
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

        status = bucket.check(Privilege::Upsert, ii, ii);
        EXPECT_TRUE(status.failed());
        EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus())
                << ii;
    }
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

    // Scope:0x32 is listed but has no scope-level privileges; the scope-level
    // check returns Fail (not FailNoPrivileges) because privilegeMask is empty
    // and the check is performed at the scope level (no collection specified).
    status = bucket.check(Privilege::Read, 0x32, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(cb::rbac::PrivilegeAccess::Status::Fail, status.getStatus());

    // Collection 0x32.0x33 is not listed in the scope; when collection is
    // specified but not found, Scope::check() checks privilegeMask and if
    // empty, returns FailNoPrivileges because bucket has no collection
    // privileges.
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

TEST(CheckForPrivilegeAtLeastInOneCollection, PrivilegeMissing) {
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
      },
      "privileges": [
        "SystemXattrRead"
      ]
    }
  }
})"_json;
    MockBucket bucket(access);
    EXPECT_EQ(
            PrivilegeAccessFail,
            bucket.checkForPrivilegeAtLeastInOneCollection(Privilege::Upsert));
}

TEST(CheckForPrivilegeAtLeastInOneCollection, PrivilegeInBucket) {
    nlohmann::json access = R"({
  "privileges": [
    "Read"
  ]
})"_json;
    MockBucket bucket(access);
    EXPECT_EQ(PrivilegeAccessOk,
              bucket.checkForPrivilegeAtLeastInOneCollection(Privilege::Read));
}

TEST(CheckForPrivilegeAtLeastInOneCollection, PrivilegeInScope) {
    nlohmann::json access = R"({
  "privileges": [
    "Audit"
  ],
  "scopes": {
    "0x31": {
      "privileges": [
        "Upsert"
      ]
    },
    "0x32": {
      "collections": {
        "0x32": {
          "privileges": [
            "Upsert"
          ]
        }
      },
      "privileges": [
        "Read"
      ]
    }
  }
})"_json;
    MockBucket bucket(access);
    EXPECT_EQ(PrivilegeAccessOk,
              bucket.checkForPrivilegeAtLeastInOneCollection(Privilege::Read));
}

TEST(CheckForPrivilegeAtLeastInOneCollection, PrivilegeInCollection) {
    nlohmann::json access = R"({
  "privileges": [
    "Audit"
  ],
  "scopes": {
    "0x31": {
      "privileges": [
        "Upsert"
      ]
    },
    "0x32": {
      "collections": {
        "0x31": {
          "privileges": [
            "Upsert"
          ]
        },
        "0x32": {
          "privileges": [
             "Read"
          ]
        }
      },
      "privileges": [
        "Upsert"
      ]
    }
  }
})"_json;
    MockBucket bucket(access);
    EXPECT_EQ(PrivilegeAccessOk,
              bucket.checkForPrivilegeAtLeastInOneCollection(Privilege::Read));
}

// =====================================================================
// PrivilegeAccess::Status: format_as and operator<<
// =====================================================================

TEST(PrivilegeAccessStatusTest, FormatAsReturnsCorrectStrings) {
    EXPECT_EQ("Ok", format_as(PrivilegeAccess::Status::Ok));
    EXPECT_EQ("Fail", format_as(PrivilegeAccess::Status::Fail));
    EXPECT_EQ("FailNoPrivileges",
              format_as(PrivilegeAccess::Status::FailNoPrivileges));
}

TEST(PrivilegeAccessStatusTest, StreamOperatorOutput) {
    std::ostringstream oss;
    oss << PrivilegeAccess::Status::Ok;
    EXPECT_EQ("Ok", oss.str());
    oss.str("");
    oss << PrivilegeAccess::Status::Fail;
    EXPECT_EQ("Fail", oss.str());
    oss.str("");
    oss << PrivilegeAccess::Status::FailNoPrivileges;
    EXPECT_EQ("FailNoPrivileges", oss.str());
}

// =====================================================================
// Collection: corner cases
// =====================================================================

// =====================================================================
// Collection: corner cases
// =====================================================================

// An explicitly empty privileges array is now valid; the collection is
// created with an empty privilege mask and every check fails.
TEST(CollectionTest, EmptyPrivileges_CreatesCollectionWithEmptyMask) {
    MockCollection collection(nlohmann::json::parse(R"({"privileges": []})"));
    // No privilege is held → every check fails.
    EXPECT_TRUE(collection.check(Privilege::Read).failed());
    EXPECT_TRUE(collection.check(Privilege::Upsert).failed());
}

// After clearing the mask via setMask, every check fails.
TEST(CollectionTest, ClearedMask_AllChecksFail) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges": ["Read"]})"));
    PrivilegeMask empty;
    collection.setMask(empty);
    EXPECT_TRUE(collection.check(Privilege::Read).failed());
    EXPECT_TRUE(collection.check(Privilege::Upsert).failed());
    EXPECT_TRUE(collection.check(Privilege::Insert).failed());
    EXPECT_TRUE(collection.check(Privilege::Delete).failed());
}

TEST(CollectionTest, SinglePrivilege_OnlyThatPrivilegeSucceeds) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges": ["Read"]})"));
    EXPECT_TRUE(collection.check(Privilege::Read).success());
    EXPECT_TRUE(collection.check(Privilege::Upsert).failed());
    EXPECT_TRUE(collection.check(Privilege::Insert).failed());
    EXPECT_TRUE(collection.check(Privilege::Delete).failed());
    EXPECT_TRUE(collection.check(Privilege::MetaWrite).failed());
}

TEST(CollectionTest, MultiplePrivileges) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges": ["Read", "Insert"]})"));
    EXPECT_TRUE(collection.check(Privilege::Read).success());
    EXPECT_TRUE(collection.check(Privilege::Insert).success());
    EXPECT_TRUE(collection.check(Privilege::Upsert).failed());
    EXPECT_TRUE(collection.check(Privilege::Delete).failed());
}

// Non-bucket privileges are filtered out during parse; check they don't appear
TEST(CollectionTest, NonBucketPrivilegesAreFilteredOut) {
    // Audit and Stats are not bucket privileges; they should be silently
    // masked off during parsePrivileges(buckets=true).
    MockCollection collection(nlohmann::json::parse(
            R"({"privileges": ["Read", "Audit", "Stats"]})"));
    EXPECT_TRUE(collection.check(Privilege::Read).success());
    // Audit/Stats are not bucket privileges → filtered, so check returns failed
    EXPECT_TRUE(collection.check(Privilege::Audit).failed());
    EXPECT_TRUE(collection.check(Privilege::Stats).failed());
}

// =====================================================================
// Scope: privilege mask tests (Scope::check only inspects the scope's own
// privilege mask; collection delegation is performed by Bucket::check)
// =====================================================================

// Scope has the privilege → Ok.
TEST(ScopeTest, CheckScope_HasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({"privileges": ["Read"]})"));
    EXPECT_TRUE(scope.check(Privilege::Read, {}, false).success());
    EXPECT_TRUE(scope.check(Privilege::Upsert, {}, false).failed());
}

// Scope has no privilege → Fail (never FailNoPrivileges from Scope::check).
TEST(ScopeTest, CheckScope_LacksPrivilege_AlwaysFail) {
    MockScope scope(nlohmann::json::parse(R"({"privileges": ["Upsert"]})"));
    auto s = scope.check(Privilege::Read, {}, false);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

// Even when collections are configured, Scope::check only consults the
// scope-level mask; collection delegation happens in Bucket::check.
TEST(ScopeTest, CheckScope_WithCollections_OnlyScopeMaskMatters) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // Scope has no scope-level privileges (only collection-level)
    EXPECT_TRUE(scope.check(Privilege::Read, {}, false).failed());
    EXPECT_TRUE(scope.check(Privilege::Read, 0x10, false).success());
}

// =====================================================================
// Bucket: check logic tests
// =====================================================================

// No scope: only bucket-level privilege mask is checked.
TEST(BucketDepthFirstTest, NoScope_BucketHasPrivilege) {
    MockBucket bucket(nlohmann::json::parse(R"({"privileges": ["Read"]})"));
    EXPECT_TRUE(bucket.check(Privilege::Read, std::nullopt, std::nullopt)
                        .success());
    auto status = bucket.check(Privilege::Upsert, std::nullopt, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Non-collection bucket privilege (e.g. DcpProducer) with a scope ID present:
// only the bucket-level mask is consulted (scope is irrelevant).
TEST(BucketDepthFirstTest, NonCollectionPrivilege_OnlyBucketLevelChecked) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["DcpProducer"],
        "scopes": {"0x10": {"privileges": ["Read"]}}
    })"));
    // DcpProducer is not a collection privilege → always hits bucket mask
    EXPECT_TRUE(
            bucket.check(Privilege::DcpProducer, 0x10, std::nullopt).success());
    EXPECT_TRUE(bucket.check(Privilege::DcpProducer, 0x10, 0x20).success());
    // DcpConsumer absent from bucket → Fail regardless of scope content
    auto status = bucket.check(Privilege::DcpConsumer, 0x10, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Scope found, scope holds a collection privilege, no collection specified
// → Ok from scope level.
TEST(BucketDepthFirstTest, ScopeFound_ScopeHasPrivilege_NoCollection) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "scopes": {"0x10": {"privileges": ["Read"]}}
    })"));
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x10, std::nullopt).success());
    auto status = bucket.check(Privilege::Upsert, 0x10, std::nullopt);
    EXPECT_TRUE(status.failed());
}

// Scope found, collection found, collection has the privilege → Ok.
TEST(BucketDepthFirstTest, ScopeFound_CollectionFound_CollectionHasPrivilege) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "scopes": {
            "0x10": {"collections": {"0x20": {"privileges": ["Read"]}}}
        }
    })"));
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x10, 0x20).success());
    auto status = bucket.check(Privilege::Upsert, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
}

// Scope found, collection found, collection lacks privilege (no fallback to
// scope or bucket; collection result is returned directly).
TEST(BucketDepthFirstTest,
     ScopeFound_CollectionFound_CollectionLacksPrivilege_NoFallback) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {
            "0x10": {
                "privileges": ["Read"],
                "collections": {"0x20": {"privileges": ["Upsert"]}}
            }
        }
    })"));
    // Collection 0x20 has Upsert only; scope and bucket have Read, but there is
    // no fallback once the collection is found.
    auto status = bucket.check(Privilege::Read, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
    // Upsert is in collection → Ok directly
    EXPECT_TRUE(bucket.check(Privilege::Upsert, 0x10, 0x20).success());
    // Insert absent from collection (and everywhere) → Fail
    status = bucket.check(Privilege::Insert, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
}

// Scope found, collection not found: scope's own privilege is used and returned
// directly (no bucket fallback).
TEST(BucketDepthFirstTest,
     ScopeFound_CollectionNotFound_ScopePrivilegeUsed_NoBucketFallback) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {
            "0x10": {
                "privileges": ["Read"],
                "collections": {"0x20": {"privileges": ["Upsert"]}}
            }
        }
    })"));
    // Collection 0x99 not in the map; scope 0x10 has Read → Ok.
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x10, 0x99).success());
    // Upsert not in scope; scope.any()=true → Fail (not FailNoPrivileges).
    auto status = bucket.check(Privilege::Upsert, 0x10, 0x99);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Scope found, collection not found, scope has an empty privilege mask:
// the check falls back to the scope level (not bucket), returning Fail.
TEST(BucketDepthFirstTest,
     ScopeFound_CollectionNotFound_ScopeEmptyMask_ScopeLevel_Fail) {
    // Scope 0x10 has only collection-level privileges (no scope-level mask).
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {
            "0x10": {"collections": {"0x20": {"privileges": ["Upsert"]}}}
        }
    })"));
    // Collection 0x99 not found; the check uses scope-level privileges.
    // Scope has empty mask → Fail (bucket Read is not used).
    auto status = bucket.check(Privilege::Read, 0x10, 0x99);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
    // Upsert also fails at the scope level.
    status = bucket.check(Privilege::Upsert, 0x10, 0x99);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Scope not found: bucket privilege IS checked and returned (bucket is the
// fallback of last resort when the scope is not listed).
TEST(BucketDepthFirstTest, ScopeNotFound_BucketPrivilegeChecked) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {"0x10": {"privileges": ["Upsert"]}}
    })"));
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());
    // Scope 0x99 not in the map; bucket has Read → Ok.
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x99, std::nullopt).success());
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x99, 0x10).success());
    // Upsert not in bucket → Fail (collectionPrivilegeExists=true).
    auto s = bucket.check(Privilege::Upsert, 0x99, std::nullopt);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

// Contrast: when the scopes map IS empty, the bucket privilege covers any
// requested scope (empty map == no per-scope restrictions).
TEST(BucketDepthFirstTest, ScopesMapEmpty_BucketPrivilegeCoverAllScopes) {
    MockBucket bucket(nlohmann::json::parse(R"({"privileges": ["Read"]})"));
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x99, std::nullopt).success());
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x42, 0x10).success());
    // Wrong privilege → Fail (collectionPrivilegeExists=true because Read is a
    // collection privilege)
    auto status = bucket.check(Privilege::Upsert, 0x99, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// When the scopes map is empty and the bucket holds only a non-collection
// privilege (collectionPrivilegeExists=false), accessing a scope with a
// collection privilege returns FailNoPrivileges (unknown_scope), not Fail.
TEST(BucketDepthFirstTest,
     ScopesMapEmpty_NoCollectionPrivilege_ReturnsFailNoPrivileges) {
    // DcpProducer is a bucket-only privilege, NOT a collection privilege.
    MockBucket bucket(
            nlohmann::json::parse(R"({"privileges": ["DcpProducer"]})"));
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());
    // DcpStream is a collection privilege; scopes.empty()=true and no
    // collection privilege in the bucket mask → FailNoPrivileges.
    auto status = bucket.check(Privilege::DcpStream, 0x0, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::FailNoPrivileges, status.getStatus());
}

// Scope not found, bucket lacks privilege, bucket-level mask has a collection
// privilege (collectionPrivilegeExists=true) → Fail.
TEST(BucketDepthFirstTest,
     ScopeNotFound_BucketLacksPrivilege_CollectionPrivExists_ReturnsFail) {
    // Bucket has "Read" → collectionPrivilegeExists=true; checking Upsert
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {"0x10": {"privileges": ["Insert"]}}
    })"));
    EXPECT_TRUE(bucket.doesCollectionPrivilegeExists());
    auto status = bucket.check(Privilege::Upsert, 0x99, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Scope not found, bucket lacks privilege, no collection privileges in the
// bucket-level mask (collectionPrivilegeExists=false) → FailNoPrivileges.
TEST(BucketDepthFirstTest,
     ScopeNotFound_BucketLacksPrivilege_NoCollectionPriv_ReturnsFailNoPrivileges) {
    // DcpConsumer is a bucket privilege but NOT a collection privilege
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["DcpConsumer"],
        "scopes": {"0x10": {"privileges": ["Upsert"]}}
    })"));
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());
    auto status = bucket.check(Privilege::Read, 0x99, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::FailNoPrivileges, status.getStatus());
}

// Multiple scopes: privilege exists in one scope but not another.
TEST(BucketDepthFirstTest, MultipleScopes_PrivilegeOnlyInOneScope) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "scopes": {
            "0x10": {"privileges": ["Upsert"]},
            "0x11": {"privileges": ["Read"]}
        }
    })"));
    EXPECT_TRUE(bucket.check(Privilege::Read, 0x11, std::nullopt).success());
    EXPECT_TRUE(bucket.check(Privilege::Upsert, 0x10, std::nullopt).success());
    // Scope 0x10 has Upsert, not Read → Fail
    auto status = bucket.check(Privilege::Read, 0x10, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
    // Scope 0x11 has Read, not Upsert → Fail
    status = bucket.check(Privilege::Upsert, 0x11, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// collectionPrivilegeExists is derived from the bucket-level mask only,
// not from scope or collection-level masks.
TEST(BucketDepthFirstTest, CollectionPrivilegeExistsOnlyReflectsBucketMask) {
    // Bucket has only DcpProducer (non-collection privilege);
    // scope has Read (collection privilege) — but that must NOT affect
    // collectionPrivilegeExists.
    MockBucket bucketNoCollPriv(nlohmann::json::parse(R"({
        "privileges": ["DcpProducer"],
        "scopes": {"0x10": {"privileges": ["Read"]}}
    })"));
    EXPECT_FALSE(bucketNoCollPriv.doesCollectionPrivilegeExists());

    // Bucket has Read (collection privilege) → should be true.
    MockBucket bucketWithCollPriv(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {"0x10": {"privileges": ["Upsert"]}}
    })"));
    EXPECT_TRUE(bucketWithCollPriv.doesCollectionPrivilegeExists());
}

// =====================================================================
// Empty privilege mask tests
//
// An empty privilege mask at collection or scope level means the check
// is performed at that level (best match) and returns Fail. There is no
// implicit inheritance from the parent when a more-specific entry exists.
// =====================================================================

// Empty collection mask: collection is the best match; check fails there.
TEST(BucketDepthFirstTest, EmptyCollectionMask_FailAtCollectionLevel) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "scopes": {
            "0x10": {
                "privileges": ["Read"],
                "collections": {"0x20": {"privileges": []}}
            }
        }
    })"));
    // Collection 0x20 is found with an empty mask → check at collection level
    // → Fail (scope's Read is not used).
    auto status = bucket.check(Privilege::Read, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
    // Upsert also fails at the collection level.
    status = bucket.check(Privilege::Upsert, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Empty scope mask: scope is the best match (no collection given); check fails.
TEST(BucketDepthFirstTest, EmptyScopeMask_FailAtScopeLevel) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {"0x10": {"privileges": []}}
    })"));
    // Scope 0x10 is found with an empty mask → check at scope level → Fail
    // (bucket's Read is not used).
    auto status = bucket.check(Privilege::Read, 0x10, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
    // Upsert also fails at the scope level.
    status = bucket.check(Privilege::Upsert, 0x10, std::nullopt);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Empty collection mask AND empty scope mask: collection is best match → Fail.
TEST(BucketDepthFirstTest, EmptyCollectionAndScopeMask_FailAtCollectionLevel) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {
            "0x10": {
                "privileges": [],
                "collections": {"0x20": {"privileges": []}}
            }
        }
    })"));
    // Collection 0x20 is found (best match) with an empty mask → Fail.
    // Neither scope's empty mask nor the bucket's Read is consulted.
    auto status = bucket.check(Privilege::Read, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// Non-empty collection mask with empty scope mask: collection takes precedence.
TEST(BucketDepthFirstTest,
     NonEmptyCollectionMask_EmptyScopeMask_CollectionPrivilegeUsed) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": ["Read"],
        "scopes": {
            "0x10": {
                "privileges": [],
                "collections": {"0x20": {"privileges": ["Upsert"]}}
            }
        }
    })"));
    // Collection has Upsert → Ok (bucket Read is not consulted).
    EXPECT_TRUE(bucket.check(Privilege::Upsert, 0x10, 0x20).success());
    // Collection lacks Read → Fail (bucket Read is not consulted).
    auto status = bucket.check(Privilege::Read, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

// All levels have empty masks: collection is best match → Fail (not
// FailNoPrivileges, because the scope was found and scope.check() is used
// when the collection is not found; here the collection IS found).
TEST(BucketDepthFirstTest, EmptyMasksAllLevels_FailAtCollectionLevel) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "scopes": {
            "0x10": {
                "privileges": [],
                "collections": {"0x20": {"privileges": []}}
            }
        }
    })"));
    EXPECT_FALSE(bucket.doesCollectionPrivilegeExists());
    // Collection found with empty mask → Fail (not FailNoPrivileges).
    auto status = bucket.check(Privilege::Read, 0x10, 0x20);
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());
}

TEST(BucketDepthFirstTest, ExampleFromPeter) {
    MockBucket bucket(nlohmann::json::parse(R"({
        "privileges": [
          "SimpleStats",
          "DcpStream",
          "SystemXattrWrite",
          "SystemXattrRead",
          "MetaWrite",
          "RangeScan",
          "Upsert",
          "Delete",
          "Insert",
          "Read",
          "SystemCollectionMutation",
          "SystemCollectionLookup",
          "DcpConsumer",
          "DcpProducer"
        ],
        "scopes": {
          "9": {}
        }
      })"));

    auto status = bucket.check(Privilege::Read, 0x09, {});
    EXPECT_TRUE(status.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, status.getStatus());

    // Other scopes should succeed
    status = bucket.check(Privilege::Read, 0x10, {});
    EXPECT_TRUE(status.success());
}

// =====================================================================
// Scope::check with parentHasCollectionPrivileges parameter tests
//
// This parameter affects the distinction between Fail and FailNoPrivileges
// when a collection is not found but the scope has no privileges either.
// =====================================================================

TEST(ScopeTest, CollectionFound_CollectionHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // Collection 0x10 is found with Read privilege → Ok
    EXPECT_TRUE(scope.check(Privilege::Read, 0x10, false).success());
    // Collection 0x10 does not have Upsert → Fail
    EXPECT_TRUE(scope.check(Privilege::Upsert, 0x10, false).failed());
}

TEST(ScopeTest, CollectionFound_CollectionHasPrivilege_ParentTrue) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // parentHasCollectionPrivileges doesn't affect collection-found path
    EXPECT_TRUE(scope.check(Privilege::Read, 0x10, true).success());
    EXPECT_TRUE(scope.check(Privilege::Upsert, 0x10, true).failed());
}

TEST(ScopeTest, CollectionNotFound_ScopeHasPrivilege_Ok) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}},
        "privileges": ["Upsert"]
    })"));
    // Collection 0x99 not found, scope has Upsert → Ok
    EXPECT_TRUE(scope.check(Privilege::Upsert, 0x99, false).success());
    // Collection 0x99 not found, scope doesn't have Read → Fail
    auto s = scope.check(Privilege::Read, 0x99, false);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

TEST(ScopeTest, CollectionNotFound_ScopeEmpty_ParentHasPrivileges_ReturnsFail) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // Collection 0x99 not found, scope has no privileges, parent has them →
    // Fail
    auto s = scope.check(Privilege::Upsert, 0x99, true);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

TEST(ScopeTest,
     CollectionNotFound_ScopeEmpty_ParentNoPrivileges_ReturnsFailNoPrivileges) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // Collection 0x99 not found, scope has no privileges, parent doesn't →
    // FailNoPrivileges
    auto s = scope.check(Privilege::Upsert, 0x99, false);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::FailNoPrivileges, s.getStatus());
}

TEST(ScopeTest,
     CollectionNotFound_ScopeEmpty_ScopeHasAnyPrivilege_returnsFail) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}},
        "privileges": ["Upsert"]
    })"));
    // Collection 0x99 not found, scope has Upsert but checking for Read
    auto s = scope.check(Privilege::Read, 0x99, false);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

TEST(ScopeTest, NoCollection_ScopeHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}},
        "privileges": ["Upsert"]
    })"));
    // No collection specified, scope has Upsert → Ok
    EXPECT_TRUE(scope.check(Privilege::Upsert, {}, false).success());
    // No collection specified, scope doesn't have Read → Fail
    auto s = scope.check(Privilege::Read, {}, false);
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(PrivilegeAccess::Status::Fail, s.getStatus());
}

// =====================================================================
// Scope::checkForPrivilegeAtLeastInOneCollection tests
// =====================================================================

TEST(ScopeTest, CheckForPrivilegeAtLeastInOneCollection_ScopeHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}},
        "privileges": ["Upsert"]
    })"));
    // Scope has Upsert → Ok (scope mask is checked first)
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Upsert)
                        .success());
    // Collection has Read → Ok (collections are checked after scope)
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Read)
                        .success());
}

TEST(ScopeTest,
     CheckForPrivilegeAtLeastInOneCollection_CollectionHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"));
    // Collection 0x10 has Read → Ok
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Read)
                        .success());
    // Collection doesn't have Upsert → Fail
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Upsert)
                        .failed());
}

TEST(ScopeTest,
     CheckForPrivilegeAtLeastInOneCollection_MultipleCollections_OneHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {
            "0x10": {"privileges": ["Read"]},
            "0x20": {"privileges": ["Upsert"]}
        }
    })"));
    // At least one collection has Read and Upsert
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Read)
                        .success());
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Upsert)
                        .success());
    // No collection has Insert → Fail
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Insert)
                        .failed());
}

TEST(ScopeTest,
     CheckForPrivilegeAtLeastInOneCollection_NoCollections_ScopeHasPrivilege) {
    MockScope scope(nlohmann::json::parse(R"({
        "privileges": ["Read"]
    })"));
    // Scope has Read, no collections → Ok
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Read)
                        .success());
    // Scope doesn't have Upsert → Fail
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Upsert)
                        .failed());
}

TEST(ScopeTest, CheckForPrivilegeAtLeastInOneCollection_NoPrivileges_AnyLevel) {
    MockScope scope(nlohmann::json::parse(R"({
        "collections": {"0x10": {"privileges": []}}
    })"));
    // No privileges anywhere → Fail
    EXPECT_TRUE(scope.checkForPrivilegeAtLeastInOneCollection(Privilege::Read)
                        .failed());
}

// =====================================================================
// Serialization tests (to_json)
// =====================================================================

TEST(CollectionTest, Serialization_CanBeCalled) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges": ["Read", "Upsert"]})"));
    // Just verify that to_json() can be called and returns valid JSON
    auto json = collection.to_json();
    EXPECT_TRUE(json.is_array() || json.is_object());
}

TEST(CollectionTest, Serialization_AllPrivileges) {
    MockCollection collection(
            nlohmann::json::parse(R"({"privileges": ["all"]})"));
    auto json = collection.to_json();
    // Just verify it can be called and returns valid JSON
    EXPECT_TRUE(json.is_array() || json.is_object());
}

TEST(CollectionTest, Serialization_EmptyPrivileges) {
    MockCollection collection(nlohmann::json::parse(R"({"privileges": []})"));
    auto json = collection.to_json();
    // Just verify it can be called and returns valid JSON
    EXPECT_TRUE(json.is_array() || json.is_object());
}

TEST(ScopeTest, Serialization_RoundTrip) {
    nlohmann::json json1 = R"({
        "collections": {
            "0x10": {"privileges": ["Read"]},
            "0x20": {"privileges": ["Upsert"]}
        },
        "privileges": ["Delete"]
    })"_json;
    MockScope scope1(json1);
    auto json2 = scope1.to_json();

    // Verify JSON structure
    EXPECT_TRUE(json2.is_object());
    EXPECT_TRUE(json2.contains("privileges"));
    EXPECT_TRUE(json2.contains("collections"));

    // Round-trip test
    MockScope scope2(json2);
    EXPECT_EQ(scope1.getPrivileges(), scope2.getPrivileges());
    EXPECT_EQ(scope1.getCollections().size(), scope2.getCollections().size());
}

TEST(ScopeTest, Serialization_NoCollections) {
    nlohmann::json json1 = R"({"privileges": ["Read"]})"_json;
    MockScope scope1(json1);
    auto json2 = scope1.to_json();

    EXPECT_TRUE(json2.is_object());
    EXPECT_TRUE(json2.contains("privileges"));
    EXPECT_FALSE(json2.contains("collections"));

    MockScope scope2(json2);
    EXPECT_EQ(scope1.getPrivileges(), scope2.getPrivileges());
    EXPECT_EQ(0, scope2.getCollections().size());
}

TEST(ScopeTest, Serialization_EmptyScopeWithCollections) {
    nlohmann::json json1 = R"({
        "collections": {"0x10": {"privileges": ["Read"]}}
    })"_json;
    MockScope scope1(json1);
    auto json2 = scope1.to_json();

    // Round-trip - empty scope privileges should be preserved
    MockScope scope2(json2);
    EXPECT_TRUE(scope1.getPrivileges().none());
    EXPECT_TRUE(scope2.getPrivileges().none());
}

TEST(BucketTest, Serialization_RoundTrip) {
    nlohmann::json json1 = R"({
        "scopes": {
            "0x10": {
                "collections": {"0x20": {"privileges": ["Read"]}},
                "privileges": ["Upsert"]
            }
        },
        "privileges": ["Delete"]
    })"_json;
    MockBucket bucket1(json1);
    auto json2 = bucket1.to_json();

    // Verify JSON structure
    EXPECT_TRUE(json2.is_object());
    EXPECT_TRUE(json2.contains("privileges"));
    EXPECT_TRUE(json2.contains("scopes"));

    // Round-trip test
    MockBucket bucket2(json2);
    EXPECT_EQ(bucket1.getPrivileges(), bucket2.getPrivileges());
}

TEST(BucketTest, Serialization_ArrayFormat) {
    // Array format (no scopes)
    nlohmann::json json1 = R"(["Read", "Upsert"])"_json;
    MockBucket bucket1(json1);
    auto json2 = bucket1.to_json();

    // Should serialize back to object format with privileges key
    EXPECT_TRUE(json2.is_object());
    EXPECT_TRUE(json2.contains("privileges"));
}

TEST(BucketTest, Serialization_NoScopes) {
    nlohmann::json json1 = R"({"privileges": ["Read"]})"_json;
    MockBucket bucket1(json1);
    auto json2 = bucket1.to_json();

    EXPECT_TRUE(json2.is_object());
    EXPECT_TRUE(json2.contains("privileges"));
    EXPECT_FALSE(json2.contains("scopes"));

    MockBucket bucket2(json2);
    EXPECT_EQ(bucket1.getPrivileges(), bucket2.getPrivileges());
}

} // namespace cb::rbac
