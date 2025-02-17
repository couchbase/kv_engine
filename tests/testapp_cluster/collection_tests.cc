/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"
#include "memcached/rbac/privileges.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <include/mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <filesystem>
#include <thread>

class CollectionsTests : public cb::test::ClusterTest {
public:
    void SetUp() override {
        cb::test::ClusterTest::SetUp();
        // Upsert a user who has privs only against a scope
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [],
      "scopes": {
        "0": {
          "privileges": [
            "Read"
          ]
        }
      }
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
    }

    const std::string username{"ScopeUser"};
    const std::string password{"ScopeUser"};

    void testSubdocRbac(MemcachedConnection& conn);

    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin");
        conn->selectBucket(bucket->getName());
        conn->setFeature(cb::mcbp::Feature::Collections, true);
        return conn;
    }

    static void mutate(MemcachedConnection& conn,
                       std::string id,
                       MutationType type) {
        Document doc{};
        doc.value = R"({"json":true})";
        doc.info.id = std::move(id);
        doc.info.datatype = cb::mcbp::Datatype::JSON;
        const auto info = conn.mutate(doc, Vbid{0}, type);
        EXPECT_NE(0, info.cas);
    }
};

// Verify that I can store documents within the collections
TEST_F(CollectionsTests, TestBasicOperations) {
    auto conn = getConnection();
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                             "TestBasicOperations"),
           MutationType::Add);
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                             "TestBasicOperations"),
           MutationType::Set);
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                             "TestBasicOperations"),
           MutationType::Replace);
}

TEST_F(CollectionsTests, TestInvalidCollection) {
    // Test using a non-bucket privileged user so we exercise the
    // Connection::collectionInfo functionality
    auto conn = getConnection();
    conn->authenticate(username);
    conn->selectBucket("default");

    // collections 1 to 7 are reserved and invalid from a client
    for (int illegalCollection = 1; illegalCollection <= 7;
         illegalCollection++) {
        try {
            mutate(*conn,
                   DocKeyView::makeWireEncodedString(
                           CollectionID(illegalCollection,
                                        CollectionID::SkipIDVerificationTag{}),
                           "TestInvalidCollection"),
                   MutationType::Add);
            FAIL() << "Einval not detected cid:" << illegalCollection;
        } catch (const ConnectionError& e) {
            EXPECT_EQ(cb::mcbp::Status::Einval, e.getReason());
        }
    }

    try {
        // collection 1000 is unknown
        mutate(*conn,
               DocKeyView::makeWireEncodedString(1000, "TestInvalidCollection"),
               MutationType::Add);
        FAIL() << "UnknownCollection not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection, e.getReason());
        EXPECT_EQ("6", e.getErrorJsonContext()["manifest_uid"]);
    }
}

static BinprotSubdocCommand subdocInsertXattrPath(const std::string& key,
                                                  const std::string& path) {
    using namespace cb::mcbp;
    using namespace cb::mcbp::subdoc;
    return {ClientOpcode::SubdocDictAdd,
            key,
            path,
            "{}",
            PathFlag::XattrPath | PathFlag::Mkdir_p,
            DocFlag::Mkdoc};
}

void CollectionsTests::testSubdocRbac(MemcachedConnection& conn) {
    auto fruit = DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                                   "TestBasicRbac");

    // Check subdoc, privilege checks happen in 2 places
    // 1) we're checking we can do the xattr write
    auto subdoc = subdocInsertXattrPath(fruit, "nosys");
    auto response = BinprotMutationResponse(conn.execute(subdoc));
    EXPECT_TRUE(response.isSuccess()) << to_string(response.getStatus());
    subdoc = subdocInsertXattrPath(fruit, "_sys");
    response = BinprotMutationResponse(conn.execute(subdoc));
    EXPECT_TRUE(response.isSuccess()) << to_string(response.getStatus());

    // 2) XTOC evaluates xattr read privs separately.
    BinprotSubdocMultiLookupCommand cmd;
    BinprotSubdocMultiLookupResponse resp;
    cmd.setKey(fruit);
    cmd.addGet("$XTOC", cb::mcbp::subdoc::PathFlag::XattrPath);

    // Response will include system xattrs
    conn.sendCommand(cmd);
    conn.recvResponse(resp);
    EXPECT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
    ASSERT_EQ(1, resp.getResults().size());
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getResults().front().status);
    EXPECT_EQ(R"(["_sys","nosys"])", resp.getResults().front().value);

    // Response won't include system xattrs
    conn.dropPrivilege(cb::rbac::Privilege::SystemXattrRead);
    resp = BinprotSubdocMultiLookupResponse();
    conn.sendCommand(cmd);
    conn.recvResponse(resp);
    EXPECT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
    const auto results = resp.getResults();
    ASSERT_EQ(1, results.size());
    EXPECT_EQ(cb::mcbp::Status::Success, results.front().status);
    EXPECT_EQ(R"(["nosys"])", results.front().value);
}

TEST_F(CollectionsTests, TestBasicRbac) {
    const std::string username{"TestBasicRbac"};
    const std::string password{"TestBasicRbac"};
    cluster->getAuthProviderService().upsertUser({username, password, R"({
        "buckets": {
            "default": {
            "scopes": {
                "0": {
                "collections": {
                    "0": {
                    "privileges": [
                        "Read"
                    ]
                    },
                    "9": {
                    "privileges": [
                        "Read",
                        "Insert",
                        "Delete",
                        "Upsert",
                        "SystemXattrRead",
                        "SystemXattrWrite"
                    ]
                    }
                }
                }
            }
            }
        },
        "privileges": [],
        "domain": "external"
        })"_json});

    auto conn = getConnection();
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::defaultC,
                                             "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                             "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                             "TestBasicRbac"),
           MutationType::Add);

    // I'm allowed to read from the default collection and read and write
    // to the fruit collection
    conn->authenticate(username);
    conn->selectBucket("default");

    testSubdocRbac(*conn);

    conn->get(DocKeyView::makeWireEncodedString(CollectionEntry::defaultC,
                                                "TestBasicRbac"),
              Vbid{0});
    conn->get(DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                                "TestBasicRbac"),
              Vbid{0});

    // I cannot get from the vegetable collection
    try {
        conn->get(DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                                    "TestBasicRbac"),
                  Vbid{0});
        FAIL() << "Should not be able to fetch in vegetable collection";
    } catch (const ConnectionError& error) {
        // No privileges so we would get unknown collection error
        ASSERT_TRUE(error.isUnknownCollection())
                << to_string(error.getReason());
        EXPECT_EQ(
                cluster->collections.getUidString(),
                error.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }

    // I'm only allowed to write in Fruit
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::fruit,
                                             "TestBasicRbac"),
           MutationType::Set);
    for (auto c : std::vector<CollectionID>{
                 {CollectionEntry::defaultC, CollectionEntry::vegetable}}) {
        try {
            mutate(*conn,
                   DocKeyView::makeWireEncodedString(c, "TestBasicRbac"),
                   MutationType::Set);
            FAIL() << "Should only be able to store in the fruit collection";
        } catch (const ConnectionError& error) {
            if (c == CollectionEntry::defaultC.getId()) {
                // Default: we have read, but not write so access denied
                ASSERT_TRUE(error.isAccessDenied());
            } else {
                // Nothing at all in vegetable, so unknown
                ASSERT_TRUE(error.isUnknownCollection());
                EXPECT_EQ(cluster->collections.getUidString(),
                          error.getErrorJsonContext()["manifest_uid"]
                                  .get<std::string>());
            }
        }
    }

    auto fruit = conn->getCollectionId("_default.fruit");
    EXPECT_EQ(CollectionEntry::fruit.getId(), fruit.getCollectionId());
    auto defaultScope = conn->getScopeId("_default");
    EXPECT_EQ(CollectionEntry::defaultC.getId(),
              uint32_t{defaultScope.getScopeId()});
    // Now failure, we have no privs in customer_scope
    try {
        conn->getCollectionId("customer_scope.customer_collection1");
        FAIL() << "Expected an error";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isUnknownCollection());
        EXPECT_EQ(
                cluster->collections.getUidString(),
                error.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }
    try {
        conn->getScopeId("customer_scope");
        FAIL() << "Expected an error";
    } catch (const ConnectionError& error) {
        EXPECT_TRUE(error.isUnknownScope());
        EXPECT_EQ(
                cluster->collections.getUidString(),
                error.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }

    // Check we get a restricted view of the manifest, only have access to
    // two collections
    auto json = conn->getCollectionsManifest();
    ASSERT_EQ(1, json["scopes"].size());
    EXPECT_EQ("_default", json["scopes"][0]["name"]);
    EXPECT_EQ("0", json["scopes"][0]["uid"]);
    EXPECT_EQ(2, json["scopes"][0]["collections"].size());
    EXPECT_EQ(1,
              std::ranges::count_if(json["scopes"][0]["collections"],
                                    [](nlohmann::json entry) {
                                        return entry["name"] == "_default" &&
                                               entry["uid"] == "0";
                                    }));
    EXPECT_EQ(1,
              std::ranges::count_if(json["scopes"][0]["collections"],
                                    [](nlohmann::json entry) {
                                        return entry["name"] == "fruit" &&
                                               entry["uid"] == "9";
                                    }));

    cluster->getAuthProviderService().removeUser(username);
}

TEST_F(CollectionsTests, ResurrectCollection) {
    // Store a key to the collection
    auto conn = getConnection();
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                             "Generation1"),
           MutationType::Add);

    auto bucket = cluster->getBucket("default");
    ASSERT_NE(nullptr, bucket);
    cluster->collections.remove(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    cluster->collections.add(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    mutate(*conn,
           DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                             "Generation2"),
           MutationType::Add);

    // Generation 1 is lost, the first drop was accepted and the keys created
    // before the resurrection of vegetable are dropped.
    try {
        conn->get(DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                                    "Generation1"),
                  Vbid{0});
        FAIL() << "Should not be able to fetch in Generation1 key";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isNotFound()) << to_string(error.getReason());
    }
    conn->get(DocKeyView::makeWireEncodedString(CollectionEntry::vegetable,
                                                "Generation2"),
              Vbid{0});
}

// Remove the data directory and SetCollection should fail, but work again
// once the data-dir is restored.
TEST_F(CollectionsTests, SetCollectionsWithNoDirectory) {
    auto bucket = cluster->getBucket("default");
    ASSERT_NE(nullptr, bucket);

    // Remove the data dir, with a retry loop. Windows KV maybe accessing
    // the directory.
    cluster->iterateNodes([this](const cb::test::Node& node) {
        const auto path = node.directory / "default";
        int tries = 0;
        constexpr int retries = 50;
        cluster->removeWithRetry(path,
                                 [&tries, &retries](const std::exception& e) {
                                     using namespace std::chrono_literals;
                                     std::this_thread::sleep_for(100ms);
                                     return (++tries < retries);
                                 });
    });

    cluster->collections.remove(CollectionEntry::vegetable);
    try {
        bucket->setCollectionManifest(cluster->collections.getJson());
    } catch (const ConnectionError& err) {
        EXPECT_EQ(cb::mcbp::Status::CannotApplyCollectionsManifest,
                  err.getReason());
    }

    cluster->iterateNodes([this](const cb::test::Node& node) {
        const auto path = node.directory / "default";
        create_directories(path);
    });
    try {
        bucket->setCollectionManifest(cluster->collections.getJson());
    } catch (const ConnectionError& err) {
        FAIL() << "setCollectionManifest Should not of thrown "
               << err.getErrorContext();
    }
}

TEST_F(CollectionsTests, getId_MB_44807) {
    auto conn = getConnection();
    std::string maxPath = ScopeEntry::maxScope.name + "." +
                          CollectionEntry::maxCollection.name;
    EXPECT_EQ(251 + 1 + 251, maxPath.size());
    auto max = conn->getCollectionId(maxPath);
    EXPECT_EQ(CollectionEntry::maxCollection.getId(), max.getCollectionId());

    try {
        maxPath += "a"; // and just double check the expected limit
        conn->getCollectionId(maxPath);
        FAIL() << "Expected getCollectionId to fail";
    } catch (const ConnectionError& err) {
    }
}

// @todo: The key encoding will be removed - check it works until then
TEST_F(CollectionsTests, getId_MB_44807_using_keylen) {
    auto conn = getConnection();
    // Use a generic command to drive get_collection_id using key input
    BinprotGenericCommand command1(cb::mcbp::ClientOpcode::CollectionsGetID,
                                   "_default.fruit");
    const auto response1 = BinprotResponse(conn->execute(command1));
    if (!response1.isSuccess()) {
        throw ConnectionError("Failed getCollectionId", response1);
    }

    auto extras1 = response1.getResponse().getExtdata();
    if (extras1.size() != sizeof(cb::mcbp::request::GetCollectionIDPayload)) {
        throw std::logic_error("getCollectionId invalid extra length");
    }
    cb::mcbp::request::GetCollectionIDPayload payload1;
    std::copy_n(extras1.data(),
                extras1.size(),
                reinterpret_cast<uint8_t*>(&payload1));
    EXPECT_EQ(CollectionEntry::fruit.getId(), payload1.getCollectionId());

    // Use a generic command to drive get_collection_id using key input
    BinprotGenericCommand command2(
            cb::mcbp::ClientOpcode::CollectionsGetScopeID, "_default");
    const auto response2 = BinprotResponse(conn->execute(command2));
    if (!response2.isSuccess()) {
        throw ConnectionError("Failed getScopeId", response2);
    }

    auto extras2 = response2.getResponse().getExtdata();
    if (extras2.size() != sizeof(cb::mcbp::request::GetScopeIDPayload)) {
        throw std::logic_error("getScopeId invalid extra length");
    }
    cb::mcbp::request::GetScopeIDPayload payload2;
    std::copy_n(extras2.data(),
                extras2.size(),
                reinterpret_cast<uint8_t*>(&payload2));
    EXPECT_EQ(CollectionEntry::defaultC.getId(),
              uint32_t{payload2.getScopeId()});
}

TEST_F(CollectionsTests, getId_MB_64026) {
    auto conn = getConnection();
    try {
        conn->getScopeId("_default");
        conn->getCollectionId("_default.mb64026");
        FAIL() << "Expected getCollectionId to fail";
    } catch (const ConnectionError& err) {
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection, err.getReason());
        auto ctx = err.getErrorJsonContext();
        EXPECT_TRUE(ctx.contains("manifest_uid")) << ctx.dump();
    }

    // Now try an unknown scope
    try {
        conn->getCollectionId("mb64026.mb64026");
        FAIL() << "Expected getCollectionId to fail";
    } catch (const ConnectionError& err) {
        EXPECT_EQ(cb::mcbp::Status::UnknownScope, err.getReason());
        auto ctx = err.getErrorJsonContext();
        EXPECT_TRUE(ctx.contains("manifest_uid")) << ctx.dump();
    }
}
