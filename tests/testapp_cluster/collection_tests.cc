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

#include <boost/filesystem.hpp>
#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <cluster_framework/node.h>
#include <include/mcbp/protocol/unsigned_leb128.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class CollectionsTests : public cb::test::ClusterTest {
protected:
    void testSubdocRbac(MemcachedConnection& conn, const std::string& key);
    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
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
           DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                         "TestBasicOperations"),
           MutationType::Add);
    mutate(*conn,
           DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                         "TestBasicOperations"),
           MutationType::Set);
    mutate(*conn,
           DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                         "TestBasicOperations"),
           MutationType::Replace);
}

TEST_F(CollectionsTests, TestInvalidCollection) {
    auto conn = getConnection();
    try {
        // collections 1 to 7 are reserved and invalid from a client
        mutate(*conn,
               DocKey::makeWireEncodedString(1, "TestInvalidCollection"),
               MutationType::Add);
        FAIL() << "Einval not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Einval, e.getReason());
    }
    try {
        // collection 1000 is unknown
        mutate(*conn,
               DocKey::makeWireEncodedString(1000, "TestInvalidCollection"),
               MutationType::Add);
        FAIL() << "UnknownCollection not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection, e.getReason());
    }
}

static BinprotSubdocCommand subdocInsertXattrPath(const std::string& key,
                                                  const std::string& path) {
    using namespace cb::mcbp;
    return BinprotSubdocCommand(ClientOpcode::SubdocDictAdd,
                                key,
                                path,
                                "{}",
                                SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                                mcbp::subdoc::doc_flag::Mkdoc);
}

void CollectionsTests::testSubdocRbac(MemcachedConnection& conn,
                                      const std::string& key) {
    auto fruit = DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                               "TestBasicRbac");
    auto vegetable = DocKey::makeWireEncodedString(CollectionEntry::vegetable,
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
    // connection only has xattr read, 1 entry comes back
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey(fruit);
    cmd.addGet("$XTOC", SUBDOC_FLAG_XATTR_PATH);
    conn.sendCommand(cmd);
    BinprotSubdocMultiLookupResponse resp;
    resp.clear();
    conn.recvResponse(resp);
    EXPECT_TRUE(resp.isSuccess()) << to_string(resp.getStatus());
    ASSERT_EQ(1, resp.getResults().size());
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getResults().front().status);
    EXPECT_EQ(R"(["nosys"])", resp.getResults().front().value);
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
           DocKey::makeWireEncodedString(CollectionEntry::defaultC,
                                         "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                         "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           DocKey::makeWireEncodedString(CollectionEntry::vegetable,
                                         "TestBasicRbac"),
           MutationType::Add);

    // I'm allowed to read from the default collection and read and write
    // to the fruit collection
    conn->authenticate(username, password);
    conn->selectBucket("default");

    testSubdocRbac(*conn, "TestBasicRbac");

    conn->get(DocKey::makeWireEncodedString(CollectionEntry::defaultC,
                                            "TestBasicRbac"),
              Vbid{0});
    conn->get(DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                            "TestBasicRbac"),
              Vbid{0});

    // I cannot get from the vegetable collection
    try {
        conn->get(DocKey::makeWireEncodedString(CollectionEntry::vegetable,
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
           DocKey::makeWireEncodedString(CollectionEntry::fruit,
                                         "TestBasicRbac"),
           MutationType::Set);
    for (auto c : std::vector<CollectionID>{
                 {CollectionEntry::defaultC, CollectionEntry::vegetable}}) {
        try {
            mutate(*conn,
                   DocKey::makeWireEncodedString(c, "TestBasicRbac"),
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
              std::count_if(json["scopes"][0]["collections"].begin(),
                            json["scopes"][0]["collections"].end(),
                            [](nlohmann::json entry) {
                                return entry["name"] == "_default" &&
                                       entry["uid"] == "0";
                            }));
    EXPECT_EQ(1,
              std::count_if(json["scopes"][0]["collections"].begin(),
                            json["scopes"][0]["collections"].end(),
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
           DocKey::makeWireEncodedString(CollectionEntry::vegetable,
                                         "Generation1"),
           MutationType::Add);

    auto bucket = cluster->getBucket("default");
    ASSERT_NE(nullptr, bucket);
    cluster->collections.remove(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    cluster->collections.add(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    mutate(*conn,
           DocKey::makeWireEncodedString(CollectionEntry::vegetable,
                                         "Generation2"),
           MutationType::Add);

    // Generation 1 is lost, the first drop was accepted and the keys created
    // before the resurrection of vegetable are dropped.
    try {
        conn->get(DocKey::makeWireEncodedString(CollectionEntry::vegetable,
                                                "Generation1"),
                  Vbid{0});
        FAIL() << "Should not be able to fetch in Generation1 key";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isNotFound()) << to_string(error.getReason());
    }
    conn->get(DocKey::makeWireEncodedString(CollectionEntry::vegetable,
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
        const int retries = 50;
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