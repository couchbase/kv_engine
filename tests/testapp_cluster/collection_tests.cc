/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "clustertest.h"

#include "auth_provider_service.h"
#include "bucket.h"
#include "cluster.h"
#include <include/mcbp/protocol/unsigned_leb128.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class CollectionsTests : public cb::test::ClusterTest {
protected:
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
        doc.value = "body";
        doc.info.id = std::move(id);
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        const auto info = conn.mutate(doc, Vbid{0}, type);
        EXPECT_NE(0, info.cas);
    }
};

// Verify that I can store documents within the collections
TEST_F(CollectionsTests, TestBasicOperations) {
    auto conn = getConnection();
    mutate(*conn,
           createKey(CollectionEntry::fruit, "TestBasicOperations"),
           MutationType::Add);
    mutate(*conn,
           createKey(CollectionEntry::fruit, "TestBasicOperations"),
           MutationType::Set);
    mutate(*conn,
           createKey(CollectionEntry::fruit, "TestBasicOperations"),
           MutationType::Replace);
}

TEST_F(CollectionsTests, TestInvalidCollection) {
    auto conn = getConnection();
    try {
        // collections 1 to 7 are reserved and invalid from a client
        mutate(*conn, createKey(1, "TestInvalidCollection"), MutationType::Add);
        FAIL() << "Einval not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Einval, e.getReason());
    }
    try {
        // collection 1000 is unknown
        mutate(*conn,
               createKey(1000, "TestInvalidCollection"),
               MutationType::Add);
        FAIL() << "UnknownCollection not detected";
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::UnknownCollection, e.getReason());
    }
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
                "Upsert"
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
           createKey(CollectionEntry::defaultC, "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           createKey(CollectionEntry::fruit, "TestBasicRbac"),
           MutationType::Add);
    mutate(*conn,
           createKey(CollectionEntry::vegetable, "TestBasicRbac"),
           MutationType::Add);

    // I'm allowed to read from the default collection and read and write
    // to the fruit collection
    conn->authenticate(username, password);
    conn->selectBucket("default");

    conn->get(createKey(CollectionEntry::defaultC, "TestBasicRbac"), Vbid{0});
    conn->get(createKey(CollectionEntry::fruit, "TestBasicRbac"), Vbid{0});

    // I cannot get from the vegetable collection
    try {
        conn->get(createKey(CollectionEntry::vegetable, "TestBasicRbac"),
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
           createKey(CollectionEntry::fruit, "TestBasicRbac"),
           MutationType::Set);
    for (auto c : std::vector<CollectionID>{
                 {CollectionEntry::defaultC, CollectionEntry::vegetable}}) {
        try {
            mutate(*conn, createKey(c, "TestBasicRbac"), MutationType::Set);
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
    EXPECT_EQ(CollectionEntry::defaultC.getId(), defaultScope.getScopeId());
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
           createKey(CollectionEntry::vegetable, "Generation1"),
           MutationType::Add);

    auto bucket = cluster->getBucket("default");
    ASSERT_NE(nullptr, bucket);
    cluster->collections.remove(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    cluster->collections.add(CollectionEntry::vegetable);
    bucket->setCollectionManifest(cluster->collections.getJson());
    mutate(*conn,
           createKey(CollectionEntry::vegetable, "Generation2"),
           MutationType::Add);

    // Generation 1 is lost, the first drop was accepted and the keys created
    // before the resurrection of vegetable are dropped.
    try {
        conn->get(createKey(CollectionEntry::vegetable, "Generation1"),
                  Vbid{0});
        FAIL() << "Should not be able to fetch in Generation1 key";
    } catch (const ConnectionError& error) {
        ASSERT_TRUE(error.isNotFound()) << to_string(error.getReason());
    }
    conn->get(createKey(CollectionEntry::vegetable, "Generation2"), Vbid{0});
}