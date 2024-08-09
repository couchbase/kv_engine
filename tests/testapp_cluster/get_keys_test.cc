/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <mcbp/codec/frameinfo.h>
#include <memcached/storeddockey.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>

/**
 * GetKeysTests tests the GetKeys command
 *
 * For a collection-aware clients the collection id of the start key is used
 * for searching. For collection-unaware clients the default collection is
 * searched.
 *
 * GetKeys returns up to 1k keys unless the client provides a maximum limit
 * of documents to return.
 */
class GetKeysTests : public cb::test::ClusterTest {
public:
    std::string test_name;
    void SetUp() override {
        test_name =
                ::testing::UnitTest::GetInstance()->current_test_info()->name();
    }

    static void SetUpTestCase() {
        ClusterTest::SetUpTestCase();

        cluster->getAuthProviderService().upsertUser({"almighty", "bruce", R"(
{
    "buckets": {
      "*": [
        "all"
      ]
    },
    "privileges": [
      "Administrator",
      "Audit",
      "IdleConnection",
      "Impersonate",
      "SystemSettings",
      "Stats",
      "BucketThrottleManagement",
      "NodeSupervisor",
      "Unthrottled",
      "Unmetered"
    ],
    "domain": "external"
  })"_json});

        auto conn = getConnection(true);
        StoredDocKey id{"X-testGetKeys", CollectionEntry::fruit};

        uint64_t seqnoToPersist = 0;
        for (int ii = 0; ii < 1001; ++ii) {
            upsert(*conn,
                   StoredDocKey{fmt::format("Default-{}", ii),
                                CollectionEntry::defaultC},
                   "value",
                   false);
            seqnoToPersist = upsert(*conn,
                                    StoredDocKey{fmt::format("Fruit-{}", ii),
                                                 CollectionEntry::fruit},
                                    "value",
                                    false)
                                     .seqno;
        }
        // Make sure all documents are persisted
        conn->waitForSeqnoToPersist(Vbid(0), seqnoToPersist);
    }

    /**
     * Upsert a document in the provided connections current bucket and
     * optionally wait for it to be persisted to disk.
     *
     * @param conn the connection to use
     * @param id The document to store
     * @param value The documents value
     * @param wait_for_persistence Set to true if call should block until
     *                             the document is persisted to disk
     * @return The mutation info for the document stored
     */
    static MutationInfo upsert(MemcachedConnection& conn,
                               const DocKeyView& id,
                               std::string value,
                               bool wait_for_persistence) {
        Document doc;
        doc.info.id = std::string{id};
        doc.value = std::move(value);
        MutationInfo info;
        if (wait_for_persistence) {
            info = conn.mutate(doc, Vbid{0}, MutationType::Set, []() {
                using namespace cb::mcbp::request;
                FrameInfoVector ret;
                ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
                        cb::durability::Level::MajorityAndPersistOnMaster));
                return ret;
            });
        } else {
            info = conn.mutate(doc, Vbid{0}, MutationType::Set);
        }
        return info;
    }

    /// Get an authenticated client to with the "default" as the selected
    /// bucket
    static std::unique_ptr<MemcachedConnection> getConnection(
            bool collections) {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("almighty");
        conn->selectBucket(bucket->getName());
        conn->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                           cb::mcbp::Feature::XATTR,
                           cb::mcbp::Feature::XERROR,
                           cb::mcbp::Feature::SELECT_BUCKET,
                           cb::mcbp::Feature::JSON,
                           cb::mcbp::Feature::SNAPPY,
                           collections ? cb::mcbp::Feature::Collections
                                       : cb::mcbp::Feature::SELECT_BUCKET});
        return conn;
    }

    /// Validate that the keys in the provided array belongs to the correct
    /// collection (which should be Fruit for collection aware clients)
    static void validate_keys(bool collection_aware,
                              const std::vector<std::string>& keys) {
        if (collection_aware) {
            // all the keys should be in the fruits collection
            for (const auto& k : keys) {
                const auto [collection, key] =
                        DocKeyView(k.c_str(), DocKeyEncodesCollectionId::Yes)
                                .getIdAndKey();
                const auto kv = std::string_view{
                        reinterpret_cast<const char*>(key.data()), key.size()};
                EXPECT_EQ(CollectionEntry::fruit.getId(), collection);
                EXPECT_EQ(0, kv.find("Fruit"));
            }
        } else {
            // all the keys should be in the defalt collection
            for (const auto& k : keys) {
                EXPECT_EQ(0, k.find("Default"));
            }
        }
    }

    /**
     * Test GetKeys
     *
     * GetKeys returns by default a maximum of 1k documents, but the request
     * may include a maximum number of documents to return. Verify these two
     * modes.
     *
     * Verify that GetKeys honors the EUID specified in the request
     *
     * @param euid An optional EUID to use for the request
     * @param access If the provided EUID should have access to the documents
     *               or not.
     * @param priv An optional extra privilege to grant the optional euid user
     */
    static void testGetKeys(bool collection_aware,
                            std::optional<std::string> euid,
                            bool access,
                            std::optional<cb::rbac::Privilege> priv) {
        auto conn = getConnection(collection_aware);
        std::string id = "A";
        if (collection_aware) {
            id = std::string{StoredDocKey{id, CollectionEntry::fruit}};
        }
        // Run GetAllKeys
        BinprotGetKeysCommand getAllKeysCommand(id);
        if (euid) {
            getAllKeysCommand.addFrameInfo(
                    cb::mcbp::request::ImpersonateUserFrameInfo(
                            fmt::format("^{}", *euid)));

            if (priv) {
                getAllKeysCommand.addFrameInfo(
                        cb::mcbp::request::
                                ImpersonateUserExtraPrivilegeFrameInfo(*priv));
            }
        }

        // Test the default return limit of 1k
        auto rsp = BinprotGetKeysResponse{conn->execute(getAllKeysCommand)};

        // Verify that the appropriate error gets returned if we don't have
        // access
        if (!access) {
            if (!collection_aware) {
                // We might get Unknown collection in our response
                if (rsp.getStatus() == cb::mcbp::Status::UnknownCollection) {
                    return;
                }
            }

            EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
            return;
        }

        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();

        auto keys = rsp.getKeys();
        EXPECT_EQ(1000, keys.size());
        validate_keys(collection_aware, keys);

        // Set the limit of keys to return to 1 and verify that it gets honored
        getAllKeysCommand.setKeyLimit(1);
        rsp = BinprotGetKeysResponse{conn->execute(getAllKeysCommand)};
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        keys = rsp.getKeys();
        EXPECT_EQ(1, keys.size());
        validate_keys(collection_aware, keys);

        // Set the limit of keys to return to more than the amount of available
        // keys
        getAllKeysCommand.setKeyLimit(2000);
        rsp = BinprotGetKeysResponse{conn->execute(getAllKeysCommand)};
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        keys = rsp.getKeys();
        EXPECT_EQ(1001, keys.size());
        validate_keys(collection_aware, keys);
    }
};

TEST_F(GetKeysTests, GetAllKeysAdministratorCollectionAware) {
    testGetKeys(true, {}, true, {});
}

TEST_F(GetKeysTests, GetAllKeysAdministratorCollectionUnaware) {
    testGetKeys(false, {}, true, {});
}

TEST_F(GetKeysTests, GetAllKeysAdministratorWithEuidWithBucketAccess) {
    cluster->getAuthProviderService().upsertUser({test_name, "", R"(
{
  "buckets": {
    "default": {
      "privileges": ["Read"]
    }
  },
  "domain": "external"
}
)"_json});
    testGetKeys(true, test_name, true, {});
    testGetKeys(false, test_name, true, {});
}

TEST_F(GetKeysTests, GetAllKeysAdministratorWithEuidWithScopeAccess) {
    cluster->getAuthProviderService().upsertUser({test_name, "", R"(
{
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
  "domain": "external"
}
)"_json});
    testGetKeys(true, test_name, true, {});
    testGetKeys(false, test_name, true, {});
}

TEST_F(GetKeysTests, GetAllKeysAdministratorWithEuidWithCollectionAccess) {
    cluster->getAuthProviderService().upsertUser({test_name, "", R"(
{
  "buckets": {
    "default": {
      "privileges": [],
      "scopes": {
        "0": {
          "collections": {
            "9": {
              "privileges": [
                "Read"
              ]
            }
          }
        }
      }
    }
  },
  "domain": "external"
}
)"_json});
    testGetKeys(true, test_name, true, {});

    // A non-collection aware client should not be able to access the system
    // as the user don't have access to the default collection
    testGetKeys(false, test_name, false, {});

    // but it works if we grant access to the default collections
    cluster->getAuthProviderService().upsertUser({test_name, "", R"(
{
  "buckets": {
    "default": {
      "privileges": [],
      "scopes": {
        "0": {
          "collections": {
            "0": {
              "privileges": [
                "Read"
              ]},
            "9": {
              "privileges": [
                "Read"
              ]
            }
          }
        }
      }
    }
  },
  "domain": "external"
}
)"_json});
    testGetKeys(false, test_name, true, {});
}

TEST_F(GetKeysTests, GetAllKeysAdministratorWithEuidWithoutAccess) {
    testGetKeys(true, "jones", false, {});
}

/// Test that we may grant our imposed user extra privileges to let
/// them execute the command
TEST_F(GetKeysTests, GetAllKeysAdministratorWithEuidWithoutAccessGrantedPriv) {
    cluster->getAuthProviderService().upsertUser({test_name, "", R"(
{
  "buckets": {
    "default": {
      "privileges": ["Insert"]
    }
  },
  "domain": "external"
}
)"_json});
    testGetKeys(true, test_name, true, cb::rbac::Privilege::Read);
}
