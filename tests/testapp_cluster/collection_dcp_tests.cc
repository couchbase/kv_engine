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

#include <cluster_framework/auth_provider_service.h>
#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <thread>

class CollectionsDcpTests : public cb::test::ClusterTest {
protected:
    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->setFeature(cb::mcbp::Feature::Collections, true);
        return conn;
    }
};

// Setup will add a user that can do all-bucket streams
class CollectionsRbacBucket : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the entire bucket
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer", "DcpStream"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
        // Load a user that has DcpProducer privs for the entire bucket
        // (cannot stream)
        cluster->getAuthProviderService().upsertUser(
                {usernameNoStream, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacBucket");
        conn->dcpControl("enable_noop", "true");

        connNoStream = getConnection();
        connNoStream->authenticate(usernameNoStream, password);
        connNoStream->selectBucket("default");
        connNoStream->dcpOpenProducer("CollectionsRbacBucketNoStream");
        connNoStream->dcpControl("enable_noop", "true");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        cluster->getAuthProviderService().removeUser(usernameNoStream);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    std::unique_ptr<MemcachedConnection> connNoStream;

    const std::string username{"CollectionsRbacBucket"};
    const std::string usernameNoStream{"CollectionsRbacBucketNoStream"};
    const std::string password{"CollectionsRbacBucket"};
};

TEST_F(CollectionsRbacBucket, BucketAccessBucketSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
}

TEST_F(CollectionsRbacBucket, BucketAccessScopeSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessCollection2Success) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
}

TEST_F(CollectionsRbacBucket, BucketAccessFail) {
    try {
        connNoStream->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
    try {
        connNoStream->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
        FAIL() << "Expected a throw";
    } catch (const ConnectionError& e) {
        // No access and 0 privs == unknown scope
        EXPECT_TRUE(e.isUnknownScope());
        EXPECT_EQ(cluster->collections.getUidString(),
                  e.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }
    try {
        connNoStream->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
        FAIL() << "Expected a throw";
    } catch (const ConnectionError& e) {
        // No access and 0 privs == unknown collection
        EXPECT_TRUE(e.isUnknownCollection());
        EXPECT_EQ(cluster->collections.getUidString(),
                  e.getErrorJsonContext()["manifest_uid"].get<std::string>());
    }
}

// Setup will add a user that can do scope:0 streams
class CollectionsRbacScope : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the scope:0 only
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": ["DcpProducer"],
      "scopes": {
        "0": {
          "privileges": [
            "DcpStream"
          ]
        }
      }
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});
        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacScope");
        conn->dcpControl("enable_noop", "true");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    const std::string username{"CollectionsRbacScope"};
    const std::string password{"CollectionsRbacScope"};
};

TEST_F(CollectionsRbacScope, ScopeAccessBucketEaccess) {
    try {
        conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacScope, ScopeAccessScopeSuccess) {
    conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
}

TEST_F(CollectionsRbacScope, ScopeAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
}

TEST_F(CollectionsRbacScope, ScopeAccessCollection2Success) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
}

// Setup will add a user that can do collection:9 dcp streams
class CollectionsRbacCollection : public CollectionsDcpTests {
public:
    void SetUp() override {
        CollectionsDcpTests::SetUp();

        // Load a user that has DcpProducer/Stream privs for the collection:0xa
        cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ],
      "scopes": {
        "0": {
          "collections": {
            "a": {
              "privileges": [
                "DcpStream", "Upsert"
              ]
            }
          }
        },
        "8": {
          "collections": {
            "b": {
              "privileges": [
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
        conn = getConnection();
        conn->authenticate(username, password);
        conn->selectBucket("default");
        conn->dcpOpenProducer("CollectionsRbacCollection");
        conn->dcpControl("enable_noop", "true");

        connNoStream = getConnection();
        connNoStream->authenticate(username, password);
        connNoStream->selectBucket("default");
    }

    void TearDown() override {
        cluster->getAuthProviderService().removeUser(username);
        CollectionsDcpTests::TearDown();
    }

    std::unique_ptr<MemcachedConnection> conn;
    std::unique_ptr<MemcachedConnection> connNoStream;
    const std::string username{"CollectionsRbacCollection"};
    const std::string password{"CollectionsRbacCollection"};
};

TEST_F(CollectionsRbacCollection, CollectionAccessBucketEaccess) {
    try {
        conn->dcpStreamRequest(Vbid(0), 0, 0, ~0, 0, 0, 0);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessScopeEaccess) {
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"scope":"0"})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Eaccess, e.getReason());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionSuccess) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["a"]})"_json);
}

// Test checks that we can create a DCP stream, yet it closes if we lose
// the DcpStream privilege. Note prior to the fix for MB-41095 this would
// reproduce the un-handled exception seen in the MB.
TEST_F(CollectionsRbacCollection, CollectionStreamPrivsLost) {
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["a"]})"_json);

    // Now take away DcpStream from collection:a
    cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ],
      "scopes": {
        "0": {
          "collections": {
            "a": {
              "privileges": [
                "Upsert"
              ]
            }
          }
        },
        "8": {
          "collections": {
            "b": {
              "privileges": [
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

    // Poke a mutation in, second snapshot will be created that triggers the end
    connNoStream->store(
            DocKey::makeWireEncodedString(CollectionEntry::vegetable, "k1"),
            Vbid(0),
            "v1");

    // run DCP until we get the stream-end, don't attempt to validate the
    // exact message in-between as we can't guarantee how the checkpoints
    // line up (mutation can land in first snapshot or second)
    Frame frame;
    auto stepDcp = [&frame, this]() {
        conn->recvFrame(frame);
        EXPECT_EQ(cb::mcbp::Magic::ClientRequest, frame.getMagic());
        return frame.getRequest();
    };

    bool streamEndReceived = false;

    // manually calculated steps as being each event/mutation in a single
    // snapshot (snapshot_marker + event/mutation)
    // snapshot + create vegetable
    // snapshot + mutation
    // stream-end
    const int steps = 5;
    for (int ii = 0; ii < steps; ii++) {
        auto* request = stepDcp();
        if (request->getClientOpcode() ==
            cb::mcbp::ClientOpcode::DcpStreamEnd) {
            EXPECT_EQ(sizeof(uint32_t), request->getExtdata().size());
            const auto* payload = reinterpret_cast<
                    const cb::mcbp::request::DcpStreamEndPayload*>(
                    request->getExtdata().data());
            EXPECT_EQ(cb::mcbp::DcpStreamEndStatus::LostPrivileges,
                      payload->getStatus());
            streamEndReceived = true;
            break;
        }
    }
    EXPECT_TRUE(streamEndReceived);
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionUnknown1) {
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["9"]})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isUnknownCollection());
    }
}

TEST_F(CollectionsRbacCollection, CollectionAccessCollectionUnknown2) {
    try {
        // Even though we can see collection 0, this test checks we get unknown
        // collection because of the 0 privs on collection 9
        conn->dcpStreamRequest(Vbid(0),
                               0,
                               0,
                               ~0,
                               0,
                               0,
                               0,
                               R"({"collections":["0", "9", "a"]})"_json);
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isUnknownCollection());
    }
}

TEST_F(CollectionsDcpTests, TestBasicRbacCollectionsSuccess) {
    const std::string username{"TestBasicRbacCollectionsSuccess"};
    const std::string password{"TestBasicRbacCollectionsSuccess"};
    cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer", "DcpStream"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

    auto conn = getConnection();
    conn->authenticate(username, password);
    conn->selectBucket("default");
    conn->dcpOpenProducer("TestBasicRbacCollectionsSuccess");
    conn->dcpControl("enable_noop", "true");
    conn->dcpStreamRequest(
            Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
    cluster->getAuthProviderService().removeUser(username);
}

TEST_F(CollectionsDcpTests, TestBasicRbacFail) {
    const std::string username{"TestBasicRbacFail"};
    const std::string password{"TestBasicRbacFail"};
    cluster->getAuthProviderService().upsertUser({username, password, R"({
  "buckets": {
    "default": {
      "privileges": [
        "DcpProducer"
      ]
    }
  },
  "privileges": [],
  "domain": "external"
})"_json});

    auto conn = getConnection();
    conn->authenticate(username, password);
    conn->selectBucket("default");
    conn->dcpOpenProducer("TestBasicRbacFail");
    conn->dcpControl("enable_noop", "true");
    try {
        conn->dcpStreamRequest(
                Vbid(0), 0, 0, ~0, 0, 0, 0, R"({"collections":["0"]})"_json);
        FAIL() << "Expected dcpStreamRequest to throw";
    } catch (const ConnectionError& error) {
        // No privs associated with the collection, so it's unknown
        EXPECT_TRUE(error.isUnknownCollection())
                << to_string(error.getReason());
    }
    cluster->getAuthProviderService().removeUser(username);
}
