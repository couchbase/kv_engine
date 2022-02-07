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
#include <string>

class RbacTest : public cb::test::ClusterTest {
protected:
    const std::string dcpuser{"dcpuser"};
    const std::string dcppass{"dcppass"};
    const std::string bucket{"default"};
};

TEST_F(RbacTest, DcpOpenWithoutAccess) {
    cluster->getAuthProviderService().upsertUser({dcpuser, dcppass, R"({
    "buckets": {
      "default": {
        "privileges": [
          "Read",
          "XattrRead",
          "SimpleStats"
        ]
      }
    },
    "privileges": [],
    "domain": "external"
  })"_json});
    auto conn = cluster->getConnection(0);
    conn->authenticate(dcpuser, dcppass);
    conn->selectBucket(bucket);

    auto rsp = conn->execute(
            BinprotDcpOpenCommand{"DcpOpenWithoutAccess",

                                  cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
    rsp = conn->execute(BinprotDcpOpenCommand{"DcpOpenWithoutAccess"});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
}

TEST_F(RbacTest, DcpOpenWithProducerAccess) {
    cluster->getAuthProviderService().upsertUser({dcpuser, dcppass, R"({
    "buckets": {
      "default": {
        "privileges": [
          "DcpProducer",
          "Read",
          "XattrRead",
          "SimpleStats"
        ]
      }
    },
    "privileges": [],
    "domain": "external"
  })"_json});
    auto conn = cluster->getConnection(0);
    conn->authenticate(dcpuser, dcppass);
    conn->selectBucket(bucket);
    auto rsp =
            conn->execute(BinprotDcpOpenCommand{"DcpOpenWithProducerAccess"});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());

    rsp = conn->execute(
            BinprotDcpOpenCommand{"DcpOpenWithProducerAccess",

                                  cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess());
}

TEST_F(RbacTest, DcpOpenWithConsumerAccess) {
    cluster->getAuthProviderService().upsertUser({dcpuser, dcppass, R"({
    "buckets": {
      "default": {
        "privileges": [
          "DcpConsumer",
          "Read",
          "XattrRead",
          "SimpleStats"
        ]
      }
    },
    "privileges": [],
    "domain": "external"
  })"_json});
    auto conn = cluster->getConnection(0);
    conn->authenticate(dcpuser, dcppass);
    conn->selectBucket(bucket);
    auto rsp = conn->execute(
            BinprotDcpOpenCommand{"DcpOpenWithConsumerAccess",

                                  cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());

    rsp = conn->execute(BinprotDcpOpenCommand{"DcpOpenWithConsumerAccess"});
    ASSERT_TRUE(rsp.isSuccess());
}

class RbacSeqnosTests : public RbacTest {
public:
    void SetUp() override;
    void configureUsers(const nlohmann::json& userConfig);

protected:
    MutationInfo defaultCollectionHighSeqno;
    MutationInfo highSeqno;
};

void RbacSeqnosTests::SetUp() {
    RbacTest::SetUp();

    // Make high-seqno !default
    auto conn = cluster->getConnection(0);
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket("default");
    conn->setFeature(cb::mcbp::Feature::Collections, true);
    defaultCollectionHighSeqno =
            conn->store(DocKey::makeWireEncodedString(CollectionID::Default,
                                                      "RbacSeqnosTests"),
                        Vbid(0),
                        "Default");
    highSeqno = conn->store(DocKey::makeWireEncodedString(
                                    CollectionEntry::fruit, "RbacSeqnosTests"),
                            Vbid(0),
                            "APPLE");
    ASSERT_GT(highSeqno.seqno, defaultCollectionHighSeqno.seqno);

    // setup the testusers with different MetaRead/ReadSeqno privs
    configureUsers(R"({
    "buckets": {
      "default": {
        "privileges": [
        ]
      }
    },
    "privileges": [],
    "domain": "external"
    })"_json);
}

void RbacSeqnosTests::configureUsers(const nlohmann::json& userConfig) {
    auto& provider = cluster->getAuthProviderService();
    provider.upsertUser({"userCannot", "pass", userConfig});

    // userCan1 have Read permission on the entire bucket
    auto config = userConfig;
    config["buckets"]["default"]["privileges"].push_back("Read");
    provider.upsertUser({"userCan1", "pass", config});

    // userCan2 have scope wide read on Scope 0
    config = userConfig;
    config["buckets"]["default"] = nlohmann::json::parse(R"({
  "scopes": {
    "0": {
      "collections": {"0": { "privileges": ["SimpleStats"] }},
      "privileges": ["Read"]
    }
   }
})");
    provider.upsertUser({"userCan2", "pass", config});

    // userCan3 have read access to collection 0
    config = userConfig;
    config["buckets"]["default"] = nlohmann::json::parse(R"({
  "scopes": {
    "0": {
      "collections": {"0": { "privileges": ["Read"] }}
    }
   }
})");
    provider.upsertUser({"userCan3", "pass", config});
}

TEST_F(RbacSeqnosTests, ObserveSeqno) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("userCannot", "pass");
    conn->selectBucket(bucket);

    try {
        conn->observeSeqno(Vbid(0), highSeqno.vbucketuuid);
        FAIL() << "This user should not be able to run observeSeqno";
    } catch (const ConnectionError& ex) {
        EXPECT_TRUE(ex.isAccessDenied()) << ex.what();
    } catch (const std::exception& ex) {
        FAIL() << "Unknown exception: " << ex.what();
    }

    for (const auto& user :
         std::vector<std::string>{{"userCan1"}, {"userCan2"}, {"userCan3"}}) {
        conn->authenticate(user, "pass");
        conn->selectBucket(bucket);
        conn->observeSeqno(Vbid(0), highSeqno.vbucketuuid);
    }
}

/// Verify that a collection-aware client can request the all vb seqno's
/// as long as it has at least 1 read privilege within the bucket
TEST_F(RbacSeqnosTests, GetAllVbSeqnosBucket) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("userCannot", "pass");
    conn->selectBucket(bucket);
    conn->setFeature(cb::mcbp::Feature::Collections, true);

    auto rsp = conn->getAllVBucketSequenceNumbers();
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus())
            << rsp.getDataString();

    for (const auto& user :
         std::vector<std::string>{{"userCan1"}, {"userCan2"}, {"userCan3"}}) {
        conn->authenticate(user, "pass");
        conn->selectBucket(bucket);

        rsp = conn->getAllVBucketSequenceNumbers();
        ASSERT_TRUE(rsp.isSuccess())
                << "Failed for userCan" << user << " " << rsp.getStatus();
        EXPECT_EQ(highSeqno.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);
    }
}

/// When making a collection enabled request and asking about a collection,
/// Read must be present at the appropriate level.
TEST_F(RbacSeqnosTests, GetAllVbSeqnosCollections) {
    auto conn = cluster->getConnection(0);

    // userCan1 have full bucket privileges and can read everything:
    conn->authenticate("userCan1", "pass");
    conn->selectBucket(bucket);
    conn->setFeature(cb::mcbp::Feature::Collections, true);

    auto rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::fruit);
    ASSERT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    EXPECT_EQ(highSeqno.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::defaultC);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());

    // But error for unknown collections:
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionID(1));
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());

    // userCan2 have scope Read access to scope 0
    conn->authenticate("userCan2", "pass");
    conn->selectBucket(bucket);
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::fruit);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    // customer1 belongs to scope 1
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::customer1);
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());

    // userCan3 have only read access to collection 0
    conn->authenticate("userCan3", "pass");
    conn->selectBucket(bucket);
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::defaultC);
    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionUid::fruit);
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());
}

/// Test that connections which don't hello collections are allowed
/// to connect to the default collection
TEST_F(RbacSeqnosTests, GetAllVbSeqnosDefaultOnly) {
    auto conn = cluster->getConnection(0);
    for (const auto& user :
         std::vector<std::string>{{"userCan1"}, {"userCan2"}, {"userCan3"}}) {
        conn->authenticate(user, "pass");
        conn->selectBucket(bucket);

        auto rsp = conn->getAllVBucketSequenceNumbers();
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        EXPECT_EQ(defaultCollectionHighSeqno.seqno,
                  rsp.getVbucketSeqnos()[Vbid(0)]);

        // The collection encoding is not available, even for their collection
        rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/, CollectionID(0));
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
        rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/, CollectionID(9));
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    }
}
