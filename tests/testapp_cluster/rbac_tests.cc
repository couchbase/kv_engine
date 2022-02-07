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
    std::vector<std::vector<std::string>> privileges = {
            {"ReadSeqno"},
            {"MetaRead"},
            {"MetaRead", "ReadSeqno"},
    };

    int i = 1;
    for (const auto& testPrivs : privileges) {
        auto config = userConfig;
        for (const auto& priv : testPrivs) {
            config["buckets"]["default"]["privileges"].push_back(priv);
        }
        provider.upsertUser({"userCan" + std::to_string(i), "pass", config});
        i++;
    }

    // userObserveSeqnoCan1 have Read permission on the entire bucket
    auto config = userConfig;
    config["buckets"]["default"]["privileges"].push_back("Read");
    provider.upsertUser({"userObserveSeqnoCan1", "pass", config});

    // userObserveSeqnoCan2 have scope wide read on Scope 0
    config = userConfig;
    config["buckets"]["default"] = nlohmann::json::parse(R"({
  "scopes": {
    "0": {
      "collections": {"0": { "privileges": ["SimpleStats"] }},
      "privileges": ["Read"]
    }
   }
})");
    provider.upsertUser({"userObserveSeqnoCan2", "pass", config});

    // userObserveSeqnoCan3 have read access to collection 0
    config = userConfig;
    config["buckets"]["default"] = nlohmann::json::parse(R"({
  "scopes": {
    "0": {
      "collections": {"0": { "privileges": ["Read"] }}
    }
   }
})");
    provider.upsertUser({"userObserveSeqnoCan3", "pass", config});
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
         std::vector<std::string>{{"userObserveSeqnoCan1"},
                                  {"userObserveSeqnoCan2"},
                                  {"userObserveSeqnoCan3"}}) {
        conn->authenticate(user, "pass");
        conn->selectBucket(bucket);
        conn->observeSeqno(Vbid(0), highSeqno.vbucketuuid);
    }
}

TEST_F(RbacSeqnosTests, GetAllVbSeqnosBucket) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("userCannot", "pass");
    conn->selectBucket(bucket);
    conn->setFeature(cb::mcbp::Feature::Collections, true);

    auto rsp = conn->getAllVBucketSequenceNumbers();
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());

    for (int user : {1, 2, 3}) {
        conn->authenticate("userCan" + std::to_string(user), "pass");
        conn->selectBucket(bucket);

        auto rsp = conn->getAllVBucketSequenceNumbers();
        EXPECT_TRUE(rsp.isSuccess())
                << "Failed for userCan" << user << " " << rsp.getStatus();
        EXPECT_EQ(highSeqno.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);
    }
}

// When making a collection enabled request and asking about a collection,
// MetaRead must be present at the appropriate level.
// If ReadSeqno only no collection request can be made
TEST_F(RbacSeqnosTests, GetAllVbSeqnosCollections1) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("userCan1", "pass"); // Has ReadSeqno only so fail
    conn->selectBucket(bucket);
    conn->setFeature(cb::mcbp::Feature::Collections, true);

    // User has no collection privileges for ID:9 so is told it does not exist
    auto rsp = conn->getAllVBucketSequenceNumbers(0, CollectionID(9));
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());
    // same for id:0
    rsp = conn->getAllVBucketSequenceNumbers(0, CollectionID(0));
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());

    // userCan2 and userCan3 both have MetaRead, so can make a request about
    // a collection
    for (int user : {2, 3}) {
        conn->authenticate("userCan" + std::to_string(user), "pass");
        conn->selectBucket(bucket);

        // Ask for the default collection
        auto rsp = conn->getAllVBucketSequenceNumbers(0, CollectionID::Default);
        EXPECT_TRUE(rsp.isSuccess());
        EXPECT_EQ(defaultCollectionHighSeqno.seqno,
                  rsp.getVbucketSeqnos()[Vbid(0)]);

        // 9 is fruit, which is high-seqno at the moment
        rsp = conn->getAllVBucketSequenceNumbers(0, CollectionID(9));
        EXPECT_TRUE(rsp.isSuccess());
        EXPECT_EQ(highSeqno.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);
    }
}

TEST_F(RbacSeqnosTests, GetAllVbSeqnosCollections2) {
    // Extension of the above, but a more collection oriented setup. The user
    // has MetaRead against their own collection, so should be able to ask about
    // that. They have ReadSeqno at bucket only, so will be blocked asking about
    // other collections
    auto userConfig = R"(
    {"buckets":{
      "default":{
         "privileges":["ReadSeqno"],
         "scopes":{
            "0":{
               "collections":{
                  "9":{"privileges":["MetaRead"]
                  }}}}}},
    "privileges":[],
    "domain":"external"})"_json;

    cluster->getAuthProviderService().upsertUser(
            {"fruit", "fruit", userConfig});

    auto conn = cluster->getConnection(0);
    conn->authenticate("fruit", "fruit");
    conn->selectBucket(bucket);
    conn->setFeature(cb::mcbp::Feature::Collections, true);

    // No access to collection other than cid:9
    auto rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/,
                                                  CollectionID::Default);
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());

    // cid 9 is ok because of MetaRead against that collection
    rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/, CollectionID(9));
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_EQ(highSeqno.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);
}

TEST_F(RbacSeqnosTests, GetAllVbSeqnosDefaultOnly) {
    // This test the users are legacy clients and the RBAC config enforces that

    // First user has privileges only for default collection
    auto legacy1Config = R"(
    {"buckets":{
      "default":{
         "privileges":[],
         "scopes":{
            "0":{
               "collections":{
                  "0":{
                     "privileges":["MetaRead"]
                  }}}}}},
    "privileges":[],
    "domain":"external"})"_json;

    // second user has addition of ReadSeqno (which is a bucket priv)
    auto legacy2Config = R"(
    {"buckets":{
      "default":{
         "privileges":["ReadSeqno"],
         "scopes":{
            "0":{
               "collections":{
                  "0":{
                     "privileges":["Upsert"]
                  }}}}}},
    "privileges":[],
    "domain":"external"})"_json;

    cluster->getAuthProviderService().upsertUser(
            {"legacy1", "legacy", legacy1Config});
    cluster->getAuthProviderService().upsertUser(
            {"legacy2", "legacy", legacy2Config});

    // Now test that both can only query default collection via bucket request
    auto conn = cluster->getConnection(0);
    for (const auto user : {"legacy1", "legacy2"}) {
        conn->authenticate(user, "legacy");
        conn->selectBucket(bucket);
        // No HELLO collections!

        // Command is allowed, internally directed to default collection
        auto rsp = conn->getAllVBucketSequenceNumbers();
        EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        EXPECT_EQ(defaultCollectionHighSeqno.seqno,
                  rsp.getVbucketSeqnos()[Vbid(0)]);

        // The collection encoding is not available, even for their collection
        rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/, CollectionID(0));
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
        rsp = conn->getAllVBucketSequenceNumbers(1 /*active*/, CollectionID(9));
        EXPECT_EQ(cb::mcbp::Status::Einval, rsp.getStatus());
    }

    // Finally run through the "userCan" users - they are not "pinned" to the
    // default collection, but if they don't enable collections they only see
    // default high-seqno
    for (int user : {1, 2, 3}) {
        conn->authenticate("userCan" + std::to_string(user), "pass");
        conn->selectBucket(bucket);

        auto rsp = conn->getAllVBucketSequenceNumbers();
        EXPECT_TRUE(rsp.isSuccess())
                << "Failed for userCan" << user << " " << rsp.getStatus();
        EXPECT_EQ(defaultCollectionHighSeqno.seqno,
                  rsp.getVbucketSeqnos()[Vbid(0)]);
    }
}
