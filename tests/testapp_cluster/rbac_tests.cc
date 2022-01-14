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
    cluster->getAuthProviderService().upsertUser(
            {"userCannot", "pass", userConfig});
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
        cluster->getAuthProviderService().upsertUser(
                {"userCan" + std::to_string(i), "pass", config});
        i++;
    }
}

TEST_F(RbacSeqnosTests, ObserveSeqno) {
    auto conn = cluster->getConnection(0);
    conn->authenticate("userCannot", "pass");
    conn->selectBucket(bucket);

    try {
        conn->observeSeqno(Vbid(0), highSeqno.vbucketuuid);
        FAIL() << "This user should not be able to run observeSeqno";
    } catch (...) {
    }

    for (int user : {1, 2, 3}) {
        conn->authenticate("userCan" + std::to_string(user), "pass");
        conn->selectBucket(bucket);

        try {
            conn->observeSeqno(Vbid(0), highSeqno.vbucketuuid);
        } catch (...) {
            FAIL() << "Should be able to observe for userCan" << user;
        }
    }
}
