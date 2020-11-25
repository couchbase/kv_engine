/*
 *     Copyright 2020 Couchbase, Inc
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
