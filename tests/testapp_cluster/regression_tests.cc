/*
 *     Copyright 2022-Present Couchbase, Inc.
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

class RegressionTest : public cb::test::ClusterTest {
protected:
};

/// MB-51135 - data_writer role failed to use subdoc multi-mutation
///            as it would require read privilege
TEST_F(RegressionTest, MB51135) {
    // Create a user with the "data_writer" privilege
    cluster->getAuthProviderService().upsertUser({"MB51135", "MB51135", R"(
{
  "buckets": {
    "default": {
      "privileges": [
        "Delete",
        "Insert",
        "Upsert"
      ]
    }
  },
  "privileges": [
    "SystemSettings"
  ],
  "domain": "external"
})"_json});

    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("MB51135", "MB51135");
    conn->selectBucket(bucket->getName());
    // Verify that I can create a document
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey("MB51135");
        cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::Add);
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictAdd,
                        SUBDOC_FLAG_MKDIR_P,
                        "MB51135",
                        R"({"entry":true})");
        conn->sendCommand(cmd);
        BinprotSubdocMultiMutationResponse resp;
        conn->recvResponse(resp);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    // Verify that I can update the document with a multi-mutation command
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey("MB51135");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictAdd,
                        SUBDOC_FLAG_MKDIR_P,
                        "MB51135.foo",
                        R"({"entry":true})");
        conn->sendCommand(cmd);
        BinprotSubdocMultiMutationResponse resp;
        conn->recvResponse(resp);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    // Verify that I can write an XAttr with a multi-mutation command
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey("MB51135");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictAdd,
                        SUBDOC_FLAG_XATTR_PATH,
                        "MB51135",
                        R"({"object":false})");
        conn->sendCommand(cmd);
        BinprotSubdocMultiMutationResponse resp;
        conn->recvResponse(resp);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    }

    // Verify that I can't write a system XAttr
    {
        BinprotSubdocMultiMutationCommand cmd;
        cmd.setKey("MB51135");
        cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictAdd,
                        SUBDOC_FLAG_XATTR_PATH,
                        "_MB51135",
                        "{}");
        conn->sendCommand(cmd);
        BinprotSubdocMultiMutationResponse resp;
        conn->recvResponse(resp);

        ASSERT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, resp.getStatus());
        EXPECT_EQ(cb::mcbp::Status::Eaccess, resp.getResults().front().status);
    }
}
