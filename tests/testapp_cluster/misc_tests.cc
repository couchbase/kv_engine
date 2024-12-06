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
#include <cluster_framework/dcp_replicator.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <string>

class BasicClusterTest : public cb::test::ClusterTest {};

TEST_F(BasicClusterTest, GetReplica) {
    auto bucket = cluster->getBucket("default");
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        auto info = conn->store("foo", Vbid(0), "value");
        EXPECT_NE(0, info.cas);
    }

    // make sure that it is replicated to all the nodes in the
    // cluster
    const auto nrep = bucket->getVbucketMap()[0].size() - 1;
    for (std::size_t rep = 0; rep < nrep; ++rep) {
        auto conn = bucket->getConnection(Vbid(0), vbucket_state_replica, rep);
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());

        BinprotResponse rsp;
        do {
            BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::GetReplica);
            cmd.setVBucket(Vbid(0));
            cmd.setKey("foo");

            rsp = conn->execute(cmd);
        } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
        EXPECT_TRUE(rsp.isSuccess());
    }
}

TEST_F(BasicClusterTest, MultiGet) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    std::vector<std::pair<const std::string, Vbid>> keys;
    for (int ii = 0; ii < 10; ++ii) {
        keys.emplace_back(std::make_pair("key_" + std::to_string(ii), Vbid(0)));
        if ((ii & 1) == 1) {
            conn->store(keys.back().first,
                        keys.back().second,
                        "value",
                        cb::mcbp::Datatype::Raw);
        }
    }

    // and I want a not my vbucket!
    keys.emplace_back(std::make_pair("NotMyVbucket", Vbid(1)));
    bool nmvb = false;
    int nfound = 0;
    conn->mget(
            keys,
            [&nfound](std::unique_ptr<Document>&) -> void { ++nfound; },
            [&nmvb](const std::string& key,
                    const cb::mcbp::Response& rsp) -> void {
                EXPECT_EQ("NotMyVbucket", key);
                EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, rsp.getStatus());
                nmvb = true;
            });

    EXPECT_TRUE(nmvb) << "Did not get the NotMyVbucket callback";
    EXPECT_EQ(5, nfound);
}

/// Store a key on one node, and wait until its replicated all over
TEST_F(BasicClusterTest, Observe) {
    auto bucket = cluster->getBucket("default");
    auto replica = bucket->getConnection(Vbid(0), vbucket_state_replica, 0);
    replica->authenticate("@admin", "password", "PLAIN");
    replica->selectBucket(bucket->getName());

    BinprotObserveCommand observe({{Vbid{0}, "BasicClusterTest_Observe"}});
    // check that it don't exist on the replica
    auto rsp = BinprotObserveResponse{replica->execute(observe)};
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus());
    auto keys = rsp.getResults();
    ASSERT_EQ(1, keys.size());
    EXPECT_EQ(ObserveKeyState::NotFound, keys.front().status);

    // store it on the primary
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->store("BasicClusterTest_Observe", Vbid{0}, "value");
    }

    // loop and wait for it to hit the replica
    bool found = false;
    do {
        rsp = BinprotObserveResponse{replica->execute(observe)};
        ASSERT_TRUE(rsp.isSuccess());
        keys = rsp.getResults();
        ASSERT_EQ(1, keys.size());
        if (keys.front().status != ObserveKeyState::NotFound) {
            found = true;
            ASSERT_NE(0, keys.front().cas);
        }
    } while (!found);
}

TEST_F(BasicClusterTest, ObserveMulti) {
    auto bucket = cluster->getBucket("default");
    auto replica = bucket->getConnection(Vbid(0), vbucket_state_replica, 0);
    replica->authenticate("@admin", "password", "PLAIN");
    replica->selectBucket(bucket->getName());

    BinprotObserveCommand observe(
            {{Vbid{0}, "ObserveMulti"}, {Vbid{0}, "Document2"}});
    // check that it don't exist on the replica
    auto rsp = BinprotObserveResponse{replica->execute(observe)};
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus());
    auto keys = rsp.getResults();
    ASSERT_EQ(2, keys.size());
    EXPECT_EQ(ObserveKeyState::NotFound, keys[0].status);
    EXPECT_EQ("ObserveMulti", keys[0].key);
    EXPECT_EQ(ObserveKeyState::NotFound, keys[1].status);
    EXPECT_EQ("Document2", keys[1].key);

    // store it on the primary
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        conn->store("ObserveMulti", Vbid{0}, "value");
    }

    // loop and wait for it to hit the replica
    bool found = false;
    do {
        rsp = BinprotObserveResponse{replica->execute(observe)};
        ASSERT_TRUE(rsp.isSuccess());
        keys = rsp.getResults();
        ASSERT_EQ(2, keys.size());
        if (keys.front().status != ObserveKeyState::NotFound) {
            found = true;
            ASSERT_NE(0, keys.front().cas);
        }
        EXPECT_EQ("ObserveMulti", keys[0].key);
        EXPECT_EQ("Document2", keys[1].key);
        EXPECT_EQ(ObserveKeyState::NotFound, keys[1].status);
    } while (!found);
}

/// Add a test just to verify that the external auth service works (will
/// be replaced with better tests at a later time)
TEST_F(BasicClusterTest, UsingExternalAuth) {
    cb::test::UserEntry ue;
    ue.username = "extuser";
    ue.password = "extpass";
    ue.authz = R"({
    "buckets": {
      "*": [
        "all"
      ]
    },
    "privileges": [
      "all"
    ],
    "domain": "external"
  })"_json;
    cluster->getAuthProviderService().upsertUser(ue);
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("extuser", "extpass", "PLAIN");
    conn->selectBucket(bucket->getName());
    conn->store("BasicClusterTest_Observe", Vbid{0}, "value");

    cluster->getAuthProviderService().removeUser("extuser");
}

/// Verify that we don't break a DCP connection if we send a command
/// which resets the engine specific part of the command
TEST_F(BasicClusterTest, VerifyDcpSurviesResetOfEngineSpecific) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    auto rsp = conn->execute(BinprotDcpOpenCommand{
            "my-dcp-stream", cb::mcbp::request::DcpOpenPayload::Producer});
    ASSERT_TRUE(rsp.isSuccess())
            << "Failed to dcp open: " << rsp.getDataString();

    BinprotDcpStreamRequestCommand streamRequestCommand;
    streamRequestCommand.setDcpReserved(0);
    streamRequestCommand.setDcpStartSeqno(0);
    streamRequestCommand.setDcpEndSeqno(0xffffffff);
    streamRequestCommand.setDcpVbucketUuid(0);
    streamRequestCommand.setDcpSnapStartSeqno(0);
    streamRequestCommand.setDcpSnapEndSeqno(0xfffffff);
    streamRequestCommand.setVBucket(Vbid(0));
    rsp = conn->execute(streamRequestCommand);

    ASSERT_TRUE(rsp.isSuccess())
            << "Stream request failed: " << to_string(rsp.getStatus()) << ". "
            << rsp.getDataString();

    // as of now we don't really know the message sequence being received
    // so we need to send packages and read until we see the expected packet
    // returned.

    // Compact the database
    BinprotCompactDbCommand compactDbCommand;
    compactDbCommand.setDbFileId(Vbid(0));

    Frame frame;
    compactDbCommand.encode(frame.payload);
    conn->sendFrame(frame);
    bool found = false;
    do {
        frame.reset();
        conn->recvFrame(frame);
        const auto* header = frame.getHeader();
        if (header->isResponse() && header->getResponse().getClientOpcode() ==
                                            cb::mcbp::ClientOpcode::CompactDb) {
            found = true;
        }
    } while (!found);

    ASSERT_EQ(cb::mcbp::Status::Success, frame.getResponse()->getStatus());

    // Store an item

    BinprotMutationCommand mutationCommand;
    mutationCommand.setKey("foo");
    mutationCommand.setMutationType(MutationType::Set);
    mutationCommand.encode(frame.payload);
    conn->sendFrame(frame);

    // Wait to see that we receive the item
    found = false;
    bool dcp_mutation = false;
    do {
        frame.reset();
        conn->recvFrame(frame);
        const auto* header = frame.getHeader();
        if (header->isResponse() && header->getResponse().getClientOpcode() ==
                                            cb::mcbp::ClientOpcode::Set) {
            found = true;
        } else if (header->isRequest() &&
                   header->getRequest().getClientOpcode() ==
                           cb::mcbp::ClientOpcode::DcpMutation) {
            auto k = header->getRequest().getKey();
            const auto key = std::string_view{
                    reinterpret_cast<const char*>(k.data()), k.size()};
            if (key == "foo") {
                dcp_mutation = true;
            }
        }
    } while (!found);
    ASSERT_EQ(cb::mcbp::Status::Success, frame.getResponse()->getStatus());

    if (!dcp_mutation) {
        // Wait for the mutation to arrive
        found = false;
        do {
            frame.reset();
            conn->recvFrame(frame);
            const auto* header = frame.getHeader();
            if (header->isRequest() &&
                header->getRequest().getClientOpcode() ==
                        cb::mcbp::ClientOpcode::DcpMutation) {
                auto k = header->getRequest().getKey();
                const auto key = std::string_view{
                        reinterpret_cast<const char*>(k.data()), k.size()};
                if (key == "foo") {
                    found = true;
                }
            }
        } while (!found);
    }
}

/// Verify that we can only run a subset of the commands until
/// the connection is authenticated
TEST_F(BasicClusterTest, MB_47216) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    auto rsp =
            conn->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    EXPECT_EQ(cb::mcbp::Status::Eaccess, rsp.getStatus());
    conn->authenticate("@admin", "password", "PLAIN");
    rsp = conn->execute(BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    EXPECT_TRUE(rsp.isSuccess());
}
