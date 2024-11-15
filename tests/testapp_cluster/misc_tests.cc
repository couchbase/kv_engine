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
#include <cluster_framework/node.h>
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/base64.h>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <snapshot/download_properties.h>
#include <snapshot/manifest.h>
#include <tests/testapp/testapp_subdoc_common.h>

#include <string>

using cb::mcbp::ClientOpcode;
using cb::mcbp::Status;
using cb::mcbp::subdoc::DocFlag;
using cb::mcbp::subdoc::PathFlag;

class BasicClusterTest : public cb::test::ClusterTest {
protected:
    /**
     * Wait until we can fetch a named document via "SubdocExists" on a
     * a given connection. You may ask yourself why we need this command
     * when we store with durability options, but the document may have been
     * replicated to enough replicas to satisfy the durability spec; just not
     * the one we're trying to connect to. (We're using durability spec to
     * hopefully reduce the number of "polling requests" to check if the
     * value is replicated.
     *
     * We can't use the simpler GetReplica as that won't support getting
     * deleted documents, but we can use SubdocExists to check for a path.
     * It'll return KeyEnoent until the document is available on the node,
     * and then:
     *
     *    Success[Deleted] - if the path exists
     *    SubdocPathEnoent - if the path don't exist
     *    SubdocDocNotJson - if the document isn't JSON
     *
     * These return values indicates that the document is replicated to the
     * node
     *
     * @param conn The connection to the node where we want to read the replica
     * @param key The document identiier
     */
    static void waitForReplication(MemcachedConnection& conn, std::string key) {
        BinprotSubdocCommand cmd{ClientOpcode::SubdocExists,
                                 std::move(key),
                                 "WaitForReplication"};
        cmd.addDocFlags(DocFlag::ReplicaRead);
        cmd.addDocFlags(DocFlag::AccessDeleted);

        auto rsp = conn.execute(cmd);
        while (rsp.getStatus() == Status::KeyEnoent) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            rsp = conn.execute(cmd);
        }

        ASSERT_TRUE(rsp.isSuccess() ||
                    rsp.getStatus() == Status::SubdocPathEnoent ||
                    rsp.getStatus() == Status::SubdocDocNotJson)
                << rsp.getStatus() << " - " << rsp.getDataView();
    }

    template <typename T>
    void testSingleSubdocReplicaCommand(T& cmd) {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getAuthedConnection(Vbid(0));
        auto replica =
                bucket->getAuthedConnection(Vbid(0), vbucket_state_replica);

        Document doc;
        doc.info.id = cmd.getKey();
        doc.value = R"({"array":[ "foo", "bar"]})";

        conn->mutate(doc, Vbid{0}, MutationType::Set, []() -> FrameInfoVector {
            FrameInfoVector ret;
            ret.emplace_back(
                    std::make_unique<cb::mcbp::request::DurabilityFrameInfo>(
                            cb::durability::Level::Majority));
            return ret;
        });

        // Unfortunately, there is no "All" and we don't know the order it
        // got replicated, and I've seen failures where we got a "not found".
        // Just wait until it got replicated to the node we're going to check
        waitForReplication(*replica, cmd.getKey());

        // We can read it from the active vbucket
        auto rsp = conn->execute(cmd);
        EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

        // We should not be able to fetch it from the replica unless asking for
        // it
        rsp = replica->execute(cmd);
        EXPECT_EQ(Status::NotMyVbucket, rsp.getStatus());

        // Add ReplicaRead, and we should get it!
        cmd.addDocFlags(DocFlag::ReplicaRead);
        rsp = replica->execute(cmd);
        EXPECT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataView();

        // Verify that we can mix with AccessDeleted (note that the document
        // isn't deleted)
        cmd.addDocFlags(DocFlag::AccessDeleted);
        rsp = replica->execute(cmd);
        EXPECT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataView();

        // It should fail on the active vbucket
        rsp = conn->execute(cmd);
        EXPECT_EQ(Status::NotMyVbucket, rsp.getStatus()) << rsp.getDataView();
    }

    void testSingleSubdocReplicaCommand(cb::mcbp::ClientOpcode opcode) {
        BinprotSubdocCommand cmd{
                opcode, "testSingleSubdocReplicaCommand", "array"};
        testSingleSubdocReplicaCommand<BinprotSubdocCommand>(cmd);
    }
};

TEST_F(BasicClusterTest, GetReplica) {
    auto bucket = cluster->getBucket("default");
    {
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin");
        conn->selectBucket(bucket->getName());
        auto info = conn->store("foo", Vbid(0), "value");
        EXPECT_NE(0, info.cas);
    }

    // make sure that it is replicated to all the nodes in the
    // cluster
    const auto nrep = bucket->getVbucketMap()[0].size() - 1;
    for (std::size_t rep = 0; rep < nrep; ++rep) {
        auto conn = bucket->getConnection(Vbid(0), vbucket_state_replica, rep);
        conn->authenticate("@admin");
        conn->selectBucket(bucket->getName());

        BinprotResponse rsp;
        do {
            BinprotGenericCommand cmd(ClientOpcode::GetReplica);
            cmd.setVBucket(Vbid(0));
            cmd.setKey("foo");

            rsp = conn->execute(cmd);
        } while (rsp.getStatus() == Status::KeyEnoent);
        EXPECT_TRUE(rsp.isSuccess());
    }
}

TEST_F(BasicClusterTest, MultiGet) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin");
    conn->selectBucket(bucket->getName());

    std::vector<std::pair<const std::string, Vbid>> keys;
    for (int ii = 0; ii < 10; ++ii) {
        keys.emplace_back("key_" + std::to_string(ii), Vbid(0));
        if ((ii & 1) == 1) {
            conn->store(keys.back().first,
                        keys.back().second,
                        "value",
                        cb::mcbp::Datatype::Raw);
        }
    }

    // and I want a not my vbucket!
    keys.emplace_back("NotMyVbucket", Vbid(1));
    bool nmvb = false;
    int nfound = 0;
    conn->mget(
            keys,
            [&nfound](std::unique_ptr<Document>&) -> void { ++nfound; },
            [&nmvb](const std::string& key,
                    const cb::mcbp::Response& rsp) -> void {
                EXPECT_EQ("NotMyVbucket", key);
                EXPECT_EQ(Status::NotMyVbucket, rsp.getStatus());
                nmvb = true;
            });

    EXPECT_TRUE(nmvb) << "Did not get the NotMyVbucket callback";
    EXPECT_EQ(5, nfound);
}

/// Store a key on one node, and wait until its replicated all over
TEST_F(BasicClusterTest, Observe) {
    auto bucket = cluster->getBucket("default");
    auto replica = bucket->getConnection(Vbid(0), vbucket_state_replica, 0);
    replica->authenticate("@admin");
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
        conn->authenticate("@admin");
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
    replica->authenticate("@admin");
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
        conn->authenticate("@admin");
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
    conn->authenticate("extuser");
    conn->selectBucket(bucket->getName());
    conn->store("BasicClusterTest_Observe", Vbid{0}, "value");

    cluster->getAuthProviderService().removeUser("extuser");
}

/// Verify that we don't break a DCP connection if we send a command
/// which resets the engine specific part of the command
TEST_F(BasicClusterTest, VerifyDcpSurviesResetOfEngineSpecific) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin");
    conn->selectBucket(bucket->getName());

    auto rsp = conn->execute(BinprotDcpOpenCommand{
            "my-dcp-stream", cb::mcbp::DcpOpenFlag::Producer});
    ASSERT_TRUE(rsp.isSuccess()) << "Failed to dcp open: " << rsp.getDataView();

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
            << rsp.getDataView();

    // as of now we don't really know the message sequence being received
    // so we need to send packages and read until we see the expected packet
    // returned.

    // Compact the database
    BinprotCompactDbCommand compactDbCommand;

    Frame frame;
    compactDbCommand.encode(frame.payload);
    conn->sendFrame(frame);
    bool found = false;
    do {
        frame.reset();
        conn->recvFrame(frame);
        const auto* header = frame.getHeader();
        if (header->isResponse() && header->getResponse().getClientOpcode() ==
                                            ClientOpcode::CompactDb) {
            found = true;
        }
    } while (!found);

    ASSERT_EQ(Status::Success, frame.getResponse()->getStatus());

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
        if (header->isResponse() &&
            header->getResponse().getClientOpcode() == ClientOpcode::Set) {
            found = true;
        } else if (header->isRequest() &&
                   header->getRequest().getClientOpcode() ==
                           ClientOpcode::DcpMutation) {
            const auto key = header->getRequest().getKeyString();
            if (key == "foo") {
                dcp_mutation = true;
            }
        }
    } while (!found);
    ASSERT_EQ(Status::Success, frame.getResponse()->getStatus());

    if (!dcp_mutation) {
        // Wait for the mutation to arrive
        found = false;
        do {
            frame.reset();
            conn->recvFrame(frame);
            const auto* header = frame.getHeader();
            if (header->isRequest() && header->getRequest().getClientOpcode() ==
                                               ClientOpcode::DcpMutation) {
                const auto key = header->getRequest().getKeyString();
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
    auto rsp = conn->execute(BinprotGenericCommand{ClientOpcode::Noop});
    EXPECT_EQ(Status::Eaccess, rsp.getStatus());
    conn->authenticate("@admin");
    rsp = conn->execute(BinprotGenericCommand{ClientOpcode::Noop});
    EXPECT_TRUE(rsp.isSuccess());
}

// Verifies that we can successfully request all stat groups. Given the
// different and varying formats of different stat groups, there's no
// check on the actual content returned, other than that _something_ is
// returned for the groups which we expect there to be.
TEST_F(BasicClusterTest, AllStatGroups) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getAuthedConnection(Vbid(0));

    // Add a single key so we can ask for stats on it.
    Document doc;
    doc.info.id = "test_key";
    conn->mutate(doc, Vbid{0}, MutationType::Set);

    StatsGroupManager::getInstance().iterate([&conn](const StatGroup& group) {
        std::string key{group.key};

        // Adjust request 'key' for some stats (groups require an
        // argument etc).
        switch (group.id) {
        case StatGroupId::DcpVbtakeover:
            // Requires a vBucket and name of a DCP connection
            key += " 0 n_0->n_1";
            break;
        case StatGroupId::Dcpagg:
            // While Dcpagg group's stat name is 'dcpagg', a client must
            // specify a prefix to aggregate, or '_' as a wildcard.
            key += " _";
            break;
        case StatGroupId::Key:
        case StatGroupId::KeyById:
        case StatGroupId::Vkey:
        case StatGroupId::VkeyById:
            // These require a key to exist otherwise they return
            // ENOENT.
            key += " test_key 0";
            break;
        case StatGroupId::DiskSlowness:
            // Requires a threshold in seconds.
            key += " 1";
            break;
        case StatGroupId::DurabilityMonitor:
        case StatGroupId::_CheckpointDump:
        case StatGroupId::_HashDump:
        case StatGroupId::_DurabilityDump:
        case StatGroupId::_VbucketDump:
            // These require a vbucket argument.
            key += " 0";
            break;
        default:
            break;
        }

        SCOPED_TRACE(key);

        // Use adminConnection as some stats are privileged.
        // stats() throws if the request is not successful (and
        // hence test would fail).
        EXPECT_NO_THROW(conn->stats(key));
    });
}

TEST_F(BasicClusterTest, SubdocReplicaRead) {
    testSingleSubdocReplicaCommand(ClientOpcode::SubdocGet);
}

TEST_F(BasicClusterTest, SubdocReplicaExists) {
    testSingleSubdocReplicaCommand(ClientOpcode::SubdocExists);
}

TEST_F(BasicClusterTest, SubdocReplicaGetCount) {
    testSingleSubdocReplicaCommand(ClientOpcode::SubdocGetCount);
}

TEST_F(BasicClusterTest, SubdocReplicaMulti) {
    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey("testMultiSubdocReplicaCommand");
    cmd.setVBucket(Vbid{0});
    cmd.addGet("array").addExists("array").addGetCount("array");
    testSingleSubdocReplicaCommand<BinprotSubdocMultiLookupCommand>(cmd);
}

TEST_F(BasicClusterTest, SubdocReplicaGetWholedoc) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getAuthedConnection(Vbid(0));
    auto replica = bucket->getAuthedConnection(Vbid(0), vbucket_state_replica);

    Document doc;
    doc.info.id = "SubdocReplicaGetWholedoc";
    doc.value = "This is the full document value";

    conn->mutate(doc, Vbid{0}, MutationType::Set, []() -> FrameInfoVector {
        FrameInfoVector ret;
        ret.emplace_back(
                std::make_unique<cb::mcbp::request::DurabilityFrameInfo>(
                        cb::durability::Level::Majority));
        return ret;
    });

    waitForReplication(*replica, doc.info.id);

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey("SubdocReplicaGetWholedoc");
    cmd.setVBucket(Vbid{0});

    cmd.addLookup("$document", ClientOpcode::SubdocGet, PathFlag::XattrPath);
    cmd.addLookup("", ClientOpcode::Get);

    // We can read it from the active vbucket
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    auto getResultPair = [](BinprotResponse resp) {
        BinprotSubdocMultiLookupResponse response(std::move(resp));
        const auto results = response.getResults();
        return std::pair<nlohmann::json, std::string>{
                nlohmann::json::parse(results[0].value), results[1].value};
    };
    const auto [active_meta, active_value] = getResultPair(rsp);
    EXPECT_EQ(doc.value, active_value);

    // We should not be able to fetch it from the replica unless asking for
    // it
    rsp = replica->execute(cmd);
    EXPECT_EQ(Status::NotMyVbucket, rsp.getStatus());

    // Add ReplicaRead, and we should get it!
    cmd.addDocFlags(DocFlag::ReplicaRead);
    rsp = replica->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataView();
    {
        const auto [replica_meta, replica_value] = getResultPair(rsp);
        EXPECT_EQ(active_meta, replica_meta);
        EXPECT_EQ(active_value, replica_value);
    }

    // mix that with access deleted (the document isn't deleted)
    cmd.addDocFlags(DocFlag::AccessDeleted);
    rsp = replica->execute(cmd);
    EXPECT_EQ(Status::Success, rsp.getStatus()) << rsp.getDataView();
    {
        const auto [replica_meta, replica_value] = getResultPair(rsp);
        EXPECT_EQ(active_meta, replica_meta);
        EXPECT_EQ(active_value, replica_value);
    }

    // It should fail on the active vbucket
    rsp = conn->execute(cmd);
    EXPECT_EQ(Status::NotMyVbucket, rsp.getStatus());
}

TEST_F(BasicClusterTest, SubdocReplicaReadDeletedDocument) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getAuthedConnection(Vbid(0));
    auto replica = bucket->getAuthedConnection(Vbid(0), vbucket_state_replica);

    {
        BinprotSubdocCommand cmd;
        cmd.setOp(ClientOpcode::SubdocDictAdd);
        cmd.setKey("SubdocReplicaReadDeletedDocument");
        cmd.setPath("txn.deleted");
        cmd.setValue("true");
        cmd.addPathFlags(PathFlag::Mkdir_p | PathFlag::XattrPath);
        cmd.addDocFlags(DocFlag::Mkdoc | DocFlag::CreateAsDeleted);
        cb::mcbp::request::DurabilityFrameInfo fi(
                cb::durability::Level::Majority);
        cmd.addFrameInfo(fi);
        auto rsp = conn->execute(cmd);

        EXPECT_EQ(Status::SubdocSuccessDeleted, rsp.getStatus());
    }

    // This issues a SubdocExists on a deleted document to wait, so if it
    // succeeds it actually verifies that we can use Subdoc on deleted
    // documents on a replica ;)
    waitForReplication(*replica, "SubdocReplicaReadDeletedDocument");

    // Issue a SubdocMulti to verify that it works ;)

    BinprotSubdocMultiLookupCommand cmd;
    cmd.setKey("SubdocReplicaReadDeletedDocument");
    cmd.setVBucket(Vbid{0});

    cmd.addLookup(
            "$document.flags", ClientOpcode::SubdocGet, PathFlag::XattrPath);
    cmd.addLookup("txn.deleted", ClientOpcode::SubdocGet, PathFlag::XattrPath);

    // Document is deleted so we can't fetch it from the replica
    cmd.addDocFlags(DocFlag::ReplicaRead);
    auto rsp = BinprotSubdocMultiLookupResponse{replica->execute(cmd)};
    EXPECT_EQ(Status::KeyEnoent, rsp.getStatus()) << rsp.getDataView();

    cmd.addDocFlags(DocFlag::AccessDeleted);
    rsp = BinprotSubdocMultiLookupResponse{replica->execute(cmd)};
    ASSERT_EQ(Status::SubdocSuccessDeleted, rsp.getStatus())
            << rsp.getDataView();
    EXPECT_EQ(Status::Success, rsp.getResults()[0].status);
    EXPECT_EQ("0", rsp.getResults()[0].value);
    EXPECT_EQ(Status::Success, rsp.getResults()[1].status);
    EXPECT_EQ("true", rsp.getResults()[1].value);

    cmd.addLookup("txn.foo", ClientOpcode::SubdocGet, PathFlag::XattrPath);
    rsp = BinprotSubdocMultiLookupResponse{replica->execute(cmd)};
    EXPECT_EQ(Status::SubdocMultiPathFailureDeleted, rsp.getStatus())
            << rsp.getDataView();
    EXPECT_EQ(Status::Success, rsp.getResults()[0].status);
    EXPECT_EQ("0", rsp.getResults()[0].value);
    EXPECT_EQ(Status::Success, rsp.getResults()[1].status);
    EXPECT_EQ("true", rsp.getResults()[1].value);
    EXPECT_EQ(Status::SubdocPathEnoent, rsp.getResults()[2].status);
}

TEST_F(BasicClusterTest, DISABLED_OAUTHBEARER) {
    auto builder = cb::test::AuthProviderService::getTokenBuilder("jwt");
    builder->addClaim("cb-rbac", cb::base64url::encode(R"({
  "buckets": {
    "default": {
      "privileges": ["Read"]
    }
  },
  "privileges": [],
  "domain": "external"
})"));
    builder->setExpiration(std::chrono::system_clock::now() +
                           std::chrono::seconds(2));
    const auto readOnlyToken = builder->build();
    builder = cb::test::AuthProviderService::getTokenBuilder("jwt");
    builder->addClaim("cb-rbac", cb::base64url::encode(R"({
  "buckets": {
    "default": {
      "privileges": ["Read", "Upsert"]
    }
  },
  "privileges": [],
  "domain": "external"
})"));
    builder->setExpiration(std::chrono::system_clock::now() +
                           std::chrono::seconds(2));
    const auto readWriteToken = builder->build();

    builder = cb::test::AuthProviderService::getTokenBuilder("jwt");
    builder->addClaim("cb-rbac", cb::base64url::encode(R"({
  "buckets": {
    "default": {
      "privileges": ["Read", "Upsert"]
    }
  },
  "privileges": [],
  "domain": "external"
})"));
    builder->setNotBefore(std::chrono::system_clock::now() +
                          std::chrono::seconds(2));
    const auto readNotReadyToken = builder->build();

    auto readOnlyConn = cluster->getBucket("default")->getConnection(Vbid{0});
    readOnlyConn->authenticate("jwt", readOnlyToken, "OAUTHBEARER");
    readOnlyConn->selectBucket("default");
    try {
        readOnlyConn->arithmetic("counter", 1, 0);
        FAIL() << "Read only connection should not be able to mutate data";
    } catch (ConnectionError& error) {
        EXPECT_TRUE(error.isAccessDenied()) << error.what();
    }

    auto notReadyConn = cluster->getBucket("default")->getConnection(Vbid{0});
    notReadyConn->authenticate("jwt", readNotReadyToken, "OAUTHBEARER");
    try {
        notReadyConn->selectBucket("default");
        FAIL() << "Token should not be ready yet";
    } catch (ConnectionError& error) {
        EXPECT_EQ(cb::mcbp::Status::AuthStale, error.getReason());
    }

    // The readWrite token have write access
    auto readWriteConn = cluster->getBucket("default")->getConnection(Vbid{0});
    readWriteConn->authenticate("jwt", readWriteToken, "OAUTHBEARER");
    readWriteConn->selectBucket("default");
    readWriteConn->arithmetic("counter", 1, 0);

    // The token expire after two seconds
    std::this_thread::sleep_for(std::chrono::seconds{3});
    auto resp = readWriteConn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Noop});
    EXPECT_EQ(cb::mcbp::Status::AuthStale, resp.getStatus());

    builder->setExpiration(std::chrono::system_clock::now() +
                           std::chrono::seconds(2));
    auto refreshToken = builder->build();
    readWriteConn->authenticate("jwt", refreshToken, "OAUTHBEARER");

    // now that we've waited the "not before time" should have passed
    notReadyConn->selectBucket("default");
}

TEST_F(BasicClusterTest, Snapshots) {
    auto bucket = cluster->getBucket("default");
    auto conn = bucket->getAuthedConnection(Vbid(0));
    auto replica = bucket->getAuthedConnection(Vbid(0), vbucket_state_replica);

    cb::snapshot::DownloadProperties properties;
    properties.hostname = conn->getFamily() == AF_INET ? "127.0.0.1" : "::1";
    properties.port = conn->getPort();
    properties.bucket = "default";
    properties.sasl = {"PLAIN", "@admin", "password"};

    BinprotGenericCommand download(ClientOpcode::DownloadSnapshot,
                                   {},
                                   nlohmann::json(properties).dump());
    download.setVBucket(Vbid{0});
    download.setDatatype(cb::mcbp::Datatype::JSON);

    auto rsp = replica->execute(download);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus());
}
