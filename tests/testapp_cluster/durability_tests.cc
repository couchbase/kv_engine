/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "clustertest.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <condition_variable>
#include <string>

/**
 * Get a frame info vector with a durability entry with Majority
 */
static FrameInfoVector GetMajorityDurabilityFrameInfoVector() {
    FrameInfoVector ret;
    ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
            cb::durability::Level::Majority));
    return ret;
}

class DurabilityTest : public cb::test::ClusterTest {
protected:
    static void mutate(MemcachedConnection& conn,
                       std::string id,
                       MutationType type) {
        MutationInfo old{};
        if (type != MutationType::Add) {
            old = conn.store(id, Vbid{0}, "", cb::mcbp::Datatype::Raw);
        }

        Document doc{};
        doc.value = "body";
        doc.info.id = std::move(id);
        doc.info.datatype = cb::mcbp::Datatype::Raw;
        const auto info = conn.mutate(
                doc, Vbid{0}, type, GetMajorityDurabilityFrameInfoVector);
        EXPECT_NE(0, info.cas);
        EXPECT_NE(old.cas, info.cas);
    }

    static void subdoc(MemcachedConnection& conn,
                       std::string id,
                       cb::mcbp::ClientOpcode opcode) {
        BinprotSubdocCommand cmd(opcode);
        cmd.setKey(std::move(id));
        cmd.setPath("foo");
        if (opcode != cb::mcbp::ClientOpcode::SubdocDelete) {
            cmd.setValue("1");
            if (opcode == cb::mcbp::ClientOpcode::SubdocArrayInsert) {
                cmd.setPath("foo.[0]");
            } else if (opcode != cb::mcbp::ClientOpcode::SubdocReplace) {
                cmd.addPathFlags(SUBDOC_FLAG_MKDIR_P);
                cmd.addDocFlags(cb::mcbp::subdoc::doc_flag::Mkdoc);
            }
        }
        cmd.addFrameInfo(DurabilityFrameInfo{cb::durability::Level::Majority});
        auto rsp = conn.execute(cmd);
        EXPECT_TRUE(rsp.isSuccess())
                << "Status: " << to_string(rsp.getStatus()) << std::endl
                << "Value: " << rsp.getDataString();
        EXPECT_NE(0, rsp.getCas());
    }

    static std::unique_ptr<MemcachedConnection> getConnection() {
        auto bucket = cluster->getBucket("default");
        auto conn = bucket->getConnection(Vbid(0));
        conn->authenticate("@admin", "password", "PLAIN");
        conn->selectBucket(bucket->getName());
        return conn;
    }

    void checkMB50413(cb::test::Bucket& bucket, const MutationInfo& minfo);
};

TEST_F(DurabilityTest, Set) {
    mutate(*getConnection(), "Set", MutationType::Set);
}

TEST_F(DurabilityTest, Add) {
    mutate(*getConnection(), "Add", MutationType::Add);
}

TEST_F(DurabilityTest, Replace) {
    mutate(*getConnection(), "Replace", MutationType::Replace);
}

TEST_F(DurabilityTest, Append) {
    mutate(*getConnection(), "Append", MutationType::Append);
}

TEST_F(DurabilityTest, Prepend) {
    mutate(*getConnection(), "Prepend", MutationType::Prepend);
}

TEST_F(DurabilityTest, Delete) {
    auto conn = getConnection();
    const auto old =
            conn->store("Delete", Vbid{0}, "", cb::mcbp::Datatype::Raw);
    auto info = getConnection()->remove(
            "Delete", Vbid{0}, 0, GetMajorityDurabilityFrameInfoVector);
    EXPECT_NE(0, info.cas);
    EXPECT_NE(old.cas, info.cas);
}

TEST_F(DurabilityTest, Increment) {
    auto conn = getConnection();
    for (uint64_t ii = 0; ii < 10ULL; ++ii)
        EXPECT_EQ(ii,
                  conn->increment("Increment",
                                  1,
                                  0,
                                  0,
                                  nullptr,
                                  GetMajorityDurabilityFrameInfoVector));
}

TEST_F(DurabilityTest, Decrement) {
    auto conn = getConnection();
    for (uint64_t ii = 10; ii > 0ULL; --ii)
        EXPECT_EQ(ii,
                  conn->decrement("Decrement",
                                  1,
                                  10,
                                  0,
                                  nullptr,
                                  GetMajorityDurabilityFrameInfoVector));
}

TEST_F(DurabilityTest, SubdocDictAdd) {
    subdoc(*getConnection(),
           "SubdocDictAdd",
           cb::mcbp::ClientOpcode::SubdocDictAdd);
}

TEST_F(DurabilityTest, SubdocDictUpsert) {
    subdoc(*getConnection(),
           "SubdocDictUpsert",
           cb::mcbp::ClientOpcode::SubdocDictUpsert);
}

TEST_F(DurabilityTest, SubdocDelete) {
    auto conn = getConnection();
    conn->store("SubdocDelete",
                Vbid{0},
                R"({"foo":"bar"})",
                cb::mcbp::Datatype::JSON);
    subdoc(*conn, "SubdocDelete", cb::mcbp::ClientOpcode::SubdocDelete);
}

TEST_F(DurabilityTest, SubdocReplace) {
    auto conn = getConnection();
    conn->store("SubdocReplace",
                Vbid{0},
                R"({"foo":"bar"})",
                cb::mcbp::Datatype::JSON);
    subdoc(*conn, "SubdocReplace", cb::mcbp::ClientOpcode::SubdocReplace);
}

TEST_F(DurabilityTest, SubdocArrayPushLast) {
    subdoc(*getConnection(),
           "SubdocArrayPushLast",
           cb::mcbp::ClientOpcode::SubdocArrayPushLast);
}
TEST_F(DurabilityTest, SubdocArrayPushFirst) {
    subdoc(*getConnection(),
           "SubdocArrayPushFirst",
           cb::mcbp::ClientOpcode::SubdocArrayPushFirst);
}
TEST_F(DurabilityTest, SubdocArrayInsert) {
    auto conn = getConnection();
    conn->store("SubdocArrayInsert",
                Vbid{0},
                R"({"foo":[]})",
                cb::mcbp::Datatype::JSON);
    subdoc(*conn,
           "SubdocArrayInsert",
           cb::mcbp::ClientOpcode::SubdocArrayInsert);
}
TEST_F(DurabilityTest, SubdocArrayAddUnique) {
    subdoc(*getConnection(),
           "SubdocArrayAddUnique",
           cb::mcbp::ClientOpcode::SubdocArrayAddUnique);
}

TEST_F(DurabilityTest, SubdocCounter) {
    subdoc(*getConnection(),
           "SubdocCounter",
           cb::mcbp::ClientOpcode::SubdocCounter);
}

TEST_F(DurabilityTest, SubdocMultiMutation) {
    BinprotSubdocMultiMutationCommand cmd;
    cmd.setKey("SubdocMultiMutation");
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictAdd,
                    SUBDOC_FLAG_MKDIR_P,
                    "foo",
                    R"("value")");
    cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::Mkdoc);
    cmd.addFrameInfo(DurabilityFrameInfo{cb::durability::Level::Majority});
    auto rsp = getConnection()->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess())
            << "Status: " << to_string(rsp.getStatus()) << std::endl
            << "Value: " << rsp.getDataString();
    EXPECT_NE(0, rsp.getCas());
}

/// Verify that sync write Revive of a deleted document succeeds
TEST_F(DurabilityTest, SyncWriteReviveDeletedDocument) {
    BinprotSubdocMultiMutationCommand cmd;
    std::string name = "foobar";
    cmd.setKey(name);
    cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::Add);
    cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::CreateAsDeleted);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                    "tnx.foo",
                    R"({})");
    auto conn = getConnection();
    conn->sendCommand(cmd);

    BinprotSubdocMultiMutationResponse resp;
    conn->recvResponse(resp);
    ASSERT_EQ(cb::mcbp::Status::SubdocSuccessDeleted, resp.getStatus());

    cmd = {};
    cmd.setKey(name);
    cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::AccessDeleted);
    cmd.addFrameInfo(DurabilityFrameInfo{cb::durability::Level::Majority});
    cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::ReviveDocument);
    cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                    SUBDOC_FLAG_XATTR_PATH,
                    "tnx.bar",
                    R"("This should succeed")");
    conn->sendCommand(cmd);
    conn->recvResponse(resp);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

/**
 * MB-34780 - Bucket delete fails if we've got pending sync writes
 *
 * As part of bucket deletion all of the DCP streams get torn down so
 * a sync write will _never_ complete. This caused bucket deletion to
 * block as the cookie was in an ewouldblock state
 */
TEST_F(DurabilityTest, MB34780) {
    cluster->deleteBucket("default");
    std::mutex mutex;
    std::condition_variable cond;
    bool prepare_seen = false;

    auto bucket = cluster->createBucket(
            "default",
            {{"replicas", 2}, {"max_vbuckets", 8}},
            [&mutex, &cond, &prepare_seen](const std::string& source,
                                           const std::string& destination,
                                           std::vector<uint8_t>& packet) {
                if (prepare_seen) {
                    // Swallow everything..
                    packet.clear();
                    return;
                }

                const auto* h = reinterpret_cast<const cb::mcbp::Header*>(
                        packet.data());
                if (h->isRequest()) {
                    const auto& req = h->getRequest();
                    if (req.getClientOpcode() ==
                        cb::mcbp::ClientOpcode::DcpPrepare) {
                        std::lock_guard<std::mutex> guard(mutex);
                        prepare_seen = true;
                        cond.notify_all();
                        packet.clear();
                    }
                }
            });
    ASSERT_TRUE(bucket) << "Failed to create bucket default";

    auto conn1 = bucket->getAuthedConnection(Vbid(0));

    // Test expanded to cover MB-50413, store and capture the seqno of one
    // 'visible' item
    auto mutationInfo = conn1->store("Key1", Vbid(0), "Default");
    auto rsp = conn1->getAllVBucketSequenceNumbers();
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_EQ(mutationInfo.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);

    BinprotMutationCommand command;
    command.setKey("MB34780");
    command.setMutationType(MutationType::Set);
    DurabilityFrameInfo frameInfo(cb::durability::Level::Majority);
    command.addFrameInfo(frameInfo);
    conn1->sendCommand(command);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&prepare_seen]() { return prepare_seen; });

    checkMB50413(*bucket, mutationInfo);

    // At this point we've sent the prepare, it is registered in the
    // durability manager.. We should be able to delete the bucket
    cluster->deleteBucket("default");
}

void DurabilityTest::checkMB50413(cb::test::Bucket& bucket,
                                  const MutationInfo& mutationInfo) {
    // Create two new connections (main test is blocked on prepare)
    auto conn2 = bucket.getAuthedConnection(Vbid(0));
    auto conn3 = bucket.getAuthedConnection(Vbid(0));
    conn3->setFeature(cb::mcbp::Feature::Collections, true);

    // Non collection client cannot see the prepare, we expect to see the seqno
    // of the given MutationInfo
    auto rsp = conn2->getAllVBucketSequenceNumbers();
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    // Still the seqno of the first mutation
    EXPECT_EQ(mutationInfo.seqno, rsp.getVbucketSeqnos()[Vbid(0)]);

    // Collection aware client should see the prepare seqno for either bucket or
    // collection specific request
    rsp = conn3->getAllVBucketSequenceNumbers();
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_GT(rsp.getVbucketSeqnos()[Vbid(0)], mutationInfo.seqno);
    rsp = conn3->getAllVBucketSequenceNumbers(0, CollectionID::Default);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_GT(rsp.getVbucketSeqnos()[Vbid(0)], mutationInfo.seqno);
}
