/*
 *     Copyright 2019 Couchbase, Inc
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
                cmd.addDocFlags(mcbp::subdoc::doc_flag::Mkdoc);
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
    cmd.addDocFlag(mcbp::subdoc::doc_flag::Mkdoc);
    cmd.addFrameInfo(DurabilityFrameInfo{cb::durability::Level::Majority});
    auto rsp = getConnection()->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess())
            << "Status: " << to_string(rsp.getStatus()) << std::endl
            << "Value: " << rsp.getDataString();
    EXPECT_NE(0, rsp.getCas());
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

    auto conn = bucket->getConnection(Vbid(0));
    conn->authenticate("@admin", "password", "PLAIN");
    conn->selectBucket(bucket->getName());

    BinprotMutationCommand command;
    command.setKey("MB34780");
    command.setMutationType(MutationType::Set);
    DurabilityFrameInfo frameInfo(cb::durability::Level::Majority);
    command.addFrameInfo(frameInfo);
    conn->sendCommand(command);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&prepare_seen]() { return prepare_seen; });

    // At this point we've sent the prepare, it is registered in the
    // durability manager.. We should be able to delete the bucket
    cluster->deleteBucket("default");
}
