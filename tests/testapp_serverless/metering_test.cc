/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "serverless_test.h"

#include <cluster_framework/bucket.h>
#include <cluster_framework/cluster.h>
#include <folly/lang/Assume.h>
#include <folly/portability/GTest.h>
#include <memcached/storeddockey.h>
#include <platform/base64.h>
#include <platform/timeutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <serverless/config.h>
#include <spdlog/fmt/fmt.h>
#include <xattr/blob.h>
#include <chrono>
#include <deque>
#include <thread>

using ClientOpcode = cb::mcbp::ClientOpcode;
using Status = cb::mcbp::Status;

namespace cb::test {

enum class MeteringType {
    Metered,
    UnmeteredByPrivilege,
    UnmeteredByCollection
};

static std::string to_string(MeteringType type) {
    switch (type) {
    case MeteringType::Metered:
        return "Metered";
    case MeteringType::UnmeteredByPrivilege:
        return "UnmeteredByPrivilege";
    case MeteringType::UnmeteredByCollection:
        return "UnmeteredByCollection";
    }
    folly::assume_unreachable();
}

class MeteringTest : public ::testing::TestWithParam<MeteringType> {
public:
    static void SetUpTestCase() {
        // Set one collection with no metering
        auto entry = CollectionEntry::dairy;
        entry.metered = false;
        cluster->collections.add(entry);
        cluster->getBucket("metering")
                ->setCollectionManifest(cluster->collections.getJson());
    }

    void SetUp() override {
        conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("metering");
        if (GetParam() != MeteringType::UnmeteredByPrivilege) {
            conn->dropPrivilege(cb::rbac::Privilege::Unmetered);
        }
        conn->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);
        conn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
        conn->setFeature(cb::mcbp::Feature::Collections, true);
    }

    void TearDown() override {
        conn.reset();
    }

protected:
    // @return the collection the test should use
    CollectionID getTestCollection() const {
        if (GetParam() == MeteringType::UnmeteredByCollection) {
            return CollectionEntry::dairy.getId();
        }
        return CollectionID::Default;
    }

    bool isUnmetered() const {
        return GetParam() != MeteringType::Metered;
    }

    /**
     * Upsert a document
     *
     * Helper function to update or insert a document with or without
     * extended attributes. Given that xattrs will be copied over when updating
     * a document it also offers the option to delete the document before
     * updating it (which removes the user attributes)
     *
     * @param id The documents identifier
     * @param value The documents value
     * @param xattr_path An optional XAttr path
     * @param xattr_value An optional XAttr value
     * @param wait_for_persistence if set to true it'll use durable write
     *                             and wait for persistence on master
     * @param second_xattr_path An optional second XAttr pair
     * @param second_xattr_value An optional second XAttr value
     */
    void upsert(DocKey id,
                std::string value,
                std::string xattr_path = {},
                std::string xattr_value = {},
                bool wait_for_persistence = false,
                std::string second_xattr_path = {},
                std::string second_xattr_value = {});

    size_t to_ru(size_t size) {
        return cb::serverless::Config::instance().to_ru(size);
    }

    size_t to_wu(size_t size) {
        return cb::serverless::Config::instance().to_wu(size);
    }

    /**
     * Calulate the document size for a document with various components
     *
     * @param key The documents key
     * @param value The documents value
     * @param xp The XAttr path for an xattr
     * @param xv The XAttr value for the path above
     * @param sxp The xattr path for a second xattr
     * @param sxv The xattr value for the path above
     * @return The size of the document (key + value including the encoded
     *         xattr blob)
     */
    size_t calculateDocumentSize(DocKey key,
                                 std::string_view value,
                                 std::string_view xp = {},
                                 std::string_view xv = {},
                                 std::string_view sxp = {},
                                 std::string_view sxv = {}) {
        if (xp.empty()) {
            return key.size() + value.size();
        }
        cb::xattr::Blob blob;
        blob.set(xp, xv);
        if (!sxp.empty()) {
            blob.set(sxp, sxv);
        }
        return key.size() + value.size() + blob.size();
    }

    /**
     * Get a string value of a given length which may be used as an xattr value
     *
     * @param size The length of the string (or almost a full RU if no length
     *             specified)
     * @return A string starting and ending with '"'
     */
    std::string getStringValue(bool quote = true, size_t size = 0) {
        std::string ret;
        if (size == 0) {
            ret.resize(cb::serverless::Config::instance().readUnitSize - 20);
        } else {
            ret.resize(size);
        }
        std::fill(ret.begin(), ret.end(), 'a');
        if (quote) {
            ret.front() = '"';
            ret.back() = '"';
        }
        return ret;
    }

    nlohmann::json getJsonDoc() {
        nlohmann::json ret;
        auto size = cb::serverless::Config::instance().readUnitSize * 2;
        ret["v1"] = "version 1";
        ret["v2"] = "version 2";
        ret["counter"] = 0;
        ret["array"] = nlohmann::json::array();
        for (int ii = 0; ii < 5; ++ii) {
            ret["array"].push_back(std::to_string(ii));
        }
        auto current = ret.dump().size();
        ret["fill"] =
                getStringValue(false, size - current - 10 /* "fill":"",  */);
        return ret;
    }

    std::unique_ptr<MemcachedConnection> getReplicaConn() {
        auto bucket = cluster->getBucket("metering");
        auto rconn = bucket->getConnection(Vbid(0), vbucket_state_replica, 1);
        rconn->authenticate("@admin", "password");
        rconn->selectBucket("metering");
        rconn->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);

        if (GetParam() == MeteringType::Metered) {
            rconn->dropPrivilege(cb::rbac::Privilege::Unmetered);
        }

        rconn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
        rconn->setFeature(cb::mcbp::Feature::Collections, true);
        return rconn;
    }

    /// MB-53560:
    /// Operating on a document which isn't a numeric value should
    /// account for 0 ru's and fail
    void testArithmeticBadValue(ClientOpcode opcode,
                                DocKey id,
                                std::string value,
                                std::string xattr_path = {},
                                std::string xattr_value = {}) {
        auto cmd = BinprotIncrDecrCommand{
                opcode, std::string{id}, Vbid{0}, 1ULL, 0ULL, 0};

        upsert(id, value, xattr_path, xattr_value);
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(Status::DeltaBadval, rsp.getStatus());
        ASSERT_FALSE(rsp.getReadUnits().has_value());
        EXPECT_FALSE(rsp.getWriteUnits());
    }

    /// When creating a value as part of incr/decr it should not cost any
    /// RU, but 1 WU for a normal create, and 2 WU for durable writes.
    void testArithmeticCreateValue(ClientOpcode opcode,
                                   DocKey id,
                                   bool durable) {
        auto cmd = BinprotIncrDecrCommand{
                opcode, std::string{id}, Vbid{0}, 1ULL, 0ULL, 0};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        EXPECT_FALSE(rsp.getReadUnits());

        if (isUnmetered()) {
            EXPECT_FALSE(rsp.getWriteUnits());
        } else {
            ASSERT_TRUE(rsp.getWriteUnits());
            if (durable) {
                EXPECT_EQ(2, *rsp.getWriteUnits());
            } else {
                EXPECT_EQ(1, *rsp.getWriteUnits());
            }
        }
    }

    // Operating on a document without XAttrs should account 1WU during
    // create and 1RU + 1WU during update (it is 1 because it only contains
    // the body and we don't support an interger which consumes 4k digits ;)
    // For documents with XATTRS the size of the xattrs (encoded) gets added
    // to the RU/WU units.
    // For durable writes it costs 2x WU
    void testArithmetic(ClientOpcode opcode,
                        DocKey id,
                        std::string xattr_path,
                        std::string xattr_value,
                        bool durable) {
        upsert(id, "10", xattr_path, xattr_value);

        auto cmd = BinprotIncrDecrCommand{
                opcode, std::string{id}, Vbid{0}, 1ULL, 0ULL, 0};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

        if (isUnmetered()) {
            EXPECT_FALSE(rsp.getWriteUnits());
        } else {
            const auto expected_wu = to_wu(
                    calculateDocumentSize(id, "10", xattr_path, xattr_value));
            ASSERT_TRUE(rsp.getWriteUnits());
            if (durable) {
                EXPECT_EQ(expected_wu * 2, *rsp.getWriteUnits());
            } else {
                EXPECT_EQ(expected_wu, *rsp.getWriteUnits());
            }
        }
    }

    // Deleting a normal document should cost 1 WU (no RU)
    // Delete a document with user XATTRs should cost 1WU and doc size RU
    // Delete a document with system xattrs should cost xattr + key wu and doc
    //        size ru
    // Durable reads should cost twice
    void testDelete(ClientOpcode opcode,
                    DocKey id,
                    std::string value,
                    std::string user_xattr_path,
                    std::string user_xattr_value,
                    std::string system_xattr_path,
                    std::string system_xattr_value,
                    bool durable) {
        upsert(id,
               value,
               user_xattr_path,
               user_xattr_value,
               false,
               system_xattr_path,
               system_xattr_value);

        auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete,
                                         std::string{id}};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

        if (isUnmetered()) {
            EXPECT_FALSE(rsp.getWriteUnits());
        } else {
            ASSERT_TRUE(rsp.getWriteUnits());
            if (user_xattr_path.empty()) {
                if (durable) {
                    EXPECT_EQ(2, *rsp.getWriteUnits());
                } else {
                    EXPECT_EQ(1, *rsp.getWriteUnits());
                }
            } else {
                if (durable) {
                    EXPECT_EQ(to_wu(calculateDocumentSize(id,
                                                          {},
                                                          system_xattr_path,
                                                          system_xattr_value)) *
                                      2,
                              *rsp.getWriteUnits());
                } else {
                    EXPECT_EQ(to_wu(calculateDocumentSize(id,
                                                          {},
                                                          system_xattr_path,
                                                          system_xattr_value)),
                              *rsp.getWriteUnits());
                }
            }
        }
    }

    void waitForPersistence(MemcachedConnection* c = nullptr) {
        MemcachedConnection& connection = c ? *c : *conn;
        size_t ep_queue_size;
        do {
            using namespace std::string_view_literals;
            connection.stats([&ep_queue_size](auto k, auto v) {
                if (k == "ep_queue_size"sv) {
                    ep_queue_size = std::stoi(v);
                }
            });
            if (ep_queue_size) {
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        } while (ep_queue_size != 0);
    }

    std::string getStatForKey(std::string_view key,
                              MemcachedConnection* c = nullptr) {
        MemcachedConnection& connection = c ? *c : *conn;
        std::string value;
        connection.stats([&key, &value](auto k, auto v) {
            if (k == key) {
                value = v;
            }
        });
        return value;
    }

    void testWithMeta(cb::mcbp::ClientOpcode opcode, DocKey id);
    void testReturnMeta(cb::mcbp::request::ReturnMetaType type,
                        DocKey id,
                        bool success);

    void testRangeScan(bool keyOnly);

    std::unique_ptr<MemcachedConnection> conn;
};

void MeteringTest::upsert(DocKey id,
                          std::string value,
                          std::string xattr_path,
                          std::string xattr_value,
                          bool wait_for_persistence,
                          std::string second_xattr_path,
                          std::string second_xattr_value) {
    if (xattr_path.empty()) {
        Document doc;
        doc.info.id = std::string{id};
        doc.value = std::move(value);
        if (wait_for_persistence) {
            conn->mutate(doc, Vbid{0}, MutationType::Set, []() {
                FrameInfoVector ret;
                ret.emplace_back(std::make_unique<DurabilityFrameInfo>(
                        cb::durability::Level::MajorityAndPersistOnMaster));
                return ret;
            });
        } else {
            conn->mutate(doc, Vbid{0}, MutationType::Set);
        }
    } else {
        for (int ii = 0; ii < 2; ++ii) {
            BinprotSubdocMultiMutationCommand cmd;
            cmd.setKey(std::string{id});
            cmd.setVBucket(Vbid{0});
            if (ii == 0) {
                cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                SUBDOC_FLAG_XATTR_PATH,
                                xattr_path,
                                xattr_value);
                cmd.addMutation(cb::mcbp::ClientOpcode::Set,
                                SUBDOC_FLAG_NONE,
                                "",
                                value);
                cmd.addDocFlag(cb::mcbp::subdoc::doc_flag::Mkdoc);
            } else {
                cmd.addMutation(cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                SUBDOC_FLAG_XATTR_PATH,
                                second_xattr_path,
                                second_xattr_value);
            }
            DurabilityFrameInfo fi(
                    cb::durability::Level::MajorityAndPersistOnMaster);
            if (wait_for_persistence) {
                cmd.addFrameInfo(fi);
            }
            auto rsp = conn->execute(cmd);
            if (!rsp.isSuccess()) {
                throw ConnectionError("Subdoc failed", rsp);
            }
            if (second_xattr_path.empty()) {
                return;
            }
        }
    }
}

/// Verify that the unmetered privilege allows to execute commands
/// were its usage isn't being metered.
TEST_P(MeteringTest, UnmeteredPrivilege) {
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("metering");
    admin->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);

    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");

    Document doc;
    doc.info.id = "UnmeteredPrivilege";
    doc.value = "This is the value";
    admin->mutate(doc, Vbid{0}, MutationType::Set);
    admin->get("UnmeteredPrivilege", Vbid{0});

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");

    EXPECT_EQ(before["ru"].get<std::size_t>(), after["ru"].get<std::size_t>());
    EXPECT_EQ(before["wu"].get<std::size_t>(), after["wu"].get<std::size_t>());
    EXPECT_EQ(before["num_commands_with_metered_units"].get<std::size_t>(),
              after["num_commands_with_metered_units"].get<std::size_t>());

    // Drop the privilege and verify that the counters increase
    admin->dropPrivilege(cb::rbac::Privilege::Unmetered);
    admin->get("UnmeteredPrivilege", Vbid{0});
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");

    EXPECT_EQ(1,
              after["ru"].get<std::size_t>() - before["ru"].get<std::size_t>());
    EXPECT_EQ(before["wu"].get<std::size_t>(), after["wu"].get<std::size_t>());
    EXPECT_EQ(1,
              after["num_commands_with_metered_units"].get<std::size_t>() -
                      before["num_commands_with_metered_units"]
                              .get<std::size_t>());
}

/// Test that we meter all operations according to their spec (well, there
/// is no spec at the moment ;)
///
/// To make sure that we don't sneak in a new opcode without considering if
/// it should be metered or not the code loops over all available opcodes
/// and call a function which performs a switch (so the compiler will barf
/// out if we don't handle the case). By doing so one must explicitly think
/// if the new opcode needs to be metered or not.
TEST_P(MeteringTest, OpsMetered) {
    using namespace cb::mcbp;
    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->dropPrivilege(cb::rbac::Privilege::Unmetered);

    auto executeWithExpectedCU = [&admin](std::function<void()> func,
                                          size_t ru,
                                          size_t wu) {
        nlohmann::json before;
        admin->stats([&before](auto k,
                               auto v) { before = nlohmann::json::parse(v); },
                     "bucket_details metering");
        func();
        nlohmann::json after;
        admin->stats(
                [&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");
        EXPECT_EQ(ru, after["ru"].get<size_t>() - before["ru"].get<size_t>());
        EXPECT_EQ(wu, after["wu"].get<size_t>() - before["wu"].get<size_t>());
    };

    auto testOpcode = [&executeWithExpectedCU](MemcachedConnection& conn,
                                               ClientOpcode opcode) {
        auto createDocument = [&conn](std::string key,
                                      std::string value,
                                      MutationType op = MutationType::Set,
                                      uint64_t cas = 0) {
            Document doc;
            doc.info.id = std::move(key);
            doc.info.cas = cas;
            doc.value = std::move(value);
            return conn.mutate(doc, Vbid{0}, op);
        };

        BinprotResponse rsp;
        switch (opcode) {
        case ClientOpcode::Flush:
        case ClientOpcode::Quitq:
        case ClientOpcode::Flushq:
        case ClientOpcode::Getq:
        case ClientOpcode::Getk:
        case ClientOpcode::Getkq:
        case ClientOpcode::Gatq:
        case ClientOpcode::Deleteq:
        case ClientOpcode::Incrementq:
        case ClientOpcode::Decrementq:
        case ClientOpcode::Setq:
        case ClientOpcode::Addq:
        case ClientOpcode::Replaceq:
        case ClientOpcode::Appendq:
        case ClientOpcode::Prependq:
        case ClientOpcode::GetqMeta:
        case ClientOpcode::SetqWithMeta:
        case ClientOpcode::AddqWithMeta:
        case ClientOpcode::DelqWithMeta:
        case ClientOpcode::Rget_Unsupported:
        case ClientOpcode::Rset_Unsupported:
        case ClientOpcode::Rsetq_Unsupported:
        case ClientOpcode::Rappend_Unsupported:
        case ClientOpcode::Rappendq_Unsupported:
        case ClientOpcode::Rprepend_Unsupported:
        case ClientOpcode::Rprependq_Unsupported:
        case ClientOpcode::Rdelete_Unsupported:
        case ClientOpcode::Rdeleteq_Unsupported:
        case ClientOpcode::Rincr_Unsupported:
        case ClientOpcode::Rincrq_Unsupported:
        case ClientOpcode::Rdecr_Unsupported:
        case ClientOpcode::Rdecrq_Unsupported:
        case ClientOpcode::TapConnect_Unsupported:
        case ClientOpcode::TapMutation_Unsupported:
        case ClientOpcode::TapDelete_Unsupported:
        case ClientOpcode::TapFlush_Unsupported:
        case ClientOpcode::TapOpaque_Unsupported:
        case ClientOpcode::TapVbucketSet_Unsupported:
        case ClientOpcode::TapCheckpointStart_Unsupported:
        case ClientOpcode::TapCheckpointEnd_Unsupported:
        case ClientOpcode::ResetReplicationChain_Unsupported:
        case ClientOpcode::SnapshotVbStates_Unsupported:
        case ClientOpcode::VbucketBatchCount_Unsupported:
        case ClientOpcode::NotifyVbucketUpdate_Unsupported:
        case ClientOpcode::ChangeVbFilter_Unsupported:
        case ClientOpcode::CheckpointPersistence_Unsupported:
        case ClientOpcode::CreateCheckpoint_Unsupported:
        case ClientOpcode::LastClosedCheckpoint_Unsupported:
        case ClientOpcode::SetDriftCounterState_Unsupported:
        case ClientOpcode::GetAdjustedTime_Unsupported:
        case ClientOpcode::DcpFlush_Unsupported:
        case ClientOpcode::DeregisterTapClient_Unsupported:
            // Just verify that we don't support them
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_EQ(Status::NotSupported, rsp.getStatus()) << opcode;

        case ClientOpcode::SaslListMechs:
        case ClientOpcode::SaslAuth:
        case ClientOpcode::SaslStep:
            // SASL commands aren't being metered (and not necessairly bound
            // to a bucket so its hard to check as we don't know where it'll
            // go).
            break;

        case ClientOpcode::CreateBucket:
        case ClientOpcode::DeleteBucket:
        case ClientOpcode::PauseBucket:
        case ClientOpcode::ResumeBucket:
            // These are management commands which should only be called
            // on a connection with the "unmetered" interface anyway.
            // Like the SASL commands we don't know which bucket the data
            // would go so it's a bit hard to test ;)
        case ClientOpcode::SelectBucket:
            // Select bucket flip the bucket and isnt' being metered
            // anyway so we don't really need a unit test for it
            break;

        case ClientOpcode::Quit:
            // Quit close the connection so its hard to test (and it would
            // be weird if someone updated the code to start collecting data).
            // Don't test it as it would needs a "wait" in order for the
            // stats to be updated as we could be connecting on another
            // front end thread when running the stat and it would then
            // race the thread potentially updating the stats
            break;

        case ClientOpcode::ListBuckets:
        case ClientOpcode::Version:
        case ClientOpcode::Noop:
        case ClientOpcode::GetClusterConfig:
        case ClientOpcode::GetFailoverLog:
        case ClientOpcode::CollectionsGetManifest:
            // The commands above don't carry any extra information
            // in the request and aren't subject to metering.. just
            // verify that..
            rsp = conn.execute(BinprotGenericCommand{opcode});
            EXPECT_TRUE(rsp.isSuccess()) << opcode;
            EXPECT_FALSE(rsp.getReadUnits()) << opcode;
            EXPECT_FALSE(rsp.getWriteUnits()) << opcode;
            break;

        case ClientOpcode::Stat:
            executeWithExpectedCU([&conn]() { conn.stats(""); }, 0, 0);
            break;
        case ClientOpcode::Verbosity:
            rsp = conn.execute(BinprotVerbosityCommand{0});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;
        case ClientOpcode::Hello:
            executeWithExpectedCU(
                    [&conn]() {
                        conn.setFeature(Feature::AltRequestSupport, true);
                    },
                    0,
                    0);
            break;

        case ClientOpcode::ObserveSeqno:
            do {
                uint64_t uuid = 0;
                conn.stats(
                        [&uuid](auto k, auto v) {
                            if (k == "vb_0:uuid") {
                                uuid = std::stoull(v);
                            }
                        },
                        "vbucket-details 0");
                rsp = conn.execute(BinprotObserveSeqnoCommand{Vbid{0}, uuid});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                EXPECT_FALSE(rsp.getWriteUnits());
            } while (false);
            break;
        case ClientOpcode::Observe:
            do {
                createDocument("ClientOpcode::Observe", "myvalue");
                std::vector<std::pair<Vbid, std::string>> keys;
                keys.emplace_back(std::make_pair<Vbid, std::string>(
                        Vbid{0}, "ClientOpcode::Observe"));
                rsp = conn.execute(BinprotObserveCommand{std::move(keys)});
                EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
                EXPECT_FALSE(rsp.getReadUnits());
                EXPECT_FALSE(rsp.getWriteUnits());
            } while (false);
            break;

        case ClientOpcode::Set:
        case ClientOpcode::Add:
        case ClientOpcode::Replace:
        case ClientOpcode::Append:
        case ClientOpcode::Prepend:
        case ClientOpcode::Delete:
        case ClientOpcode::Increment:
        case ClientOpcode::Decrement:
        case ClientOpcode::Touch:
        case ClientOpcode::Gat:
        case ClientOpcode::Get:
        case ClientOpcode::GetReplica:
        case ClientOpcode::GetLocked:
        case ClientOpcode::UnlockKey:
        case ClientOpcode::GetMeta:
        case ClientOpcode::GetRandomKey:
        case ClientOpcode::GetKeys:
        case ClientOpcode::SubdocGet:
        case ClientOpcode::SubdocExists:
        case ClientOpcode::SubdocDictAdd:
        case ClientOpcode::SubdocDictUpsert:
        case ClientOpcode::SubdocDelete:
        case ClientOpcode::SubdocReplace:
        case ClientOpcode::SubdocArrayPushLast:
        case ClientOpcode::SubdocArrayPushFirst:
        case ClientOpcode::SubdocArrayAddUnique:
        case ClientOpcode::SubdocArrayInsert:
        case ClientOpcode::SubdocCounter:
        case ClientOpcode::SubdocGetCount:
        case ClientOpcode::SubdocMultiLookup:
        case ClientOpcode::SubdocMultiMutation:
        case ClientOpcode::SubdocReplaceBodyWithXattr:
            // Tested in its own unit test
            break;

        case ClientOpcode::GetCmdTimer:
            rsp = conn.execute(
                    BinprotGetCmdTimerCommand{"metering", ClientOpcode::Noop});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;

        case ClientOpcode::GetErrorMap:
            rsp = conn.execute(BinprotGetErrorMapCommand{});
            EXPECT_TRUE(rsp.isSuccess());
            EXPECT_FALSE(rsp.getReadUnits());
            EXPECT_FALSE(rsp.getWriteUnits());
            break;

        case ClientOpcode::SetWithMeta:
        case ClientOpcode::AddWithMeta:
        case ClientOpcode::DelWithMeta:
        case ClientOpcode::ReturnMeta:
            // Tested in its own unit test;
            break;

        case ClientOpcode::SeqnoPersistence:
        case ClientOpcode::CollectionsGetID:
        case ClientOpcode::CollectionsGetScopeID:
            // @todo add unit tests!
            break;

        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::RangeScanContinue:
        case ClientOpcode::RangeScanCancel:
            // tested in MeteringTest::testRangeScan
            break;

        case ClientOpcode::DcpOpen:
        case ClientOpcode::DcpAddStream:
        case ClientOpcode::DcpCloseStream:
        case ClientOpcode::DcpStreamReq:
        case ClientOpcode::DcpGetFailoverLog:
        case ClientOpcode::DcpStreamEnd:
        case ClientOpcode::DcpSnapshotMarker:
        case ClientOpcode::DcpMutation:
        case ClientOpcode::DcpDeletion:
        case ClientOpcode::DcpExpiration:
        case ClientOpcode::DcpSetVbucketState:
        case ClientOpcode::DcpNoop:
        case ClientOpcode::DcpBufferAcknowledgement:
        case ClientOpcode::DcpControl:
        case ClientOpcode::DcpSystemEvent:
        case ClientOpcode::DcpPrepare:
        case ClientOpcode::DcpSeqnoAcknowledged:
        case ClientOpcode::DcpCommit:
        case ClientOpcode::DcpAbort:
        case ClientOpcode::DcpSeqnoAdvanced:
        case ClientOpcode::DcpOsoSnapshot:
            // tested in dcp_metering_test.cc
            break;

        // The following are "internal"/advanced commands not intended
        // for the average users. We may add unit tests at a later time for
        // them
        case ClientOpcode::IoctlGet:
        case ClientOpcode::IoctlSet:
        case ClientOpcode::ConfigValidate:
        case ClientOpcode::ConfigReload:
        case ClientOpcode::AuditPut:
        case ClientOpcode::AuditConfigReload:
        case ClientOpcode::Shutdown:
        case ClientOpcode::SetBucketThrottleProperties:
        case ClientOpcode::SetBucketDataLimitExceeded:
        case ClientOpcode::SetNodeThrottleProperties:
        case ClientOpcode::SetVbucket:
        case ClientOpcode::GetVbucket:
        case ClientOpcode::DelVbucket:
        case ClientOpcode::GetAllVbSeqnos:
        case ClientOpcode::StopPersistence:
        case ClientOpcode::StartPersistence:
        case ClientOpcode::SetParam:
        case ClientOpcode::EnableTraffic:
        case ClientOpcode::DisableTraffic:
        case ClientOpcode::Ifconfig:
        case ClientOpcode::CompactDb:
        case ClientOpcode::SetClusterConfig:
        case ClientOpcode::CollectionsSetManifest:
        case ClientOpcode::EvictKey:
        case ClientOpcode::Scrub:
        case ClientOpcode::IsaslRefresh:
        case ClientOpcode::SslCertsRefresh:
        case ClientOpcode::SetCtrlToken:
        case ClientOpcode::GetCtrlToken:
        case ClientOpcode::UpdateExternalUserPermissions:
        case ClientOpcode::RbacRefresh:
        case ClientOpcode::AuthProvider:
        case ClientOpcode::DropPrivilege:
        case ClientOpcode::AdjustTimeofday:
        case ClientOpcode::EwouldblockCtl:
        case ClientOpcode::Invalid:
            break;
        }
    };

    auto connection = cluster->getConnection(0);
    connection->authenticate("@admin", "password");
    connection->selectBucket("metering");
    connection->dropPrivilege(cb::rbac::Privilege::Unmetered);
    connection->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
    connection->setReadTimeout(std::chrono::seconds{3});

    for (int ii = 0; ii < 0x100; ++ii) {
        auto opcode = ClientOpcode(ii);
        if (is_valid_opcode(opcode)) {
            testOpcode(*connection, opcode);
        }
    }
}

TEST_P(MeteringTest, IncrBadValuePlain) {
    testArithmeticBadValue(
            ClientOpcode::Increment,
            StoredDocKey{"IncrBadValuePlain", getTestCollection()},
            getStringValue());
}

TEST_P(MeteringTest, IncrBadValueWithXattr) {
    testArithmeticBadValue(
            ClientOpcode::Increment,
            StoredDocKey{"IncrBadValueWithXattr", getTestCollection()},
            getStringValue(),
            "xattr",
            getStringValue());
}

TEST_P(MeteringTest, DecrBadValuePlain) {
    testArithmeticBadValue(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrBadValuePlain", getTestCollection()},
            getStringValue());
}

TEST_P(MeteringTest, DecrBadValueWithXattr) {
    testArithmeticBadValue(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrBadValueWithXattr", getTestCollection()},
            getStringValue(),
            "xattr",
            getStringValue());
}

TEST_P(MeteringTest, IncrCreateValue) {
    testArithmeticCreateValue(
            ClientOpcode::Increment,
            StoredDocKey{"IncrCreateValue", getTestCollection()},
            false);
}

TEST_P(MeteringTest, IncrCreateValue_Durability) {
    testArithmeticCreateValue(
            ClientOpcode::Increment,
            StoredDocKey{"IncrCreateValue_Durability", getTestCollection()},
            true);
}

TEST_P(MeteringTest, DecrCreateValue) {
    testArithmeticCreateValue(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrCreateValue", getTestCollection()},
            false);
}

TEST_P(MeteringTest, DecrCreateValue_Durability) {
    testArithmeticCreateValue(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrCreateValue_Durability", getTestCollection()},
            true);
}

TEST_P(MeteringTest, IncrementPlain) {
    testArithmetic(ClientOpcode::Increment,
                   StoredDocKey{"IncrementPlain", getTestCollection()},
                   {},
                   {},
                   false);
}

TEST_P(MeteringTest, IncrementPlain_Durability) {
    testArithmetic(
            ClientOpcode::Increment,
            StoredDocKey{"IncrementWithXattr_Durability", getTestCollection()},
            {},
            {},
            true);
}

TEST_P(MeteringTest, IncrementWithXattr) {
    testArithmetic(ClientOpcode::Increment,
                   StoredDocKey{"IncrementWithXattr", getTestCollection()},
                   "xattr",
                   getStringValue(),
                   false);
}

TEST_P(MeteringTest, IncrementWithXattr_Durability) {
    testArithmetic(
            ClientOpcode::Increment,
            StoredDocKey{"IncrementWithXattr_Durability", getTestCollection()},
            "xattr",
            getStringValue(),
            true);
}

TEST_P(MeteringTest, DecrementPlain) {
    testArithmetic(ClientOpcode::Decrement,
                   StoredDocKey{"DecrementPlain", getTestCollection()},
                   {},
                   {},
                   false);
}

TEST_P(MeteringTest, DecrementPlain_Durability) {
    testArithmetic(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrementWithXattr_Durability", getTestCollection()},
            {},
            {},
            true);
}

TEST_P(MeteringTest, DecrementWithXattr) {
    testArithmetic(ClientOpcode::Decrement,
                   StoredDocKey{"DecrementWithXattr", getTestCollection()},
                   "xattr",
                   getStringValue(),
                   false);
}

TEST_P(MeteringTest, DecrementWithXattr_Durability) {
    testArithmetic(
            ClientOpcode::Decrement,
            StoredDocKey{"DecrementWithXattr_Durability", getTestCollection()},
            "xattr",
            getStringValue(),
            true);
}

/// Delete of a non-existing document should be free
TEST_P(MeteringTest, DeleteNonexistingItem) {
    const StoredDocKey id{"DeleteNonexistingItem", getTestCollection()};
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete,
                                     std::string{id}};
    auto rsp = conn->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

// Delete of a single document should cost 1WU
TEST_P(MeteringTest, DeletePlain) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeletePlain", getTestCollection()},
               getStringValue(),
               {},
               {},
               {},
               {},
               false);
}

TEST_P(MeteringTest, DeletePlain_Durability) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeletePlain_Durability", getTestCollection()},
               getStringValue(),
               {},
               {},
               {},
               {},
               true);
}

TEST_P(MeteringTest, DeleteUserXattr) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeleteUserXattr", getTestCollection()},
               getStringValue(),
               "xattr",
               getStringValue(),
               {},
               {},
               false);
}

TEST_P(MeteringTest, DeleteUserXattr_Durability) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeleteUserXattr_Durability", getTestCollection()},
               getStringValue(),
               "xattr",
               getStringValue(),
               {},
               {},
               true);
}

TEST_P(MeteringTest, DeleteUserAndSystemXattr) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeleteUserAndSystemXattr", getTestCollection()},
               getStringValue(),
               "xattr",
               getStringValue(),
               "_xattr",
               getStringValue(),
               false);
}

TEST_P(MeteringTest, DeleteUserAndSystemXattr_Durability) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeleteUserAndSystemXattr_Durability",
                            getTestCollection()},
               getStringValue(),
               "xattr",
               getStringValue(),
               "_xattr",
               getStringValue(),
               true);
}

TEST_P(MeteringTest, DeleteSystemXattr) {
    testDelete(ClientOpcode::Delete,
               StoredDocKey{"DeleteSystemXattr", getTestCollection()},
               getStringValue(),
               {},
               {},
               "_xattr",
               getStringValue(),
               false);
}

TEST_P(MeteringTest, DeleteSystemXattr_Durability) {
    testDelete(
            ClientOpcode::Delete,
            StoredDocKey{"DeleteSystemXattr_Durability", getTestCollection()},
            getStringValue(),
            {},
            {},
            "_xattr",
            getStringValue(),
            true);
}

/// Get of a non-existing document should not cost anything
TEST_P(MeteringTest, GetNonExistingDocument) {
    auto rsp = conn->execute(
            BinprotGenericCommand{ClientOpcode::Get, "GetNonExistingDocument"});
    EXPECT_EQ(cb::mcbp::Status::UnknownCollection, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Get of a single document without xattrs costs the size of the document
TEST_P(MeteringTest, GetDocumentPlain) {
    const StoredDocKey id{"GetDocumentPlain", getTestCollection()};
    const auto value = getStringValue();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotGenericCommand{ClientOpcode::Get, std::string{id}});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
    }
}

/// Get of a single document with xattrs costs the size of the document plus
/// the size of the xattrs
TEST_P(MeteringTest, GetDocumentWithXAttr) {
    const StoredDocKey id{"GetDocumentWithXAttr", getTestCollection()};
    const auto value = getStringValue();
    upsert(id, value, "xattr", value);
    auto rsp = conn->execute(
            BinprotGenericCommand{ClientOpcode::Get, std::string{id}});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", value)),
                  *rsp.getReadUnits());
    }
}

/// Get of a non-existing document should not cost anything
TEST_P(MeteringTest, GetReplicaNonExistingDocument) {
    const StoredDocKey id{"GetNonExistingDocument", getTestCollection()};
    auto rsp = getReplicaConn()->execute(
            BinprotGenericCommand{ClientOpcode::GetReplica, std::string{id}});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Get of a single document without xattrs costs the size of the document
TEST_P(MeteringTest, GetReplicaDocumentPlain) {
    const StoredDocKey id{"GetDocumentPlain", getTestCollection()};
    const auto value = getStringValue();
    upsert(id, value);

    auto rconn = getReplicaConn();
    BinprotResponse rsp;
    do {
        rsp = rconn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetReplica, std::string{id}});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
    }
}

/// Get of a single document with xattrs costs the size of the document plus
/// the size of the xattrs
TEST_P(MeteringTest, GetReplicaDocumentWithXAttr) {
    const StoredDocKey id{"GetDocumentWithXAttr", getTestCollection()};
    const auto value = getStringValue();
    upsert(id, value, "xattr", value);

    auto rconn = getReplicaConn();
    BinprotResponse rsp;
    do {
        rsp = rconn->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetReplica, std::string{id}});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);

    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", value)),
                  *rsp.getReadUnits());
    }
}

TEST_P(MeteringTest, MeterDocumentLocking) {
    auto& sconfig = cb::serverless::Config::instance();

    const StoredDocKey id{"MeterDocumentLocking/" + to_string(GetParam()),
                          getTestCollection()};
    const auto getl = BinprotGetAndLockCommand{std::string{id}};
    auto rsp = conn->execute(getl);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    std::string document_value;
    document_value.resize(sconfig.readUnitSize - 5);
    std::fill(document_value.begin(), document_value.end(), 'a');

    upsert(id, document_value);
    rsp = conn->execute(getl);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
                  *rsp.getReadUnits());
    }

    auto unl = BinprotUnlockCommand{std::string{id}, Vbid{0}, rsp.getCas()};
    rsp = conn->execute(unl);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

TEST_P(MeteringTest, MeterDocumentTouch) {
    auto& sconfig = cb::serverless::Config::instance();
    const StoredDocKey id{"MeterDocumentTouch/" + to_string(GetParam()),
                          getTestCollection()};

    // Touch of non-existing document should fail and is free
    auto rsp = conn->execute(BinprotTouchCommand{std::string{id}, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Gat should fail and free
    rsp = conn->execute(BinprotGetAndTouchCommand{std::string{id}, Vbid{0}, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    std::string document_value;
    document_value.resize(sconfig.readUnitSize - 5);
    std::fill(document_value.begin(), document_value.end(), 'a');
    upsert(id, document_value);

    // Touch of a document is a full read and write of the document on the
    // server, but no data returned
    rsp = conn->execute(BinprotTouchCommand{std::string{id}, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
                  *rsp.getWriteUnits());
    }
    EXPECT_TRUE(rsp.getDataString().empty());

    rsp = conn->execute(BinprotGetAndTouchCommand{std::string{id}, Vbid{0}, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
                  *rsp.getWriteUnits());
    }
    EXPECT_EQ(document_value, rsp.getDataString());
}

TEST_P(MeteringTest, MeterDocumentSimpleMutations) {
    auto& sconfig = cb::serverless::Config::instance();

    const StoredDocKey id{"MeterDocumentSimpleMutations", getTestCollection()};
    std::string document_value;
    std::string xattr_path = "xattr";
    std::string xattr_value;
    document_value.resize(sconfig.readUnitSize - 10);
    std::fill(document_value.begin(), document_value.end(), 'a');
    xattr_value.resize(sconfig.readUnitSize - 10);
    std::fill(xattr_value.begin(), xattr_value.end(), 'a');
    xattr_value.front() = '"';
    xattr_value.back() = '"';

    BinprotMutationCommand command;
    command.setKey(std::string{id});
    command.addValueBuffer(document_value);

    // Set of an nonexistent document shouldn't cost any RUs and the
    // size of the new document's WUs
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
                  *rsp.getWriteUnits());
    }
    // Using Set on an existing document is a replace and will be tested
    // later on.

    // Add of an existing document should fail, and cost 1RU to read the
    // metadata
    command.setMutationType(MutationType::Add);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
    // @todo it currently don't cost an RU - fix this
    EXPECT_FALSE(rsp.getReadUnits());
    //    ASSERT_TRUE(rsp.getReadUnits());
    //    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
    //    rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    // Add of a new document should cost the same as a set (no read, just write)
    conn->remove(std::string{id}, Vbid{0});
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
                  *rsp.getWriteUnits());
    }

    // Replace of the document should cost 1 ru (for the metadata read)
    // then X WUs
    command.setMutationType(MutationType::Replace);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    // @todo it currently don't cost the 1 ru for the metadata read!
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
                  *rsp.getWriteUnits());
    }

    // But if we try to replace a document containing XATTRs we would
    // need to read the full document in order to replace, and it should
    // cost the size of the full size of the old document and the new one
    // (containing the xattrs)
    upsert(id, document_value, xattr_path, xattr_value);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() +
                                xattr_path.size() + xattr_value.size()),
                  *rsp.getWriteUnits());
    }

    // Trying to replace a document with incorrect CAS should cost 1 RU and
    // no WU
    command.setCas(1);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
    command.setCas(0);

    // Trying to replace a nonexisting document should not cost anything
    conn->remove(std::string{id}, Vbid{0});
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::NotStored, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::NotStored, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();

    upsert(id, document_value);
    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
                  *rsp.getWriteUnits());
    }

    upsert(id, document_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
                  *rsp.getWriteUnits());
    }
    // And if we have XATTRs they should be copied as well
    upsert(id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                                xattr_path.size() + xattr_value.size()),
                  *rsp.getWriteUnits());
    }
    upsert(id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                                xattr_path.size() + xattr_value.size()),
                  *rsp.getWriteUnits());
    }
    // @todo add test cases for durability
}

TEST_P(MeteringTest, MeterGetRandomKey) {
    // Random key needs at least one key to be stored in the bucket so that
    // it may return the key. To make sure that the unit tests doesn't depend
    // on other tests lets store a document in vbucket 0.
    // However GetRandomKey needs to check the collection item count, which only
    // updates when we flush a committed item, yet a durable write is successful
    // once all pending writes are "in-place" - thus GetRandomKey could race
    // with the flush of a commit, to get around that we can store twice, this
    // single connection will then ensure a non zero item count is observed by
    // GetRandomKey
    upsert(StoredDocKey{"MeterGetRandomKey", getTestCollection()}, "hello");
    upsert(StoredDocKey{"MeterGetRandomKey", getTestCollection()},
           "hello",
           {},
           {},
           true);

    BinprotGenericCommand getRandom{cb::mcbp::ClientOpcode::GetRandomKey};
    mcbp::request::GetRandomKeyPayload payload(
            CollectionIDType{getTestCollection()});
    getRandom.setExtras(payload.getBuffer());
    const auto rsp = conn->execute(getRandom);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_NE(0, *rsp.getReadUnits());
    }
}

TEST_P(MeteringTest, MeterGetKeys) {
    // GetKeys needs at least one key to be stored in the bucket so that
    // it may return the key. To make sure that the unit tests doesn't depend
    // on other tests lets store a document in vbucket 0.
    // However GetKeys needs to check the collection item count, which only
    // updates when we flush a committed item, yet a durable write is successful
    // once all pending writes are "in-place" - thus GetKeys could race
    // with the flush of a commit, to get around that we can store twice, this
    // single connection will then ensure a non zero item count is observed by
    // GetKeys
    upsert(StoredDocKey{"MeterGetKeys", getTestCollection()}, "hello");
    upsert(StoredDocKey{"MeterGetKeys", getTestCollection()},
           "hello",
           {},
           {},
           true);

    const auto rsp = conn->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::GetKeys,
            DocKey::makeWireEncodedString(getTestCollection(), {"\0", 1})});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getData().empty());
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits()) << rsp.getDataString();
        // Depending on how many keys we've got in the database..
        EXPECT_LE(1, *rsp.getReadUnits());
    }
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_P(MeteringTest, GetMetaNonexistentDocument) {
    // Verify cost of nonexistent value
    const StoredDocKey id{"ClientOpcode::GetMeta", getTestCollection()};
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta,
                                           std::string{id}};
    auto rsp = conn->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_P(MeteringTest, GetMetaPlainDocument) {
    const StoredDocKey id{"ClientOpcode::GetMeta", getTestCollection()};
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta,
                                           std::string{id}};
    upsert(id, getStringValue());
    const auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(1, *rsp.getReadUnits());
    }
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_P(MeteringTest, GetMetaDocumentWithXattr) {
    const StoredDocKey id{"ClientOpcode::GetMeta", getTestCollection()};
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta,
                                           std::string{id}};
    upsert(id, getStringValue(), "xattr", getStringValue());
    const auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(1, *rsp.getReadUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocGetNoSuchPath) {
    const StoredDocKey id{"SubdocGetENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    ASSERT_LT(1, to_ru(calculateDocumentSize(id, value)));
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGet, std::string{id}, "hello"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Subdoc get should cost the entire doc read; and not just the returned
/// path
TEST_P(MeteringTest, SubdocGet) {
    const StoredDocKey id{"SubdocGet", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGet, std::string{id}, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    }
}

/// Subdoc get should cost the entire doc read (including xattr); and not just
//  the returned path
TEST_P(MeteringTest, SubdocGetWithXattr) {
    const StoredDocKey id{"SubdocGetXattr", getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGet, std::string{id}, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocExistsNoSuchPath) {
    const StoredDocKey id{"SubdocExistsNoSuchPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, std::string{id}, "hello"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Subdoc exists should cost the entire doc read
TEST_P(MeteringTest, SubdocExistsPlainDoc) {
    const StoredDocKey id{"SubdocExistsPlainDoc", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, std::string{id}, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    }
}

/// Subdoc exists should cost the entire doc read (that include xattrs)
TEST_P(MeteringTest, SubdocExistsWithXattr) {
    const StoredDocKey id{"SubdocExistsWithXattr", getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, std::string{id}, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocDictAddEExist) {
    const StoredDocKey id{"SubdocDictAddEExist", getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocDictAdd,
                                 std::string{id},
                                 "v1",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Dict add should cost RU for the document, and WUs for the new document
TEST_P(MeteringTest, SubdocDictAddPlainDoc) {
    const StoredDocKey id{"SubdocDictAddPlainDoc", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto v3 = getStringValue();
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDictAdd, std::string{id}, "v3", v3});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + value.size() + v3.size()),
                  *rsp.getWriteUnits());
    }
}

/// Dict add should cost RU for the document, and 2x WUs for the new document
TEST_P(MeteringTest, SubdocDictAddPlainDoc_Durability) {
    const StoredDocKey id{"SubdocDictAddPlainDoc_Durability",
                          getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto v3 = getStringValue();
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDictAdd, std::string{id}, "v3", v3};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + value.size() + v3.size()) * 2,
                  *rsp.getWriteUnits());
    }
}

/// Dict add should cost RU for the document, and WUs for the new document
/// (including the XAttrs copied over)
TEST_P(MeteringTest, SubdocDictAddPlainDocWithXattr) {
    const StoredDocKey id{"SubdocDictAddPlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    auto value = json.dump();
    auto v3 = getStringValue();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDictAdd, std::string{id}, "v3", v3});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v3"] = v3;
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// Dict add should cost RU for the document, and 2x WUs for the new document
/// (including the XAttrs copied over)
TEST_P(MeteringTest, SubdocDictAddPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocDictAddPlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    auto value = json.dump();
    auto v3 = getStringValue();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDictAdd, std::string{id}, "v3", v3};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v3"] = v3;
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// Dict Upsert should cost RU for the document, and WUs for the new document
TEST_P(MeteringTest, SubdocDictUpsertPlainDoc) {
    const StoredDocKey id{"SubdocDictUpsertPlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                 std::string{id},
                                 "v1",
                                 R"("this is the new value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v1"] = "this is the new value";
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
                  *rsp.getWriteUnits());
    }
}

/// Dict Upsert should cost RU for the document, and 2x WUs for the new document
TEST_P(MeteringTest, SubdocDictUpsertPlainDoc_Durability) {
    const StoredDocKey id{"SubdocDictUpsertPlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             std::string{id},

                             "v1",
                             R"("this is the new value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v1"] = "this is the new value";
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
                  *rsp.getWriteUnits());
    }
}

/// Dict Upsert should cost RU for the document, and WUs for the new document
/// (including the XAttrs copied over)
TEST_P(MeteringTest, SubdocDictUpsertPlainDocWithXattr) {
    const StoredDocKey id{"SubdocDictAddPlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                 std::string{id},
                                 "v1",
                                 R"("this is the new value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v1"] = "this is the new value";
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// Dict Upsert should cost RU for the document, and 2x WUs for the new document
/// (including the XAttrs copied over)
TEST_P(MeteringTest, SubdocDictUpsertPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocDictAddPlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             std::string{id},
                             "v1",
                             R"("this is the new value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["v1"] = "this is the new value";
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocDeleteENoPath) {
    const StoredDocKey id{"SubdocDeleteENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, std::string{id}, "ENOPATH"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Delete should cost the full read, and the write of the full size of the
/// new document
TEST_P(MeteringTest, SubdocDeletePlainDoc) {
    const StoredDocKey id{"SubdocDeletePlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, std::string{id}, "fill"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json.erase("fill");
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
                  *rsp.getWriteUnits());
    }
}

/// Delete should cost the full read, and the write of the full size of the
/// new document
TEST_P(MeteringTest, SubdocDeletePlainDoc_Durability) {
    const StoredDocKey id{"SubdocDeletePlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDelete, std::string{id}, "fill"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json.erase("fill");
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
                  *rsp.getWriteUnits());
    }
}

/// Delete should cost the full read (including xattrs), and the write of the
/// full size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocDeletePlainDocWithXattr) {
    const StoredDocKey id{"SubdocDeletePlainDocWithXattr", getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, std::string{id}, "fill"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json.erase("fill");
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// Delete should cost the full read (including xattrs), and the write 2x of the
/// full size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocDeletePlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocDeletePlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDelete, std::string{id}, "fill"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json.erase("fill");
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocReplaceENoPath) {
    const StoredDocKey id{"SubdocReplaceENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocReplace,
                                 std::string{id},
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Replace should cost the read of the document, and the write of the
/// new document
TEST_P(MeteringTest, SubdocReplacePlainDoc) {
    const StoredDocKey id{"SubdocReplacePlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocReplace,
                                 std::string{id},
                                 "fill",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        json["fill"] = true;

        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
                  *rsp.getWriteUnits());
    }
}

/// Replace should cost the read of the document, and the write 2x of the
/// new document
TEST_P(MeteringTest, SubdocReplacePlainDoc_Durability) {
    const StoredDocKey id{"SubdocReplacePlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocReplace,
                             std::string{id},
                             "fill",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["fill"] = true;
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
                  *rsp.getWriteUnits());
    }
}

/// Replace should cost the full read (including xattrs), and the write of the
/// full size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocReplacePlainDocWithXattr) {
    const StoredDocKey id{"SubdocReplacePlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocReplace,
                                 std::string{id},
                                 "fill",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["fill"] = true;
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// Replace should cost the full read (including xattrs), and the write 2x of
/// the full size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocReplacePlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocReplacePlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocReplace,
                             std::string{id},
                             "fill",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["fill"] = true;
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocCounterENoCounter) {
    const StoredDocKey id{"SubdocCounterENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocCounter,
                                 std::string{id},
                                 "array",
                                 "1"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// Counter should cost the read of the full document and the write of
/// the new document
TEST_P(MeteringTest, SubdocCounterPlainDoc) {
    const StoredDocKey id{"SubdocCounterPlainDoc", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocCounter,
                                 std::string{id},
                                 "counter",
                                 "1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, value)),
                  *rsp.getWriteUnits());
    }
}

/// Counter should cost the read of the full document and the write 2x of
/// the new document
TEST_P(MeteringTest, SubdocCounterPlainDoc_Durability) {
    const StoredDocKey id{"SubdocCounterPlainDoc_Durability",
                          getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocCounter,
                             std::string{id},
                             "counter",
                             "1"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, value)) * 2,
                  *rsp.getWriteUnits());
    }
}

/// Counter should cost the read of the full document including xattr and
/// write of the new document with the xattrs copied over
TEST_P(MeteringTest, SubdocCounterPlainDocWithXattr) {
    const StoredDocKey id{"SubdocCounterPlainDocWithXattr",
                          getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocCounter,
                                 std::string{id},
                                 "counter",
                                 "1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, value, "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// Counter should cost the read of the full document including xattr and
/// write 2x of the new document with the xattrs copied over
TEST_P(MeteringTest, SubdocCounterPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocCounterPlainDocWithXattr_Durability",
                          getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocCounter,
                             std::string{id},
                             "counter",
                             "1"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, value, "xattr", xattr)) * 2,
                  *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocGetCountENoPath) {
    const StoredDocKey id{"SubdocGetCountENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocGetCount,
                                 std::string{id},
                                 "ENOPATH"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocGetCountENotArray) {
    const StoredDocKey id{"SubdocGetCountENotArray", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, std::string{id}, "v1"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// GetCount should cost the read of the entire document
TEST_P(MeteringTest, SubdocGetCountPlainDoc) {
    const StoredDocKey id{"SubdocGetCountPlainDoc", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, std::string{id}, "array"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    }
}

/// GetCount should cost the read of the entire document including xattrs
TEST_P(MeteringTest, SubdocGetCountPlainDocWithXattr) {
    const StoredDocKey id{"SubdocGetCountPlainDocWithXattr",
                          getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, std::string{id}, "array"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayPushLastENoPath) {
    const StoredDocKey id{"SubdocArrayPushLastENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 std::string{id},
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayPushLastENotArray) {
    const StoredDocKey id{"SubdocArrayPushLastENotArray", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 std::string{id},
                                 "counter",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// ArrayPushLast should cost the read of the document, and then the
/// write of the new document
TEST_P(MeteringTest, SubdocArrayPushLastPlainDoc) {
    const StoredDocKey id{"SubdocArrayPushLastPlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 std::string{id},
                                 "array",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back(true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + json.dump().size()), *rsp.getWriteUnits());
    }
}

/// ArrayPushLast should cost the read of the document, and then 2x the
/// write of the new document
TEST_P(MeteringTest, SubdocArrayPushLastPlainDoc_Durability) {
    const StoredDocKey id{"SubdocArrayPushLastPlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                             std::string{id},
                             "array",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back(true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + json.dump().size()) * 2,
                  *rsp.getWriteUnits());
    }
}

/// ArrayPushLast should cost the read of the document, and then the
/// write of the new document (including xattrs copied over)
TEST_P(MeteringTest, SubdocArrayPushLastPlainDocWithXattr) {
    const StoredDocKey id{"SubdocArrayPushLastPlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 std::string{id},
                                 "array",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back(true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// ArrayPushLast should cost the read of the document, and then 2x the
/// write of the new document (including xattrs copied over)
TEST_P(MeteringTest, SubdocArrayPushLastPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocArrayPushLastPlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                             std::string{id},
                             "array",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back(true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayPushFirstENoPath) {
    const StoredDocKey id{"SubdocArrayPushFirstENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 std::string{id},
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayPushFirstENotArray) {
    const StoredDocKey id{"SubdocArrayPushFirstENotArray", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 std::string{id},
                                 "counter",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// ArrayPushFirst should cost the read of the document, and then the
/// write of the new document
TEST_P(MeteringTest, SubdocArrayPushFirstPlainDoc) {
    const StoredDocKey id{"SubdocArrayPushFirstPlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 std::string{id},
                                 "array",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + json.dump().size()), *rsp.getWriteUnits());
    }
}

/// ArrayPushFirst should cost the read of the document, and then 2x the
/// write of the new document
TEST_P(MeteringTest, SubdocArrayPushFirstPlainDoc_Durability) {
    const StoredDocKey id{"SubdocArrayPushFirstPlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                             std::string{id},
                             "array",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(id.size() + json.dump().size()) * 2,
                  *rsp.getWriteUnits());
    }
}

/// ArrayPushFirst should cost the read of the document, and then the
/// write of the new document (including xattrs copied over)
TEST_P(MeteringTest, SubdocArrayPushFirstPlainDocWithXattr) {
    const StoredDocKey id{"SubdocArrayPushFirstPlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 std::string{id},
                                 "array",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// ArrayPushFirst should cost the read of the document, and then 2x the
/// write of the new document (including xattrs copied over)
TEST_P(MeteringTest, SubdocArrayPushFirstPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocArrayPushFirstPlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                             std::string{id},
                             "array",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayAddUniqueENoPath) {
    const StoredDocKey id{"SubdocArrayAddUniqueENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 std::string{id},
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayAddUniqueENotArray) {
    const StoredDocKey id{"SubdocArrayAddUniqueENotArray", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 std::string{id},
                                 "v1",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayAddUniqueEExists) {
    const StoredDocKey id{"SubdocArrayAddUniqueEExists", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 std::string{id},
                                 "array",
                                 R"("1")"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document
TEST_P(MeteringTest, SubdocArrayAddUniquePlainDoc) {
    const StoredDocKey id{"SubdocArrayAddUniquePlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 std::string{id},
                                 "array",
                                 R"("Unique value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back("Unique value");
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
                  *rsp.getWriteUnits());
    }
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document
TEST_P(MeteringTest, SubdocArrayAddUniquePlainDoc_Durability) {
    const StoredDocKey id{"SubdocArrayAddUniquePlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                             std::string{id},
                             "array",
                             R"("Unique value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back("Unique value");
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
                  *rsp.getWriteUnits());
    }
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document including the XATTRs copied over
TEST_P(MeteringTest, SubdocArrayAddUniquePlainDocWithXattr) {
    const StoredDocKey id{"SubdocArrayAddUniquePlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 std::string{id},
                                 "array",
                                 R"("Unique value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back("Unique value");
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// ArrayAddUnique should cost the read of the document, and the write
/// 2x of the size of the new document including the XATTRs copied over
TEST_P(MeteringTest, SubdocArrayAddUniquePlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocArrayAddUniquePlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                             std::string{id},
                             "array",
                             R"("Unique value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].push_back("Unique value");
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayInsertENoPath) {
    const StoredDocKey id{"SubdocArrayInsertENoPath", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 std::string{id},
                                 "ENOPATH.[0]",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// MB-53560: Failing operations should not cost read or write
TEST_P(MeteringTest, SubdocArrayInsertENotArray) {
    const StoredDocKey id{"SubdocArrayInsertENotArray", getTestCollection()};
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 std::string{id},
                                 "v1.[0]",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
}

/// ArrayInsert should cost the read of the document, and the write of
/// the size of the new document
TEST_P(MeteringTest, SubdocArrayInsertPlainDoc) {
    const StoredDocKey id{"SubdocArrayInsertPlainDoc", getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 std::string{id},
                                 "array.[0]",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
                  *rsp.getWriteUnits());
    }
}

/// ArrayInsert should cost the read of the document, and the write 2x of
/// the size of the new document
TEST_P(MeteringTest, SubdocArrayInsertPlainDoc_Durability) {
    const StoredDocKey id{"SubdocArrayInsertPlainDoc_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                             std::string{id},
                             "array.[0]",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
                  *rsp.getWriteUnits());
    }
}

/// ArrayInsert should cost the read of the document, and the write of
/// the size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocArrayInsertPlainDocWithXattr) {
    const StoredDocKey id{"SubdocArrayInsertPlainDocWithXattr",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 std::string{id},
                                 "array.[0]",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// ArrayInsert should cost the read of the document, and the write 2x of
/// the size of the new document (including the xattrs copied over)
TEST_P(MeteringTest, SubdocArrayInsertPlainDocWithXattr_Durability) {
    const StoredDocKey id{"SubdocArrayInsertPlainDocWithXattr_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                             std::string{id},
                             "array.[0]",
                             "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["array"].insert(json["array"].begin(), true);
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// MultiLookup should cost the read of the full document even if no
/// data gets returned
TEST_P(MeteringTest, SubdocMultiLookupAllMiss) {
    const StoredDocKey id{"SubdocMultiLookupAllMiss", getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiLookupCommand{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "missing1"},
             {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "missing2"}},
            cb::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, rsp.getStatus());
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MultiLookup should cost the read of the full document
TEST_P(MeteringTest, SubdocMultiLookup) {
    const StoredDocKey id{"SubdocMultiLookup", getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiLookupCommand{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "array.[0]"},
             {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "counter"}},
            cb::mcbp::subdoc::doc_flag::None});

    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());

        EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MultiMutation should cost the read of the full document even if no
/// updates was made
TEST_P(MeteringTest, SubdocMultiMutationAllFailed) {
    const StoredDocKey id{"SubdocMultiMutationAllFailed", getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo.missing.bar",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo.missing.foo",
              "true"}},
            cb::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, rsp.getStatus());
    EXPECT_FALSE(rsp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getReadUnits());
    } else {
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getReadUnits());
    }
}

/// MultiMutation should cost the read of the full document, and write
/// of the full size (including xattrs copied over)
TEST_P(MeteringTest, SubdocMultiMutation) {
    const StoredDocKey id{"SubdocMultiMutation", getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "bar",
              "true"}},
            cb::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["foo"] = true;
        json["bar"] = true;
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
                  *rsp.getWriteUnits());
    }
}

/// MultiMutation should cost the read of the full document, and write 2x
/// of the full size (including xattrs copied over)
TEST_P(MeteringTest, SubdocMultiMutation_Durability) {
    const StoredDocKey id{"SubdocMultiMutation_Durability",
                          getTestCollection()};
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    BinprotSubdocMultiMutationCommand cmd{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "bar",
              "true"}},
            cb::mcbp::subdoc::doc_flag::None};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);

    auto rsp = conn->execute(cmd);

    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        json["foo"] = true;
        json["bar"] = true;
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(
                to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) *
                        2,
                *rsp.getWriteUnits());
    }
}

/// SubdocReplaceBodyWithXattr should cost the read of the full document,
/// and write of the full size
TEST_P(MeteringTest, SubdocReplaceBodyWithXattr) {
    const StoredDocKey id{"SubdocReplaceBodyWithXattr", getTestCollection()};
    const auto new_value = getJsonDoc().dump();
    const auto old_value = getStringValue(false);
    upsert(id, old_value, "tnx.op.staged", new_value);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}},
             {cb::mcbp::ClientOpcode::SubdocDelete,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}}},
            cb::mcbp::subdoc::doc_flag::None});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, new_value)),
                  *rsp.getWriteUnits());
    }
}

/// SubdocReplaceBodyWithXattr should cost the read of the full
/// document, and write 2x of the full size when durability is enabled
TEST_P(MeteringTest, SubdocReplaceBodyWithXattr_Durability) {
    const StoredDocKey id{"SubdocReplaceBodyWithXattr_Durability",
                          getTestCollection()};
    const auto new_value = getJsonDoc().dump();
    const auto old_value = getStringValue(false);
    upsert(id, old_value, "tnx.op.staged", new_value);

    BinprotSubdocMultiMutationCommand cmd{
            std::string{id},
            {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}},
             {cb::mcbp::ClientOpcode::SubdocDelete,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}}},
            cb::mcbp::subdoc::doc_flag::None};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);

    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
    } else {
        ASSERT_TRUE(rsp.getWriteUnits());
        EXPECT_EQ(to_wu(calculateDocumentSize(id, new_value)) * 2,
                  *rsp.getWriteUnits());
    }
}

TEST_P(MeteringTest, TTL_Expiry_Get) {
    const StoredDocKey id{"TTL_Expiry_Get", getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr_value = getStringValue(false);

    Document doc;
    doc.info.id = std::string{id};
    doc.info.expiration = 1;
    doc.value = value;
    conn->mutate(doc, Vbid{0}, MutationType::Set);
    waitForPersistence();

    nlohmann::json before;
    conn->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");
    size_t expiredBefore = std::stoull(getStatForKey("vb_active_expired"));

    BinprotResponse rsp;
    waitForPredicateUntil(
            [&rsp, &id, this]() {
                rsp = conn->execute(BinprotGetCommand{std::string{id}});
                return !rsp.isSuccess();
            },
            std::chrono::seconds{3});

    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << "should have been TTL expired";

    // TTL wu calculated at flush
    waitForPersistence();

    nlohmann::json after;
    conn->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");

    size_t expiredAfter = std::stoull(getStatForKey("vb_active_expired"));
    EXPECT_NE(expiredBefore, expiredAfter);

    // @todo: MB-51979 expiry in an unmetered collection is updating wu
    EXPECT_EQ(1,
              after["wu"].get<std::size_t>() - before["wu"].get<std::size_t>());
}

TEST_P(MeteringTest, TTL_Expiry_Compaction) {
    const StoredDocKey id{"TTL_Expiry_Compaction", getTestCollection()};
    const auto value = getJsonDoc().dump();
    const auto xattr_value = getStringValue(false);

    Document doc;
    doc.info.id = std::string{id};
    doc.info.expiration = 1;
    doc.value = value;
    conn->mutate(doc, Vbid{0}, MutationType::Set);
    waitForPersistence();

    nlohmann::json before;
    conn->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");
    size_t expiredBefore = std::stoull(getStatForKey("vb_active_expired"));

    // wait 2 seconds and the document should be expired
    std::this_thread::sleep_for(std::chrono::seconds{2});

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("metering");
    auto rsp = admin->execute(BinprotCompactDbCommand{});
    EXPECT_TRUE(rsp.isSuccess());
    waitForPersistence();

    nlohmann::json after;
    conn->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");

    // @todo: MB-51979 expiry in an unmetered collection is updating wu
    EXPECT_EQ(1,
              after["wu"].get<std::size_t>() - before["wu"].get<std::size_t>());

    size_t expiredAfter = std::stoull(getStatForKey("vb_active_expired"));
    EXPECT_NE(expiredBefore, expiredAfter);
}

void MeteringTest::testRangeScan(bool keyOnly) {
    Document doc;
    doc.value = "value";
    doc.info.id = DocKey::makeWireEncodedString(getTestCollection(), "key");
    auto mInfo = conn->mutate(doc, Vbid(0), MutationType::Set);

    auto start = cb::base64::encode("key", false);
    auto end = cb::base64::encode("key\xFF", false);
    nlohmann::json range = {{"range", {{"start", start}, {"end", end}}}};
    range["snapshot_requirements"] = {
            {"seqno", mInfo.seqno},
            {"vb_uuid", std::to_string(mInfo.vbucketuuid)},
            {"timeout_ms", 120000}};

    if (keyOnly) {
        range["key_only"] = true;
    }

    range["collection"] = fmt::format("{:x}", uint32_t(getTestCollection()));

    nlohmann::json before;
    conn->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");

    BinprotRangeScanCreate create(Vbid(0), range);
    auto resp = conn->execute(create);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    EXPECT_FALSE(resp.getWriteUnits());

    nlohmann::json after;
    conn->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");

    if (isUnmetered()) {
        EXPECT_FALSE(resp.getReadUnits());
        EXPECT_EQ(after["ru"].get<std::size_t>(),
                  before["ru"].get<std::size_t>());
    } else {
        ASSERT_TRUE(resp.getReadUnits());
        ASSERT_EQ(1, *resp.getReadUnits());
        // ru should increase when creating a scan
        EXPECT_GT(after["ru"].get<std::size_t>(),
                  before["ru"].get<std::size_t>());
    }
    cb::rangescan::Id id;
    std::memcpy(id.data, resp.getData().data(), resp.getData().size());

    // 1 key in scan, issue 1 continue
    BinprotRangeScanContinue scanContinue(
            Vbid(0),
            id,
            0, /* no limit */
            std::chrono::milliseconds(0) /* no time limit*/,
            0 /*no byte limit*/);
    resp = conn->execute(scanContinue);
    ASSERT_EQ(cb::mcbp::Status::RangeScanComplete, resp.getStatus());
    EXPECT_FALSE(resp.getWriteUnits());

    if (isUnmetered()) {
        EXPECT_FALSE(resp.getReadUnits());
    } else if (keyOnly) {
        EXPECT_EQ(to_ru(doc.info.id.size()), *resp.getReadUnits());
    } else {
        EXPECT_EQ(to_ru(doc.info.id.size() + doc.value.size()),
                  *resp.getReadUnits());
    }
}

TEST_P(MeteringTest, RangeScanKey) {
    testRangeScan(true);
}
TEST_P(MeteringTest, RangeScanValue) {
    testRangeScan(false);
}

void MeteringTest::testWithMeta(cb::mcbp::ClientOpcode opcode, DocKey id) {
    Document doc;
    doc.info.id = std::string{id};
    doc.info.cas = 0xdeadbeef;
    doc.info.flags = 0xcafefeed;
    doc.value = getJsonDoc().dump();

    std::vector<uint8_t> meta;
    BinprotSetWithMetaCommand cmd{
            doc,
            Vbid{0},
            cb::mcbp::cas::Wildcard,
            127,
            REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG,
            meta};
    cmd.setOp(opcode);
    auto rsp = conn->execute(cmd);

    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    if (isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
        EXPECT_FALSE(rsp.getReadUnits());
        return;
    }

    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    if (opcode == cb::mcbp::ClientOpcode::DelWithMeta) {
        // Del with meta strips off value
        EXPECT_EQ(1, *rsp.getWriteUnits());
    } else {
        EXPECT_EQ(to_wu(calculateDocumentSize(id, doc.value)),
                  *rsp.getWriteUnits());
    }
    // @todo add unit tests with / without xattrs
}

TEST_P(MeteringTest, AddWithMeta) {
    testWithMeta(cb::mcbp::ClientOpcode::AddWithMeta,
                 StoredDocKey{"AddWithMeta/" + to_string(GetParam()),
                              getTestCollection()});
}

TEST_P(MeteringTest, SetWithMeta) {
    testWithMeta(cb::mcbp::ClientOpcode::SetWithMeta,
                 StoredDocKey{"SetWithMeta", getTestCollection()});
}

TEST_P(MeteringTest, DelWithMeta) {
    testWithMeta(cb::mcbp::ClientOpcode::DelWithMeta,
                 StoredDocKey{"DelWithMeta", getTestCollection()});
}

void MeteringTest::testReturnMeta(cb::mcbp::request::ReturnMetaType type,
                                  DocKey id,
                                  bool success) {
    Document document;
    document.info.id = std::string{id};
    document.value = getStringValue();
    if (!success) {
        document.info.cas = 0xdeadbeef;
    }

    auto rsp = conn->execute(BinprotReturnMetaCommand{type, document});
    if (success && isUnmetered()) {
        EXPECT_FALSE(rsp.getWriteUnits());
        EXPECT_FALSE(rsp.getReadUnits());
    } else if (success) {
        ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
        EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
        ASSERT_TRUE(rsp.getWriteUnits());
        if (type == cb::mcbp::request::ReturnMetaType::Del) {
            EXPECT_EQ(1, *rsp.getWriteUnits());
        } else {
            EXPECT_EQ(to_wu(calculateDocumentSize(id, document.value)),
                      *rsp.getWriteUnits());
        }
    } else {
        if (type == cb::mcbp::request::ReturnMetaType::Del) {
            EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
        } else {
            EXPECT_EQ(cb::mcbp::Status::NotStored, rsp.getStatus());
        }
        EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
        EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
    }
}

TEST_P(MeteringTest, ReturnMetaAdd) {
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Add,
                   StoredDocKey{"ReturnMetaAdd", getTestCollection()},
                   true);
}

TEST_P(MeteringTest, ReturnMetaAddFailing) {
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Add,
                   StoredDocKey{"ReturnMetaAddFailing", getTestCollection()},
                   false);
}

TEST_P(MeteringTest, ReturnMetaSet) {
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Set,
                   StoredDocKey{"ReturnMetaSet", getTestCollection()},
                   true);
}

TEST_P(MeteringTest, ReturnMetaSetFailing) {
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Add,
                   StoredDocKey{"ReturnMetaSetFailing", getTestCollection()},
                   false);
}

TEST_P(MeteringTest, ReturnMetaDel) {
    const StoredDocKey id{"ReturnMetaDel", getTestCollection()};
    upsert(id, getStringValue());
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Del, id, true);
}

TEST_P(MeteringTest, ReturnMetaDelFailing) {
    const StoredDocKey id{"ReturnMetaDelFailing", getTestCollection()};
    upsert(id, getStringValue());
    testReturnMeta(cb::mcbp::request::ReturnMetaType::Del, id, false);
}

static std::string MeteringTypeToString(
        const ::testing::TestParamInfo<MeteringTest::ParamType>& info) {
    return to_string(info.param);
}

TEST_P(MeteringTest, ImposedUsersMayMeter) {
    upsert(StoredDocKey{"ImposedUsersMayMeter", getTestCollection()},
           getStringValue());

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("metering");
    admin->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);

    nlohmann::json before;
    admin->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");

    // Verify that we don't meter
    BinprotGetCommand cmd("ImposedUsersMayMeter");
    auto rsp = admin->execute(cmd);
    ASSERT_TRUE(rsp.isSuccess());
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();

    nlohmann::json after;
    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");
    EXPECT_EQ(before["ru"].get<int>(), after["ru"].get<int>());

    ImpersonateUserFrameInfo fi("^metering");
    cmd.addFrameInfo(fi);
    rsp = admin->execute(cmd);
    ASSERT_TRUE(rsp.isSuccess());
    EXPECT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(2, *rsp.getReadUnits());

    admin->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                 "bucket_details metering");
    EXPECT_EQ(before["ru"].get<int>() + 2, after["ru"].get<int>());
}

// Store a document in vbucket 2 and wait for it to expire, and
// verify that once it is gone on the replica we didn't charge any
// write units
TEST_P(MeteringTest, MB54479) {
    auto bucket = cluster->getBucket("metering");
    auto active = bucket->getConnection(Vbid{2}, vbucket_state_active);
    auto replica = bucket->getConnection(Vbid{2}, vbucket_state_replica, 1);

    active->authenticate("@admin", "password");
    active->selectBucket("metering");
    replica->authenticate("@admin", "password");
    replica->selectBucket("metering");

    nlohmann::json before;
    replica->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");

    const size_t vb_replica_curr_items =
            std::stoull(getStatForKey("vb_replica_curr_items", replica.get()));

    const StoredDocKey id{"MB54479", getTestCollection()};
    Document doc;
    doc.info.id = std::string{id};
    doc.info.expiration = 2;
    doc.value = getStringValue(false, 100);
    active->mutate(doc, Vbid{2}, MutationType::Set);
    waitForPersistence(active.get());

    // wait for the item to be replicated over
    waitForPredicateUntil(
            [&vb_replica_curr_items, this, &replica]() {
                return vb_replica_curr_items !=
                       std::stoull(getStatForKey("vb_replica_curr_items",
                                                 replica.get()));
            },
            std::chrono::seconds{3});

    {
        // verify that it is actually the document we expected
        BinprotGenericCommand cmd{ClientOpcode::GetReplica, std::string{id}};
        cmd.setVBucket(Vbid{2});
        EXPECT_TRUE(replica->execute(cmd).isSuccess());
    }

    // and to be persisted on disk
    waitForPersistence(replica.get());

    // Wait for the object to expire (and by requesting it on the active
    // vbucket we'll make sure we push a DCP Expiration message to the
    // replica to trigger TTL.
    BinprotResponse rsp;
    waitForPredicateUntil(
            [&rsp, &id, &active]() {
                BinprotGetCommand cmd{std::string{id}};
                cmd.setVBucket(Vbid{2});
                rsp = active->execute(cmd);
                return !rsp.isSuccess();
            },
            std::chrono::seconds{10});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << "should have been TTL expired";

    // Wait until we're back at the expected number of items on the replica
    waitForPredicateUntil(
            [&replica, vb_replica_curr_items, this]() {
                // Wait for persistence again to make sure that the replica
                // count gets updated
                waitForPersistence(replica.get());
                return vb_replica_curr_items ==
                       std::stoull(getStatForKey("vb_replica_curr_items",
                                                 replica.get()));
            },
            std::chrono::seconds{10});

    // Verify that we didn't charge any WUs on the replica
    nlohmann::json after;
    replica->stats(
            [&after](auto k, auto v) { after = nlohmann::json::parse(v); },
            "bucket_details metering");
    EXPECT_EQ(after["wu"].get<int>(), before["wu"].get<int>());
}

INSTANTIATE_TEST_SUITE_P(MeteringTest,
                         MeteringTest,

                         ::testing::Values(MeteringType::Metered,
                                           MeteringType::UnmeteredByPrivilege,
                                           MeteringType::UnmeteredByCollection),
                         MeteringTypeToString);

} // namespace cb::test
