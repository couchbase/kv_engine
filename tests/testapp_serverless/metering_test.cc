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
#include <folly/portability/GTest.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <protocol/connection/frameinfo.h>
#include <serverless/config.h>
#include <xattr/blob.h>
#include <chrono>
#include <deque>
#include <thread>

using ClientOpcode = cb::mcbp::ClientOpcode;
using Status = cb::mcbp::Status;

namespace cb::test {

class MeteringTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        conn = cluster->getConnection(0);
        conn->authenticate("@admin", "password");
        conn->selectBucket("metering");
        conn->dropPrivilege(cb::rbac::Privilege::Unmetered);
        conn->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);
        conn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
    }

    static void TearDownTestCase() {
        conn.reset();
    }

protected:
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
    static void upsert(std::string id,
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
    size_t calculateDocumentSize(std::string_view key,
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
        rconn->dropPrivilege(cb::rbac::Privilege::Unmetered);
        rconn->dropPrivilege(cb::rbac::Privilege::NodeSupervisor);
        rconn->setFeature(cb::mcbp::Feature::ReportUnitUsage, true);
        return rconn;
    }

    /// Operating on a document which isn't a numeric value should
    /// account for X ru's and fail
    void testArithmeticBadValue(ClientOpcode opcode,
                                std::string id,
                                std::string value,
                                std::string xattr_path = {},
                                std::string xattr_value = {}) {
        auto cmd = BinprotIncrDecrCommand{opcode, id, Vbid{0}, 1ULL, 0ULL, 0};

        upsert(id, value, xattr_path, xattr_value);
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(Status::DeltaBadval, rsp.getStatus());
        ASSERT_TRUE(rsp.getReadUnits().has_value());
        EXPECT_EQ(to_ru(calculateDocumentSize(
                          id, value, xattr_path, xattr_value)),
                  *rsp.getReadUnits());
        EXPECT_FALSE(rsp.getWriteUnits());
    }

    /// When creating a value as part of incr/decr it should not cost any
    /// RU, but 1 WU for a normal create, and 2 WU for durable writes.
    void testArithmeticCreateValue(ClientOpcode opcode,
                                   std::string id,
                                   bool durable) {
        auto cmd = BinprotIncrDecrCommand{opcode, id, Vbid{0}, 1ULL, 0ULL, 0};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        EXPECT_FALSE(rsp.getReadUnits());
        ASSERT_TRUE(rsp.getWriteUnits());
        if (durable) {
            EXPECT_EQ(2, *rsp.getWriteUnits());
        } else {
            EXPECT_EQ(1, *rsp.getWriteUnits());
        }
    }

    // Operating on a document without XAttrs should account 1WU during
    // create and 1RU + 1WU during update (it is 1 because it only contains
    // the body and we don't support an interger which consumes 4k digits ;)
    // For documents with XATTRS the size of the xattrs (encoded) gets added
    // to the RU/WU units.
    // For durable writes it costs 2x WU
    void testArithmetic(ClientOpcode opcode,
                        std::string id,
                        std::string xattr_path,
                        std::string xattr_value,
                        bool durable) {
        upsert(id, "10", xattr_path, xattr_value);

        auto cmd = BinprotIncrDecrCommand{opcode, id, Vbid{0}, 1ULL, 0ULL, 0};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        ASSERT_TRUE(rsp.getReadUnits());
        EXPECT_EQ(
                to_ru(calculateDocumentSize(id, "10", xattr_path, xattr_value)),
                *rsp.getReadUnits());

        const auto expected_wu =
                to_wu(calculateDocumentSize(id, "10", xattr_path, xattr_value));
        ASSERT_TRUE(rsp.getWriteUnits());
        if (durable) {
            EXPECT_EQ(expected_wu * 2, *rsp.getWriteUnits());
        } else {
            EXPECT_EQ(expected_wu, *rsp.getWriteUnits());
        }
    }

    // Deleting a normal document should cost 1 WU (no RU)
    // Delete a document with user XATTRs should cost 1WU and doc size RU
    // Delete a document with system xattrs should cost xattr + key wu and doc
    //        size ru
    // Durable reads should cost twice
    void testDelete(ClientOpcode opcode,
                    std::string id,
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

        auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, id};
        DurabilityFrameInfo fi(
                cb::durability::Level::MajorityAndPersistOnMaster);
        if (durable) {
            cmd.addFrameInfo(fi);
        }
        auto rsp = conn->execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
        ASSERT_TRUE(rsp.getWriteUnits());
        if (user_xattr_path.empty()) {
            EXPECT_FALSE(rsp.getReadUnits());
            if (durable) {
                EXPECT_EQ(2, *rsp.getWriteUnits());
            } else {
                EXPECT_EQ(1, *rsp.getWriteUnits());
            }
        } else {
            ASSERT_TRUE(rsp.getReadUnits());
            EXPECT_EQ(to_ru(calculateDocumentSize(id,
                                                  value,
                                                  user_xattr_path,
                                                  user_xattr_value,
                                                  system_xattr_path,
                                                  system_xattr_value)),
                      *rsp.getReadUnits());
            if (durable) {
                EXPECT_EQ(to_wu(calculateDocumentSize(id,
                                                      {},
                                                      system_xattr_path,
                                                      system_xattr_value)) *
                                  2,
                          *rsp.getWriteUnits());
            } else {
                EXPECT_EQ(
                        to_wu(calculateDocumentSize(
                                id, {}, system_xattr_path, system_xattr_value)),
                        *rsp.getWriteUnits());
            }
        }
    }

    void waitForPersistence() {
        size_t ep_queue_size;
        do {
            using namespace std::string_view_literals;
            conn->stats([&ep_queue_size](auto k, auto v) {
                if (k == "ep_queue_size"sv) {
                    ep_queue_size = std::stoi(v);
                }
            });
            if (ep_queue_size) {
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        } while (ep_queue_size != 0);
    }

    std::string getStatForKey(std::string_view key) {
        std::string value;
        conn->stats([&key, &value](auto k, auto v) {
            if (k == key) {
                value = v;
            }
        });
        return value;
    }

    static std::unique_ptr<MemcachedConnection> conn;
};

std::unique_ptr<MemcachedConnection> MeteringTest::conn;

void MeteringTest::upsert(std::string id,
                          std::string value,
                          std::string xattr_path,
                          std::string xattr_value,
                          bool wait_for_persistence,
                          std::string second_xattr_path,
                          std::string second_xattr_value) {
    if (xattr_path.empty()) {
        Document doc;
        doc.info.id = std::move(id);
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
            cmd.setKey(id);
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
                cmd.addDocFlag(::mcbp::subdoc::doc_flag::Mkdoc);
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
TEST_F(MeteringTest, UnmeteredPrivilege) {
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
TEST_F(MeteringTest, OpsMetered) {
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
            // MetaWrite ops require meta write privilege... probably not
            // something we'll need initially...
            break;

        case ClientOpcode::SeqnoPersistence:
        case ClientOpcode::CollectionsGetID:
        case ClientOpcode::CollectionsGetScopeID:
            // @todo add unit tests!
            break;

        case ClientOpcode::RangeScanCreate:
        case ClientOpcode::RangeScanContinue:
        case ClientOpcode::RangeScanCancel:
            // @todo create a test case for range scans
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
        case ClientOpcode::SetBucketUnitThrottleLimits:
        case ClientOpcode::SetBucketDataLimitExceeded:
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

TEST_F(MeteringTest, IncrBadValuePlain) {
    testArithmeticBadValue(
            ClientOpcode::Increment, "IncrBadValuePlain", getStringValue());
}

TEST_F(MeteringTest, IncrBadValueWithXattr) {
    testArithmeticBadValue(ClientOpcode::Increment,
                           "IncrBadValueWithXattr",
                           getStringValue(),
                           "xattr",
                           getStringValue());
}

TEST_F(MeteringTest, DecrBadValuePlain) {
    testArithmeticBadValue(
            ClientOpcode::Decrement, "DecrBadValuePlain", getStringValue());
}

TEST_F(MeteringTest, DecrBadValueWithXattr) {
    testArithmeticBadValue(ClientOpcode::Decrement,
                           "DecrBadValueWithXattr",
                           getStringValue(),
                           "xattr",
                           getStringValue());
}

TEST_F(MeteringTest, IncrCreateValue) {
    testArithmeticCreateValue(
            ClientOpcode::Increment, "IncrCreateValue", false);
}

TEST_F(MeteringTest, IncrCreateValue_Durability) {
    testArithmeticCreateValue(
            ClientOpcode::Increment, "IncrCreateValue_Durability", true);
}

TEST_F(MeteringTest, DecrCreateValue) {
    testArithmeticCreateValue(
            ClientOpcode::Decrement, "DecrCreateValue", false);
}

TEST_F(MeteringTest, DecrCreateValue_Durability) {
    testArithmeticCreateValue(
            ClientOpcode::Decrement, "DecrCreateValue_Durability", true);
}

TEST_F(MeteringTest, IncrementPlain) {
    testArithmetic(ClientOpcode::Increment, "IncrementPlain", {}, {}, false);
}

TEST_F(MeteringTest, IncrementPlain_Durability) {
    testArithmetic(ClientOpcode::Increment,
                   "IncrementWithXattr_Durability",
                   {},
                   {},
                   true);
}

TEST_F(MeteringTest, IncrementWithXattr) {
    testArithmetic(ClientOpcode::Increment,
                   "IncrementWithXattr",
                   "xattr",
                   getStringValue(),
                   false);
}

TEST_F(MeteringTest, IncrementWithXattr_Durability) {
    testArithmetic(ClientOpcode::Increment,
                   "IncrementWithXattr_Durability",
                   "xattr",
                   getStringValue(),
                   true);
}

TEST_F(MeteringTest, DecrementPlain) {
    testArithmetic(ClientOpcode::Decrement, "DecrementPlain", {}, {}, false);
}

TEST_F(MeteringTest, DecrementPlain_Durability) {
    testArithmetic(ClientOpcode::Decrement,
                   "DecrementWithXattr_Durability",
                   {},
                   {},
                   true);
}

TEST_F(MeteringTest, DecrementWithXattr) {
    testArithmetic(ClientOpcode::Decrement,
                   "DecrementWithXattr",
                   "xattr",
                   getStringValue(),
                   false);
}

TEST_F(MeteringTest, DecrementWithXattr_Durability) {
    testArithmetic(ClientOpcode::Decrement,
                   "DecrementWithXattr_Durability",
                   "xattr",
                   getStringValue(),
                   true);
}

/// Delete of a non-existing document should be free
TEST_F(MeteringTest, DeleteNonexistingItem) {
    const std::string id = "DeleteNonexistingItem";
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, id};
    auto rsp = conn->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

// Delete of a single document should cost 1WU
TEST_F(MeteringTest, DeletePlain) {
    testDelete(ClientOpcode::Delete,
               "DeletePlain",
               getStringValue(),
               {},
               {},
               {},
               {},
               false);
}

TEST_F(MeteringTest, DeletePlain_Durability) {
    testDelete(ClientOpcode::Delete,
               "DeletePlain_Durability",
               getStringValue(),
               {},
               {},
               {},
               {},
               true);
}

TEST_F(MeteringTest, DeleteUserXattr) {
    testDelete(ClientOpcode::Delete,
               "DeleteUserXattr",
               getStringValue(),
               "xattr",
               getStringValue(),
               {},
               {},
               false);
}

TEST_F(MeteringTest, DeleteUserXattr_Durability) {
    testDelete(ClientOpcode::Delete,
               "DeleteUserXattr_Durability",
               getStringValue(),
               "xattr",
               getStringValue(),
               {},
               {},
               true);
}

TEST_F(MeteringTest, DeleteUserAndSystemXattr) {
    testDelete(ClientOpcode::Delete,
               "DeleteUserAndSystemXattr",
               getStringValue(),
               "xattr",
               getStringValue(),
               "_xattr",
               getStringValue(),
               false);
}

TEST_F(MeteringTest, DeleteUserAndSystemXattr_Durability) {
    testDelete(ClientOpcode::Delete,
               "DeleteUserAndSystemXattr_Durability",
               getStringValue(),
               "xattr",
               getStringValue(),
               "_xattr",
               getStringValue(),
               true);
}

TEST_F(MeteringTest, DeleteSystemXattr) {
    testDelete(ClientOpcode::Delete,
               "DeleteSystemXattr",
               getStringValue(),
               {},
               {},
               "_xattr",
               getStringValue(),
               false);
}

TEST_F(MeteringTest, DeleteSystemXattr_Durability) {
    testDelete(ClientOpcode::Delete,
               "DeleteSystemXattr_Durability",
               getStringValue(),
               {},
               {},
               "_xattr",
               getStringValue(),
               true);
}

/// Get of a non-existing document should not cost anything
TEST_F(MeteringTest, GetNonExistingDocument) {
    auto rsp = conn->execute(
            BinprotGenericCommand{ClientOpcode::Get, "GetNonExistingDocument"});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Get of a single document without xattrs costs the size of the document
TEST_F(MeteringTest, GetDocumentPlain) {
    const std::string id = "GetDocumentPlain";
    const auto value = getStringValue();
    upsert(id, value);
    auto rsp = conn->execute(BinprotGenericCommand{ClientOpcode::Get, id});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
}

/// Get of a single document with xattrs costs the size of the document plus
/// the size of the xattrs
TEST_F(MeteringTest, GetDocumentWithXAttr) {
    const std::string id = "GetDocumentWithXAttr";
    const auto value = getStringValue();
    upsert(id, value, "xattr", value);
    auto rsp = conn->execute(BinprotGenericCommand{ClientOpcode::Get, id});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", value)),
              *rsp.getReadUnits());
}

/// Get of a non-existing document should not cost anything
TEST_F(MeteringTest, GetReplicaNonExistingDocument) {
    auto rsp = getReplicaConn()->execute(BinprotGenericCommand{
            ClientOpcode::GetReplica, "GetNonExistingDocument"});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Get of a single document without xattrs costs the size of the document
TEST_F(MeteringTest, GetReplicaDocumentPlain) {
    const std::string id = "GetDocumentPlain";
    const auto value = getStringValue();
    upsert(id, value);

    auto rconn = getReplicaConn();
    BinprotResponse rsp;
    do {
        rsp = rconn->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::GetReplica, id});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Get of a single document with xattrs costs the size of the document plus
/// the size of the xattrs
TEST_F(MeteringTest, GetReplicaDocumentWithXAttr) {
    const std::string id = "GetDocumentWithXAttr";
    const auto value = getStringValue();
    upsert(id, value, "xattr", value);

    auto rconn = getReplicaConn();
    BinprotResponse rsp;
    do {
        rsp = rconn->execute(
                BinprotGenericCommand{cb::mcbp::ClientOpcode::GetReplica, id});
    } while (rsp.getStatus() == cb::mcbp::Status::KeyEnoent);

    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", value)),
              *rsp.getReadUnits());
}

TEST_F(MeteringTest, MeterDocumentLocking) {
    auto& sconfig = cb::serverless::Config::instance();

    const std::string id = "MeterDocumentLocking";
    const auto getl = BinprotGetAndLockCommand{id};
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
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    auto unl = BinprotUnlockCommand{id, Vbid{0}, rsp.getCas()};
    rsp = conn->execute(unl);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

TEST_F(MeteringTest, MeterDocumentTouch) {
    auto& sconfig = cb::serverless::Config::instance();
    const std::string id = "MeterDocumentTouch";

    // Touch of non-existing document should fail and is free
    auto rsp = conn->execute(BinprotTouchCommand{id, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    // Gat should fail and free
    rsp = conn->execute(BinprotGetAndTouchCommand{id, Vbid{0}, 0});
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus());
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());

    std::string document_value;
    document_value.resize(sconfig.readUnitSize - 5);
    std::fill(document_value.begin(), document_value.end(), 'a');
    upsert(id, document_value);

    // Touch of a document is a full read and write of the document on the
    // server, but no data returned
    rsp = conn->execute(BinprotTouchCommand{id, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
              *rsp.getWriteUnits());
    EXPECT_TRUE(rsp.getDataString().empty());
    rsp = conn->execute(BinprotGetAndTouchCommand{id, Vbid{0}, 0});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(document_value.size() + id.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(document_value.size() + id.size()),
              *rsp.getWriteUnits());
    EXPECT_EQ(document_value, rsp.getDataString());
}

TEST_F(MeteringTest, MeterDocumentSimpleMutations) {
    auto& sconfig = cb::serverless::Config::instance();

    const std::string id = "MeterDocumentSimpleMutations";
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
    command.setKey(id);
    command.addValueBuffer(document_value);

    // Set of an nonexistent document shouldn't cost any RUs and the
    // size of the new document's WUs
    command.setMutationType(MutationType::Set);
    auto rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

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
    conn->remove(id, Vbid{0});
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

    // Replace of the document should cost 1 ru (for the metadata read)
    // then X WUs
    command.setMutationType(MutationType::Replace);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    // @todo it currently don't cost the 1 ru for the metadata read!
    EXPECT_FALSE(rsp.getReadUnits()) << *rsp.getReadUnits();
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size()),
              *rsp.getWriteUnits());

    // But if we try to replace a document containing XATTRs we would
    // need to read the full document in order to replace, and it should
    // cost the size of the full size of the old document and the new one
    // (containing the xattrs)
    upsert(id, document_value, xattr_path, xattr_value);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    // Trying to replace a document with incorrect CAS should cost 1 RU and
    // no WU
    command.setCas(1);
    rsp = conn->execute(command);
    EXPECT_EQ(cb::mcbp::Status::KeyEexists, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    // @todo it currently fails and return the size of the old document!
    // EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits()) << *rsp.getWriteUnits();
    command.setCas(0);

    // Trying to replace a nonexisting document should not cost anything
    conn->remove(id, Vbid{0});
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
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
              *rsp.getWriteUnits());

    upsert(id, document_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2),
              *rsp.getWriteUnits());

    // And if we have XATTRs they should be copied as well
    upsert(id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Append);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    upsert(id, document_value, xattr_path, xattr_value);
    command.setMutationType(MutationType::Prepend);
    rsp = conn->execute(command);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(sconfig.to_ru(id.size() + document_value.size() +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(sconfig.to_wu(id.size() + document_value.size() * 2 +
                            xattr_path.size() + xattr_value.size()),
              *rsp.getWriteUnits());

    // @todo add test cases for durability
}

TEST_F(MeteringTest, MeterGetRandomKey) {
    // Random key needs at least one key to be stored in the bucket so that
    // it may return the key. To make sure that the unit tests doesn't depend
    // on other tests lets store a document in vbucket 0.
    // However GetRandomKey needs to check the collection item count, which only
    // updates when we flush a committed item, yet a durable write is successful
    // once all pending writes are "in-place" - thus GetRandomKey could race
    // with the flush of a commit, to get around that we can store twice, this
    // single connection will then ensure a non zero item count is observed by
    // GetRandomKey
    upsert("MeterGetRandomKey", "hello");
    upsert("MeterGetRandomKey", "hello", {}, {}, true);

    const auto rsp = conn->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetRandomKey});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_NE(0, *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

TEST_F(MeteringTest, MeterGetKeys) {
    // GetKeys needs at least one key to be stored in the bucket so that
    // it may return the key. To make sure that the unit tests doesn't depend
    // on other tests lets store a document in vbucket 0.
    // However GetKeys needs to check the collection item count, which only
    // updates when we flush a committed item, yet a durable write is successful
    // once all pending writes are "in-place" - thus GetKeys could race
    // with the flush of a commit, to get around that we can store twice, this
    // single connection will then ensure a non zero item count is observed by
    // GetKeys
    upsert("MeterGetKeys", "hello");
    upsert("MeterGetKeys", "hello", {}, {}, true);

    const auto rsp = conn->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::GetKeys, std::string{"\0", 1}});
    ASSERT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getData().empty());
    ASSERT_TRUE(rsp.getReadUnits()) << rsp.getDataString();
    // Depending on how many keys we've got in the database..
    EXPECT_LE(1, *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_F(MeteringTest, GetMetaNonexistentDocument) {
    // Verify cost of nonexistent value
    const std::string id = "ClientOpcode::GetMeta";
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta, id};
    auto rsp = conn->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus()) << rsp.getStatus();
    EXPECT_FALSE(rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_F(MeteringTest, GetMetaPlainDocument) {
    const std::string id = "ClientOpcode::GetMeta";
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta, id};
    upsert(id, getStringValue());
    const auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetMeta should cost 1 RU (we only look up metadata)
TEST_F(MeteringTest, GetMetaDocumentWithXattr) {
    const std::string id = "ClientOpcode::GetMeta";
    const auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::GetMeta, id};
    upsert(id, getStringValue(), "xattr", getStringValue());
    const auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(1, *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc get should cost the entire doc read; even if the requested path
/// doesn't exists
TEST_F(MeteringTest, SubdocGetNoSuchPath) {
    const std::string id = "SubdocGetENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    ASSERT_LT(1, to_ru(calculateDocumentSize(id, value)));
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGet, id, "hello"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc get should cost the entire doc read; and not just the returned
/// path
TEST_F(MeteringTest, SubdocGet) {
    const std::string id = "SubdocGet";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocGet, id, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc get should cost the entire doc read (including xattr); and not just
//  the returned path
TEST_F(MeteringTest, SubdocGetWithXattr) {
    const std::string id = "SubdocGetXattr";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocGet, id, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc exists should cost the entire doc read no matter if the path
/// exists or not
TEST_F(MeteringTest, SubdocExistsNoSuchPath) {
    const std::string id = "SubdocExistsNoSuchPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, id, "hello"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc exists should cost the entire doc read
TEST_F(MeteringTest, SubdocExistsPlainDoc) {
    const std::string id = "SubdocExistsPlainDoc";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, id, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Subdoc exists should cost the entire doc read (that include xattrs)
TEST_F(MeteringTest, SubdocExistsWithXattr) {
    const std::string id = "SubdocExistsWithXattr";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocExists, id, "v1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Dict add should cost RU for the document event if the path already
/// exists
TEST_F(MeteringTest, SubdocDictAddEExist) {
    const std::string id = "SubdocDictAddEExist";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDictAdd, id, "v1", "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Dict add should cost RU for the document, and WUs for the new document
TEST_F(MeteringTest, SubdocDictAddPlainDoc) {
    const std::string id = "SubdocDictAddPlainDoc";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto v3 = getStringValue();
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDictAdd, id, "v3", v3});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + value.size() + v3.size()),
              *rsp.getWriteUnits());
}

/// Dict add should cost RU for the document, and 2x WUs for the new document
TEST_F(MeteringTest, SubdocDictAddPlainDoc_Durability) {
    const std::string id = "SubdocDictAddPlainDoc_Durability";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto v3 = getStringValue();
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDictAdd, id, "v3", v3};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + value.size() + v3.size()) * 2,
              *rsp.getWriteUnits());
}

/// Dict add should cost RU for the document, and WUs for the new document
/// (including the XAttrs copied over)
TEST_F(MeteringTest, SubdocDictAddPlainDocWithXattr) {
    const std::string id = "SubdocDictAddPlainDocWithXattr";
    auto json = getJsonDoc();
    auto value = json.dump();
    auto v3 = getStringValue();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDictAdd, id, "v3", v3});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v3"] = v3;
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// Dict add should cost RU for the document, and 2x WUs for the new document
/// (including the XAttrs copied over)
TEST_F(MeteringTest, SubdocDictAddPlainDocWithXattr_Durability) {
    const std::string id = "SubdocDictAddPlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    auto value = json.dump();
    auto v3 = getStringValue();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocDictAdd, id, "v3", v3};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v3"] = v3;
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// Dict Upsert should cost RU for the document, and WUs for the new document
TEST_F(MeteringTest, SubdocDictUpsertPlainDoc) {
    const std::string id = "SubdocDictUpsertPlainDoc";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                 id,
                                 "v1",
                                 R"("this is the new value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v1"] = "this is the new value";
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
              *rsp.getWriteUnits());
}

/// Dict Upsert should cost RU for the document, and 2x WUs for the new document
TEST_F(MeteringTest, SubdocDictUpsertPlainDoc_Durability) {
    const std::string id = "SubdocDictUpsertPlainDoc_Durability";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             id,
                             "v1",
                             R"("this is the new value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v1"] = "this is the new value";
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
              *rsp.getWriteUnits());
}

/// Dict Upsert should cost RU for the document, and WUs for the new document
/// (including the XAttrs copied over)
TEST_F(MeteringTest, SubdocDictUpsertPlainDocWithXattr) {
    const std::string id = "SubdocDictAddPlainDocWithXattr";
    auto json = getJsonDoc();
    auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                                 id,
                                 "v1",
                                 R"("this is the new value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v1"] = "this is the new value";
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// Dict Upsert should cost RU for the document, and 2x WUs for the new document
/// (including the XAttrs copied over)
TEST_F(MeteringTest, SubdocDictUpsertPlainDocWithXattr_Durability) {
    const std::string id = "SubdocDictAddPlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDictUpsert,
                             id,
                             "v1",
                             R"("this is the new value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["v1"] = "this is the new value";
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// Delete should cost the full read even if the path doesn't exist
TEST_F(MeteringTest, SubdocDeleteENoPath) {
    const std::string id = "SubdocDeleteENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, id, "ENOPATH"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Delete should cost the full read, and the write of the full size of the
/// new document
TEST_F(MeteringTest, SubdocDeletePlainDoc) {
    const std::string id = "SubdocDeletePlainDoc";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, id, "fill"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    json.erase("fill");
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
              *rsp.getWriteUnits());
}

/// Delete should cost the full read, and the write of the full size of the
/// new document
TEST_F(MeteringTest, SubdocDeletePlainDoc_Durability) {
    const std::string id = "SubdocDeletePlainDoc_Durability";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDelete, id, "fill"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    json.erase("fill");
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
              *rsp.getWriteUnits());
}

/// Delete should cost the full read (including xattrs), and the write of the
/// full size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocDeletePlainDocWithXattr) {
    const std::string id = "SubdocDeletePlainDocWithXattr";
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocDelete, id, "fill"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json.erase("fill");
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// Delete should cost the full read (including xattrs), and the write 2x of the
/// full size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocDeletePlainDocWithXattr_Durability) {
    const std::string id = "SubdocDeletePlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocDelete, id, "fill"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json.erase("fill");
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// Replace should cost the read of the document even if the path isn't found
TEST_F(MeteringTest, SubdocReplaceENoPath) {
    const std::string id = "SubdocReplaceENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocReplace, id, "ENOPATH", "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Replace should cost the read of the document, and the write of the
/// new document
TEST_F(MeteringTest, SubdocReplacePlainDoc) {
    const std::string id = "SubdocReplacePlainDoc";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocReplace, id, "fill", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    json["fill"] = true;
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
              *rsp.getWriteUnits());
}

/// Replace should cost the read of the document, and the write 2x of the
/// new document
TEST_F(MeteringTest, SubdocReplacePlainDoc_Durability) {
    const std::string id = "SubdocReplacePlainDoc_Durability";
    auto json = getJsonDoc();
    const auto value = json.dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocReplace, id, "fill", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    json["fill"] = true;
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
              *rsp.getWriteUnits());
}

/// Replace should cost the full read (including xattrs), and the write of the
/// full size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocReplacePlainDocWithXattr) {
    const std::string id = "SubdocReplacePlainDocWithXattr";
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocReplace, id, "fill", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["fill"] = true;
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// Replace should cost the full read (including xattrs), and the write 2x of
/// the full size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocReplacePlainDocWithXattr_Durability) {
    const std::string id = "SubdocReplacePlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto value = json.dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocReplace, id, "fill", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    json["fill"] = true;
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// Counter should cost the read of the document even if the requested
/// path isn't a counter
TEST_F(MeteringTest, SubdocCounterENoCounter) {
    const std::string id = "SubdocCounterENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocCounter, id, "array", "1"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// Counter should cost the read of the full document and the write of
/// the new document
TEST_F(MeteringTest, SubdocCounterPlainDoc) {
    const std::string id = "SubdocCounterPlainDoc";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocCounter, id, "counter", "1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, value)), *rsp.getWriteUnits());
}

/// Counter should cost the read of the full document and the write 2x of
/// the new document
TEST_F(MeteringTest, SubdocCounterPlainDoc_Durability) {
    const std::string id = "SubdocCounterPlainDoc_Durability";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocCounter, id, "counter", "1"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value)), *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, value)) * 2,
              *rsp.getWriteUnits());
}

/// Counter should cost the read of the full document including xattr and
/// write of the new document with the xattrs copied over
TEST_F(MeteringTest, SubdocCounterPlainDocWithXattr) {
    const std::string id = "SubdocCounterPlainDocWithXattr";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocCounter, id, "counter", "1"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// Counter should cost the read of the full document including xattr and
/// write 2x of the new document with the xattrs copied over
TEST_F(MeteringTest, SubdocCounterPlainDocWithXattr_Durability) {
    const std::string id = "SubdocCounterPlainDocWithXattr_Durability";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocCounter, id, "counter", "1"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, value, "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// GetCount should cost the read of the document even if the path doesn't
/// exists
TEST_F(MeteringTest, SubdocGetCountENoPath) {
    const std::string id = "SubdocGetCountENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, id, "ENOPATH"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetCount should cost the read of the document even if the path doesn't
/// doesn't point to an array
TEST_F(MeteringTest, SubdocGetCountENotArray) {
    const std::string id = "SubdocGetCountENotArray";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, id, "v1"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetCount should cost the read of the entire document
TEST_F(MeteringTest, SubdocGetCountPlainDoc) {
    const std::string id = "SubdocGetCountPlainDoc";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, id, "array"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// GetCount should cost the read of the entire document including xattrs
TEST_F(MeteringTest, SubdocGetCountPlainDocWithXattr) {
    const std::string id = "SubdocGetCountPlainDocWithXattr";
    const auto value = getJsonDoc().dump();
    const auto xattr = getStringValue();
    upsert(id, value, "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocGetCount, id, "array"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, value, "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document even if the path
/// doesn't exist
TEST_F(MeteringTest, SubdocArrayPushLastENoPath) {
    const std::string id = "SubdocArrayPushLastENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 id,
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document even if the path
/// isn't an array
TEST_F(MeteringTest, SubdocArrayPushLastENotArray) {
    const std::string id = "SubdocArrayPushLastENotArray";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushLast,
                                 id,
                                 "counter",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document, and then the
/// write of the new document
TEST_F(MeteringTest, SubdocArrayPushLastPlainDoc) {
    const std::string id = "SubdocArrayPushLastPlainDoc";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, id, "array", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + json.dump().size()), *rsp.getReadUnits());
    json["array"].push_back(true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + json.dump().size()), *rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document, and then 2x the
/// write of the new document
TEST_F(MeteringTest, SubdocArrayPushLastPlainDoc_Durability) {
    const std::string id = "SubdocArrayPushLastPlainDoc_Durability";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, id, "array", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + json.dump().size()), *rsp.getReadUnits());
    json["array"].push_back(true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + json.dump().size()) * 2, *rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document, and then the
/// write of the new document (including xattrs copied over)
TEST_F(MeteringTest, SubdocArrayPushLastPlainDocWithXattr) {
    const std::string id = "SubdocArrayPushLastPlainDocWithXattr";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, id, "array", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].push_back(true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// ArrayPushLast should cost the read of the document, and then 2x the
/// write of the new document (including xattrs copied over)
TEST_F(MeteringTest, SubdocArrayPushLastPlainDocWithXattr_Durability) {
    const std::string id = "SubdocArrayPushLastPlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayPushLast, id, "array", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].push_back(true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document even if the path
/// doesn't exist
TEST_F(MeteringTest, SubdocArrayPushFirstENoPath) {
    const std::string id = "SubdocArrayPushFirstENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 id,
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document even if the path
/// isn't an array
TEST_F(MeteringTest, SubdocArrayPushFirstENotArray) {
    const std::string id = "SubdocArrayPushFirstENotArray";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayPushFirst,
                                 id,
                                 "counter",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document, and then the
/// write of the new document
TEST_F(MeteringTest, SubdocArrayPushFirstPlainDoc) {
    const std::string id = "SubdocArrayPushFirstPlainDoc";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, id, "array", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + json.dump().size()), *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + json.dump().size()), *rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document, and then 2x the
/// write of the new document
TEST_F(MeteringTest, SubdocArrayPushFirstPlainDoc_Durability) {
    const std::string id = "SubdocArrayPushFirstPlainDoc_Durability";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, id, "array", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + json.dump().size()), *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(id.size() + json.dump().size()) * 2, *rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document, and then the
/// write of the new document (including xattrs copied over)
TEST_F(MeteringTest, SubdocArrayPushFirstPlainDocWithXattr) {
    const std::string id = "SubdocArrayPushFirstPlainDocWithXattr";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, id, "array", "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// ArrayPushFirst should cost the read of the document, and then 2x the
/// write of the new document (including xattrs copied over)
TEST_F(MeteringTest, SubdocArrayPushFirstPlainDocWithXattr_Durability) {
    const std::string id = "SubdocArrayPushFirstPlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayPushFirst, id, "array", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, even if the path
/// doesn't exists
TEST_F(MeteringTest, SubdocArrayAddUniqueENoPath) {
    const std::string id = "SubdocArrayAddUniqueENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 id,
                                 "ENOPATH",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, even if the path
/// isn't an array
TEST_F(MeteringTest, SubdocArrayAddUniqueENotArray) {
    const std::string id = "SubdocArrayAddUniqueENotArray";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayAddUnique, id, "v1", "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, even if the array
/// already contains the value
TEST_F(MeteringTest, SubdocArrayAddUniqueEExists) {
    const std::string id = "SubdocArrayAddUniqueEExists";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 id,
                                 "array",
                                 R"("1")"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEexists, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document
TEST_F(MeteringTest, SubdocArrayAddUniquePlainDoc) {
    const std::string id = "SubdocArrayAddUniquePlainDocWithXattr";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 id,
                                 "array",
                                 R"("Unique value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump())),
              *rsp.getReadUnits());
    json["array"].push_back("Unique value");
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
              *rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document
TEST_F(MeteringTest, SubdocArrayAddUniquePlainDoc_Durability) {
    const std::string id = "SubdocArrayAddUniquePlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                             id,
                             "array",
                             R"("Unique value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump())),
              *rsp.getReadUnits());
    json["array"].push_back("Unique value");
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
              *rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, and the write
/// of the size of the new document including the XATTRs copied over
TEST_F(MeteringTest, SubdocArrayAddUniquePlainDocWithXattr) {
    const std::string id = "SubdocArrayAddUniquePlainDocWithXattr";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                                 id,
                                 "array",
                                 R"("Unique value")"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].push_back("Unique value");
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// ArrayAddUnique should cost the read of the document, and the write
/// 2x of the size of the new document including the XATTRs copied over
TEST_F(MeteringTest, SubdocArrayAddUniquePlainDocWithXattr_Durability) {
    const std::string id = "SubdocArrayAddUniquePlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{cb::mcbp::ClientOpcode::SubdocArrayAddUnique,
                             id,
                             "array",
                             R"("Unique value")"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].push_back("Unique value");
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, even if the path
/// doesn't exists
TEST_F(MeteringTest, SubdocArrayInsertENoPath) {
    const std::string id = "SubdocArrayInsertENoPath";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 id,
                                 "ENOPATH.[0]",
                                 "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathEnoent, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, even if the path
/// isn't an array
TEST_F(MeteringTest, SubdocArrayInsertENotArray) {
    const std::string id = "SubdocArrayInsertENotArray";
    const auto value = getJsonDoc().dump();
    upsert(id, value);
    auto rsp = conn->execute(BinprotSubdocCommand{
            cb::mcbp::ClientOpcode::SubdocArrayInsert, id, "v1.[0]", "true"});
    EXPECT_EQ(cb::mcbp::Status::SubdocPathMismatch, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(id.size() + value.size()), *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, and the write of
/// the size of the new document
TEST_F(MeteringTest, SubdocArrayInsertPlainDoc) {
    const std::string id = "SubdocArrayInsertPlainDoc";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 id,
                                 "array.[0]",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump())),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())),
              *rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, and the write 2x of
/// the size of the new document
TEST_F(MeteringTest, SubdocArrayInsertPlainDoc_Durability) {
    const std::string id = "SubdocArrayInsertPlainDoc_Durability";
    auto json = getJsonDoc();
    upsert(id, json.dump());
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayInsert, id, "array.[0]", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump())),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump())) * 2,
              *rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, and the write of
/// the size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocArrayInsertPlainDocWithXattr) {
    const std::string id = "SubdocArrayInsertPlainDocWithXattr";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    auto rsp = conn->execute(
            BinprotSubdocCommand{cb::mcbp::ClientOpcode::SubdocArrayInsert,
                                 id,
                                 "array.[0]",
                                 "true"});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// ArrayInsert should cost the read of the document, and the write 2x of
/// the size of the new document (including the xattrs copied over)
TEST_F(MeteringTest, SubdocArrayInsertPlainDocWithXattr_Durability) {
    const std::string id = "SubdocArrayInsertPlainDocWithXattr_Durability";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);
    BinprotSubdocCommand cmd{
            cb::mcbp::ClientOpcode::SubdocArrayInsert, id, "array.[0]", "true"};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);
    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["array"].insert(json["array"].begin(), true);
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// MultiLookup should cost the read of the full document even if no
/// data gets returned
TEST_F(MeteringTest, SubdocMultiLookupAllMiss) {
    const std::string id = "SubdocMultiLookupAllMiss";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiLookupCommand{
            id,
            {{cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "missing1"},
             {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "missing2"}},
            ::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// MultiLookup should cost the read of the full document
TEST_F(MeteringTest, SubdocMultiLookup) {
    const std::string id = "SubdocMultiLookup";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiLookupCommand{
            id,
            {{cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "array.[0]"},
             {cb::mcbp::ClientOpcode::SubdocGet, SUBDOC_FLAG_NONE, "counter"}},
            ::mcbp::subdoc::doc_flag::None});

    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// MultiMutation should cost the read of the full document even if no
/// updates was made
TEST_F(MeteringTest, SubdocMultiMutationAllFailed) {
    const std::string id = "SubdocMultiMutationAllFailed";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            id,
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo.missing.bar",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo.missing.foo",
              "true"}},
            ::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::SubdocMultiPathFailure, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    EXPECT_FALSE(rsp.getWriteUnits());
}

/// MultiMutation should cost the read of the full document, and write
/// of the full size (including xattrs copied over)
TEST_F(MeteringTest, SubdocMultiMutation) {
    const std::string id = "SubdocMultiMutation";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            id,
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "bar",
              "true"}},
            ::mcbp::subdoc::doc_flag::None});

    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["foo"] = true;
    json["bar"] = true;
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getWriteUnits());
}

/// MultiMutation should cost the read of the full document, and write 2x
/// of the full size (including xattrs copied over)
TEST_F(MeteringTest, SubdocMultiMutation_Durability) {
    const std::string id = "SubdocMultiMutation_Durability";
    auto json = getJsonDoc();
    const auto xattr = getStringValue();
    upsert(id, json.dump(), "xattr", xattr);

    BinprotSubdocMultiMutationCommand cmd{
            id,
            {{cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "foo",
              "true"},
             {cb::mcbp::ClientOpcode::SubdocDictUpsert,
              SUBDOC_FLAG_NONE,
              "bar",
              "true"}},
            ::mcbp::subdoc::doc_flag::None};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);

    auto rsp = conn->execute(cmd);

    EXPECT_EQ(cb::mcbp::Status::Success, rsp.getStatus());
    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(id, json.dump(), "xattr", xattr)),
              *rsp.getReadUnits());
    json["foo"] = true;
    json["bar"] = true;
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, json.dump(), "xattr", xattr)) * 2,
              *rsp.getWriteUnits());
}

/// SubdocReplaceBodyWithXattr should cost the read of the full document,
/// and write of the full size
TEST_F(MeteringTest, SubdocReplaceBodyWithXattr) {
    const std::string id = "SubdocReplaceBodyWithXattr";
    const auto new_value = getJsonDoc().dump();
    const auto old_value = getStringValue(false);
    upsert(id, old_value, "tnx.op.staged", new_value);

    auto rsp = conn->execute(BinprotSubdocMultiMutationCommand{
            id,
            {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}},
             {cb::mcbp::ClientOpcode::SubdocDelete,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}}},
            ::mcbp::subdoc::doc_flag::None});
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(
                      id, old_value, "tnx.op.staged", new_value)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, new_value)),
              *rsp.getWriteUnits());
}

/// SubdocReplaceBodyWithXattr should cost the read of the full document,
/// and write 2x of the full size when durability is enabled
TEST_F(MeteringTest, SubdocReplaceBodyWithXattr_Durability) {
    const std::string id = "SubdocReplaceBodyWithXattr_Durability";
    const auto new_value = getJsonDoc().dump();
    const auto old_value = getStringValue(false);
    upsert(id, old_value, "tnx.op.staged", new_value);

    BinprotSubdocMultiMutationCommand cmd{
            id,
            {{cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}},
             {cb::mcbp::ClientOpcode::SubdocDelete,
              SUBDOC_FLAG_XATTR_PATH,
              "tnx.op.staged",
              {}}},
            ::mcbp::subdoc::doc_flag::None};
    DurabilityFrameInfo fi(cb::durability::Level::MajorityAndPersistOnMaster);
    cmd.addFrameInfo(fi);

    auto rsp = conn->execute(cmd);
    EXPECT_TRUE(rsp.isSuccess()) << rsp.getStatus();

    ASSERT_TRUE(rsp.getReadUnits());
    EXPECT_EQ(to_ru(calculateDocumentSize(
                      id, old_value, "tnx.op.staged", new_value)),
              *rsp.getReadUnits());
    ASSERT_TRUE(rsp.getWriteUnits());
    EXPECT_EQ(to_wu(calculateDocumentSize(id, new_value)) * 2,
              *rsp.getWriteUnits());
}

TEST_F(MeteringTest, TTL_Expiry_Get) {
    const std::string id = "TTL_Expiry_Get";
    const auto value = getJsonDoc().dump();
    const auto xattr_value = getStringValue(false);

    Document doc;
    doc.info.id = id;
    doc.info.expiration = 1;
    doc.value = value;
    conn->mutate(doc, Vbid{0}, MutationType::Set);
    waitForPersistence();

    nlohmann::json before;
    conn->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");
    size_t expiredBefore = std::stoull(getStatForKey("vb_active_expired"));

    // fast forward 2 second and the document should have been expired
    conn->adjustMemcachedClock(
            2, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);

    auto rsp = conn->execute(BinprotGetCommand{id});
    ASSERT_EQ(cb::mcbp::Status::KeyEnoent, rsp.getStatus())
            << "should have been TTL expired";

    // TTL wu calculated at flush
    waitForPersistence();

    nlohmann::json after;
    conn->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");

    size_t expiredAfter = std::stoull(getStatForKey("vb_active_expired"));
    EXPECT_NE(expiredBefore, expiredAfter);

    EXPECT_EQ(1,
              after["wu"].get<std::size_t>() - before["wu"].get<std::size_t>());
    // We can't reset the offset as that would cause ep-engine to disconnect
    // the DCP stream as it doesn't look like it handle the clock going
    // backwards very well:
    //
    //  eq_dcpq:n_0->n_2 - Disconnecting because a message has not been
    //  received for the DCP idle timeout of 360s. Sent last message (e.g.
    //  mutation/noop/streamEnd) 4294967276s ago.
    //  Received last message 4294967276s ago. DCP noop [lastSent:4294967276s,
    //  lastRecv:4294967276s, interval:1s, opaque:10000008, pendingRecv:false],
    //  paused:true, pausedReason:ReadyListEmpty
}

TEST_F(MeteringTest, TTL_Expiry_Compaction) {
    const std::string id = "TTL_Expiry_Compaction";
    const auto value = getJsonDoc().dump();
    const auto xattr_value = getStringValue(false);

    Document doc;
    doc.info.id = id;
    doc.info.expiration = 1;
    doc.value = value;
    conn->mutate(doc, Vbid{0}, MutationType::Set);
    waitForPersistence();

    nlohmann::json before;
    conn->stats(
            [&before](auto k, auto v) { before = nlohmann::json::parse(v); },
            "bucket_details metering");
    size_t expiredBefore = std::stoull(getStatForKey("vb_active_expired"));

    // fast forward another 2 seconds and the document should have been expired
    conn->adjustMemcachedClock(
            4, cb::mcbp::request::AdjustTimePayload::TimeType::Uptime);

    auto admin = cluster->getConnection(0);
    admin->authenticate("@admin", "password");
    admin->selectBucket("metering");
    auto rsp = admin->execute(BinprotCompactDbCommand{});
    EXPECT_TRUE(rsp.isSuccess());
    waitForPersistence();

    nlohmann::json after;
    conn->stats([&after](auto k, auto v) { after = nlohmann::json::parse(v); },
                "bucket_details metering");

    EXPECT_EQ(1,
              after["wu"].get<std::size_t>() - before["wu"].get<std::size_t>());

    size_t expiredAfter = std::stoull(getStatForKey("vb_active_expired"));
    EXPECT_NE(expiredBefore, expiredAfter);
    // We can't reset the offset as that would cause ep-engine to disconnect
    // the DCP stream as it doesn't look like it handle the clock going
    // backwards very well:
    //
    //  eq_dcpq:n_0->n_2 - Disconnecting because a message has not been
    //  received for the DCP idle timeout of 360s. Sent last message (e.g.
    //  mutation/noop/streamEnd) 4294967276s ago.
    //  Received last message 4294967276s ago. DCP noop [lastSent:4294967276s,
    //  lastRecv:4294967276s, interval:1s, opaque:10000008, pendingRecv:false],
    //  paused:true, pausedReason:ReadyListEmpty
}

} // namespace cb::test