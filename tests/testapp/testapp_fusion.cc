/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "testapp_client_test.h"

#include <gmock/gmock.h>
#include <nlohmann/json.hpp>
#include <platform/timeutils.h>
#include <utilities/fusion_support.h>

class FusionTest : public TestappClientTest {
protected:
    static void SetUpTestCase();
    void SetUp() override;
    void TearDown() override;

    BinprotResponse mountVbucket(Vbid vbid, const nlohmann::json& volumes);

    /**
     * @param vbid
     * @param snapshotUuid
     * @param validity Timestamp in seconds
     * @return BinprotResponse
     */
    BinprotResponse getFusionStorageSnapshot(Vbid vbid,
                                             std::string_view snapshotUuid,
                                             size_t validity);

    BinprotResponse releaseFusionStorageSnapshot(Vbid vbid,
                                                 std::string_view snapshotUuid);

    BinprotResponse startFusionUploader(Vbid vbid, const nlohmann::json& term);

    BinprotResponse stopFusionUploader(Vbid vbid);

    BinprotResponse syncFusionLogstore(Vbid vbid);

    /**
     * Issues a STAT("fusion <subGroup> <vbid>") call to memcached.
     *
     * @param subGroup
     * @param vbid Note: string type as this function is used for invalid vbid
     *             string (eg non numeric) tests
     * @return The payload, which is always a in json format
     */
    std::pair<cb::engine_errc, nlohmann::json> fusionStats(
            std::string_view subGroup, std::string_view vbid = {});

    std::pair<cb::engine_errc, nlohmann::json> fusionStats(
            std::string_view subGroup, Vbid vbid) {
        return fusionStats(subGroup, std::to_string(vbid.get()));
    }

    std::pair<cb::engine_errc, size_t> getStat(std::string_view key);

    void setMemcachedConfig(std::string_view key, size_t value);

    /// Check to see if Fusion is supported in the current bucket.
    /// In order to support Fusion, the bucket must be a Magma bucket and
    /// the support must be enabled in the build configuration.
    bool isFusionSupportedInBucket() const {
        return mcd_env->getTestBucket().isMagma() && isFusionSupportEnabled();
    }

    /**
     * Waits for the Fusion uploader to be in the specified state for the
     * test vbucket.
     *
     * @param state The expected state of the uploader (eg "enabled",
     * "disabled")
     * @throws std::runtime_error if the uploader does not reach the expected
     *         state within 5 seconds.
     */
    void waitForUploaderState(std::string_view state);

public:
    static constexpr auto logstoreRelativePath = "logstore";
    static constexpr auto chronicleAuthToken = "some-token1!";
    static constexpr auto bucketUuid = "uuid-123";
    static const Vbid vbid;
    std::unique_ptr<MemcachedConnection> connection;
};

const Vbid FusionTest::vbid = Vbid(0);

void FusionTest::SetUpTestCase() {
    createUserConnection = true;
    const auto dbPath = mcd_env->getDbPath().generic_string();
    const auto bucketConfig = fmt::format(
            "magma_fusion_logstore_uri={};magma_fusion_metadatastore_uri={"
            "};chronicle_auth_token={};uuid={}",
            "local://" + dbPath + "/" + logstoreRelativePath,
            "local://" + dbPath + "/metadatastore",
            chronicleAuthToken,
            bucketUuid);
    doSetUpTestCaseWithConfiguration(generate_config(), bucketConfig);

    // Note: magma KVStore creation executes at the first flusher path run,
    // which is asynchronous.
    // We need to ensure that the KVStore is successfully created before
    // executing and Fusion API against it in the various test cases. We would
    // hit sporadic failures by "kvstore invalid" otherwise.
    adminConnection->selectBucket(bucketName);
    adminConnection->store("bump-vb-high-seqno", vbid, {});
    adminConnection->waitForSeqnoToPersist(vbid, 1);
}

void FusionTest::SetUp() {
    TestappTest::SetUp();

    // Create a new "admin" connection for the test case and make sure
    // it identifies itself with the test name to make it easier to locate
    // it in the logs we dump if there is a unit test failure.
    connection = adminConnection->clone(true, {}, name);
    connection->authenticate("@admin");
    connection->selectBucket(bucketName);
}

void FusionTest::TearDown() {
    if (isFusionSupportedInBucket()) {
        // Some tests assume the uploader disabled for vbid
        stopFusionUploader(vbid);
    }
}

BinprotResponse FusionTest::mountVbucket(Vbid vbid,
                                         const nlohmann::json& volumes) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::MountFusionVbucket};
    cmd.setVBucket(vbid);
    nlohmann::json json;
    json["mountPaths"] = volumes;
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    return connection->execute(cmd);
}

BinprotResponse FusionTest::getFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid, size_t validity) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::GetFusionStorageSnapshot};
    cmd.setVBucket(vbid);
    nlohmann::json json;
    json["snapshotUuid"] = snapshotUuid;
    json["validity"] = validity;
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    return connection->execute(cmd);
}

BinprotResponse FusionTest::releaseFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseFusionStorageSnapshot};
    cmd.setVBucket(vbid);
    nlohmann::json json;
    json["snapshotUuid"] = snapshotUuid;
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    return connection->execute(cmd);
}

BinprotResponse FusionTest::startFusionUploader(Vbid vbid,
                                                const nlohmann::json& term) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::StartFusionUploader};
    cmd.setVBucket(vbid);
    nlohmann::json json;
    json["term"] = term;
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto resp = connection->execute(cmd);
    if (resp.isSuccess()) {
        waitForUploaderState("enabled");
    }
    return resp;
}

BinprotResponse FusionTest::stopFusionUploader(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::StopFusionUploader};
    cmd.setVBucket(vbid);
    auto resp = connection->execute(cmd);
    if (resp.isSuccess()) {
        waitForUploaderState("disabled");
    }
    return resp;
}

void FusionTest::waitForUploaderState(std::string_view state) {
    if (!cb::waitForPredicateUntil(
                [this, state]() {
                    auto [ec, json] = fusionStats("uploader", vbid);
                    if (ec != cb::engine_errc::success) {
                        throw std::runtime_error(
                                fmt::format("waitForUploaderState: Failed to "
                                            "fetch fusion uploader stats: {}",
                                            ec));
                    }
                    return json["state"].get<std::string>() == state;
                },
                std::chrono::seconds(5))) {
        throw std::runtime_error(
                fmt::format("waitForUploaderState: Timeout waiting for "
                            "uploader state to be: {}",
                            state));
    }
}

BinprotResponse FusionTest::syncFusionLogstore(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SyncFusionLogstore};
    cmd.setVBucket(vbid);
    return connection->execute(cmd);
}

std::pair<cb::engine_errc, nlohmann::json> FusionTest::fusionStats(
        std::string_view subGroup, std::string_view vbid) {
    try {
        nlohmann::json res;
        // Note: subGroup and vbid are optional so the final command
        // might be just "fusion" followed by some space. Memcached is
        // expected to be resilient to that, so I don't trim the cmd
        // string here on purpose for stressing the validation code out.
        connection->stats(
                [&res](auto&, auto& v) { res = nlohmann::json::parse(v); },
                fmt::format("fusion {} {}", subGroup, vbid));
        return {cb::engine_errc::success, res};
    } catch (const ConnectionError& e) {
        if (e.isNotSupported()) {
            return {cb::engine_errc::not_supported, {}};
        }
        if (e.isInvalidArguments()) {
            return {cb::engine_errc::invalid_arguments, {}};
        }
        if (e.isNotMyVbucket()) {
            return {cb::engine_errc::not_my_vbucket, {}};
        }
        abort();
    }
}

std::pair<cb::engine_errc, size_t> FusionTest::getStat(std::string_view key) {
    cb::engine_errc ec = cb::engine_errc::not_supported;
    size_t value;
    connection->stats(
            [&value, &ec, key](auto& k, auto& v) {
                if (k == key) {
                    value = std::stoul(v);
                    ec = cb::engine_errc::success;
                }
            },
            ""); // we convert empty to null to get engine stats
    return {ec, value};
}

void FusionTest::setMemcachedConfig(std::string_view key, size_t value) {
    memcached_cfg[key] = value;
    reconfigure();
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(FusionTest, FusionMigrationRateLimit) {
    auto [ec, migrationRatelimit] = getStat("fusion_migration_rate_limit");
    if (!isFusionSupportEnabled()) {
        EXPECT_EQ(ec, cb::engine_errc::not_supported);
        return;
    }

    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(75_MiB, migrationRatelimit)
            << "Default value of fusion_migration_rate_limit is not as "
               "expected";

    setMemcachedConfig("fusion_migration_rate_limit", 0);
    std::tie(ec, migrationRatelimit) = getStat("fusion_migration_rate_limit");
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(0, migrationRatelimit)
            << "migration rate limit should be 0 after setting it to 0";

    // All done, unblock the data migration. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
}

TEST_P(FusionTest, FusionSyncRateLimit) {
    auto [ec, syncRatelimit] = getStat("fusion_sync_rate_limit");
    if (!isFusionSupportEnabled()) {
        EXPECT_EQ(ec, cb::engine_errc::not_supported);
        return;
    }

    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(75_MiB, syncRatelimit)
            << "Default value of fusion_sync_rate_limit is not as "
               "expected";

    setMemcachedConfig("fusion_sync_rate_limit", 0);
    std::tie(ec, syncRatelimit) = getStat("fusion_sync_rate_limit");
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(0, syncRatelimit)
            << "sync rate limit in magma should be 0 after setting it to 0";

    // All done, unblock sync uploads. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_sync_rate_limit", 75_MiB);
}

/**
 * The format for the fusion stats command is:
 *    fusion <subGroup> [vbid]
 *
 * where <subGroup> is one of the supported subgroups and is a mandatory
 * argument. Verify that the command fails if the <subGroup> is not
 * present
 */
TEST_P(FusionTest, FusionStatNoSubgroup) {
    const auto [ec, _] = fusionStats({});
    ASSERT_EQ(cb::engine_errc::not_supported, ec);
}

TEST_P(FusionTest, InvalidStat) {
    const auto [ec, _] = fusionStats("someInvalidStat", "0");
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments, ec);
    } else {
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, Stat_SyncInfo) {
    const auto [ec, json] = fusionStats("sync_info", vbid);
    if (!isFusionSupportedInBucket()) {
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.contains("logSeqno"));
    EXPECT_EQ(0, json["logSeqno"]);
    ASSERT_TRUE(json.contains("logTerm"));
    EXPECT_EQ(0, json["logTerm"]);
    ASSERT_TRUE(json.contains("version"));
    EXPECT_EQ(1, json["version"]);
}

TEST_P(FusionTest, Stat_SyncInfo_VbidInvalid) {
    const auto [ec, json] = fusionStats("sync_info", "a");
    if (isFusionSupportedInBucket()) {
        ASSERT_EQ(cb::engine_errc::invalid_arguments, ec);
    } else {
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, Stat_SyncInfo_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    const auto [ec, json] = fusionStats("sync_info", Vbid{1});
    if (isFusionSupportedInBucket()) {
        ASSERT_EQ(cb::engine_errc::not_my_vbucket, ec);
    } else {
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, Stat_Uploader) {
    const auto [ec, json] = fusionStats("uploader", vbid);
    if (!isFusionSupportedInBucket()) {
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains("state"));
    ASSERT_TRUE(json["state"].is_string());
    EXPECT_EQ("disabled", json["state"]);
    ASSERT_TRUE(json.contains("term"));
    ASSERT_TRUE(json["term"].is_number_integer());
    EXPECT_EQ(0, json["term"]);

    ASSERT_TRUE(json.contains("sync_session_completed_bytes"));
    ASSERT_TRUE(json["sync_session_completed_bytes"].is_number_integer());
    EXPECT_EQ(0, json["sync_session_completed_bytes"]);
    ASSERT_TRUE(json.contains("sync_session_total_bytes"));
    ASSERT_TRUE(json["sync_session_total_bytes"].is_number_integer());
    EXPECT_EQ(0, json["sync_session_total_bytes"]);
    ASSERT_TRUE(json.contains("snapshot_pending_bytes"));
    ASSERT_TRUE(json["snapshot_pending_bytes"].is_number_integer());
    EXPECT_EQ(0, json["snapshot_pending_bytes"]);
}

TEST_P(FusionTest, Stat_Uploader_Aggregate) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto [ec, res] = fusionStats("uploader");
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.is_object());
    ASSERT_TRUE(res.contains("vb_0"));
    ASSERT_TRUE(res.contains("vb_1"));

    // verify vb_0 stats
    const auto vb_0 = res["vb_0"];
    ASSERT_FALSE(vb_0.empty());
    ASSERT_TRUE(vb_0.is_object());

    ASSERT_TRUE(vb_0.contains("state"));
    ASSERT_TRUE(vb_0["state"].is_string());
    EXPECT_EQ("disabled", vb_0["state"]);
    ASSERT_TRUE(vb_0.contains("term"));
    ASSERT_TRUE(vb_0["term"].is_number_integer());
    EXPECT_EQ(0, vb_0["term"]);

    ASSERT_TRUE(vb_0.contains("sync_session_completed_bytes"));
    ASSERT_TRUE(vb_0["sync_session_completed_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_0["sync_session_completed_bytes"]);
    ASSERT_TRUE(vb_0.contains("sync_session_total_bytes"));
    ASSERT_TRUE(vb_0["sync_session_total_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_0["sync_session_total_bytes"]);
    ASSERT_TRUE(vb_0.contains("snapshot_pending_bytes"));
    ASSERT_TRUE(vb_0["snapshot_pending_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_0["snapshot_pending_bytes"]);

    // verify vb_1 stats
    const auto vb_1 = res["vb_1"];
    ASSERT_FALSE(vb_1.empty());
    ASSERT_TRUE(vb_1.is_object());

    ASSERT_TRUE(vb_1.contains("state"));
    ASSERT_TRUE(vb_1["state"].is_string());
    EXPECT_EQ("disabled", vb_1["state"]);
    ASSERT_TRUE(vb_1.contains("term"));
    ASSERT_TRUE(vb_1["term"].is_number_integer());
    EXPECT_EQ(0, vb_1["term"]);

    ASSERT_TRUE(vb_1.contains("sync_session_completed_bytes"));
    ASSERT_TRUE(vb_1["sync_session_completed_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_1["sync_session_completed_bytes"]);
    ASSERT_TRUE(vb_1.contains("sync_session_total_bytes"));
    ASSERT_TRUE(vb_1["sync_session_total_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_1["sync_session_total_bytes"]);
    ASSERT_TRUE(vb_1.contains("snapshot_pending_bytes"));
    ASSERT_TRUE(vb_1["snapshot_pending_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_1["snapshot_pending_bytes"]);

    // Delete vb_1
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::DelVbucket};
    cmd.setVBucket(vb1);
    ASSERT_EQ(cb::mcbp::Status::Success, connection->execute(cmd).getStatus());
}

TEST_P(FusionTest, Stat_IsMounted) {
    auto vbid = Vbid(1);
    auto [ec, json] = fusionStats("is_mounted", vbid);
    if (!isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }
    auto key = "is_mounted";
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains(key));
    ASSERT_TRUE(json[key].is_boolean());
    // vbucket is not mounted
    EXPECT_FALSE(json[key]);

    // mount vbucket
    const auto resp = mountVbucket(Vbid(1), nlohmann::json::array());
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();

    json = fusionStats("is_mounted", Vbid(1)).second;
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains(key));
    ASSERT_TRUE(json[key].is_boolean());
    // vbucket is mounted
    EXPECT_TRUE(json[key]);
}

TEST_P(FusionTest, Stat_Migration) {
    auto [ec, json] = fusionStats("migration", vbid);
    if (!isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains("completed_bytes"));
    ASSERT_TRUE(json["completed_bytes"].is_number_integer());
    EXPECT_EQ(0, json["completed_bytes"]);
    ASSERT_TRUE(json.contains("total_bytes"));
    ASSERT_TRUE(json["total_bytes"].is_number_integer());
    EXPECT_EQ(0, json["total_bytes"]);
}

TEST_P(FusionTest, Stat_Migration_Aggregate) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto [ec, res] = fusionStats("migration");
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.is_object());
    ASSERT_TRUE(res.contains("vb_0"));
    ASSERT_TRUE(res.contains("vb_1"));

    // Verify vb_0 stats
    const auto vb_0 = res["vb_0"];
    ASSERT_FALSE(vb_0.empty());
    ASSERT_TRUE(vb_0.is_object());

    ASSERT_TRUE(vb_0.contains("completed_bytes"));
    ASSERT_TRUE(vb_0["completed_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_0["completed_bytes"]);
    ASSERT_TRUE(vb_0.contains("total_bytes"));
    ASSERT_TRUE(vb_0["total_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_0["total_bytes"]);

    // Verify vb_1 stats
    const auto vb_1 = res["vb_0"];
    ASSERT_FALSE(vb_1.empty());
    ASSERT_TRUE(vb_1.is_object());

    ASSERT_TRUE(vb_1.contains("completed_bytes"));
    ASSERT_TRUE(vb_1["completed_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_1["completed_bytes"]);
    ASSERT_TRUE(vb_1.contains("total_bytes"));
    ASSERT_TRUE(vb_1["total_bytes"].is_number_integer());
    EXPECT_EQ(0, vb_1["total_bytes"]);

    // Delete vb_1
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::DelVbucket};
    cmd.setVBucket(vb1);
    ASSERT_EQ(cb::mcbp::Status::Success, connection->execute(cmd).getStatus());
}

/**
 * Active guest volumes are volumes involved in a "migration" process in fusion.
 * "migration" is the process of loading some previously mounted volume's data
 * when a VBucket is created by SetVBucketState(use_snapshot).
 *
 * So for testing out the "active_guest_volumes" stat we do:
 *  - Create a vbucket and store some data into it
 *  - Sync that data to the fusion logstore
 *  - Prepare a "volume" and copy the fusion logstore data to it
 *  - NOTE: Steps so far are just preliminary steps for ending up with some
 *          magma data files on a volume. That volume is used in the next steps
 *          for initiating a migration process. As "volume" we use just a local
 *          directory in the local filesystem.
 *  - Delete the vbucket
 *  - MountVBucket(volume)
 *  - Recreate the vbucket by SetVBucketState(use_snapshot). That initiates the
 *    migration process.
 *  - Verify that STAT("active_guest_volumes") returns the volume involved in
 *    the migration process.
 */
TEST_P(FusionTest, Stat_ActiveGuestVolumes) {
    if (!isFusionSupportedInBucket()) {
        auto [ec, json] = fusionStats("active_guest_volumes", vbid);
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }

    // We need to read back "active guest volumes" during a data migration
    // triggered by mountVBucket(volumes). Volumes are considered "active" only
    // during the transfer, so we need to start and "stall" the migration for
    // reading that information back.
    setMemcachedConfig("fusion_migration_rate_limit", 0);

    // Start uploader (necessary before SyncFusionLogstore)
    const auto term = "1";
    ASSERT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, term).getStatus());

    // Store some data
    connection->store("bump-vb-high-seqno", vbid, {});
    connection->waitForSeqnoToPersist(vbid, 1);
    // And SyncFusionLogstore. That creates the log-<term>.1 file in the
    // logstore.
    syncFusionLogstore(vbid);

    // Create a snapshot - That returns the volumeID
    const auto snapshotUuid = "some-snapshot-uuid";
    const auto tp = std::chrono::system_clock::now() + std::chrono::minutes(10);
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    const auto validity = secs.time_since_epoch().count();
    const auto resp = getFusionStorageSnapshot(vbid, snapshotUuid, validity);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    const auto& snapshotData = resp.getDataJson();
    ASSERT_FALSE(snapshotData.empty());
    ASSERT_TRUE(snapshotData.contains("volumeID"));
    const auto volumeId = snapshotData["volumeID"].get<std::string>();
    ASSERT_EQ(fmt::format("kv/{}/kvstore-{}", bucketUuid, vbid.get()),
              volumeId);

    // In the next few steps we set up a fake "volume" with some magma data that
    // we'll use later for creating a vbucket by MountVbucket(volume) +
    // SetVBucketState(use_snapshot)

    const auto dbPath = mcd_env->getDbPath().generic_string();
    ASSERT_TRUE(std::filesystem::exists(dbPath));

    // Create guest volumes (just using a folder within the test path)
    const std::string guestVolume = dbPath + "/guest_volume";
    ASSERT_TRUE(std::filesystem::create_directory(guestVolume));

    // Create guest volume directory
    const std::string guestVolumePath = guestVolume + "/" + volumeId;
    ASSERT_TRUE(std::filesystem::create_directories(guestVolumePath));

    // Copy data from the fusion logstore to the guest volume directory
    const auto volumeIdPathInLogstore =
            dbPath + "/" + logstoreRelativePath + "/" + volumeId;
    ASSERT_TRUE(std::filesystem::exists(volumeIdPathInLogstore));
    std::filesystem::copy(volumeIdPathInLogstore,
                          guestVolumePath,
                          std::filesystem::copy_options::recursive);

    // Delete vbucket (mount fails by EExists otherwise)
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::DelVbucket};
    cmd.setVBucket(vbid);
    ASSERT_EQ(cb::mcbp::Status::Success, connection->execute(cmd).getStatus());

    // Mount by providing the given volume
    ASSERT_EQ(cb::mcbp::Status::Success,
              mountVbucket(vbid, {guestVolume}).getStatus());

    // Create vbucket from volume data
    connection->setVbucket(
            vbid, vbucket_state_active, {{"use_snapshot", "fusion"}});

    // Verify active_guest_volumes stat
    // Note: Implicit format check, this throws if json isn't list of strings
    auto [ec, json] = fusionStats("active_guest_volumes", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);

    const std::vector<std::string> expectedVolumes = json;
    ASSERT_EQ(1, expectedVolumes.size());
    EXPECT_EQ(guestVolume, expectedVolumes.at(0));

    // All done, unblock the data migration. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_Aggregated) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    const auto [ec, json] = fusionStats("active_guest_volumes");
    ASSERT_EQ(cb::engine_errc::success, ec);

    // @todo MB-63679: Implement test with multiple vbuckets in place
    ASSERT_TRUE(json.is_array());
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    const auto [ec, json] = fusionStats("active_guest_volumes", Vbid{1});
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::not_my_vbucket, ec);
    } else {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, ReleaseStorageSnapshot_Nonexistent) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // MB-64494: snapshot uuid reported in the error message allocated by
    // magma. Here big-enough for preventing SSO that hides memory domain
    // alloc issues.
    const auto nonexistentUuid = std::string(1024, 'u');
    auto resp = releaseFusionStorageSnapshot(vbid, nonexistentUuid);
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
}

TEST_P(FusionTest, GetReleaseStorageSnapshot) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Create a snapshot
    // MB-65649: snaps uuid reported in the GetFusionStorageSnapshot response.
    // Here big-enough for preventing SSO that hides memory domain alloc issues.
    const auto snapshotUuid = std::string(1024, 'u');

    const auto tp = std::chrono::system_clock::now() + std::chrono::minutes(10);
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    const auto validity = secs.time_since_epoch().count();

    const auto resp = getFusionStorageSnapshot(vbid, snapshotUuid, validity);
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& json = resp.getDataJson();
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.contains("createdAt"));
    EXPECT_NE(0, json["createdAt"]);
    ASSERT_TRUE(json.contains("logManifestName"));
    ASSERT_TRUE(json.contains("snapshotUUID"));
    EXPECT_EQ(snapshotUuid, json["snapshotUUID"].get<std::string>());
    ASSERT_TRUE(json.contains("validTill"));
    ASSERT_TRUE(json.contains("version"));
    EXPECT_EQ(1, json["version"]);
    ASSERT_TRUE(json.contains("volumeID"));
    EXPECT_FALSE(json["volumeID"].empty());

    // Then release it
    EXPECT_EQ(cb::mcbp::Status::Success,
              releaseFusionStorageSnapshot(vbid, snapshotUuid).getStatus());
}

TEST_P(FusionTest, MountFusionVbucket_InvalidArgs) {
    auto resp = mountVbucket(Vbid(1), {1, 2});
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, MountFusionVbucket) {
    const auto resp = mountVbucket(Vbid(1), {"path1", "path2"});
    if (!isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
        return;
    }
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& res = resp.getDataJson();
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("deks")) << res.dump();
    ASSERT_TRUE(res["deks"].is_array());
}

TEST_P(FusionTest, MountFusionVbucket_NoVolumes) {
    const auto resp = mountVbucket(Vbid(1), nlohmann::json::array());
    if (!isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
        return;
    }
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& res = resp.getDataJson();
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("deks")) << res.dump();
    ASSERT_TRUE(res["deks"].is_array());
}

TEST_P(FusionTest, UnmountFusionVbucket_NeverMounted) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::UnmountFusionVbucket};
    cmd.setVBucket(Vbid(1));
    const auto res = connection->execute(cmd);
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, res.getStatus());
    }
}

TEST_P(FusionTest, UnmountFusionVbucket_PreviouslyMounted) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    const Vbid vbid{1};
    const auto resp = mountVbucket(vbid, {"path1", "path2"});
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::UnmountFusionVbucket};
    cmd.setVBucket(vbid);
    const auto res = connection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
}

TEST_P(FusionTest, SyncFusionLogstore) {
    if (isFusionSupportedInBucket()) {
        ASSERT_EQ(cb::mcbp::Status::Success,
                  startFusionUploader(vbid, "1").getStatus());
    }
    auto res = syncFusionLogstore(vbid);
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, res.getStatus());
    }
}

TEST_P(FusionTest, StartFusionUploaderInvalidTerm_not_string) {
    // arg invalid (not string)
    auto resp = startFusionUploader(vbid, 1234);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, StartFusionUploaderInvalidTerm_not_numeric_string) {
    // arg invalid (not numeric string)
    auto resp = startFusionUploader(vbid, "abc123");
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, StartFusionUploaderInvalidTerm_too_long_string) {
    // arg invalid (too large numeric string)
    auto resp = startFusionUploader(vbid, std::string(100, '1'));
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, StartFusionUploaderInvalidTerm_negative_number) {
    // arg invalid (negative numeric string)
    auto resp = startFusionUploader(vbid, "-1");
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, StartFusionUploader) {
    auto resp = startFusionUploader(vbid, "123");
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
    }
}

TEST_P(FusionTest, StopFusionUploader) {
    if (isFusionSupportedInBucket()) {
        // Baseline test, uploader never started, call is a successful NOP
        const auto [ec, json] = fusionStats("uploader", vbid);
        ASSERT_EQ(cb::engine_errc::success, ec);
        ASSERT_EQ("disabled", json["state"]);
    }

    auto resp = stopFusionUploader(vbid);
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
    }
}

TEST_P(FusionTest, ToggleUploader) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Uploader disabled at start
    auto [ec, json] = fusionStats("uploader", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains("state"));
    ASSERT_TRUE(json["state"].is_string());
    EXPECT_EQ("disabled", json["state"]);
    ASSERT_TRUE(json.contains("term"));
    ASSERT_TRUE(json["term"].is_number_integer());
    EXPECT_EQ(0, json["term"]);

    // Start uploader..
    const uint64_t term = 123;
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(term)).getStatus());
    // verify stats
    std::tie(ec, json) = fusionStats("uploader", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(term, json["term"]);

    // Verify that starting an enabled uploader doesn't fail (it's just a NOP).
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(term)).getStatus());

    // Stop uploader..
    EXPECT_EQ(cb::mcbp::Status::Success, stopFusionUploader(vbid).getStatus());
    // verify stats
    std::tie(ec, json) = fusionStats("uploader", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    EXPECT_EQ("disabled", json["state"]);
    EXPECT_EQ(0, json["term"]);

    // Verify that stopping a disabled uploader doesn't fail (it's just a NOP).
    EXPECT_EQ(cb::mcbp::Status::Success, stopFusionUploader(vbid).getStatus());

    // Try start uploader again.
    // This step verifies that internally at StopUploader we have correctly
    // cleared vbid for accepting new StartUploader requests.
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(term)).getStatus());
    // verify stats
    std::tie(ec, json) = fusionStats("uploader", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(term, json["term"]);

    // Verify that re-starting a running uploader with a new term is equivalent
    // to Stop + Start(newTerm)
    const auto newTerm = term + 1;
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(newTerm)).getStatus());
    // verify stats
    std::tie(ec, json) = fusionStats("uploader", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(newTerm, json["term"]);
}

TEST_P(FusionTest, Stat_UploaderState_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    const auto [ec, _] = fusionStats("uploader", Vbid{1});
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::not_my_vbucket, ec);
    } else {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, GetPrometheusFusionStats) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    std::array<std::string_view, 30> statKeysExpected = {
            "ep_fusion_namespace",
            "ep_fusion_syncs",
            "ep_fusion_bytes_synced",
            "ep_fusion_logs_migrated",
            "ep_fusion_bytes_migrated",
            "ep_fusion_log_store_data_size",
            "ep_fusion_log_store_garbage_size",
            "ep_fusion_logs_cleaned",
            "ep_fusion_log_clean_bytes_read",
            "ep_fusion_extent_merger_reads",
            "ep_fusion_extent_merger_bytes_read",
            "ep_fusion_log_clean_reads",
            "ep_fusion_log_store_remote_puts",
            "ep_fusion_log_store_reads",
            "ep_fusion_log_store_remote_gets",
            "ep_fusion_log_store_remote_lists",
            "ep_fusion_log_store_remote_deletes",
            "ep_fusion_file_map_mem_used",
            "ep_fusion_sync_failures",
            "ep_fusion_migration_failures",
            "ep_fusion_num_logs_mounted",
            "ep_fusion_num_log_segments",
            "ep_fusion_num_file_extents",
            "ep_fusion_num_files",
            "ep_fusion_total_file_size",
            "ep_fusion_sync_session_total_bytes",
            "ep_fusion_sync_session_completed_bytes",
            "ep_fusion_migration_total_bytes",
            "ep_fusion_migration_completed_bytes",
            "ep_fusion_log_store_pending_delete_size",
    };
    std::vector<std::string> actualKeys;
    connection->stats(
            [&actualKeys](auto& k, auto& v) {
                if (k.starts_with("ep_fusion")) {
                    actualKeys.emplace_back(k);
                }
            },
            ""); // we convert empty to null to get engine stats

    // Sort both collections
    std::ranges::sort(actualKeys);
    std::ranges::sort(statKeysExpected);

    // Find missing keys that are expected but not found in actual
    std::vector<std::string_view> missingKeys;
    std::ranges::set_difference(
            statKeysExpected,
            actualKeys,
            std::inserter(missingKeys, missingKeys.begin()));

    bool error = false;
    for (const auto& key : missingKeys) {
        error = true;
        fmt::println(stderr, "Expected {} but not found in actual", key);
    }

    // Find any extra fusion stats - those that are found in actual but not
    // expected
    std::vector<std::string_view> extraKeys;
    std::ranges::set_difference(actualKeys,
                                statKeysExpected,
                                std::inserter(extraKeys, extraKeys.begin()));

    for (const auto& key : extraKeys) {
        error = true;
        fmt::println(stderr, "Found stat {} but was not expected", key);
    }

    EXPECT_EQ(false, error) << "Missing stats found";
}

TEST_P(FusionTest, DeleteInvalidFusionNamespace) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteFusionNamespace};
    nlohmann::json json;
    json["logstore_uri"] = "uri1";
    json["metadatastore_uri"] = "uri2";
    json["metadatastore_auth_token"] = "some-token";
    // Invalid namespace: does not begin with "kv/"
    json["namespace"] = "cbas/uuid";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, DeleteFusionNamespace) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteFusionNamespace};
    nlohmann::json json;
    const auto dbPath = mcd_env->getDbPath().generic_string();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    json["logstore_uri"] = "local://" + dbPath + "/logstore";
    json["metadatastore_uri"] = "local://" + dbPath + "/metadatastore";
    json["metadatastore_auth_token"] = "some-token";
    json["namespace"] = "kv/uuid";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    const auto resp = adminConnection->execute(cmd);
    if (isFusionSupportEnabled()) {
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
    }
}

TEST_P(FusionTest, GetFusionNamespaces) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetFusionNamespaces};
    nlohmann::json json;
    const auto dbPath = mcd_env->getDbPath().generic_string();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    json["metadatastore_uri"] = "local://" + dbPath + "/metadatastore";
    json["metadatastore_auth_token"] = "some-token";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    const auto resp = adminConnection->execute(cmd);
    if (isFusionSupportEnabled()) {
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    } else {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
    }
}
