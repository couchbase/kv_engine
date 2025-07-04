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

#ifdef USE_FUSION
#include <gmock/gmock.h>
#include <nlohmann/json.hpp>
#include <platform/timeutils.h>

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
    nlohmann::json fusionStats(std::string_view subGroup,
                               std::string_view vbid);

    size_t getStat(std::string_view key);

    void setMemcachedConfig(std::string_view key, size_t value);

    bool waitForUploaderState(Vbid vbid, std::string_view state);

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
    const std::string dbPath = mcd_env->getDbPath();
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
    TestappClientTest::SetUp();
    if (!mcd_env->getTestBucket().isMagma()) {
        GTEST_SKIP();
    }

    // Create a new "admin" connection for the test case and make sure
    // it identifies itself with the test name to make it easier to locate
    // it in the logs we dump if there is a unit test failure.
    connection = adminConnection->clone(true, {}, name);
    connection->authenticate("@admin");
    connection->selectBucket(bucketName);
}

void FusionTest::TearDown() {
    if (!mcd_env->getTestBucket().isMagma()) {
        GTEST_SKIP();
    }

    // Some tests assume the uploader disabled for vbid
    if (fusionStats("uploader", std::to_string(vbid.get()))["state"] ==
        "enabled") {
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
        EXPECT_TRUE(waitForUploaderState(vbid, "enabled"));
    }
    return resp;
}

BinprotResponse FusionTest::stopFusionUploader(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::StopFusionUploader};
    cmd.setVBucket(vbid);
    auto resp = connection->execute(cmd);
    if (resp.isSuccess()) {
        EXPECT_TRUE(waitForUploaderState(vbid, "disabled"));
    }
    return resp;
}

BinprotResponse FusionTest::syncFusionLogstore(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SyncFusionLogstore};
    cmd.setVBucket(vbid);
    return connection->execute(cmd);
}

nlohmann::json FusionTest::fusionStats(std::string_view subGroup,
                                       std::string_view vbid) {
    nlohmann::json res;
    // Note: subGroup and vbid are optional so the final command
    // might be just "fusion" followed by some space. Memcached is
    // expected to be resilient to that, so I don't trim the cmd
    // string here on purpose for stressing the validation code out.
    connection->stats(
            [&res](auto k, auto v) { res = nlohmann::json::parse(v); },
            fmt::format("fusion {} {}", subGroup, vbid));
    return res;
}

size_t FusionTest::getStat(std::string_view key) {
    size_t value;
    connection->stats(
            [&value, key](auto& k, auto& v) {
                if (k == key) {
                    value = std::stoul(v);
                }
            },
            "");
    return value;
}

void FusionTest::setMemcachedConfig(std::string_view key, size_t value) {
    memcached_cfg[key] = value;
    reconfigure();
}

bool FusionTest::waitForUploaderState(Vbid vbid, std::string_view state) {
    return cb::waitForPredicateUntil(
            [this, vbid, state]() {
                return fusionStats("uploader",
                                   std::to_string(vbid.get()))["state"] ==
                       state;
            },
            std::chrono::seconds(5));
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(FusionTest, FusionMigrationRateLimit) {
    size_t migrationRatelimit = getStat("fusion_migration_rate_limit");
    EXPECT_EQ(75_MiB, migrationRatelimit)
            << "Default value of fusion_migration_rate_limit is not as "
               "expected";

    setMemcachedConfig("fusion_migration_rate_limit", 0);
    migrationRatelimit = getStat("fusion_migration_rate_limit");
    EXPECT_EQ(0, migrationRatelimit) << "migration rate limit in magma should "
                                        "be 0 after setting it to 0";

    // All done, unblock the data migration. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
}

TEST_P(FusionTest, FusionSyncRateLimit) {
    size_t syncRatelimit = getStat("fusion_sync_rate_limit");
    EXPECT_EQ(75_MiB, syncRatelimit)
            << "Default value of fusion_sync_rate_limit is not as "
               "expected";

    setMemcachedConfig("fusion_sync_rate_limit", 0);
    syncRatelimit = getStat("fusion_sync_rate_limit");
    EXPECT_EQ(0, syncRatelimit)
            << "sync rate limit in magma should be 0 after setting it to 0";

    // All done, unblock sync uploads. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_sync_rate_limit", 75_MiB);
}

TEST_P(FusionTest, AggregatedStats) {
    try {
        fusionStats({}, {});
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotSupported, e.getReason());
    }
}

TEST_P(FusionTest, InvalidStat) {
    try {
        fusionStats("someInvalidStat", "0");
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Einval, e.getReason());
    }
}

TEST_P(FusionTest, Stat_SyncInfo) {
    const auto json = fusionStats("sync_info", "0");
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.contains("logSeqno"));
    EXPECT_EQ(0, json["logSeqno"]);
    ASSERT_TRUE(json.contains("logTerm"));
    EXPECT_EQ(0, json["logTerm"]);
    ASSERT_TRUE(json.contains("version"));
    EXPECT_EQ(1, json["version"]);
}

TEST_P(FusionTest, Stat_SyncInfo_VbidInvalid) {
    try {
        fusionStats("sync_info", "a");
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::Einval, e.getReason());
    }
}

TEST_P(FusionTest, Stat_SyncInfo_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    try {
        fusionStats("sync_info", "1");
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, Stat_Uploader) {
    auto json = fusionStats("uploader", "0");
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
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto res = fusionStats("uploader", {});
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

TEST_P(FusionTest, Stat_Migration) {
    auto json = fusionStats("migration", "0");
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
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto res = fusionStats("migration", {});
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
    ASSERT_EQ("kv/" + bucketName + "/" + bucketUuid + "/kvstore-" +
                      std::to_string(vbid.get()),
              volumeId);

    // In the next few steps we set up a fake "volume" with some magma data that
    // we'll use later for creating a vbucket by MountVbucket(volume) +
    // SetVBucketState(use_snapshot)

    const std::string dbPath = mcd_env->getDbPath();
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

    /// Delete vbucket (mount fails by EExists otherwise)
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
    const std::vector<std::string> expectedVolumes =
            fusionStats("active_guest_volumes", std::to_string(vbid.get()));
    ASSERT_EQ(1, expectedVolumes.size());
    EXPECT_EQ(guestVolume, expectedVolumes.at(0));

    // All done, unblock the data migration. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_Aggregated) {
    const auto json = fusionStats("active_guest_volumes", {});

    // @todo MB-63679: Implement test with multiple vbuckets in place
    ASSERT_TRUE(json.is_array());
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    try {
        fusionStats("active_guest_volumes", "1");
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, ReleaseStorageSnapshot_Nonexistent) {
    // MB-64494: snapshot uuid reported in the error message allocated by
    // magma. Here big-enough for preventing SSO that hides memory domain
    // alloc issues.
    const auto nonexistentUuid = std::string(1024, 'u');
    auto resp = releaseFusionStorageSnapshot(vbid, nonexistentUuid);
    // @todo MB-66688: Return less generic error
    EXPECT_EQ(cb::mcbp::Status::Einternal, resp.getStatus());
}

TEST_P(FusionTest, GetReleaseStorageSnapshot) {
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
    ASSERT_TRUE(json.contains("logFiles"));
    ASSERT_TRUE(json["logFiles"].is_array());
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
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& res = resp.getDataJson();
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("deks")) << res.dump();
    ASSERT_TRUE(res["deks"].is_array());
}

TEST_P(FusionTest, MountFusionVbucket_NoVolumes) {
    const auto resp = mountVbucket(Vbid(1), nlohmann::json::array());
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
    EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
}

TEST_P(FusionTest, UnmountFusionVbucket_PreviouslyMounted) {
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
    ASSERT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, "1").getStatus());
    EXPECT_EQ(cb::mcbp::Status::Success, syncFusionLogstore(vbid).getStatus());
}

TEST_P(FusionTest, StartFusionUploader) {
    // arg invalid (not string)
    auto resp = startFusionUploader(vbid, 1234);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // arg invalid (not numeric string)
    resp = startFusionUploader(vbid, "abc123");
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // arg invalid (too large numeric string)
    resp = startFusionUploader(vbid, std::string(100, '1'));
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // arg invalid (negative numeric string)
    resp = startFusionUploader(vbid, "-1");
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());

    // arg valid
    resp = startFusionUploader(vbid, "123");
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

TEST_P(FusionTest, StopFusionUploader) {
    // Baseline test, uploader never started, call is a successful NOP
    ASSERT_EQ("disabled", fusionStats("uploader", "0")["state"]);
    EXPECT_EQ(cb::mcbp::Status::Success, stopFusionUploader(vbid).getStatus());
}

TEST_P(FusionTest, ToggleUploader) {
    // Uploader disabled at start
    auto json = fusionStats("uploader", "0");
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
    json = fusionStats("uploader", "0");
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(term, json["term"]);

    // Verify that starting an enabled uploader doesn't fail (it's just a NOP).
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(term)).getStatus());

    // Stop uploader..
    EXPECT_EQ(cb::mcbp::Status::Success, stopFusionUploader(vbid).getStatus());
    // verify stats
    json = fusionStats("uploader", "0");
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
    json = fusionStats("uploader", "0");
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(term, json["term"]);

    // Verify that re-starting a running uploader with a new term is equivalent
    // to Stop + Start(newTerm)
    const auto newTerm = term + 1;
    EXPECT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, std::to_string(newTerm)).getStatus());
    // verify stats
    json = fusionStats("uploader", "0");
    EXPECT_EQ("enabled", json["state"]);
    EXPECT_EQ(newTerm, json["term"]);
}

TEST_P(FusionTest, Stat_UploaderState_KVStoreInvalid) {
    // Note: vbid:1 doesn't exist
    try {
        fusionStats("uploader", "1");
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, GetPrometheusFusionStats) {
    std::array<std::string_view, 30> statKeysExpected = {
            "ep_fusion_namespace",
            "ep_fusion_syncs",
            "ep_fusion_bytes_synced",
            "ep_fusion_logs_migrated",
            "ep_fusion_bytes_migrated",
            "ep_fusion_log_store_size",
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
    json["namespace"] = "cbas/namespace-to-delete/uuid";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
}

TEST_P(FusionTest, DeleteFusionNamespace) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteFusionNamespace};
    nlohmann::json json;
    const std::string dbPath = mcd_env->getDbPath();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    json["logstore_uri"] = "local://" + dbPath + "/logstore";
    json["metadatastore_uri"] = "local://" + dbPath + "/metadatastore";
    json["metadatastore_auth_token"] = "some-token";
    json["namespace"] = "kv/namespace-to-delete/uuid";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

TEST_P(FusionTest, GetFusionNamespaces) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetFusionNamespaces};
    nlohmann::json json;
    const std::string dbPath = mcd_env->getDbPath();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    json["metadatastore_uri"] = "local://" + dbPath + "/metadatastore";
    json["metadatastore_auth_token"] = "some-token";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    const auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
}

#else

/**
 * Test class used to verify the behaviour of fusion APIs in a non-fusion env.
 */
class NonFusionTest : public TestappClientTest {};

TEST_P(NonFusionTest, DeleteFusionNamespace) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteFusionNamespace};
    nlohmann::json json;
    json["logstore_uri"] = "uri1";
    json["metadatastore_uri"] = "uri2";
    json["metadatastore_auth_token"] = "some-token";
    json["namespace"] = "kv/namespace-to-delete/uuid";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    const auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
}

TEST_P(NonFusionTest, GetFusionNamespaces) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetFusionNamespaces};
    nlohmann::json json;
    json["metadatastore_uri"] = "uri";
    json["metadatastore_auth_token"] = "some-token";
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    const auto resp = adminConnection->execute(cmd);
    EXPECT_EQ(cb::mcbp::Status::NotSupported, resp.getStatus());
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         NonFusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

#endif // USE_FUSION
