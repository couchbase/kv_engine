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
#include <utilities/magma_support.h>

class FusionTest : public TestappClientTest {
protected:
    /**
     * Helper function to force at least one mutation stored to disk for the
     * given vbucket, which ensures that the related KVStore is created in the
     * storage.
     *
     * @param vbid
     */
    static void ensureKVStoreCreated(Vbid vbid);

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
    BinprotResponse getFusionStorageSnapshot(
            const std::vector<Vbid>& vbucketList,
            std::string_view snapshotUuid,
            std::string_view bucketUuid,
            std::string_view metadatastoreUri,
            std::string_view metadatastoreAuthToken,
            size_t validity);

    BinprotResponse releaseFusionStorageSnapshot(
            const std::vector<Vbid>& vbucketList,
            std::string_view snapshotUuid,
            std::string_view bucketUuid,
            std::string_view metadatastoreUri,
            std::string_view metadatastoreAuthToken);

    BinprotResponse startFusionUploader(Vbid vbid, const nlohmann::json& term);

    BinprotResponse stopFusionUploader(Vbid vbid);

    BinprotResponse syncFusionLogstore(Vbid vbid);

    /**
     * @param vbid
     * @return The full local path used as "guest volume" in a data migration
     *  process for the given vbucket.
     */
    std::filesystem::path getGuestVolumeFullPath(Vbid vbid) const;

    /**
     * This helper function's purpose is mainly testing out Active Guest
     * Volumes.
     *
     * Active guest volumes are volumes involved in a "migration" process in
     * fusion. "migration" is the process of loading some previously mounted
     * volume's data when a VBucket is created by SetVBucketState(use_snapshot).
     *
     * So for testing out the "active_guest_volumes" stat we do:
     *  - Create a vbucket and store some data into it
     *  - Sync that data to the fusion logstore
     *  - Prepare a "volume" and copy the fusion logstore data to it
     *  - NOTE: Steps so far are just preliminary steps for ending up with some
     *          magma data files on a volume. That volume is used in the next
     *          steps for initiating a migration process. As "volume" we use
     *          just a local directory in the local filesystem.
     *  - Delete the vbucket
     *  - MountVBucket(volume)
     *  - Recreate the vbucket by SetVBucketState(use_snapshot). That initiates
     *    the migration process.
     *  - During the migration STAT("active_guest_volumes") returns the volume
     *    involved in the process.
     *
     * @param vbid
     */
    void recreateVbucketByMount(Vbid vbid);

    /**
     * Issues a STAT("fusion <subGroup> <arg>") call to memcached.
     *
     * @param subGroup
     * @param arg Note: string type as this second optional arg
     *              might represent Vbid or other special string
     *              tokens. Plus, this function is used for invalid
     *              Vbid string (eg non numeric) tests
     * @return The payload, which is always a in json format
     */
    std::pair<cb::engine_errc, nlohmann::json> fusionStats(
            std::string_view subGroup, std::string_view arg = {});

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
     * given vbucket.
     *
     * @param vbid
     * @param state The expected state of the uploader (eg "enabled",
     * "disabled")
     * @throws std::runtime_error if the uploader does not reach the expected
     *         state within 5 seconds.
     */
    void waitForUploaderState(Vbid vbid, std::string_view state);

public:
    static constexpr auto logstoreRelativePath = "logstore";
    static constexpr auto chronicleAuthToken = "some-token1!";
    static constexpr auto bucketUuid = "uuid-123";
    static constexpr auto guestVolumePrefix = "guest_volume_vbid_";
    static const Vbid vbid;
    std::unique_ptr<MemcachedConnection> connection;
};

const Vbid FusionTest::vbid = Vbid(0);

void FusionTest::ensureKVStoreCreated(Vbid vbid) {
    // Note: magma KVStore creation executes at the first flusher path run,
    // which is asynchronous.
    // We need to ensure that the KVStore is successfully created before
    // executing and Fusion API against it in the various test cases. We would
    // hit sporadic failures by "kvstore invalid" otherwise.
    adminConnection->selectBucket(bucketName);
    const auto lookForStat = "vb_" + std::to_string(vbid.get()) + ":high_seqno";
    uint64_t highSeqno{};
    adminConnection->stats(
            [&lookForStat, &highSeqno](auto& k, auto& v) {
                if (k.contains(lookForStat)) {
                    highSeqno = std::stoull(v);
                    return true; // stop executing this callback
                }
                return false;
            },
            fmt::format("vbucket-details {}", vbid.get()));
    adminConnection->store("bump-vb-high-seqno", vbid, {});
    adminConnection->waitForSeqnoToPersist(vbid, highSeqno + 1);
}

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
    ensureKVStoreCreated(vbid);
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
        const std::vector<Vbid>& vbucketList,
        std::string_view snapshotUuid,
        std::string_view bucketUuid,
        std::string_view metadatastoreUri,
        std::string_view metadatastoreAuthToken,
        size_t validity) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::GetFusionStorageSnapshot};
    nlohmann::json json;
    json["snapshot_uuid"] = snapshotUuid;
    json["bucket_uuid"] = bucketUuid;
    json["metadatastore_uri"] = metadatastoreUri;
    json["metadatastore_auth_token"] = metadatastoreAuthToken;
    json["vbucket_list"] = nlohmann::json::array();
    for (const auto& vb : vbucketList) {
        json["vbucket_list"].push_back(vb.get());
    }
    json["valid_till"] = validity;
    cmd.setValue(json.dump());
    cmd.setDatatype(cb::mcbp::Datatype::JSON);
    return connection->execute(cmd);
}

BinprotResponse FusionTest::releaseFusionStorageSnapshot(
        const std::vector<Vbid>& vbucketList,
        std::string_view snapshotUuid,
        std::string_view bucketUuid,
        std::string_view metadatastoreUri,
        std::string_view metadatastoreAuthToken) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ReleaseFusionStorageSnapshot};
    nlohmann::json json;
    json["snapshot_uuid"] = snapshotUuid;
    json["bucket_uuid"] = bucketUuid;
    json["metadatastore_uri"] = metadatastoreUri;
    json["metadatastore_auth_token"] = metadatastoreAuthToken;
    json["vbucket_list"] = nlohmann::json::array();
    for (const auto& vb : vbucketList) {
        json["vbucket_list"].push_back(vb.get());
    }
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
        waitForUploaderState(vbid, "enabled");
    }
    return resp;
}

BinprotResponse FusionTest::stopFusionUploader(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::StopFusionUploader};
    cmd.setVBucket(vbid);
    auto resp = connection->execute(cmd);
    if (resp.isSuccess()) {
        waitForUploaderState(vbid, "disabled");
    }
    return resp;
}

void FusionTest::waitForUploaderState(Vbid vbid, std::string_view state) {
    if (!cb::waitForPredicateUntil(
                [this, vbid, state]() {
                    auto [ec, json] = fusionStats("uploader", vbid);
                    if (ec != cb::engine_errc::success) {
                        throw std::runtime_error(fmt::format(
                                "waitForUploaderState: Failed to fetch fusion "
                                "uploader stats for {} state:{}: {}",
                                vbid,
                                state,
                                ec));
                    }
                    return json["state"].get<std::string>() == state;
                },
                std::chrono::seconds(5))) {
        throw std::runtime_error(fmt::format(
                "waitForUploaderState: Timeout {} state:{}", vbid, state));
    }
}

BinprotResponse FusionTest::syncFusionLogstore(Vbid vbid) {
    auto cmd =
            BinprotGenericCommand{cb::mcbp::ClientOpcode::SyncFusionLogstore};
    cmd.setVBucket(vbid);
    return connection->execute(cmd);
}

std::pair<cb::engine_errc, nlohmann::json> FusionTest::fusionStats(
        std::string_view subGroup, std::string_view arg) {
    try {
        nlohmann::json res;
        // Note: subGroup and vbid are optional so the final command
        // might be just "fusion" followed by some space. Memcached is
        // expected to be resilient to that, so I don't trim the cmd
        // string here on purpose for stressing the validation code out.
        connection->stats(
                [&res](auto&, auto& v) { res = nlohmann::json::parse(v); },
                fmt::format("fusion {} {}", subGroup, arg));
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
    // Note: This encodes "fusion <vbid>"
    const auto [ec, _] = fusionStats({"0"});
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments, ec);
    } else {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
    }
}

TEST_P(FusionTest, FusionStatNoSubgroupNoVbid) {
    const auto [ec, _] = fusionStats({});
    if (isFusionSupportedInBucket()) {
        EXPECT_EQ(cb::engine_errc::invalid_arguments, ec);
    } else {
        EXPECT_EQ(cb::engine_errc::not_supported, ec);
    }
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

TEST_P(FusionTest, Stat_Uploader_Detail) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto [ec, res] = fusionStats("uploader", "detail");
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

TEST_P(FusionTest, Stat_Migration_Detail) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }
    // Create second vbucket
    const auto vb1 = Vbid(1);
    connection->setVbucket(vb1, vbucket_state_active, {});
    connection->store("bump-vb-high-seqno", vb1, {});
    connection->waitForSeqnoToPersist(Vbid(1), 1);

    const auto [ec, res] = fusionStats("migration", "detail");
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

std::filesystem::path FusionTest::getGuestVolumeFullPath(Vbid vbid) const {
    const auto dbPath = mcd_env->getDbPath();
    EXPECT_TRUE(exists(dbPath));
    return dbPath / (guestVolumePrefix + std::to_string(vbid.get()));
}

void FusionTest::recreateVbucketByMount(Vbid vbid) {
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
    const auto dbPath = mcd_env->getDbPath();
    ASSERT_TRUE(exists(dbPath));
    const auto metadatastoreUri =
            "local://" + dbPath.generic_string() + "/metadatastore";
    const auto metadatastoreAuthToken = "some-token";
    const auto tp = std::chrono::system_clock::now() + std::chrono::minutes(10);
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    const auto validity = secs.time_since_epoch().count();
    std::vector vbucketList{vbid};
    // GetFusionStorageSnapshot is expected to be called after the uploader
    // has been started on the vb, as part of which, the entry is created on
    // chronicle.
    ASSERT_EQ(cb::mcbp::Status::Success,
              startFusionUploader(vbid, "1").getStatus());
    const auto resp = getFusionStorageSnapshot(vbucketList,
                                               snapshotUuid,
                                               bucketUuid,
                                               metadatastoreUri,
                                               metadatastoreAuthToken,
                                               validity);
    ASSERT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    const auto& respData = resp.getDataJson();
    ASSERT_FALSE(respData.empty());
    ASSERT_TRUE(respData.is_array());
    const auto snapshotData = respData[0];
    ASSERT_TRUE(snapshotData.contains("volumeID"));
    const auto volumeId = snapshotData["volumeID"].get<std::string>();
    ASSERT_EQ(fmt::format("kv/{}/kvstore-{}", bucketUuid, vbid.get()),
              volumeId);

    // In the next few steps we set up a fake "volume" with some magma data that
    // we'll use later for creating a vbucket by MountVbucket(volume) +
    // SetVBucketState(use_snapshot)

    // Create guest volumes (just using a folder within the test path)
    const auto guestVolume = getGuestVolumeFullPath(vbid);
    if (!exists(guestVolume)) {
        ASSERT_TRUE(create_directory(guestVolume));
    }

    // Create guest volume directory
    const auto guestVolumePath = guestVolume / volumeId;
    ASSERT_TRUE(std::filesystem::create_directories(guestVolumePath));

    // Copy data from the fusion logstore to the guest volume directory
    ASSERT_TRUE(exists(dbPath));
    const auto volumeIdPathInLogstore =
            dbPath / logstoreRelativePath / volumeId;
    ASSERT_TRUE(exists(volumeIdPathInLogstore));
    copy(volumeIdPathInLogstore,
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
}

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

    // Perform data migration
    recreateVbucketByMount(vbid);

    // Verify active_guest_volumes stat
    // Note: Implicit format check, this throws if json isn't list of strings
    auto [ec, json] = fusionStats("active_guest_volumes", vbid);
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_EQ(1, json.size());
    const std::vector<std::string> resVolumes = json;
    const auto expectedGuestVolume = getGuestVolumeFullPath(vbid);
    EXPECT_EQ(expectedGuestVolume.generic_string(), resVolumes.at(0));

    // All done, unblock the data migration. The test process would get stuck
    // otherwise.
    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_Aggregate) {
    if (!isFusionSupportedInBucket()) {
        auto [ec, json] = fusionStats("active_guest_volumes", vbid);
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }

    // Create another vbucket (vbid:0 already exists)
    const auto vbid9 = Vbid(9);
    nlohmann::json meta;
    meta["topology"] = nlohmann::json::array({{"active"}});
    adminConnection->setVbucket(vbid9, vbucket_state_active, meta);
    ensureKVStoreCreated(vbid9);

    setMemcachedConfig("fusion_migration_rate_limit", 0);

    recreateVbucketByMount(vbid9);

    // Verify active_guest_volumes stat
    // Note: Implicit format check, this throws if json isn't list of strings
    auto [ec, json] = fusionStats("active_guest_volumes");
    ASSERT_EQ(cb::engine_errc::success, ec);
    ASSERT_EQ(2, json.size());
    const std::vector<std::string> resVolumes = json;
    const std::vector<std::string> expected = {
            getGuestVolumeFullPath(vbid9).generic_string(),
            getGuestVolumeFullPath(vbid).generic_string()};
    EXPECT_EQ(expected, resVolumes);

    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);

    // Clean up
    auto cmd = BinprotGenericCommand{cb::mcbp::ClientOpcode::DelVbucket};
    cmd.setVBucket(vbid9);
    EXPECT_EQ(cb::mcbp::Status::Success,
              adminConnection->execute(cmd).getStatus());
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_Detail) {
    if (!isFusionSupportedInBucket()) {
        auto [ec, json] = fusionStats("active_guest_volumes", "detail");
        ASSERT_EQ(cb::engine_errc::not_supported, ec);
        return;
    }

    // Create 2 vbuckets
    const auto vbid5 = Vbid(5);
    const auto vbid7 = Vbid(7);
    nlohmann::json meta;
    meta["topology"] = nlohmann::json::array({{"active"}});
    adminConnection->setVbucket(vbid5, vbucket_state_active, meta);
    ensureKVStoreCreated(vbid5);
    adminConnection->setVbucket(vbid7, vbucket_state_active, meta);
    ensureKVStoreCreated(vbid7);

    setMemcachedConfig("fusion_migration_rate_limit", 0);

    recreateVbucketByMount(vbid5);
    recreateVbucketByMount(vbid7);

    // Verify active_guest_volumes stat
    auto [ec, json] = fusionStats("active_guest_volumes", "detail");
    ASSERT_EQ(cb::engine_errc::success, ec);

    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_object());
    ASSERT_TRUE(json.contains("vb_5"));
    const auto vb_5 = json["vb_5"];
    ASSERT_TRUE(vb_5.is_array());
    ASSERT_TRUE(json.contains("vb_7"));
    const auto vb_7 = json["vb_7"];
    ASSERT_TRUE(vb_7.is_array());

    const std::vector<std::string> expected5 = {
            getGuestVolumeFullPath(vbid5).generic_string()};
    const std::vector<std::string> volume5 = json["vb_5"];
    EXPECT_EQ(expected5, volume5);
    const std::vector<std::string> expected7 = {
            getGuestVolumeFullPath(vbid7).generic_string()};
    const std::vector<std::string> volume7 = json["vb_7"];
    EXPECT_EQ(expected7, volume7);

    setMemcachedConfig("fusion_migration_rate_limit", 75_MiB);
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
    const auto dbPath = mcd_env->getDbPath().generic_string();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    const auto metadatastoreUri = "local://" + dbPath + "/metadatastore";
    const auto metadatastoreAuthToken = "some-token";
    std::vector vbucketList{vbid};
    auto resp = releaseFusionStorageSnapshot(vbucketList,
                                             nonexistentUuid,
                                             bucketUuid,
                                             metadatastoreUri,
                                             metadatastoreAuthToken);
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
}

TEST_P(FusionTest, GetReleaseStorageSnapshot) {
    if (!isFusionSupportedInBucket()) {
        GTEST_SKIP() << "Fusion is not supported in this bucket";
    }

    // Create another vbucket (vbid:0 already exists)
    nlohmann::json meta;
    meta["topology"] = nlohmann::json::array({{"active"}});
    adminConnection->setVbucket(Vbid(1), vbucket_state_active, meta);
    ensureKVStoreCreated(Vbid(1));

    // Create a snapshot
    // MB-65649: snaps uuid reported in the GetFusionStorageSnapshot response.
    // Here big-enough for preventing SSO that hides memory domain alloc issues.
    const auto snapshotUuid = std::string(1024, 'u');
    const auto dbPath = mcd_env->getDbPath().generic_string();
    ASSERT_TRUE(std::filesystem::exists(dbPath));
    const auto metadatastoreUri = "local://" + dbPath + "/metadatastore";
    const auto metadatastoreAuthToken = "some-token";
    const auto tp = std::chrono::system_clock::now() + std::chrono::minutes(10);
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    const auto validTill = secs.time_since_epoch().count();
    std::vector vbucketList{vbid, Vbid(1)};
    for (const auto& vb : vbucketList) {
        // GetFusionStorageSnapshot is expected to be called after the uploader
        // has been started on the vb, as part of which, the entry is created on
        // chronicle.
        ASSERT_EQ(cb::mcbp::Status::Success,
                  startFusionUploader(vb, "1").getStatus());
    }

    const auto resp = getFusionStorageSnapshot(vbucketList,
                                               snapshotUuid,
                                               bucketUuid,
                                               metadatastoreUri,
                                               metadatastoreAuthToken,
                                               validTill);
    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& json = resp.getDataJson();
    ASSERT_FALSE(json.empty());
    ASSERT_TRUE(json.is_array());
    for (const auto& vb : json) {
        ASSERT_TRUE(vb.contains("createdAt"));
        EXPECT_NE(0, vb["createdAt"]);
        ASSERT_TRUE(vb.contains("logManifestName"));
        ASSERT_TRUE(vb.contains("snapshotUUID"));
        EXPECT_EQ(snapshotUuid, vb["snapshotUUID"].get<std::string>());
        ASSERT_TRUE(vb.contains("validTill"));
        ASSERT_TRUE(vb.contains("version"));
        EXPECT_EQ(1, vb["version"]);
        ASSERT_TRUE(vb.contains("volumeID"));
        EXPECT_FALSE(vb["volumeID"].empty());
    }

    // Then release it
    EXPECT_EQ(cb::mcbp::Status::Success,
              releaseFusionStorageSnapshot(vbucketList,
                                           snapshotUuid,
                                           bucketUuid,
                                           metadatastoreUri,
                                           metadatastoreAuthToken)
                      .getStatus());

    // Clean up additional vbucket
    adminConnection->delVbucket(Vbid(1));
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
    constexpr uint64_t term = 123;
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
    constexpr auto newTerm = term + 1;
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

TEST_P(FusionTest, FusionNumUploaderThreads) {
    auto [ec, num] = getStat("fusion_num_uploader_threads");
    if (!isFusionSupportEnabled()) {
        EXPECT_EQ(ec, cb::engine_errc::not_supported);
        return;
    }
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(1, num);
    setMemcachedConfig("fusion_num_uploader_threads", 2);
    std::tie(ec, num) = getStat("fusion_num_uploader_threads");
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(2, num);
}

TEST_P(FusionTest, FusionNumMigratorThreads) {
    auto [ec, num] = getStat("fusion_num_migrator_threads");
    if (!isFusionSupportEnabled()) {
        EXPECT_EQ(ec, cb::engine_errc::not_supported);
        return;
    }
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(1, num);
    setMemcachedConfig("fusion_num_migrator_threads", 2);
    std::tie(ec, num) = getStat("fusion_num_migrator_threads");
    EXPECT_EQ(ec, cb::engine_errc::success);
    EXPECT_EQ(2, num);
}