/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#ifdef USE_FUSION

#include "testapp_client_test.h"

#include <gmock/gmock.h>
#include <nlohmann/json.hpp>

class FusionTest : public TestappClientTest {
protected:
    static void SetUpTestCase();
    void SetUp() override;

    BinprotResponse mountVbucket(Vbid vbid, const nlohmann::json& volumes);

public:
    static constexpr auto chronicleAuthToken = "some-token1!";
};

void FusionTest::SetUpTestCase() {
    const std::string dbPath = mcd_env->getDbPath();
    const auto bucketConfig = fmt::format(
            "magma_fusion_logstore_uri={};magma_fusion_metadatastore_uri={"
            "};chronicle_auth_token={}",
            "local://" + dbPath + "/logstore",
            "local://" + dbPath + "/metadatastore",
            chronicleAuthToken);
    doSetUpTestCaseWithConfiguration(generate_config(), bucketConfig);

    // Note: magma KVStore creation executes at the first flusher path run,
    // which is asynchronous.
    // We need to ensure that the KVStore is successfully created before
    // executing and Fusion API against it in the various test cases. We would
    // hit sporadic failures by "kvstore invalid" otherwise.
    adminConnection->selectBucket(bucketName);
    adminConnection->store("bump-vb-high-seqno", Vbid(0), {});
    adminConnection->waitForSeqnoToPersist(Vbid(0), 1);
}

void FusionTest::SetUp() {
    rebuildUserConnection(false);
    if (userConnection->statsMap("")["ep_backend"] != "magma") {
        GTEST_SKIP();
    }
}

BinprotResponse FusionTest::mountVbucket(Vbid vbid,
                                         const nlohmann::json& volumes) {
    BinprotResponse resp;
    adminConnection->executeInBucket(
            bucketName, [&resp, vbid, &volumes](auto& conn) {
                auto cmd = BinprotGenericCommand{
                        cb::mcbp::ClientOpcode::MountFusionVbucket};
                cmd.setVBucket(vbid);
                nlohmann::json json;
                json["mountPaths"] = volumes;
                cmd.setValue(json.dump());
                cmd.setDatatype(cb::mcbp::Datatype::JSON);
                resp = conn.execute(cmd);
            });
    return resp;
}

INSTANTIATE_TEST_SUITE_P(TransportProtocols,
                         FusionTest,
                         ::testing::Values(TransportProtocols::McbpPlain),
                         ::testing::PrintToStringParamName());

TEST_P(FusionTest, AggregatedStats) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isNotSupported());
    }
}

TEST_P(FusionTest, InvalidStat) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion someInvalidStat 0");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }
}

TEST_P(FusionTest, Stat_SyncInfo) {
    nlohmann::json res;
    try {
        adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion sync_info a");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_TRUE(e.isInvalidArguments());
    }

    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion sync_info 0");
    });
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("logSeqno"));
    EXPECT_EQ(0, res["logSeqno"]);
    ASSERT_TRUE(res.contains("logTerm"));
    EXPECT_EQ(0, res["logTerm"]);
    ASSERT_TRUE(res.contains("version"));
    EXPECT_EQ(1, res["version"]);
}

TEST_P(FusionTest, Stat_SyncInfo_KVStoreInvalid) {
    try {
        adminConnection->executeInBucket(bucketName, [](auto& conn) {
            nlohmann::json res;
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion sync_info 1");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes) {
    nlohmann::json res;
    adminConnection->executeInBucket(bucketName, [&res](auto& conn) {
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion active_guest_volumes 0");
    });

    // @todo MB-63679: Actual values will be populated once we have MountKVStore
    // + SetVBstate(open_snapshot=true). At the time of writing MountKVStore is
    // ready, we need the latter.
    ASSERT_TRUE(res.is_array());
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_KVStoreInvalid) {
    try {
        adminConnection->executeInBucket(bucketName, [](auto& conn) {
            nlohmann::json res;
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion active_guest_volumes 1");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, Stat_ActiveGuestVolumes_Aggregated) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        nlohmann::json res;
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion active_guest_volumes");
        ASSERT_TRUE(res.is_array());
    });
}

TEST_P(FusionTest, GetReleaseStorageSnapshot) {
    // Negative test: try to release a non-existent snapshot
    BinprotResponse resp;
    adminConnection->executeInBucket(bucketName, [&resp](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::ReleaseFusionStorageSnapshot};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        // MB-64494: snapshot uuid reported in the error message allocated by
        // magma. Here big-enough for preventing SSO that hides memory domain
        // alloc issues.
        json["snapshotUuid"] = std::string(1024, 'u');
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        resp = conn.execute(cmd);
    });
    EXPECT_EQ(cb::mcbp::Status::Einternal, resp.getStatus());

    // Create a snapshot
    const auto snapshotUuid = "some-snapshot-uuid";
    const auto tp = std::chrono::system_clock::now() + std::chrono::minutes(10);
    const auto secs = std::chrono::time_point_cast<std::chrono::seconds>(tp);
    const auto validity = secs.time_since_epoch().count();

    adminConnection->executeInBucket(
            bucketName, [&resp, &snapshotUuid, validity](auto& conn) {
                auto cmd = BinprotGenericCommand{
                        cb::mcbp::ClientOpcode::GetFusionStorageSnapshot};
                cmd.setVBucket(Vbid(0));
                nlohmann::json json;
                json["snapshotUuid"] = snapshotUuid;
                json["validity"] = validity;
                cmd.setValue(json.dump());
                cmd.setDatatype(cb::mcbp::Datatype::JSON);
                resp = conn.execute(cmd);
            });

    ASSERT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
    const auto& res = resp.getDataJson();
    ASSERT_FALSE(res.empty());
    ASSERT_TRUE(res.contains("createdAt"));
    EXPECT_NE(0, res["createdAt"]);
    ASSERT_TRUE(res.contains("logFiles"));
    ASSERT_TRUE(res["logFiles"].is_array());
    ASSERT_TRUE(res.contains("logManifestName"));
    ASSERT_TRUE(res.contains("snapshotUUID"));
    EXPECT_EQ(snapshotUuid, res["snapshotUUID"].get<std::string>());
    ASSERT_TRUE(res.contains("validTill"));
    ASSERT_TRUE(res.contains("version"));
    EXPECT_EQ(1, res["version"]);
    ASSERT_TRUE(res.contains("volumeID"));
    EXPECT_FALSE(res["volumeID"].empty());

    // Then release it
    adminConnection->executeInBucket(
            bucketName, [&resp, &snapshotUuid](auto& conn) {
                auto cmd = BinprotGenericCommand{
                        cb::mcbp::ClientOpcode::ReleaseFusionStorageSnapshot};
                cmd.setVBucket(Vbid(0));
                nlohmann::json json;
                json["snapshotUuid"] = snapshotUuid;
                cmd.setValue(json.dump());
                cmd.setDatatype(cb::mcbp::Datatype::JSON);
                resp = conn.execute(cmd);
            });

    EXPECT_TRUE(resp.isSuccess()) << "status:" << resp.getStatus();
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
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::UnmountFusionVbucket};
        cmd.setVBucket(Vbid(1));
        const auto res = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
    });
}

TEST_P(FusionTest, UnmountFusionVbucket_PreviouslyMounted) {
    const Vbid vbid{1};
    const auto resp = mountVbucket(vbid, {"path1", "path2"});
    EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    adminConnection->executeInBucket(bucketName, [vbid](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::UnmountFusionVbucket};
        cmd.setVBucket(vbid);
        const auto res = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, res.getStatus());
    });
}

TEST_P(FusionTest, SyncFusionLogstore) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = "1";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::SyncFusionLogstore};
        cmd.setVBucket(Vbid(0));
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

TEST_P(FusionTest, StartFusionUploader) {
    // arg invalid (not string)
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = 1234;
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });

    // arg invalid (not numeric string)
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = "abc123";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });

    // arg invalid (too large numeric string)
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = std::string(100, '1');
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });

    // arg invalid (negative numeric string)
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = "-1";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Einval, resp.getStatus());
    });

    // arg valid
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = "123";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

TEST_P(FusionTest, StopFusionUploader) {
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StopFusionUploader};
        cmd.setVBucket(Vbid(0));
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });
}

TEST_P(FusionTest, Stat_UploaderState) {
    // Uploader disabled at start
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        nlohmann::json res;
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion uploader_state 0");
        ASSERT_FALSE(res.empty());
        ASSERT_TRUE(res.is_object());
        ASSERT_TRUE(res.contains("state"));
        ASSERT_TRUE(res["state"].is_string());
        EXPECT_EQ("disabled", res["state"]);
        ASSERT_TRUE(res.contains("term"));
        ASSERT_TRUE(res["term"].is_number_integer());
        EXPECT_EQ(0, res["term"]);
    });

    // Start uploader
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StartFusionUploader};
        cmd.setVBucket(Vbid(0));
        nlohmann::json json;
        json["term"] = "123";
        cmd.setValue(json.dump());
        cmd.setDatatype(cb::mcbp::Datatype::JSON);
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    // verify stat
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        nlohmann::json res;
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion uploader_state 0");
        ASSERT_FALSE(res.empty());
        ASSERT_TRUE(res.is_object());
        ASSERT_TRUE(res.contains("state"));
        ASSERT_TRUE(res["state"].is_string());
        EXPECT_EQ("enabled", res["state"]);
        ASSERT_TRUE(res.contains("term"));
        ASSERT_TRUE(res["term"].is_number_integer());
        EXPECT_EQ(123, res["term"]);
    });

    // Stop uploader
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        auto cmd = BinprotGenericCommand{
                cb::mcbp::ClientOpcode::StopFusionUploader};
        cmd.setVBucket(Vbid(0));
        const auto resp = conn.execute(cmd);
        EXPECT_EQ(cb::mcbp::Status::Success, resp.getStatus());
    });

    // verify stat
    adminConnection->executeInBucket(bucketName, [](auto& conn) {
        nlohmann::json res;
        conn.stats([&res](auto& k, auto& v) { res = nlohmann::json::parse(v); },
                   "fusion uploader_state 0");
        ASSERT_FALSE(res.empty());
        ASSERT_TRUE(res.is_object());
        ASSERT_TRUE(res.contains("state"));
        ASSERT_TRUE(res["state"].is_string());
        EXPECT_EQ("disabled", res["state"]);
        ASSERT_TRUE(res.contains("term"));
        ASSERT_TRUE(res["term"].is_number_integer());
        EXPECT_EQ(0, res["term"]);
    });
}

TEST_P(FusionTest, Stat_UploaderState_KVStoreInvalid) {
    try {
        adminConnection->executeInBucket(bucketName, [](auto& conn) {
            nlohmann::json res;
            conn.stats([&res](auto& k,
                              auto& v) { res = nlohmann::json::parse(v); },
                       "fusion uploader_state 1");
        });
        FAIL();
    } catch (const ConnectionError& e) {
        EXPECT_EQ(cb::mcbp::Status::NotMyVbucket, e.getReason());
    }
}

TEST_P(FusionTest, GetPrometheusFusionStats) {
    std::array<std::string_view, 18> statKeysExpected = {
            "ep_fusion_namespace",
            "ep_fusion_syncs",
            "ep_fusion_bytes_synced",
            "ep_fusion_logs_migrated",
            "ep_fusion_bytes_migrated",
            "ep_fusion_log_store_size",
            "ep_fusion_log_store_garbage_size",
            "ep_fusion_logs_cleaned",
            "ep_fusion_log_clean_bytes_read",
            "ep_fusion_log_clean_reads",
            "ep_fusion_log_store_remote_puts",
            "ep_fusion_log_store_reads",
            "ep_fusion_log_store_remote_gets",
            "ep_fusion_log_store_remote_lists",
            "ep_fusion_log_store_remote_deletes",
            "ep_fusion_file_map_mem_used",
            "ep_fusion_sync_failures",
            "ep_fusion_migration_failures"};

    std::vector<std::string> actualKeys;
    adminConnection->executeInBucket(bucketName, [&actualKeys](auto& conn) {
        conn.stats(
                [&actualKeys](auto& k, auto& v) {
                    if (k.starts_with("ep_fusion")) {
                        actualKeys.emplace_back(k);
                    }
                },
                ""); // we convert empty to null to get engine stats
    });

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
        fprintf(stderr,
                "Expected %s but not found in actual\n",
                std::string{key}.c_str());
    }

    // Find any extra fusion stats - those that are found in actual but not
    // expected
    std::vector<std::string_view> extraKeys;
    std::ranges::set_difference(actualKeys,
                                statKeysExpected,
                                std::inserter(extraKeys, extraKeys.begin()));

    for (const auto& key : extraKeys) {
        error = true;
        fprintf(stderr,
                "Found stat %s but was not expected\n",
                std::string{key}.c_str());
    }

    EXPECT_EQ(false, error) << "Missing stats found";
}

TEST_P(FusionTest, DeleteFusionNamespace) {
    auto cmd = BinprotGenericCommand{
            cb::mcbp::ClientOpcode::DeleteFusionNamespace};
    nlohmann::json json;
    json["logstore_uri"] = "uri1";
    json["metadatastore_uri"] = "uri2";
    json["metadatastore_auth_token"] = "some-token";
    json["namespace"] = "namespace-to-delete";
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
    json["namespace"] = "namespace-to-delete";
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