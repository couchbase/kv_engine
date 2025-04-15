/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_bucket.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"

#include <gtest/gtest.h>

class SnapshotEngineTest
    : public SingleThreadedEPBucketTest,
      public ::testing::WithParamInterface<std::tuple<bool, std::string>> {
public:
    void SetUp() override {
        config_string = generateBucketTypeConfig(std::get<1>(GetParam()));
        SingleThreadedEPBucketTest::SetUp();

        if (std::get<0>(GetParam())) {
            setupEncryptionKeys();
        }
    }

    bool isEncrypted() const {
        return std::get<0>(GetParam());
    }

    void warmup() {
        if (isEncrypted()) {
            resetEngineAndWarmup({}, false, getEncryptionKeys());
        } else {
            resetEngineAndWarmup();
        }
    }
};

TEST_P(SnapshotEngineTest, nmvb) {
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot_no_disk_state) {
    // Active VB with nothing on disk
    setVBucketState(vbid, vbucket_state_active);

    // this is returning failed, but should not be something to fail rebalance.
    EXPECT_EQ(cb::engine_errc::failed,
              engine->prepare_snapshot(*cookie, vbid, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&manifest](auto& m) { manifest = m; }));
    EXPECT_TRUE(manifest.contains("uuid"));

    if (isCouchstore()) {
        ASSERT_EQ(1, manifest["files"].size());
        EXPECT_EQ(1, manifest["files"][0]["id"]);
        EXPECT_EQ("0.couch.1", manifest["files"][0]["path"]);
    } else {
        // Let's not assume too much about magma, at least verify some fields
        // are set.
        ASSERT_GT(manifest["files"].size(), 1);
        EXPECT_FALSE(manifest["files"][0]["path"].empty());
    }
    EXPECT_GT(manifest["files"][0]["size"], 0);
    if (isEncrypted()) {
        ASSERT_EQ(1, manifest["deks"].size());
        EXPECT_EQ("deks/MyActiveKey.key.1", manifest["deks"][0]["path"]);
        EXPECT_EQ("44", manifest["deks"][0]["size"]);
    } else {
        EXPECT_TRUE(manifest["deks"].empty());
    }

    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(*cookie,
                               "snapshot-status 0",
                               {},
                               [](auto k, auto v, auto& c) {
                                   EXPECT_EQ(k, "vb_0:status");
                                   EXPECT_EQ(v, "available");
                               }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot_warmup) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json preWarmupManifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&preWarmupManifest](auto& m) {
                          preWarmupManifest = m;
                      }));

    warmup();

    // Test harness doesn't hit EPBucket::initialize so must manually call
    // the cache initialise.
    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    mockEPBucket.initialiseSnapshots();

    const auto& cache = mockEPBucket.public_getSnapshotCache();
    EXPECT_TRUE(cache.lookup(preWarmupManifest["uuid"]))
            << "No manifest found after warmup";

    nlohmann::json postWarmupManifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, [&postWarmupManifest](auto& m) {
                          postWarmupManifest = m;
                      }));

    EXPECT_EQ(preWarmupManifest, postWarmupManifest);
}

TEST_P(SnapshotEngineTest, prepare_snapshot_warmup_invalid_snap) {
    // Prepare 4 snapshots
    Vbid vb1(vbid);
    Vbid vb2(vbid.get() + 1);
    Vbid vb3(vbid.get() + 2);
    Vbid vb4(vbid.get() + 3);
    setVBucketStateAndRunPersistTask(vb1, vbucket_state_active);
    setVBucketStateAndRunPersistTask(vb2, vbucket_state_active);
    setVBucketStateAndRunPersistTask(vb3, vbucket_state_active);
    setVBucketStateAndRunPersistTask(vb4, vbucket_state_active);
    nlohmann::json m1, m2, m3, m4;
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->prepare_snapshot(*cookie, vb1, [&m1](auto& m) { m1 = m; }));
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->prepare_snapshot(*cookie, vb2, [&m2](auto& m) { m2 = m; }));
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->prepare_snapshot(*cookie, vb3, [&m3](auto& m) { m3 = m; }));
    EXPECT_EQ(
            cb::engine_errc::success,
            engine->prepare_snapshot(*cookie, vb4, [&m4](auto& m) { m4 = m; }));

    warmup();

    // Perform various "corruptions" to the different snapshots. Delete the JSON
    // will make m1 "invalid", corrupt the file makes m2 "invalid".
    // m3 and m4 are valid/resumable

    // Create Manifest objects to get nicer code (avoid accessing the json);
    cb::snapshot::Manifest manifest1{m1};
    cb::snapshot::Manifest manifest2{m2};
    cb::snapshot::Manifest manifest3{m3};
    cb::snapshot::Manifest manifest4{m4};

    {
        // snapshot 1, remove the json
        std::error_code ec;
        auto path = std::filesystem::path{test_dbname} / "snapshots" /
                    manifest1.uuid;
        std::filesystem::remove_all(path / "manifest.json", ec);
        ASSERT_FALSE(ec);
    }

    {
        // snapshot 2, force mismatch of sha512 of the file
        auto path = std::filesystem::path{test_dbname} / "snapshots" /
                    manifest2.uuid / manifest2.files.at(0).path;
        std::fstream file(path,
                          std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.is_open());
        // invert byte 0
        file.seekg(0);
        char byte{0};
        file.get(byte);
        file.seekp(0);
        file.put(~byte);
        file.close();
    }

    std::filesystem::path removePath;
    if (isEncrypted()) {
        // snapshot 3, remove a DEK file
        removePath = std::filesystem::path{test_dbname} / "snapshots" /
                     manifest3.uuid / "deks" / "MyActiveKey.key.1";
    } else {
        // snapshot 3, remove a file
        removePath = std::filesystem::path{test_dbname} / "snapshots" /
                     manifest3.uuid / manifest3.files.at(0).path;
    }

    std::error_code ec;
    std::filesystem::remove_all(removePath, ec);
    ASSERT_FALSE(ec);

    std::filesystem::path truncatePath;
    size_t truncatedSize{0};
    if (isEncrypted()) {
        // snapshot 4, truncate a DEK file
        truncatePath = std::filesystem::path{test_dbname} / "snapshots" /
                       manifest4.uuid / "deks" / "MyActiveKey.key.1";
        truncatedSize = manifest4.deks.at(0).size / 2;
    } else {
        // snapshot 4, truncate a file
        truncatePath = std::filesystem::path{test_dbname} / "snapshots" /
                       manifest4.uuid / manifest4.files.at(0).path;
        truncatedSize = manifest4.files.at(0).size / 2;
    }
    std::filesystem::resize_file(truncatePath, truncatedSize, ec);
    ASSERT_FALSE(ec);

    // Test harness doesn't call EPBucket::initialize so must manually call
    // to process existing snapshots and drop invalid ones.
    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    mockEPBucket.initialiseSnapshots();
    const auto& cache = mockEPBucket.public_getSnapshotCache();

    // Snapshot refused to load
    EXPECT_FALSE(cache.lookup(manifest1.uuid));
    EXPECT_FALSE(cache.lookup(manifest2.uuid));

    auto manifest = cache.lookup(manifest3.uuid);
    ASSERT_TRUE(manifest);
    // Expect that the removed file is marked as such
    if (isEncrypted()) {
        EXPECT_EQ(cb::snapshot::FileStatus::Absent,
                  manifest->deks.at(0).status);
    } else {
        EXPECT_EQ(cb::snapshot::FileStatus::Absent,
                  manifest->files.at(0).status);
    }

    manifest = cache.lookup(manifest4.uuid);
    ASSERT_TRUE(manifest);
    // Expect that the truncated file is marked as such
    if (isEncrypted()) {
        EXPECT_EQ(cb::snapshot::FileStatus::Truncated,
                  manifest->deks.at(0).status);
    } else {
        EXPECT_EQ(cb::snapshot::FileStatus::Truncated,
                  manifest->files.at(0).status);
    }

    EXPECT_EQ(cb::engine_errc::success,
              engine->getStats(
                      *cookie,
                      "snapshot-status",
                      {},
                      [](auto k, auto v, auto& c) {
                          if (k == "vb_0:status" || k == "vb_1:status") {
                              EXPECT_EQ(v, "none");
                          } else if (k == "vb_2:status" || k == "vb_3:status") {
                              EXPECT_EQ(v, "incomplete");
                          } else {
                              FAIL() << "Unexpected key " << k;
                          }
                      }));
}

static std::string PrintToStringParamName(
        const ::testing::TestParamInfo<SnapshotEngineTest::ParamType>& info) {
    if (std::get<0>(info.param)) {
        return "encrypted_" + std::get<1>(info.param);
    }
    return std::get<1>(info.param);
}

#ifdef EP_USE_MAGMA
#define TEST_VALUES ::testing::Values("persistent_couchdb", "persistent_magma")
#else
#define TEST_VALUES ::testing::Values("persistent_couchdb")
#endif

// todo: add magma (and maybe nexus)
INSTANTIATE_TEST_SUITE_P(SnapshotEngineTests,
                         SnapshotEngineTest,
                         ::testing::Combine(::testing::Values(true, false),
                                            TEST_VALUES),
                         PrintToStringParamName);
#undef TEST_VALUES
