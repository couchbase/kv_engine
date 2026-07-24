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
#include "failover-table.h"
#include "tests/mock/mock_ep_bucket.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"

#include <folly/portability/GTest.h>
#include <platform/dirutils.h>

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
              engine->prepare_snapshot(*cookie, vbid, {}, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
    setVBucketState(vbid, vbucket_state_replica);
    EXPECT_EQ(cb::engine_errc::not_my_vbucket,
              engine->prepare_snapshot(*cookie, vbid, {}, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot_no_disk_state) {
    // Active VB with nothing on disk
    setVBucketState(vbid, vbucket_state_active);

    // this is returning failed, but should not be something to fail rebalance.
    EXPECT_EQ(cb::engine_errc::failed,
              engine->prepare_snapshot(*cookie, vbid, {}, [](auto) {
                  throw std::runtime_error("should not be called");
              }));
}

TEST_P(SnapshotEngineTest, prepare_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(*cookie, vbid, {}, [&manifest](auto& m) {
                  manifest = m;
              }));
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
                      *cookie, vbid, {}, [&preWarmupManifest](auto& m) {
                          preWarmupManifest = m;
                      }));

    warmup();

    // warmup() loads snapshots into the cache via processSnapshots.
    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto& cache = mockEPBucket.public_getSnapshotCache();
    EXPECT_TRUE(cache.lookup(preWarmupManifest["uuid"]))
            << "No manifest found after warmup";

    nlohmann::json postWarmupManifest;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, {}, [&postWarmupManifest](auto& m) {
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
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb1, {}, [&m1](auto& m) { m1 = m; }));
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb2, {}, [&m2](auto& m) { m2 = m; }));
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb3, {}, [&m3](auto& m) { m3 = m; }));
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb4, {}, [&m4](auto& m) { m4 = m; }));

    // Perform various "corruptions" to the different snapshots *before* warmup
    // (which now processes snapshots via WarmupState::ProcessSnapshots).
    // Removing the json makes m1 "invalid"; a bad sha512 makes m2 "invalid"; m3
    // and m4 are valid but resumable (absent/truncated file). The corruptions
    // are made to snapshot-only state (manifest.json, or snapshot file copies)
    // so the live vbucket files - hard-linked into the snapshot - are left
    // intact and the vbuckets still warm up.

    // Create Manifest objects to get nicer code (avoid accessing the json);
    cb::snapshot::Manifest manifest1{m1};
    cb::snapshot::Manifest manifest2{m2};
    cb::snapshot::Manifest manifest3{m3};
    cb::snapshot::Manifest manifest4{m4};

    const auto snapshotsDir = std::filesystem::path{test_dbname} / "snapshots";

    auto editManifest = [](const std::filesystem::path& manifestPath,
                           auto edit) {
        auto json = nlohmann::json::parse(cb::io::loadFile(manifestPath));
        edit(json);
        std::error_code ec;
        ASSERT_TRUE(cb::io::saveFile(manifestPath, json.dump(), ec))
                << ec.message();
    };

    {
        // snapshot 1, remove the json
        std::error_code ec;
        cb::io::remove_with_retry(
                snapshotsDir / manifest1.uuid / "manifest.json", ec);
        ASSERT_FALSE(ec);
    }

    {
        // snapshot 2, record a bad sha512 so validation sees a mismatch
        editManifest(snapshotsDir / manifest2.uuid / "manifest.json",
                     [](nlohmann::json& json) {
                         json["files"][0]["sha512"] = std::string(128, 'a');
                     });
    }

    {
        // snapshot 3, remove a file (the live file survives via its own hard
        // link). For encrypted snapshots the DEK is a copy, so removing it is
        // equally safe.
        std::filesystem::path removePath;
        if (isEncrypted()) {
            removePath = snapshotsDir / manifest3.uuid / "deks" /
                         "MyActiveKey.key.1";
        } else {
            removePath =
                    snapshotsDir / manifest3.uuid / manifest3.files.at(0).path;
        }
        std::error_code ec;
        cb::io::remove_with_retry(removePath, ec);
        ASSERT_FALSE(ec);
    }

    {
        // snapshot 4, inflate the recorded file size so it appears truncated
        editManifest(snapshotsDir / manifest4.uuid / "manifest.json",
                     [this, &manifest4](nlohmann::json& json) {
                         if (isEncrypted()) {
                             json["deks"][0]["size"] = std::to_string(
                                     (manifest4.deks.at(0).size * 2) + 1);
                         } else {
                             json["files"][0]["size"] = std::to_string(
                                     (manifest4.files.at(0).size * 2) + 1);
                         }
                     });
    }

    warmup();

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
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

TEST_P(SnapshotEngineTest, delete_vbucket_removes_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    nlohmann::json manifest;
    ASSERT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(*cookie, vbid, {}, [&manifest](auto& m) {
                  manifest = m;
              }));
    const std::string uuid = manifest["uuid"];

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto& cache = mockEPBucket.public_getSnapshotCache();
    const auto snapshotPath =
            std::filesystem::path{test_dbname} / "snapshots" / uuid;

    ASSERT_TRUE(cache.lookup(vbid));
    ASSERT_TRUE(exists(snapshotPath));

    // Delete the vbucket (sync path, no cookie).
    ASSERT_EQ(cb::engine_errc::success, store->deleteVBucket(vbid, nullptr));

    // The in-memory snapshot entry is removed synchronously as part of
    // deletion, closing the stale-lookup race before the deferred task runs.
    EXPECT_FALSE(cache.lookup(vbid));
    EXPECT_FALSE(cache.lookup(uuid));
    // Files remain until the deferred deletion task runs.
    EXPECT_TRUE(exists(snapshotPath));

    // Run the deferred deletion task; it removes the vbucket disk files and the
    // snapshot directory.
    runNextTask(TaskType::AuxIO, "Removing (dead) vb:0 from memory and disk");
    EXPECT_FALSE(exists(snapshotPath));

    // Recreating the vbucket and preparing again yields a fresh snapshot with a
    // new uuid (no stale reuse of the old snapshot).
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json manifest2;
    ASSERT_EQ(
            cb::engine_errc::success,
            engine->prepare_snapshot(*cookie, vbid, {}, [&manifest2](auto& m) {
                manifest2 = m;
            }));
    EXPECT_NE(uuid, manifest2["uuid"].get<std::string>());
}

TEST_P(SnapshotEngineTest, delete_vbucket_sync_removes_snapshot) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    nlohmann::json manifest;
    ASSERT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(*cookie, vbid, {}, [&manifest](auto& m) {
                  manifest = m;
              }));
    const std::string uuid = manifest["uuid"];

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto& cache = mockEPBucket.public_getSnapshotCache();
    const auto snapshotPath =
            std::filesystem::path{test_dbname} / "snapshots" / uuid;
    ASSERT_TRUE(exists(snapshotPath));

    // Delete via the engine sync=true path. The first call returns would_block
    // (having synchronously set up deferred deletion, including detaching the
    // snapshot), then completes once the deferred deletion task has run.
    auto& taskQ = *task_executor->getLpTaskQ(TaskType::AuxIO);
    for (;;) {
        const auto ret = engine->deleteVBucket(*cookie, vbid, true);
        if (ret != cb::engine_errc::would_block) {
            EXPECT_EQ(cb::engine_errc::success, ret);
            break;
        }
        // The snapshot is detached from the cache synchronously as part of the
        // (would_block) delete, before the deferred task runs.
        EXPECT_FALSE(cache.lookup(vbid));
        EXPECT_TRUE(exists(snapshotPath));
        runNextTask(taskQ, "Removing (dead) vb:0 from memory and disk");
    }

    EXPECT_FALSE(cache.lookup(vbid));
    EXPECT_FALSE(exists(snapshotPath));
}

// A snapshot whose recorded vbucket UUID matches the vbucket's creation UUID
// (same incarnation) is retained during warmup.
TEST_P(SnapshotEngineTest, SnapshotWarmupVbucketUuidMatch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json m;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, {}, [&m](auto& mm) { m = mm; }));
    cb::snapshot::Manifest manifest{m};

    // prepare_snapshot must have captured the vbucket's creation UUID.
    ASSERT_TRUE(m.contains("vbucket_uuid"));
    ASSERT_FALSE(m["vbucket_uuid"].get<std::string>().empty());
    { // VBucketPtr must not outlive warmup() below - it resets the engine and
      // frees the EPStats the VBucket references.
        auto vb = engine->getKVBucket()->getVBucket(vbid);
        ASSERT_TRUE(vb);
        EXPECT_EQ(vb->getVbucketUuid(), m["vbucket_uuid"].get<std::string>());
    }

    warmup();

    // warmup() runs processSnapshots (load + discardOrphanedSnapshots) itself -
    // no manual calls needed (and a second processSnapshots would be a bug, see
    // MB-64963). A matching-uuid snapshot is retained in the cache and its
    // files remain on disk.
    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto& cache = mockEPBucket.public_getSnapshotCache();
    EXPECT_TRUE(cache.lookup(manifest.uuid))
            << "Snapshot with a matching vbucket UUID should be retained";
    EXPECT_TRUE(exists(std::filesystem::path{test_dbname} / "snapshots" /
                       manifest.uuid))
            << "Retained snapshot's files must remain on disk";
}

// A snapshot whose recorded vbucket UUID does not match the vbucket's creation
// UUID belongs to a different incarnation and is discarded during warmup.
TEST_P(SnapshotEngineTest, SnapshotWarmupVbucketUuidMismatch) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json m;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, {}, [&m](auto& mm) { m = mm; }));
    cb::snapshot::Manifest manifest{m};

    // Rewrite the on-disk manifest's vbucket_uuid to a different incarnation so
    // warmup discards the snapshot. The live vbucket files are hard-linked into
    // the snapshot and left intact so the vbucket still warms up.
    const auto manifestPath = std::filesystem::path{test_dbname} / "snapshots" /
                              manifest.uuid / "manifest.json";
    auto json = nlohmann::json::parse(cb::io::loadFile(manifestPath));
    json["vbucket_uuid"] = "a-different-incarnation";
    std::error_code ec;
    ASSERT_TRUE(cb::io::saveFile(manifestPath, json.dump(), ec))
            << ec.message();

    warmup();

    // warmup() runs processSnapshots (load + discardOrphanedSnapshots) itself -
    // the mismatched-uuid snapshot is discarded during warmup.
    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto& cache = mockEPBucket.public_getSnapshotCache();
    EXPECT_FALSE(cache.lookup(manifest.uuid))
            << "Snapshot with a mismatched vbucket UUID should be discarded";

    // ... and its files removed from disk.
    EXPECT_FALSE(exists(std::filesystem::path{test_dbname} / "snapshots" /
                        manifest.uuid));
}

// When the bucket starts without warmup, EPBucket::initialize calls
// deleteSnapshots() which throws away every snapshot on disk (none can be
// recovered without a warmup). Verify all on-disk snapshots are removed and
// none are loaded into the cache.
TEST_P(SnapshotEngineTest, deleteSnapshots_removes_all_on_disk) {
    Vbid vb1(vbid);
    Vbid vb2(vbid.get() + 1);
    setVBucketStateAndRunPersistTask(vb1, vbucket_state_active);
    setVBucketStateAndRunPersistTask(vb2, vbucket_state_active);

    nlohmann::json m1, m2;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb1, {}, [&m1](auto& m) { m1 = m; }));
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vb2, {}, [&m2](auto& m) { m2 = m; }));

    const auto snapshotsDir = std::filesystem::path{test_dbname} / "snapshots";
    cb::snapshot::Manifest manifest1{m1};
    cb::snapshot::Manifest manifest2{m2};
    ASSERT_TRUE(exists(snapshotsDir / manifest1.uuid));
    ASSERT_TRUE(exists(snapshotsDir / manifest2.uuid));

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    EXPECT_EQ(cb::engine_errc::success, mockEPBucket.deleteSnapshots());

    // Every snapshot removed from disk...
    EXPECT_FALSE(exists(snapshotsDir / manifest1.uuid));
    EXPECT_FALSE(exists(snapshotsDir / manifest2.uuid));
    // ... leaving the snapshots directory empty. deleteSnapshots does not load
    // anything into the cache.
    EXPECT_TRUE(std::filesystem::is_empty(snapshotsDir));

    // Idempotent: a second call with nothing to delete still succeeds.
    EXPECT_EQ(cb::engine_errc::success, mockEPBucket.deleteSnapshots());
}

// processSnapshots (via initialiseSnapshots) must be idempotent: loading a
// snapshot that is already cached is a no-op and must NOT delete the
// still-valid on-disk files (MB-64963). prepare_snapshot already caches the
// snapshot, so a subsequent load rediscovers the same uuid on disk and
// exercises that path.
TEST_P(SnapshotEngineTest, processSnapshots_idempotent) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    nlohmann::json m;
    EXPECT_EQ(cb::engine_errc::success,
              engine->prepare_snapshot(
                      *cookie, vbid, {}, [&m](auto& mm) { m = mm; }));
    cb::snapshot::Manifest manifest{m};

    auto& mockEPBucket = dynamic_cast<MockEPBucket&>(*engine->getKVBucket());
    const auto snapDir =
            std::filesystem::path{test_dbname} / "snapshots" / manifest.uuid;
    const auto& cache = mockEPBucket.public_getSnapshotCache();

    // Sanity: prepare_snapshot cached it and the files are on disk.
    ASSERT_TRUE(cache.lookup(manifest.uuid));
    ASSERT_TRUE(exists(snapDir));

    // Loading again (twice) must be a no-op - snapshot stays cached, files
    // stay.
    EXPECT_EQ(cb::engine_errc::success, mockEPBucket.initialiseSnapshots());
    EXPECT_EQ(cb::engine_errc::success, mockEPBucket.initialiseSnapshots());

    EXPECT_TRUE(cache.lookup(manifest.uuid))
            << "Snapshot must remain cached after repeat load";
    EXPECT_TRUE(exists(snapDir))
            << "Repeat load must not delete the still-valid snapshot files";
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
