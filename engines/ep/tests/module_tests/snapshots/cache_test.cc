/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "snapshots/cache.h"
#include <boost/filesystem/operations.hpp>
#include <folly/ScopeGuard.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Unistd.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/uuid.h>

class CacheTest : public ::testing::Test {
public:
    void SetUp() override {
    }

    void TearDown() override {
        cb::io::rmrf(test_dir.string());
    }

protected:
    std::expected<cb::snapshot::Manifest, cb::engine_errc> doCreateSnapshot(
            const std::filesystem::path& directory, Vbid vb) {
        // Generate a path/uuid for the snapshot
        auto uuid = ::to_string(cb::uuid::random());
        const auto snapshotPath = directory / uuid;
        create_directories(snapshotPath);

        cb::snapshot::Manifest manifest{vb, uuid};
        for (auto f : {"1.couch.32", "dek.32"}) {
            cb::io::saveFile(snapshotPath / f, f);
        }
        manifest.files.emplace_back(
                "1.couch.32", file_size(snapshotPath / "1.couch.32"), 0);
        manifest.deks.emplace_back(
                "dek.32", file_size(snapshotPath / "dek.32"), 1);
        return manifest;
    }

    std::filesystem::path test_dir{cb::io::mkdtemp("snapshot_test")};
    cb::snapshot::Cache cache{test_dir};
};

TEST_F(CacheTest, PrepareFailed) {
    const auto rv = cache.prepare(Vbid{0}, [this](const auto&, auto) {
        return std::unexpected(cb::engine_errc::not_supported);
    });
    EXPECT_EQ(cb::engine_errc::not_supported, rv.error());
}

TEST_F(CacheTest, Prepare) {
    auto rv = cache.prepare(Vbid{0}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = *rv;
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));
    for (const auto& file : manifest.files) {
        EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid / file.path));
        EXPECT_TRUE(exists(cache.make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache.make_absolute(file.path, manifest.uuid)),
                  file.size);
    }
    for (const auto& file : manifest.deks) {
        EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid / file.path));
        EXPECT_TRUE(exists(cache.make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache.make_absolute(file.path, manifest.uuid)),
                  file.size);
    }

    // Verify that it may be looked up
    auto searched = cache.lookup(manifest.uuid);
    EXPECT_EQ(searched, manifest);
}

TEST_F(CacheTest, ReleaseByVb) {
    auto rv = cache.prepare(Vbid{1}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = *rv;
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    cache.release(Vbid{1});
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache.lookup(manifest.uuid));
}

TEST_F(CacheTest, ReleaseByUuid) {
    auto rv = cache.prepare(Vbid{0}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = *rv;
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));
    cache.release(manifest.uuid);
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache.lookup(manifest.uuid));
}

TEST_F(CacheTest, Detach) {
    auto rv = cache.prepare(Vbid{1}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = *rv;
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    // detach removes the in-memory entry and returns the uuid, but leaves the
    // files on disk.
    auto uuid = cache.detach(Vbid{1});
    ASSERT_TRUE(uuid.has_value());
    EXPECT_EQ(manifest.uuid, *uuid);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    // No longer looked up by vb or uuid.
    EXPECT_EQ(std::nullopt, cache.lookup(Vbid{1}));
    EXPECT_EQ(std::nullopt, cache.lookup(manifest.uuid));

    // A second detach for the same vb finds nothing.
    EXPECT_EQ(std::nullopt, cache.detach(Vbid{1}));
}

TEST_F(CacheTest, DetachNoSnapshot) {
    EXPECT_EQ(std::nullopt, cache.detach(Vbid{7}));
}

TEST_F(CacheTest, RemoveFromDisk) {
    auto rv = cache.prepare(Vbid{1}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = *rv;

    auto uuid = cache.detach(Vbid{1});
    ASSERT_TRUE(uuid.has_value());
    EXPECT_TRUE(exists(test_dir / "snapshots" / *uuid));

    // removeFromDisk deletes the files for the (already detached) uuid.
    EXPECT_EQ(cb::engine_errc::success, cache.removeFromDisk(*uuid));
    EXPECT_FALSE(exists(test_dir / "snapshots" / *uuid));

    // A uuid with nothing on disk reports failure (nothing removed), matching
    // the existing Cache::remove contract.
    EXPECT_EQ(cb::engine_errc::failed,
              cache.removeFromDisk(::to_string(cb::uuid::random())));
}

TEST_F(CacheTest, Download) {
    auto rv = cache.download(
            Vbid{0},
            [] {
                return cb::snapshot::Manifest{Vbid{0},
                                              ::to_string(cb::uuid::random())};
            },
            [](const auto&, const auto&) { return cb::engine_errc::success; });
    ASSERT_TRUE(rv.has_value());
    EXPECT_TRUE(exists(test_dir / "snapshots" / rv->uuid / "manifest.json"));
    EXPECT_EQ(*rv, cache.lookup(rv->uuid));
}

/**
 * The snapshot directory must be synced in its parent directory once the
 * manifest is written. Verify that if that fails the download fails and the
 * snapshot is removed (rather than reporting a snapshot which may not
 * survive a crash). The sync is forced to fail by removing read permission
 * from the snapshots directory (it may still be written to).
 */
TEST_F(CacheTest, DownloadFailsIfSnapshotDirectoryCantBeSynced) {
#ifdef WIN32
    GTEST_SKIP() << "Directories can't be synced on Windows";
#else
    if (geteuid() == 0) {
        GTEST_SKIP() << "Permissions are not enforced for root";
    }
#endif
    const auto snapshots = test_dir / "snapshots";
    create_directories(snapshots);
    using std::filesystem::perms;
    permissions(snapshots, perms::owner_write | perms::owner_exec);
    auto restore = folly::makeGuard(
            [&snapshots] { permissions(snapshots, perms::owner_all); });

    bool downloaded = false;
    auto rv = cache.download(
            Vbid{0},
            [] {
                return cb::snapshot::Manifest{Vbid{0},
                                              ::to_string(cb::uuid::random())};
            },
            [&downloaded](const auto&, const auto&) {
                downloaded = true;
                return cb::engine_errc::success;
            });
    restore.dismiss();
    permissions(snapshots, perms::owner_all);

    ASSERT_FALSE(rv.has_value());
    EXPECT_EQ(cb::engine_errc::failed, rv.error());
    EXPECT_FALSE(downloaded);
    EXPECT_TRUE(std::filesystem::is_empty(snapshots));
    EXPECT_EQ(std::nullopt, cache.lookup(Vbid{0}));
}
