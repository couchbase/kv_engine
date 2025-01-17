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
#include <folly/portability/GTest.h>
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
    std::variant<cb::engine_errc, cb::snapshot::Manifest> doCreateSnapshot(
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
        return cb::engine_errc::not_supported;
    });
    EXPECT_EQ(cb::engine_errc::not_supported, std::get<cb::engine_errc>(rv));
}

TEST_F(CacheTest, Prepare) {
    auto rv = cache.prepare(Vbid{0}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = std::get<cb::snapshot::Manifest>(rv);
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
    auto manifest = std::get<cb::snapshot::Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    cache.release(Vbid{1});
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache.lookup(manifest.uuid));
}

TEST_F(CacheTest, ReleaseByUuid) {
    auto rv = cache.prepare(Vbid{0}, [this](const auto& directory, auto vb) {
        return doCreateSnapshot(directory, vb);
    });
    auto manifest = std::get<cb::snapshot::Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));
    cache.release(manifest.uuid);
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache.lookup(manifest.uuid));
}
