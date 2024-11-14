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

using namespace cb::snapshot;

class CacheTest : public ::testing::Test {
public:
    void SetUp() override {
        test_dir = cb::io::mkdtemp("snapshot_test");
        cache = std::make_unique<Cache>(test_dir);
    }

    void TearDown() override {
        cb::io::rmrf(test_dir.string());
    }

protected:
    cb::engine_errc doCreateSnapshot(const std::filesystem::path& directory,
                                     Vbid,
                                     Manifest& manifest) {
        for (auto f : {"1.couch.32", "dek.32"}) {
            FILE* fp = fopen((directory / f).string().c_str(), "w");
            fprintf(fp, "%s\n", f);
            fclose(fp);
        }
        manifest.files.emplace_back(
                "1.couch.32", file_size(directory / "1.couch.32"), 0);
        manifest.deks.emplace_back(
                "dek.32", file_size(directory / "dek.32"), 1);
        return cb::engine_errc::success;
    }

    std::filesystem::path test_dir;
    std::unique_ptr<Cache> cache;
};

TEST_F(CacheTest, PrepareFailed) {
    const auto rv = cache->prepare(Vbid{0}, [this](const auto&, auto, auto&) {
        return cb::engine_errc::not_supported;
    });
    EXPECT_EQ(cb::engine_errc::not_supported, std::get<cb::engine_errc>(rv));
}

TEST_F(CacheTest, Prepare) {
    auto rv = cache->prepare(
            Vbid{0}, [this](const auto& directory, auto vb, auto& manifest) {
                return doCreateSnapshot(directory, vb, manifest);
            });
    auto manifest = std::get<Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));
    for (const auto& file : manifest.files) {
        EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid / file.path));
        EXPECT_TRUE(exists(cache->make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache->make_absolute(file.path, manifest.uuid)),
                  file.size);
    }
    for (const auto& file : manifest.deks) {
        EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid / file.path));
        EXPECT_TRUE(exists(cache->make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache->make_absolute(file.path, manifest.uuid)),
                  file.size);
    }

    // Verify that it may be looked up
    auto searched = cache->lookup(manifest.uuid);
    EXPECT_EQ(searched, manifest);
}

TEST_F(CacheTest, ReleaseByVb) {
    auto rv = cache->prepare(
            Vbid{0}, [this](const auto& directory, auto vb, auto& manifest) {
                return doCreateSnapshot(directory, vb, manifest);
            });
    auto manifest = std::get<Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    cache->release(Vbid{0});
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache->lookup(manifest.uuid));
}

TEST_F(CacheTest, ReleaseByUuid) {
    auto rv = cache->prepare(
            Vbid{0}, [this](const auto& directory, auto vb, auto& manifest) {
                return doCreateSnapshot(directory, vb, manifest);
            });
    auto manifest = std::get<Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));
    cache->release(manifest.uuid);
    EXPECT_FALSE(exists(test_dir / "snapshots" / manifest.uuid));
    EXPECT_EQ(std::nullopt, cache->lookup(manifest.uuid));
}

TEST_F(CacheTest, InitializeFromDiskSnapshots) {
    auto rv = cache->prepare(
            Vbid{0}, [this](const auto& directory, auto vb, auto& manifest) {
                return doCreateSnapshot(directory, vb, manifest);
            });
    auto manifest = std::get<Manifest>(rv);
    EXPECT_TRUE(exists(test_dir / "snapshots" / manifest.uuid));

    cache = std::make_unique<Cache>(test_dir);
    auto searched = cache->lookup(manifest.uuid);
    EXPECT_EQ(searched, manifest);
}
