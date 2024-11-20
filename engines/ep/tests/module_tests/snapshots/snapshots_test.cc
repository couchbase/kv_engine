/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "../kvstore_test.h"
#include "kvstore/kvstore_config.h"
#include "snapshots/cache.h"

#include <platform/dirutils.h>

#include <filesystem>

class SnapshotsTests : public KVStoreParamTest {
public:
    void SetUp() override {
        KVStoreParamTest::SetUp();
        create_directories(snapshotdir);
        cache.initialise();
    }

    void TearDown() override {
        KVStoreParamTest::TearDown();
        std::error_code ec;
        remove_all(snapshotdir, ec);
        EXPECT_FALSE(ec) << ec.message();
    }

    bool released(std::string_view uuid, Vbid vb) const {
        const auto p1 = snapshotdir / uuid;
        const auto p2 = snapshotdir / std::to_string(vb.get());
        return !exists(p1) && !exists(p2);
    }

    cb::engine_errc doPrepareSnapshot(const std::filesystem::path& directory,
                                      Vbid vbid,
                                      cb::snapshot::Manifest& manifest) {
        return kvstore->prepareSnapshot(directory, vbid, manifest);
    }

    std::filesystem::path snapshotdir{cb::io::mkdtemp("snapshot_test")};
    cb::snapshot::Cache cache{snapshotdir};
};

TEST_P(SnapshotsTests, prepare) {
    auto rv = cache.prepare(vbid,
                            [this](const auto& dir, auto vb, auto& manifest) {
                                return doPrepareSnapshot(dir, vb, manifest);
                            });
    auto manifest = std::get<cb::snapshot::Manifest>(rv);
    EXPECT_FALSE(manifest.uuid.empty());
    EXPECT_FALSE(manifest.files.empty());
    for (const auto& file : manifest.files) {
        EXPECT_TRUE(exists(cache.make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache.make_absolute(file.path, manifest.uuid)),
                  file.size);
    }
}

/* NOLINTNEXTLINE(modernize-avoid-c-arrays) */
static std::string testParams[] = {"couchdb"};

INSTANTIATE_TEST_SUITE_P(
        SnapshotsTests,
        SnapshotsTests,
        ::testing::ValuesIn(testParams),
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });