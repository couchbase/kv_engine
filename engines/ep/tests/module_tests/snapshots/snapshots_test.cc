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
#include "../test_helpers.h"
#include "item.h"
#include "kvstore/kvstore_config.h"
#include "snapshots/cache.h"
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_cookie.h>

#include <filesystem>

class SnapshotsTests : public KVStoreParamTest {
public:
    void SetUp() override {
        KVStoreParamTest::SetUp();
        create_directories(snapshotdir);
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        StoredDocKey key = makeStoredDocKey("key");
        auto qi = makeCommittedItem(key, "value");
        qi->setBySeqno(1);
        kvstore->set(*ctx, qi);
        EXPECT_TRUE(kvstore->commit(std::move(ctx), flush));
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

    auto doPrepareSnapshot(const std::filesystem::path& directory,
                           Vbid vbid,
                           bool generateChecksums) {
        MockCookie cookie;
        return kvstore->prepareSnapshot(
                cookie, directory, vbid, generateChecksums);
    }

    std::filesystem::path snapshotdir{cb::io::mkdtemp("snapshot_test")};
    cb::time::steady_clock::time_point time = cb::time::steady_clock::now();
    cb::snapshot::Cache cache{snapshotdir, [this]() { return time; }};
};

TEST_P(SnapshotsTests, prepare) {
    auto rv = cache.prepare(vbid, [this](const auto& dir, auto vb) {
        return doPrepareSnapshot(dir, vb, true);
    });
    EXPECT_TRUE(std::holds_alternative<cb::snapshot::Manifest>(rv));
    auto manifest = std::get<cb::snapshot::Manifest>(rv);
    EXPECT_FALSE(manifest.uuid.empty());
    EXPECT_FALSE(manifest.files.empty());
    for (const auto& file : manifest.files) {
        EXPECT_TRUE(exists(cache.make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache.make_absolute(file.path, manifest.uuid)),
                  file.size);
    }
}

TEST_P(SnapshotsTests, purge) {
    // For this test we don't need to really create snapshots, we can just
    // return a manifest. When purge runs it will log a warning only.
    auto rv = cache.prepare(Vbid(0), [this](const auto& dir, auto vb) {
        return cb::snapshot::Manifest{Vbid(0), "vb0"};
    });
    EXPECT_TRUE(std::holds_alternative<cb::snapshot::Manifest>(rv));

    rv = cache.prepare(Vbid(1), [this](const auto& dir, auto vb) {
        return cb::snapshot::Manifest{Vbid(1), "vb1"};
    });
    EXPECT_TRUE(std::holds_alternative<cb::snapshot::Manifest>(rv));

    // move time 10s and touch one snapshot
    time += std::chrono::seconds(10);
    EXPECT_TRUE(cache.lookup(Vbid(1)));

    // Purge everything older than 9 seconds
    cache.purge(std::chrono::seconds(9));

    // Expect that vb0 snapshot is gone, but vb1 remains.
    EXPECT_FALSE(cache.lookup(Vbid(0)));
    EXPECT_TRUE(cache.lookup(Vbid(1)));
}

/* NOLINTNEXTLINE(modernize-avoid-c-arrays) */
#ifdef EP_USE_MAGMA
#define TEST_VALUES ::testing::Values("couchdb", "magma")
#else
#define TEST_VALUES ::testing::Values("couchdb")
#endif

INSTANTIATE_TEST_SUITE_P(
        SnapshotsTests,
        SnapshotsTests,
        TEST_VALUES,
        [](const ::testing::TestParamInfo<std::string>& testInfo) {
            return testInfo.param;
        });
#undef TEST_VALUES
