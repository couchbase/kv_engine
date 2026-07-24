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
#include <snapshot/disk_format_constraint.h>

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
        cb::io::remove_with_retry(snapshotdir, ec);
        EXPECT_FALSE(ec) << ec.message();
    }

    bool released(std::string_view uuid, Vbid vb) const {
        const auto p1 = snapshotdir / uuid;
        const auto p2 = snapshotdir / std::to_string(vb.get());
        return !exists(p1) && !exists(p2);
    }

    auto doPrepareSnapshot(const std::filesystem::path& directory,
                           Vbid vbid,
                           bool generateChecksums,
                           cb::snapshot::DiskFormatConstraint constraint = {},
                           std::string vbucketUuid = "test-vbucket-uuid") {
        MockCookie cookie;
        return kvstore->prepareSnapshot(cookie,
                                        directory,
                                        vbid,
                                        constraint,
                                        vbucketUuid,
                                        generateChecksums);
    }

    std::filesystem::path snapshotdir{cb::io::mkdtemp("snapshot_test")};
    cb::time::steady_clock::time_point time = cb::time::steady_clock::now();
    cb::snapshot::Cache cache{snapshotdir, [this]() { return time; }};
};

TEST_P(SnapshotsTests, prepare) {
    auto rv = cache.prepare(vbid, [this](const auto& dir, auto vb) {
        return doPrepareSnapshot(dir, vb, true);
    });
    EXPECT_TRUE(rv.has_value());
    auto manifest = *rv;
    EXPECT_FALSE(manifest.uuid.empty());
    EXPECT_FALSE(manifest.files.empty());
    for (const auto& file : manifest.files) {
        EXPECT_TRUE(exists(cache.make_absolute(file.path, manifest.uuid)));
        EXPECT_EQ(file_size(cache.make_absolute(file.path, manifest.uuid)),
                  file.size);
    }
}

/// The manifest must record the storage backend and (for couchstore) the
/// disk format version of the snapshot files
TEST_P(SnapshotsTests, prepareRecordsDiskFormat) {
    auto rv = cache.prepare(vbid, [this](const auto& dir, auto vb) {
        return doPrepareSnapshot(dir, vb, true);
    });
    ASSERT_TRUE(rv.has_value());
    const auto manifest = *rv;
    const auto constraint = kvstore->getSnapshotDiskFormatConstraint();
    EXPECT_EQ(constraint.backend, manifest.backend);
    if (isCouchstore()) {
        EXPECT_EQ("couchstore", manifest.backend);
        EXPECT_NE(0, manifest.diskFormatVersion);
        EXPECT_LE(manifest.diskFormatVersion, constraint.maxVersion);
    } else {
        EXPECT_EQ("magma", manifest.backend);
    }
}

/// A snapshot must not be created for a requester which does not support
/// the disk format version of the snapshot files
TEST_P(SnapshotsTests, prepareUnsupportedDiskFormatVersion) {
    if (!isCouchstore()) {
        // magma reports a storage format version of 1, and a maxVersion of 0
        // means "unknown" (the check is skipped), so no constraint can be low
        // enough to reject a magma snapshot on version grounds. Testable once
        // magma's storage format version is bumped.
        GTEST_SKIP();
    }
    auto rv = cache.prepare(vbid, [this](const auto& dir, auto vb) {
        // Version 1 predates all disk format versions in use
        return doPrepareSnapshot(dir, vb, true, {"couchstore", 1});
    });
    ASSERT_FALSE(rv.has_value());
    EXPECT_EQ(cb::engine_errc::not_supported, rv.error());
    EXPECT_FALSE(cache.lookup(vbid));
}

/// A snapshot must not be created for a requester using a different
/// storage backend
TEST_P(SnapshotsTests, prepareWrongBackend) {
    auto rv = cache.prepare(vbid, [this](const auto& dir, auto vb) {
        const auto other = isCouchstore() ? "magma" : "couchstore";
        return doPrepareSnapshot(dir, vb, true, {other, 0});
    });
    ASSERT_FALSE(rv.has_value());
    EXPECT_EQ(cb::engine_errc::not_supported, rv.error());
    EXPECT_FALSE(cache.lookup(vbid));
}

TEST_P(SnapshotsTests, purge) {
    // For this test we don't need to really create snapshots, we can just
    // return a manifest. When purge runs it will log a warning only.
    auto rv = cache.prepare(Vbid(0), [this](const auto& dir, auto vb) {
        return cb::snapshot::Manifest{Vbid(0), "vb0"};
    });
    EXPECT_TRUE(rv.has_value());

    rv = cache.prepare(Vbid(1), [this](const auto& dir, auto vb) {
        return cb::snapshot::Manifest{Vbid(1), "vb1"};
    });
    EXPECT_TRUE(rv.has_value());

    // move time 10s and touch one snapshot
    time += std::chrono::seconds(10);
    EXPECT_TRUE(cache.lookup(Vbid(1)));

    // Purge everything older than 9 seconds
    cache.purge(std::chrono::seconds(9));

    // Expect that vb0 snapshot is gone, but vb1 remains.
    EXPECT_FALSE(cache.lookup(Vbid(0)));
    EXPECT_TRUE(cache.lookup(Vbid(1)));
}

TEST_P(SnapshotsTests, processSnapshotsInsertsSingle) {
    const auto dir = snapshotdir / "snapshots";
    create_directories(dir);
    auto s1 = doPrepareSnapshot(dir, vbid, true);
    ASSERT_TRUE(s1.has_value());
    const auto uuid1 = s1->uuid;
    ASSERT_TRUE(exists(dir / uuid1));

    EXPECT_EQ(cb::engine_errc::success, kvstore->processSnapshots(dir, cache));

    // The single snapshot is loaded into the cache and kept on disk.
    auto m = cache.lookup(vbid);
    ASSERT_TRUE(m);
    EXPECT_EQ(uuid1, m->uuid);
    EXPECT_TRUE(exists(dir / uuid1));
}

TEST_P(SnapshotsTests, processSnapshotsRemovesDuplicates) {
    // Create two snapshots for the same vbucket on disk (bypassing the cache's
    // one-per-vbucket dedup). This simulates an orphaned snapshot left behind
    // by a deleteVBucket cleanup task that never ran, plus a freshly prepared
    // one for the recreated vbucket. On restart we cannot know which is
    // correct, so processSnapshots must discard both and return to the
    // no-snapshot state.
    const auto dir = snapshotdir / "snapshots";
    create_directories(dir);
    auto s1 = doPrepareSnapshot(dir, vbid, true);
    auto s2 = doPrepareSnapshot(dir, vbid, true);
    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    const auto uuid1 = s1->uuid;
    const auto uuid2 = s2->uuid;
    ASSERT_NE(uuid1, uuid2);
    ASSERT_TRUE(exists(dir / uuid1));
    ASSERT_TRUE(exists(dir / uuid2));

    EXPECT_EQ(cb::engine_errc::success, kvstore->processSnapshots(dir, cache));

    // Both snapshots removed from disk and nothing registered for the vbucket.
    EXPECT_FALSE(cache.lookup(vbid));
    EXPECT_FALSE(exists(dir / uuid1));
    EXPECT_FALSE(exists(dir / uuid2));
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
