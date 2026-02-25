/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "backup/backup.h"
#include "backup/backup_generated.h"
#include "ep_bucket.h"
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#include "memcached/vbucket.h"
#include "statistics/labelled_collector.h"
#include "statistics/tests/mock/mock_stat_collector.h"
#include "tests/mock/mock_magma_kvstore.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "utilities/test_manifest.h"
#include "vbucket.h"
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/cb_arena_malloc.h>
#include <platform/cb_arena_malloc_client.h>

class ContinousBackupTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        config_string +=
                "continuous_backup_enabled=true;"
                "continuous_backup_interval=1000;"
                // Ensure the path is within the unit test
                "continuous_backup_path=@continuous_backup;"
                "history_retention_seconds=2000";

        STParameterizedBucketTest::SetUp();
        replaceMagmaKVStore();

        setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    }

    MockMagmaKVStore& getMockKVStore(Vbid vbid) {
        auto* kvstore = store->getRWUnderlying(vbid);
        return dynamic_cast<MockMagmaKVStore&>(*kvstore);
    }

    std::string runContinuousBackupCallback(Vbid vbid,
                                            const KVFileHandle& kvFileHandle) {
        auto& store = getMockKVStore(vbid);

        auto primaryMemory =
                getDomainMemoryAllocated(cb::MemoryDomain::Primary);
        auto secondaryMemory =
                getDomainMemoryAllocated(cb::MemoryDomain::Secondary);

        magma::Status status;
        std::string metadata;
        {
            cb::UseArenaMallocSecondaryDomain guard;
            std::tie(status, metadata) =
                    store.onContinuousBackupCallback(kvFileHandle);
        }

        if (engine->getEpStats().isMemoryTrackingEnabled()) {
            // The callback logic in KV should be in the KV memory domain. For
            // efficiency, the result makes sense to be in the Magma memory
            // domain. We verify that is the case here.
            EXPECT_EQ(primaryMemory,
                      getDomainMemoryAllocated(cb::MemoryDomain::Primary));
            EXPECT_LT(secondaryMemory,
                      getDomainMemoryAllocated(cb::MemoryDomain::Secondary));
        }

        if (!status.IsOK()) {
            throw std::runtime_error(status.String());
        }

        return metadata;
    }
};

TEST_P(ContinousBackupTest, Config) {
    using namespace std::chrono_literals;
    auto& store = getMockKVStore(vbid);
    auto& config = dynamic_cast<const MagmaKVStoreConfig&>(store.getConfig());

    EXPECT_EQ(config.isContinousBackupEnabled(), true);
    EXPECT_EQ(config.getContinousBackupInterval(), 1000s);

    engine->getConfiguration().setContinuousBackupEnabled(false);
    engine->getConfiguration().setContinuousBackupInterval(123);

    EXPECT_EQ(config.isContinousBackupEnabled(), false);
    EXPECT_EQ(config.getContinousBackupInterval(), 123s);
}

TEST_P(ContinousBackupTest, PathConfig) {
    auto& config = engine->getConfiguration();
    std::filesystem::path dbName =
            std::filesystem::canonical(config.getDbname());
    constexpr auto bucketDirName = "123";

    {
        config.parseConfiguration("uuid=123;continuous_backup_path=path");
        MagmaKVStoreConfig kvStoreConfig(config, "magma", 1, 1);
        EXPECT_EQ(kvStoreConfig.getContinuousBackupPath(),
                  dbName / "path" / bucketDirName);
    }
    {
        config.parseConfiguration("uuid=123;continuous_backup_path=path/");
        MagmaKVStoreConfig kvStoreConfig(config, "magma", 1, 1);
        EXPECT_EQ(kvStoreConfig.getContinuousBackupPath(),
                  dbName / "path" / bucketDirName);
    }
    {
        config.parseConfiguration(
                "uuid=123;continuous_backup_path=a/b/../@continuous_backup");
        MagmaKVStoreConfig kvStoreConfig(config, "magma", 1, 1);
        EXPECT_EQ(kvStoreConfig.getContinuousBackupPath(),
                  dbName / "a" / "@continuous_backup" / bucketDirName);
    }
}

TEST_P(ContinousBackupTest, CallbackInitialSnapshot) {
    auto& store = getMockKVStore(vbid);
    const auto maxCas = engine->getKVBucket()->getVBucket(vbid)->getMaxCas();

    auto initialSnapshot = store.makeFileHandle(vbid);
    ASSERT_TRUE(initialSnapshot.get());

    auto metadataString = runContinuousBackupCallback(vbid, *initialSnapshot);

    auto& metadata = Backup::decodeBackupMetadata(metadataString);
    EXPECT_EQ(maxCas, metadata.maxCas());
    EXPECT_EQ(1, metadata.failovers()->size());
    EXPECT_EQ(1, metadata.openCollections()->entries()->size());
    EXPECT_EQ(1, metadata.scopes()->entries()->size());
}

TEST_P(ContinousBackupTest, CallbackOldSnapshot) {
    auto& store = getMockKVStore(vbid);

    auto initialSnapshot = store.makeFileHandle(vbid);
    ASSERT_TRUE(initialSnapshot.get());

    auto initialMetadataString =
            runContinuousBackupCallback(vbid, *initialSnapshot);
    auto& initialMetadata = Backup::decodeBackupMetadata(initialMetadataString);

    {
        CollectionsManifest cm;
        setCollections(
                cookie,
                cm.add(ScopeEntry::shop1)
                        .add(CollectionEntry::vegetable, ScopeEntry::shop1)
                        .add(CollectionEntry::fruit)
                        .add(CollectionEntry::dairy));
        flush_vbucket_to_disk(vbid, 4);
    }

    auto latestSnapshot = store.makeFileHandle(vbid);
    auto latestMetadataString =
            runContinuousBackupCallback(vbid, *latestSnapshot);
    auto& latestMetadata = Backup::decodeBackupMetadata(latestMetadataString);

    // Expect the manifest to have changed.
    ASSERT_EQ(initialMetadata.failovers()->size(),
              latestMetadata.failovers()->size());
    ASSERT_NE(initialMetadata.maxCas(), latestMetadata.maxCas());
    ASSERT_NE(initialMetadata.openCollections()->entries()->size(),
              latestMetadata.openCollections()->entries()->size());
    ASSERT_NE(initialMetadata.scopes()->entries()->size(),
              latestMetadata.scopes()->entries()->size());
    ASSERT_NE(initialMetadata.manifest()->uid(),
              latestMetadata.manifest()->uid());

    EXPECT_EQ(initialMetadataString,
              runContinuousBackupCallback(vbid, *initialSnapshot))
            << "Expected to read the same metadata from the initial snapshot.";
}

TEST_P(ContinousBackupTest, StartBackup) {
    auto& store = getMockKVStore(vbid);
    // Started and should remain started.
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, StopBackupOnStateChange) {
    auto& store = getMockKVStore(vbid);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

    // Stop after flush of vbucket_state_replica.
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, stopBackupOnVBucketDeletion) {
    auto& store = getMockKVStore(vbid);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

    size_t callbackCountBeforeDelete = 0;
    engine->getKVBucket()->getKVStoreStat("continuous_backup_callback_count",
                                          callbackCountBeforeDelete);

    engine->getKVBucket()->deleteVBucket(vbid);

    size_t callbackCountAfterDelete = 0;
    engine->getKVBucket()->getKVStoreStat("continuous_backup_callback_count",
                                          callbackCountAfterDelete);

    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));
    EXPECT_EQ(callbackCountBeforeDelete + 1, callbackCountAfterDelete);
}

TEST_P(ContinousBackupTest, BackupRequiresHistoryRetention) {
    auto& store = getMockKVStore(vbid);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setHistoryRetentionSeconds(0);
    dynamic_cast<EPBucket&>(*engine->getKVBucket()).flushVBucket(vbid);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setHistoryRetentionSeconds(30000);
    dynamic_cast<EPBucket&>(*engine->getKVBucket()).flushVBucket(vbid);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, DoNotStartBackupIfConfigDisabled) {
    auto& store = getMockKVStore(vbid);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setContinuousBackupEnabled(false);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, StartBackupOnConfigEnabled) {
    auto& store = getMockKVStore(vbid);
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_replica);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setContinuousBackupEnabled(false);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setContinuousBackupEnabled(true);
    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));

    // Changing the config queues a vbstate flush.
    dynamic_cast<EPBucket&>(*engine->getKVBucket()).flushVBucket(vbid);

    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, StopBackupOnConfigDisabled) {
    auto& store = getMockKVStore(vbid);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

    engine->getConfiguration().setContinuousBackupEnabled(false);
    EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

    // Changing the config queues a vbstate flush.
    dynamic_cast<EPBucket&>(*engine->getKVBucket()).flushVBucket(vbid);

    EXPECT_FALSE(store.isContinuousBackupStarted(vbid));
}

TEST_P(ContinousBackupTest, StartBackupAfterWarmup) {
    {
        auto& store = getMockKVStore(vbid);
        EXPECT_TRUE(store.isContinuousBackupStarted(vbid));

        reinitialise(config_string + ";warmup=true");
    }

    {
        replaceMagmaKVStore();
        auto& store = getMockKVStore(vbid);
        auto& kvBucket = *static_cast<EPBucket*>(engine->getKVBucket());

        EXPECT_FALSE(store.isContinuousBackupStarted(vbid));
        EXPECT_TRUE(store.isHistoryEvictionPaused());

        kvBucket.initializeWarmupTask();
        kvBucket.startWarmupTask();
        runReadersUntilWarmedUp();

        EXPECT_TRUE(store.isContinuousBackupStarted(vbid));
        EXPECT_FALSE(store.isHistoryEvictionPaused());
    }
}

TEST_P(ContinousBackupTest, Stats) {
    using namespace testing;
    auto& store = getMockKVStore(vbid);

    StrictMock<MockStatCollector> collector;
    auto bucketCollector = collector.forBucket("foo");

    {
        EXPECT_CALL(collector,
                    addStat(StatDefNameMatcher(
                                    "ep_continuous_backup_callback_count"),
                            A<uint64_t>(),
                            _));
        EXPECT_CALL(
                collector,
                addStat(StatDefNameMatcher(
                                "ep_continuous_backup_callback_time_seconds"),
                        A<uint64_t>(),
                        _));
        engine->doContinuousBackupStats(bucketCollector);
    }

    runContinuousBackupCallback(vbid, *store.makeFileHandle(vbid));

    {
        EXPECT_CALL(collector,
                    addStat(StatDefNameMatcher(
                                    "ep_continuous_backup_callback_count"),
                            A<uint64_t>(),
                            _));
        EXPECT_CALL(
                collector,
                addStat(StatDefNameMatcher(
                                "ep_continuous_backup_callback_time_seconds"),
                        A<uint64_t>(),
                        _));
        engine->doContinuousBackupStats(bucketCollector);
    }
}

INSTANTIATE_TEST_SUITE_P(ContinousBackupTests,
                         ContinousBackupTest,
                         STParameterizedBucketTest::magmaBucket(),
                         STParameterizedBucketTest::PrintToStringParamName);