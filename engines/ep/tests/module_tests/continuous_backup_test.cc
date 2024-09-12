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
#include "tests/mock/mock_magma_kvstore.h"
#include "tests/module_tests/evp_store_single_threaded_test.h"
#include "tests/module_tests/test_helpers.h"
#include "vbucket.h"
#include <nlohmann/json_fwd.hpp>
#include <platform/cb_arena_malloc.h>
#include <platform/cb_arena_malloc_client.h>
#include <cstdint>

class ContinousBackupTest : public STParameterizedBucketTest {
public:
    void SetUp() override {
        config_string +=
                "continuous_backup_enabled=true;"
                "continuous_backup_interval=1000";

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

    EXPECT_EQ(store.isContinuousBackupEnabled(), true);
    EXPECT_EQ(store.getContinuousBackupInterval(), 1000s);

    engine->getConfiguration().setContinuousBackupEnabled(false);
    engine->getConfiguration().setContinuousBackupInterval(123);

    EXPECT_EQ(store.isContinuousBackupEnabled(), false);
    EXPECT_EQ(store.getContinuousBackupInterval(), 123s);
}

TEST_P(ContinousBackupTest, CallbackInitialSnapshot) {
    auto& store = getMockKVStore(vbid);
    const auto maxCas = engine->getKVBucket()->getVBucket(vbid)->getMaxCas();

    auto initialSnapshot = store.makeFileHandle(vbid);
    ASSERT_TRUE(initialSnapshot.get());

    auto metadataString = runContinuousBackupCallback(vbid, *initialSnapshot);

    auto metadata = Backup::decodeBackupMetadata(metadataString);
    EXPECT_EQ(maxCas, metadata["maxCas"].template get<uint64_t>());
    EXPECT_TRUE(metadata["failovers"].is_array());
    EXPECT_EQ(1, metadata["failovers"].size()) << metadata.dump();
}

INSTANTIATE_TEST_SUITE_P(ContinousBackupTests,
                         ContinousBackupTest,
                         STParameterizedBucketTest::magmaBucket(),
                         STParameterizedBucketTest::PrintToStringParamName);