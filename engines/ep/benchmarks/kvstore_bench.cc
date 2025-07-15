/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "callbacks.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "environment.h"
#include "item.h"
#include "kvstore/kvstore_config.h"
#include "kvstore/kvstore_iface.h"
#include "kvstore/kvstore_transaction_context.h"
#include "tests/module_tests/test_helpers.h"
#include "vb_commit.h"
#include "vbucket_state.h"
#include <benchmark/benchmark.h>
#include <executor/workload.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>

using namespace std::string_literals;

enum Storage {
    COUCHSTORE = 0
#ifdef EP_USE_MAGMA
    ,
    MAGMA
#endif
};

class MockCacheCallback : public StatusCallback<CacheLookup> {
public:
    MockCacheCallback() = default;
    void callback(CacheLookup& lookup) override {
        // I want to simulate DGM scenarios where we have a HT-miss most times.
        // So, here I return what KVStore understands as "Item not in the
        // HashTable, go to the Storage".
        setStatus(cb::engine_errc::success);
    }
};

class MockDiskCallback : public StatusCallback<GetValue> {
public:
    MockDiskCallback() = default;
    void callback(GetValue& val) override {
        // Just increase the item count
        // Note: this callback is invoked for each item read from the storage.
        //     This is where the real DiskCallback pushes the Item to
        //     the DCP stream.
        itemCount++;
    }
    // Get the number of items found during a scan
    size_t getItemCount() const {
        return itemCount;
    }

protected:
    // Number of items found during a scan
    size_t itemCount = 0;
};

/*
 * Benchmark fixture for KVStore.
 */
class KVStoreBench : public benchmark::Fixture {
protected:
    void SetUp(benchmark::State& state) override {
        auto& env = Environment::get();
        env.engineFileDescriptors = env.reservedFileDescriptors;

        numItems = state.range(0);
        auto storage = state.range(1);

        // Initialize KVStoreConfig
        Configuration config;
        uint16_t shardId = 0;

        auto configStr = "dbname=KVStoreBench.db"s;
        config.setMaxSize(536870912);
        switch (storage) {
        case COUCHSTORE: {
            state.SetLabel("Couchstore");
            config.parseConfiguration(configStr + ";backend=couchdb");
            break;
        }
#ifdef EP_USE_MAGMA
        case MAGMA: {
            state.SetLabel("Magma");
            config.parseConfiguration(configStr + ";backend=magma");
            break;
        }
#endif
        }
        WorkLoadPolicy workload(
                gsl::narrow_cast<int>(config.getMaxNumWorkers()),
                gsl::narrow_cast<int>(config.getMaxNumShards()));

        // Initialize KVStore
        kvstoreConfig = KVStoreConfig::createKVStoreConfig(
                config,
                config.getBackendString(),
                gsl::narrow_cast<uint16_t>(workload.getNumShards()),
                shardId);

        std::filesystem::remove_all(kvstoreConfig->getDBName());
        std::filesystem::create_directories(kvstoreConfig->getDBName());
        kvstore = setup_kv_store(*kvstoreConfig);

        // Load some data
        const std::string key = "key";
        std::string value = "value";
        auto ctx =
                kvstore->begin(vbid, std::make_unique<PersistenceCallback>());
        for (uint64_t i = 1; i <= numItems; i++) {
            auto docKey = makeStoredDocKey(key + std::to_string(i));
            auto qi = makeCommittedItem(docKey, value);
            qi->setBySeqno(i);
            kvstore->set(*ctx, qi);
        }

        Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
        VB::Commit f(m);
        f.proposedVBState.lastSnapStart = 0;
        f.proposedVBState.lastSnapEnd = numItems;

        kvstore->commit(std::move(ctx), f);
        // Just check that the VBucket High Seqno has been updated correctly
        EXPECT_EQ(kvstore->getCachedVBucketState(vbid)->highSeqno, numItems);
    }

    void TearDown(const benchmark::State& state) override {
        kvstore.reset();
        std::filesystem::remove_all(kvstoreConfig->getDBName());
    }

private:
    std::unique_ptr<KVStoreIface> setup_kv_store(KVStoreConfig& config) {
        auto kvstore = KVStoreFactory::create(config, {}, {});
        Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
        VB::Commit meta(m);
        meta.proposedVBState.transition.state = vbucket_state_active;
        if (!kvstore->snapshotVBucket(vbid, meta)) {
            throw std::runtime_error(
                    "Could not persist vbstate, benchmark "
                    "cannot continue");
        }
        return kvstore;
    }

protected:
    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStoreIface> kvstore;
    Vbid vbid{0};
    uint64_t numItems;
};

/*
 * Benchmark for KVStore::scan()
 */
BENCHMARK_DEFINE_F(KVStoreBench, Scan)(benchmark::State& state) {
    size_t itemCountTotal = 0;

    while (state.KeepRunning()) {
        // Note: the CacheCallback here just make the code to flow into reading
        // data from disk
        auto scanContext = kvstore->initBySeqnoScanContext(
                std::make_unique<MockDiskCallback>(),
                std::make_unique<MockCacheCallback>(),
                vbid,
                0 /*startSeqno*/,
                DocumentFilter::ALL_ITEMS,
                ValueFilter::VALUES_COMPRESSED,
                SnapshotSource::Head);
        ASSERT_TRUE(scanContext);

        auto scanStatus = kvstore->scan(*scanContext);
        ASSERT_EQ(scanStatus, ScanStatus::Success);
        const auto& callback = static_cast<const MockDiskCallback&>(
                scanContext->getValueCallback());

        auto itemCount = callback.getItemCount();
        ASSERT_EQ(itemCount, numItems);
        itemCountTotal += itemCount;
    }

    state.SetItemsProcessed(itemCountTotal);
}

BENCHMARK_DEFINE_F(KVStoreBench, GetAggrDbFileInfo)(benchmark::State& state) {
    while (state.KeepRunning()) {
        kvstore->getAggrDbFileInfo();
    }
}

const int NUM_ITEMS = 100000;

BENCHMARK_REGISTER_F(KVStoreBench, Scan)
        ->Args({NUM_ITEMS, COUCHSTORE})
#ifdef EP_USE_MAGMA
        ->Args({NUM_ITEMS, MAGMA})
#endif
        ;

BENCHMARK_REGISTER_F(KVStoreBench, GetAggrDbFileInfo)
        ->Args({NUM_ITEMS, COUCHSTORE})
#ifdef EP_USE_MAGMA
        ->Args({NUM_ITEMS, MAGMA})
#endif
        ;