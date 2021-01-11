/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "callbacks.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "item.h"
#include "kvstore.h"
#include "kvstore_config.h"
#include "vb_commit.h"
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "tests/module_tests/test_helpers.h"
#include "vbucket_state.h"

#include <benchmark/benchmark.h>
#include <engines/ep/src/workload.h>
#include <folly/portability/GTest.h>
#include <platform/dirutils.h>
#include <programs/engine_testapp/mock_server.h>

using namespace std::string_literals;

enum Storage {
    COUCHSTORE = 0
#ifdef EP_USE_ROCKSDB
    ,
    ROCKSDB
#endif
};

class MockWriteCallback {
public:
    void operator()(TransactionContext&, KVStore::FlushStateMutation) {
    }
};

class MockCacheCallback : public StatusCallback<CacheLookup> {
public:
    MockCacheCallback(){};
    void callback(CacheLookup& lookup) override {
        // I want to simulate DGM scenarios where we have a HT-miss most times.
        // So, here I return what KVStore understands as "Item not in the
        // HashTable, go to the Storage".
        setStatus(ENGINE_SUCCESS);
    };
};

class MockDiskCallback : public StatusCallback<GetValue> {
public:
    MockDiskCallback() : itemCount(0){};
    void callback(GetValue& val) override {
        // Just increase the item count
        // Note: this callback is invoked for each item read from the storage.
        //     This is where the real DiskCallback pushes the Item to
        //     the DCP stream.
        itemCount++;
    };
    // Get the number of items found during a scan
    size_t getItemCount() const {
        return itemCount;
    }

protected:
    // Number of items found during a scan
    size_t itemCount;
};

/*
 * Benchmark fixture for KVStore.
 */
class KVStoreBench : public benchmark::Fixture {
protected:
    void SetUp(benchmark::State& state) override {
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
            config.parseConfiguration((configStr + ";backend=couchdb").c_str(),
                                      get_mock_server_api());
            WorkLoadPolicy workload(config.getMaxNumWorkers(),
                                    config.getMaxNumShards());
            kvstoreConfig = std::make_unique<KVStoreConfig>(
                    config, workload.getNumShards(), shardId);
            break;
        }
#ifdef EP_USE_ROCKSDB
        case ROCKSDB: {
            state.SetLabel("CouchRocks");
            config.parseConfiguration((configStr + ";backend=rocksdb").c_str(),
                                      get_mock_server_api());
            WorkLoadPolicy workload(config.getMaxNumWorkers(),
                                    config.getMaxNumShards());
            kvstoreConfig = std::make_unique<RocksDBKVStoreConfig>(
                    config, workload.getNumShards(), shardId);
            break;
        }
#endif
        }

        // Initialize KVStore
        kvstore = setup_kv_store(*kvstoreConfig);

        // Load some data
        const std::string key = "key";
        std::string value = "value";
        Vbid vbid = Vbid(0);
        kvstore->begin(std::make_unique<TransactionContext>(vbid));
        for (int i = 1; i <= numItems; i++) {
            auto docKey = makeStoredDocKey(key + std::to_string(i));
            auto qi = makeCommittedItem(docKey, value);
            qi->setBySeqno(i);
            kvstore->set(qi);
        }

        Collections::VB::Manifest m{std::make_shared<Collections::Manager>()};
        VB::Commit f(m);

        kvstore->commit(f);
        // Just check that the VBucket High Seqno has been updated correctly
        EXPECT_EQ(kvstore->getCachedVBucketState(vbid)->highSeqno, numItems);
    }

    void TearDown(const benchmark::State& state) override {
        kvstore.reset();
        cb::io::rmrf(kvstoreConfig->getDBName());
    }

private:
    std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config) {
        auto kvstore = KVStoreFactory::create(config);
        vbucket_state state;
        state.transition.state = vbucket_state_active;
        kvstore.rw->snapshotVBucket(vbid, state);
        return std::move(kvstore.rw);
    }

protected:
    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
    Vbid vbid = Vbid(0);
    int numItems;
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
        ASSERT_EQ(scanStatus, scan_success);
        const auto& callback = static_cast<const MockDiskCallback&>(
                scanContext->getValueCallback());

        auto itemCount = callback.getItemCount();
        ASSERT_EQ(itemCount, numItems);
        itemCountTotal += itemCount;
    }

    state.SetItemsProcessed(itemCountTotal);
}

const int NUM_ITEMS = 100000;

BENCHMARK_REGISTER_F(KVStoreBench, Scan)
        ->Args({NUM_ITEMS, COUCHSTORE})
#ifdef EP_USE_ROCKSDB
        ->Args({NUM_ITEMS, ROCKSDB})
#endif
        ;
