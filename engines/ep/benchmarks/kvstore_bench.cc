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
#include "collections/vbucket_manifest.h"
#include "kvstore.h"
#include "kvstore_config.h"
#ifdef EP_USE_ROCKSDB
#include "rocksdb-kvstore/rocksdb-kvstore_config.h"
#endif
#include "tests/module_tests/test_helpers.h"

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <platform/dirutils.h>

enum Storage {
    COUCHSTORE = 0
#ifdef EP_USE_ROCKSDB
    ,
    ROCKSDB
#endif
};

class MockWriteCallback : public Callback<TransactionContext, mutation_result> {
public:
    MockWriteCallback() {
    }
    void callback(TransactionContext&, mutation_result& result) {
    }
};

class MockCacheCallback : public StatusCallback<CacheLookup> {
public:
    MockCacheCallback(){};
    void callback(CacheLookup& lookup) {
        // I want to simulate DGM scenarios where we have a HT-miss most times.
        // So, here I return what KVStore understands as "Item not in the
        // HashTable, go to the Storage".
        setStatus(ENGINE_SUCCESS);
    };
};

class MockDiskCallback : public StatusCallback<GetValue> {
public:
    MockDiskCallback() : itemCount(0){};
    void callback(GetValue& val) {
        // Just increase the item count
        // Note: this callback is invoked for each item read from the storage.
        //     This is where the real DiskCallback pushes the Item to
        //     the DCP stream.
        itemCount++;
    };
    // Get the number of items found during a scan
    size_t getItemCount() {
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
        config.setDbname("KVStoreBench.db");
        config.setMaxSize(536870912);
        switch (storage) {
        case COUCHSTORE:
            state.SetLabel("Couchstore");
            config.setBackend("couchdb");
            kvstoreConfig = std::make_unique<KVStoreConfig>(config, shardId);
            break;
#ifdef EP_USE_ROCKSDB
        case ROCKSDB:
            state.SetLabel("CouchRocks");
            config.setBackend("rocksdb");
            kvstoreConfig =
                    std::make_unique<RocksDBKVStoreConfig>(config, shardId);
            break;
#endif
        }

        // Initialize KVStore
        kvstore = setup_kv_store(*kvstoreConfig);

        // Load some data
        const std::string key = "key";
        std::string value = "value";
        MockWriteCallback wc;
        uint16_t vbid = 0;
        kvstore->begin(std::make_unique<TransactionContext>());
        for (int i = 1; i <= numItems; i++) {
            Item item(makeStoredDocKey(key + std::to_string(i)),
                      0 /*flags*/,
                      0 /*exptime*/,
                      value.c_str(),
                      value.size(),
                      PROTOCOL_BINARY_RAW_BYTES,
                      0 /*cas*/,
                      i /*bySeqno*/,
                      vbid);
            kvstore->set(item, wc);
        }
        Collections::VB::Manifest m({});
        Collections::VB::Flush f(m);
        kvstore->commit(f);
        // Just check that the VBucket High Seqno has been updated correctly
        EXPECT_EQ(kvstore->getVBucketState(vbid)->highSeqno, numItems);
    }

    void TearDown(const benchmark::State& state) override {
        kvstore.reset();
        cb::io::rmrf(kvstoreConfig->getDBName());
    }

private:
    std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config) {
        auto kvstore = KVStoreFactory::create(config);
        vbucket_state state(vbucket_state_active,
                            0 /*chkid*/,
                            0 /*maxDelSeqNum*/,
                            0 /*highSeqno*/,
                            0 /*purgeSeqno*/,
                            0 /*lastSnapStart*/,
                            0 /*lastSnapEnd*/,
                            0 /*maxCas*/,
                            0 /*hlcCasEpochSeqno*/,
                            false /*mightContainXattrs*/,
                            "" /*failovers*/);
        kvstore.rw->snapshotVBucket(
                vbid, state, VBStatePersist::VBSTATE_PERSIST_WITHOUT_COMMIT);
        return std::move(kvstore.rw);
    }

protected:
    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
    uint16_t vbid = 0;
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
        auto cb = std::make_shared<MockDiskCallback>();
        auto cl = std::make_shared<MockCacheCallback>();
        ScanContext* scanContext =
                kvstore->initScanContext(cb,
                                         cl,
                                         vbid,
                                         0 /*startSeqno*/,
                                         DocumentFilter::ALL_ITEMS,
                                         ValueFilter::VALUES_COMPRESSED);
        ASSERT_TRUE(scanContext);

        auto scanStatus = kvstore->scan(scanContext);
        ASSERT_EQ(scanStatus, scan_success);
        auto itemCount = cb->getItemCount();
        ASSERT_EQ(itemCount, numItems);
        itemCountTotal += itemCount;

        kvstore->destroyScanContext(scanContext);
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
