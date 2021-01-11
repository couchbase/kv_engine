/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#pragma once

#include "callbacks.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "kvstore.h"
#include "vb_commit.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <memory>
#include <string>

class KVStoreConfig;
class KVStore;

/**
 * Test fixture for KVStore tests.
 */
class KVStoreTest : public ::testing::Test {
public:
    KVStoreTest();

protected:
    void SetUp() override;

    void TearDown() override;

    std::unique_lock<std::mutex> getVbLock() {
        return std::unique_lock<std::mutex>{vbLock};
    }

    std::string data_dir;
    Collections::VB::Manifest manifest{
            std::make_shared<Collections::Manager>()};
    VB::Commit flush;
    const Vbid vbid = Vbid(0);
    std::mutex vbLock;
};

// class that takes the backend configuration parameter only and initialises
// the KVStore which tests can use
class KVStoreBackend {
public:
    void setup(const std::string& dataDir, const std::string& backend);

    void teardown();

    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;

    // KV-engine may have one or two instances of the KVStore. With couchstore
    // two instances exist.
    std::unique_ptr<KVStore> kvsReadOnly;
    KVStore* kvstoreReadOnly;
};

// Test fixture for tests which run on all KVStore implementations (Couchstore
// and RocksDB).
// The string parameter represents the KVStore implementation that each test
// of this class will use (e.g., "couchdb", "magma" or "rocksdb").
class KVStoreParamTest : public KVStoreTest,
                         public KVStoreBackend,
                         public ::testing::WithParamInterface<std::string> {
public:
    static auto persistentConfigValues() {
        using namespace std::string_literals;
        return ::testing::Values(
#ifdef EP_USE_MAGMA
                "magma",
#endif
                "couchdb");
    }

protected:
    void SetUp() override;

    void TearDown() override;

    /**
     * @returns true if the KVStore implementation this test is parameterized
     * for supports bgfetching items in Snappy compressed format.
     */
    bool supportsFetchingAsSnappy() const;

    /**
     * Test that the io_bg_fetch_docs_read stat is tracked correctly for gets
     *
     * @param deleted run the test for a deleted doc?
     */
    void testBgFetchDocsReadGet(bool deleted);

    /**
     * Test that the io_bg_fetch_docs_read stat is tracked correctly for
     * getMulti
     *
     * @param deleted run the test for a deleted doc?
     * @param filter run the fetch with the given ValueFilter.
     */
    void testBgFetchDocsReadGetMulti(bool deleted, ValueFilter filter);

    // Test different combinations of ValueFilter for bgfetch of the same key.
    void testBgFetchValueFilter(ValueFilter requestMode1,
                                ValueFilter requestMode2,
                                ValueFilter fetchedMode);

    // Test getRange with the given ValueFilter.
    void testGetRange(ValueFilter filter);

    // Helper method to store a test document named "key".
    queued_item storeDocument(bool deleted = false);

    /// Checks the result of a bgfetch (fetched) matches the original testDoc
    /// docucment, when the given ValueFilter was used.
    void checkBGFetchResult(const ValueFilter& filter,
                            const Item& testDoc,
                            const vb_bgfetch_item_ctx_t& fetched) const;
};

/**
 * Utility template for generating callbacks for various
 * KVStore functions from a lambda/std::function
 */
template <typename... RV>
class CustomCallback : public StatusCallback<RV...> {
public:
    explicit CustomCallback(std::function<void(RV...)> _cb) : cb(_cb) {
    }
    CustomCallback() : cb([](RV... val) {}) {
    }

    void callback(RV&... result) override {
        cb(std::forward<RV>(result)...);
        processed++;
    }

    uint32_t getProcessedCount() {
        return processed;
    }

protected:
    std::function<void(RV...)> cb;

private:
    uint32_t processed = 0;
};

/**
 * Callback that can be given a lambda to use, specifically
 * for the Rollback callback
 */
class CustomRBCallback : public RollbackCB {
public:
    explicit CustomRBCallback(std::function<void(GetValue)> _cb)
        : cb(std::move(_cb)) {
    }
    CustomRBCallback() : cb([](GetValue val) {}) {
    }

    void callback(GetValue& result) override {
        cb(std::move(result));
    }

protected:
    std::function<void(GetValue)> cb;
};

void checkGetValue(GetValue& result,
                   ENGINE_ERROR_CODE expectedErrorCode = ENGINE_SUCCESS,
                   bool expectCompressed = false);

// Initializes a KVStore
void initialize_kv_store(KVStore* kvstore, Vbid vbid = Vbid(0));

// Creates and initializes a KVStore with the given config
std::unique_ptr<KVStore> setup_kv_store(KVStoreConfig& config,
                                        std::vector<Vbid> vbids = {Vbid(0)});

/* Test callback for stats handling.
 * 'cookie' is a std::unordered_map<std::string, std::string) which stats
 * are accumulated in.
 */
void add_stat_callback(std::string_view key,
                       std::string_view value,
                       gsl::not_null<const void*> cookie);

class MockTransactionContext : public TransactionContext {
public:
    MockTransactionContext(Vbid vb) : TransactionContext(vb) {
    }

    MOCK_METHOD2(setCallback,
                 void(const queued_item&, KVStore::FlushStateMutation));
    MOCK_METHOD2(deleteCallback,
                 void(const queued_item&, KVStore::FlushStateDeletion));
};

class KVStoreTestCacheCallback : public StatusCallback<CacheLookup> {
public:
    KVStoreTestCacheCallback(int64_t s, int64_t e, Vbid vbid)
        : start(s), end(e), vb(vbid) {
    }

    void callback(CacheLookup& lookup) override;

private:
    int64_t start;
    int64_t end;
    Vbid vb;
};

class GetCallback : public StatusCallback<GetValue> {
public:
    explicit GetCallback(ENGINE_ERROR_CODE _expectedErrorCode = ENGINE_SUCCESS)
        : expectCompressed(false), expectedErrorCode(_expectedErrorCode) {
    }

    explicit GetCallback(bool expect_compressed,
                         ENGINE_ERROR_CODE _expectedErrorCode = ENGINE_SUCCESS)
        : expectCompressed(expect_compressed),
          expectedErrorCode(_expectedErrorCode) {
    }

    void callback(GetValue& result) override;

private:
    bool expectCompressed;
    ENGINE_ERROR_CODE expectedErrorCode;
};

class MockGetValueCallback : public StatusCallback<GetValue> {
public:
    MockGetValueCallback();
    ~MockGetValueCallback() override;

    MOCK_METHOD(void, callback, (GetValue&), (override));
};
