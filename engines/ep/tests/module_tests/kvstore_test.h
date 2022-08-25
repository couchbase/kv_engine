/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "callbacks.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest.h"
#include "configuration.h"
#include "kvstore/kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "vb_commit.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <memory>
#include <string>

class KVStoreConfig;
class KVStore;

class TestEPVBucketFactory {
public:
    static std::shared_ptr<VBucket> makeVBucket(Vbid vbid);
};

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
    std::unique_ptr<KVStoreIface> kvstore;
    Configuration config;
};

// Test fixture for tests which run on different KVStore implementations.
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

    bool isCouchstore() const {
        return config.getBackend() == "couchdb" || isNexusCouchstorePrimary();
    }

    bool isNexusCouchstorePrimary() const {
        return config.getBackend() == "nexus" &&
               config.getNexusPrimaryBackend() == "couchdb";
    }

    bool isNexus() const {
        return config.getBackend() == "nexus";
    }
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
                   cb::engine_errc expectedErrorCode = cb::engine_errc::success,
                   bool expectCompressed = false);

// Initializes a KVStore
void initialize_kv_store(KVStoreIface* kvstore, Vbid vbid = Vbid(0));

// Creates and initializes a KVStore with the given config
std::unique_ptr<KVStoreIface> setup_kv_store(KVStoreConfig& config,
                                             std::vector<Vbid> vbids = {
                                                     Vbid(0)});

class MockPersistenceCallback : public PersistenceCallback {
public:
    MockPersistenceCallback();
    ~MockPersistenceCallback() override;

    MOCK_METHOD2(setCallback, void(const Item&, FlushStateMutation));

    void operator()(const Item& qi, FlushStateMutation state) override {
        setCallback(qi, state);
    }

    MOCK_METHOD2(deleteCallback, void(const Item&, FlushStateDeletion));

    void operator()(const Item& qi, FlushStateDeletion state) override {
        deleteCallback(qi, state);
    }
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
    explicit GetCallback(
            cb::engine_errc _expectedErrorCode = cb::engine_errc::success)
        : expectCompressed(false), expectedErrorCode(_expectedErrorCode) {
    }

    explicit GetCallback(
            bool expect_compressed,
            cb::engine_errc _expectedErrorCode = cb::engine_errc::success)
        : expectCompressed(expect_compressed),
          expectedErrorCode(_expectedErrorCode) {
    }

    void callback(GetValue& result) override;

private:
    bool expectCompressed;
    cb::engine_errc expectedErrorCode;
};

class MockGetValueCallback : public StatusCallback<GetValue> {
public:
    MockGetValueCallback();
    ~MockGetValueCallback() override;

    MOCK_METHOD(void, callback, (GetValue&), (override));
};
