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

    std::string data_dir;
    Collections::VB::Manifest manifest;
    VB::Commit flush;
    Vbid vbid = Vbid(0);
};

// Test fixture for tests which run on all KVStore implementations (Couchstore
// and RocksDB).
// The string parameter represents the KVStore implementation that each test
// of this class will use (e.g., "couchdb" and "rocksdb").
class KVStoreParamTest : public KVStoreTest,
                         public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override;

    void TearDown() override;

    std::unique_ptr<KVStoreConfig> kvstoreConfig;
    std::unique_ptr<KVStore> kvstore;
};

/**
 * Utility template for generating callbacks for various
 * KVStore functions from a lambda/std::function
 */
template <typename... RV>
class CustomCallback : public StatusCallback<RV...> {
public:
    CustomCallback(std::function<void(RV...)> _cb) : cb(_cb) {
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
    CustomRBCallback(std::function<void(GetValue)> _cb) : cb(std::move(_cb)) {
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
