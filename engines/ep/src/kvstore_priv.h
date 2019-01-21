/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

/**
 * Types and functions private to KVStore implementations.
 */

#include "config.h"

#include "callbacks.h"
#include "kvstore.h"
#include <unordered_map>

class KVStoreConfig;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

typedef union {
    Callback<TransactionContext, mutation_result>* setCb;
    Callback<TransactionContext, int>* delCb;
} MutationRequestCallback;

class IORequest {
public:
    /**
     * @param vbid
     * @param cb callback to the engine
     * @param del Is deletion?
     * @param key The key of the item to persist
     * @param pending Prefix for Pending SyncWrite?
     */
    IORequest(Vbid vbid,
              MutationRequestCallback& cb,
              bool del,
              const DocKey key,
              bool pending = false);

    virtual ~IORequest() {
    }

    bool isDelete() {
        return deleteItem;
    }

    Vbid getVBucketId() {
        return vbucketId;
    }

    std::chrono::microseconds getDelta() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
    }

    Callback<TransactionContext, mutation_result>* getSetCallback(void) {
        return callback.setCb;
    }

    Callback<TransactionContext, int>* getDelCallback(void) {
        return callback.delCb;
    }

    const StoredDocKey& getKey(void) const {
        return key;
    }

protected:
    Vbid vbucketId;
    bool deleteItem;
    MutationRequestCallback callback;
    std::chrono::steady_clock::time_point start;
    StoredDocKey key;
    size_t dataSize;
};
