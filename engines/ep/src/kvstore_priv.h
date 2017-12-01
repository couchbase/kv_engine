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

#include <cJSON.h>

#include <unordered_map>

class KVStoreConfig;

static const int MUTATION_FAILED = -1;
static const int DOC_NOT_FOUND = 0;
static const int MUTATION_SUCCESS = 1;

struct KVStatsCtx {
    KVStatsCtx(const KVStoreConfig& _config)
        : vbucket(std::numeric_limits<uint16_t>::max()), config(_config) {
    }

    uint16_t vbucket;
    std::unordered_map<StoredDocKey, kstat_entry_t> keyStats;
    const KVStoreConfig& config;
};

typedef struct KVStatsCtx kvstats_ctx;

typedef union {
    Callback<TransactionContext, mutation_result>* setCb;
    Callback<TransactionContext, int>* delCb;
} MutationRequestCallback;

class IORequest {
public:
    IORequest(uint16_t vbId,
              MutationRequestCallback& cb,
              bool del,
              const DocKey itmKey);

    virtual ~IORequest() {
    }

    bool isDelete() {
        return deleteItem;
    }

    uint16_t getVBucketId() {
        return vbucketId;
    }

    std::chrono::microseconds getDelta() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                ProcessClock::now() - start);
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
    uint16_t vbucketId;
    bool deleteItem;
    MutationRequestCallback callback;
    ProcessClock::time_point start;
    StoredDocKey key;
    size_t dataSize;
};

inline const std::string getJSONObjString(const cJSON* i) {
    if (i == NULL) {
        return "";
    }
    if (i->type != cJSON_String) {
        throw std::invalid_argument("getJSONObjString: type of object (" +
                                    std::to_string(i->type) +
                                    ") is not cJSON_String");
    }
    return i->valuestring;
}

inline const bool getJSONObjBool(const cJSON* i) {
    if (i == nullptr) {
        return false;
    } else if (i->type != cJSON_True && i->type != cJSON_False) {
        throw std::invalid_argument("getJSONObjBool: type of object (" +
                                    std::to_string(i->type) + ") is not bool");
    }
    return i->type == cJSON_True;
}
