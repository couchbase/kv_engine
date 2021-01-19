/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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
#include "diskdockey.h"
#include "globaltask.h"

#include <memcached/engine_common.h>
#include <optional>

class EventuallyPersistentEngine;

enum class AllKeysCallbackStatus {
    KeyAdded,
    KeySkipped,
};

/**
 * Callback class used by AllKeysAPI, for caching fetched keys
 *
 * As by default (or in most cases), number of keys is 1000,
 * and an average key could be 32B in length, initialize buffersize of
 * allKeys to 34000 (1000 * 32 + 1000 * 2), the additional 2 bytes per
 * key is for the keylength.
 *
 * This initially allocated buffersize is doubled whenever the length
 * of the buffer holding all the keys, crosses the buffersize.
 */
class AllKeysCallback : public StatusCallback<const DiskDocKey&> {
public:
    AllKeysCallback(std::optional<CollectionID> collection, uint32_t maxCount)
        : collection(std::move(collection)), maxCount(maxCount) {
        buffer.reserve((avgKeySize + sizeof(uint16_t)) * expNumKeys);
    }

    void callback(const DiskDocKey& key) override;

    char* getAllKeysPtr() {
        return buffer.data();
    }
    uint64_t getAllKeysLen() {
        return buffer.size();
    }

private:
    std::vector<char> buffer;
    std::optional<CollectionID> collection;
    uint32_t addedKeyCount = 0;
    uint32_t maxCount = 0;
    static const int avgKeySize = 32;
    static const int expNumKeys = 1000;
};

/*
 * Task that fetches all_docs and returns response,
 * runs in background.
 */
class FetchAllKeysTask : public GlobalTask {
public:
    FetchAllKeysTask(EventuallyPersistentEngine* e,
                     const void* c,
                     AddResponseFn resp,
                     const DocKey start_key_,
                     Vbid vbucket,
                     uint32_t count_,
                     std::optional<CollectionID> collection);

    std::string getDescription() override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Duration will be a function of how many documents are fetched;
        // however for simplicity just return a fixed "reasonable" duration.
        return std::chrono::milliseconds(100);
    }

    bool run() override;

private:
    const void* cookie;
    const std::string description;
    AddResponseFn response;
    DiskDocKey start_key;
    Vbid vbid;
    uint32_t count;
    std::optional<CollectionID> collection;
};