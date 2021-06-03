/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "callbacks.h"
#include "diskdockey.h"
#include <executor/globaltask.h>

#include <memcached/engine_common.h>
#include <optional>

class CookieIface;
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
                     const CookieIface* c,
                     AddResponseFn resp,
                     const DocKey start_key_,
                     Vbid vbucket,
                     uint32_t count_,
                     std::optional<CollectionID> collection);

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Duration will be a function of how many documents are fetched;
        // however for simplicity just return a fixed "reasonable" duration.
        return std::chrono::milliseconds(100);
    }

    bool run() override;

private:
    const CookieIface* cookie;
    const std::string description;
    AddResponseFn response;
    DiskDocKey start_key;
    Vbid vbid;
    uint32_t count;
    std::optional<CollectionID> collection;
};
