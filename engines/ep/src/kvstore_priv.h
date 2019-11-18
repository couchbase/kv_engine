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

#include "callbacks.h"
#include "diskdockey.h"
#include "kvstore.h"
#include <boost/variant.hpp>
#include <unordered_map>

class KVStoreConfig;

/**
 * Callback for the mutation being performed.
 */
using MutationRequestCallback =
        boost::variant<KVStore::SetCallback, KVStore::DeleteCallback>;

class IORequest {
public:
    /**
     * @param cb callback to the engine. Also specifies if this is a set
     *        or delete based on the callback type.
     * @param key The key of the item to persist
     */
    IORequest(MutationRequestCallback, DiskDocKey key);

    ~IORequest();

    /// @returns true if the document to be persisted is for DELETE
    bool isDelete() const {
        return boost::get<KVStore::DeleteCallback>(&callback);
    }

    std::chrono::microseconds getDelta() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
    }

    const KVStore::SetCallback& getSetCallback() const {
        return boost::get<KVStore::SetCallback>(callback);
    }

    const KVStore::DeleteCallback& getDelCallback() const {
        return boost::get<KVStore::DeleteCallback>(callback);
    }

    const DiskDocKey& getKey() const {
        return key;
    }

protected:
    MutationRequestCallback callback;
    DiskDocKey key;
    std::chrono::steady_clock::time_point start;
};
