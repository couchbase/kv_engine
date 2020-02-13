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
#include "item.h"
#include "kvstore.h"
#include <boost/variant.hpp>
#include <unordered_map>

class KVStoreConfig;

class IORequest {
public:
    IORequest(queued_item item);
    ~IORequest();

    /// @returns true if the document to be persisted is for a delete.
    bool isDelete() const;

    std::chrono::microseconds getDelta() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
    }

    const DiskDocKey& getKey() const {
        return key;
    }

    const queued_item getItem() const {
        return item;
    }

protected:
    const queued_item item;
    DiskDocKey key;
    std::chrono::steady_clock::time_point start;
};
