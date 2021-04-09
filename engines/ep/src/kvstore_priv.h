/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/**
 * Types and functions private to KVStore implementations.
 */

#include "callbacks.h"
#include "diskdockey.h"
#include "item.h"
#include "kvstore.h"
#include <unordered_map>
#include <variant>

class KVStoreConfig;

class IORequest {
public:
    explicit IORequest(queued_item item);
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
