/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memory>
#include <typeinfo>

struct EngineIface;

namespace cb {
/**
 * Interface type for engine-specific storage.
 */
class EngineStorageIface {
public:
    EngineStorageIface() = default;
    virtual ~EngineStorageIface() = default;

    EngineStorageIface(const EngineStorageIface&) = delete;
    EngineStorageIface(EngineStorageIface&&) = delete;
    EngineStorageIface& operator=(const EngineStorageIface&) = delete;
    EngineStorageIface& operator=(EngineStorageIface&&) = delete;

protected:
    friend struct EngineStorageDeleter;

    /**
     * Deallocate *this.
     * Called by EngineStorageDeleter for objects owned by a
     * unique_engine_storage_ptr.
     */
    virtual void deallocate() const = 0;
};

/**
 * A deleter for engine specific storage.
 */
struct EngineStorageDeleter {
    void operator()(const EngineStorageIface* engine_storage) const;
};

/**
 * A unique_ptr to use with engine storage allocated from the engine interface.
 */
using unique_engine_storage_ptr =
        std::unique_ptr<EngineStorageIface, EngineStorageDeleter>;
} // namespace cb
