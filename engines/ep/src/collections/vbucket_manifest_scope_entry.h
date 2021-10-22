/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/collections_types.h"

#include <platform/atomic.h>
#include <platform/non_negative_counter.h>

#include <cstddef>
#include <iosfwd>

namespace Collections::VB {

/**
 * The Collections::VB::ScopeEntry stores the data a scope needs per vbucket
 */
class ScopeEntry {
public:
    ScopeEntry(size_t dataSize,
               SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta)
        : dataSize(dataSize), sharedMeta(std::move(sharedMeta)) {
    }

    // Mark copy and move as deleted as it simplifies the lifetime of
    // ScopeSharedMetaData
    ScopeEntry(const ScopeEntry&) = delete;
    ScopeEntry& operator=(const ScopeEntry&) = delete;
    ScopeEntry(ScopeEntry&&) = delete;
    ScopeEntry& operator=(ScopeEntry&&) = delete;

    bool operator==(const ScopeEntry& other) const;
    bool operator!=(const ScopeEntry& other) const {
        return !(*this == other);
    }

    /// @return the size of the data stored in the scope
    size_t getDataSize() const {
        return dataSize;
    }

    /// Update the size of the data of this scope
    /// @param delta the delta to apply to the current data size
    void updateDataSize(ssize_t delta) const {
        dataSize += delta;
    }

    /// Set the data size to the given value
    void setDataSize(size_t value) const {
        dataSize = value;
    }

    /// @return the name of the scope
    std::string_view getName() const {
        return sharedMeta->name;
    }

    /**
     * Take the ScopeSharedMetaData from this entry (moves the data) to
     * the caller.
     */
    SingleThreadedRCPtr<ScopeSharedMetaData>&& takeMeta() {
        return std::move(sharedMeta);
    }

private:
    // @todo MB-49040: Remove the clamp at zero policy.
    // This is here for now as warmup will return this to zero and we underflow
    // if a delete occurs. A later commit fixes warmup and changes this
    mutable cb::NonNegativeCounter<size_t, cb::ClampAtZeroUnderflowPolicy>
            dataSize;
    SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta;
};

std::ostream& operator<<(std::ostream& os, const ScopeEntry& scopeEntry);

} // namespace Collections::VB
