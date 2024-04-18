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
    ScopeEntry(SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta)
        : sharedMeta(std::move(sharedMeta)) {
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
    SingleThreadedRCPtr<ScopeSharedMetaData> sharedMeta;
};

std::ostream& operator<<(std::ostream& os, const ScopeEntry& scopeEntry);

} // namespace Collections::VB
