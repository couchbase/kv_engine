/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/collections_types.h"

#include <unordered_set>

struct DocKey;

namespace Collections {

namespace KVStore {
struct DroppedCollection;
}

namespace VB {

/**
 * The ScanContext holds data relevant to performing a scan of the disk index
 * e.g. collection erasing may iterate the index and use data with the
 * ScanContext for choosing which keys to ignore.
 */
class ScanContext {
public:
    explicit ScanContext(
            const std::vector<Collections::KVStore::DroppedCollection>&
                    droppedCollections);

    /// @return true if the key@seqno belongs to a dropped collection
    bool isLogicallyDeleted(const DocKey& key, uint64_t seqno) const;

    /// @return if true dropped set is empty
    bool empty() const {
        return dropped.empty();
    }

    /// @return the set of dropped collections
    const std::unordered_set<CollectionID>& getDroppedCollections() const {
        return dropped;
    }

protected:
    friend std::ostream& operator<<(std::ostream&, const ScanContext&);

    std::unordered_set<CollectionID> dropped;
    uint64_t startSeqno = std::numeric_limits<uint64_t>::max();
    uint64_t endSeqno = 0;
};

std::ostream& operator<<(std::ostream& os, const ScanContext& scanContext);

} // namespace VB
} // namespace Collections
