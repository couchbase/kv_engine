/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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
