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

    bool operator==(const ScanContext& other) const {
        return dropped == other.dropped && startSeqno == other.startSeqno &&
               endSeqno == other.endSeqno;
    }

    bool operator!=(const ScanContext& other) const {
        return !operator==(other);
    }

    /**
     * Filtering function for use by KVStore scans when a scan is required to
     * filter out the items of a dropped collection. This function can be called
     * with mutations or system events and will be able to handle both. System
     * events require the key "decoding" to extract the event type and an ID.
     * For collection events Create/Modify they will be considered logically
     * deleted if they are in dropped collection. With the addition of history
     * retention a KVStore may now scan and see create(c1), drop(c1) - the
     * create must be filtered, the drop remains visible. Prior to history
     * retention create(c1) or drop(c1) would be seen, but not both. The
     * isDeleted flag is the only way to distinguish these events (they have
     * the same key).
     *
     * @param key The key of the document
     * @param isDeleted if the key represents a deleted document
     * @param seqno the seqno of the key
     * @return true if the key@seqno belongs to a dropped collection
     */
    bool isLogicallyDeleted(const DocKey& key,
                            bool isDeleted,
                            uint64_t seqno) const;

    /// @return if true dropped set is empty
    bool empty() const {
        return dropped.empty();
    }

    /// @return the set of dropped collections
    const std::unordered_set<CollectionID>& getDroppedCollections() const {
        return dropped;
    }

    /// @return the endSeqno - the maximum endSeqno of all dropped collections
    uint64_t getEndSeqno() const {
        return endSeqno;
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
