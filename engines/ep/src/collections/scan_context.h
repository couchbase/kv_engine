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

#include <unordered_map>
#include <unordered_set>

struct DocKey;

namespace Collections {

namespace KVStore {
struct OpenCollection;
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
    /**
     * Construct a ScanContext which always requires a vector of dropped
     * collections and optionally a vector of open collections.
     * These inputs are used to build the open/dropped members which are then
     * evaluated inside isLogicallyDeleted to determine if a key belongs to a
     * dropped collection.
     * @param openCollections pointer (can be null) to a vector of
     *        OpenCollection objects used to build the open map.
     * @param droppedCollections a reference to a vector of DroppedCollection
     *        objects used to build the dropped set.
     */
    explicit ScanContext(
            const std::vector<Collections::KVStore::OpenCollection>*
                    openCollections,
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
    bool isLogicallyDeleted(CollectionID cid,
                            bool isDeleted,
                            uint64_t seqno) const;

    friend std::ostream& operator<<(std::ostream&, const ScanContext&);

    /**
     * All of the collections that are dropped
     */
    std::unordered_set<CollectionID> dropped;

    /**
     * All of the collections that are open, mapped to their start-seqno.
     */
    std::unordered_map<CollectionID, uint64_t> open;

    uint64_t startSeqno = std::numeric_limits<uint64_t>::max();
    uint64_t endSeqno = 0;
    bool canCheckOpenMap{false};
};

std::ostream& operator<<(std::ostream& os, const ScanContext& scanContext);

} // namespace VB
} // namespace Collections

#include <fmt/ostream.h>
#if FMT_VERSION >= 100000
template <>
struct fmt::formatter<Collections::VB::ScanContext> : ostream_formatter {};
#endif
