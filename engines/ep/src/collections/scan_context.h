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

#include <fmt/ostream.h>
#include <concepts>
#include <expected>
#include <limits>
#include <type_traits>
#include <unordered_map>

struct DocKeyView;

namespace Collections {

namespace KVStore {
struct OpenCollection;
struct DroppedCollection;
} // namespace KVStore

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
     * @param openMaxSeqno seqnos above this are not checked against the open
     *        map (only meaningful with openCollections)
     */
    explicit ScanContext(
            const std::vector<Collections::KVStore::OpenCollection>*
                    openCollections,
            const std::vector<Collections::KVStore::DroppedCollection>&
                    droppedCollections,
            uint64_t openMaxSeqno = std::numeric_limits<uint64_t>::max());

    /// Which collections metadata ScanContext::make uses
    enum class OpenCollections {
        /// Only when no dropped list but an erase seqno is stored
        Fallback,
        /// Always (with any dropped list), e.g. a scan of all versions
        Always
    };

    /**
     * The (bounded) dropped list is preferred for identifying logically
     * deleted keys. Without a list but with an erase seqno, the open
     * collections are used. With neither, nothing is logically deleted.
     *
     * @return true if the open collections must be used
     */
    static bool useOpenCollectionsForDroppedData(bool droppedListStored,
                                                 uint64_t eraseSeqno) {
        return !droppedListStored && eraseSeqno != 0;
    }

    /**
     * Create the ScanContext for a scan of stored data, reading only the
     * metadata which useOpenCollectionsForDroppedData selects. Each reader
     * returns a std::expected<T, E>, the first error is returned.
     *
     * @param readDropped reads the dropped list
     * @param readEraseSeqno reads the erase seqno (0 when not stored)
     * @param readOpen reads the open collections
     * @param openCollections when to use the open collections
     */
    template <std::invocable ReadDropped,
              std::invocable ReadEraseSeqno,
              std::invocable ReadOpen>
    [[nodiscard]] static auto make(
            ReadDropped readDropped,
            ReadEraseSeqno readEraseSeqno,
            ReadOpen readOpen,
            OpenCollections openCollections = OpenCollections::Fallback)
            -> std::expected<
                    ScanContext,
                    typename std::invoke_result_t<ReadDropped>::error_type> {
        using Error = typename std::invoke_result_t<ReadDropped>::error_type;
        static_assert(std::is_same_v<Error,
                                     typename std::invoke_result_t<
                                             ReadEraseSeqno>::error_type> &&
                              std::is_same_v<Error,
                                             typename std::invoke_result_t<
                                                     ReadOpen>::error_type>,
                      "ScanContext::make readers must share an error type");

        // 1) The dropped list is always read first
        auto dropped = readDropped();
        if (!dropped) {
            return std::unexpected(dropped.error());
        }
        if (openCollections == OpenCollections::Fallback) {
            // 2) A stored list defines the dropped data, nothing else is read
            if (!dropped->empty()) {
                return ScanContext{nullptr, *dropped};
            }
            // 3) No list, read the erase seqno
            auto eraseSeqno = readEraseSeqno();
            if (!eraseSeqno) {
                return std::unexpected(eraseSeqno.error());
            }
            // 4) No list and no erase seqno, nothing is dropped
            if (!useOpenCollectionsForDroppedData(!dropped->empty(),
                                                  *eraseSeqno)) {
                return ScanContext{nullptr, *dropped};
            }
            // 5) Fallback: no list but an erase seqno, the open collections
            //    define the dropped data
            auto open = readOpen();
            if (!open) {
                return std::unexpected(open.error());
            }
            // Only data at or below the erase seqno awaits a purge
            return ScanContext{&*open, *dropped, *eraseSeqno};
        }
        // OpenCollections::Always: the open collections are used with any
        // list (the erase seqno is not read)
        auto open = readOpen();
        if (!open) {
            return std::unexpected(open.error());
        }
        return ScanContext{&*open, *dropped};
    }

    bool operator==(const ScanContext& other) const {
        return droppedEndSeqnos == other.droppedEndSeqnos &&
               startSeqno == other.startSeqno && endSeqno == other.endSeqno &&
               canCheckOpenMap == other.canCheckOpenMap &&
               openMaxSeqno == other.openMaxSeqno && open == other.open;
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
    bool isLogicallyDeleted(const DocKeyView& key,
                            bool isDeleted,
                            uint64_t seqno) const;

    /// @return true if the collection@seqno is logically deleted
    bool isLogicallyDeleted(CollectionID cid, uint64_t seqno) const;

    /// @return true if the open collections identify logically deleted keys
    bool isUsingOpenCollections() const {
        return canCheckOpenMap;
    }

    /// @return the open collections (and start-seqno) if built from them
    const std::unordered_map<CollectionID, uint64_t>& getOpenCollections()
            const {
        return open;
    }

    /// @return if true dropped set is empty
    bool empty() const {
        return droppedEndSeqnos.empty();
    }

    /// @return the dropped collections, each mapped to its end-seqno
    const std::unordered_map<CollectionID, uint64_t>& getDroppedCollections()
            const {
        return droppedEndSeqnos;
    }

    /// @return the endSeqno - the maximum endSeqno of all dropped collections
    uint64_t getEndSeqno() const {
        return endSeqno;
    }

    /**
     * @return the end-seqno of the given dropped collection, or 0 if the
     *         collection is not dropped.
     */
    uint64_t getEndSeqno(CollectionID cid) const {
        auto itr = droppedEndSeqnos.find(cid);
        return itr == droppedEndSeqnos.end() ? 0 : itr->second;
    }

protected:
    /// @return true if cid@seqno is within the dropped set (and its range)
    bool isInDroppedSet(CollectionID cid, uint64_t seqno) const;

    friend std::ostream& operator<<(std::ostream&, const ScanContext&);

    /**
     * For each dropped collection the end-seqno of the drop (or flush). A
     * key@seqno of the collection is logically deleted when seqno < end-seqno.
     * The dropped list has one entry per collection, a flush of a listed
     * collection extends its end-seqno. A flushed collection is also alive, so
     * the range check must be per collection - one seqno window spanning every
     * dropped collection would wrongly cover the new generation of a flushed
     * collection (MB-41351).
     */
    std::unordered_map<CollectionID, uint64_t> droppedEndSeqnos;

    /**
     * All of the collections that are open, mapped to their start-seqno.
     */
    std::unordered_map<CollectionID, uint64_t> open;

    uint64_t startSeqno = std::numeric_limits<uint64_t>::max();
    uint64_t endSeqno = 0;
    bool canCheckOpenMap{false};

    /**
     * Seqnos above this are not logically deleted by the open map. Without a
     * dropped list, all data of dropped collections is at or below the erase
     * seqno, so a scan avoids a map lookup for every newer item.
     */
    uint64_t openMaxSeqno{std::numeric_limits<uint64_t>::max()};
};

std::ostream& operator<<(std::ostream& os, const ScanContext& scanContext);

} // namespace VB
} // namespace Collections

template <>
struct fmt::formatter<Collections::VB::ScanContext> : ostream_formatter {};
