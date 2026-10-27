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

#include "collections/scan_context.h"
#include "systemevent_factory.h"

#pragma once

namespace Collections::VB {

/**
 * The EraserContext subclasses ScanContext and provides extra methods for
 * tracking when collections are completely erased.
 */
class EraserContext : public ScanContext {
public:
    /**
     * @param droppedCollections the collections to erase
     * @param maxEraseSeqno the greatest of the stored erase seqnos (0 if none)
     *        when the snapshot to be compacted was read
     */
    EraserContext(const std::vector<Collections::KVStore::DroppedCollection>&
                          droppedCollections,
                  uint64_t maxEraseSeqno);

    /**
     * Called by kvstore for deleted keys, when the deleted key is a drop of a
     * collection, the set of dropped collections is updated.
     * @param key The key of the deleted value (will returns if not system)
     * @param se The flags...SystemEvent of the value
     */
    void processSystemEvent(const DocKeyView& key, SystemEvent se);

    /**
     * @return true if the on-disk collection meta-data should be updated
     */
    bool needToUpdateCollectionsMetadata() const;

    /**
     * @return true if data associated with dropped collections exists on disk
     */
    bool doesOnDiskDroppedDataExist() const {
        return onDiskDroppedDataExists;
    }

    /**
     * Eraser (compaction) may not always remove *all* dropped data, compaction
     * sets the status with this method.
     */
    void setOnDiskDroppedDataExists(bool value) {
        onDiskDroppedDataExists = value;
    }

    /**
     * Compaction purges all dropped collection data at or below this seqno, so
     * on success the stored erase seqnos at or below it are removed. Seqnos
     * stored by a flush during the compaction are greater and remain.
     * @return the greatest erase seqno read with the snapshot (0 if none)
     */
    uint64_t getMaxEraseSeqno() const {
        return maxEraseSeqno;
    }

private:
    friend std::ostream& operator<<(std::ostream&, const EraserContext&);

    /// set to true if a 'drop' system event was seen
    bool seenEndOfCollection = false;

    /// set to true when 'erase' completes and dropped collection data exists
    bool onDiskDroppedDataExists = false;

    /// The greatest erase seqno read with the snapshot to be compacted
    const uint64_t maxEraseSeqno;
};

std::ostream& operator<<(std::ostream&, const EraserContext&);

} // namespace Collections::VB
