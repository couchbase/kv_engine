/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/scan_context.h"

#include "collections/kvstore.h"

namespace Collections::VB {

ScanContext::ScanContext(
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections) {
    for (const auto& droppedCollection : droppedCollections) {
        dropped.insert(droppedCollection.collectionId);
        // Find the full extent of dropped collections, we will only map lookup
        // for keys inside the range.
        startSeqno =
                std::min<uint64_t>(startSeqno, droppedCollection.startSeqno);
        endSeqno = std::max<uint64_t>(endSeqno, droppedCollection.endSeqno);
    }
}

bool ScanContext::isLogicallyDeleted(const DocKey& key, uint64_t seqno) const {
    if (dropped.empty() || key.isInSystemCollection()) {
        return false;
    }

    // Is the key in a range which contains dropped collections and in the set?
    return (seqno >= startSeqno && seqno <= endSeqno) &&
           dropped.count(key.getCollectionID()) > 0;
}

std::ostream& operator<<(std::ostream& os, const ScanContext& scanContext) {
    os << "ScanContext: startSeqno:" << scanContext.startSeqno
       << ", endSeqno:" << scanContext.endSeqno;
    os << " dropped:[";
    for (CollectionID cid : scanContext.dropped) {
        os << cid.to_string() << ", ";
    }
    os << "]";
    return os;
}

} // namespace Collections::VB
