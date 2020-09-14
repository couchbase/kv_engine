/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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
    for (const auto cid : scanContext.dropped) {
        os << cid.to_string() << ", ";
    }
    os << "]";
    return os;
}

} // namespace Collections::VB
