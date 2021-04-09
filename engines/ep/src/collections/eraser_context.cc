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

#include "collections/eraser_context.h"

#include <flatbuffers/flatbuffers.h>

namespace Collections::VB {

EraserContext::EraserContext(
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections)
    : ScanContext(droppedCollections) {
}

void EraserContext::processSystemEvent(const DocKey& key, SystemEvent se) {
    if (se == SystemEvent::Collection) {
        // The system event we are processing could be:
        // 1) A deleted collection (so represents the end of dropped collection)
        // 2) A create event which came after a drop (collection resurrection)
        //    In this case we still need to look in the drop list so we know
        //    that the old generation of the collection was fully visited.
        // If the ID is in the drop list, then we remove it and set the
        // 'removed' flag.
        auto itr = dropped.find(getCollectionIDFromKey(key));
        if (itr != dropped.end()) {
            seenEndOfCollection = true;
        }
    }
}

bool EraserContext::needToUpdateCollectionsMetadata() const {
    return seenEndOfCollection;
}

std::ostream& operator<<(std::ostream& os, const EraserContext& eraserContext) {
    os << "EraserContext: seenEndOfCollection:"
       << (eraserContext.seenEndOfCollection ? "true, " : "false, ");
    os << static_cast<const ScanContext&>(eraserContext);
    return os;
}

} // namespace Collections::VB