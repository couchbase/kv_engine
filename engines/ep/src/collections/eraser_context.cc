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
        //    that the old generation of the collection was fully visited and
        //    can now be removed.
        // If the ID is in the drop list, then we remove it and set the
        // 'removed' flag.
        auto itr = dropped.find(getCollectionIDFromKey(key));
        if (itr != dropped.end()) {
            dropped.erase(itr);
            removed = true;
        }
    }
}

bool EraserContext::needToUpdateCollectionsMetadata() const {
    return removed;
}

std::ostream& operator<<(std::ostream& os, const EraserContext& eraserContext) {
    os << "EraserContext: removed:"
       << (eraserContext.removed ? "true, " : "false, ");
    os << static_cast<const ScanContext&>(eraserContext);
    return os;
}

} // namespace Collections::VB