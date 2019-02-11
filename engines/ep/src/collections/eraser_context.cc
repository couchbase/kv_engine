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

namespace Collections {
namespace VB {

EraserContext::EraserContext(
        const std::vector<Collections::KVStore::DroppedCollection>&
                droppedCollections)
    : ScanContext(droppedCollections) {
}

void EraserContext::processEndOfCollection(const DocKey& key, SystemEvent se) {
    if (!key.getCollectionID().isSystem() || se != SystemEvent::Collection) {
        return;
    }

    remove(getCollectionIDFromKey(key));
}

bool EraserContext::needToUpdateCollectionsMetadata() const {
    return removed;
}

void EraserContext::remove(CollectionID id) {
    auto itr = dropped.find(id);
    // OK to not find the id. It could be a tombstone
    if (itr != dropped.end()) {
        dropped.erase(itr);
        removed = true;
    }
}

std::ostream& operator<<(std::ostream& os, const EraserContext& eraserContext) {
    os << "EraserContext: removed:"
       << (eraserContext.removed ? "true, " : "false, ");
    os << static_cast<const ScanContext&>(eraserContext);
    return os;
}

} // namespace VB
} // namespace Collections