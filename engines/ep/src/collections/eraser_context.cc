/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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
#include "collections/collections_dockey.h"
#include "collections/collections_types.h"
#include "item.h"
#include "systemevent.h"
#include "vbucket.h"

#include <iostream>

namespace Collections {
namespace VB {

bool EraserContext::manageSeparator(const ::DocKey& key) {
    if (ScanContext::manageSeparator(key)) {
        // If the last key is valid, save it in the list of keys which will
        // be deleted at the end of erasing process.
        if (!lastSeparatorChangeKey.empty()) {
            staleSeparatorChangeKeys.push_back(lastSeparatorChangeKey);
        }

        // Now save this new key
        lastSeparatorChangeKey = key;
        return true;
    }
    return false;
}

void EraserContext::processKeys(VBucket& vb) {
    for (auto& key : staleSeparatorChangeKeys) {
        auto item = SystemEventFactory::make(
                key, SystemEvent::CollectionsSeparatorChanged);
        item->setDeleted();
        vb.queueItem(item.release(), {});
    }
}

} // end VB
} // end Collections