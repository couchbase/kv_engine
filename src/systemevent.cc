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

#include "systemevent.h"
#include "collections/collections_types.h"

std::unique_ptr<Item> SystemEventFactory::make(SystemEvent se,
                                               const std::string& keyExtra,
                                               size_t itemSize) {
    std::string key;
    switch (se) {
    case SystemEvent::CreateCollection: {
        // CreateCollection SystemEvent results in:
        // 1) A special marker document representing the creation.
        // 2) An update to the persisted collection manifest.
        key = Collections::CreateEventKey + keyExtra;
        break;
    }
    case SystemEvent::BeginDeleteCollection: {
        // BeginDeleteCollection SystemEvent results in:
        // 1) The deletion of the special marker document representing the
        //    creation.
        // 2) An update to the persisted collection manifest.
        // Note: uses CreateEventKey because we are deleting it.
        key = Collections::CreateEventKey + keyExtra;
        break;
    }
    case SystemEvent::DeleteCollectionHard: {
        // DeleteCollectionHard SystemEvent results in:
        // An update to the persisted collection manifest removing an entry.
        // No document is persisted.
        key = Collections::DeleteEventKey + keyExtra;
    }
    case SystemEvent::DeleteCollectionSoft: {
        // DeleteCollectionHard SystemEvent results in:
        // An update to the persisted collection manifest (updating the end
        // seqno).
        // No document is persisted.
        key = Collections::DeleteEventKey + keyExtra;
    }
    }

    auto item = std::make_unique<Item>(DocKey(key, DocNamespace::System),
                                       uint32_t(se) /*flags*/,
                                       0 /*exptime*/,
                                       nullptr, /*no data to copy-in*/
                                       itemSize);

    item->setOperation(queue_op::system_event);

    return item;
}
