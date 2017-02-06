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
#include "kvstore.h"

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
        // 1) An update to the persisted collection manifest.
        // 2) Trigger DCP to tell clients the collection is being deleted.
        key = Collections::DeleteEventKey + keyExtra;
        break;
    }
    case SystemEvent::DeleteCollectionHard: {
        // DeleteCollectionHard SystemEvent results in:
        // 1. An update to the persisted collection manifest removing an entry.
        // 2. A deletion of the SystemEvent::CreateCollection document.
        // Note: uses CreateEventKey because we are deleting the create item
        key = Collections::CreateEventKey + keyExtra;
    }
    case SystemEvent::DeleteCollectionSoft: {
        // DeleteCollectionHard SystemEvent results in:
        // 1. An update to the persisted collection manifest (updating the end
        // seqno).
        // 2. A deletion of the SystemEvent::CreateCollection document.
        // Note: uses CreateEventKey because we are deleting the create item
        key = Collections::CreateEventKey + keyExtra;
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

SystemEventFlushStatus SystemEventFlush::process(const queued_item& item) {
    if (item->getOperation() != queue_op::system_event) {
        return SystemEventFlushStatus::Continue;
    }

    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::CreateCollection:
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft: {
        // CreateCollection updates the manifest and writes a SystemEvent
        // DeleteCollection* both update the manifest and write a SystemEvent
        saveCollectionsManifestItem(item);
        return SystemEventFlushStatus::Continue;
    }
    case SystemEvent::BeginDeleteCollection: {
        // This will update the manifest but should not write an Item
        saveCollectionsManifestItem(item);
        return SystemEventFlushStatus::Skip;
    }
    }

    throw std::invalid_argument("SystemEventFlush::process unknown event " +
                                std::to_string(item->getFlags()));
}

bool SystemEventFlush::isUpsert(const Item& item) {
    if (item.getOperation() == queue_op::system_event) {
        // CreateCollection and DeleteCollection* are the only valid events.
        // The ::process function should of skipped BeginDeleteCollection.
        switch (SystemEvent(item.getFlags())) {
        case SystemEvent::CreateCollection: {
            return true;
        }
        case SystemEvent::BeginDeleteCollection: {
            throw std::logic_error("SystemEventFlush::isUpsert event " +
                                   to_string(SystemEvent(item.getFlags())) +
                                   " should neither delete or upsert ");
        }
        case SystemEvent::DeleteCollectionHard:
        case SystemEvent::DeleteCollectionSoft: {
            return false;
        }
        }
    } else {
        return !item.isDeleted();
    }
    throw std::invalid_argument("SystemEventFlush::isUpsert unknown event " +
                                std::to_string(item.getFlags()));
}

const Item* SystemEventFlush::getCollectionsManifestItem() const {
    return collectionManifestItem.get();
}

void SystemEventFlush::saveCollectionsManifestItem(const queued_item& item) {
    // For a given checkpoint only the highest system event should be the
    // one which writes the manifest
    if ((collectionManifestItem &&
         item->getBySeqno() > collectionManifestItem->getBySeqno()) ||
        !collectionManifestItem) {
        collectionManifestItem = item;
    }
}