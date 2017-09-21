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
#include "collections/vbucket_manifest.h"
#include "dcp/response.h"
#include "item.h"
#include "kvstore.h"

std::unique_ptr<Item> SystemEventFactory::make(
        SystemEvent se,
        const std::string& collectionsSeparator,
        const std::string& keyExtra,
        size_t itemSize,
        OptionalSeqno seqno) {
    std::string key = makeKey(se, collectionsSeparator, keyExtra);

    auto item = std::make_unique<Item>(DocKey(key, DocNamespace::System),
                                       uint32_t(se) /*flags*/,
                                       0 /*exptime*/,
                                       nullptr, /*no data to copy-in*/
                                       itemSize);

    if (seqno) {
        item->setBySeqno(seqno.value());
    }

    return item;
}

// Build a key using the separator so we can split it if needed
std::string SystemEventFactory::makeKey(SystemEvent se,
                                        const std::string& collectionsSeparator,
                                        const std::string& keyExtra) {
    std::string key = Collections::SystemEventPrefix;
    switch (se) {
    case SystemEvent::Collection:
        key += collectionsSeparator + keyExtra;
        break;
    case SystemEvent::DeleteCollectionSoft:
    case SystemEvent::DeleteCollectionHard: {
        key += collectionsSeparator + Collections::DeleteEventKey +
               collectionsSeparator + keyExtra;
        break;
    }
    case SystemEvent::CollectionsSeparatorChanged: {
        key += collectionsSeparator + Collections::SeparatorChangedKey;
        break;
    }
    }
    return key;
}

ProcessStatus SystemEventFlush::process(const queued_item& item) {
    if (item->getOperation() != queue_op::system_event) {
        return ProcessStatus::Continue;
    }

    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection:
    case SystemEvent::CollectionsSeparatorChanged: {
        saveCollectionsManifestItem(item); // Updates manifest
        return ProcessStatus::Continue; // And flushes an item
    }
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft: {
        saveCollectionsManifestItem(item); // Updates manifest
        return ProcessStatus::Skip; // But skips flushing the item
    }
    }

    throw std::invalid_argument("SystemEventFlush::process unknown event " +
                                std::to_string(item->getFlags()));
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

ProcessStatus SystemEventReplicate::process(const Item& item) {
    if (item.shouldReplicate()) {
        if (item.getOperation() != queue_op::system_event) {
            // Not a system event, so no further filtering
            return ProcessStatus::Continue;
        } else {
            switch (SystemEvent(item.getFlags())) {
            case SystemEvent::Collection:
            case SystemEvent::CollectionsSeparatorChanged:
                // Collection and change separator events both replicate
                return ProcessStatus::Continue;
            case SystemEvent::DeleteCollectionHard:
            case SystemEvent::DeleteCollectionSoft: {
                // Delete H/S do not replicate
                return ProcessStatus::Skip;
            }
            }
        }
    }
    return ProcessStatus::Skip;
}

std::unique_ptr<SystemEventProducerMessage> SystemEventProducerMessage::make(
        uint32_t opaque, queued_item& item) {
    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection: {
        auto dcpData = Collections::VB::Manifest::getSystemEventData(
                {item->getData(), item->getNBytes()});
        // Note: constructor is private and make_unique is a pain to make friend
        return std::unique_ptr<SystemEventProducerMessage>{
                new SystemEventProducerMessage(
                        opaque, item, dcpData.first, dcpData.second)};
    }
    case SystemEvent::CollectionsSeparatorChanged: {
        auto dcpData = Collections::VB::Manifest::getSystemEventSeparatorData(
                {item->getData(), item->getNBytes()});
        // Note: constructor is private and make_unique is a pain to make friend
        return std::unique_ptr<SystemEventProducerMessage>{
                new SystemEventProducerMessage(opaque, item, dcpData, {})};
    }
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft:
        break;
    }

    throw std::logic_error("SystemEventProducerMessage::make not valid for " +
                           std::to_string(item->getFlags()));
}
