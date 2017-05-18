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
    case SystemEvent::CreateCollection:
    case SystemEvent::DeleteCollectionSoft:
    case SystemEvent::DeleteCollectionHard: {
        // These all operate on the same key
        key += collectionsSeparator + Collections::CreateEventKey +
               collectionsSeparator + keyExtra;
        break;
    }
    case SystemEvent::BeginDeleteCollection: {
        key += collectionsSeparator + Collections::DeleteEventKey + keyExtra;
        break;
    }
    case SystemEvent::CollectionsSeparatorChanged: {
        key += collectionsSeparator + Collections::SeparatorChangedKey;
        break;
    }
    }
    return key;
}

mcbp::systemevent::id SystemEventFactory::mapToMcbp(SystemEvent se) {
    switch (se) {
    case SystemEvent::CreateCollection:
        return mcbp::systemevent::id::CreateCollection;
    case SystemEvent::BeginDeleteCollection:
        return mcbp::systemevent::id::DeleteCollection;
    case SystemEvent::CollectionsSeparatorChanged:
        return mcbp::systemevent::id::CollectionsSeparatorChanged;
    case SystemEvent::DeleteCollectionSoft:
    case SystemEvent::DeleteCollectionHard:
        break;
    }

    throw std::invalid_argument("SystemEventFactory::mapToMcbp unknown input se:" + std::to_string(int(se)));
}

ProcessStatus SystemEventFlush::process(const queued_item& item) {
    if (item->getOperation() != queue_op::system_event) {
        return ProcessStatus::Continue;
    }

    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::CreateCollection:
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft:
    case SystemEvent::CollectionsSeparatorChanged: {
        saveCollectionsManifestItem(item); // Updates manifest
        return ProcessStatus::Continue; // And flushes an item
    }
    case SystemEvent::BeginDeleteCollection: {
        saveCollectionsManifestItem(item); // Updates manifest
        return ProcessStatus::Skip; // But skips flushing the item
    }
    }

    throw std::invalid_argument("SystemEventFlush::process unknown event " +
                                std::to_string(item->getFlags()));
}

bool SystemEventFlush::isUpsert(const Item& item) {
    if (item.getOperation() == queue_op::system_event) {
        // CreateCollection, CollectionsSeparatorChanged and DeleteCollection*
        // are the only valid events.(returning true/false)
        // The ::process function should of skipped BeginDeleteCollection and
        // thus is an error if calling this method with such an event.
        switch (SystemEvent(item.getFlags())) {
        case SystemEvent::CreateCollection:
        case SystemEvent::CollectionsSeparatorChanged: {
            return true;
        }
        case SystemEvent::BeginDeleteCollection: {
            throw std::invalid_argument(
                    "SystemEventFlush::isUpsert event " +
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

ProcessStatus SystemEventReplicate::process(const Item& item) {
    if (item.shouldReplicate()) {
        if (item.getOperation() != queue_op::system_event) {
            // Not a system event, so no further filtering
            return ProcessStatus::Continue;
        } else {
            switch (SystemEvent(item.getFlags())) {
            case SystemEvent::CreateCollection:
            case SystemEvent::BeginDeleteCollection:
            case SystemEvent::CollectionsSeparatorChanged:
                // Create, BeginDelete and change separator all replicate
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
    std::pair<cb::const_char_buffer, cb::const_byte_buffer> dcpData;
    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::CreateCollection:
    case SystemEvent::BeginDeleteCollection: {
        dcpData = Collections::VB::Manifest::getSystemEventData(
                {item->getData(), item->getNBytes()});
        break;
    }
    case SystemEvent::CollectionsSeparatorChanged: {
        dcpData = Collections::VB::Manifest::getSystemEventSeparatorData(
                {item->getData(), item->getNBytes()});
        break;
    }
    case SystemEvent::DeleteCollectionHard:
    case SystemEvent::DeleteCollectionSoft:
        throw std::logic_error(
                "SystemEventProducerMessage::make not valid for " +
                std::to_string(item->getFlags()));
    }

    // Note: constructor is private and make_unique is a pain to add as a friend
    return std::unique_ptr<SystemEventProducerMessage>{
            new SystemEventProducerMessage(
                    opaque, item, dcpData.first, dcpData.second)};
}
