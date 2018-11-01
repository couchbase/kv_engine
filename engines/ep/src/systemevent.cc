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

std::unique_ptr<Item> SystemEventFactory::make(SystemEvent se,
                                               const std::string& keyExtra,
                                               cb::const_byte_buffer data,
                                               OptionalSeqno seqno) {
    auto item = std::make_unique<Item>(
            StoredDocKey(makeKey(se, keyExtra), CollectionID::System),
            uint32_t(se) /*flags*/,
            0 /*exptime*/,
            data.data(),
            data.size());

    if (seqno) {
        item->setBySeqno(seqno.value());
    }

    return item;
}

// Build a key using the separator so we can split it if needed
std::string SystemEventFactory::makeKey(SystemEvent se,
                                        const std::string& keyExtra) {
    std::string key;
    switch (se) {
    case SystemEvent::Collection:
        // _collection:<collection-id>
        key = Collections::CollectionEventPrefixWithSeparator + keyExtra;
        break;
    case SystemEvent::Scope:
        // _scope:<scope-id>
        key = Collections::ScopeEventPrefixWithSeparator + keyExtra;
        break;
    }

    return key;
}

//
// Locate the 'keyExtra' data by locating our separator character and returning
// a byte_buffer of the data
//
const cb::const_byte_buffer::iterator SystemEventFactory::findKeyExtra(
        const DocKey& key, const std::string& separator) {
    if (key.size() == 0 || separator.size() == 0 ||
        separator.size() > key.size()) {
        return nullptr;
    }

    auto rv = std::search(key.data(),
                          key.data() + key.size(),
                          separator.begin(),
                          separator.end());
    if (rv != (key.data() + key.size())) {
        return rv + separator.size();
    }
    return nullptr;
}

// Reverse what makeKey did so we get 'keyExtra' back
cb::const_byte_buffer SystemEventFactory::getKeyExtra(const DocKey& key) {
    const uint8_t* collection = findKeyExtra(key, Collections::SystemSeparator);
    if (collection) {
        return {collection,
                gsl::narrow<uint8_t>(key.size() - (collection - key.data()))};
    } else {
        throw std::invalid_argument("DocKey::make incorrect namespace:" +
                                    std::to_string(int(key.getCollectionID())));
    }
}

ProcessStatus SystemEventFlush::process(const queued_item& item) {
    if (item->getOperation() != queue_op::system_event) {
        return ProcessStatus::Continue;
    }

    // All system-events we encounter update the manifest
    collectionsFlush.processManifestChange(item);

    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection:
    case SystemEvent::Scope:
        return ProcessStatus::Continue; // And flushes an item
    }

    throw std::invalid_argument("SystemEventFlush::process unknown event " +
                                std::to_string(item->getFlags()));
}

ProcessStatus SystemEventReplicate::process(const Item& item) {
    if (item.shouldReplicate()) {
        if (item.getOperation() != queue_op::system_event) {
            // Not a system event, so no further filtering
            return ProcessStatus::Continue;
        } else {
            switch (SystemEvent(item.getFlags())) {
            case SystemEvent::Collection:
            case SystemEvent::Scope:
                return ProcessStatus::Continue;
            }
        }
    }
    return ProcessStatus::Skip;
}

std::unique_ptr<SystemEventProducerMessage> SystemEventProducerMessage::make(
        uint32_t opaque, const queued_item& item) {
    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection:
    case SystemEvent::Scope: {
        if (!item->isDeleted()) {
            // Note: constructor is private and make_unique is a pain to make
            // friend
            auto data = Collections::VB::Manifest::getCreateEventData(
                    {item->getData(), item->getNBytes()});
            if (data.maxTtl) {
                return std::unique_ptr<
                        CollectionsCreateWithMaxTtlProducerMessage>{
                        new CollectionsCreateWithMaxTtlProducerMessage(
                                opaque, item, data)};
            } else {
                return std::unique_ptr<CollectionsCreateProducerMessage>{
                        new CollectionsCreateProducerMessage(
                                opaque, item, data)};
            }
        } else {
            // Note: constructor is private and make_unique is a pain to make
            // friend
            return std::unique_ptr<CollectionsDropProducerMessage>{
                    new CollectionsDropProducerMessage(
                            opaque,
                            item,
                            Collections::VB::Manifest::getDropEventData(
                                    {item->getData(), item->getNBytes()}))};
        }
    }
    }

    throw std::logic_error("SystemEventProducerMessage::make not valid for " +
                           std::to_string(item->getFlags()));
}
