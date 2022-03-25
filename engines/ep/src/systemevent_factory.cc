/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "systemevent_factory.h"

#include "collections/collections_types.h"
#include "collections/vbucket_manifest.h"
#include "dcp/response.h"
#include "diskdockey.h"
#include "item.h"

#include <mcbp/protocol/unsigned_leb128.h>

#include <memory>

std::unique_ptr<Item> SystemEventFactory::make(const DocKey& key,
                                               SystemEvent se,
                                               cb::const_byte_buffer data,
                                               OptionalSeqno seqno) {
    auto item = std::make_unique<Item>(key,
                                       uint32_t(se) /*flags*/,
                                       0 /*exptime*/,
                                       data.data(),
                                       data.size());

    if (seqno) {
        item->setBySeqno(seqno.value());
    }

    return item;
}

std::unique_ptr<Item> SystemEventFactory::makeCollectionEvent(
        CollectionID cid, cb::const_byte_buffer data, OptionalSeqno seqno) {
    return make(
            makeCollectionEventKey(cid), SystemEvent::Collection, data, seqno);
}

std::unique_ptr<Item> SystemEventFactory::makeScopeEvent(
        ScopeID sid, cb::const_byte_buffer data, OptionalSeqno seqno) {
    // Make a key which is:
    // [0x01] [0x01] [0xsid] _scope
    StoredDocKey key1{std::string_view{Collections::ScopeEventDebugTag},
                      CollectionID(ScopeIDType(sid))};
    StoredDocKey key2{key1, CollectionID{uint32_t(SystemEvent::Scope)}};
    return make(StoredDocKey(key2, CollectionID::System),
                SystemEvent::Scope,
                data,
                seqno);
}

StoredDocKey SystemEventFactory::makeCollectionEventKey(CollectionID cid) {
    // Make a key which is:
    // [0x01] [0x00] [0xcid] _collection
    StoredDocKey key1{std::string_view{Collections::CollectionEventDebugTag},
                      cid};
    StoredDocKey key2{key1, CollectionID{uint32_t(SystemEvent::Collection)}};
    return StoredDocKey(key2, CollectionID::System);
}

std::pair<DiskDocKey, DiskDocKey>
SystemEventFactory::makeCollectionEventKeyPairForRangeScan(CollectionID cid) {
    auto start = makeCollectionEventKey(cid);
    std::string end(start.keyData(), start.size());
    // Append 255 so that end now encompasses the required range
    end += std::numeric_limits<uint8_t>::max();
    return {DiskDocKey{start}, DiskDocKey{end.data(), end.size()}};
}

CollectionID SystemEventFactory::getCollectionIDFromKey(const DocKey& key) {
    // Input key is made up of a sequence of prefixes as per makeCollectionEvent
    // or makeScopeEvent
    // This function skips (1), checks (2) and returns 3
    auto se = getSystemEventType(key);
    Expects(se.first == SystemEvent::Collection); // expected Collection
    return cb::mcbp::unsigned_leb128<CollectionIDType>::decode(se.second).first;
}

ScopeID SystemEventFactory::getScopeIDFromKey(const DocKey& key) {
    // logic same as getCollectionIDFromKey, but the event type is expected to
    // be a scope event.
    auto se = getSystemEventType(key);
    Expects(se.first == SystemEvent::Scope);
    return cb::mcbp::unsigned_leb128<ScopeIDType>::decode(se.second).first;
}

std::pair<SystemEvent, cb::const_byte_buffer>
SystemEventFactory::getSystemEventType(const DocKey& key) {
    // Input key is made up of a sequence of prefixes.
    // (1) System (system namespace of 0x01)
    // (2) SystemEvent type (scope 0x1 or collection 0x0)
    // (3) ScopeID or CollectionID
    // This function skips (1) and returns (2)
    auto event = cb::mcbp::skip_unsigned_leb128<CollectionIDType>(
            {key.data(), key.size()});
    auto type = cb::mcbp::unsigned_leb128<uint32_t>::decode(event);
    return {SystemEvent(type.first), type.second};
}

std::pair<SystemEvent, uint32_t> SystemEventFactory::getTypeAndID(
        const DocKey& key) {
    // Input key is made up of a sequence of prefixes.
    // (1) System (system namespace of 0x01)
    // (2) SystemEvent type (scope 0x1 or collection 0x0)
    // (3) ScopeID or CollectionID
    // This function skips (1) and returns (2) and (3)
    auto [type, buffer] = getSystemEventType(key);
    return {type, cb::mcbp::unsigned_leb128<uint32_t>::decode(buffer).first};
}

std::unique_ptr<SystemEventProducerMessage> SystemEventProducerMessage::make(
        uint32_t opaque, const queued_item& item, cb::mcbp::DcpStreamId sid) {
    // Always ensure decompressed as we are about to use the value
    item->decompressValue();
    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection: {
        if (!item->isDeleted()) {
            // Note: constructor is private and make_unique is a pain to make
            // friend
            auto data = Collections::VB::Manifest::getCreateEventData(
                    {item->getData(), item->getNBytes()});
            if (data.metaData.maxTtl) {
                return std::make_unique<
                        CollectionCreateWithMaxTtlProducerMessage>(
                        opaque, item, data, sid);
            } else {
                return std::make_unique<CollectionCreateProducerMessage>(

                        opaque, item, data, sid);
            }
        } else {
            // Note: constructor is private and make_unique is a pain to make
            // friend
            return std::make_unique<CollectionDropProducerMessage>(
                    opaque,
                    item,
                    Collections::VB::Manifest::getDropEventData(
                            {item->getData(), item->getNBytes()}),
                    sid);
        }
    }
    case SystemEvent::Scope: {
        if (!item->isDeleted()) {
            return std::make_unique<ScopeCreateProducerMessage>(
                    opaque,
                    item,
                    Collections::VB::Manifest::getCreateScopeEventData(
                            {item->getData(), item->getNBytes()}),
                    sid);
        } else {
            return std::make_unique<ScopeDropProducerMessage>(
                    opaque,
                    item,
                    Collections::VB::Manifest::getDropScopeEventData(
                            {item->getData(), item->getNBytes()}),
                    sid);
        }
    }
    }

    throw std::logic_error("SystemEventProducerMessage::make not valid for " +
                           std::to_string(item->getFlags()));
}
