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
#include "collections/events_generated.h"
#include "collections/vbucket_manifest.h"
#include "dcp/response.h"
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
    return make(makeCollectionEventKey(cid, SystemEvent::Collection),
                SystemEvent::Collection,
                data,
                seqno);
}

std::unique_ptr<Item> SystemEventFactory::makeModifyCollectionEvent(
        CollectionID cid, cb::const_byte_buffer data, OptionalSeqno seqno) {
    return make(makeCollectionEventKey(cid, SystemEvent::ModifyCollection),
                SystemEvent::ModifyCollection,
                data,
                seqno);
}

std::unique_ptr<Item> SystemEventFactory::makeScopeEvent(
        ScopeID sid, cb::const_byte_buffer data, OptionalSeqno seqno) {
    return make(makeScopeEventKey(sid), SystemEvent::Scope, data, seqno);
}

StoredDocKey SystemEventFactory::makeScopeEventKey(ScopeID sid) {
    // Make a key which is:
    // [0x01] [0x01] [0xsid] _scope
    StoredDocKey key1{Collections::ScopeEventDebugTag,
                      CollectionID(ScopeIDType(sid))};
    StoredDocKey key2{key1, CollectionID{uint32_t(SystemEvent::Scope)}};
    return StoredDocKey(key2, CollectionID::System);
}

StoredDocKey SystemEventFactory::makeCollectionEventKey(CollectionID cid,
                                                        SystemEvent type) {
    Expects(type != SystemEvent::Scope);
    // Make a key which is:
    // [0x01] [type] [0xcid] _collection
    // i.e.
    // [System Collection] [SystemEvent type] [cid] _collection
    std::string key;
    auto lebType = cb::mcbp::unsigned_leb128<uint32_t>(uint32_t(type));
    key.append(lebType.begin(), lebType.end());
    auto lebCid = cb::mcbp::unsigned_leb128<uint32_t>(uint32_t(cid));
    key.append(lebCid.begin(), lebCid.end());
    key.append(Collections::CollectionEventDebugTag);
    return StoredDocKey(key, CollectionID::System);
}

CollectionID SystemEventFactory::getCollectionIDFromKey(const DocKey& key) {
    // Input key is made up of a sequence of prefixes as per makeCollectionEvent
    // or makeModifyCollectionEvent
    // This function skips (1), checks (2) and returns 3
    auto se = getSystemEventType(key);
    // expected Collection/ModifyCollection event
    Expects(se.first == SystemEvent::Collection ||
            se.first == SystemEvent::ModifyCollection);
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

std::optional<CollectionID> SystemEventFactory::isModifyCollection(
        const DocKey& key) {
    auto [event, id] = SystemEventFactory::getTypeAndID(key);
    if (event != SystemEvent::ModifyCollection) {
        return {};
    }
    return CollectionID(id);
}

std::unique_ptr<SystemEventProducerMessage> SystemEventProducerMessage::make(
        uint32_t opaque, queued_item& item, cb::mcbp::DcpStreamId sid) {
    // Always ensure decompressed as we are about to use the value
    item->decompressValue();
    switch (SystemEvent(item->getFlags())) {
    case SystemEvent::Collection:
    // Modify stores/sends the same data as create
    case SystemEvent::ModifyCollection: {
        if (!item->isDeleted()) {
            // Note: constructor is private and make_unique is a pain to make
            // friend
            auto data = Collections::VB::Manifest::getCreateEventData(*item);
            if (data.metaData.maxTtl) {
                return std::make_unique<
                        CollectionCreateWithMaxTtlProducerMessage>(
                        opaque, item, data, sid);
            } else {
                return std::make_unique<CollectionCreateProducerMessage>(
                        opaque, item, data, sid);
            }
        } else {
            // Only create can be marked isDeleted
            Expects(SystemEvent(item->getFlags()) == SystemEvent::Collection);
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

std::unique_ptr<SystemEventFlatBuffers>
SystemEventProducerMessage::makeWithFlatBuffersValue(
        uint32_t opaque, queued_item& item, cb::mcbp::DcpStreamId sid) {
    std::string_view name;

    const auto event = SystemEvent(item->getFlags());
    switch (event) {
    case SystemEvent::Collection:
    case SystemEvent::ModifyCollection:
        if (!item->isDeleted()) {
            item->decompressValue();
            // Need the name for the outbound message
            const auto& fb =
                    Collections::VB::Manifest::getCollectionFlatbuffer(*item);
            name = fb.name()->string_view();
        } else {
            // Only create can be marked isDeleted
            Expects(event == SystemEvent::Collection);
        }
        // else drop collection doesn't use the collection name
        break;
    case SystemEvent::Scope:
        if (!item->isDeleted()) {
            item->decompressValue();

            // Need the name for the outbound message
            const auto* fb = Collections::VB::Manifest::getScopeFlatbuffer(
                    item->getValueView());
            name = fb->name()->string_view();
        }
        // else drop scope doesn't use the scope name
        break;
    default:
        throw std::logic_error(
                "SystemEventProducerMessage::makeWithFlatBuffersValue not"
                " valid for event:" +
                std::to_string(item->getFlags()));
    }

    return std::make_unique<SystemEventFlatBuffers>(opaque, name, item, sid);
}