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

#pragma once

#include "collections/flush.h"
#include "ep_types.h"

#include <memcached/storeddockey_fwd.h>
#include <memcached/systemevent.h>

#include <string>

class DiskDocKey;
class Item;
class KVStore;
class SystemEventMessage;

namespace Collections::VB {
class Manifest;
} // namespace Collections::VB

class SystemEventFactory {
public:
    /**
     * Make an Item representing a Collection. When this event is alive, it
     * represents the current beginning of the collection, when this event is
     * deleted it represents the end of the collection (dropped collection).
     *
     * Flushing of a collection is an update of this Item, moving the beginning.
     *
     * @param cid The ID of the collection
     * @param data The data which will be written to the value of the Item
     * @param seqno An OptionalSeqno - if defined the returned Item will have
     *        the seqno value set as its bySeqno.
     * @return Item with correct configuration for a system event
     */
    static std::unique_ptr<Item> makeCollectionEvent(CollectionID cid,
                                                     cb::const_byte_buffer data,
                                                     OptionalSeqno seqno);

    /**
     * Make an Item representing the modification of a collection.
     * @param cid The ID of the collection
     * @param data The data which will be written to the value of the Item
     * @param seqno An OptionalSeqno - if defined the returned Item will have
     *        the seqno value set as its bySeqno.
     * @return Item with correct configuration for a system event
     */
    static std::unique_ptr<Item> makeModifyCollectionEvent(
            CollectionID cid, cb::const_byte_buffer data, OptionalSeqno seqno);

    /**
     * Make an Item representing the Scope SystemEvent, the returned Item
     * will represent a "Create of SID" but can be marked deleted by the caller
     * to represent a "Drop of SID"
     *
     * @param sid The ID of the scope
     * @param data The data which will be written to the value of the Item
     * @param seqno An OptionalSeqno - if defined the returned Item will have
     *        the seqno value set as its bySeqno.
     * @return Item with correct configuration for a system event
     */
    static std::unique_ptr<Item> makeScopeEvent(ScopeID sid,
                                                cb::const_byte_buffer data,
                                                OptionalSeqno seqno);

    /**
     * Make a key for a Collection SystemEvent. This is the same key that an
     * Item of makeCollectionEvent would have.
     * @param cid The ID of the collection
     * @param type The type of collection event to encode create/drop or modify
     * @return StoredDocKey with a collection system event key
     */
    static StoredDocKey makeCollectionEventKey(CollectionID cid,
                                               SystemEvent type);

    /**
     * Make a key for Scope SystemEvent. This is the same key that an Item of
     * makeScopeEvent would use.
     * @param sid The ID of the scope
     * @return StoredDocKey with scope system event key
     */
    static StoredDocKey makeScopeEventKey(ScopeID sid);

    /**
     * make a pair of keys (start and end) for use in a OSO range scan. This
     * pair of keys will return the set of keys that are system events for the
     * given collection.
     * @param cid The ID of the collection
     * @return pair of keys denoting the range for the collection
     */
    static std::pair<DiskDocKey, DiskDocKey>
    makeCollectionEventKeyPairForRangeScan(CollectionID cid);

    /**
     * Given a key from makeCollectionEventKey/makeCollectionEvent, returns the
     * collection ID that was used in the key's construction.
     */
    static CollectionID getCollectionIDFromKey(const DocKeyView& key);

    /**
     * Given a key from makeScopeEvent returns the scope ID that was used in the
     * key's construction.
     */
    static ScopeID getScopeIDFromKey(const DocKeyView& key);

    /**
     * Given a key from makeCollectionEventKey, makeCollectionEvent or
     * makeScopeEvent retrieve the system event type which is embedded in the
     * key. A second buffer is returned that is the key data after the type.
     */
    static std::pair<SystemEvent, cb::const_byte_buffer> getSystemEventType(
            const DocKeyView& key);

    /**
     * Given a key from makeCollectionEventKey, makeCollectionEvent or
     * makeScopeEvent retrieve the system event type which is embedded in the
     * key and the ID (as a u32) which is embedded in the key. Called can
     * switch on the event to determine if the ID is Scope or Collection
     */
    static std::pair<SystemEvent, uint32_t> getTypeAndID(const DocKeyView& key);

    /**
     * Given a key which the caller knows isSystemCollection return an optional
     * which stores the ID of the modified collection iff the key represent a
     * ModifyCollection event.
     */
    static std::optional<CollectionID> isModifyCollection(
            const DocKeyView& key);

private:
    /**
     * Make an Item representing the SystemEvent
     * @param se The SystemEvent being created. The returned Item will have this
     *           value stored in the flags field.
     * @param keyExtra Every SystemEvent has defined key, keyExtra is appended
     *        to the defined key
     * @param data The data which will be written to the value of the Item
     * @param seqno An OptionalSeqno - if defined the returned Item will have
     *        the seqno value set as its bySeqno.
     */
    static std::unique_ptr<Item> make(const DocKeyView& key,
                                      SystemEvent se,
                                      cb::const_byte_buffer data,
                                      OptionalSeqno seqno);
};
