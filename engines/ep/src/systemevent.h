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

#pragma once

#include "atomic.h"
#include "collections/flush.h"
#include "ep_types.h"
#include "storeddockey_fwd.h"

#include <string>

class Item;
class KVStore;
class SystemEventMessage;

namespace Collections {
namespace VB {
class Manifest;
} // namespace VB
} // namespace Collections

/// underlying size of uint32_t as this is to be stored in the Item flags field.
enum class SystemEvent : uint32_t {
    /**
     * The Collection system event represents the beginning or end of a
     * collection. Each Collection system event has a key which contains the
     * collection ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * collection became accessible unless that queued/stored item is deleted,
     * then it represent when that collection became inaccesible (logically
     * deleted).
     *
     * A Collection system event when queued into a checkpoint carries with it
     * a value, the value is used to maintain a per vbucket JSON collection's
     * manifest (for persisted buckets).
     */
    Collection,

    /**
     * The Scope system event represents the beginning or end of a
     * scope. Each Scope system event has a key which contains the
     * Scope ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * scope became accessible unless that queued/stored item is deleted,
     * then it represent when that scope became inaccessible
     *
     */
    Scope
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::Collection:
        return "Collection";
    case SystemEvent::Scope:
        return "Scope";
    }
    throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                std::to_string(int(se)));
}

class SystemEventFactory {
public:
    /**
     * Make an Item representing the Collection SystemEvent, the returned Item
     * will represent a "Create of CID" but can be marked deleted by the caller
     * to represent a "Drop of CID"
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
     * Item of makeCollectionEVent would have.
     * @param cid The ID of the collection
     * @return StoredDocKey with a collection system event key
     */
    static StoredDocKey makeCollectionEventKey(CollectionID cid);

    /**
     * Given a key from makeCollectionEventKey/makeCollectionEvent, returns the
     * collection ID that was used in the key's construction.
     */
    static CollectionID getCollectionIDFromKey(const DocKey& key);

    /**
     * Given a key from makeScopeEvent returns the scope ID that was used in the
     * key's construction.
     */
    static ScopeID getScopeIDFromKey(const DocKey& key);

    /**
     * Given a key from makeCollectionEventKey, makeCollectionEvent or
     * makeScopeEvent retrieve the system event type which is embedded in the
     * key. A second buffer is returned that is the key data after the type.
     */
    static std::pair<SystemEvent, cb::const_byte_buffer> getSystemEventType(
            const DocKey& key);

    /**
     * Given a key from makeCollectionEventKey, makeCollectionEvent or
     * makeScopeEvent retrieve the system event type which is embedded in the
     * key and the ID (as a u32) which is embedded in the key. Called can
     * switch on the event to determine if the ID is Scope or Collection
     */
    static std::pair<SystemEvent, uint32_t> getTypeAndID(const DocKey& key);

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
    static std::unique_ptr<Item> make(const DocKey& key,
                                      SystemEvent se,
                                      cb::const_byte_buffer data,
                                      OptionalSeqno seqno);
};
