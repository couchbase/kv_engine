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

#include <string>

#include "item.h"

/// underlying size of uint32_t as this is to be stored in the Item flags field.
enum class SystemEvent : uint32_t {
    /**
     * The CreateCollection system event is generated when a VBucket receives
     * knowledge of a new collection. The event's purpose is to carry data
     * to the flusher so we can persist a new collections JSON manifest that
     * includes the new collection and persist a special marker document
     * allowing DCP backfills to re-transite collection creation at the correct
     * point in seqno-time. This event will also be used to generate
     * DCP messages to inform consumers of the new collection (for in-memory
     * streaming).
     */
    CreateCollection,

    /**
     * The BeginDeleteCollection system event is generated when a VBucket
     * receives a manifest that removes a collection. The events's purpose is to
     * carry data to the flusher so we can persist a new collections JSON
     * manifest that indicates the collection is now in the process of being
     * removed. This is indicated by changing the end-seqno of a collection's
     * entry. This event also deletes the orginal create marker document from
     * the data store. This event will also be used to generate DCP messages to
     * inform consumers of the deleted collection (for in-memory streaming).
     */
    BeginDeleteCollection,

    /**
     * The DeleteCollectionHard system event is generated when a VBucket has
     * completed the deletion of all items of a collection. The hard delete
     * carries data to the flusher so we can persist a JSON manifest that now
     * fully removes the collection.
     */
    DeleteCollectionHard,

    /**
     * The DeleteCollectionSoft system event is generated when a VBucket has
     * completed the deletion of all items of a collection *but*  a
     * collection of the same name was added back during the deletion. The soft
     * delete carries data to the flusher so we can persist a JSON manifest that
     * only updates the end-seqno of the deleted collection entry.
     */
    DeleteCollectionSoft
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::CreateCollection:
        return "CreateCollection";
    case SystemEvent::BeginDeleteCollection:
        return "BeginDeleteCollection";
    case SystemEvent::DeleteCollectionHard:
        return "DeleteCollectionHard";
    case SystemEvent::DeleteCollectionSoft:
        return "DeleteCollectionSoft";
    }
    throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                std::to_string(int(se)));
}

class SystemEventFactory {
public:
    /**
     * Make an Item representing the SystemEvent
     * @param se The SystemEvent being created. The returned Item will have this
     *           value stored in the flags field.
     * @param keyExtra Every SystemEvent has defined key, keyExtra is appended
     *        to the defined key
     * @param itemSize The returned Item can be requested to allocate a value
     *        of itemSize. Some SystemEvents will update the value with data to
     *        be persisted/replicated.
     */
    static std::unique_ptr<Item> make(SystemEvent se,
                                      const std::string& keyExtra,
                                      size_t itemSize);
};
