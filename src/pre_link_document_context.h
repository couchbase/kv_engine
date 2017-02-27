/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <cstdint>

class EventuallyPersistentEngine;
class Item;

/**
 * The PreLinkDocumetnContext is a helper class used when items are modified
 * by the frontend. It carries the context of the calling thread down to
 * where the CAS is generated, and fires the callback to the front-end
 * before the object is made available for other threads.
 *
 * The initial use of this is to allow the "CAS macro expansion" in
 * extended attributes.
 *
 * See the Document API in the server interface for more description
 * of the pre_Link callback.
 */
class PreLinkDocumentContext {
public:
    /**
     * Initialize the object
     *
     * @param engine_ The engine who owns the item
     * @param cookie_ The cookie representing the connection
     * @param item_ The document to operate on
     */
    PreLinkDocumentContext(EventuallyPersistentEngine& engine_,
                           const void* cookie_,
                           Item* item_)
        : engine(engine_), cookie(cookie_), item(item_) {
    }

    /**
     * Set the CAS value
     *
     * This method should be called _before_ the item is made available
     * for other threads
     *
     * @param cas the new CAS value for the object
     */
    void setCas(uint64_t cas);

    PreLinkDocumentContext(const PreLinkDocumentContext&) = delete;

protected:
    EventuallyPersistentEngine& engine;
    const void* cookie;
    const Item* item;
};
