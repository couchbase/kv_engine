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

#include <cstdint>

class EventuallyPersistentEngine;
class Item;

/**
 * The PreLinkDocumetnContext is a helper class used when items are modified
 * by the frontend. It carries the context of the calling thread down to
 * where the CAS and seqno are generated, and fires the callback to the
 * front-end before the object is made available for other threads.
 *
 * The initial use of this is to allow the "(CAS & Seqno) macro expansion" in
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
     * Set the CAS and Seqno values
     *
     * This method should be called _before_ the item is made available
     * for other threads
     *
     * @param cas the new CAS value for the object
     * @param seqno the new seqno value for the object
     */
    void preLink(uint64_t cas, uint64_t seqno);

    PreLinkDocumentContext(const PreLinkDocumentContext&) = delete;

protected:
    EventuallyPersistentEngine& engine;
    const void* cookie;
    const Item* item;
};
