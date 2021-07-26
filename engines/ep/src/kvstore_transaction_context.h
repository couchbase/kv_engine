/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore.h"
#include "persistence_callback.h"

/**
 * State associated with a KVStore transaction (begin() / commit() pair).
 * Users would typically subclass this, and provide an instance to begin().
 * The KVStore will then provide a pointer to it during every persistence
 * callback.
 */
struct TransactionContext {
    TransactionContext(KVStore& kvstore,
                       Vbid vbid,
                       std::unique_ptr<PersistenceCallback> cb)
        : kvstore(kvstore), vbid(vbid), cb(std::move(cb)) {
    }
    virtual ~TransactionContext() {
        kvstore.endTransaction(vbid);
    }

    /**
     * Callback for sets. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void setCallback(const Item& i, FlushStateMutation m) {
        (*cb)(i, m);
    }

    /**
     * Callback for deletes. Invoked after persisting an item. Does nothing by
     * default as a subclass should provide functionality but we want to allow
     * simple tests to run without doing so.
     */
    virtual void deleteCallback(const Item& i, FlushStateDeletion d) {
        (*cb)(i, d);
    }

    KVStore& kvstore;
    const Vbid vbid;
    std::unique_ptr<PersistenceCallback> cb;
};
