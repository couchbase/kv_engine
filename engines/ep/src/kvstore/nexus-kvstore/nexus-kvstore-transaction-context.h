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

#include "kvstore/kvstore.h"
#include "kvstore/kvstore_transaction_context.h"
#include "kvstore/persistence_callback.h"

struct NexusKVStoreTransactionContext : public TransactionContext {
    NexusKVStoreTransactionContext(KVStoreIface& kvstore, Vbid vbid)
        : TransactionContext(kvstore, vbid, {}) {
    }

    std::unique_ptr<TransactionContext> primaryContext;
    std::unique_ptr<TransactionContext> secondaryContext;

    // Caching the results of the PersistenceCallback of the primary so that
    // when we run the flush against the secondary we can check that the
    // key was persisted and the state returned for it. These maps are
    // referenced by the NexusKVStorePersistenceCallbacks which use them
    // directly. We're storing the DiskDocKey rather than and Item because:
    // a) The Item passed in the PersistenceCallback API is a reference and
    //    will be freed before we run persistence for the secondary
    // b) The Item shouldn't change
    // c) The DiskDocKey is unique when it comes to prepares etc.
    std::unordered_map<DiskDocKey, FlushStateMutation> primarySets;
    std::unordered_map<DiskDocKey, FlushStateDeletion> primaryDeletions;
};
