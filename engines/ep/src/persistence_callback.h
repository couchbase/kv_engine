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

#include "callbacks.h"
#include "kvstore.h"
#include "vbucket.h"

class EPStats;

struct EPTransactionContext : public TransactionContext {
    EPTransactionContext(EPStats& stats, VBucket& vbucket)
        : TransactionContext(vbucket.getId()), stats(stats), vbucket(vbucket) {
    }

    EPStats& stats;
    VBucket& vbucket;
};

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * KVBucket::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback {
public:
    PersistenceCallback(const queued_item& qi,
                        uint64_t c);

    ~PersistenceCallback();

    // This callback is invoked for set only.
    void operator()(TransactionContext&,
                    KVStore::MutationSetResultState mutationResult);

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void operator()(TransactionContext&, KVStore::MutationStatus deleteStatus);

private:
    void redirty(EPStats& stats, VBucket& vbucket);

    const queued_item queuedItem;
    uint64_t cas;
};
