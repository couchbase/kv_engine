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

#include "kvstore.h"

class EPStats;
class VBucket;
struct EPTransactionContext;

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * KVBucket::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback {
public:
    PersistenceCallback();

    ~PersistenceCallback();

    // This callback is invoked for set only.
    void operator()(EPTransactionContext&,
                    queued_item,
                    KVStore::FlushStateMutation);

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void operator()(EPTransactionContext&,
                    queued_item,
                    KVStore::FlushStateDeletion);
};

struct EPTransactionContext : public TransactionContext {
    EPTransactionContext(EPStats& stats, VBucket& vbucket);

    void setCallback(const queued_item&, KVStore::FlushStateMutation) override;

    void deleteCallback(const queued_item&,
                        KVStore::FlushStateDeletion) override;

    EPStats& stats;
    VBucket& vbucket;

protected:
    PersistenceCallback cb;
};
