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

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * KVBucket::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
class PersistenceCallback {
public:
    virtual ~PersistenceCallback() = default;

    // This callback is invoked for set only.
    virtual void operator()(const Item&, KVStore::FlushStateMutation){};

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    virtual void operator()(const Item&, KVStore::FlushStateDeletion){};
};

class EPPersistenceCallback : public PersistenceCallback {
public:
    EPPersistenceCallback(EPStats& stats, VBucket& vb);

    // This callback is invoked for set only.
    void operator()(const Item&, KVStore::FlushStateMutation) override;

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    void operator()(const Item&, KVStore::FlushStateDeletion) override;

private:
    EPStats& stats;
    VBucket& vbucket;
};
