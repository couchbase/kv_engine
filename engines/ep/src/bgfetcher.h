/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <list>
#include <set>
#include <string>

#include "vb_ready_queue.h"
#include "vbucket.h"

// Forward declarations.
class EPStats;
class KVBucket;
class GlobalTask;

/**
 * Dispatcher job responsible for batching data reads and push to
 * underlying storage
 */
class BgFetcher {
public:
    /**
     * Construct a BgFetcher
     *
     * @param s  The store
     * @param st reference to statistics
     */
    BgFetcher(KVBucket& s, EPStats& st);

    /**
     * Construct a BgFetcher
     *
     * Equivalent to above constructor except stats reference is obtained
     * from KVBucket's reference to EPEngine's epstats.
     *
     * @param s The store
     */
    BgFetcher(KVBucket& s);

    ~BgFetcher();

    void start();
    void stop();
    bool run(GlobalTask *task);
    void setTaskId(size_t newId) { taskId = newId; }

    /**
     * Add a Vbid to pendingVbs and notify the task if necessary
     */
    void addPendingVB(Vbid vbId);

    // Test hook called before we complete a bg fetch
    TestingHook<> preCompleteHook;

private:
    size_t doFetch(Vbid vbId, vb_bgfetch_queue_t& items);

    /// If the BGFetch task is currently snoozed (not scheduled to
    /// run), wake it up. Has no effect the if the task has already
    /// been woken.
    void wakeUpTaskIfSnoozed();

    KVBucket& store;
    size_t taskId;
    EPStats &stats;

    std::atomic<bool> pendingFetch;

    VBReadyQueue queue;
};
