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

#include "diskdockey.h"
#include "item.h"
#include "kvstore/persistence_callback.h"
#include "nexus-kvstore.h"

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <memcached/vbucket.h>
#include <utilities/logtags.h>

#include <memory>

/**
 * PersistenceCallback for the primary KVstore in NexusKVStore. Stores the
 * parameters of the calls made by the primary KVStore for later comparison
 * with the secondary KVStore before calling the "original" PersistenceCallback.
 */
class NexusKVStorePrimaryPersistenceCallback : public PersistenceCallback {
public:
    NexusKVStorePrimaryPersistenceCallback(
            std::unique_ptr<PersistenceCallback> pcb,
            std::unordered_map<DiskDocKey, FlushStateMutation>& sets,
            std::unordered_map<DiskDocKey, FlushStateDeletion>& deletions)
        : pcb(std::move(pcb)), sets(sets), deletions(deletions) {
    }

    void operator()(const Item& item, FlushStateMutation m) override {
        auto diskKey = DiskDocKey(item);
        auto [itr, inserted] = sets.emplace(diskKey, m);
        // Only expect to flush each item once
        Expects(inserted);

        auto deletionsItr = deletions.find(diskKey);
        Expects(deletionsItr == deletions.end());

        (*pcb)(item, m);
    }

    void operator()(const Item& item, FlushStateDeletion d) override {
        auto diskKey = DiskDocKey(item);
        auto [itr, inserted] = deletions.emplace(diskKey, d);
        Expects(inserted);

        auto setsItr = sets.find(diskKey);
        Expects(setsItr == sets.end());

        (*pcb)(item, d);
    }

    // "Original" PersistenceCallback to forward on to after we store the result
    // for later comparison
    std::unique_ptr<PersistenceCallback> pcb;

    // References to the maps in the NexusKVStoreTransactionContext so that we
    // can compare the results of the primary against the secondary
    std::unordered_map<DiskDocKey, FlushStateMutation>& sets;
    std::unordered_map<DiskDocKey, FlushStateDeletion>& deletions;
};

/**
 * PersistenceCallback for the secondary KVStore in NexusKVStore. Compares the
 * parameters of the calls made by the secondary KVStore to the results
 * previously supplies by the primary KVStore.
 */
class NexusKVStoreSecondaryPersistenceCallback : public PersistenceCallback {
public:
    NexusKVStoreSecondaryPersistenceCallback(
            const NexusKVStore& kvstore,
            const std::unordered_map<DiskDocKey, FlushStateMutation>& sets,
            const std::unordered_map<DiskDocKey, FlushStateDeletion>& deletions)
        : kvstore(kvstore), sets(sets), deletions(deletions) {
    }

    void operator()(const Item& item, FlushStateMutation m) override {
        // Check for the item
        auto diskKey = DiskDocKey(item);
        auto itr = sets.find(diskKey);
        if (itr == sets.end()) {
            auto msg = fmt::format(
                    "NexusKVStoreSecondaryPersistenceCallback::set: {}: didn't "
                    "call primary set callback for key:{}",
                    item.getVBucketId(),
                    cb::UserData(diskKey.to_string()));
            kvstore.handleError(msg);
        }

        if (kvstore.getPurgeSeqno(item.getVBucketId()) != 0) {
            // All bets are off now as we have purged something. This means that
            // a flush against one KVStore may be an Insert and against another
            // it may be an Update. We could technically work out if a
            // comparison was valid or not but we'd need the original item (that
            // would be getting replaced in one of the KVStores) to do that.
            return;
        }

        // And check the state
        if (itr->second != m) {
            if (itr->second == FlushStateMutation::LogicalInsert ||
                m == FlushStateMutation::LogicalInsert) {
                // Magma may use the LogicalDelete state to deal with item
                // counting whilst couchstore does not, skip this check
                return;
            }
            auto msg = fmt::format(
                    "NexusKVStoreSecondaryPersistenceCallback::set: {}: "
                    "different "
                    "state for key:{} primary:{} secondary:{}",
                    item.getVBucketId(),
                    cb::UserData(diskKey.to_string()),
                    itr->second,
                    m);
            kvstore.handleError(msg);
        }
    }

    void operator()(const Item& item, FlushStateDeletion d) override {
        // Check for the item
        auto diskKey = DiskDocKey(item);
        auto itr = deletions.find(diskKey);
        if (itr == deletions.end()) {
            auto msg = fmt::format(
                    "NexusKVStoreSecondaryPersistenceCallback::delete: {}: "
                    "didn't call primary deletion callback for key:{}",
                    item.getVBucketId(),
                    cb::UserData(diskKey.to_string()));
            kvstore.handleError(msg);
        }

        if (kvstore.getPurgeSeqno(item.getVBucketId()) != 0) {
            // All bets are off now as we have purged something. This means that
            // a flush against one KVStore may be an Insert and against another
            // it may be an Update. We could technically work out if a
            // comparison was valid or not but we'd need the original item (that
            // would be getting replaced in one of the KVStores) to do that.
            return;
        }

        // And check the state
        if (itr->second != d) {
            if (itr->second == FlushStateDeletion::LogicallyDocNotFound ||
                d == FlushStateDeletion::LogicallyDocNotFound) {
                // Magma may use the LogicalDelete state to deal with item
                // counting whilst couchstore does not, skip this check
                return;
            }
            auto msg = fmt::format(
                    "NexusKVStoreSecondaryPersistenceCallback::delete: {}: "
                    "different stats for key:{} primary:{} secondary:{}",
                    item.getVBucketId(),
                    cb::UserData(diskKey.to_string()),
                    itr->second,
                    d);
            kvstore.handleError(msg);
        }
    }

    // KVStore for error handling
    const NexusKVStore& kvstore;

    // References to the maps in the NexusKVStoreTransactionContext to compare
    // the results of the primary
    const std::unordered_map<DiskDocKey, FlushStateMutation>& sets;
    const std::unordered_map<DiskDocKey, FlushStateDeletion>& deletions;
};
