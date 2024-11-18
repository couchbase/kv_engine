/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "hash_table.h"
#include "kv_bucket.h"
#include "mutation_log.h"
#include "stats.h"
#include "vb_visitors.h"

#include <platform/semaphore_guard.h>

class EPStats;
class KVBucket;
class Configuration;
class StoredValue;
class VBucketFilter;

class ItemAccessVisitor : public CappedDurationVBucketVisitor,
                          public HashTableVisitor {
public:
    ItemAccessVisitor(KVBucket& _store,
                      Configuration& conf,
                      EPStats& _stats,
                      uint16_t sh,
                      std::atomic<bool>& sfin,
                      AccessScanner& aS,
                      uint64_t items_to_scan,
                      std::unique_ptr<mlog::FileIface> fileIface =
                              std::make_unique<mlog::DefaultFileIface>());
    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;
    void visitBucket(VBucket& vb) override;
    void complete() override;

protected:
    void update(Vbid vbid);

private:
    /**
     * Finalizer method called at the end of completing a visit.
     * @param created_log: Did we successfully create a MutationLog object on
     * this run?
     */
    void updateStateFinalizer(bool created_log) {
        if (++(as.completedCount) == store.getVBuckets().getNumShards()) {
            bool inverse = false;
            stateFinalizer.compare_exchange_strong(inverse, true);
        }
        if (created_log) {
            // Successfully created an access log - increment stat.
            // Done after the new file created
            // to aid in testing - once the stat has the new value the
            // access.log file can be safely checked.
            ++stats.alogRuns;
        }
    }

    VBucketFilter vBucketFilter;

    KVBucket& store;
    EPStats& stats;
    rel_time_t startTime;
    std::chrono::steady_clock::time_point taskStart;
    std::string prev;
    std::string next;
    std::string name;
    uint16_t shardID;

    std::vector<StoredDocKey> accessed;

    std::unique_ptr<MutationLog> log;
    std::atomic<bool>& stateFinalizer;
    AccessScanner& as;

    // The number items scanned since last pause
    uint64_t items_scanned;
    // The number of items to scan before we pause
    const uint64_t items_to_scan;
    // Write to disk, while persisting mutation log failed?
    bool writeFailed = false;
};