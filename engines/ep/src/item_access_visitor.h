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
#include "mutation_log_writer.h"
#include "stats.h"
#include "vb_visitors.h"

#include <memcached/storeddockey_fwd.h>
#include <platform/semaphore_guard.h>

class EPStats;
class KVBucket;
class Configuration;
class StoredValue;
class VBucketFilter;

class ItemAccessVisitor : public CappedDurationVBucketVisitor,
                          public HashTableVisitor {
public:
    ItemAccessVisitor(
            KVBucket& _store,
            Configuration& conf,
            EPStats& _stats,
            uint16_t sh,
            cb::SemaphoreGuard<> guard,
            uint64_t items_to_scan,
            std::function<void(std::string_view)> fileWriteTestHook = [](auto) {
            });
    ~ItemAccessVisitor() override;
    bool visit(const HashTable::HashBucketLock& lh, StoredValue& v) override;
    void visitBucket(VBucket& vb) override;
    void complete() override;

protected:
    void update(Vbid vbid);

private:
    VBucketFilter vBucketFilter;

    EPStats& stats;
    rel_time_t startTime;
    cb::time::steady_clock::time_point taskStart;
    std::string prev;
    std::string next;
    std::string name;
    uint16_t shardID;

    std::vector<StoredDocKey> accessed;

    std::unique_ptr<MutationLogWriter> log;
    /**
     * The parent AccessScanner is tracking how many visitors exist, this
     * guard will update the parent when the visitor destructs.
     */
    cb::SemaphoreGuard<> semaphoreGuard;

    // The number items scanned since last pause
    uint64_t items_scanned = 0;
    // The number of items to scan before we pause
    const uint64_t items_to_scan;
    // Write to disk, while persisting mutation log failed?
    bool writeFailed = false;
};