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

#include "libmagma/magma.h"

#include <folly/Synchronized.h>
#include <memcached/dockey.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/monotonic.h>

/**
 * MagmaDbStats are a set of stats maintained within MagmaKVStore rather than
 * the vBucket state. They are required because magma::CompactKVStore() and
 * magma's implicit compactions do not provide us with any mechanism to update
 * local docs during the call in an atomic manner. To ensure that stats are
 * updated atomically with the result of the CompactKVStore call any stats that
 * are updated during it should be a part of MagmaDbStats. Magma is responsible
 * for Merging these changes atomically along with the post compaction file(s).
 */
class MagmaDbStats : public magma::UserStats {
public:
    explicit MagmaDbStats() = default;

    MagmaDbStats(int64_t docCount, uint64_t purgeSeqno, int64_t highSeqno) {
        reset(docCount, purgeSeqno, highSeqno, {});
    }

    MagmaDbStats(const MagmaDbStats& other) = default;

    MagmaDbStats& operator=(const MagmaDbStats& other) {
        docCount = other.docCount;
        purgeSeqno = other.purgeSeqno;
        highSeqno = other.highSeqno;
        droppedCollectionCounts = other.droppedCollectionCounts;
        return *this;
    }

    void reset(const MagmaDbStats& other) {
        reset(other.docCount,
              other.purgeSeqno,
              other.highSeqno,
              other.droppedCollectionCounts);
    }

    /**
     * Merge the stats with existing stats.
     * For docCount and onDiskPrepares, we add the delta.
     * For highSeqno and purgeSeqno, we set them if the new
     * value is higher than the old value.
     *
     * @param other should be a MagmaDbStats instance
     */
    void Merge(const magma::UserStats& other) override;

    /**
     * clone the stats
     *
     * @return MagmaDbStats
     */
    std::unique_ptr<magma::UserStats> Clone() override;

    /**
     * Marshal the stats into a json string
     *
     * @return string MagmaDbStats in a json string format
     * @throws logic_error if unable to parse json
     */
    std::string Marshal() override;

    /**
     * Unmarshal the json string into DbStats
     *
     * @return Status potential error code; for magma wrapper we throw
     *                errors rather than return status.
     */
    magma::Status Unmarshal(const std::string& encoded) override;

    void reset(
            int64_t newDocCount,
            uint64_t newPurgeSeqno,
            int64_t newHighSeqno,
            std::unordered_map<uint32_t, int64_t> newDroppedCollectionStats) {
        docCount = newDocCount;
        purgeSeqno.reset(newPurgeSeqno);
        highSeqno.reset(newHighSeqno);
        droppedCollectionCounts = newDroppedCollectionStats;
    }

    /**
     * We don't want to keep around (either in memory or on disk)
     * droppedCollectionStats entries for which the itemCount is 0 (implying
     * that they have been purged). This helper functions erases those entries.
     */
    void sanitizeDroppedCollections();

    int64_t docCount{0};
    Monotonic<uint64_t> purgeSeqno{0};

    // Store highSeqno in UserStats
    // Magma rollback callback iterates from rollback seqno to highSeqno. If the
    // tombstone/prepare/collection has been purged, the docs being rolled back
    // will be missing from the rollback callback. We store highSeqno in
    // UserStats and retrieve it from from Magma's oldest checkpoint. This
    // oldest rollbackable highSeqno will be used to prevent purge of docs that
    // can be rolled back.
    Monotonic<int64_t> highSeqno{0};

    /**
     * Map of collection id to item count. We use the underlying
     * CollectionIDType here rather than the CollectionID type specifically to
     * simplify the to_json and from_json conversions.
     *
     * Magma can't track item counts during compaction as it may keep around
     * stale items. To correct item counts after a collection is dropped we add
     * an entry to droppedCollectionStats when we drop a collection that is kept
     * until we erase it (via explicit compaction). When compaction runs we can
     * decrement the vBucket count by that item count.
     */
    using DroppedCollectionsCountsMap =
            std::unordered_map<CollectionIDType, int64_t>;
    DroppedCollectionsCountsMap droppedCollectionCounts;
};

void to_json(nlohmann::json& json, const MagmaDbStats& stats);
void from_json(const nlohmann::json& j, MagmaDbStats& stats);
