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

    MagmaDbStats(int64_t docCount, uint64_t purgeSeqno) {
        reset(docCount, purgeSeqno);
    }

    MagmaDbStats(const MagmaDbStats& other) = default;

    MagmaDbStats& operator=(const MagmaDbStats& other) {
        docCount = other.docCount;
        purgeSeqno = other.purgeSeqno;
        return *this;
    }

    void reset(const MagmaDbStats& other) {
        reset(other.docCount, other.purgeSeqno);
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

    void reset(int64_t newDocCount, uint64_t newPurgeSeqno) {
        docCount = newDocCount;
        purgeSeqno.reset(newPurgeSeqno);
    }

    int64_t docCount{0};
    Monotonic<uint64_t> purgeSeqno{0};
};

void to_json(nlohmann::json& json, const MagmaDbStats& stats);
void from_json(const nlohmann::json& j, MagmaDbStats& stats);
