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

#include "kv_magma_common/magma-kvstore_magma_db_stats.h"

/**
 * Magma specific update function for the rollback purge seqno. See also
 * CompactionContext which holds this context object.
 *
 * Magma tracks the rollback purge seqno in MagmaDbStats, as such, any update to
 * the value stored in the CompactionContext that is used to update the in
 * memory purge seqno at the end of compaction also needs to update the
 * MagmaDbStats value which is the authoritative on disk version of the stat.
 */
class MagmaRollbackPurgeSeqnoCtx : public RollbackPurgeSeqnoCtx {
public:
    MagmaRollbackPurgeSeqnoCtx(uint64_t purgeSeqno, MagmaDbStats& dbStats)
        : RollbackPurgeSeqnoCtx(purgeSeqno), dbStats(dbStats) {
    }

    void updateRollbackPurgeSeqno(uint64_t seqno) override {
        RollbackPurgeSeqnoCtx::updateRollbackPurgeSeqno(seqno);

        if (dbStats.purgeSeqno < seqno) {
            dbStats.purgeSeqno = seqno;
        }
    }

protected:
    MagmaDbStats& dbStats;
};