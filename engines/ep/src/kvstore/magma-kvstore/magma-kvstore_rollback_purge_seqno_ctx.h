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

/**
 * Magma implicit compaction specific update function for the rollback purge
 * seqno. See also MagmaRollbackPurgeSeqnoCtx and CompactionContext.
 *
 * Explicit compactions updated the in memory purge seqno at the end of a
 * compaction. This purge seqno is used to turn away DcpConsumers that have
 * fallen too far behind before we open the file to scan. Implicit compaction
 * is driven entirely by magma though and does not have a corresponding
 * completion callback. Instead, implicit compaction moves the in memory
 * vBucket purge seqno whenever we move the rollbackPurgeSeqno. This class
 * encapsulates the logic to perform the in memory update whenever we update the
 * rollbackPurgeSeqno.
 */
class MagmaImplicitCompactionPurgedItemContext
    : public MagmaRollbackPurgeSeqnoCtx {
public:
    MagmaImplicitCompactionPurgedItemContext(
            uint64_t purgeSeqno,
            MagmaDbStats& dbStats,
            std::function<void(uint64_t)>& maybeUpdateVBucketPurgeSeqno)
        : MagmaRollbackPurgeSeqnoCtx(purgeSeqno, dbStats),
          maybeUpdateVBucketPurgeSeqno(maybeUpdateVBucketPurgeSeqno) {
    }

    void updateRollbackPurgeSeqno(uint64_t seqno) override {
        MagmaRollbackPurgeSeqnoCtx::updateRollbackPurgeSeqno(seqno);

        maybeUpdateVBucketPurgeSeqno(seqno);
    }

protected:
    std::function<void(uint64_t)>& maybeUpdateVBucketPurgeSeqno;
};
