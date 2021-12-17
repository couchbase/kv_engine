/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/backfill_by_seqno.h"
#include "dcp/backfill_disk.h"
#include <optional>

namespace Collections::VB {
class Filter;
}
class BySeqnoScanContext;
class KVBucket;
class KVStoreIface;

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress.
 * This class calls asynchronous kvstore apis and manages a state machine to
 * read items in the sequential order from the disk and to call the DCP stream
 * for disk snapshot, backfill items and backfill completion.
 */
class DCPBackfillBySeqnoDisk : public DCPBackfillDisk,
                               public DCPBackfillBySeqno {
public:
    DCPBackfillBySeqnoDisk(KVBucket& bucket,
                           std::shared_ptr<ActiveStream> stream,
                           uint64_t startSeqno,
                           uint64_t endSeqno);

    // explicitly state how we want run to be called as it technically exists
    // from both parent classes
    backfill_status_t run() override {
        return DCPBackfillDisk::run();
    }

    // explicitly state how we want cancel to be called as it technically exists
    // from both parent classes
    void cancel() override {
        DCPBackfillDisk::cancel();
    }

private:
    /**
     * Creates a scan context with the KV Store to read items in the sequential
     * order from the disk. Backfill snapshot range is decided here.
     */
    backfill_status_t create() override;

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot range created in the create scan context. This is an
     * asynchronous operation, KVStore calls the CacheCallback and DiskCallback
     * to populate the items read in the snapshot of scan.
     */
    backfill_status_t scan() override;

    /**
     * Indicates the completion to the stream.
     */
    void complete() override;

    /**
     * Method to get hold of the highest high seqno of collections that are in
     * a streams filter.
     * @param seqnoScanCtx needed to get handle to KV data file
     * @param kvStore to get the high collection seqnos from
     * @param filter of the stream, which contains the collections we need to
     *        check against.
     * @return Returns the highest seqno of the collection in filter. No seqno
     *         value is returned if the filter is a pass-through filter or
     *         seqnoScanCtx is null.
     */
    std::pair<bool, std::optional<uint64_t>> getHighSeqnoOfCollections(
            const BySeqnoScanContext& seqnoScanCtx,
            const KVStoreIface& kvStore,
            const Collections::VB::Filter& filter) const;

    /**
     * Private method for deciding how to call ActiveStream::markDiskSnapshot
     */
    bool markDiskSnapshot(ActiveStream& stream,
                          BySeqnoScanContext& scanCtx,
                          const KVStoreIface& kvs);

    /**
     * Private method for deciding how to call ActiveStream::markDiskSnapshot
     * for a legacy stream
     */
    bool markLegacyDiskSnapshot(ActiveStream& stream,
                                BySeqnoScanContext& scanCtx,
                                const KVStoreIface& kvs);
};
