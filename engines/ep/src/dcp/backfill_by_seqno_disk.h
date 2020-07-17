/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
class KVStore;

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
     * Handles the completion of the backfill.
     * Destroys the scan context, indicates the completion to the stream.
     *
     * @param cancelled indicates the if backfill finished fully or was
     *                  cancelled in between; for debug
     */
    void complete(bool cancelled) override;

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
            KVStore& kvStore,
            const Collections::VB::Filter& filter) const;
};
