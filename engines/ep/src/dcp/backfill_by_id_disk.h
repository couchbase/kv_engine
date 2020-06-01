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

#include "dcp/backfill.h"
#include "dcp/backfill_disk.h"

class ByIdScanContext;
class KVBucket;

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress - this provides for execution of a ById (key to key)
 * backfill
 */
class DCPBackfillByIdDisk : public DCPBackfillDisk {
public:
    DCPBackfillByIdDisk(KVBucket& bucket,
                        std::shared_ptr<ActiveStream> s,
                        CollectionID cid);

protected:
    /**
     * Creates a scan context with the KV Store to read items that would match
     * the collection, cid.
     */
    backfill_status_t create() override;

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot for collection cid.
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

    /// collection to scan for
    CollectionID cid;
};
