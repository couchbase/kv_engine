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

#include "dcp/backfill_disk.h"
#include "dcp/backfill_disk_to_stream.h"

class KVBucket;

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress - this provides for execution of a ById (key to key)
 * backfill
 */
class DCPBackfillByIdDisk : public DCPBackfillDiskToStream {
public:
    DCPBackfillByIdDisk(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

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

    backfill_status_t scanHistory() override;

    /**
     * Indicates the completion to the stream.
     */
    void complete(ActiveStream& stream);

    void complete(ActiveStream& stream, bool cancelled);
};
