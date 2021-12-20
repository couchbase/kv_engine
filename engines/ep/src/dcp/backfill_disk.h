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

#include "callbacks.h"

#include <memory>

class ActiveStream;
class KVBucket;
class ScanContext;
class VBucket;

/* Callback to get the items that are found to be in the cache */
class CacheCallback : public StatusCallback<CacheLookup> {
public:
    CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s);
    /**
     * Function called for each key during stream backfill. Informs the caller
     * if the information required to populate the DCP stream for the item is
     * in-memory.
     *
     * If all required information is in-memory, the item is backfilled and
     * status is set to cb::engine_errc::key_already_exists.
     *
     * If there is additional information required that isn't in-memory, or the
     * item should not be included in the stream for some reason, the status is
     * set to cb::engine_errc::success.
     *
     * The status is set to cb::engine_errc::no_memory only if there is not
     * enough memory to backfill, pausing backfilling temporarily.
     *
     * @param lookup a reference to a CacheLookup
     */
    void callback(CacheLookup& lookup) override;

private:
    /**
     * Attempt to perform the get of lookup
     *
     * @return return a GetValue by performing a vb::get with lookup::getKey.
     */
    GetValue get(VBucket& vb, CacheLookup& lookup, ActiveStream& stream);

    KVBucket& bucket;
    std::weak_ptr<ActiveStream> streamPtr;
};

/* Callback to get the items that are found to be in the disk */
class DiskCallback : public StatusCallback<GetValue> {
public:
    explicit DiskCallback(std::shared_ptr<ActiveStream> s);

    void callback(GetValue& val) override;

    /**
     * The on disk "High Completed Seqno", used in the callback method to decide
     * if we need to send a prepare we find on disk or not (prepare seqnos <= to
     * persistedCompletedSeqno do not need to be sent over a DCP stream).
     */
    uint64_t persistedCompletedSeqno;

private:
    std::weak_ptr<ActiveStream> streamPtr;
};

class DCPBackfillDisk {
public:
    explicit DCPBackfillDisk(KVBucket& bucket);

protected:
    KVBucket& bucket;

    std::unique_ptr<ScanContext> scanCtx;
};
