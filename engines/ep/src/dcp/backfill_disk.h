/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "callbacks.h"
#include "dcp/backfill.h"

#include <mutex>

class EventuallyPersistentEngine;
class ScanContext;
class VBucket;

/* The possible states of the DCPBackfillDisk */
enum backfill_state_t {
    backfill_state_init,
    backfill_state_scanning,
    backfill_state_completing,
    backfill_state_done
};

/* Callback to get the items that are found to be in the cache */
class CacheCallback : public StatusCallback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine& e,
                  std::shared_ptr<ActiveStream> s);

    void callback(CacheLookup& lookup);

private:
    /**
     * Attempt to perform the get of lookup
     *
     * @return return a GetValue by performing a vb::get with lookup::getKey.
     */
    GetValue get(VBucket& vb, CacheLookup& lookup, ActiveStream& stream);

    EventuallyPersistentEngine& engine_;
    std::weak_ptr<ActiveStream> streamPtr;
};

/* Callback to get the items that are found to be in the disk */
class DiskCallback : public StatusCallback<GetValue> {
public:
    DiskCallback(std::shared_ptr<ActiveStream> s);

    void callback(GetValue& val);

private:
    std::weak_ptr<ActiveStream> streamPtr;
};

/**
 * Concrete class that does backfill from the disk and informs the DCP stream
 * of the backfill progress.
 * This class calls asynchronous kvstore apis and manages a state machine to
 * read items in the sequential order from the disk and to call the DCP stream
 * for disk snapshot, backfill items and backfill completion.
 */
class DCPBackfillDisk : public DCPBackfill {
public:
    DCPBackfillDisk(EventuallyPersistentEngine& e,
                    std::shared_ptr<ActiveStream> s,
                    uint64_t startSeqno,
                    uint64_t endSeqno);

    backfill_status_t run() override;

    void cancel() override;

private:
    /**
     * Creates a scan context with the KV Store to read items in the sequential
     * order from the disk. Backfill snapshot range is decided here.
     */
    backfill_status_t create();

    /**
     * Scan the disk (by calling KVStore apis) for the items in the backfill
     * snapshot range created in the create scan context. This is an
     * asynchronous operation, KVStore calls the CacheCallback and DiskCallback
     * to populate the items read in the snapshot of scan.
     */
    backfill_status_t scan();

    /**
     * Handles the completion of the backfill.
     * Destroys the scan context, indicates the completion to the stream.
     *
     * @param cancelled indicates the if backfill finished fully or was
     *                  cancelled in between; for debug
     */
    void complete(bool cancelled);

    /**
     * Makes transitions to the state machine to backfill from the disk
     * asynchronously and to inform the DCP stream of the backfill progress.
     */
    void transitionState(backfill_state_t newState);

    /**
     * Ptr to the ep-engine.
     * [TODO]: Check if ep-engine ptr is really needed or we can do with a
     *         ptr/ref to kvbucket/vbucket.
     */
    EventuallyPersistentEngine& engine;

    ScanContext* scanCtx;
    backfill_state_t state;
    std::mutex lock;
};
