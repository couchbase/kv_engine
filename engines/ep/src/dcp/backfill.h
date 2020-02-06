/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include <memcached/vbucket.h>

#include <memory>

class ActiveStream;
class ScanContext;

/**
 * Indicates the status of the backfill that is run
 */
enum backfill_status_t {
    backfill_success,
    backfill_finished,
    backfill_snooze
};

/**
 * This is the base class for creating backfill classes that perform specific
 * jobs (disk scan vs memory, scanning seqno index vs id index).
 *
 * This exposes common elements required by BackfillManager and all concrete
 * backfill classes.
 *
 */
class DCPBackfill {
public:
    DCPBackfill() = default;

    DCPBackfill(std::shared_ptr<ActiveStream> s);

    virtual ~DCPBackfill() {
    }

    /**
     * Run the DCP backfill and return the status of the run
     *
     * @return status of the current run
     */
    virtual backfill_status_t run() = 0;

    /**
     * Cancels the backfill
     */
    virtual void cancel() = 0;

    /**
     * Get the id of the vbucket for which this object is created
     *
     * @return vbid
     */
    Vbid getVBucketId() const {
        return vbid;
    }

    /**
     * Indicates if the DCP stream associated with the backfill is dead
     *
     * @return true if stream is in dead state; else false
     */
    bool isStreamDead() const;

protected:
    /**
     * Ptr to the associated Active DCP stream. Backfill can be run for only
     * an active DCP stream.
     * We use a weak_ptr instead of a shared_ptr to avoid cyclic reference.
     * DCPBackfill objs do not primarily own the stream objs, they only need
     * reference to a valid stream obj when backfills are run. Hence, they
     * should only hold a weak_ptr to the stream objs.
     */
    std::weak_ptr<ActiveStream> streamPtr;

    /**
     * Id of the vbucket on which the backfill is running
     */
    const Vbid vbid{0};
};

using UniqueDCPBackfillPtr = std::unique_ptr<DCPBackfill>;
