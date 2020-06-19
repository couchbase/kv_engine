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
 * Interface for classes which perform DCP Backfills.
 */
struct DCPBackfillIface {
    virtual ~DCPBackfillIface() {
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
     * @returns true if the DCP stream associated with the backfill is dead,
     * else false.
     */
    virtual bool isStreamDead() const = 0;
};

class DCPBackfill : public DCPBackfillIface {
public:
    DCPBackfill(std::shared_ptr<ActiveStream> s,
                uint64_t startSeqno,
                uint64_t endSeqno);

    virtual ~DCPBackfill() {
    }

    Vbid getVBucketId() const {
        return vbid;
    }

    bool isStreamDead() const override;

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
     * Start seqno of the backfill
     */
    const uint64_t startSeqno;

    /**
     * End seqno of the backfill
     */
    uint64_t endSeqno;

    /**
     * Id of the vbucket on which the backfill is running
     */
    const Vbid vbid;
};

/**
 * Interface for classes which support tracking the total number of
 * Backfills across an entire Bucket.
 */
struct BackfillTrackingIface {
    virtual ~BackfillTrackingIface() = default;

    /**
     * Checks if one more backfill can be added to the active set. If so
     * then returns true, and notes that one more backfill is active.
     * If no more backfills can be added to the active set, returns false.
     */
    virtual bool canAddBackfillToActiveQ() = 0;

    /**
     * Decrement by one the number of active / snoozing backfills.
     */
    virtual void decrNumActiveSnoozingBackfills() = 0;
};

using UniqueDCPBackfillPtr = std::unique_ptr<DCPBackfillIface>;
