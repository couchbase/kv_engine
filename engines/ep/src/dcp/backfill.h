/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/vbucket.h>

#include <chrono>
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
    virtual ~DCPBackfillIface() = default;

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
     * @returns true if the backfill task should be cancelled
     */
    virtual bool shouldCancel() const = 0;
};

/**
 * This is the base class for creating backfill classes that perform specific
 * jobs (disk scan vs memory, scanning seqno index vs id index).
 *
 * This exposes common elements required by BackfillManager and all concrete
 * backfill classes.
 *
 */
class DCPBackfill : public DCPBackfillIface {
public:
    DCPBackfill() = default;

    explicit DCPBackfill(std::shared_ptr<ActiveStream> s);

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
    bool shouldCancel() const override;

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

    /// Cumulative runtime of this backfill.
    std::chrono::steady_clock::duration runtime{0};
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
     * Decrement by one the number of running (active/initializing/snoozing)
     * backfills. Does not include pending backfills.
     */
    virtual void decrNumRunningBackfills() = 0;
};

using UniqueDCPBackfillPtr = std::unique_ptr<DCPBackfillIface>;
