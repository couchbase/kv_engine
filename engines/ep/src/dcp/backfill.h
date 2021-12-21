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

#include <folly/Synchronized.h>

#include <chrono>
#include <memory>

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

    explicit DCPBackfill(Vbid vbid);

    /**
     * Get the id of the vbucket for which this object is created
     *
     * @return vbid
     */
    Vbid getVBucketId() const {
        return vbid;
    }

    /**
     * Implements DCPBackfillIface::run
     * Runs the backfill for the current state and will throw if called when
     * in State::Done.
     */
    backfill_status_t run() override;

    /**
     * Implements DCPBackfillIface::cancel
     * log a warning when called and state != State::Done
     */
    void cancel() override;

    // virtual methods to be implemented by concrete backfill classes

    /**
     * Create the backfills objects, open snapshots etc...
     * return success (move to scan)
     * return snooze (will retry create soon)
     * return failed (cannot proceed and task can be removed)
     */
    virtual backfill_status_t create() = 0;

    /**
     * The Scan part of the backfill, i.e. iterate over a seqno range.
     * return success (scan is finished and task can be removed)
     * return again (scan was paused and should be called again soon)
     * return failed (cannot proceed and task can be removed)
     */
    virtual backfill_status_t scan() = 0;

private:
    /// States of a backfill
    enum class State { Create = 0, Scan, Done };
    friend std::ostream& operator<<(std::ostream&, State);

    /**
     * Validate if currentState can be changed to newState and if so write to
     * currentState
     */
    static void transitionState(State& currentState, State newState);

    /// will default-initialise to 0, or Create
    folly::Synchronized<State> state{};

protected:

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
