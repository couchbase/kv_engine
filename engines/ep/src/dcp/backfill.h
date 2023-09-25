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

std::ostream& operator<<(std::ostream&, backfill_status_t);

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

    /**
     * Get the u64 value which uniquely identifies this object
     */
    virtual uint64_t getUID() const = 0;
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

    /**
     * The ScanHistory part of the backfill, i.e. iterate over a seqno range
     * and return all versions of a key that are stored.
     * return success (scan is finished and task can be removed)
     * return again (scan was paused and should be called again soon)
     * return failed (cannot proceed and task can be removed)
     */
    virtual backfill_status_t scanHistory() = 0;

    /// States of a backfill
    enum class State { Create = 0, Scan, ScanHistory, Done };

    State getState() const {
        return *state.rlock();
    };

    uint64_t getUID() const override {
        return uid;
    }

protected:
    friend std::ostream& operator<<(std::ostream&, State);

    /**
     * Sub-classes must implement and use this to decide the next state. This
     * allows the sub-class to decide how to progress from
     * Create->Scan->ScanHistory
     */
    virtual State getNextScanState(DCPBackfill::State current) = 0;

    /**
     * Id of the vbucket on which the backfill is running
     */
    const Vbid vbid{0};

    const uint64_t uid{0};

    /// Cumulative runtime of this backfill.
    std::chrono::steady_clock::duration runtime{0};
    /// start time for each invocation of run
    std::chrono::steady_clock::time_point runStart;

private:
    /**
     * This helper function pushes execution to the correct scan phase based on
     * the state.
     */
    backfill_status_t scan(DCPBackfill::State state);

    /**
     * Validate if currentState can be changed to newState and if so return
     * newState
     */
    static State validateTransition(State currentState, State newState);

    /// will default-initialise to 0, or Create
    folly::Synchronized<State> state{};
};

/**
 * KVStoreScanTracker is a class used to count how many scans are in existence
 * and providing methods to know if a new scan can be created.
 */
class KVStoreScanTracker {
public:
    virtual ~KVStoreScanTracker() = default;

    /**
     * Check if a backfill can be created (which will create a ScanContext).
     * If true is returned the KVStoreScanTracker has incremented the tracking
     * to include a new scan and the caller must now proceed to create the scan.
     * If no more backfills can be created returns false.
     *
     * @param numInProgress count of Backfills already in progress by the
     *        calling connection.
     * @return true if a Backfill can be created
     */
    virtual bool canCreateBackfill(size_t numInProgress);

    /**
     * Check if a RangeScan can be created (which will create a ScanContext).
     * If true is returned the KVStoreScanTracker has incremented the tracking
     * to include/reserve a new scan and the caller must now proceed to create
     * the scan. If no more backfills can be created returns false.
     *
     * @return true if a RangeScan can be created
     */
    virtual bool canCreateAndReserveRangeScan();

    /// Decrement number of running backfills by one
    virtual void decrNumRunningBackfills();

    /// Decrement number of running RangeScans by one
    virtual void decrNumRunningRangeScans();

    /// update only the maxNumBackfillsPerConnection limit
    void updateMaxRunningDcpBackfills(size_t maxBackfills);

    /**
     * Update the maxRunning and maxRunningRangeScans limits based on a quota.
     * The values are found by calling:
     *   getMaxRunningScansForQuota(maxDataSize, rangeScanRatio)
     * @param maxDataSize The bucket quota
     * @param rangeScanRatio The ratio of total scans available for RangeScan
     * @param maxBackfills The maximum number of DCP backfills
     */
    void updateMaxRunningScans(size_t maxDataSize,
                               float rangeScanRatio,
                               size_t maxBackfills);

    /**
     * Set the max running scans (separate limits for backfill/RangeScan). This
     * function allows any value to permit simpler testing.
     *
     * @param newMaxRunningBackfills How many DCP backfills can exist
     * @param newMaxRunningRangeScans How many RangeScans can exist
     * @param maxBackfills The maximum number of backfills
     */
    void setMaxRunningScans(uint16_t newMaxRunningBackfills,
                            uint16_t newMaxRunningRangeScans,
                            size_t maxBackfills);

    /// @return how many DCP backfills are running
    uint16_t getNumRunningBackfills() {
        return scans.lock()->runningBackfills;
    }

    /// @return the maximum number of DCP backfills
    uint16_t getMaxRunningBackfills() const {
        return scans.lock()->maxRunning;
    }

    /// @return how many RangeScans are running
    uint16_t getNumRunningRangeScans() {
        return scans.lock()->runningRangeScans;
    }

    /// @return the maximum number of RangeScans
    uint16_t getMaxRunningRangeScans() {
        return scans.lock()->maxRunningRangeScans;
    }

    /**
     * Return the maximum number of backfills and RangeScans from the given
     * maxDataSize (quota).
     *
     * @param maxDataSize The bucket quota
     * @param rangeScanRatio The ratio of total scans available for RangeScan
     * @return std::pair, first is max backfills, second is max RangeScans
     */
    static std::pair<uint16_t, uint16_t> getMaxRunningScansForQuota(
            size_t maxDataSize, float rangeScanRatio);

private:
    // Current and maximum number of scans (i.e. DCPBackfills). These may not
    // be actively scanning, but have an open snapshot.
    struct Scans {
        uint16_t getTotalRunning() const {
            return runningBackfills + runningRangeScans;
        }

        uint16_t runningBackfills{0};

        uint16_t runningRangeScans{0};

        // The upper limit
        uint16_t maxRunning{0};

        // The upper limit for RangeScan and is generally lower than maxRunning
        uint16_t maxRunningRangeScans{0};

        /// Maximum number of backfills per connection - taken from
        /// dcp_backfill_in_progress_per_connection_limit
        size_t maxNumBackfillsPerConnection{0};
    };
    folly::Synchronized<Scans, std::mutex> scans;
};

using UniqueDCPBackfillPtr = std::unique_ptr<DCPBackfillIface>;
