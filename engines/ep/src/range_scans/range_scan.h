/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "diskdockey.h"
#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>
#include <memcached/storeddockey.h>
#include <memcached/vbucket.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <random>

class ByIdScanContext;
class CookieIface;
class EPBucket;
class EventuallyPersistentEngine;
class KVStoreIface;
class KVStoreScanTracker;
class RangeScanDataHandlerIFace;
class StatCollector;
class VBucket;

namespace cb::rangescan {
struct SamplingConfiguration;
struct SnapshotRequirements;
}

/**
 * RangeScan class is the object created by each successful range-scan-create
 * command.
 *
 * The class is constructed using as input a start and end key which form an
 * inclusive range [a, b]. The object opens (and holds open) a KVStore snapshot
 * so that the KVStore::scan function can be used to iterate over the range and
 * return keys or Items to the RangeScanDataHandlerIFace.
 *
 */
class RangeScan {
public:
    /**
     * Create a RangeScan object for the given vbucket and with the given
     * callbacks. The constructed RangeScan will open the underlying KVStore.
     *
     * @throws cb::engine_error with error code + details
     * @param bucket The EPBucket to use to obtain the KVStore and pass to the
     *               RangeScanCacheCallback
     * @param vbucket vbucket to scan
     * @param start The range start
     * @param end The range end
     * @param handler key/item handler to process key/items of the scan
     * @param cookie connection cookie creating the RangeScan
     * @param keyOnly configure key or value scan
     * @param snapshotReqs optional requirements for the snapshot
     * @param samplingConfig optional configuration for random sampling mode
     */
    RangeScan(
            EPBucket& bucket,
            const VBucket& vbucket,
            DiskDocKey start,
            DiskDocKey end,
            std::unique_ptr<RangeScanDataHandlerIFace> handler,
            CookieIface& cookie,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig);

    /**
     * Create a RangeScan initialising the uuid to the given value.
     *
     * This constructor exists only to aid testing of RangeScanOwner. A scan
     * constructed with this will fail to scan.
     *
     * @param id value to use for RangeScan::uuid
     * @param resourceTracker to track how many scans are open
     */
    RangeScan(cb::rangescan::Id id, KVStoreScanTracker& resourceTracker);

    ~RangeScan();

    /**
     * Check if the calling connection is privileged to progress this scan.
     * Continue/Cancel must defer privilege checks until we reach the engine
     * and locate the scan as we must look up the collection being scanned from
     * this object.
     *
     * Note this call also has the side affect of rechecking the scan-collection
     * exists (which could be dropped whilst a scan is idle). Caller can use
     * that to cancel scans.
     */
    cb::engine_errc hasPrivilege(CookieIface& cookie,
                                 const EventuallyPersistentEngine& engine);

    /**
     * Prepare the scan ready to continue. This function performs "pre-flight"
     * checks and sets the continueRunState. If this function
     * returns range_scan_more - continueOnIOThread can be called
     *
     * range_scan_more - the scan is ready to progress
     * range_scan_cancelled - the the scan has been cancelled
     *
     * @return success, range_scan_more or range_scan_cancelled
     */
    cb::engine_errc prepareToContinueOnIOThread();

    /**
     * Continue the scan on an IO task forwards until a condition is reached
     * that requires the scan to stop. The scan may hit a limit or a condition
     * which means the scan is now cancelled (by request or some environmental
     * state change)
     *
     * success - scan reached the end key
     * range_scan_more - scan reached a limit and is paused
     * range_scan_cancelled - scan cannot continue
     *
     * @param kvstore A KVStoreIface on which to call scan
     * @return success, range_scan_more or range_scan_cancelled
     */
    cb::engine_errc continueOnIOThread(KVStoreIface& kvstore);

    /// @return the universally unique id of this scan (exposed to the client)
    cb::rangescan::Id getUuid() const {
        return uuid;
    }

    /// @return true if the scan is currently idle
    bool isIdle() const;

    /// @return true if the scan is currently continuing
    bool isContinuing() const;

    /// @return true if the scan is cancelled
    bool isCancelled() const;

    /// @return true if the scan is completed (reached the end)
    bool isCompleted() const;

    /**
     * Change the state of the scan to Continuing
     * @param client cookie of the client which continued the scan
     * @param itemLimit how many items the scan can return (0 no limit)
     * @param timeLimit how long the scan can run for (0 no limit)
     * @param byteLimit how many bytes the continue can return (0 no limit)
     */
    void setStateContinuing(CookieIface& client,
                            size_t itemLimit,
                            std::chrono::milliseconds timeLimit,
                            size_t byteLimit);

    /// change the state of the scan to Cancelled
    void setStateCancelled();

    /// change the state of the scan to Completed
    void setStateCompleted();

    /// @return how many seconds the scan has left (based on the timeLimit)
    std::chrono::seconds getRemainingTime(std::chrono::seconds timeLimit);

    /// @return the vbucket ID owning this scan
    Vbid getVBucketId() const {
        return vbid;
    }

    /// @return true if the scan is configured for keys only
    bool isKeyOnly() const {
        return keyOnly == cb::rangescan::KeyOnly::Yes;
    }

    /// method for use by RangeScans to ensure we only queue a scan once
    bool isQueued() const {
        return queued;
    }
    /// method for use by RangeScans to ensure we only queue a scan once
    void setQueued(bool q) {
        queued = q;
    }

    /**
     * Callback method invoked for each key that is read from the snapshot. This
     * is only invoked for a KeyOnly::Yes scan.
     *
     * @param key A key read from a Key only scan
     * @return false if the key cannot be sent (disconnected continue)
     */
    bool handleKey(DocKey key);

    enum Source { Memory, Disk };

    /**
     * Callback method invoked for each Item that is read from the snapshot.
     * This is only invoked for a KeyOnly::No scan.
     *
     * @param item An Item read from a Key/Value scan
     * @param source Item was found in memory or on disk
     * @return false if the key cannot be sent (disconnected continue)
     */
    bool handleItem(std::unique_ptr<Item> item, Source source);

    /**
     * Callback method for when a scan has finished a "continue" and is used to
     * set the status of the scan. A "continue" can finish prematurely due to
     * an error or successfully because it has reached the end of the scan or
     * a limit.
     *
     * @param status The status of the just completed continue
     * @return false if the status cannot be sent (disconnected continue)
     */
    bool handleStatus(cb::engine_errc status);

    /**
     * Callback method for when a scan encounters an unknown collection during
     * the continue
     *
     * @param manifestUid The uid of the manifest in which the collection was
     *        not found.
     */
    void handleUnknownCollection(uint64_t manifestUid);

    /**
     * Increment the scan's itemCount/byteCount/totalCount for an something
     * read by the scan
     * @param size value to increment byteCount by
     */
    void incrementItemCounters(size_t size);

    /// Increment the scan's itemCount for an item read by the scan
    void incrementValueFromMemory();

    /// Increment the scan's itemCount for an item read by the scan
    void incrementValueFromDisk();

    /// @return true if limits have been reached
    bool areLimitsExceeded() const;

    /// @return true if the current item should be skipped
    bool skipItem();

    /// @return true if the vbucket can be scanned (correct state/uuid)
    bool isVbucketScannable(const VBucket& vb) const;

    /// Generate stats for this scan
    void addStats(const StatCollector& collector) const;

    /// dump this to std::cerr
    void dump() const;

    /// @return the collection being scanned
    CollectionID getCollectionID() const;

    /**
     * To facilitate testing, the now function, which returns a time point can
     * be replaced
     */
    static void setClockFunction(
            std::function<std::chrono::steady_clock::time_point()> func) {
        now = func;
    }

    /**
     * Change the clock function (which drives now()) back to default
     */
    static void resetClockFunction();

    /// @return the current time which is used by RangeScans
    static std::chrono::steady_clock::time_point getTime() {
        return now();
    }

protected:
    /**
     * Function to create a scan by opening the underlying disk snapshot. The
     * KVStore is located from the EPBucket and the snapshot obtained by calling
     * initByIdScanContext. The EPBucket is also passed to the
     * RangeScanCacheCallback for use when the scan loads items.
     *
     * On failure throws a std::runtime_error
     *
     * @param cookie The client cookie creating the range-scan
     * @param bucket The EPBucket to use to obtain the KVStore and pass to the
     *               RangeScanCacheCallback
     * @param snapshotReqs optional requirements for the snapshot
     * @param samplingConfig optional configuration for random sampling mode
     * @return the Id to use for this scan
     */
    cb::rangescan::Id createScan(
            CookieIface& cookie,
            EPBucket& bucket,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig);

    /**
     * method used in construction - a range scan can only be created if 1 or
     * more keys exist in the range. Detecting an empty range allows KV to fail
     * the create
     */
    size_t tryAndScanOneKey(KVStoreIface& kvstore);

    /// @return true if this scan is a random sample scan
    bool isSampling() const;

    /**
     * Try to change state to idle and send the status to the cookie/client.
     * If the scan has been cancelled the state cannot be changed and the given
     * status is replaced with range_scan_cancelled.
     * @return success if state was changed and status consumed
     */
    cb::engine_errc tryAndSetStateIdle(cb::engine_errc status);

    /**
     * Change to idle if the current state allows, if the state is cancelled
     * no state change occurs.
     *
     * @return true if state change did not occur, false if not (scan is
     *         cancelled)
     */
    bool setStateIdle(cb::engine_errc status);

    // member variables ideally ordered by size large -> small
    cb::rangescan::Id uuid;
    DiskDocKey start;
    DiskDocKey end;
    // uuid of the vbucket to assist detection of a vbucket state change
    uint64_t vbUuid{0};
    std::unique_ptr<ByIdScanContext> scanCtx;
    std::unique_ptr<RangeScanDataHandlerIFace> handler;
    KVStoreScanTracker& resourceTracker;
    /// keys read for the life of this scan (counted for key and value scans)
    size_t totalKeys{0};
    /// items read from memory for the life of this scan (only for value scans)
    size_t totalValuesFromMemory{0};
    /// items read from disk for the life of this scan (only for value scans)
    size_t totalValuesFromDisk{0};
    /// Time the scan was created
    std::chrono::steady_clock::time_point createTime;

    /**
     * Following 2 member variables are used only when a
     * cb::rangescan::SamplingConfiguration is provided to the constructor.
     * The prng is allocated on demand as it's quite large (~2500bytes) and only
     * needed by sampling scans.
     */
    std::unique_ptr<std::mt19937> prng;
    std::bernoulli_distribution distribution{0.0};

    Vbid vbid{0};

    // RangeScan can be continued with client defined limits
    // These are written from the worker (continue) into the locked 'runState'
    // As the scan runs on the I/O task all limit checks occur against this copy
    // from the runState - this removes the need for many lock/unlock
    struct ContinueLimits {
        /// current limit for the continuation of this scan
        size_t itemLimit{0};

        /// current time limit for the continuation of this scan
        std::chrono::milliseconds timeLimit{0};

        /// current byte limit for the continuation of this scan
        size_t byteLimit{0};

        /// dump this to std::cerr
        void dump() const;
    } continueLimits;

    /**
     * Idle: The scan is awaiting a range-scan-continue or a cancel event.
     * Continuing: The scan is contiuning, this state covers the point from
     *             processing the range-scan-continue, waiting for a task and
     *             whilst scanning on the task.
     * Cancelled: A cancellation occurred - this could be a client request or
     *            some other issue (timeout, vbucket state change). When a
     *            scan is in this state, the VB::RangeScanOwner does not have
     *            the scan any more, but note that the object (via shared_ptr)
     *            could be queued waiting for a task or running on the task.
     * Completed: A scan reached the end of the range. When a scan is in this
     *            state, the VB::RangeScanOwner does not have the scan.
     *            Only a small part of the RangeScanContinueTask can still have
     *            a reference to the scan (via a shared_ptr).
     *
     * The following transitions can occur:
     *
     * Idle -> Continuing
     *   - range-scan-continue
     * Continuing -> Idle
     *   - RangeScanCreateTask when a scan must Yield
     * Continuing -> Completed
     *   - RangeScanCreateTask calling EPVbucket::completeRangeScan() because
     *     scan reached the end.
     * Continuing -> Cancelled
     *   - RangeScanCreateTask calling EPVBucket::cancelRangeScan because a
     *     KVStore::scan failure or runtime state change occurred.
     *   - range-scan-cancel
     *   - VB::~RangeScanOwner (~VBucket path)
     *   - RangeScanWatchDog
     * Idle -> Cancelled
     *   - range-scan-cancel
     *   - VB::~RangeScanOwner (~VBucket path)
     *   - RangeScanWatchDog
     */
    enum class State : char { Idle, Continuing, Cancelled, Completed };

    /**
     * The 'continue' state of the scan is updated from different threads.
     * E.g. worker thread moves from Idle to Continue, but I/O thread moves from
     * Continue to Idle. The cookie also changes, so these are all managed as
     * a single structure via folly::Synchronized
     */
    struct ContinueState {
        /**
         * A scan changes from Continue to Idle, any cookie and limits are now
         * cleared. This method changes the state to Idle and clears cookie and
         * limits.
         */
        void setupForIdle();

        /**
         * A scan changes from Idle to Continue and copies in the cookie and
         * limits so that the run can process the scan and respond to the
         * cookie.
         */
        void setupForContinue(CookieIface& c,
                              size_t limit,
                              std::chrono::milliseconds timeLimit,
                              size_t byteLimit);

        /**
         * Complete only occurs once a scan has successfully reached and sent
         * the end key, any cookie/limits are now cleared. The scan will not
         * run again and will soon delete. This method changes the state to
         * Complete and clears cookie and limits.
         */
        void setupForComplete();

        /**
         * Cancellation can occur at any point. Whilst the scan is idle, waiting
         * to run or running. This method will change only the state to
         * Cancelled and leave the cookie/limits unchanged. This allows any
         * waiting/running scan to end correctly passing a status to the cookie.
         */
        void setupForCancel();

        /// dump this to std::cerr
        void dump() const;

        // cookie will transition from null -> cookie -> null ...
        CookieIface* cookie{nullptr};
        State state{State::Idle};
        ContinueLimits limits;
        cb::engine_errc finalStatus{cb::engine_errc::success};
    };
    folly::Synchronized<ContinueState> continueState;

    /**
     * The ContinueRunState is the state used by I/O task run() loop for a
     * RangeScan. It contains a copy of the ContinueState that defines how the
     * continue operates (client and limits)
     */
    struct ContinueRunState {
        ContinueRunState();
        ContinueRunState(const ContinueState& cs);
        /// dump this to std::cerr
        void dump() const;

        ContinueState cState;
        /// item count for the continuation of this scan
        size_t itemCount{0};
        /// byte count for the continuation of this scan
        size_t byteCount{0};
        /// deadline for the continue (only enforced if a time limit is set)
        std::chrono::steady_clock::time_point scanContinueDeadline;
        /// Written after each key/doc is read from cookie->checkThrottle
        bool limitByThrottle{false};
    } continueRunState;

    cb::rangescan::KeyOnly keyOnly{cb::rangescan::KeyOnly::No};
    /// is this scan in the run queue? This bool is read/written only by
    /// RangeScans under the queue lock
    bool queued{false};

    // To facilitate testing, the clock can be replaced with something else
    // this is static as there's no need for replacement on a scan by scan basis
    static std::function<std::chrono::steady_clock::time_point()> now;

    friend std::ostream& operator<<(std::ostream&, const RangeScan::State&);
    friend std::ostream& operator<<(std::ostream&, const RangeScan&);
    friend std::ostream& operator<<(std::ostream&,
                                    const RangeScan::ContinueLimits&);
    friend std::ostream& operator<<(std::ostream&,
                                    const RangeScan::ContinueState&);
    friend std::ostream& operator<<(std::ostream&,
                                    const RangeScan::ContinueRunState&);
};

std::ostream& operator<<(std::ostream&, const RangeScan::State&);
std::ostream& operator<<(std::ostream&, const RangeScan&);
std::ostream& operator<<(std::ostream&, const RangeScan::ContinueLimits&);
std::ostream& operator<<(std::ostream&, const RangeScan::ContinueState&);
std::ostream& operator<<(std::ostream&, const RangeScan::ContinueRunState&);
