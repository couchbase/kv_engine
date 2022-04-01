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

#include "callbacks.h"
#include "storeddockey.h"

#include <folly/Synchronized.h>
#include <memcached/range_scan.h>
#include <memcached/range_scan_id.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>

class ByIdScanContext;
class CookieIface;
class EPBucket;
class KVStoreIface;
class RangeScanDataHandlerIFace;
class StatCollector;
class VBucket;

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
     */
    RangeScan(EPBucket& bucket,
              const VBucket& vbucket,
              DiskDocKey start,
              DiskDocKey end,
              RangeScanDataHandlerIFace& handler,
              const CookieIface& cookie,
              cb::rangescan::KeyOnly keyOnly);

    /**
     * Continue the range scan by calling kvstore.scan()
     *
     * @param kvstore A KVStoreIface on which to call scan
     * @return success or too_busy
     */
    cb::engine_errc continueScan(KVStoreIface& kvstore);

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

    /// change the state of the scan to Idle
    void setStateIdle(cb::engine_errc status);

    /**
     * Change the state of the scan to Continuing
     * @param client cookie of the client which continued the scan
     * @param itemLimit how many items the scan can return
     * @param timeLimit how long the scan can run for (0 no limit)
     */
    void setStateContinuing(const CookieIface& client,
                            size_t itemLimit,
                            std::chrono::milliseconds timeLimit);

    /// change the state of the scan to Cancelled
    void setStateCancelled();

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
     */
    void handleKey(DocKey key);

    enum Source { Memory, Disk };

    /**
     * Callback method invoked for each Item that is read from the snapshot.
     * This is only invoked for a KeyOnly::No scan.
     *
     * @param item An Item read from a Key/Value scan
     * @param source Item was found in memory or on disk
     */
    void handleItem(std::unique_ptr<Item> item, Source source);

    /**
     * Callback method for when a scan has finished a "continue" and is used to
     * set the status of the scan. A "continue" can finish prematurely due to
     * an error or successfully because it has reached the end of the scan or
     * a limit.
     *
     * @param status The status of the just completed continue
     */
    void handleStatus(cb::engine_errc status);

    /// Increment the scan's itemCount for an item read by the scan
    void incrementItemCount();

    /// Increment the scan's itemCount for an item read by the scan
    void incrementValueFromMemory();

    /// Increment the scan's itemCount for an item read by the scan
    void incrementValueFromDisk();

    /// @return true if limits have been reached
    bool areLimitsExceeded();

    /// Generate stats for this scan
    void addStats(const StatCollector& collector) const;

    /// dump the object to the ostream (default of cerr)
    void dump(std::ostream& os = std::cerr) const;
    /**
     * To facilitate testing, the now function, which returns a time point can
     * be replaced
     */
    static void setClockFunction(
            std::function<std::chrono::steady_clock::time_point()> func) {
        now = func;
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
     * @return the cb::rangescan::Id to use for this scan
     */
    cb::rangescan::Id createScan(const CookieIface& cookie, EPBucket& bucket);

    // member variables ideally ordered by size large -> small
    cb::rangescan::Id uuid;
    DiskDocKey start;
    DiskDocKey end;
    // uuid of the vbucket to assist detection of a vbucket state change
    uint64_t vbUuid;
    std::unique_ptr<ByIdScanContext> scanCtx;
    RangeScanDataHandlerIFace& handler;
    /// keys read for the life of this scan (counted for key and value scans)
    size_t totalKeys{0};
    /// items read from memory for the life of this scan (only for value scans)
    size_t totalValuesFromMemory{0};
    /// items read from disk for the life of this scan (only for value scans)
    size_t totalValuesFromDisk{0};

    Vbid vbid;

    // RangeScan can be continued with client defined limits
    // These are written from the worker (continue) into the locked 'runState'
    // As the scan runs on the I/O task all limit checks occur against this copy
    // from the runState - this removes the need for many lock/unlock
    struct ContinueLimits {
        /// current limit for the continuation of this scan
        size_t itemLimit{0};

        /// current time limit for the continuation of this scan
        std::chrono::milliseconds timeLimit{0};
    } continueLimits;

    /**
     * RangeScan has a state with the following legal transitions. Also
     * shown is the operation which makes that transition.
     *
     * Idle->Continuing  (via range-scan-continue)
     * Idle->Cancelled (via range-scan-cancel)
     * Continuing->Idle (via I/O task after a successful continue)
     * Continuing->Cancelled (via range-scan-cancel)
     */
    enum class State : char { Idle, Continuing, Cancelled };

    /**
     * The 'continue' state of the scan is updated from different threads.
     * E.g. worker thread moves from Idle to Continue, but I/O thread moves from
     * Continue to Idle. The cookie also changes, so these are all managed as
     * a single structure via folly::Synchronized
     */
    struct ContinueState {
        // cookie will transition from null -> cookie -> null ...
        const CookieIface* cookie{nullptr};
        State state{State::Idle};
        ContinueLimits limits;
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
        ContinueState cState;
        /// item count for the continuation of this scan
        size_t itemCount{0};
        /// deadline for the continue (only enforced if a time limit is set)
        std::chrono::steady_clock::time_point scanContinueDeadline;
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
};

std::ostream& operator<<(std::ostream&, const RangeScan::State&);
std::ostream& operator<<(std::ostream&, const RangeScan&);
