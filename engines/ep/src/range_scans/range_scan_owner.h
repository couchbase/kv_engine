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

#include "range_scans/range_scan.h"

#include <boost/functional/hash.hpp>
#include <folly/Synchronized.h>
#include <memcached/dockey.h>
#include <memcached/engine_error.h>
#include <memcached/range_scan_id.h>
#include <memcached/vbucket.h>

#include <queue>
#include <unordered_map>

class KVStoreIface;

/**
 * ReadyRangeScans keeps a reference (shared_ptr) to all scans that are ready
 * for execution on a I/O task. These are scans that the client has continued or
 * cancelled.
 */
class ReadyRangeScans {
public:
    /**
     * Take the next available scan out of the 'ready' scans container
     */
    std::shared_ptr<RangeScan> takeNextScan();

    /**
     * Add scan to the 'ready' scans container
     */
    void addScan(std::shared_ptr<RangeScan> scan);

protected:
    folly::Synchronized<std::queue<std::shared_ptr<RangeScan>>> rangeScans;
};

namespace VB {

/**
 * RangeScanOwner owns all of the RangeScan objects (for 1 vbucket) that are
 * available to be continued or cancelled and provides methods to drive a
 * RangeScan
 *
 * Having a VBucket level container (instead of 1 KVBucket container)
 * simplifies some aspects of RangeScan life-time for when a VBucket is deleted.
 *
 */
class RangeScanOwner {
public:
    /**
     * Construct the owner with a pointer to the ReadyRangeScans (bucket
     * container). This is a pointer as some unit tests create vbuckets with
     * no bucket.
     */
    RangeScanOwner(ReadyRangeScans* scans);

    /**
     * Add a new scan to the set of available scans.
     *
     * @param scan to add
     * @return success if added
     */
    cb::engine_errc addNewScan(std::shared_ptr<RangeScan> scan);

    /**
     * Handler for a range-scan-continue operation. Method will locate the
     * scan and make it available for running.
     *
     * Failure to locate the scan -> cb::engine_errc::no_such_key
     * Scan already continued -> cb::engine_errc::too_busy
     *
     * @param id of the scan to continue
     * @param cookie client cookie requesting the continue
     * @param itemLimit limit for the items that can be read in this continue
     * @return success or other status (see above)
     */
    cb::engine_errc continueScan(cb::rangescan::Id id,
                                 const CookieIface& cookie,
                                 size_t itemLimit);

    /**
     * Handler for a range-scan-cancel operation. Method will locate the
     * scan and mark it cancelled and remove it from the set of known scans.
     *
     * Failure to locate the scan -> cb::engine_errc::no_such_key
     * @param id of the scan to cancel
     * @param addScan should the cancelled scan be added to ::RangeScans
     * @return success or other status (see above)
     */
    cb::engine_errc cancelScan(cb::rangescan::Id id, bool addScan);

    /**
     * Find the scan for the given id
     */
    std::shared_ptr<RangeScan> getScan(cb::rangescan::Id id) const;

protected:
    ReadyRangeScans* readyScans;

    /**
     * All scans that are available for continue/cancel
     */
    folly::Synchronized<std::unordered_map<cb::rangescan::Id,
                                           std::shared_ptr<RangeScan>,
                                           boost::hash<boost::uuids::uuid>>>
            rangeScans;
};
} // namespace VB