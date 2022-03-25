/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <unordered_map>

class KVStoreIface;

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
     * @return success or other status (see above)
     */
    cb::engine_errc continueScan(cb::rangescan::Id id);

    /**
     * Handler for a range-scan-cancel operation. Method will locate the
     * scan and mark it cancelled and remove it from the set of known scans.
     *
     * Failure to locate the scan -> cb::engine_errc::no_such_key
     * @param id of the scan to cancel
     * @return success or other status (see above)
     */
    cb::engine_errc cancelScan(cb::rangescan::Id id);

    /**
     * Find the scan for the given id
     */
    std::shared_ptr<RangeScan> getScan(cb::rangescan::Id id) const;

protected:
    /**
     * All scans that are available for continue/cancel
     */
    folly::Synchronized<std::unordered_map<cb::rangescan::Id,
                                           std::shared_ptr<RangeScan>,
                                           boost::hash<boost::uuids::uuid>>>
            rangeScans;
};
} // namespace VB