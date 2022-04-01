/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_owner.h"

#include "bucket_logger.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>

void ReadyRangeScans::addScan(std::shared_ptr<RangeScan> scan) {
    auto locked = rangeScans.wlock();

    // RangeScan should only be queued once. It is ok for the state to change
    // whilst queued. This isn't overly critical, but prevents a
    // continue->cancel placing the same shared_ptr in the queue twice resulting
    // in two runs of the continue task
    if (scan->isQueued()) {
        return;
    }
    locked->push(scan);
    scan->setQueued(true);
}

std::shared_ptr<RangeScan> ReadyRangeScans::takeNextScan() {
    std::shared_ptr<RangeScan> scan;
    auto locked = rangeScans.wlock();
    if (locked->size()) {
        scan = locked->front();
        locked->pop();
        scan->setQueued(false);
    }
    return scan;
}

VB::RangeScanOwner::RangeScanOwner(ReadyRangeScans* scans) : readyScans(scans) {
}

cb::engine_errc VB::RangeScanOwner::addNewScan(
        std::shared_ptr<RangeScan> scan) {
    Expects(readyScans);
    if (rangeScans.withWLock([&scan](auto& map) {
            auto [itr, emplaced] =
                    map.try_emplace(scan->getUuid(), std::move(scan));
            return emplaced;
        })) {
        return cb::engine_errc::success;
    }
    EP_LOG_WARN("VB::RangeScanOwner::addNewScan failed to insert for uuid:{}",
                scan->getUuid());
    return cb::engine_errc::key_already_exists;
}

cb::engine_errc VB::RangeScanOwner::continueScan(
        cb::rangescan::Id id,
        const CookieIface& cookie,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit) {
    Expects(readyScans);
    EP_LOG_DEBUG(
            "VB::RangeScanOwner::continueScan {} itemLimit:{} timeLimit:{}",
            id,
            itemLimit,
            timeLimit.count());
    auto locked = rangeScans.wlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return cb::engine_errc::no_such_key;
    }

    // Only an idle scan can be continued
    if (!itr->second->isIdle()) {
        return cb::engine_errc::too_busy;
    }

    // set scan to 'continuing'
    itr->second->setStateContinuing(cookie, itemLimit, timeLimit);

    // Make the scan available to I/O task(s)
    readyScans->addScan(itr->second);

    return cb::engine_errc::success;
}

cb::engine_errc VB::RangeScanOwner::cancelScan(cb::rangescan::Id id,
                                               bool addScan) {
    Expects(readyScans);
    EP_LOG_DEBUG("VB::RangeScanOwner::cancelScan {}", id);
    auto locked = rangeScans.wlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return cb::engine_errc::no_such_key;
    }

    // Set to cancel
    itr->second->setStateCancelled();

    // obtain the scan
    auto scan = itr->second;

    // Erase from the map, no further continue/cancel allowed.
    locked->erase(itr);

    if (addScan) {
        // Make the scan available to I/O task(s) for final closure of data file
        readyScans->addScan(scan);
    }
    // scan should now destruct here (!addScan) - this case is used when the
    // I/O task itself calls cancelRangeScan, not when the worker thread does

    return cb::engine_errc::success;
}

std::shared_ptr<RangeScan> VB::RangeScanOwner::getScan(
        cb::rangescan::Id id) const {
    auto locked = rangeScans.rlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return {};
    }
    return itr->second;
}
