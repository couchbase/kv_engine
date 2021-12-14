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

#include "range_scans/range_scan_owner.h"

#include "bucket_logger.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan.h"
#include "range_scans/range_scan_callbacks.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/ostream.h>

cb::engine_errc VB::RangeScanOwner::addNewScan(
        std::shared_ptr<RangeScan> scan) {
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

cb::engine_errc VB::RangeScanOwner::continueScan(cb::rangescan::Id id) {
    EP_LOG_DEBUG("VB::RangeScanOwner::continueScan {}", id);

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
    itr->second->setStateContinuing();

    // @todo ensure an I/O task picks up the scan and continues
    return cb::engine_errc::success;
}

cb::engine_errc VB::RangeScanOwner::cancelScan(cb::rangescan::Id id) {
    EP_LOG_DEBUG("VB::RangeScanOwner::cancelScan {}", id);

    auto locked = rangeScans.wlock();
    auto itr = locked->find(id);
    if (itr == locked->end()) {
        return cb::engine_errc::no_such_key;
    }

    // Set to cancel
    itr->second->setStateCancelled();

    // @todo: task should cancel (destruct) on I/O task as it will need to
    // work with the kvstore to close the file(s) backing the scan.

    // Erase from the map, no further continue/cancel allowed
    locked->erase(itr);

    return cb::engine_errc::success;
}