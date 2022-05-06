/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan.h"

#include "bucket_logger.h"
#include "ep_bucket.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "vbucket.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memcached/cookie_iface.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>

RangeScan::RangeScan(EPBucket& bucket,
                     const VBucket& vbucket,
                     DiskDocKey start,
                     DiskDocKey end,
                     RangeScanDataHandlerIFace& handler,
                     const CookieIface& cookie,
                     cb::rangescan::KeyOnly keyOnly)
    : start(std::move(start)),
      end(std::move(end)),
      vbUuid(vbucket.failovers->getLatestUUID()),
      handler(handler),
      vbid(vbucket.getId()),
      keyOnly(keyOnly) {
    // Don't init the uuid in the initialisation list as we may read various
    // members which must be initialised first
    uuid = createScan(cookie, bucket);

    EP_LOG_DEBUG("RangeScan {} created for {}", uuid, getVBucketId());
}

cb::rangescan::Id RangeScan::createScan(const CookieIface& cookie,
                                        EPBucket& bucket) {
    auto valFilter = cookie.isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY)
                             ? ValueFilter::VALUES_COMPRESSED
                             : ValueFilter::VALUES_DECOMPRESSED;

    scanCtx = bucket.getRWUnderlying(getVBucketId())
                      ->initByIdScanContext(
                              std::make_unique<RangeScanDiskCallback>(*this),
                              std::make_unique<RangeScanCacheCallback>(*this,
                                                                       bucket),
                              getVBucketId(),
                              {ByIdRange{start, end}},
                              DocumentFilter::NO_DELETES,
                              valFilter);

    if (!scanCtx) {
        // KVStore logs more details
        throw cb::engine_error(
                cb::engine_errc::failed,
                fmt::format("RangeScan::createScan {} initByIdScanContext "
                            "returned nullptr",
                            getVBucketId()));
    }

    // Generate the scan ID (which may also incur i/o)
    return boost::uuids::random_generator()();
}

cb::engine_errc RangeScan::continueScan(KVStoreIface& kvstore) {
    EP_LOG_DEBUG("RangeScan {} continue for {}", uuid, getVBucketId());

    // Only attempt scan when !cancelled
    if (isCancelled()) {
        // ensure the client/cookie sees cancelled
        handleStatus(cb::engine_errc::range_scan_cancelled);
        // but return success so the scan now gets cleaned-up
        return cb::engine_errc::success;
    }

    auto status = kvstore.scan(*scanCtx);

    switch (status) {
    case ScanStatus::Yield:
        // Scan reached a limit and has yielded.
        // Set to idle, which will send success to the handler
        setStateIdle(cb::engine_errc::range_scan_more);
        // return too_busy status so scan is eligible for further continue
        return cb::engine_errc::too_busy;
    case ScanStatus::Success:
        // Scan has reached the end
        setStateIdle(cb::engine_errc::success);
        break;
    case ScanStatus::Cancelled:
        // Scan cannot continue and has been cancelled, e.g. vbucket has gone
        // The engine_err has been passed to the handler already
        break;
    case ScanStatus::Failed:
        // Scan cannot continue due to KVStore failure
        handleStatus(cb::engine_errc::failed);
        break;
    }

    // For Success/Cancelled/Failed return success. The actual client visible
    // status has been push though handleStatus. The caller of this method only
    // needs too_busy (for Yield) or success (please cancel and clean-up)
    return cb::engine_errc::success;
}

bool RangeScan::isIdle() const {
    return continueState.rlock()->state == State::Idle;
}

bool RangeScan::isContinuing() const {
    return continueState.rlock()->state == State::Continuing;
}

bool RangeScan::isCancelled() const {
    return continueState.rlock()->state == State::Cancelled;
}

void RangeScan::setStateIdle(cb::engine_errc status) {
    continueState.withWLock([](auto& cs) {
        switch (cs.state) {
        case State::Idle:
        case State::Cancelled:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateIdle invalid state:{}", cs.state));
        case State::Continuing:
            cs.state = State::Idle;
            cs.cookie = nullptr;
            break;
        }
    });

    // Changing to Idle implies a successful Continue.
    // success is the end of the scan
    // range_scan_more is a 'pause' due to limits/yield of the task
    Expects(status == cb::engine_errc::success ||
            status == cb::engine_errc::range_scan_more);

    // The ordering here is deliberate, set the status after the cs.state update
    // so there's no chance a client sees 'success' then fails to continue again
    // (cannot continue an already continued scan)
    handler.handleStatus(status);
}

void RangeScan::setStateContinuing(const CookieIface& client) {
    continueState.withWLock([&client](auto& cs) {
        switch (cs.state) {
        case State::Continuing:
        case State::Cancelled:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateContinuing invalid state:{}",
                    cs.state));
        case State::Idle:
            cs.state = State::Continuing;
            cs.cookie = &client;
            break;
        }
    });
}

void RangeScan::setStateCancelled() {
    continueState.withWLock([](auto& cs) {
        switch (cs.state) {
        case State::Cancelled:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateCancelled invalid state:{}", cs.state));
        case State::Idle:
        case State::Continuing:
            cs.state = State::Cancelled;
            break;
        }
    });
}

void RangeScan::handleKey(DocKey key) {
    handler.handleKey(key);
}

void RangeScan::handleItem(std::unique_ptr<Item> item) {
    handler.handleItem(std::move(item));
}

void RangeScan::handleStatus(cb::engine_errc status) {
    handler.handleStatus(status);
}

void RangeScan::addStats(const StatCollector& collector) const {
    fmt::memory_buffer prefix;
    fmt::format_to(std::back_inserter(prefix), "vb_{}:{}", vbid.get(), uuid);
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        format_to(std::back_inserter(key),
                  "{}:{}",
                  std::string_view{prefix.data(), prefix.size()},
                  statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };

    addStat("vbuuid", vbUuid);
    addStat("start", cb::UserDataView(start.to_string()).getRawValue());
    addStat("end", cb::UserDataView(end.to_string()).getRawValue());
    addStat("key_value",
            keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value");
    addStat("queued", queued);

    // copy state and then addStat the copy
    auto cs = *continueState.rlock();
    addStat("state", fmt::format("{}", cs.state));
    addStat("cookie", cs.cookie);
}

void RangeScan::dump(std::ostream& os) const {
    // copy state then print, avoiding invoking ostream whilst locked
    auto cs = *continueState.rlock();
    fmt::print(os,
               "RangeScan: uuid:{}, {}, vbuuid:{}, range:({},{}), kv:{},"
               "queued:{}, state:{}, cookie:{}\n",
               uuid,
               vbid,
               vbUuid,
               cb::UserDataView(start.to_string()),
               cb::UserDataView(end.to_string()),
               (keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value"),
               queued,
               cs.state,
               reinterpret_cast<const void*>(cs.cookie));
}

std::ostream& operator<<(std::ostream& os, const RangeScan::State& state) {
    switch (state) {
    case RangeScan::State::Idle:
        return os << "State::Idle";
    case RangeScan::State::Continuing:
        return os << "State::Continuing";
    case RangeScan::State::Cancelled:
        return os << "State::Cancelled";
    }
    throw std::runtime_error(
            fmt::format("RangeScan::State ostream operator<< invalid state:{}",
                        int(state)));
}

std::ostream& operator<<(std::ostream& os, const RangeScan& scan) {
    scan.dump(os);
    return os;
}
