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
#include "collections/collection_persisted_stats.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "vbucket.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/chrono.h>
#include <memcached/cookie_iface.h>
#include <memcached/range_scan_optional_configuration.h>
#include <statistics/cbstat_collector.h>
#include <utilities/logtags.h>

RangeScan::RangeScan(
        EPBucket& bucket,
        const VBucket& vbucket,
        DiskDocKey start,
        DiskDocKey end,
        std::unique_ptr<RangeScanDataHandlerIFace> handler,
        const CookieIface& cookie,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig)
    : start(std::move(start)),
      end(std::move(end)),
      vbUuid(vbucket.failovers->getLatestUUID()),
      handler(std::move(handler)),
      prng(samplingConfig ? std::make_unique<std::mt19937>(samplingConfig->seed)
                          : nullptr),
      totalLimit(samplingConfig ? samplingConfig->samples : 0),
      vbid(vbucket.getId()),
      keyOnly(keyOnly) {
    // Don't init the uuid in the initialisation list as we may read various
    // members which must be initialised first
    uuid = createScan(cookie, bucket, snapshotReqs, samplingConfig);

    EP_LOG_DEBUG("RangeScan {} created for {}", uuid, getVBucketId());
}

cb::rangescan::Id RangeScan::createScan(
        const CookieIface& cookie,
        EPBucket& bucket,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
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

    if (snapshotReqs) {
        auto& handle = *scanCtx->handle.get();

        // Must check that vb-uuid of the snapshot matches, it could of changed
        // since the create was scheduled. We could just do failovers[0]["id"]
        // but instead choose to construct a FailoverTable for some reuse of
        // the parsing to cover against a bad JSON structure.
        auto state = bucket.getRWUnderlying(getVBucketId())
                             ->getPersistedVBucketState(handle, getVBucketId());
        FailoverTable ft(state.state.transition.failovers,
                         bucket.getEPEngine().getMaxFailoverEntries(),
                         state.state.highSeqno);
        if (ft.getLatestUUID() != snapshotReqs->vbUuid) {
            throw cb::engine_error(cb::engine_errc::not_my_vbucket,
                                   fmt::format("RangeScan::createScan {} "
                                               "snapshotReqs vbUuid mismatch "
                                               "res:{} vs vbstate:{}",
                                               getVBucketId(),
                                               snapshotReqs->vbUuid,
                                               ft.getLatestUUID()));
        }

        // This could fail, but when we have uuid checking it should not
        Expects(uint64_t(scanCtx->maxSeqno) >= snapshotReqs->seqno);
        if (snapshotReqs->seqnoMustBeInSnapshot) {
            auto gv = bucket.getRWUnderlying(getVBucketId())
                              ->getBySeqno(handle,
                                           getVBucketId(),
                                           snapshotReqs->seqno,
                                           ValueFilter::KEYS_ONLY);
            if (gv.getStatus() != cb::engine_errc::success) {
                throw cb::engine_error(
                        cb::engine_errc::not_stored,
                        fmt::format("RangeScan::createScan {} snapshotReqs not "
                                    "met seqno:{} not stored",
                                    getVBucketId(),
                                    snapshotReqs->seqno));
            }
        }
    }

    if (samplingConfig) {
        const auto& handle = *scanCtx->handle.get();
        auto stats =
                bucket.getRWUnderlying(getVBucketId())
                        ->getCollectionStats(
                                handle, start.getDocKey().getCollectionID());
        if (stats.first == KVStore::GetCollectionStatsStatus::Success) {
            if (stats.second.itemCount == 0 ||
                stats.second.itemCount < samplingConfig->samples) {
                throw cb::engine_error(
                        cb::engine_errc::out_of_range,
                        fmt::format("RangeScan::createScan {} sampling cannot "
                                    "be met items:{} samples:{}",
                                    getVBucketId(),
                                    stats.second.itemCount,
                                    samplingConfig->samples));
            }

            // Now we can compute the distribution and assign the first sample
            // index. Example, if asked for 999 samples and 1,000 keys exist
            // then we set 0.999 as the probability.
            distribution = std::bernoulli_distribution{
                    double(samplingConfig->samples) /
                    double(stats.second.itemCount)};
        } else {
            throw cb::engine_error(cb::engine_errc::failed,
                                   fmt::format("RangeScan::createScan {} no "
                                               "collection stats for sampling",
                                               getVBucketId(),
                                               snapshotReqs->seqno));
        }
    }

    // Generate the scan ID (which may also incur i/o)
    return boost::uuids::random_generator()();
}

cb::engine_errc RangeScan::continueScan(KVStoreIface& kvstore) {
    // continue works on a copy of the state
    continueRunState = *continueState.rlock();

    // Only attempt scan when !cancelled
    if (isCancelled()) {
        EP_LOG_DEBUG("RangeScan {} is cancelled for {}", uuid, getVBucketId());
        // ensure the client/cookie sees cancelled
        handleStatus(cb::engine_errc::range_scan_cancelled);
        // but return success so the scan now gets cleaned-up
        return cb::engine_errc::success;
    } else if (isTotalLimitReached()) {
        // No point scanning if the total has been hit. Change to Idle and
        // return success. Any waiting client will see the complete status and
        // the scan now cleans-up
        setStateIdle(cb::engine_errc::range_scan_complete);
        return cb::engine_errc::success;
    }

    EP_LOG_DEBUG("RangeScan {} continue for {}", uuid, getVBucketId());
    auto status = kvstore.scan(*scanCtx);

    switch (status) {
    case ScanStatus::Yield: {
        // Scan reached a limit and has yielded.
        // Set to idle, which will send success to the handler
        setStateIdle(cb::engine_errc::range_scan_more);

        // For RangeScan we have already consumed the last key, which is
        // different to DCP scans where the last key is read and 'rejected', so
        // must be read again. We adjust the startKey so we continue from next
        scanCtx->ranges[0].startKey.append(0);

        // return too_busy status so scan is eligible for further continue
        return cb::engine_errc::too_busy;
    }
    case ScanStatus::Success:
        // Scan has reached the end
        setStateIdle(cb::engine_errc::range_scan_complete);
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

    EP_LOG_DEBUG("RangeScan {} complete with status:{} for {}",
                 uuid,
                 status,
                 getVBucketId());

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
            cs.limits.itemLimit = 0;
            cs.limits.timeLimit = std::chrono::milliseconds(0);
            break;
        }
    });

    // Changing to Idle implies a successful Continue.
    // range_scan_complete is the end of the scan
    // range_scan_more is a 'pause' due to limits/yield of the task
    Expects(status == cb::engine_errc::range_scan_more ||
            status == cb::engine_errc::range_scan_complete);

    // The ordering here is deliberate, set the status after the cs.state update
    // so there's no chance a client sees 'success' then fails to continue again
    // (because you cannot continue a Continuing scan)
    handleStatus(status);
}

void RangeScan::setStateContinuing(const CookieIface& client,
                                   size_t limit,
                                   std::chrono::milliseconds timeLimit) {
    continueState.withWLock([&client, limit, timeLimit](auto& cs) {
        switch (cs.state) {
        case State::Continuing:
        case State::Cancelled:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateContinuing invalid state:{}",
                    cs.state));
        case State::Idle:
            cs.state = State::Continuing;
            cs.cookie = &client;
            cs.limits.itemLimit = limit;
            cs.limits.timeLimit = timeLimit;
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
    incrementItemCount();
    Expects(continueRunState.cState.cookie);
    handler->handleKey(*continueRunState.cState.cookie, key);
}

void RangeScan::handleItem(std::unique_ptr<Item> item, Source source) {
    if (source == Source::Memory) {
        incrementValueFromMemory();
    } else {
        incrementValueFromDisk();
    }
    incrementItemCount();
    Expects(continueRunState.cState.cookie);
    handler->handleItem(*continueRunState.cState.cookie, std::move(item));
}

void RangeScan::handleStatus(cb::engine_errc status) {
    if (continueRunState.cState.cookie) {
        // Only handle a status if the cookie is set.
        handler->handleStatus(*continueRunState.cState.cookie, status);
    } else {
        // No other error should be here. handleStatus maybe called when
        // continue task detects the task is in state cancelled and it
        // unconditionally calls handleStatus. If there is no waiting
        // continue (no cookie) the status can be dropped
        Expects(status == cb::engine_errc::range_scan_cancelled);
    }
}

void RangeScan::incrementItemCount() {
    ++continueRunState.itemCount;
    ++totalKeys;
}

void RangeScan::incrementValueFromMemory() {
    ++totalValuesFromMemory;
}

void RangeScan::incrementValueFromDisk() {
    ++totalValuesFromDisk;
}

bool RangeScan::areLimitsExceeded() const {
    if (continueRunState.cState.limits.itemLimit) {
        return continueRunState.itemCount >=
               continueRunState.cState.limits.itemLimit;
    } else if (continueRunState.cState.limits.timeLimit.count()) {
        return now() >= continueRunState.scanContinueDeadline;
    }
    return false;
}

bool RangeScan::skipItem() {
    return isSampling() && !distribution(*prng);
}

bool RangeScan::isTotalLimitReached() const {
    return totalLimit && (totalKeys >= totalLimit);
}

bool RangeScan::isSampling() const {
    return prng != nullptr;
}

bool RangeScan::isVbucketScannable(const VBucket& vb) const {
    return vb.getState() == vbucket_state_active &&
           vb.failovers->getLatestUUID() == vbUuid;
}

std::function<std::chrono::steady_clock::time_point()> RangeScan::now = []() {
    return std::chrono::steady_clock::now();
};

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
    addStat("total_keys", totalKeys);
    addStat("total_items_from_memory", totalValuesFromMemory);
    addStat("total_items_from_disk", totalValuesFromDisk);
    addStat("total_limit", totalLimit);

    addStat("crs_item_count", continueRunState.itemCount);
    addStat("crs_cookie", continueRunState.cState.cookie);
    addStat("crs_item_limit", continueRunState.cState.limits.itemLimit);
    addStat("crs_time_limit", continueRunState.cState.limits.timeLimit.count());

    // copy state and then addStat the copy
    auto cs = *continueState.rlock();
    addStat("state", fmt::format("{}", cs.state));
    addStat("cookie", cs.cookie);
    addStat("item_limit", cs.limits.itemLimit);
    addStat("time_limit", cs.limits.timeLimit.count());

    if (isSampling()) {
        addStat("dist_p", distribution.p());
    }
}

void RangeScan::dump(std::ostream& os) const {
    // copy state then print, avoiding invoking ostream whilst locked
    auto cs = *continueState.rlock();
    fmt::print(os,
               "RangeScan: uuid:{}, {}, vbuuid:{}, range:({},{}), mode:{}, "
               "queued:{}, totalKeys:{} values m:{}, d:{}, totalLimit:{}, "
               "crs_itemCount:{}, crs_cookie:{}, crs_item_limit:{}, "
               "crs_time_limit:{}, crs_deadline:{}, cs.state:{}, cs.cookie:{}, "
               "cs.itemLimit:{}, cs.timeLimit:{}",
               uuid,
               vbid,
               vbUuid,
               cb::UserDataView(start.to_string()),
               cb::UserDataView(end.to_string()),
               (keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value"),
               queued,
               totalKeys,
               totalValuesFromMemory,
               totalValuesFromDisk,
               totalLimit,
               continueRunState.itemCount,
               reinterpret_cast<const void*>(continueRunState.cState.cookie),
               continueRunState.cState.limits.itemLimit,
               continueRunState.cState.limits.timeLimit.count(),
               continueRunState.scanContinueDeadline.time_since_epoch(),
               cs.state,
               reinterpret_cast<const void*>(cs.cookie),
               cs.limits.itemLimit,
               cs.limits.timeLimit.count());

    if (isSampling()) {
        fmt::print(os, ", distribution(p:{})", distribution.p());
    }
    fmt::print(os, "\n");
}

RangeScan::ContinueRunState::ContinueRunState() = default;
RangeScan::ContinueRunState::ContinueRunState(const ContinueState& cs)
    : cState(cs),
      itemCount(0),
      scanContinueDeadline(now() + cs.limits.timeLimit) {
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
