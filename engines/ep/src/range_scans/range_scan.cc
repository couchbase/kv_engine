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
#include "dcp/dcpconnmap.h"
#include "ep_bucket.h"
#include "ep_engine.h"
#include "failover-table.h"
#include "item.h"
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
        CookieIface& cookie,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig)
    : start(std::move(start)),
      end(std::move(end)),
      vbUuid(vbucket.failovers->getLatestUUID()),
      handler(std::move(handler)),
      resourceTracker(bucket.getKVStoreScanTracker()),
      vbid(vbucket.getId()),
      keyOnly(keyOnly) {
    if (!resourceTracker.canCreateRangeScan()) {
        throw cb::engine_error(cb::engine_errc::too_busy,
                               fmt::format("RangeScan::createScan {} denied by "
                                           "BackfillTrackingIface",
                                           getVBucketId()));
    }

    try {
        uuid = createScan(cookie, bucket, snapshotReqs, samplingConfig);
        createTime = now();

    } catch (...) {
        // Failed to create the scan, so we no longer need to count it
        resourceTracker.decrNumRunningRangeScans();
        throw;
    }

    fmt::memory_buffer snapshotLog, samplingLog;
    if (snapshotReqs) {
        fmt::format_to(std::back_inserter(snapshotLog),
                       ", snapshot_reqs:uuid:{}, seqno:{}, strict:{}",
                       snapshotReqs->vbUuid,
                       snapshotReqs->seqno,
                       snapshotReqs->seqnoMustBeInSnapshot);
        if (snapshotReqs->timeout) {
            fmt::format_to(std::back_inserter(snapshotLog),
                           ", timeout:{}",
                           snapshotReqs->timeout.value());
        }
    }

    if (samplingConfig) {
        fmt::format_to(std::back_inserter(samplingLog),
                       ", sampling_config:samples:{}, seed:{}",
                       samplingConfig->samples,
                       samplingConfig->seed);
        if (prng) {
            fmt::format_to(std::back_inserter(samplingLog),
                           ", prng:yes, distribution:{}",
                           distribution.p());
        } else {
            fmt::format_to(std::back_inserter(samplingLog), ", prng:no");
        }
    }

    EP_LOG_INFO("{}: {} RangeScan {} created. cid:{}, mode:{}{}{}",
                cookie.getConnectionId(),
                getVBucketId(),
                uuid,
                this->start.getDocKey().getCollectionID(),
                keyOnly == cb::rangescan::KeyOnly::Yes ? "keys" : "values",
                std::string_view{snapshotLog.data(), snapshotLog.size()},
                std::string_view{samplingLog.data(), samplingLog.size()});
}

RangeScan::RangeScan(cb::rangescan::Id id, KVStoreScanTracker& resourceTracker)
    : uuid(id),
      start(StoredDocKey("start", CollectionID::Default)),
      end(StoredDocKey("end", CollectionID::Default)),
      resourceTracker(resourceTracker) {
    if (!resourceTracker.canCreateRangeScan()) {
        throw cb::engine_error(cb::engine_errc::temporary_failure,
                               fmt::format("RangeScan::createScan {} denied by "
                                           "BackfillTrackingIface",
                                           getVBucketId()));
    }
    createTime = now();
}

RangeScan::~RangeScan() {
    resourceTracker.decrNumRunningRangeScans();

    fmt::memory_buffer valueScanStats;
    if (keyOnly == cb::rangescan::KeyOnly::No) {
        // format the value read stats
        fmt::format_to(std::back_inserter(valueScanStats),
                       ", from:mem:{}, disk:{}",
                       totalValuesFromMemory,
                       totalValuesFromDisk);
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now() - createTime)
                            .count();

    EP_LOG_INFO(
            "{} RangeScan {} finished in state:{} {}, after {}ms, keys:{}{}",
            getVBucketId(),
            uuid,
            continueState.rlock()->state,
            continueState.rlock()->finalStatus,
            duration,
            totalKeys,
            std::string_view{valueScanStats.data(), valueScanStats.size()});
}

cb::rangescan::Id RangeScan::createScan(
        CookieIface& cookie,
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

    // We'll estimate how much gets read to use in metering
    size_t approxBytesRead{0};

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
            throw cb::engine_error(cb::engine_errc::vbuuid_not_equal,
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

        // Data read/stored as text/JSON, but this is only an approximate size
        approxBytesRead += (ft.getNumEntries() * 16) + sizeof(state.state);
    }

    if (samplingConfig) {
        const auto& handle = *scanCtx->handle.get();
        auto stats =
                bucket.getRWUnderlying(getVBucketId())
                        ->getCollectionStats(
                                handle, start.getDocKey().getCollectionID());
        if (stats.first == KVStore::GetCollectionStatsStatus::Success) {
            if (stats.second.itemCount == 0) {
                // same errc as an empty range-scan
                throw cb::engine_error(
                        cb::engine_errc::no_such_key,
                        fmt::format("RangeScan::createScan {} cannot sample "
                                    "empty cid:{}, items:{}, samples:{}",
                                    getVBucketId(),
                                    start.getDocKey().getCollectionID(),
                                    stats.second.itemCount,
                                    samplingConfig->samples));
            } else if (stats.second.itemCount > samplingConfig->samples) {
                // Create the prng so that sampling is enabled
                prng = std::make_unique<std::mt19937>(samplingConfig->seed);

                // Now we can compute the distribution and assign the first
                // sample index. Example, if asked for 999 samples and 1,000
                // keys exist then we set 0.999 as the probability.
                distribution = std::bernoulli_distribution{
                        double(samplingConfig->samples) /
                        double(stats.second.itemCount)};
            }
            // else no prng, the entire collection is now returned
        } else if (stats.first == KVStore::GetCollectionStatsStatus::NotFound) {
            // same errc as an empty range-scan
            throw cb::engine_error(
                    cb::engine_errc::no_such_key,
                    fmt::format("RangeScan::createScan {} no "
                                "collection stats for sampling cid:{}",
                                getVBucketId(),
                                start.getDocKey().getCollectionID()));
        } else {
            throw cb::engine_error(
                    cb::engine_errc::failed,
                    fmt::format("RangeScan::createScan {} failed reading "
                                "collection stats for sampling cid:{}",
                                getVBucketId(),
                                start.getDocKey().getCollectionID()));
        }

        approxBytesRead += sizeof(stats.second);
    }

    if (!samplingConfig) {
        // When not sampling, check for a key in the range (sampling works on
        // the entire collection, and we've checked the collection stats)
        approxBytesRead +=
                tryAndScanOneKey(*bucket.getRWUnderlying(getVBucketId()));
    }

    cookie.addDocumentReadBytes(approxBytesRead);

    // Generate the scan ID (which may also incur i/o)
    return boost::uuids::random_generator()();
}

size_t RangeScan::tryAndScanOneKey(KVStoreIface& kvstore) {
    struct FindMaxCommittedItem : public StatusCallback<CacheLookup> {
        void callback(CacheLookup&) override {
            // Immediately yield and the caller to scan will see the Yield
            // status and know at least one key exists
            yield();
        }
    };

    struct FailingGetValueCallback : public StatusCallback<GetValue> {
        void callback(GetValue&) override {
            // should never get here as the CacheLookup stops the scan if any
            // keys exist
            Expects(false);
        }
    };

    auto checkOneKey = kvstore.initByIdScanContext(
            std::make_unique<FailingGetValueCallback>(),
            std::make_unique<FindMaxCommittedItem>(),
            getVBucketId(),
            {ByIdRange{start, end}},
            DocumentFilter::NO_DELETES,
            ValueFilter::KEYS_ONLY,
            std::move(scanCtx->handle));

    auto status = kvstore.scan(*checkOneKey);

    switch (status) {
    case ScanStatus::Success:
        throw cb::engine_error(cb::engine_errc::no_such_key,
                               fmt::format("RangeScan::createScan {} no "
                                           "keys in range",
                                           getVBucketId()));
    case ScanStatus::Cancelled:
    case ScanStatus::Failed:
        throw cb::engine_error(
                cb::engine_errc::failed,
                fmt::format("RangeScan::createScan {} scan failed "
                            "{}",
                            status,
                            getVBucketId()));
    case ScanStatus::Yield: {
        // At least 1 key, return the handle and the scan can run from the user
        // initiated range-scan-continue
        scanCtx->handle = std::move(checkOneKey->handle);
    }
    }
    return checkOneKey->diskBytesRead;
}

cb::engine_errc RangeScan::hasPrivilege(
        CookieIface& cookie, const EventuallyPersistentEngine& engine) {
    return engine.checkPrivilege(cookie,
                                 cb::rbac::Privilege::RangeScan,
                                 start.getDocKey().getCollectionID());
}

cb::engine_errc RangeScan::prepareToContinueOnIOThread() {
    // continue works on a copy of the state.
    continueRunState = continueState.withWLock([](auto& cs) {
        auto state = cs;
        cs.cookie = nullptr; // This cookie is now 'used'
        return state;
    });

    // Only attempt scan when !cancelled
    if (continueRunState.cState.state == State::Cancelled) {
        // ensure the client/cookie sees cancelled
        handleStatus(cb::engine_errc::range_scan_cancelled);
        // ignoring the handleStatus return value as scan-cancelled is the
        // outcome here.
        return cb::engine_errc::range_scan_cancelled;
    }

    Expects(continueRunState.cState.state == State::Continuing);
    return cb::engine_errc::range_scan_more;
}

cb::engine_errc RangeScan::continueOnIOThread(KVStoreIface& kvstore) {
    EP_LOG_DEBUG(
            "RangeScan {} continueOnIOThread for {}", uuid, getVBucketId());
    auto status = kvstore.scan(*scanCtx);

    switch (status) {
    case ScanStatus::Yield: {
        // Scan reached a limit and has yielded.
        // Set to idle, which will send success to the handler
        auto status = tryAndSetStateIdle(cb::engine_errc::range_scan_more);

        // For RangeScan we have already consumed the last key, so we adjust the
        // startKey so we continue from next.
        scanCtx->ranges[0].startKey.append(0);

        // return range_scan_more status so scan can continue
        return status == cb::engine_errc::success
                       ? cb::engine_errc::range_scan_more
                       : status;
    }
    case ScanStatus::Success:
        // Scan has reached the end
        if (handleStatus(cb::engine_errc::range_scan_complete)) {
            return cb::engine_errc::success;
        }
        return cb::engine_errc::range_scan_cancelled;
    case ScanStatus::Failed:
        // Scan cannot continue due to KVStore failure
        handleStatus(cb::engine_errc::failed);
        // ignoring the handleStatus return value as cancelled is the outcome
        // here.
    case ScanStatus::Cancelled:
        // Scan cannot continue, it has been cancelled, e.g. the "handler"
        // spotted the vbucket is no longer compatible. In this case an
        // appropriate engine_errc has already been passed to
        // handler::handleStatus at the point it detected the issue.

        // For both Failed/Cancelled return range_scan_cancelled so the caller
        // knows this scan is to be cancelled (leading to the destruction of
        // this object)
        return cb::engine_errc::range_scan_cancelled;
    }
    throw std::runtime_error(
            "RangeScan::continueOnIOThread all case statements should return a "
            "status");
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

bool RangeScan::isCompleted() const {
    return continueState.rlock()->state == State::Completed;
}

cb::engine_errc RangeScan::tryAndSetStateIdle(cb::engine_errc status) {
    if (setStateIdle(status)) {
        return cb::engine_errc::success;
    }
    return cb::engine_errc::range_scan_cancelled;
}

bool RangeScan::setStateIdle(cb::engine_errc status) {
    // Changing to Idle implies a successful Continue.
    // range_scan_complete is the end of the scan
    // range_scan_more is a 'pause' due to limits/yield of the task
    // Note status is overridden if state is now Cancelled
    Expects(status == cb::engine_errc::range_scan_more ||
            status == cb::engine_errc::range_scan_complete);

    auto* cookie = continueRunState.cState.cookie;
    Expects(cookie);
    continueRunState.cState.cookie = nullptr;

    continueState.withWLock([&status](auto& cs) {
        switch (cs.state) {
        case State::Idle:
        case State::Completed:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateIdle invalid state:{}", cs.state));
        case State::Cancelled:
            status = cb::engine_errc::range_scan_cancelled;
            break;
        case State::Continuing:
            cs.setupForIdle();
            break;
        }
    });

    // 1) The ordering here is deliberate, set the status after the cs.state
    // update so there's no chance a client sees 'success' then fails to
    // continue again (because you cannot continue a Continuing scan)
    // 2) The use of handler->handleStatus direct (instead of ::handleStatus) is
    // important as this scan is now in the idle (or cancelled state). When idle
    // the scan can be continued again so continueRunState is not safe to use,
    // so instead use the cookie copied before the state change.
    handler->handleStatus(*cookie, status);

    return status != cb::engine_errc::range_scan_cancelled;
}

void RangeScan::setStateContinuing(CookieIface& client,
                                   size_t limit,
                                   std::chrono::milliseconds timeLimit,
                                   size_t byteLimit) {
    continueState.withWLock([&client, limit, timeLimit, byteLimit](auto& cs) {
        switch (cs.state) {
        case State::Continuing:
        case State::Cancelled:
        case State::Completed:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateContinuing invalid state:{}",
                    cs.state));
        case State::Idle:
            cs.setupForContinue(client, limit, timeLimit, byteLimit);
            break;
        }
    });
}

void RangeScan::setStateCancelled() {
    continueState.withWLock([](auto& cs) {
        switch (cs.state) {
        case State::Cancelled:
        case State::Completed:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateCancelled invalid state:{}", cs.state));
        case State::Idle:
        case State::Continuing:
            cs.setupForCancel();
            break;
        }
    });
}

void RangeScan::setStateCompleted() {
    continueState.withWLock([](auto& cs) {
        switch (cs.state) {
        case State::Completed:
        case State::Cancelled:
        case State::Idle:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateCompleted invalid state:{}", cs.state));
        case State::Continuing:
            cs.setupForComplete();
            break;
        }
    });
}

std::chrono::seconds RangeScan::getRemainingTime(
        std::chrono::seconds timeLimit) {
    // 16:00 + 30 seconds limit = 16:00:30
    // Then subtract now()  e.g. if now is 16:01, return 0 seconds
    // or if now() is 16:00:20, return 10 seconds.
    return std::max(std::chrono::seconds(0),
                    std::chrono::duration_cast<std::chrono::seconds>(
                            (createTime + timeLimit) - now()));
}

bool RangeScan::handleKey(DocKey key) {
    incrementItemCounters(key.size());
    Expects(continueRunState.cState.cookie);
    switch (handler->handleKey(*continueRunState.cState.cookie, key)) {
    case RangeScanDataHandler::Status::OK:
        break;
    case RangeScanDataHandler::Status::Throttle:
        continueRunState.limitByThrottle = true;
        break;
    case RangeScanDataHandler::Status::Disconnected:
        return false;
    }
    return true;
}

bool RangeScan::handleItem(std::unique_ptr<Item> item, Source source) {
    Expects(continueRunState.cState.cookie);

    if (source == Source::Memory) {
        incrementValueFromMemory();
    } else {
        incrementValueFromDisk();
    }

    // Disk items should be in the correct state, but memory sourced values may
    // need to be decompressed. MB-55225
    if (!continueRunState.cState.cookie->isDatatypeSupported(
                PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        // no-op if already decompressed.
        item->decompressValue();
    }

    incrementItemCounters(item->getNBytes() + item->getKey().size());

    switch (handler->handleItem(*continueRunState.cState.cookie,
                                std::move(item))) {
    case RangeScanDataHandler::Status::OK:
        break;
    case RangeScanDataHandler::Status::Throttle:
        continueRunState.limitByThrottle = true;
        break;
    case RangeScanDataHandler::Status::Disconnected:
        return false;
    }
    return true;
}

bool RangeScan::handleStatus(cb::engine_errc status) {
    bool rv{true};
    if (continueRunState.cState.cookie) {
        // Only handle a status if the cookie is set.
        switch (handler->handleStatus(*continueRunState.cState.cookie,
                                      status)) {
        case RangeScanDataHandler::Status::Throttle:
            // Don't expect handleStatus to do the throttling
            Expects(false);
        case RangeScanDataHandler::Status::Disconnected:
            rv = false;
            break;
        case RangeScanDataHandler::Status::OK:
            break;
        }
        // The cookie can only receive a status once, for safety clear the
        // cookie now. Code inspection and current testing deems this
        // unnecessary but it is a safer approach for any future change or
        // missed test coverage
        continueRunState.cState.cookie = nullptr;
        continueState.wlock()->finalStatus = status;
    } else {
        // No other error should be here. handleStatus maybe called when
        // continue task detects the task is in state cancelled and it
        // unconditionally calls handleStatus. If there is no waiting
        // continue (no cookie) the status can be dropped
        Expects(status == cb::engine_errc::range_scan_cancelled);
    }
    return rv;
}

void RangeScan::handleUnknownCollection(uint64_t manifestUid) {
    Expects(continueRunState.cState.cookie);
    handler->handleUnknownCollection(*continueRunState.cState.cookie,
                                     manifestUid);
}

void RangeScan::incrementItemCounters(size_t size) {
    ++continueRunState.itemCount;
    continueRunState.byteCount += size;
    ++totalKeys;
}

void RangeScan::incrementValueFromMemory() {
    ++totalValuesFromMemory;
}

void RangeScan::incrementValueFromDisk() {
    ++totalValuesFromDisk;
}

bool RangeScan::areLimitsExceeded() const {
    if (continueRunState.cState.limits.itemLimit &&
        continueRunState.itemCount >=
                continueRunState.cState.limits.itemLimit) {
        return true;
    }

    if (continueRunState.cState.limits.timeLimit.count() &&
        now() >= continueRunState.scanContinueDeadline) {
        return true;
    }

    if (continueRunState.cState.limits.byteLimit &&
        continueRunState.byteCount >=
                continueRunState.cState.limits.byteLimit) {
        return true;
    }
    return continueRunState.limitByThrottle;
}

bool RangeScan::skipItem() {
    return isSampling() && !distribution(*prng);
}

bool RangeScan::isSampling() const {
    return prng != nullptr;
}

bool RangeScan::isVbucketScannable(const VBucket& vb) const {
    return vb.getState() == vbucket_state_active &&
           vb.failovers->getLatestUUID() == vbUuid;
}

static std::function<std::chrono::steady_clock::time_point()>
getDefaultClockFunction() {
    return []() { return std::chrono::steady_clock::now(); };
}

std::function<std::chrono::steady_clock::time_point()> RangeScan::now =
        getDefaultClockFunction();

void RangeScan::resetClockFunction() {
    now = getDefaultClockFunction();
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

    addStat("create_time", createTime.time_since_epoch().count());
    addStat("vbuuid", vbUuid);
    addStat("start", cb::UserDataView(start.to_string()).getRawValue());
    addStat("end", cb::UserDataView(end.to_string()).getRawValue());
    addStat("key_value",
            keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value");
    addStat("queued", queued);
    addStat("total_keys", totalKeys);
    addStat("total_items_from_memory", totalValuesFromMemory);
    addStat("total_items_from_disk", totalValuesFromDisk);

    addStat("crs_item_count", continueRunState.itemCount);
    addStat("crs_cookie", continueRunState.cState.cookie);
    addStat("crs_item_limit", continueRunState.cState.limits.itemLimit);
    addStat("crs_time_limit", continueRunState.cState.limits.timeLimit.count());
    addStat("crs_byte_limit", continueRunState.cState.limits.byteLimit);

    // copy state and then addStat the copy
    auto cs = *continueState.rlock();
    addStat("state", fmt::format("{}", cs.state));
    addStat("cookie", cs.cookie);
    addStat("item_limit", cs.limits.itemLimit);
    addStat("time_limit", cs.limits.timeLimit.count());
    addStat("byte_limit", cs.limits.byteLimit);

    if (isSampling()) {
        addStat("dist_p", distribution.p());
    }

    handler->addStats(std::string_view{prefix.data(), prefix.size()},
                      collector);
}

void RangeScan::dump() const {
    std::cerr << *this;
}

std::ostream& operator<<(std::ostream& os, const RangeScan& scan) {
    // copy state then print, avoiding invoking ostream whilst locked
    auto cs = *scan.continueState.rlock();
    fmt::print(os,
               "RangeScan: uuid:{}, {}, vbuuid:{}, created:{}. range:({},{}), "
               "mode:{}, queued:{}, totalKeys:{} values m:{}, d:{}, "
               "crs{{{}}}, cs{{{}}}",
               scan.uuid,
               scan.vbid,
               scan.vbUuid,
               scan.createTime.time_since_epoch().count(),
               cb::UserDataView(scan.start.to_string()),
               cb::UserDataView(scan.end.to_string()),
               (scan.keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value"),
               scan.queued,
               scan.totalKeys,
               scan.totalValuesFromMemory,
               scan.totalValuesFromDisk,
               scan.continueRunState,
               cs);

    if (scan.isSampling()) {
        fmt::print(os, ", distribution(p:{})", scan.distribution.p());
    }
    return os;
}

void RangeScan::ContinueLimits::dump() const {
    std::cerr << *this;
}

std::ostream& operator<<(std::ostream& os,
                         const RangeScan::ContinueLimits& limits) {
    fmt::print(os,
               "items:{}, time:{}ms, bytes:{}",
               limits.itemLimit,
               limits.timeLimit.count(),
               limits.byteLimit);
    return os;
}

void RangeScan::ContinueState::dump() const {
    std::cerr << *this;
}

std::ostream& operator<<(std::ostream& os,
                         const RangeScan::ContinueState& continueState) {
    fmt::print(os,
               "cookie:{}, {}, limits:{{{}}}",
               static_cast<const void*>(continueState.cookie),
               continueState.state,
               continueState.limits);
    return os;
}

void RangeScan::ContinueRunState::dump() const {
    std::cerr << *this;
}

std::ostream& operator<<(std::ostream& os,
                         const RangeScan::ContinueRunState& state) {
    fmt::print(os,
               "{} itemCount:{}, byteCount:{}, scanContinueDeadline:{}, "
               "limitByThrottle:{}",
               state.cState,
               state.itemCount,
               state.byteCount,
               state.scanContinueDeadline.time_since_epoch(),
               state.limitByThrottle);
    return os;
}

CollectionID RangeScan::getCollectionID() const {
    return start.getDocKey().getCollectionID();
}

void RangeScan::ContinueState::setupForIdle() {
    *this = {};
    state = State::Idle;
}

void RangeScan::ContinueState::setupForContinue(
        CookieIface& c,
        size_t limit,
        std::chrono::milliseconds timeLimit,
        size_t byteLimit) {
    state = State::Continuing;
    cookie = &c;
    limits.itemLimit = limit;
    limits.timeLimit = timeLimit;
    limits.byteLimit = byteLimit;
}

void RangeScan::ContinueState::setupForComplete() {
    *this = {};
    state = State::Completed;
}

void RangeScan::ContinueState::setupForCancel() {
    state = State::Cancelled;
}

RangeScan::ContinueRunState::ContinueRunState() = default;
RangeScan::ContinueRunState::ContinueRunState(const ContinueState& cs)
    : cState(cs),
      itemCount(0),
      scanContinueDeadline(now() + cs.limits.timeLimit),
      limitByThrottle(false) {
}

std::ostream& operator<<(std::ostream& os, const RangeScan::State& state) {
    switch (state) {
    case RangeScan::State::Idle:
        return os << "State::Idle";
    case RangeScan::State::Continuing:
        return os << "State::Continuing";
    case RangeScan::State::Cancelled:
        return os << "State::Cancelled";
    case RangeScan::State::Completed:
        return os << "State::Completed";
    }
    throw std::runtime_error(
            fmt::format("RangeScan::State ostream operator<< invalid state:{}",
                        int(state)));
}
