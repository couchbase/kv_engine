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
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig,
        std::string name)
    : start(std::move(start)),
      end(std::move(end)),
      vbUuid(vbucket.failovers->getLatestUUID()),
      handler(std::move(handler)),
      resourceTracker(bucket.getKVStoreScanTracker()),
      name(std::move(name)),
      vbid(vbucket.getId()),
      keyOnly(keyOnly) {
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

    EP_LOG_DEBUG("{}: {} created. cid:{}, mode:{}{}{}",
                 cookie.getConnectionId(),
                 getLogId(),
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
    createTime = now();
}

RangeScan::~RangeScan() {
    resourceTracker.decrNumRunningRangeScans();

    fmt::memory_buffer valueScanStats;
    if (keyOnly == cb::rangescan::KeyOnly::No) {
        // format the value read stats
        fmt::format_to(std::back_inserter(valueScanStats),
                       ", values-mem:{}, values-disk:{}",
                       totalValuesFromMemory,
                       totalValuesFromDisk);
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now() - createTime)
                            .count();

    auto cs = *continueState.rlock();

    EP_LOG_DEBUG(
            "{} finished in {} status:{}, after {}ms, continued:{}, keys:{}{}",
            getLogId(),
            cs.state,
            cs.finalStatus,
            duration,
            continueCount,
            totalKeys,
            std::string_view{valueScanStats.data(), valueScanStats.size()});

    // All waiting cookies must of been notified before we destruct. This should
    // be null as the cookie is "taken" out of the object by the I/O task.
    if (cs.cookie) {
        EP_LOG_WARN("{} destruct but cookie should be nullptr {}",
                    getLogId(),
                    reinterpret_cast<const void*>(cs.cookie));
    }
#ifdef CB_DEVELOPMENT_ASSERTS
    Expects(!cs.cookie);
#endif
}

std::string RangeScan::getLogId() const {
    if (name.empty()) {
        return fmt::format("RangeScan {} {}", getVBucketId(), uuid);
    }
    return fmt::format("RangeScan {} {} {}", getVBucketId(), uuid, name);
}

cb::rangescan::Id RangeScan::createScan(
        CookieIface& cookie,
        EPBucket& bucket,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
    auto privStatus = hasPrivilege(cookie, bucket.getEPEngine());
    if (privStatus != cb::engine_errc::success) {
        throw cb::engine_error(
                privStatus,
                fmt::format("{} createScan hasPrivilege returned failure",
                            getLogId()));
    }

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
                fmt::format(
                        "{} createScan initByIdScanContext returned nullptr",
                        getLogId()));
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
                                   fmt::format("{} createScan "
                                               "snapshotReqs vbUuid mismatch "
                                               "res:{} vs vbstate:{}",
                                               getLogId(),
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
                        fmt::format("{} createScan snapshotReqs not met "
                                    "seqno:{} not stored",
                                    getLogId(),
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
                        fmt::format("{} createScan cannot sample empty cid:{}, "
                                    "items:{}, samples:{}",
                                    getLogId(),
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
                    fmt::format("{} createScan no collection stats for "
                                "sampling cid:{}",
                                getLogId(),
                                start.getDocKey().getCollectionID()));
        } else {
            throw cb::engine_error(
                    cb::engine_errc::failed,
                    fmt::format("{} createScan failed reading collection stats "
                                "for sampling cid:{}",
                                getLogId(),
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
        throw cb::engine_error(
                cb::engine_errc::no_such_key,
                fmt::format("{} tryAndScanOneKey no keys in range",
                            getLogId()));
    case ScanStatus::Cancelled:
    case ScanStatus::Failed:
        throw cb::engine_error(cb::engine_errc::failed,
                               fmt::format("{} tryAndScanOneKey scan failed {}",
                                           getLogId(),
                                           status));
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
    return engine.checkCollectionAccess(
            cookie,
            {},
            cb::rbac::Privilege::SystemCollectionLookup,
            cb::rbac::Privilege::RangeScan,
            start.getDocKey().getCollectionID());
}

RangeScan::ContinueIOThreadResult RangeScan::prepareToRunOnContinueTask() {
    // Take a copy of the ContinueState and clear the cookie
    auto cs = continueState.withWLock([](auto& cs) {
        auto state = cs;
        cs.cookie = nullptr; // This cookie (if any) is now taken by the IO task
        return state;
    });

    switch (cs.state) {
    case State::Cancelled:
        cancelOnIOThread(cb::engine_errc::range_scan_cancelled);
    case State::Completed:
        return {cs.finalStatus, cs.cookie};
    case State::Continuing:
        // setup the runstate for this continuation of the scan
        continueRunState.setup(cs);
        return {cb::engine_errc::range_scan_more, cs.cookie};
    case State::Idle:
        Expects(false);
    }

    folly::assume_unreachable();
}

std::unique_ptr<RangeScanContinueResult>
RangeScan::continuePartialOnFrontendThread(CookieIface& client) {
    continueState.withWLock([&client, this](auto& cs) {
        switch (cs.state) {
        case State::Idle:
        case State::Cancelled:
        case State::Completed:
            throw std::runtime_error(
                    fmt::format("RangeScan::continuePartialOnFrontendThread "
                                "{} invalid state:{}",
                                getLogId(),
                                cs.state));
        // Only permitted when already Continuing
        case State::Continuing:
            cs.setupForContinuePartial(client);
            break;
        }
    });
    return handler->continuePartialOnFrontendThread();
}

std::unique_ptr<RangeScanContinueResult>
RangeScan::continueMoreOnFrontendThread() {
    return handler->continueMoreOnFrontendThread();
}

std::unique_ptr<RangeScanContinueResult> RangeScan::completeOnFrontendThread() {
    return handler->completeOnFrontendThread();
}

std::unique_ptr<RangeScanContinueResult> RangeScan::cancelOnFrontendThread() {
    return handler->cancelOnFrontendThread();
}

cb::engine_errc RangeScan::continueOnIOThread(KVStoreIface& kvstore) {
    EP_LOG_DEBUG("{} continueOnIOThread", getLogId());
    auto scanStatus = kvstore.scan(*scanCtx);
    cb::engine_errc engineStatus = cb::engine_errc::success;
    switch (scanStatus) {
    case ScanStatus::Yield: {
        engineStatus = continueRunState.getYieldStatusCodeAndReset();
        break;
    }
    case ScanStatus::Success:
        engineStatus = cb::engine_errc::range_scan_complete;
        break;
    case ScanStatus::Failed:
        // Scan cannot continue due to KVStore failure
        engineStatus = cb::engine_errc::failed;
        break;
    case ScanStatus::Cancelled:
        // Scan cannot continue, it has been cancelled, e.g. the "handler"
        // spotted the vbucket is no longer compatible. In this case an
        // appropriate engine_errc has already been passed to
        // handler::handleStatus and onto the continueRunState at the point it
        // detected the issue.
        engineStatus = continueRunState.getCancelledStatus();
        Expects(engineStatus != cb::engine_errc::success);
        break;
    }

    EP_LOG_DEBUG("{} continueOnIOThread complete {} {}",
                 getLogId(),
                 engineStatus,
                 scanStatus);
    return engineStatus;
}

void RangeScan::cancelOnIOThread(cb::engine_errc status) {
    // This status will get returned via notifyIOComplete
    continueRunState.setCancelledStatus(status);
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

void RangeScan::setStateIdle() {
    continueState.withWLock([this](auto& cs) {
        switch (cs.state) {
        case State::Cancelled:
        case State::Completed:
        case State::Idle:
            throw std::runtime_error(
                    fmt::format("RangeScan::setStateIdle {} invalid state:{}",
                                getLogId(),
                                cs.state));
        case State::Continuing:
            cs.setupForIdle();
            break;
        }
    });
}

void RangeScan::setStateContinuing(CookieIface& client,
                                   size_t limit,
                                   std::chrono::milliseconds timeLimit,
                                   size_t byteLimit) {
    continueCount++;
    continueState.withWLock(
            [&client, limit, timeLimit, byteLimit, this](auto& cs) {
                switch (cs.state) {
                case State::Continuing:
                case State::Cancelled:
                case State::Completed:
                    throw std::runtime_error(fmt::format(
                            "RangeScan::setStateContinuing {} invalid state:{}",
                            getLogId(),
                            cs.state));
                case State::Idle:
                    cs.setupForContinue(client, limit, timeLimit, byteLimit);
                    break;
                }
            });
}

void RangeScan::setStateCancelled(cb::engine_errc finalStatus) {
    continueState.withWLock([finalStatus, this](auto& cs) {
        switch (cs.state) {
        case State::Cancelled:
        case State::Completed:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateCancelled {} invalid state:{}",
                    getLogId(),
                    cs.state));
        case State::Idle:
        case State::Continuing:
            cs.setupForCancel(finalStatus);
            break;
        }
    });
}

void RangeScan::setStateCompleted() {
    continueState.withWLock([this](auto& cs) {
        switch (cs.state) {
        case State::Completed:
        case State::Cancelled:
        case State::Idle:
            throw std::runtime_error(fmt::format(
                    "RangeScan::setStateCompleted {} invalid state:{}",
                    getLogId(),
                    cs.state));
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

void RangeScan::handleKey(DocKey key) {
    incrementItemCounters(key.size());
    switch (handler->handleKey(key)) {
    case RangeScanDataHandler::Status::OK:
        break;
    case RangeScanDataHandler::Status::ExceededBufferLimit:
        continueRunState.setExceededBufferLimit();
        break;
    case RangeScanDataHandler::Status::Throttle:
        continueRunState.setThrottled();
        break;
    }
}

void RangeScan::handleItem(std::unique_ptr<Item> item, Source source) {
    if (source == Source::Memory) {
        incrementValueFromMemory();
    } else {
        incrementValueFromDisk();
    }

    // Disk items should be in the correct state, but memory sourced values may
    // need to be decompressed. MB-55225
    if (!continueRunState.isSnappyEnabled()) {
        // no-op if already decompressed.
        item->decompressValue();
    }

    incrementItemCounters(item->getNBytes() + item->getKey().size());

    switch (handler->handleItem(std::move(item))) {
    case RangeScanDataHandler::Status::OK:
        break;
    case RangeScanDataHandler::Status::ExceededBufferLimit:
        continueRunState.setExceededBufferLimit();
        break;
    case RangeScanDataHandler::Status::Throttle:
        continueRunState.setThrottled();
        break;
    }
}

void RangeScan::setUnknownCollectionManifestUid(uint64_t manifestUid) {
    // Save the manifest-ID so that the frontend can correctly respond
    continueRunState.setManifestUid(manifestUid);
}

uint64_t RangeScan::getManifestUid() const {
    return continueRunState.getManifestUid();
}

void RangeScan::incrementItemCounters(size_t size) {
    continueRunState.accountForItem(size);
    ++totalKeys;
}

void RangeScan::incrementValueFromMemory() {
    ++totalValuesFromMemory;
}

void RangeScan::incrementValueFromDisk() {
    ++totalValuesFromDisk;
}

bool RangeScan::shouldScanYield() const {
    return continueRunState.shouldScanYield();
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
    addStat("continues", continueCount);

    continueRunState.addStats(std::string_view{prefix.data(), prefix.size()},
                              collector);

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

    if (!name.empty()) {
        addStat("name", name);
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
               "{} vbuuid:{}, created:{}. range:({},{}), "
               "mode:{}, queued:{}, totalKeys:{} values m:{}, d:{}, "
               "crs{{{}}}, cs{{{}}} name",
               scan.getLogId(),
               scan.vbUuid,
               scan.createTime.time_since_epoch().count(),
               cb::UserDataView(scan.start.to_string()),
               cb::UserDataView(scan.end.to_string()),
               (scan.keyOnly == cb::rangescan::KeyOnly::Yes ? "key" : "value"),
               scan.queued,
               scan.totalKeys,
               scan.totalValuesFromMemory,
               scan.totalValuesFromDisk,
               scan.getContinueCount(),
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
               "items:{}, time:{}ms, bytes:{} deadline:{}",
               limits.itemLimit,
               limits.timeLimit.count(),
               limits.byteLimit,
               limits.scanContinueDeadline.time_since_epoch());
    return os;
}

void RangeScan::ContinueState::dump() const {
    std::cerr << *this;
}

std::ostream& operator<<(std::ostream& os,
                         const RangeScan::ContinueState& continueState) {
    fmt::print(os,
               "cookie:{}, {}, limits:{{{}}}, finalStatus:{}",
               static_cast<const void*>(continueState.cookie),
               continueState.state,
               continueState.limits,
               continueState.finalStatus);

    return os;
}

std::ostream& operator<<(std::ostream& os,
                         const RangeScan::ContinueRunState& state) {
    return os << state.to_string();
}

CollectionID RangeScan::getCollectionID() const {
    return start.getDocKey().getCollectionID();
}

void RangeScan::logForTimeout() const {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now() - createTime)
                            .count();
    EP_LOG_WARN(
            "{} timeout after {}ms, continues:{}, keys:{}, values-mem:{}, "
            "values-disk:{}",
            getLogId(),
            duration,
            continueCount,
            totalKeys,
            totalValuesFromMemory,
            totalValuesFromDisk);
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
    limits.byteLimit = byteLimit;
    // compute the deadline only once and keep it for all runs of this continue
    limits.scanContinueDeadline = now() + timeLimit;

    // record that a timeLimit is set, so we know to check the continue lifetime
    // against the deadline
    limits.timeLimit = timeLimit;
}

void RangeScan::ContinueState::setupForContinuePartial(CookieIface& c) {
    // Just copy the cookie for the next run of the I/O task
    cookie = &c;
}

void RangeScan::ContinueState::setupForComplete() {
    *this = {};
    state = State::Completed;
    this->finalStatus = cb::engine_errc::range_scan_complete;
}

void RangeScan::ContinueState::setupForCancel(cb::engine_errc finalStatus) {
    state = State::Cancelled;
    this->finalStatus = finalStatus;
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

void RangeScan::ContinueRunState::setup(const ContinueState& cs) {
    // Should not be trying to setup the run state without a cookie
    Expects(cs.cookie);

    limits = cs.limits;
    // setup the compression state from the client's datatype support
    snappyEnabled =
            cs.cookie->isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY);
}

cb::engine_errc RangeScan::ContinueRunState::getYieldStatusCodeAndReset() {
    if (isItemLimitExceeded() || isTimeLimitExceeded() ||
        isByteLimitExceeded() || isThrottled()) {
        // This first case is for yield which must not automatically continue.
        // Only the client can continue this scan. Signal this case using
        // range_scan_more. The worker thread will send any remaining scanned
        // data and set the scan back to Idle.
        //
        // In this case we can reset ContinueRunState back to default settings,
        // ready for the a future continue.
        *this = {};
        return cb::engine_errc::range_scan_more;
    } else if (hasExceededBufferLimit()) {
        // This second case is for a yield which must "auto" continue. Signal
        // this case with success. The worker thread will send the scanned data
        // and reschedule the IO task to run this scan.
        //
        // In this case we need retain how many keys/bytes have been read and
        // the deadline. Only clear the buffer limit flag.
        exceededBufferLimit = false;

        return cb::engine_errc::success;
    }
    folly::assume_unreachable();
}

bool RangeScan::ContinueRunState::hasExceededBufferLimit() const {
    return exceededBufferLimit;
}

void RangeScan::ContinueRunState::setExceededBufferLimit() {
    exceededBufferLimit = true;
}

bool RangeScan::ContinueRunState::isThrottled() const {
    return limitByThrottle;
}

void RangeScan::ContinueRunState::setThrottled() {
    limitByThrottle = true;
}

void RangeScan::ContinueRunState::accountForItem(size_t size) {
    ++itemCount;
    byteCount += size;
}

bool RangeScan::ContinueRunState::shouldScanYield() const {
    return isItemLimitExceeded() || isTimeLimitExceeded() ||
           isByteLimitExceeded() || isThrottled() || hasExceededBufferLimit();
}

bool RangeScan::ContinueRunState::isItemLimitExceeded() const {
    return limits.itemLimit && (itemCount >= limits.itemLimit);
}

bool RangeScan::ContinueRunState::isTimeLimitExceeded() const {
    return limits.timeLimit.count() && now() >= limits.scanContinueDeadline;
}

bool RangeScan::ContinueRunState::isByteLimitExceeded() const {
    return limits.byteLimit && byteCount >= limits.byteLimit;
}

void RangeScan::ContinueRunState::setManifestUid(uint64_t uid) {
    manifestUid = uid;
}

uint64_t RangeScan::ContinueRunState::getManifestUid() const {
    return manifestUid;
}

void RangeScan::ContinueRunState::setCancelledStatus(cb::engine_errc status) {
    cancelledStatus = status;
}

cb::engine_errc RangeScan::ContinueRunState::getCancelledStatus() const {
    return cancelledStatus;
}

bool RangeScan::ContinueRunState::isSnappyEnabled() const {
    return snappyEnabled;
}

void RangeScan::ContinueRunState::addStats(
        std::string_view prefix, const StatCollector& collector) const {
    const auto addStat = [&prefix, &collector](const auto& statKey,
                                               auto statValue) {
        fmt::memory_buffer key;
        format_to(std::back_inserter(key), "{}:{}", prefix, statKey);
        collector.addStat(std::string_view(key.data(), key.size()), statValue);
    };
    addStat("crs_item_count", itemCount);
    addStat("crs_snappy", snappyEnabled);
    addStat("crs_item_limit", limits.itemLimit);
    addStat("crs_time_limit", limits.timeLimit.count());
    addStat("crs_byte_limit", limits.byteLimit);
    addStat("crs_deadline",
            fmt::format("{}", limits.scanContinueDeadline.time_since_epoch()));
    addStat("crs_exceeded_buffer", exceededBufferLimit);
    addStat("crs_throttled", limitByThrottle);
}

std::string RangeScan::ContinueRunState::to_string() const {
    return fmt::format(
            "{}, itemCount:{}, byteCount:{}, snappy:{}, limitByThrottle:{}, "
            "exceededBufferLimit:{}, cancelStatus:{}, manifestUid:{}",
            limits,
            itemCount,
            byteCount,
            snappyEnabled,
            limitByThrottle,
            exceededBufferLimit,
            cancelledStatus,
            manifestUid);
}

void RangeScan::ContinueRunState::dump() const {
    std::cerr << *this;
}
