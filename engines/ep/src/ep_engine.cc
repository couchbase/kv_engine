/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_engine.h"
#include "ep_parameters.h"
#include "kv_bucket.h"

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/consumer.h"
#include "dcp/dcpconnmap_impl.h"
#include "dcp/flow-control-manager.h"
#include "dcp/msg_producers_border_guard.h"
#include "dcp/producer.h"
#include "dockey_validator.h"
#include "environment.h"
#include "ep_bucket.h"
#include "ep_engine_group.h"
#include "ep_engine_public.h"
#include "ep_engine_storage.h"
#include "ep_vb.h"
#include "ephemeral_bucket.h"
#include "error_handler.h"
#include "failover-table.h"
#include "file_ops_tracker.h"
#include "flusher.h"
#include "getkeys.h"
#include "hash_table_stat_visitor.h"
#include "htresizer.h"
#include "kvstore/kvstore.h"
#include "quota_sharing_item_pager.h"
#include "range_scans/range_scan_callbacks.h"
#include "stats-info.h"
#include "string_utils.h"
#include "trace_helpers.h"
#include "vb_count_visitor.h"
#include "warmup.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>
#include <cbcrypto/common.h>
#include <cbcrypto/file_utilities.h>
#include <executor/executorpool.h>
#include <folly/CancellationToken.h>
#include <hdrhistogram/hdrhistogram.h>
#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/collections.h>
#include <memcached/connection_iface.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/server_core_iface.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_arena_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/compress.h>
#include <platform/dirutils.h>
#include <platform/platform_time.h>
#include <platform/scope_timer.h>
#include <platform/split_string.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <serverless/config.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/fusion_utilities.h>
#include <utilities/logtags.h>
#include <utilities/magma_support.h>
#include <utilities/math_utilities.h>
#include <xattr/utils.h>
#include <functional>

#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using cb::tracing::Code;
using namespace std::string_view_literals;

/// Deleter used with EPHandle which sets the calling threads' engine back
/// to the previous value before the EPHandle was created.
class EPHandleReleaser {
public:
    EPHandleReleaser(EventuallyPersistentEngine* previous)
        : previous(previous) {
    }

    void operator()(const EventuallyPersistentEngine*) {
        ObjectRegistry::onSwitchThread(previous);
    }

    // The previous engine associated with the calling thread before the
    // engine owned by the unique_ptr was switched to.
    // Note: This should always be null in production - the daemon calls into
    // the engine via EngineIface, whose methods all set the called-to engine
    // as the 'current' engine, when the engine call finishes (and
    // EPHandleReleaser::operator() is invoked) we return to the previous
    // (null) engine.
    // However, unit tests can do things like call a method on EpEngine via
    // the EngineIface (which does the switching described), then call a
    // method not on that interface which doesn't perform
    // onSwitchThread(thisEngine), meaning if EPHandleReleaser::operator()
    // simply set the current engine to nullptr then unit tests would observe
    // their 'current' engine change to null after calls to EngineIface, and
    // hence would need to make a call to onSwitchThread(thisEngine)
    // after every EngineIface call, which can be verbose and error-prone.
    // As such, this class records the previous engine, and restores the
    // thread's current engine back to that on destruction.
    EventuallyPersistentEngine* previous;
};

using EPHandle = std::unique_ptr<EventuallyPersistentEngine, EPHandleReleaser>;
using ConstEPHandle =
        std::unique_ptr<const EventuallyPersistentEngine, EPHandleReleaser>;

// Unique types we store in the engine specific when a call is ewould blocked.
struct ScheduledCompactionToken {};
struct ScheduledVBucketDeleteToken {};
struct ScheduledSeqnoPersistenceToken {};

/**
 * Helper function to acquire a handle to the engine which allows access to
 * the engine while the handle is in scope.
 * @param handle pointer to the engine
 * @return EPHandle which is a unique_ptr to an EventuallyPersistentEngine
 * with a custom deleter (EPHandleReleaser) which performs the required
 * ObjectRegistry release.
 */

static inline EPHandle acquireEngine(EngineIface* handle) {
    auto ret = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    auto* previous = ObjectRegistry::onSwitchThread(ret, true);

    return {ret, {previous}};
}

static inline ConstEPHandle acquireEngine(const EngineIface* handle) {
    auto ret = reinterpret_cast<const EventuallyPersistentEngine*>(handle);
    // A const engine call, can in theory still mutate the engine in that
    // memory allocation can trigger an update of the stats counters. It's
    // difficult to express const/mutable through the thread-local engine, but
    // that is the assumption that only a limited amount of the engine may
    // mutate through the ObjectRegistry. Note with MB-23086 in-place, EPStats
    // won't be updated by general memory allocation, so the scope for changing
    // the const engine* is very much reduced.
    auto* previous = ObjectRegistry::onSwitchThread(
            const_cast<EventuallyPersistentEngine*>(ret), true);

    return {ret, {previous}};
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static cb::engine_errc sendResponse(const AddResponseFn& response,
                                    std::string_view key,
                                    std::string_view ext,
                                    std::string_view body,
                                    ValueIsJson json,
                                    cb::mcbp::Status status,
                                    uint64_t cas,
                                    CookieIface& cookie) {
    response(key, ext, body, json, status, cas, cookie);
    return cb::engine_errc::success;
}

template <typename T>
static void validate(T v, T l, T h) {
    if (v < l || v > h) {
        throw std::runtime_error("Value out of range.");
    }
}


static void checkNumeric(const char* str) {
    int i = 0;
    if (str[0] == '-') {
        i++;
    }
    for (; str[i]; i++) {
        using namespace std;
        if (!isdigit(str[i])) {
            throw std::runtime_error("Value is not numeric");
        }
    }
}

void EventuallyPersistentEngine::destroy(const bool force) {
    Expects((ObjectRegistry::getCurrentEngine() != this) &&
            "Calling thread should not have currentEngine set to the object "
            "being destroyed when calling EpEngine::destroy() as that could "
            "result in use-after-free");
    auto eng = acquireEngine(this);

    // Take a copy of the arena client before we deallocate the EPEngine object
    // - so we can unregister the client afterwards.
    auto arena = eng->getArenaMallocClient();
    cb::ArenaMalloc::switchToClient(arena);

    eng->destroyInner(force);
    delete eng.get();
    // Now unregister the arena - EpEngine has finished with it (all memory
    // should have been deallocated from it).
    cb::ArenaMalloc::unregisterClient(arena);
}

cb::unique_item_ptr EventuallyPersistentEngine::allocateItem(
        CookieIface& cookie,
        const DocKeyView& key,
        size_t nbytes,
        size_t priv_nbytes,
        uint32_t flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    auto [status, item] = acquireEngine(this)->itemAllocate(
            key, nbytes, priv_nbytes, flags, exptime, datatype, vbucket);

    if (status != cb::engine_errc::success) {
        throw cb::engine_error(status,
                               "EventuallyPersistentEngine::allocateItem: "
                               "failed to allocate item");
    }

    return std::move(item);
}

cb::engine_errc EventuallyPersistentEngine::remove(
        CookieIface& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    // Maybe upgrade Durability Level
    using namespace cb::durability;
    std::optional<Requirements> durReqs = durability;
    const auto level = durReqs ? durReqs->getLevel() : Level::None;
    const auto minLevel = kvBucket->getMinDurabilityLevel();
    if (level < minLevel) {
        // Transitioning from NormalWrite to SyncWrite?
        if (level == Level::None) {
            durReqs = Requirements(minLevel, Timeout());
        } else {
            durReqs->setLevel(minLevel);
        }
    }

    return acquireEngine(this)->removeInner(
            cookie, key, cas, vbucket, durReqs, mut_info);
}

void EventuallyPersistentEngine::release(ItemIface& itm) {
    acquireEngine(this)->itemRelease(&itm);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | HIDE_LOCKED_CAS |
            TRACK_STATISTICS);

    switch (documentStateFilter) {
    case DocStateFilter::Alive:
        break;
    case DocStateFilter::Deleted:
        // MB-23640 was caused by this bug as the frontend asked for
        // Alive and Deleted documents. The internals don't have a
        // way of requesting just deleted documents, and luckily for
        // us no part of our code is using this yet. Return an error
        // if anyone start using it
        return std::make_pair(
                cb::engine_errc::not_supported,
                cb::unique_item_ptr{nullptr, cb::ItemDeleter{this}});
    case DocStateFilter::AliveOrDeleted:
        options = static_cast<get_options_t>(options | GET_DELETED_VALUE);
        break;
    }
    return acquireEngine(this)->getInner(cookie, key, vbucket, options);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_replica(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const DocStateFilter documentStateFilter) {
    return acquireEngine(this)->getReplicaInner(
            cookie, key, vbucket, documentStateFilter);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_random_document(
        CookieIface& cookie, CollectionID cid) {
    return acquireEngine(this)->getRandomDocument(cookie, cid);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_if(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter) {
    return acquireEngine(this)->getIfInner(cookie, key, vbucket, filter);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_and_touch(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t expiry_time,
        const std::optional<cb::durability::Requirements>& durability) {
    if (durability) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::not_supported);
    }
    return acquireEngine(this)->getAndTouchInner(
            cookie, key, vbucket, expiry_time);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_locked(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        std::chrono::seconds lock_timeout) {
    return acquireEngine(this)->getLockedInner(
            cookie, key, vbucket, lock_timeout);
}

cb::engine_errc EventuallyPersistentEngine::unlock(CookieIface& cookie,
                                                   const DocKeyView& key,
                                                   Vbid vbucket,
                                                   uint64_t cas) {
    return acquireEngine(this)->unlockInner(cookie, key, vbucket, cas);
}

/**
 * generic lambda function which creates an "ExitBorderGuard" thunk - a wrapper
 * around the passed-in function which uses NoArenaGuard guard; to switch
 * away from the current engine's arena before invoking 'wrapped', then
 * switches back to the original arena after wrapped returns.
 *
 * The intended use-case of this is to invoke methods / callbacks outside
 * of ep-engine without mis-accounting memory - we need to ensure that any
 * memory allocated from inside the callback is *not* accounted to ep-engine,
 * but when the callback returns we /do/ account any subsequent allocations
 * to the given engine.
 *
 * Note this uses cb::NoArenaGuard, and not NonBucketAllocationGuard as
 * the callback can be called in contexts where the cb_malloc-level client
 * has been switched away from, but the ep-engine thread local
 * (ObjectRegistry::getCurrentEngine) has not - e.g.
 * FollyExecutorPool::doTaskQStat. This means that we need this thunk to
 * respect the currently selected malloc client (global, "no" arena, even if
 * ObjectRegistry::getCurrentEngine() is currently non-null. Otherwise we could
 * incorrectly switch back to arena associated with the current ep-engine
 * when this callback returns, overriding what the underlying caller (e.g.
 * FollyExecutorPool::doTaskQStat) actually set the arena to.
 */
auto makeExitBorderGuard = [](auto&& wrapped) {
    return [wrapped](auto&&... args) {
        cb::NoArenaGuard guard;
        return wrapped(std::forward<decltype(args)>(args)...);
    };
};

cb::engine_errc EventuallyPersistentEngine::get_stats(
        CookieIface& cookie,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat,
        const CheckYieldFn& check_yield) {
    // The AddStatFn callback may allocate memory (temporary buffers for
    // stat data) which will be de-allocated inside the server, after the
    // engine call (get_stat) has returned. As such we do not want to
    // account such memory against this bucket.
    // Create an exit border guard around the original callback.
    // Perf: use std::cref to avoid copying (and the subsequent `new` call) of
    // the input add_stat function.
    auto addStatExitBorderGuard = makeExitBorderGuard(std::cref(add_stat));

    return acquireEngine(this)->getStats(
            cookie, key, value, addStatExitBorderGuard, check_yield);
}

cb::engine_errc EventuallyPersistentEngine::get_prometheus_stats(
        const BucketStatCollector& collector,
        cb::prometheus::MetricGroup metricGroup) {
    try {
        using cb::prometheus::MetricGroup;
        switch (metricGroup) {
        case MetricGroup::High:
            return doMetricGroupHigh(collector);
        case MetricGroup::Low:
            return doMetricGroupLow(collector);
        case MetricGroup::Metering:
            return doMetricGroupMetering(collector);
        case MetricGroup::All:
            // nothing currently requests All metrics at this level, so rather
            // than leaving an unused impl to rot, defer until it is actually
            // required.
            throw std::invalid_argument(
                    "EventuallyPersistentEngine::get_prometheus_stats: "
                    "fetching group 'All' not implemented");
        }
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doMetricGroupHigh(
        const BucketStatCollector& collector) {
    cb::engine_errc status;

    doTimingStats(collector);
    doRunTimeStats(collector);

    if (status = doEngineStatsHighCardinality(
                collector, cb::config::ExcludeWhenValueIsDefaultValue::Yes);
        status != cb::engine_errc::success) {
        return status;
    }
    if (status = Collections::Manager::doPrometheusCollectionStats(
                *getKVBucket(), collector);
        status != cb::engine_errc::success) {
        return status;
    }

    // aggregate DCP producer metrics
    ConnCounter aggregator;
    dcpConnMap_->each([&aggregator](const std::shared_ptr<ConnHandler>& tc) {
        ++aggregator.totalConns;
        if (auto tp = std::dynamic_pointer_cast<DcpProducer>(tc); tp) {
            tp->aggregateQueueStats(aggregator);
        }
    });
    addAggregatedProducerStats(collector, aggregator);
    return status;
}

cb::engine_errc EventuallyPersistentEngine::doMetricGroupLow(
        const BucketStatCollector& collector) {
    cb::engine_errc status;

    if (status = doEngineStatsLowCardinality(collector, nullptr);
        status != cb::engine_errc::success) {
        return status;
    }

    if (const auto* warmup = getKVBucket()->getPrimaryWarmup()) {
        warmup->addStatusMetrics(collector);
    }

    // do dcp aggregated stats, using ":" as the separator to split
    // connection names to find the connection type.
    return doConnAggStats(collector, ":");
}

cb::engine_errc EventuallyPersistentEngine::doMetricGroupMetering(
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    // equivalent to Key::ep_db_file_size but named to match metering
    // requirements.
    collector.addStat(Key::storage, kvBucket->getTotalDiskSize());
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::store(
        CookieIface& cookie,
        ItemIface& itm,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    Item& item = static_cast<Item&>(itm);

    if (item.getNBytes() == 0 &&
        item.getDataType() != PROTOCOL_BINARY_RAW_BYTES) {
        std::stringstream ss;
        ss << item;
        throw std::invalid_argument(
                "EventuallyPersistentEngine::store: Invalid datatype for empty "
                "payload: " +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    if (document_state == DocumentState::Deleted) {
        item.setDeleted();
    }
    if (durability) {
        item.setPendingSyncWrite(durability.value());
    }

    item.increaseDurabilityLevel(kvBucket->getMinDurabilityLevel());

    return acquireEngine(this)->storeInner(
            cookie, item, cas, operation, preserveTtl);
}

cb::EngineErrorCasPair EventuallyPersistentEngine::store_if(
        CookieIface& cookie,
        ItemIface& itm,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    Item& item = static_cast<Item&>(itm);

    if (item.getNBytes() == 0 &&
        item.getDataType() != PROTOCOL_BINARY_RAW_BYTES) {
        std::stringstream ss;
        ss << item;
        throw std::invalid_argument(
                "EventuallyPersistentEngine::store_if: Invalid datatype for "
                "empty payload: " +
                cb::UserDataView(ss.str()).getSanitizedValue());
    }

    if (document_state == DocumentState::Deleted) {
        item.setDeleted();
    }
    if (durability) {
        item.setPendingSyncWrite(durability.value());
    }

    item.increaseDurabilityLevel(kvBucket->getMinDurabilityLevel());

    return acquireEngine(this)->storeIfInner(
            cookie, item, cas, operation, predicate, preserveTtl);
}

void EventuallyPersistentEngine::reset_stats(CookieIface& cookie) {
    acquireEngine(this)->resetStats();
}

cb::engine_errc EventuallyPersistentEngine::setConfigurationParameter(
        std::string_view key, const std::string& val, std::string& msg) {
    auto rv = cb::engine_errc::success;

    try {
        if (key == "max_size" || key == "cache_size") {
            size_t vsize = std::stoull(val);
            kvBucket->processBucketQuotaChange(vsize);
            return cb::engine_errc::success;
        }

        getConfiguration().parseAndSetParameter(key, val);
        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        rv = cb::engine_errc::invalid_arguments;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = cb::engine_errc::invalid_arguments;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = cb::engine_errc::invalid_arguments;

        // Handles any miscellaenous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = cb::engine_errc::invalid_arguments;
    }

    return rv;
}

cb::engine_errc EventuallyPersistentEngine::setCheckpointParam(
        std::string_view key, const std::string& val, std::string& msg) {
    if (!checkSetParameterCategory(key, EngineParamCategory::Checkpoint)) {
        msg = "Unknown checkpoint param";
        return cb::engine_errc::no_such_key;
    }
    return setConfigurationParameter(key, val, msg);
}

cb::engine_errc EventuallyPersistentEngine::setFlushParam(
        std::string_view key, const std::string& val, std::string& msg) {
    // Handle the actual mutation.
    try {
        configuration.requirementsMetOrThrow(key);

        // TODO MB-61655: This can be removed when the local ns_server (same
        // version) always sets this via config param rather than flush param.
        if (key == "max_size" || key == "cache_size") {
            size_t vsize = std::stoull(val);
            kvBucket->processBucketQuotaChange(vsize);
            return cb::engine_errc::success;
        }
        if (key == "mem_low_wat" || key == "mem_high_wat") {
            msg = fmt::format(
                    "Setting of key '{}' is no longer supported "
                    "using set flush param. Use the Buckets REST API instead.",
                    key);
            return cb::engine_errc::invalid_arguments;
        }
        if (key == "timing_log") {
            EPStats& epStats = getEpStats();
            epStats.timingLog = nullptr;
            if (val == "off") {
                EP_LOG_DEBUG_RAW("Disabled timing log.");
            } else {
                auto tmp = std::make_unique<std::ofstream>(val);
                if (tmp->good()) {
                    EP_LOG_DEBUG_CTX("Logging detailed timings", {"path", val});
                    epStats.timingLog = std::move(tmp);
                } else {
                    EP_LOG_WARN_CTX("Error setting detailed timing log",
                                    {"path", val},
                                    {"status", errno},
                                    {"error", strerror(errno)});
                }
            }
            return cb::engine_errc::success;
        }
        if (key == "warmup_min_memory_threshold" ||
            key == "warmup_min_items_threshold") {
            // warn for legacy parameter
            msg = fmt::format(
                    "Setting of key '{}' is no longer supported "
                    "using set flush param. Use the Buckets REST API instead to"
                    "set warmup_behavior",
                    key);
            return cb::engine_errc::invalid_arguments;
        }
        if (key == "num_reader_threads" || key == "num_writer_threads" ||
            key == "num_auxio_threads" || key == "num_nonio_threads") {
            msg = fmt::format(
                    "Setting of key '{0}' is no longer supported "
                    "using set flush param, a post request to the REST API "
                    "(POST "
                    "http://<host>:<port>/pools/default/settings/memcached/"
                    "global with payload {0}=N) should be made instead",
                    key);
            return cb::engine_errc::invalid_arguments;
        }
        if (key == "access_scanner_run") {
            if (!(runAccessScannerTask())) {
                return cb::engine_errc::temporary_failure;
            }
            return cb::engine_errc::success;
        }
        if (key == "vb_state_persist_run") {
            size_t vbid = std::stoi(val);
            if (vbid >= kvBucket->getVBMapSize()) {
                return cb::engine_errc::invalid_arguments;
            }
            runVbStatePersistTask(Vbid(gsl::narrow_cast<Vbid::id_type>(vbid)));
            return cb::engine_errc::success;
        }
        if (key == "defragmenter_run") {
            runDefragmenterTask();
            return cb::engine_errc::success;
        }
        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        return cb::engine_errc::invalid_arguments;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        return cb::engine_errc::invalid_arguments;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        return cb::engine_errc::invalid_arguments;

        // Handles any miscellaneous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        return cb::engine_errc::invalid_arguments;
    }

    if (!checkSetParameterCategory(key, EngineParamCategory::Flush)) {
        EP_LOG_WARN_CTX("Rejecting setFlushParam request",
                        {"key", key},
                        {"value", val});
        msg = "Unknown flush param " + std::string{key};
        return cb::engine_errc::invalid_arguments;
    }
    return setConfigurationParameter(key, val, msg);
}

cb::engine_errc EventuallyPersistentEngine::setDcpParam(std::string_view key,
                                                        const std::string& val,
                                                        std::string& msg) {
    if (!checkSetParameterCategory(key, EngineParamCategory::Dcp)) {
        EP_LOG_WARN_CTX(
                "Rejecting setDcpParam request", {"key", key}, {"value", val});
        msg = "Unknown dcp param " + std::string{key};
        return cb::engine_errc::invalid_arguments;
    }
    return setConfigurationParameter(key, val, msg);
}

cb::engine_errc EventuallyPersistentEngine::setVbucketParam(
        Vbid vbucket,
        std::string_view key,
        const std::string& val,
        std::string& msg) {
    try {
        if (key == "max_cas") {
            uint64_t v = std::strtoull(val.c_str(), nullptr, 10);
            checkNumeric(val.c_str());
            EP_LOG_INFO_CTX("setVbucketParam",
                            {"key", key},
                            {"value", v},
                            {"vb", vbucket});
            if (getKVBucket()->forceMaxCas(vbucket, v) !=
                cb::engine_errc::success) {
                msg = "Not my vbucket";
                return cb::engine_errc::not_my_vbucket;
            }
        }
    } catch (std::exception& e) {
        msg = e.what();
        return cb::engine_errc::invalid_arguments;
    }

    if (!checkSetParameterCategory(key, EngineParamCategory::Vbucket)) {
        EP_LOG_WARN_CTX("Rejecting setVbucketParam request",
                        {"key", key},
                        {"value", val});
        msg = "Unknown vbucket param " + std::string{key};
        return cb::engine_errc::invalid_arguments;
    }
    return setConfigurationParameter(key, val, msg);
}

cb::engine_errc EventuallyPersistentEngine::evictKey(CookieIface& cookie,
                                                     const DocKeyView& key,
                                                     Vbid vbucket) {
    EP_LOG_DEBUG_CTX("Manually evicting object",
                     {"key", cb::UserDataView(key.to_string())});
    const char* msg = nullptr;
    const auto rv = kvBucket->evictKey(key, vbucket, &msg);
    if (rv == cb::engine_errc::not_my_vbucket ||
        rv == cb::engine_errc::no_such_key) {
        if (isDegradedMode()) {
            if (msg) {
                setErrorContext(cookie, msg);
            }
            return cb::engine_errc::temporary_failure;
        }
    }
    if (msg) {
        setErrorContext(cookie, msg);
    }
    return rv;
}

cb::engine_errc EventuallyPersistentEngine::setParameter(
        CookieIface& cookie,
        EngineParamCategory category,
        std::string_view key,
        std::string_view value,
        Vbid vbid) {
    return acquireEngine(this)->setParameterInner(
            cookie, category, key, value, vbid);
}

cb::engine_errc EventuallyPersistentEngine::setParameterInner(
        CookieIface& cookie,
        EngineParamCategory category,
        std::string_view key,
        std::string_view value,
        Vbid vbid) {
    std::lock_guard<std::mutex> lh(setParameterMutex);

    const std::string keyz(key);
    const std::string valz(value);

    std::string msg;
    cb::engine_errc ret = cb::engine_errc::no_such_key;
    switch (category) {
    case EngineParamCategory::Flush:
        ret = setFlushParam(keyz, valz, msg);
        break;
    case EngineParamCategory::Checkpoint:
        ret = setCheckpointParam(keyz, valz, msg);
        break;
    case EngineParamCategory::Dcp:
        ret = setDcpParam(keyz, valz, msg);
        break;
    case EngineParamCategory::Vbucket:
        ret = setVbucketParam(vbid, keyz, valz, msg);
        break;
    case EngineParamCategory::Config:
        ret = setConfigurationParameter(keyz, valz, msg);
        break;
    }

    if (ret == cb::engine_errc::success) {
        EP_LOG_INFO_CTX("setParameter",
                        {"category", category},
                        {"key", key},
                        {"value", value});
    } else if (!msg.empty()) {
        setErrorContext(cookie, msg);
    }

    return ret;
}

std::pair<cb::engine_errc, vbucket_state_t>
EventuallyPersistentEngine::getVBucketInner(CookieIface& cookie, Vbid vbucket) {
    HdrMicroSecBlockTimer timer(&stats.getVbucketCmdHisto);

    auto vb = getVBucket(vbucket);
    if (!vb) {
        return {cb::engine_errc::not_my_vbucket, vbucket_state_dead};
    }
    return {cb::engine_errc::success, vb->getState()};
}

cb::engine_errc EventuallyPersistentEngine::setVBucketInner(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t cas,
        vbucket_state_t state,
        nlohmann::json* meta) {
    HdrMicroSecBlockTimer timer(&stats.setVbucketCmdHisto);
    return setVBucketState(cookie, vbid, state, meta, TransferVB::No, cas);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getReplicaInner(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const DocStateFilter documentStateFilter) {
    auto options = QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE |
                   HIDE_LOCKED_CAS | TRACK_STATISTICS;

    if (documentStateFilter == DocStateFilter::AliveOrDeleted) {
        options |= GET_DELETED_VALUE;
    }

    GetValue rv(getKVBucket()->getReplica(
            key, vbucket, &cookie, static_cast<get_options_t>(options)));
    auto error_code = rv.getStatus();
    if (error_code != cb::engine_errc::would_block) {
        ++(getEpStats().numOpsGet);
    }

    if (error_code == cb::engine_errc::success) {
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, rv.item.release(), this);
    }

    // Remap the error code if bucket is in degraded mode.
    return cb::makeEngineErrorItemPair(maybeRemapStatus(error_code));
}

cb::engine_errc EventuallyPersistentEngine::compactDatabaseInner(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes,
        const std::vector<std::string>& obsoleteKeys) {
    if (takeEngineSpecific<ScheduledCompactionToken>(cookie)) {
        // This is a completion of a compaction. We cleared the engine-specific
        return cb::engine_errc::success;
    }

    CompactionConfig compactionConfig{purge_before_ts,
                                      purge_before_seq,
                                      drop_deletes,
                                      false,
                                      obsoleteKeys};

    // Set something in the EngineSpecfic so we can determine which phase of the
    // command is executing.
    storeEngineSpecific(cookie, ScheduledCompactionToken{});

    // returns would_block for success or another status (e.g. nmvb)
    const auto status = scheduleCompaction(vbid, compactionConfig, &cookie);

    Expects(status != cb::engine_errc::success);
    if (status != cb::engine_errc::would_block) {
        // failed, clear the engine-specific
        clearEngineSpecific(cookie);
        EP_LOG_WARN_CTX("Compaction failed", {"vb", vbid}, {"status", status});
    }

    return status;
}

cb::engine_errc EventuallyPersistentEngine::processUnknownCommandInner(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    auto res = cb::mcbp::Status::UnknownCommand;

    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::GetAllVbSeqnos:
        return getAllVBucketSequenceNumbers(cookie, request, response);
    case cb::mcbp::ClientOpcode::ObserveSeqno:
        return observe_seqno(cookie, request, response);
    case cb::mcbp::ClientOpcode::SetWithMeta:
    case cb::mcbp::ClientOpcode::SetqWithMeta:
    case cb::mcbp::ClientOpcode::AddWithMeta:
    case cb::mcbp::ClientOpcode::AddqWithMeta:
        return setWithMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::DelWithMeta:
    case cb::mcbp::ClientOpcode::DelqWithMeta:
        return deleteWithMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::ReturnMeta:
        return returnMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::GetKeys:
        return getAllKeys(cookie, request, response);
    default:
        res = cb::mcbp::Status::UnknownCommand;
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        ValueIsJson::No,
                        res,
                        0,
                        cookie);
}

cb::engine_errc EventuallyPersistentEngine::unknown_command(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    auto engine = acquireEngine(this);

    // The AddResponseFn callback may allocate memory (temporary buffers for
    // data) which will be de-allocated inside the server, after the
    // engine call (response) has returned. As such we do not want to
    // account such memory against this bucket.
    // Create an exit border guard around the original callback.
    // Perf: use std::cref to avoid copying (and the subsequent `new` call) of
    // the input function.
    auto addResponseExitBorderGuard = makeExitBorderGuard(std::cref(response));

    auto ret = engine->processUnknownCommandInner(
            cookie, request, addResponseExitBorderGuard);
    return ret;
}

cb::engine_errc EventuallyPersistentEngine::step(
        CookieIface& cookie,
        bool throttled,
        DcpMessageProducersIface& producers) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    DcpMsgProducersBorderGuard guardedProducers(producers);
    return conn->step(throttled, guardedProducers);
}

cb::engine_errc EventuallyPersistentEngine::open(CookieIface& cookie,
                                                 uint32_t opaque,
                                                 uint32_t seqno,
                                                 cb::mcbp::DcpOpenFlag flags,
                                                 std::string_view conName,
                                                 std::string_view value) {
    return acquireEngine(this)->dcpOpen(
            cookie, opaque, seqno, flags, conName, value);
}

cb::engine_errc EventuallyPersistentEngine::add_stream(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpAddStreamFlag flags) {
    return acquireEngine(this)->dcpAddStream(cookie, opaque, vbucket, flags);
}

cb::engine_errc EventuallyPersistentEngine::close_stream(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamId sid) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->closeStream(opaque, vbucket, sid);
}

cb::engine_errc EventuallyPersistentEngine::stream_req(
        CookieIface& cookie,
        cb::mcbp::DcpAddStreamFlag flags,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t startSeqno,
        uint64_t endSeqno,
        uint64_t vbucketUuid,
        uint64_t snapStartSeqno,
        uint64_t snapEndSeqno,
        uint64_t* rollbackSeqno,
        dcp_add_failover_log callback,
        std::optional<std::string_view> json) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    try {
        auto ret = conn->streamRequest(flags,
                                      opaque,
                                      vbucket,
                                      startSeqno,
                                      endSeqno,
                                      vbucketUuid,
                                      snapStartSeqno,
                                      snapEndSeqno,
                                      rollbackSeqno,
                                      callback,
                                      json);
        if (ret == cb::engine_errc::no_memory) {
            return memoryCondition();
        }
        return ret;
    } catch (const cb::engine_error& e) {
        EP_LOG_INFO_CTX("stream_req error",
                        {"error", e.what()},
                        {"status", e.engine_code()});
        return cb::engine_errc(e.code().value());
    } catch (const std::exception& e) {
        EP_LOG_ERR("EventuallyPersistentEngine::stream_req: Exception {}",
                   e.what());
        return cb::engine_errc::disconnect;
    }
}

cb::engine_errc EventuallyPersistentEngine::get_failover_log(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        dcp_add_failover_log callback) {
    // This function covers two commands:
    // 1) DCP_GET_FAILOVER_LOG
    //     It is valid only on a DCP Producer connection. Updates the
    //     'lastReceiveTime' for the Producer.
    // 2) GET_FAILOVER_LOG
    //     It does not require a DCP connection (the client has opened
    //     a regular MCBP connection).
    auto engine = acquireEngine(this);

    auto conn = engine->getSharedPtrConnHandler(cookie);
    // Note: (conn != nullptr) only if conn is a DCP connection
    if (conn) {
        auto* producer = dynamic_cast<DcpProducer*>(conn.get());
        // GetFailoverLog not supported for DcpConsumer
        if (!producer) {
            EP_LOG_WARN_RAW(
                    "Disconnecting - This connection doesn't support the dcp "
                    "get "
                    "failover log API");
            return cb::engine_errc::disconnect;
        }
        producer->setLastReceiveTime(ep_uptime_now());
        if (producer->doDisconnect()) {
            return cb::engine_errc::disconnect;
        }
    }

    // Only continue to getVBucket if warmup is not running (so we have a more
    // consistent vbMap)
    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        EP_LOG_WARN_CTX(
                "Get Failover Log failed because this vbucket doesn't exist",
                {"conn", conn ? conn->logHeader() : "MCBP-Connection"},
                {"vb", vbucket});
        return cb::engine_errc::not_my_vbucket;
    }
    auto failoverEntries = vb->failovers->getFailoverLog();
    NonBucketAllocationGuard guard;
    return callback(failoverEntries);
}

cb::engine_errc EventuallyPersistentEngine::stream_end(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamEndStatus status) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie)->streamEnd(opaque, vbucket, status);
}

cb::engine_errc EventuallyPersistentEngine::snapshot_marker(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> high_prepared_seqno,
        std::optional<uint64_t> max_visible_seqno,
        std::optional<uint64_t> purge_seqno) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->snapshotMarker(opaque,
                                vbucket,
                                start_seqno,
                                end_seqno,
                                flags,
                                high_completed_seqno,
                                high_prepared_seqno,
                                max_visible_seqno,
                                purge_seqno);
}

cb::engine_errc EventuallyPersistentEngine::mutation(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint32_t flags,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t expiration,
        uint32_t lock_time,
        uint8_t nru) {
    if (!cb::mcbp::datatype::is_valid(datatype)) {
        EP_LOG_WARN_RAW(
                "Invalid value for datatype "
                " (DCPMutation)");
        return cb::engine_errc::invalid_arguments;
    }
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->mutation(opaque,
                          key,
                          value,
                          datatype,
                          cas,
                          vbucket,
                          flags,
                          by_seqno,
                          rev_seqno,
                          expiration,
                          lock_time,
                          nru);
}

cb::engine_errc EventuallyPersistentEngine::deletion(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->deletion(
            opaque, key, value, datatype, cas, vbucket, by_seqno, rev_seqno);
}

cb::engine_errc EventuallyPersistentEngine::deletion_v2(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->deletionV2(opaque,
                            key,
                            value,
                            datatype,
                            cas,
                            vbucket,
                            by_seqno,
                            rev_seqno,
                            delete_time);
}

cb::engine_errc EventuallyPersistentEngine::expiration(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t deleteTime) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->expiration(opaque,
                            key,
                            value,
                            datatype,
                            cas,
                            vbucket,
                            by_seqno,
                            rev_seqno,
                            deleteTime);
}

cb::engine_errc EventuallyPersistentEngine::set_vbucket_state(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        vbucket_state_t state) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->setVBucketState(opaque, vbucket, state);
}

cb::engine_errc EventuallyPersistentEngine::noop(CookieIface& cookie,
                                                 uint32_t opaque) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie)->noop(opaque);
}

cb::engine_errc EventuallyPersistentEngine::buffer_acknowledgement(
        CookieIface& cookie, uint32_t opaque, uint32_t buffer_bytes) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->bufferAcknowledgement(opaque, buffer_bytes);
}

cb::engine_errc EventuallyPersistentEngine::control(CookieIface& cookie,
                                                    uint32_t opaque,
                                                    std::string_view key,
                                                    std::string_view value) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie)->control(opaque, key, value);
}

cb::engine_errc EventuallyPersistentEngine::response_handler(
        CookieIface& cookie, const cb::mcbp::Response& response) {
    auto engine = acquireEngine(this);
    auto conn = engine->getSharedPtrConnHandler(cookie);
    if (conn && conn->handleResponse(response)) {
        return cb::engine_errc::success;
    }
    return cb::engine_errc::disconnect;
}

cb::engine_errc EventuallyPersistentEngine::system_event(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->systemEvent(
            opaque, vbucket, event, bySeqno, version, key, eventData);
}

cb::engine_errc EventuallyPersistentEngine::prepare(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint32_t flags,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t expiration,
        uint32_t lock_time,
        uint8_t nru,
        DocumentState document_state,
        cb::durability::Level level) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->prepare(opaque,
                         key,
                         value,
                         datatype,
                         cas,
                         vbucket,
                         flags,
                         by_seqno,
                         rev_seqno,
                         expiration,
                         lock_time,
                         nru,
                         document_state,
                         level);
}

cb::engine_errc EventuallyPersistentEngine::seqno_acknowledged(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t prepared_seqno) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->seqno_acknowledged(opaque, vbucket, prepared_seqno);
}

cb::engine_errc EventuallyPersistentEngine::commit(CookieIface& cookie,
                                                   uint32_t opaque,
                                                   Vbid vbucket,
                                                   const DocKeyView& key,
                                                   uint64_t prepared_seqno,
                                                   uint64_t commit_seqno) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->commit(opaque, vbucket, key, prepared_seqno, commit_seqno);
}

cb::engine_errc EventuallyPersistentEngine::abort(CookieIface& cookie,
                                                  uint32_t opaque,
                                                  Vbid vbucket,
                                                  const DocKeyView& key,
                                                  uint64_t preparedSeqno,
                                                  uint64_t abortSeqno) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->abort(opaque, vbucket, key, preparedSeqno, abortSeqno);
}

cb::engine_errc EventuallyPersistentEngine::cached_value(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        cb::const_byte_buffer value,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint32_t flags,
        uint64_t bySeqno,
        uint64_t revSeqno,
        uint32_t expiration,
        uint8_t nru) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->cached_value(opaque,
                              key,
                              value,
                              datatype,
                              cas,
                              vbucket,
                              flags,
                              bySeqno,
                              revSeqno,
                              expiration,
                              nru);
}

cb::engine_errc EventuallyPersistentEngine::cached_key_meta(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKeyView& key,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint32_t flags,
        uint64_t bySeqno,
        uint64_t revSeqno,
        uint32_t expiration) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->cached_key_meta(opaque,
                                 key,
                                 datatype,
                                 cas,
                                 vbucket,
                                 flags,
                                 bySeqno,
                                 revSeqno,
                                 expiration);
}

std::unique_ptr<ConfigurationIface> create_ep_bucket_configuration() {
#ifdef CB_DEVELOPMENT_ASSERTS
    return std::make_unique<Configuration>(cb::serverless::isEnabled(), true);
#else
    return std::make_unique<Configuration>(cb::serverless::isEnabled(), false);
#endif
}

cb::engine_errc EventuallyPersistentEngine::cache_transfer_end(
        CookieIface& cookie, uint32_t opaque, Vbid vbucket) {
    auto engine = acquireEngine(this);
    auto conn = engine->getConnHandler(cookie);
    return conn->cache_transfer_end_rx(opaque, vbucket);
}

/**
 * The only public interface to the eventually persistent engine.
 * Allocate a new instance and initialize it
 * @param get_server_api callback function to get the server exported API
 *                  functions
 * @param handle Where to return the new instance
 * @return cb::engine_errc::success on success
 */
cb::engine_errc create_ep_engine_instance(GET_SERVER_API get_server_api,
                                          EngineIface** handle) {
    ServerApi* api = get_server_api();
    if (api == nullptr) {
        return cb::engine_errc::not_supported;
    }

    // Register and track the engine creation
    auto arena = cb::ArenaMalloc::registerClient();
    cb::ArenaMallocGuard trackEngineCreation(arena);

    try {
        *handle = new EventuallyPersistentEngine(get_server_api, arena);
    } catch (const std::bad_alloc&) {
        cb::ArenaMalloc::unregisterClient(arena);
        return cb::engine_errc::no_memory;
    }

    initialize_time_functions(api->core);
    return cb::engine_errc::success;
}

/*
    This method is called prior to unloading of the shared-object.
    Global clean-up should be performed from this method.
*/
void destroy_ep_engine() {
    // The executor pool was already shut down by the front end thread
    // before we get here, but the tests in ep_testsuite* don't use the
    // "main" from memcached so we need to explicitly shut it down here.
    // Note that it is safe to call shutdown multiple times :)
    ExecutorPool::shutdown();
}

EpEngineArenaHelper::EpEngineArenaHelper(EventuallyPersistentEngine& engine,
                                         cb::ArenaMallocClient arena)
    : arena(std::move(arena)) {
    ObjectRegistry::onSwitchThread(&engine);
}

bool EventuallyPersistentEngine::get_item_info(const ItemIface& itm,
                                               item_info& itm_info) {
    const auto& it = static_cast<const Item&>(itm);
    itm_info = acquireEngine(this)->getItemInfo(it);
    return true;
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::get_meta(
        CookieIface& cookie, const DocKeyView& key, Vbid vbucket) {
    return acquireEngine(this)->getMetaInner(cookie, key, vbucket);
}

cb::engine_errc EventuallyPersistentEngine::set_collection_manifest(
        CookieIface& cookie, std::string_view json) {
    auto engine = acquireEngine(this);
    auto rv = engine->getKVBucket()->setCollections(json, &cookie);

    auto status = cb::engine_errc(rv.code().value());
    if (cb::engine_errc::success != status &&
        status != cb::engine_errc::would_block) {
        engine->setErrorContext(cookie, rv.what());
    }

    return status;
}

cb::engine_errc EventuallyPersistentEngine::get_collection_manifest(
        CookieIface& cookie, const AddResponseFn& response) {
    using Collections::Visibility;
    auto engine = acquireEngine(this);
    Collections::IsVisibleFunction isVisible =
            [&cookie](ScopeID sid,
                      std::optional<CollectionID> cid,
                      Visibility visibility) -> bool {
        // One of the system privileges needed if to access a system collection.
        if (visibility == Visibility::System) {
            if (cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionLookup,
                              sid,
                              cid)
                        .failed() &&
                cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionMutation,
                              sid,
                              cid)
                        .failed()) {
                return false;
            }
        }

        return cookie.testPrivilege(cb::rbac::Privilege::Read, sid, cid)
                       .getStatus() !=
               cb::rbac::PrivilegeAccess::Status::FailNoPrivileges;
    };

    const auto [status, json] =
            engine->getKVBucket()->getCollections(isVisible);

    std::string manifest;
    if (status == cb::mcbp::Status::Success) {
        manifest = json.dump();
    }
    return sendResponse(makeExitBorderGuard(std::cref(response)),
                        {}, // key
                        {}, // extra
                        manifest, // body
                        ValueIsJson::Yes,
                        status,
                        0,
                        cookie);
}

cb::EngineErrorGetCollectionIDResult
EventuallyPersistentEngine::get_collection_id(CookieIface& cookie,
                                              std::string_view path) {
    auto engine = acquireEngine(this);
    auto rv = engine->getKVBucket()->getCollectionID(path);

    if (rv.result == cb::engine_errc::success) {
        auto [uuid, meta] =
                engine->getKVBucket()->getCollectionEntry(rv.collectionId);
        if (!meta.has_value()) {
            // We must have raced with a manifest update
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::unknown_collection};
        }

        using Collections::Visibility;
        if (Collections::getCollectionVisibility(meta->name, rv.collectionId) ==
            Visibility::System) {
            if (cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionLookup,
                              meta->sid,
                              meta->cid)
                        .failed() &&
                cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionMutation,
                              meta->sid,
                              meta->cid)
                        .failed()) {
                return cb::EngineErrorGetCollectionIDResult{
                        cb::engine_errc::no_access};
            }
        }

        // Test for any privilege, we are testing if we have visibility which
        // means at least 1 privilege in the bucket.scope.collection 'path'
        if (cookie.testPrivilege(
                          cb::rbac::Privilege::Read, meta->sid, meta->cid)
                    .getStatus() ==
            cb::rbac::PrivilegeAccess::Status::FailNoPrivileges) {
            rv.result = cb::engine_errc::unknown_collection;
        }
    }

    if (rv.result == cb::engine_errc::unknown_collection ||
        rv.result == cb::engine_errc::unknown_scope) {
        engine->setUnknownCollectionErrorContext(cookie, rv.getManifestId());
    }
    return rv;
}

cb::EngineErrorGetScopeIDResult EventuallyPersistentEngine::get_scope_id(
        CookieIface& cookie, std::string_view path) {
    auto engine = acquireEngine(this);
    auto rv = engine->getKVBucket()->getScopeID(path);
    if (rv.result == cb::engine_errc::success) {
        if (rv.isSystemScope()) {
            using Collections::Visibility;
            if (cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionLookup,
                              rv.scopeId,
                              {})
                        .failed() &&
                cookie.testPrivilege(
                              cb::rbac::Privilege::SystemCollectionMutation,
                              rv.scopeId,
                              {})
                        .failed()) {
                return cb::EngineErrorGetScopeIDResult{
                        cb::engine_errc::no_access};
            }
        }

        // Test for any privilege, we are testing if we have visibility which
        // means at least 1 privilege in the bucket.scope.collection 'path'
        if (cookie.testPrivilege(cb::rbac::Privilege::Read, rv.scopeId, {})
                    .getStatus() ==
            cb::rbac::PrivilegeAccess::Status::FailNoPrivileges) {
            rv.result = cb::engine_errc::unknown_scope;
        }
    }

    if (rv.result == cb::engine_errc::unknown_scope) {
        engine->setUnknownCollectionErrorContext(cookie, rv.getManifestId());
    }

    return rv;
}

cb::EngineErrorGetCollectionMetaResult
EventuallyPersistentEngine::get_collection_meta(
        CookieIface&, CollectionID cid, std::optional<Vbid> vbid) const {
    auto engine = acquireEngine(this);
    if (vbid) {
        auto vbucket = engine->getVBucket(*vbid);
        if (vbucket) {
            auto handle = vbucket->getManifest().lock(cid);
            if (handle.valid()) {
                return {handle.getManifestUid(),
                        handle.getScopeID(),
                        handle.isMetered() == Collections::Metered::Yes,
                        Collections::isSystemCollection(handle.getName(), cid)};
            }
            // returns unknown_collection and the manifest uid
            return cb::EngineErrorGetCollectionMetaResult(
                    handle.getManifestUid());
        }
        return cb::EngineErrorGetCollectionMetaResult(
                cb::engine_errc::not_my_vbucket);
    }
    // No vbucket, perform lookup against bucket
    auto [manifestUid, entry] = engine->getKVBucket()->getCollectionEntry(cid);
    if (entry.has_value()) {
        return {manifestUid,
                entry->sid,
                entry->metered == Collections::Metered::Yes,
                Collections::isSystemCollection(entry->name, cid)};
    }
    // returns unknown_collection and the manifest uid
    return cb::EngineErrorGetCollectionMetaResult(manifestUid);
}

cb::engine::FeatureSet EventuallyPersistentEngine::getFeatures() {
    // This function doesn't track memory against the engine, but create a
    // guard regardless to make this explicit because we only call this once per
    // bucket creation
    NonBucketAllocationGuard guard;
    if (configuration.getBucketTypeString() == "ephemeral") {
        return {cb::engine::Feature::Collections};
    }
    if (isFusionSupportEnabled()) {
        return {cb::engine::Feature::Collections,
                cb::engine::Feature::Persistence,
                cb::engine::Feature::Fusion};
    }
    return {cb::engine::Feature::Collections, cb::engine::Feature::Persistence};
}

bool EventuallyPersistentEngine::isXattrEnabled() {
    return getKVBucket()->isXattrEnabled();
}

std::optional<cb::HlcTime> EventuallyPersistentEngine::getVBucketHlcNow(
        Vbid vbucket) {
    // Initialisation and shutdown paths can means that this function could be
    // called before kvBucket is created/assigned and after a VBucket is removed
    // from the map. In both cases be resilient and return nullopt.
    auto* kvBucket = getKVBucket();
    if (!kvBucket) {
        return std::nullopt;
    }
    auto vb = kvBucket->getVBucket(vbucket);
    if (!vb) {
        return std::nullopt;
    }
    return vb->getHLCNow();
}

EventuallyPersistentEngine::EventuallyPersistentEngine(
        GET_SERVER_API get_server_api, cb::ArenaMallocClient arena)
    : arenaHelper(*this, arena),
#ifdef CB_DEVELOPMENT_ASSERTS
      configuration(cb::serverless::isEnabled(), true),
#else
      configuration(cb::serverless::isEnabled(), false),
#endif
      kvBucket(nullptr),
      workload(nullptr),
      workloadPriority(NO_BUCKET_PRIORITY),
      getServerApiFunc(get_server_api),
      checkpointConfig(nullptr),
      trafficEnabled(false),
      isCrossBucketHtQuotaSharing(false),
      startupTime(0),
      taskable(this),
      compressionMode(BucketCompressionMode::Off),
      minCompressionRatio(default_min_compression_ratio) {
    // Note: The use of offsetof below is non-standard according to GCC 13.2
    // because EventuallyPersistentEngine is not a standard layout type - and
    // GCC warns about it:
    //
    //     warning: ‘offsetof’ within non-standard-layout type
    //    ‘EventuallyPersistentEngine’ is conditionally-supported
    //    [-Winvalid-offsetof]
    //
    // However, the compilers we use provide well-defined behavior as an
    // extension (which is demonstrated since constexpr evaluation must
    // diagnose all undefined behavior). As such, disable the warning for the
    // scope of this check.
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
#endif
    static_assert(offsetof(EventuallyPersistentEngine, arenaHelper) ==
                          2 * sizeof(uintptr_t),
                  "ArenaClientHolder must be first member in "
                  "EventuallyPersistentEngine for correct memory tracking");
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

    // copy through to stats so we can ask for mem used
    getEpStats().arena = arena;

    serverApi = getServerApiFunc();
    fileOpsTracker = &FileOpsTracker::instance();

    // Switch back to "no-bucket" engine (same as before ctor was invoked),
    // so caller doesn't see an unexpected change in current engine. (Note
    // that the ctor switches to this engine as part of constructing
    // arenaHolder).
    ObjectRegistry::onSwitchThread(nullptr);
}

void EventuallyPersistentEngine::reserveCookie(CookieIface& cookie) {
    NonBucketAllocationGuard guard;
    cookie.reserve();
}

void EventuallyPersistentEngine::releaseCookie(CookieIface& cookie) {
    NonBucketAllocationGuard guard;
    cookie.release();
}

void EventuallyPersistentEngine::setErrorContext(CookieIface& cookie,
                                                 std::string_view message) {
    NonBucketAllocationGuard guard;
    cookie.setErrorContext(std::string{message});
}

void EventuallyPersistentEngine::setUnknownCollectionErrorContext(
        CookieIface& cookie, uint64_t manifestUid) const {
    NonBucketAllocationGuard guard;
    cookie.setUnknownCollectionErrorContext(manifestUid);
}

EpEngineValueChangeListener::EpEngineValueChangeListener(
        EventuallyPersistentEngine& e)
    : engine(e) {
}

void EpEngineValueChangeListener::sizeValueChanged(std::string_view key,
                                                   size_t value) {
    if (key.compare("getl_max_timeout") == 0) {
        engine.setGetlMaxTimeout(std::chrono::seconds(value));
    } else if (key.compare("getl_default_timeout") == 0) {
        engine.setGetlDefaultTimeout(std::chrono::seconds(value));
    } else if (key.compare("max_item_size") == 0) {
        engine.setMaxItemSize(value);
    } else if (key.compare("max_item_privileged_bytes") == 0) {
        engine.setMaxItemPrivilegedBytes(value);
    } else if (key == "range_scan_max_continue_tasks") {
        engine.configureRangeScanConcurrency(value);
    } else if (key == "range_scan_max_lifetime") {
        engine.configureRangeScanMaxDuration(std::chrono::seconds(value));
    } else if (key == "dcp_backfill_byte_limit") {
        engine.setDcpBackfillByteLimit(value);
    }
}

void EpEngineValueChangeListener::stringValueChanged(std::string_view key,
                                                     const char* value) {
    if (key == "compression_mode") {
        std::string value_str{value, strlen(value)};
        engine.setCompressionMode(value_str);
    } else if (key == "vbucket_mapping_sanity_checking_error_mode") {
        std::string value_str{value, strlen(value)};
        engine.vBucketMappingErrorHandlingMethod =
                cb::getErrorHandlingMethod(value_str);
    }
}

void EpEngineValueChangeListener::floatValueChanged(std::string_view key,
                                                    float value) {
    if (key == "min_compression_ratio") {
        engine.setMinCompressionRatio(value);
    } else if (key == "dcp_consumer_buffer_ratio") {
        engine.setDcpConsumerBufferRatio(value);
    }
}

void EpEngineValueChangeListener::booleanValueChanged(std::string_view key,
                                                      bool b) {
    if (key == "allow_sanitize_value_in_deletion") {
        engine.allowSanitizeValueInDeletion.store(b);
    } else if (key == "vbucket_mapping_sanity_checking") {
        engine.sanityCheckVBucketMapping = b;
    } else if (key == "not_locked_returns_tmpfail") {
        engine.setNotLockedReturnsTmpfail(b);
    }
}

size_t EventuallyPersistentEngine::getShardCount() {
    auto configShardCount = configuration.getMaxNumShards();
    if (configuration.getBackendString() != "magma") {
        return configuration.getMaxNumShards();
    }

    auto diskShardCount = getShardCountFromDisk();
    if (!diskShardCount) {
        return configShardCount;
    }

    return diskShardCount.value();
}

constexpr std::string_view magmaShardFile = "/magmaShardCount";

std::optional<size_t> EventuallyPersistentEngine::getShardCountFromDisk() {
    Expects(configuration.getBackendString() == "magma");

    // Look for the file
    const auto shardFile = std::filesystem::path(
            configuration.getDbname().append(magmaShardFile));
    if (std::filesystem::exists(shardFile)) {
        std::ifstream ifs(shardFile);
        std::string data;
        std::getline(ifs, data);
        EP_LOG_INFO_CTX("Found shard file for magma", {"shards", data});
        uint64_t shards;
        if (safe_strtoull(data, shards)) {
            return shards;
        }

        auto msg = "Couldn't read shard file or found invalid data";
        EP_LOG_CRITICAL_RAW(msg);
        throw std::logic_error(msg);
    }

    return {};
}

void EventuallyPersistentEngine::maybeSaveShardCount(
        WorkLoadPolicy& workloadPolicy) {
    if (configuration.getBackendString() == "magma") {
        // We should have created this directory already
        Expects(std::filesystem::exists(configuration.getDbname()));

        const auto shardFilePath = std::filesystem::path(
                configuration.getDbname().append(magmaShardFile));

        if (std::filesystem::exists(shardFilePath)) {
            // File already exists, don't overwrite it (we should have the same
            // of shards, it's just pointless).
            return;
        }

        auto shardStr = std::to_string(workloadPolicy.getNumShards());
        try {
            cb::io::saveFile(shardFilePath, shardStr);
        } catch (const std::exception& e) {
            throw std::runtime_error(fmt::format(
                    "EventuallyPersistentEngine::maybeSaveShardCount: Failed "
                    "to save shard file: {}",
                    e.what()));
        }

        Ensures(std::filesystem::exists(shardFilePath));
    }
}

cb::engine_errc EventuallyPersistentEngine::initialize(
        std::string_view config,
        const nlohmann::json& encryption,
        std::string_view chronicleAuthToken,
        const nlohmann::json& collectionManifest) {
    auto handle = acquireEngine(this);
    try {
        return handle->initializeInner(
                config, encryption, chronicleAuthToken, collectionManifest);
    } catch (const cb::engine_error& e) {
        NonBucketAllocationGuard guard;
        throw cb::engine_error(e.engine_code(), e.what());
    } catch (const std::exception& e) {
        NonBucketAllocationGuard guard;
        throw std::runtime_error(e.what());
    }
}

cb::engine_errc EventuallyPersistentEngine::initializeInner(
        std::string_view config,
        const nlohmann::json& encryption,
        std::string_view chronicleAuthToken,
        const nlohmann::json& collectionManifest) {
    if (config.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    auto switchToEngine = acquireEngine(this);
    resetStats();

    if (!encryption.empty()) {
        encryptionKeyProvider.setKeys(encryption);
    }

    if (!configuration.parseConfiguration(config)) {
        EP_LOG_WARN_RAW(
                "Failed to parse the configuration config "
                "during bucket initialization");
        return cb::engine_errc::failed;
    }
    name = configuration.getCouchBucket();

    // Create the bucket data directory, ns_server should have created the
    // process level one but they expect us to create the bucket level one.
    const auto dbName = configuration.getDbname();
    if (dbName.empty()) {
        EP_LOG_WARN_RAW(
                "Invalid configuration: dbname must be a non-empty value");
        return cb::engine_errc::invalid_arguments;
    }

    try {
        std::filesystem::create_directories(dbName);
    } catch (const std::system_error& error) {
        EP_LOG_WARN_CTX("Failed to create data directory",
                        {"dbname", dbName},
                        {"error", error.code().message()});
        return cb::engine_errc::failed;
    }

    // Remove the obsolete collections.manifest file (no longer used)
    std::filesystem::path dbPath(dbName);
    std::error_code ec;
    std::filesystem::remove(dbPath / "collections.manifest", ec);
    if (ec) {
        EP_LOG_WARN_CTX("Failed to remove obsolete collection.manifest file",
                        {"path", dbPath / "collections.manifest"},
                        {"error", ec.message()});
    }

    auto& env = Environment::get();
    env.engineFileDescriptors = serverApi->core->getMaxEngineFileDescriptors();

    maxFailoverEntries = configuration.getMaxFailoverEntries();

    if (configuration.getMaxSize() == 0) {
        EP_LOG_WARN_RAW(
                "Invalid configuration: max_size must be a non-zero value");
        return cb::engine_errc::failed;
    }

    isCrossBucketHtQuotaSharing = configuration.isCrossBucketHtQuotaSharing();
    if (isCrossBucketHtQuotaSharing) {
        // Ephemeral bucket are not supported together with quota sharing,
        // because we're not handling the fail_new_data policy.
        Expects(configuration.getBucketTypeString() != "ephemeral");
        getQuotaSharingManager().getGroup().add(*this);
    }

    memoryTracker = std::make_unique<StrictQuotaMemoryTracker>(*this);

    maxItemSize = configuration.getMaxItemSize();
    configuration.addValueChangedListener(
            "max_item_size",
            std::make_unique<EpEngineValueChangeListener>(*this));

    maxItemPrivilegedBytes = configuration.getMaxItemPrivilegedBytes();
    configuration.addValueChangedListener(
            "max_item_privileged_bytes",
            std::make_unique<EpEngineValueChangeListener>(*this));

    getlDefaultTimeout =
            std::chrono::seconds{configuration.getGetlDefaultTimeout()};
    configuration.addValueChangedListener(
            "getl_default_timeout",
            std::make_unique<EpEngineValueChangeListener>(*this));
    getlMaxTimeout = std::chrono::seconds{configuration.getGetlMaxTimeout()};
    configuration.addValueChangedListener(
            "getl_max_timeout",
            std::make_unique<EpEngineValueChangeListener>(*this));

    allowSanitizeValueInDeletion.store(
            configuration.isAllowSanitizeValueInDeletion());
    configuration.addValueChangedListener(
            "allow_sanitize_value_in_deletion",
            std::make_unique<EpEngineValueChangeListener>(*this));

    configuration.addValueChangedListener(
            "range_scan_max_continue_tasks",
            std::make_unique<EpEngineValueChangeListener>(*this));
    configuration.addValueChangedListener(
            "range_scan_max_lifetime",
            std::make_unique<EpEngineValueChangeListener>(*this));

    notLockedReturnsTmpfail = configuration.isNotLockedReturnsTmpfail();
    configuration.addValueChangedListener(
            "not_locked_returns_tmpfail",
            std::make_unique<EpEngineValueChangeListener>(*this));

    // The number of shards for a magma bucket cannot be changed after the first
    // bucket instantiation. This is because the number of shards determines
    // the on disk structure of the data. To solve this problem we store a file
    // to the data directory on first bucket creation that tells us how many
    // shards are to be used. We read this file here if it exists and use that
    // number, if not, this should be the first bucket creation.
    workload = std::make_unique<WorkLoadPolicy>(
            configuration.getMaxNumWorkers(), getShardCount());

    maybeSaveShardCount(*workload);

    setConflictResolutionMode(configuration.getConflictResolutionTypeString());

    dcpConnMap_ = std::make_unique<DcpConnMap>(*this);

    if (configuration.isDcpConsumerFlowControlEnabled()) {
        dcpFlowControlManager = std::make_unique<DcpFlowControlManager>(*this);
    }

    checkpointConfig = std::make_unique<CheckpointConfig>(configuration);

    this->chronicleAuthToken = std::string(chronicleAuthToken);

    kvBucket = makeBucket(configuration);

    stats.setLowWaterMarkPercent(configuration.getMemLowWatPercent());
    stats.setHighWaterMarkPercent(configuration.getMemHighWatPercent());

    setMaxDataSize(configuration.getMaxSize());

    if (!collectionManifest.empty()) {
        kvBucket->getCollectionsManager().setInitialCollectionManifest(
                collectionManifest);
    }

    // Complete the initialization of the ep-store
    if (!kvBucket->initialize()) {
        return cb::engine_errc::failed;
    }

    if(configuration.isDataTrafficEnabled()) {
        enableTraffic(true);
    }

    dcpConnMap_->initialize();

    // record engine initialization time
    startupTime.store(ep_real_time());

    EP_LOG_INFO_CTX("EP Engine: Initialization of bucket complete",
                    {"type", configuration.getBucketTypeString()});

    setCompressionMode(configuration.getCompressionModeString());

    configuration.addValueChangedListener(
            "compression_mode",
            std::make_unique<EpEngineValueChangeListener>(*this));

    setMinCompressionRatio(configuration.getMinCompressionRatio());

    configuration.addValueChangedListener(
            "min_compression_ratio",
            std::make_unique<EpEngineValueChangeListener>(*this));

    sanityCheckVBucketMapping = configuration.isVbucketMappingSanityChecking();
    vBucketMappingErrorHandlingMethod = cb::getErrorHandlingMethod(
            configuration.getVbucketMappingSanityCheckingErrorModeString());

    configuration.addValueChangedListener(
            "vbucket_mapping_sanity_checking",
            std::make_unique<EpEngineValueChangeListener>(*this));
    configuration.addValueChangedListener(
            "vbucket_mapping_sanity_checking_error_mode",
            std::make_unique<EpEngineValueChangeListener>(*this));

    configuration.addValueChangedListener(
            "dcp_consumer_buffer_ratio",
            std::make_unique<EpEngineValueChangeListener>(*this));

    configuration.addValueChangedListener(
            "dcp_backfill_byte_limit",
            std::make_unique<EpEngineValueChangeListener>(*this));

    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::setConflictResolutionMode(
        std::string_view mode) {
    if (mode == "seqno") {
        conflictResolutionMode = ConflictResolutionMode::RevisionId;
    } else if (mode == "lww") {
        conflictResolutionMode = ConflictResolutionMode::LastWriteWins;
    } else if (mode == "custom") {
        conflictResolutionMode = ConflictResolutionMode::Custom;
    } else {
        throw std::invalid_argument{
                "EventuallyPersistentEngine::setConflictResolutionMode(): "
                "Invalid value '" +
                std::string(mode) +
                "' for config option conflict_resolution_type."};
    }
}

void EventuallyPersistentEngine::destroyInner(bool force) {
    stats.forceShutdown = force;
    stats.isShutdown = true;

    if (isCrossBucketHtQuotaSharing) {
        getQuotaSharingManager().getGroup().remove(*this);
    }

    if (dcpConnMap_) {
        dcpConnMap_->shutdownAllConnections();
    }
    if (kvBucket) {
        epDestroyFailureHook();
        // deinitialize() will shutdown the flusher, bgfetcher and warmup tasks
        // then take a snapshot the stats.
        kvBucket->deinitialize();

        // Need to reset the kvBucket as we need our thread local engine ptr to
        // be valid when destructing Items in CheckpointManagers but we need to
        // reset it before destructing EPStats.
        kvBucket.reset();
    }
    EP_LOG_INFO_RAW(
            "EventuallyPersistentEngine::destroyInner(): Completed "
            "deinitialize.");
}

KVStoreIface::CreateItemCB EventuallyPersistentEngine::getCreateItemCallback() {
    // EPEngine functions are not accessible wihtin KVStore, therefore
    // createItem is passed via callback, i.e. we don't create a
    // new item if it will cause memory to exceed the mutation watermark.
    return [this](const DocKeyView& key,
                  const size_t nbytes,
                  const uint32_t flags,
                  const rel_time_t exptime,
                  const value_t& body,
                  uint8_t datatype,
                  uint64_t theCas,
                  int64_t bySeq,
                  Vbid vbid,
                  int64_t revSeq) {
        return createItem(key,
                          nbytes,
                          flags,
                          exptime,
                          body,
                          datatype,
                          theCas,
                          bySeq,
                          vbid,
                          revSeq);
    };
}

std::pair<cb::engine_errc, std::unique_ptr<Item>>
EventuallyPersistentEngine::createItem(const DocKeyView& key,
                                       size_t nbytes,
                                       uint32_t flags,
                                       rel_time_t exptime,
                                       const value_t& body,
                                       uint8_t datatype,
                                       uint64_t theCas,
                                       int64_t bySeq,
                                       Vbid vbid,
                                       int64_t revSeq) {
    if (!memoryTracker->isBelowMutationMemoryQuota(sizeof(Item) + sizeof(Blob) +
                                                   key.size() + nbytes)) {
        return {memoryCondition(), nullptr};
    }
    try {
        auto item = std::make_unique<Item>(key,
                                           flags,
                                           exptime,
                                           body,
                                           datatype,
                                           theCas,
                                           bySeq,
                                           vbid,
                                           revSeq);
        return {cb::engine_errc::success, std::move(item)};
    } catch (const std::bad_alloc&) {
        return {memoryCondition(), nullptr};
    }
}

cb::EngineErrorItemPair EventuallyPersistentEngine::itemAllocate(
        const DocKeyView& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const uint32_t flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    if ((priv_nbytes > maxItemPrivilegedBytes) ||
        ((nbytes - priv_nbytes) > maxItemSize)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::too_big);
    }

    if (!hasMemoryForItemAllocation(sizeof(Item) + sizeof(Blob) + key.size() +
                                    nbytes)) {
        return cb::makeEngineErrorItemPair(memoryCondition());
    }

    try {
        auto* item = new Item(key,
                              flags,
                              ep_convert_to_expiry_time(exptime),
                              nullptr,
                              nbytes,
                              datatype,
                              0 /*cas*/,
                              -1 /*seq*/,
                              vbucket);
        stats.itemAllocSizeHisto.addValue(nbytes);
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, item, this);
    } catch (const std::bad_alloc&) {
        return cb::makeEngineErrorItemPair(memoryCondition());
    }
}

cb::engine_errc EventuallyPersistentEngine::removeInner(
        CookieIface& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        std::optional<cb::durability::Requirements> durability,
        mutation_descr_t& mut_info) {
    if (sanityCheckVBucketMapping) {
        validateKeyMapping("EventuallyPersistentEngine::removeInner",
                           vBucketMappingErrorHandlingMethod,
                           key,
                           vbucket,
                           kvBucket->getVBMapSize());
    }

    // Check if this is a in-progress durable delete which has now completed -
    // (see 'case EWOULDBLOCK' at the end of this function where we record
    // the fact we must block the client until the SycnWrite is durable).
    if (durability) {
        auto deletedCas = takeEngineSpecific<uint64_t>(cookie);
        if (deletedCas.has_value()) {
            // Non-null means this is the second call to this function after
            // the SyncWrite has completed. Return SUCCESS.

            cas = *deletedCas;
            // @todo-durability - add support for non-sucesss (e.g. Aborted)
            // when we support non-successful completions of SyncWrites.
            return cb::engine_errc::success;
        }
    }

    cb::engine_errc ret = kvBucket->deleteItem(
            key, cas, vbucket, &cookie, durability, nullptr, mut_info);

    ret = maybeRemapStatus(ret);
    switch (ret) {
    case cb::engine_errc::sync_write_pending:
        if (durability) {
            // Record the fact that we are blocking to wait for SyncDelete
            // completion; so the next call to this function should return
            // the result of the SyncWrite (see call to getEngineSpecific at
            // the head of this function).
            // (just store non-null value to indicate this).
            storeEngineSpecific(cookie, cas);
        }
        ret = cb::engine_errc::would_block;
        break;

    case cb::engine_errc::success:
        ++stats.numOpsDelete;
        break;

    default:
        // No special handling.
        break;
    }
    return ret;
}

void EventuallyPersistentEngine::itemRelease(ItemIface* itm) {
    delete reinterpret_cast<Item*>(itm);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getInner(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        get_options_t options) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch<Code>> timer(
            std::forward_as_tuple(stats.getCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::Get));

    GetValue gv(kvBucket->get(key, vbucket, &cookie, options));
    cb::engine_errc ret = gv.getStatus();

    if (ret == cb::engine_errc::success) {
        if (options & TRACK_STATISTICS) {
            ++stats.numOpsGet;
        }
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
    }

    return cb::makeEngineErrorItemPair(maybeRemapStatus(ret));
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getAndTouchInner(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t exptime) {
    GetValue gv(kvBucket->getAndUpdateTtl(
            key, vbucket, &cookie, ep_convert_to_expiry_time(exptime)));

    auto rv = gv.getStatus();
    if (rv == cb::engine_errc::success) {
        ++stats.numOpsGet;
        ++stats.numOpsStore;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
    }

    if (rv == cb::engine_errc::key_already_exists && isDegradedMode()) {
        rv = cb::engine_errc::temporary_failure;
    } else {
        rv = maybeRemapStatus(rv);
    }

    if (rv == cb::engine_errc::key_already_exists) {
        rv = cb::engine_errc::locked;
    }

    return cb::makeEngineErrorItemPair(rv);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getIfInner(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch<Code>> timer(
            std::forward_as_tuple(stats.getCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::GetIf));

    // Fetch an item from the hashtable (without trying to schedule a bg-fetch
    // and pass it through the filter. If the filter accepts the document
    // based on the metadata, return the document. If the document's data
    // isn't resident we run another iteration in the loop and retries the
    // action but this time we _do_ schedule a bg-fetch.
    for (int ii = 0; ii < 2; ++ii) {
        auto options =
                static_cast<get_options_t>(HONOR_STATES | HIDE_LOCKED_CAS);

        // For the first pass, if we need to do a BGfetch, only fetch metadata
        // (no point in fetching the whole document if the filter doesn't want
        // it).
        if (ii == 0) {
            options = static_cast<get_options_t>(int(options) | ALLOW_META_ONLY);
        }

        // For second pass, or if full eviction, we'll need to issue a BG fetch.
        if (ii == 1 ||
            kvBucket->getItemEvictionPolicy() == EvictionPolicy::Full) {
            options = static_cast<get_options_t>(int(options) | QUEUE_BG_FETCH);
        }

        GetValue gv(kvBucket->get(key, vbucket, &cookie, options));
        cb::engine_errc status = gv.getStatus();

        if (status != cb::engine_errc::success) {
            return cb::makeEngineErrorItemPair(maybeRemapStatus(status));
        }

        const VBucketPtr vb = getKVBucket()->getVBucket(vbucket);
        uint64_t vb_uuid = 0;
        int64_t hlcEpoch = HlcCasSeqnoUninitialised;
        if (vb) {
            vb_uuid = vb->failovers->getLatestUUID();
            hlcEpoch = vb->getHLCEpochSeqno();
        }
        // Apply filter; the item value isn't guaranteed to be present
        // (meta only) so remove it to prevent people accidentally trying to
        // test it.
        auto info = gv.item->toItemInfo(vb_uuid, hlcEpoch);
        info.value[0].iov_base = nullptr;
        info.value[0].iov_len = 0;
        if (filter(info)) {
            if (!gv.isPartial()) {
                return cb::makeEngineErrorItemPair(
                        cb::engine_errc::success, gv.item.release(), this);
            }
            // We want this item, but we need to fetch it off disk
        } else {
            // the client don't care about this thing..
            return cb::makeEngineErrorItemPair(cb::engine_errc::success);
        }
    }

    // It should not be possible to get as the second iteration in the loop
    // SHOULD handle backround fetches an the item should NOT be partial!
    throw std::logic_error("EventuallyPersistentEngine::get_if: loop terminated");
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getLockedInner(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        std::chrono::seconds lock_timeout) {
    if (lock_timeout.count() == 0 || lock_timeout > getGetlMaxTimeout()) {
        lock_timeout = getGetlDefaultTimeout();
    }

    auto result = kvBucket->getLocked(key, vbucket, lock_timeout, &cookie);

    if (result.getStatus() == cb::engine_errc::success) {
        ++stats.numOpsGet;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, result.item.release(), this);
    }

    return cb::makeEngineErrorItemPair(result.getStatus());
}

cb::engine_errc EventuallyPersistentEngine::unlockInner(CookieIface& cookie,
                                                        const DocKeyView& key,
                                                        Vbid vbucket,
                                                        uint64_t cas) {
    return maybeRemapStatus(
            kvBucket->unlockKey(key, vbucket, cas, ep_current_time(), &cookie));
}

cb::EngineErrorCasPair EventuallyPersistentEngine::storeIfInner(
        CookieIface& cookie,
        Item& item,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        bool preserveTtl) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch<Code>> timer(
            std::forward_as_tuple(stats.storeCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::Store));

    if (sanityCheckVBucketMapping) {
        validateKeyMapping("EventuallyPersistentEngine::storeIfInner",
                           vBucketMappingErrorHandlingMethod,
                           item.getKey(),
                           item.getVBucketId(),
                           kvBucket->getVBMapSize());
    }

    // MB-37374: Ensure that documents in deleted state have no user value.
    if (cb::mcbp::datatype::is_xattr(item.getDataType()) && item.isDeleted()) {
        const auto& value = item.getValue();
        auto value_size = cb::xattr::get_body_size(
                item.getDataType(), {value->getData(), value->valueSize()});
        if (value_size != 0) {
            EP_LOG_WARN_CTX(
                    "EventuallyPersistentEngine::storeIfInner: attempting to "
                    "store a deleted document with non-zero value size",
                    {"value_size", value_size});
            return {cb::engine_errc::invalid_arguments, {}};
        }
    }

    // Check if this is a in-progress durable store which has now completed -
    // (see 'case EWOULDBLOCK' at the end of this function where we record
    // the fact we must block the client until the SyncWrite is durable).
    if (item.isPending()) {
        auto cookieCas = takeEngineSpecific<uint64_t>(cookie);
        if (cookieCas.has_value()) {
            // Non-null means this is the second call to this function after
            // the SyncWrite has completed. Return SUCCESS.
            return {cb::engine_errc::success, *cookieCas};
        }
    }

    cb::engine_errc status;
    switch (operation) {
    case StoreSemantics::CAS:
        if (item.getCas() == 0) {
            // Using a cas command with a cas wildcard doesn't make sense
            status = cb::engine_errc::not_stored;
            break;
        }
    // FALLTHROUGH
    case StoreSemantics::Set:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }
        item.setPreserveTtl(preserveTtl);
        status = kvBucket->set(item, &cookie, predicate);
        break;

    case StoreSemantics::Add:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }

        if (item.getCas() != 0) {
            // Adding an item with a cas value doesn't really make sense...
            return {cb::engine_errc::key_already_exists, cas};
        }

        status = kvBucket->add(item, &cookie);
        break;

    case StoreSemantics::Replace:
        // MB-48577: Don't permit replace until traffic is enabled
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }

        item.setPreserveTtl(preserveTtl);
        status = kvBucket->replace(item, &cookie, predicate);
        break;
    default:
        status = cb::engine_errc::not_supported;
    }

    switch (status) {
    case cb::engine_errc::success:
        ++stats.numOpsStore;
        // If success - check if we're now in need of some memory freeing
        kvBucket->checkAndMaybeFreeMemory();
        break;
    case cb::engine_errc::no_memory:
        status = memoryCondition();
        break;
    case cb::engine_errc::not_stored:
    case cb::engine_errc::not_my_vbucket:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }
        break;
    case cb::engine_errc::sync_write_pending:
        if (item.isPending()) {
            // Record the fact that we are blocking to wait for SyncWrite
            // completion; so the next call to this function should return
            // the result of the SyncWrite (see call to getEngineSpecific at
            // the head of this function. Store the cas of the item so that we
            // can return it to the client later.
            storeEngineSpecific(cookie, item.getCas());
        }
        status = cb::engine_errc::would_block;
        break;
    default:
        break;
    }

    return {status, item.getCas()};
}

cb::engine_errc EventuallyPersistentEngine::storeInner(CookieIface& cookie,
                                                       Item& itm,
                                                       uint64_t& cas,
                                                       StoreSemantics operation,
                                                       bool preserveTtl) {
    auto [status, _cas] =
            storeIfInner(cookie, itm, cas, operation, {}, preserveTtl);
    cas = _cas;
    return status;
}

cb::engine_errc EventuallyPersistentEngine::memoryCondition() {
    // Trigger necessary task(s) to free memory down below high watermark.
    getKVBucket()->attemptToFreeMemory();
    getKVBucket()->wakeUpCheckpointMemRecoveryTask();

    if (memoryTracker->isBelowMemoryQuota()) {
        // Still below bucket_quota - treat as temporary failure.
        ++stats.tmp_oom_errors;
        return cb::engine_errc::temporary_failure;
    }
    // Already over bucket quota - make this a hard error.
    ++stats.oom_errors;
    return cb::engine_errc::no_memory;
}

bool EventuallyPersistentEngine::hasMemoryForItemAllocation(
        size_t totalItemSize) {
    return memoryTracker->isBelowMemoryQuota(totalItemSize);
}

bool EventuallyPersistentEngine::enableTraffic(bool enable) {
    bool inverse = !enable;
    bool bTrafficEnabled =
            trafficEnabled.compare_exchange_strong(inverse, enable);
    if (bTrafficEnabled) {
        EP_LOG_INFO_CTX("EventuallyPersistentEngine::enableTraffic",
                        {"state", enable ? "enabled" : "disabled"});
    } else {
        EP_LOG_WARN_CTX("EventuallyPersistentEngine::enableTraffic",
                        {"error",
                         fmt::format("traffic is already {}",
                                     enable ? "enabled" : "disabled")},
                        {"state", enable ? "enabled" : "disabled"});
    }
    return bTrafficEnabled;
}

void EventuallyPersistentEngine::doEngineStatsCouchDB(
        const StatCollector& collector, const EPStats& epstats) {
    using namespace cb::stats;
    size_t value;
    if (kvBucket->getKVStoreStat("io_document_write_bytes", value)) {
        collector.addStat(Key::ep_io_document_write_bytes, value);

        // Lambda to print a Write Amplification stat for the given bytes
        // written counter.
        auto printWriteAmpStat = [this, &collector, docBytes = value](
                const char* writeBytesStat,
                const char* writeAmpStat) {
          double writeAmp = 0;
          size_t bytesWritten;
          if (docBytes &&
              kvBucket->getKVStoreStat(writeBytesStat, bytesWritten)) {
              writeAmp = double(bytesWritten) / docBytes;
          }
          collector.addStat(writeAmpStat, writeAmp);
        };

        printWriteAmpStat("io_flusher_write_bytes",
                          "ep_io_flusher_write_amplification");
        printWriteAmpStat("io_total_write_bytes",
                          "ep_io_total_write_amplification");
    }
    if (kvBucket->getKVStoreStat("io_total_read_bytes", value)) {
        collector.addStat(Key::ep_io_total_read_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_total_write_bytes", value)) {
        collector.addStat(Key::ep_io_total_write_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_compaction_read_bytes", value)) {
        collector.addStat(Key::ep_io_compaction_read_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_compaction_write_bytes", value)) {
        collector.addStat(Key::ep_io_compaction_write_bytes, value);
    }

    if (kvBucket->getKVStoreStat("io_bg_fetch_read_count", value)) {
        collector.addStat(Key::ep_io_bg_fetch_read_count, value);
        // Calculate read amplication (RA) in terms of disk reads:
        // ratio of number of reads performed, compared to how many docs
        // fetched.
        //
        // Note: An alternative definition would be in terms of *bytes* read -
        // count of bytes read from disk compared to sizeof(key+meta+body) for
        // for fetched documents. However this is potentially misleading given
        // we perform IO buffering and always read in 4K sized chunks, so it
        // would give very large values.
        auto fetched = epstats.bg_fetched + epstats.bg_meta_fetched;
        double readAmp = fetched ? double(value) / double(fetched) : 0.0;
        collector.addStat(Key::ep_bg_fetch_avg_read_amplification, readAmp);
    }
}

void EventuallyPersistentEngine::doEngineStatsMagma(
        const StatCollector& collector) {
    using namespace cb::stats;
    auto divide = [](double a, double b) { return b ? a / b : 0; };
    constexpr std::array<std::string_view, 71> statNames = {
            {"magma_HistorySizeBytesEvicted",
             "magma_HistoryTimeBytesEvicted",
             "magma_NCompacts",
             "magma_NDropEncryptionKeysCompacts",
             "magma_NDataLevelCompacts",
             "magma_KeyIndex_NCompacts",
             "magma_SeqIndex_NCompacts",
             "magma_NFlushes",
             "magma_NTTLCompacts",
             "magma_NFileCountCompacts",
             "magma_KeyIndex_NFileCountCompacts",
             "magma_SeqIndex_NFileCountCompacts",
             "magma_NWriterCompacts",
             "magma_KeyIndex_NWriterCompacts",
             "magma_SeqIndex_NWriterCompacts",
             "magma_BytesOutgoing",
             "magma_NReadBytes",
             "magma_FSReadBytes",
             "magma_NReadBytesGet",
             "magma_CheckpointOverhead",
             "magma_KeyIterator_ItemsRead",
             "magma_SeqIterator_ItemsRead",
             "magma_KeyIterator_ItemsSkipped",
             "magma_SeqIterator_ItemsSkipped",
             "magma_NGets",
             "magma_NSets",
             "magma_NInserts",
             "magma_NReadIO",
             "magma_NReadBytesCompact",

             // Write amp analysis.
             "magma_BytesIncoming",
             "magma_BytesOverwritten",
             "magma_KeyIndex_BytesIncoming",
             "magma_SeqIndex_BytesIncoming",
             "magma_SeqIndex_Delta_BytesIncoming",
             "magma_NWriteBytes",
             "magma_FSWriteBytes",
             "magma_NWriteBytesCompact",
             "magma_KeyIndex_NWriteBytes",
             "magma_SeqIndex_NWriteBytes",
             "magma_SeqIndex_Delta_NWriteBytes",
             "magma_KeyIndex_NWriteBytesFileCountCompact",
             "magma_SeqIndex_NWriteBytesFileCountCompact",

             "magma_ActiveDiskUsage",
             "magma_LogicalDataSize",
             "magma_LogicalDiskSize",
             "magma_HistoryLogicalDiskSize",
             "magma_HistoryLogicalDataSize",
             "magma_TotalDiskUsage",
             "magma_WALDiskUsage",
             "magma_BlockCacheMemUsed",
             "magma_KeyIndexSize",
             "magma_SeqIndex_IndexBlockSize",
             "magma_WriteCacheMemUsed",
             "magma_WALMemUsed",
             "magma_TableMetaMemUsed",
             "magma_TableObjectMemUsed",
             "magma_ReadAheadBufferMemUsed",
             "magma_LSMTreeObjectMemUsed",
             "magma_HistogramMemUsed",
             "magma_BufferMemUsed",
             "magma_TreeSnapshotMemUsed",
             "magma_TotalMemUsed",
             "magma_TotalBloomFilterMemUsed",
             "magma_BlockCacheHits",
             "magma_BlockCacheMisses",
             "magma_NTablesDeleted",
             "magma_NTablesCreated",
             "magma_NTableFiles",
             "magma_NSyncs",
             "magma_DataBlocksSize",
             "magma_DataBlocksCompressSize"}};

    auto kvStoreStats = kvBucket->getKVStoreStats(statNames);

    // Return whether stat exists. If exists, save value in output param value.
    auto statExists = [&](std::string_view statName, size_t& value) {
        auto stat = kvStoreStats.find(statName);
        if (stat != kvStoreStats.end()) {
            value = stat->second;
            return true;
        }
        return false;
    };

    // If given stat exists, add it to collector.
    auto addStat = [&](Key key, std::string_view statName) {
        size_t value = 0;
        if (statExists(statName, value)) {
            collector.addStat(key, value);
        }
    };

    // Ops counters.
    addStat(Key::ep_magma_sets, "magma_NSets");
    addStat(Key::ep_magma_gets, "magma_NGets");
    addStat(Key::ep_magma_inserts, "magma_NInserts");
    addStat(Key::ep_magma_keyitr_items_read, "magma_KeyIterator_ItemsRead");
    addStat(Key::ep_magma_keyitr_items_skipped,
            "magma_KeyIterator_ItemsSkipped");
    addStat(Key::ep_magma_seqitr_items_read, "magma_SeqIterator_ItemsRead");
    addStat(Key::ep_magma_seqitr_items_skipped,
            "magma_SeqIterator_ItemsSkipped");

    addStat(Key::ep_magma_history_time_evicted,
            "magma_HistoryTimeBytesEvicted");
    addStat(Key::ep_magma_history_size_evicted,
            "magma_HistorySizeBytesEvicted");

    // Compaction counter stats.
    addStat(Key::ep_magma_compactions, "magma_NCompacts");
    addStat(Key::ep_magma_drop_encryption_keys_compactions,
            "magma_NDropEncryptionKeysCompacts");
    addStat(Key::ep_magma_keyindex_compactions, "magma_KeyIndex_NCompacts");
    addStat(Key::ep_magma_seqindex_compactions, "magma_SeqIndex_NCompacts");
    addStat(Key::ep_magma_seqindex_data_compactions,
            "magma_NDataLevelCompacts");
    addStat(Key::ep_magma_flushes, "magma_NFlushes");
    addStat(Key::ep_magma_ttl_compactions, "magma_NTTLCompacts");
    addStat(Key::ep_magma_filecount_compactions, "magma_NFileCountCompacts");
    addStat(Key::ep_magma_keyindex_filecount_compactions,
            "magma_KeyIndex_NFileCountCompacts");
    addStat(Key::ep_magma_seqindex_filecount_compactions,
            "magma_SeqIndex_NFileCountCompacts");
    addStat(Key::ep_magma_writer_compactions, "magma_NWriterCompacts");
    addStat(Key::ep_magma_keyindex_writer_compactions,
            "magma_KeyIndex_NWriterCompacts");
    addStat(Key::ep_magma_seqindex_writer_compactions,
            "magma_SeqIndex_NWriterCompacts");

    // Read amp, ReadIOAmp.
    size_t bytesOutgoing = 0;
    size_t readBytes = 0;
    size_t fsReadBytes = 0;
    if (statExists("magma_BytesOutgoing", bytesOutgoing) &&
        statExists("magma_NReadBytes", readBytes) &&
        statExists("magma_FSReadBytes", fsReadBytes)) {
        collector.addStat(Key::ep_magma_bytes_outgoing, bytesOutgoing);
        collector.addStat(Key::ep_magma_read_bytes, readBytes);
        collector.addStat(Key::ep_io_total_read_bytes, fsReadBytes);
        auto readAmp = divide(fsReadBytes, bytesOutgoing);
        collector.addStat(Key::ep_magma_readamp, readAmp);

        size_t readBytesGet = 0;
        if (statExists("magma_NReadBytesGet", readBytesGet)) {
            collector.addStat(Key::ep_magma_read_bytes_get, readBytesGet);
            auto readAmpGet = divide(readBytesGet, bytesOutgoing);
            collector.addStat(Key::ep_magma_readamp_get, readAmpGet);

            // ReadIOAmp.
            size_t gets = 0;
            size_t readIOs = 0;
            if (statExists("magma_NGets", gets) &&
                statExists("magma_NReadIO", readIOs)) {
                collector.addStat(Key::ep_magma_readio, readIOs);
                collector.addStat(Key::ep_magma_readioamp,
                                  divide(readIOs, gets));
                collector.addStat(Key::ep_magma_bytes_per_read,
                                  divide(readBytesGet, gets));
            }
        }
    }

    // Compaction bytes read/written.
    addStat(Key::ep_magma_read_bytes_compact, "magma_NReadBytesCompact");
    addStat(Key::ep_magma_write_bytes_compact, "magma_NWriteBytesCompact");
    addStat(Key::ep_magma_keyindex_write_bytes_filecount_compact,
            "magma_KeyIndex_NWriteBytesFileCountCompact");
    addStat(Key::ep_magma_seqindex_write_bytes_filecount_compact,
            "magma_SeqIndex_NWriteBytesFileCountCompact");

    // Write amp.
    // To compute overall write amp.
    addStat(Key::ep_magma_bytes_incoming, "magma_BytesIncoming");
    addStat(Key::ep_magma_bytes_overwritten, "magma_BytesOverwritten");
    addStat(Key::ep_magma_write_bytes, "magma_NWriteBytes");
    addStat(Key::ep_io_total_write_bytes, "magma_FSWriteBytes");


    // To compute key index write amp
    addStat(Key::ep_magma_keyindex_bytes_incoming,
            "magma_KeyIndex_BytesIncoming");
    addStat(Key::ep_magma_keyindex_write_bytes, "magma_KeyIndex_NWriteBytes");

    // To compute seq index write amp.
    addStat(Key::ep_magma_seqindex_bytes_incoming,
            "magma_SeqIndex_BytesIncoming");
    addStat(Key::ep_magma_seqindex_write_bytes, "magma_SeqIndex_NWriteBytes");

    // To compute seq index delta level write amp.
    addStat(Key::ep_magma_seqindex_delta_bytes_incoming,
            "magma_SeqIndex_Delta_BytesIncoming");
    addStat(Key::ep_magma_seqindex_delta_write_bytes,
            "magma_SeqIndex_Delta_NWriteBytes");

    // Fragmentation.
    size_t logicalDataSize = 0;
    size_t logicalDiskSize = 0;
    size_t historyDiskUsage = 0;
    size_t historyDataSize = 0;
    if (statExists("magma_LogicalDataSize", logicalDataSize) &&
        statExists("magma_LogicalDiskSize", logicalDiskSize) &&
        statExists("magma_HistoryLogicalDiskSize", historyDiskUsage) &&
        statExists("magma_HistoryLogicalDataSize", historyDataSize)) {
        collector.addStat(Key::ep_magma_logical_data_size, logicalDataSize);
        collector.addStat(Key::ep_magma_logical_disk_size, logicalDiskSize);
        collector.addStat(Key::ep_magma_history_logical_data_size,
                          historyDataSize);
        collector.addStat(Key::ep_magma_history_logical_disk_size,
                          historyDiskUsage);
        double fragmentation =
                divide((logicalDiskSize - historyDiskUsage) -
                               (logicalDataSize - historyDataSize),
                       logicalDiskSize - historyDiskUsage);
        collector.addStat(Key::ep_magma_fragmentation, fragmentation);
    }

    // Disk usage.
    addStat(Key::ep_magma_total_disk_usage, "magma_TotalDiskUsage");
    addStat(Key::ep_magma_wal_disk_usage, "magma_WALDiskUsage");

    // Checkpointing related stats to make sure the overhead is within
    // configured limits.
    addStat(Key::ep_magma_checkpoint_disk_usage, "magma_CheckpointOverhead");
    addStat(Key::ep_magma_active_disk_usage, "magma_ActiveDiskUsage");

    // Memory usage.
    size_t blockCacheMemUsed = 0;
    if (statExists("magma_BlockCacheMemUsed", blockCacheMemUsed)) {
        collector.addStat(Key::ep_magma_block_cache_mem_used,
                          blockCacheMemUsed);

        size_t keyIndexSize = 0;
        size_t seqIndex_IndexBlockSize = 0;
        if (statExists("magma_KeyIndexSize", keyIndexSize) &&
            statExists("magma_SeqIndex_IndexBlockSize",
                       seqIndex_IndexBlockSize)) {
            auto total = keyIndexSize + seqIndex_IndexBlockSize;
            double residentRatio = divide(blockCacheMemUsed, total);
            collector.addStat(Key::ep_magma_index_resident_ratio,
                              residentRatio);
        }
    }
    addStat(Key::ep_magma_read_ahead_buffer_mem_used,
            "magma_ReadAheadBufferMemUsed");
    addStat(Key::ep_magma_histogram_mem_used, "magma_HistogramMemUsed");
    addStat(Key::ep_magma_table_object_mem_used, "magma_TableObjectMemUsed");
    addStat(Key::ep_magma_lsmtree_object_mem_used,
            "magma_LSMTreeObjectMemUsed");
    addStat(Key::ep_magma_write_cache_mem_used, "magma_WriteCacheMemUsed");
    addStat(Key::ep_magma_wal_mem_used, "magma_WALMemUsed");
    addStat(Key::ep_magma_table_meta_mem_used, "magma_TableMetaMemUsed");
    addStat(Key::ep_magma_buffer_mem_used, "magma_BufferMemUsed");
    addStat(Key::ep_magma_bloom_filter_mem_used,
            "magma_TotalBloomFilterMemUsed");
    addStat(Key::ep_magma_total_mem_used, "magma_TotalMemUsed");
    addStat(Key::ep_magma_tree_snapshot_mem_used, "magma_TreeSnapshotMemUsed");

    // Block cache.
    addStat(Key::ep_magma_block_cache_hits, "magma_BlockCacheHits");
    addStat(Key::ep_magma_block_cache_misses, "magma_BlockCacheMisses");

    // SST file counts.
    addStat(Key::ep_magma_tables_deleted, "magma_NTablesDeleted");
    addStat(Key::ep_magma_tables_created, "magma_NTablesCreated");
    addStat(Key::ep_magma_tables, "magma_NTableFiles");

    // NSyncs.
    addStat(Key::ep_magma_syncs, "magma_NSyncs");

    // Block Compression Ratio
    size_t dataBlocksUncompressedSize = 0;
    size_t dataBlocksCompressedSize = 0;
    if (statExists("magma_DataBlocksSize", dataBlocksUncompressedSize) &&
        statExists("magma_DataBlocksCompressSize", dataBlocksCompressedSize)) {
        collector.addStat(Key::ep_magma_data_blocks_uncompressed_size,
                          dataBlocksUncompressedSize);
        collector.addStat(Key::ep_magma_data_blocks_compressed_size,
                          dataBlocksCompressedSize);
        double compressionRatio =
                divide(dataBlocksUncompressedSize, dataBlocksCompressedSize);
        collector.addStat(Key::ep_magma_data_blocks_compression_ratio,
                          compressionRatio);
        double spaceReductionEstimatePct =
                divide((dataBlocksUncompressedSize - dataBlocksCompressedSize),
                       dataBlocksUncompressedSize) *
                100;
        collector.addStat(
                Key::ep_magma_data_blocks_space_reduction_estimate_pct,
                spaceReductionEstimatePct);
    }
}

void EventuallyPersistentEngine::doEngineStatsFusion(
        const StatCollector& collector) {
    using namespace cb::stats;

    // getStats from Magma
    constexpr std::array<std::string_view, 30> statNames = {
            {"fusion_NumSyncs",
             "fusion_NumSyncFailures",
             "fusion_NumBytesSynced",
             "fusion_SyncSessionTotalBytes",
             "fusion_SyncSessionCompletedBytes",
             "fusion_NumLogsMigrated",
             "fusion_NumLogsMounted",
             "fusion_NumMigrationFailures",
             "fusion_NumBytesMigrated",
             "fusion_MigrationTotalBytes",
             "fusion_MigrationCompletedBytes",
             "fusion_LogStoreDataSize",
             "fusion_LogStoreGarbageSize",
             "fusion_LogStoreSummarySectionSize",
             "fusion_LogStorePendingDeleteSize",
             "fusion_NumLogsCleaned",
             "fusion_NumLogCleanBytesRead",
             "fusion_NumLogCleanReads",
             "fusion_ExtentMergerReads",
             "fusion_ExtentMergerBytesRead",
             "fusion_NumLogStoreRemotePuts",
             "fusion_NumLogStoreReads",
             "fusion_NumLogStoreRemoteGets",
             "fusion_NumLogStoreRemoteLists",
             "fusion_NumLogStoreRemoteDeletes",
             "fusion_NumLogSegments",
             "fusion_NumFileExtents",
             "fusion_NumFiles",
             "fusion_TotalFileSize",
             "fusion_FileMapMemUsed"}};

    auto kvStoreStats = kvBucket->getKVStoreStats(statNames);

    // Return whether stat exists. If exists, save value in output param value.
    auto statExists = [&](std::string_view statName, size_t& value) {
        auto stat = kvStoreStats.find(statName);
        if (stat != kvStoreStats.end()) {
            value = stat->second;
            return true;
        }
        return false;
    };

    // If given stat exists, add it to collector.
    auto addStat = [&](Key key, std::string_view statName) {
        size_t value = 0;
        if (statExists(statName, value)) {
            collector.addStat(key, value);
        }
    };

    addStat(Key::ep_fusion_syncs, "fusion_NumSyncs");
    addStat(Key::ep_fusion_bytes_synced, "fusion_NumBytesSynced");
    addStat(Key::ep_fusion_logs_migrated, "fusion_NumLogsMigrated");
    addStat(Key::ep_fusion_bytes_migrated, "fusion_NumBytesMigrated");
    addStat(Key::ep_fusion_log_store_data_size, "fusion_LogStoreDataSize");
    addStat(Key::ep_fusion_log_store_garbage_size,
            "fusion_LogStoreGarbageSize");
    addStat(Key::ep_fusion_log_store_summary_section_size,
            "fusion_LogStoreSummarySectionSize");
    addStat(Key::ep_fusion_logs_cleaned, "fusion_NumLogsCleaned");
    addStat(Key::ep_fusion_log_store_remote_puts,
            "fusion_NumLogStoreRemotePuts");
    addStat(Key::ep_fusion_log_clean_bytes_read, "fusion_NumLogCleanBytesRead");
    addStat(Key::ep_fusion_log_clean_reads, "fusion_NumLogCleanReads");
    addStat(Key::ep_fusion_extent_merger_reads, "fusion_ExtentMergerReads");
    addStat(Key::ep_fusion_extent_merger_bytes_read,
            "fusion_ExtentMergerBytesRead");
    addStat(Key::ep_fusion_log_store_pending_delete_size,
            "fusion_LogStorePendingDeleteSize");
    addStat(Key::ep_fusion_log_store_reads, "fusion_NumLogStoreReads");
    addStat(Key::ep_fusion_log_store_remote_gets,
            "fusion_NumLogStoreRemoteGets");
    addStat(Key::ep_fusion_log_store_remote_lists,
            "fusion_NumLogStoreRemoteLists");
    addStat(Key::ep_fusion_log_store_remote_deletes,
            "fusion_NumLogStoreRemoteDeletes");
    addStat(Key::ep_fusion_file_map_mem_used, "fusion_FileMapMemUsed");
    addStat(Key::ep_fusion_sync_failures, "fusion_NumSyncFailures");
    addStat(Key::ep_fusion_migration_failures, "fusion_NumMigrationFailures");
    addStat(Key::ep_fusion_sync_session_total_bytes,
            "fusion_SyncSessionTotalBytes");
    addStat(Key::ep_fusion_sync_session_completed_bytes,
            "fusion_SyncSessionCompletedBytes");
    addStat(Key::ep_fusion_num_logs_mounted, "fusion_NumLogsMounted");
    addStat(Key::ep_fusion_migration_total_bytes, "fusion_MigrationTotalBytes");
    addStat(Key::ep_fusion_migration_completed_bytes,
            "fusion_MigrationCompletedBytes");
    addStat(Key::ep_fusion_total_file_size, "fusion_TotalFileSize");
    addStat(Key::ep_fusion_num_files, "fusion_NumFiles");
    addStat(Key::ep_fusion_num_file_extents, "fusion_NumFileExtents");
    addStat(Key::ep_fusion_num_log_segments, "fusion_NumLogSegments");

    // Additional Fusion Stats
    collector.addStat(Key::ep_fusion_namespace, getFusionNamespace());
}

cb::engine_errc EventuallyPersistentEngine::doEngineStats(
        const BucketStatCollector& collector, CookieIface* cookie) {
    cb::engine_errc status;
    if (status = doEngineStatsLowCardinality(collector, cookie);
        status != cb::engine_errc::success) {
        return status;
    }

    status = doEngineStatsHighCardinality(
            collector, cb::config::ExcludeWhenValueIsDefaultValue::No);
    return status;
}
cb::engine_errc EventuallyPersistentEngine::doEngineStatsLowCardinality(
        const BucketStatCollector& collector, CookieIface* cookie) {
    EPStats& epstats = getEpStats();

    using namespace cb::stats;

    collector.addStat(Key::ep_total_enqueued, epstats.getTotalEnqueued());
    collector.addStat(Key::ep_total_deduplicated, epstats.totalDeduplicated);
    collector.addStat(Key::ep_total_deduplicated_flusher,
                      epstats.totalDeduplicatedFlusher);
    collector.addStat(Key::ep_expired_access, epstats.expired_access);
    collector.addStat(Key::ep_expired_compactor, epstats.expired_compactor);
    collector.addStat(Key::ep_expired_pager, epstats.expired_pager);
    auto diskQueueSize = epstats.getDiskQueueSize();
    collector.addStat(Key::ep_queue_size, diskQueueSize);
    collector.addStat(Key::ep_diskqueue_items, diskQueueSize);
    auto* flusher = kvBucket->getOneFlusher();
    if (flusher) {
        collector.addStat(Key::ep_commit_num, epstats.flusherCommits);
        collector.addStat(Key::ep_commit_time, epstats.commit_time);
        collector.addStat(Key::ep_commit_time_total,
                          epstats.cumulativeCommitTime);
        collector.addStat(Key::ep_item_begin_failed, epstats.beginFailed);
        collector.addStat(Key::ep_item_commit_failed, epstats.commitFailed);
        collector.addStat(Key::ep_item_flush_expired, epstats.flushExpired);
        collector.addStat(Key::ep_item_flush_failed, epstats.flushFailed);
        collector.addStat(Key::ep_flusher_state, flusher->stateName());
        collector.addStat(Key::ep_flusher_todo, epstats.flusher_todo);
        collector.addStat(Key::ep_total_persisted, epstats.totalPersisted);
        collector.addStat(Key::ep_uncommitted_items, epstats.flusher_todo);
        collector.addStat(Key::ep_compaction_failed, epstats.compactionFailed);
        collector.addStat(Key::ep_compaction_aborted, epstats.compactionAborted);
    }
    collector.addStat(Key::ep_vbucket_del, epstats.vbucketDeletions);
    collector.addStat(Key::ep_vbucket_del_fail, epstats.vbucketDeletionFail);
    collector.addStat(Key::ep_flush_duration_total,
                      epstats.cumulativeFlushTime);

    kvBucket->getAggregatedVBucketStats(collector,
                                        cb::prometheus::MetricGroup::Low);

    collector.addStat(Key::ep_checkpoint_memory_pending_destruction,
                      kvBucket->getCheckpointPendingDestructionMemoryUsage());

    collector.addStat(Key::ep_checkpoint_memory_quota, kvBucket->getCMQuota());
    collector.addStat(Key::ep_checkpoint_consumer_limit,
                      kvBucket->getCheckpointConsumerLimit());
    collector.addStat(Key::ep_checkpoint_memory_recovery_upper_mark_bytes,
                      kvBucket->getCMRecoveryUpperMarkBytes());
    collector.addStat(Key::ep_checkpoint_memory_recovery_lower_mark_bytes,
                      kvBucket->getCMRecoveryLowerMarkBytes());
    collector.addStat(Key::ep_checkpoint_computed_max_size,
                      checkpointConfig->getCheckpointMaxSize());


    collector.addStat(Key::ep_persist_vbstate_total,
                      epstats.totalPersistVBState);

    collector.addStat(
            Key::mem_used_primary,
            cb::ArenaMalloc::getEstimatedAllocated(getArenaMallocClient(),
                                                   cb::MemoryDomain::Primary));
    collector.addStat(
            Key::mem_used_secondary,
            cb::ArenaMalloc::getEstimatedAllocated(
                    getArenaMallocClient(), cb::MemoryDomain::Secondary));
    collector.addStat(Key::mem_used_estimate,
                      stats.getEstimatedTotalMemoryUsed());

    // Note: Ordering of getPrecise is important - ask for it after requesting
    // the estimated values because a getPrecise call will update the estimate
    // to be the precise value. Doing this last means we can observe the
    // difference between estimate and precise
    size_t memUsed = stats.getPreciseTotalMemoryUsed();
    collector.addStat(Key::mem_used, memUsed);

    std::unordered_map<std::string, size_t> arenaStats;
    cb::ArenaMalloc::getStats(getArenaMallocClient(), arenaStats);
    auto allocated = arenaStats.find("allocated");
    if (allocated != arenaStats.end()) {
            collector.addStat(Key::ep_arena_memory_allocated, allocated->second);
    }
    auto resident = arenaStats.find("resident");
    if (resident != arenaStats.end()) {
        collector.addStat(Key::ep_arena_memory_resident, resident->second);
    }

    collector.addStat(Key::bytes, memUsed);
    collector.addStat(Key::ep_kv_size, stats.getCurrentSize());
    {
        auto blobNum = stats.getNumBlob();
        collector.addStat(Key::ep_blob_num, blobNum.loadNonNegative());
        collector.addStat(Key::ep_blob_num_allocated_total, blobNum.getAdded());
        collector.addStat(Key::ep_blob_num_freed_total, blobNum.getRemoved());
    }
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    collector.addStat(Key::ep_blob_overhead, stats.getBlobOverhead());
#else
    collector.addStat(Key::ep_blob_overhead, "unknown");
#endif
    {
        auto valueSize = stats.getTotalValueSize();
        collector.addStat(Key::ep_value_size, valueSize.loadNonNegative());
        collector.addStat(Key::ep_value_size_allocated_total, valueSize.getAdded());
        collector.addStat(Key::ep_value_size_freed_total, valueSize.getRemoved());
    }
    {
        auto storedvalSize = stats.getStoredValSize();
        collector.addStat(Key::ep_storedval_size,
                          storedvalSize.loadNonNegative());
        collector.addStat(Key::ep_storedval_size_allocated_total, storedvalSize.getAdded());
        collector.addStat(Key::ep_storedval_size_freed_total, storedvalSize.getRemoved());
    }
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    collector.addStat(Key::ep_storedval_overhead, stats.getStoredValOverhead());
#else
    collector.addStat(Key::ep_storedval_overhead, "unknown");
#endif
    {
        auto storedvalNum = stats.getNumStoredVal();
        collector.addStat(Key::ep_storedval_num,
                          storedvalNum.loadNonNegative());
        collector.addStat(Key::ep_storedval_num_allocated_total, storedvalNum.getAdded());
        collector.addStat(Key::ep_storedval_num_freed_total, storedvalNum.getRemoved());
    }
    collector.addStat(Key::ep_overhead, stats.getMemOverhead());
    {
        auto itemNum = stats.getNumItem();
        collector.addStat(Key::ep_item_num, itemNum.loadNonNegative());
        collector.addStat(Key::ep_item_num_allocated_total, itemNum.getAdded());
        collector.addStat(Key::ep_item_num_freed_total, itemNum.getRemoved());
    }

    collector.addStat(Key::ep_oom_errors, stats.oom_errors);
    collector.addStat(Key::ep_tmp_oom_errors, stats.tmp_oom_errors);
    collector.addStat(Key::ep_bg_fetched, epstats.bg_fetched);
    collector.addStat(Key::ep_bg_fetched_compaction,
                      epstats.bg_fetched_compaction);
    collector.addStat(Key::ep_bg_meta_fetched, epstats.bg_meta_fetched);
    collector.addStat(Key::ep_bg_remaining_items, epstats.numRemainingBgItems);
    collector.addStat(Key::ep_bg_remaining_jobs, epstats.numRemainingBgJobs);
    collector.addStat(Key::ep_num_pager_runs, epstats.pagerRuns);
    collector.addStat(Key::ep_num_expiry_pager_runs, epstats.expiryPagerRuns);
    collector.addStat(Key::ep_num_freq_decayer_runs, epstats.freqDecayerRuns);
    collector.addStat(Key::ep_items_expelled_from_checkpoints,
                      epstats.itemsExpelledFromCheckpoints);
    collector.addStat(Key::ep_items_rm_from_checkpoints,
                      epstats.itemsRemovedFromCheckpoints);
    collector.addStat(Key::ep_num_value_ejects, epstats.numValueEjects);
    collector.addStat(Key::ep_num_eject_failures, epstats.numFailedEjects);
    collector.addStat(Key::ep_num_not_my_vbuckets, epstats.numNotMyVBuckets);

    collector.addStat(Key::ep_pending_ops, epstats.pendingOps);
    collector.addStat(Key::ep_pending_ops_total, epstats.pendingOpsTotal);
    collector.addStat(Key::ep_pending_ops_max, epstats.pendingOpsMax);
    collector.addStat(Key::ep_pending_ops_max_duration,
                      epstats.pendingOpsMaxDuration);

    collector.addStat(Key::ep_rollback_count, epstats.rollbackCount);

    collector.addStat(Key::ep_degraded_mode, isDegradedMode());

    if (kvBucket->isExpPagerEnabled()) {
        std::array<char, 20> timestr;
        struct tm expPagerTim;
        hrtime_t expPagerTime = epstats.expPagerTime.load();
        if (cb_gmtime_r((time_t *)&expPagerTime, &expPagerTim) == -1) {
            collector.addStat(Key::ep_expiry_pager_task_time, "UNKNOWN");
        } else {
            strftime(timestr.data(), 20, "%Y-%m-%d %H:%M:%S", &expPagerTim);
            collector.addStat(Key::ep_expiry_pager_task_time, timestr.data());
        }
    } else {
        collector.addStat(Key::ep_expiry_pager_task_time, "NOT_SCHEDULED");
    }

    if (getConfiguration().getBucketTypeString() == "persistent" &&
        getConfiguration().isWarmup()) {
        Warmup* wp = kvBucket->getPrimaryWarmup();
        if (wp == nullptr) {
            throw std::logic_error("EPEngine::doEngineStats: warmup is NULL");
        }
        wp->addCommonStats(collector);
        wp = kvBucket->getSecondaryWarmup();
        if (wp) {
            wp->addSecondaryWarmupStatsToPrometheus(collector);
        }
    }

    if (getConfiguration().getBucketTypeString() == "ephemeral") {
        auto& ephemeralBucket = dynamic_cast<EphemeralBucket&>(*kvBucket);
        ephemeralBucket.doEphemeralMemRecoveryStats(collector);
    }

    collector.addStat(Key::ep_num_ops_get_meta, epstats.numOpsGetMeta);
    collector.addStat(Key::ep_num_ops_set_meta, epstats.numOpsSetMeta);
    collector.addStat(Key::ep_num_ops_del_meta, epstats.numOpsDelMeta);
    collector.addStat(Key::ep_num_ops_set_meta_res_fail,
                      epstats.numOpsSetMetaResolutionFailed +
                              epstats.numOpsSetMetaResolutionFailedIdentical);
    collector.addStat(Key::ep_num_ops_del_meta_res_fail,
                      epstats.numOpsDelMetaResolutionFailed +
                              epstats.numOpsDelMetaResolutionFailedIdentical);
    collector.addStat(Key::ep_num_ops_set_ret_meta, epstats.numOpsSetRetMeta);
    collector.addStat(Key::ep_num_ops_del_ret_meta, epstats.numOpsDelRetMeta);
    collector.addStat(Key::ep_num_ops_get_meta_on_set_meta,
                      epstats.numOpsGetMetaOnSetWithMeta);
    collector.addStat(Key::ep_workload_pattern,
                      workload->stringOfWorkLoadPattern());
    collector.addStat(Key::ep_num_invalid_cas, epstats.numInvalidCas);
    collector.addStat(Key::ep_num_cas_regenerated, epstats.numCasRegenerated);

    // these metrics do expose some duplicated information - for the sake
    // of supportability and understandability this is deemed acceptable

    collector.withLabels({{"op", "set"}, {"result", "accepted"}})
            .addStat(Key::conflicts_resolved, epstats.numOpsSetMeta);
    collector.withLabels({{"op", "del"}, {"result", "accepted"}})
            .addStat(Key::conflicts_resolved, epstats.numOpsDelMeta);

    collector.withLabels({{"op", "set"}, {"result", "rejected_behind"}})
            .addStat(Key::conflicts_resolved,
                     epstats.numOpsSetMetaResolutionFailed);
    collector.withLabels({{"op", "del"}, {"result", "rejected_behind"}})
            .addStat(Key::conflicts_resolved,
                     epstats.numOpsDelMetaResolutionFailed);

    collector.withLabels({{"op", "set"}, {"result", "rejected_identical"}})
            .addStat(Key::conflicts_resolved,
                     epstats.numOpsSetMetaResolutionFailedIdentical);
    collector.withLabels({{"op", "del"}, {"result", "rejected_identical"}})
            .addStat(Key::conflicts_resolved,
                     epstats.numOpsDelMetaResolutionFailedIdentical);


    kvBucket->getImplementationStats(collector);

    // Timing of all KVStore related stats
    const auto start = cb::time::steady_clock::now();

    doDiskFailureStats(collector);
    doDiskSlownessStats(collector);
    doContinuousBackupStats(collector);

    kvBucket->getFileStats(collector);

    // Note: These are also reported per-shard in 'kvstore' stats, however
    // we want to be able to graph these over time, and hence need to expose
    // to ns_sever at the top-level.
    if (configuration.getBackendString() == "couchdb") {
        doEngineStatsCouchDB(collector, epstats);
    } else if (configuration.getBackendString() == "magma") {
        doEngineStatsMagma(collector);
        if (isFusionSupportEnabled()) {
            doEngineStatsFusion(collector);
        }
    } else if (configuration.getBackendString() == "nexus") {
        auto primaryCollector = collector.withLabel("backend", "primary");
        if (configuration.getNexusPrimaryBackendString() == "couchdb") {
            doEngineStatsCouchDB(primaryCollector, epstats);
        } else if (configuration.getNexusPrimaryBackendString() == "magma") {
            doEngineStatsMagma(primaryCollector);
        }

        auto secondaryCollector = collector.withLabel("backend", "secondary");
        if (configuration.getNexusSecondaryBackendString() == "couchdb") {
            doEngineStatsCouchDB(secondaryCollector, epstats);
        } else if (configuration.getNexusSecondaryBackendString() == "magma") {
            doEngineStatsMagma(secondaryCollector);
        }
    }

    if (cookie) {
        NonBucketAllocationGuard guard;
        cookie->getTracer().record(
                Code::StorageEngineStats, start, cb::time::steady_clock::now());
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doEngineStatsHighCardinality(
        const BucketStatCollector& collector,
        cb::config::ExcludeWhenValueIsDefaultValue exclude) {
    configuration.addStats(collector, exclude);

    kvBucket->getAggregatedVBucketStats(collector,
                                        cb::prometheus::MetricGroup::High);

    EPStats& epstats = getEpStats();

    using namespace cb::stats;

    collector.addStat(Key::ep_startup_time, startupTime.load());

    if (getWorkloadPriority() == HIGH_BUCKET_PRIORITY) {
        collector.addStat(Key::ep_bucket_priority, "HIGH");
    } else if (getWorkloadPriority() == LOW_BUCKET_PRIORITY) {
        collector.addStat(Key::ep_bucket_priority, "LOW");
    }

    collector.addStat(Key::ep_mem_tracker_enabled,
                      EPStats::isMemoryTrackingEnabled());

    size_t numBgOps = epstats.bgNumOperations.load();
    if (numBgOps > 0) {
        collector.addStat(Key::ep_bg_num_samples, epstats.bgNumOperations);
        collector.addStat(Key::ep_bg_min_wait,
                          epstats.bgWaitHisto.getMinValue());
        collector.addStat(Key::ep_bg_max_wait,
                          epstats.bgWaitHisto.getMaxValue());
        collector.addStat(Key::ep_bg_wait_avg, epstats.bgWait / numBgOps);
        collector.addStat(Key::ep_bg_min_load,
                          epstats.bgLoadHisto.getMinValue());
        collector.addStat(Key::ep_bg_max_load,
                          epstats.bgLoadHisto.getMaxValue());
        collector.addStat(Key::ep_bg_load_avg, epstats.bgLoad / numBgOps);
        collector.addStat(Key::ep_bg_wait, epstats.bgWait);
        collector.addStat(Key::ep_bg_load, epstats.bgLoad);
    }

    collector.addStat(Key::ep_num_workers,
                      ExecutorPool::get()->getNumWorkersStat());

    size_t vbDeletions = epstats.vbucketDeletions.load();
    if (vbDeletions > 0) {
        collector.addStat(Key::ep_vbucket_del_max_walltime,
                          epstats.vbucketDelMaxWalltime);
        collector.addStat(Key::ep_vbucket_del_avg_walltime,
                          epstats.vbucketDelTotWalltime / vbDeletions);
    }

    collector.addStat(Key::ep_num_access_scanner_runs, epstats.alogRuns);
    collector.addStat(Key::ep_num_access_scanner_skips,
                      epstats.accessScannerSkips);
    collector.addStat(Key::ep_access_scanner_last_runtime, epstats.alogRuntime);
    collector.addStat(Key::ep_access_scanner_num_items, epstats.alogNumItems);

    if (kvBucket->isAccessScannerEnabled() && epstats.alogTime.load() != 0) {
        std::array<char, 20> timestr;
        struct tm alogTim;
        hrtime_t alogTime = epstats.alogTime.load();
        if (cb_gmtime_r((time_t*)&alogTime, &alogTim) == -1) {
            collector.addStat(Key::ep_access_scanner_task_time, "UNKNOWN");
        } else {
            strftime(timestr.data(), 20, "%Y-%m-%d %H:%M:%S", &alogTim);
            collector.addStat(Key::ep_access_scanner_task_time, timestr.data());
        }
    } else {
        collector.addStat(Key::ep_access_scanner_task_time, "NOT_SCHEDULED");
    }

    collector.addStat(Key::ep_defragmenter_num_visited,
                      epstats.defragNumVisited);
    collector.addStat(Key::ep_defragmenter_num_moved, epstats.defragNumMoved);
    collector.addStat(Key::ep_defragmenter_sv_num_moved,
                      epstats.defragStoredValueNumMoved);
    collector.addStat(Key::ep_defragmenter_sleep_time,
                      std::chrono::duration<double>(kvBucket->getDefragmenterTaskSleepTime()).count());

    collector.addStat(Key::ep_item_compressor_num_visited,
                      epstats.compressorNumVisited);
    collector.addStat(Key::ep_item_compressor_num_compressed,
                      epstats.compressorNumCompressed);

    collector.addStat(Key::ep_cursors_dropped, epstats.cursorsDropped);
    collector.addStat(Key::ep_mem_freed_by_checkpoint_removal,
                      epstats.memFreedByCheckpointRemoval);
    collector.addStat(Key::ep_mem_freed_by_checkpoint_item_expel,
                      epstats.memFreedByCheckpointItemExpel);
    {
        auto checkpointNum = stats.getNumCheckpoints();
        collector.addStat(Key::ep_num_checkpoints,
                          checkpointNum.loadNonNegative());
        collector.addStat(Key::ep_num_checkpoints_allocated_total, checkpointNum.getAdded());
        collector.addStat(Key::ep_num_checkpoints_freed_total, checkpointNum.getRemoved());
    }
    collector.addStat(Key::ep_num_checkpoints_pending_destruction,
                      kvBucket->getNumCheckpointsPendingDestruction());

    collector.addStat(Key::ep_mem_low_wat, epstats.mem_low_wat);
    collector.addStat(Key::ep_mem_high_wat, epstats.mem_high_wat);
    collector.addStat(Key::ep_snapshot_read, epstats.snapshotBytesRead);
    collector.addStat(Key::ep_dcp_cache_transfer_read,
                      epstats.cacheTransferBytesRead);

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doMemoryStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    add_casted_stat("mem_used_estimate",
                    stats.getEstimatedTotalMemoryUsed(),
                    add_stat,
                    cookie);
    auto memUsed = stats.getPreciseTotalMemoryUsed();
    add_casted_stat("bytes", memUsed, add_stat, cookie);
    add_casted_stat("mem_used", memUsed, add_stat, cookie);

    add_casted_stat("mem_used_merge_threshold",
                    getArenaMallocClient().estimateUpdateThreshold.load(),
                    add_stat,
                    cookie);

    // Note calling getEstimated as the precise value was requested previously
    // which will have updated these stats.
    add_casted_stat("ep_mem_used_primary",
                    cb::ArenaMalloc::getEstimatedAllocated(
                            getArenaMallocClient(), cb::MemoryDomain::Primary),
                    add_stat,
                    cookie);
    add_casted_stat(
            "ep_mem_used_secondary",
            cb::ArenaMalloc::getEstimatedAllocated(getArenaMallocClient(),
                                                   cb::MemoryDomain::Secondary),
            add_stat,
            cookie);

    add_casted_stat(
            "ht_mem_used_inactive", stats.inactiveHTMemory, add_stat, cookie);

    add_casted_stat("checkpoint_memory_overhead_inactive",
                    stats.inactiveCheckpointOverhead,
                    add_stat,
                    cookie);

    add_casted_stat("ep_kv_size", stats.getCurrentSize(), add_stat, cookie);
    add_casted_stat(
            "ep_value_size", stats.getTotalValueSize(), add_stat, cookie);
    add_casted_stat("ep_overhead", stats.getMemOverhead(), add_stat, cookie);
    auto quotaValue = stats.getMaxDataSize();
    add_casted_stat("ep_max_size", quotaValue, add_stat, cookie);
    auto desiredQuotaValue = stats.desiredMaxDataSize.load();
    if (desiredQuotaValue == 0) {
        // No quota change in progress, just return the actual quota
        desiredQuotaValue = quotaValue;
    }
    add_casted_stat("ep_desired_max_size", desiredQuotaValue, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat", stats.mem_low_wat, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat_percent",
                    ((double)stats.mem_low_wat / stats.getMaxDataSize()),
                    add_stat,
                    cookie);
    add_casted_stat("ep_mem_high_wat", stats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat_percent",
                    ((double)stats.mem_high_wat / stats.getMaxDataSize()),
                    add_stat,
                    cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat(
            "ep_tmp_oom_errors", stats.tmp_oom_errors, add_stat, cookie);

    add_casted_stat("ep_blob_num", stats.getNumBlob(), add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat(
            "ep_blob_overhead", stats.getBlobOverhead(), add_stat, cookie);
#else
    add_casted_stat("ep_blob_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat(
            "ep_storedval_size", stats.getStoredValSize(), add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat(
            "ep_storedval_overhead", stats.getBlobOverhead(), add_stat, cookie);
#else
    add_casted_stat("ep_storedval_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat(
            "ep_storedval_num", stats.getNumStoredVal(), add_stat, cookie);
    add_casted_stat("ep_item_num", stats.getNumItem(), add_stat, cookie);

    std::unordered_map<std::string, size_t> alloc_stats;
    bool missing =
            cb::ArenaMalloc::getStats(getArenaMallocClient(), alloc_stats);
    for (const auto& it : alloc_stats) {
        add_prefixed_stat("ep_arena", it.first, it.second, add_stat, cookie);
    }
    if (missing) {
        add_casted_stat("ep_arena_missing_some_keys", true, add_stat, cookie);
    }
    missing = cb::ArenaMalloc::getGlobalStats(alloc_stats);
    for (const auto& it : alloc_stats) {
        add_prefixed_stat(
                "ep_arena_global", it.first, it.second, add_stat, cookie);
    }
    if (missing) {
        add_casted_stat(
                "ep_arena_global_missing_some_keys", true, add_stat, cookie);
    }
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doVBucketStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view stat_key,
        VBucketStatsDetailLevel detail) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(KVBucketIface* store,
                           CookieIface& c,
                           AddStatFn a,
                           VBucketStatsDetailLevel detail)
            : eps(store), cookie(c), add_stat(std::move(a)), detail(detail) {
        }

        void visitBucket(VBucket& vb) override {
            addVBStats(cookie, add_stat, vb, eps, detail);
        }

        static void addVBStats(CookieIface& cookie,
                               const AddStatFn& add_stat,
                               VBucket& vb,
                               KVBucketIface* store,
                               VBucketStatsDetailLevel detail) {
            if (detail == VBucketStatsDetailLevel::PreviousState) {
                try {
                    std::array<char, 16> buf;
                    checked_snprintf(
                            buf.data(), buf.size(), "vb_%d", vb.getId().get());
                    add_casted_stat(buf.data(),
                                    VBucket::toString(vb.getInitialState()),
                                    add_stat,
                                    cookie);
                } catch (std::exception& error) {
                    EP_LOG_WARN_CTX("addVBStats: Failed building stats",
                                    {"error", error.what()});
                }
            } else {
                vb.addStats(detail, add_stat, cookie);
            }
        }

    private:
        KVBucketIface* eps;
        CookieIface& cookie;
        AddStatFn add_stat;
        VBucketStatsDetailLevel detail;
    };

    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    if (stat_key.size() > 16 && stat_key.starts_with("vbucket-details")) {
        Expects(detail == VBucketStatsDetailLevel::Full);
        auto [status, vb] = getValidVBucketFromString(stat_key.substr(16));
        if (status != cb::engine_errc::success) {
            return status;
        }
        StatVBucketVisitor::addVBStats(cookie,
                                       add_stat,
                                       *vb,
                                       kvBucket.get(),
                                       VBucketStatsDetailLevel::Full);
    } else if (stat_key.size() > 25 &&
               stat_key.starts_with("vbucket-durability-state")) {
        Expects(detail == VBucketStatsDetailLevel::Durability);
        auto [status, vb] = getValidVBucketFromString(stat_key.substr(25));
        if (status != cb::engine_errc::success) {
            return status;
        }
        StatVBucketVisitor::addVBStats(cookie,
                                       add_stat,
                                       *vb,
                                       kvBucket.get(),
                                       VBucketStatsDetailLevel::Durability);
    } else {
        StatVBucketVisitor svbv(kvBucket.get(), cookie, add_stat, detail);
        kvBucket->visit(svbv);
    }
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doEncryptionKeyIdsStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    auto rv = kvBucket->getEncryptionKeyIds();
    if (std::holds_alternative<cb::engine_errc>(rv)) {
        return std::get<cb::engine_errc>(rv);
    }

    auto& keys = std::get<std::unordered_set<std::string>>(rv);
    auto active = encryptionKeyProvider.lookup({});
    if (active) {
        std::string key(active->getId());
        keys.insert(key);
    }

    bool unencrypted = false;
    auto deks = cb::crypto::findDeksInUse(
            configuration.getDbname(),
            [&unencrypted](const auto& path) {
                if (!path.filename().string().starts_with("access.log")) {
                    return false;
                }
                if (path.extension() == ".cef") {
                    return true;
                }
                unencrypted = true;
                return false;
            },
            [](auto message, const auto& ctx) {
                LOG_WARNING_CTX(message, ctx);
            });
    if (unencrypted) {
        deks.insert(cb::crypto::DataEncryptionKey::UnencryptedKeyId);
    }
    keys.insert(deks.begin(), deks.end());

    add_stat("encryption-key-ids", nlohmann::json(keys).dump(), cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doHashStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(CookieIface& c,
                           AddStatFn a,
                           BucketCompressionMode compressMode)
            : cookie(c), add_stat(std::move(a)), compressionMode(compressMode) {
        }

        void visitBucket(VBucket& vb) override {
            Vbid vbid = vb.getId();
            std::array<char, 32> buf;
            try {
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:state", vbid.get());
                add_casted_stat(buf.data(),
                                VBucket::toString(vb.getState()),
                                add_stat,
                                cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN_CTX(
                        "StatVBucketVisitor::visitBucket: Failed to build stat",
                        {"error", error.what()});
            }

            HashTableDepthStatVisitor depthVisitor;
            vb.ht.visitDepth(depthVisitor);

            try {
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:size", vbid.get());
                add_casted_stat(buf.data(), vb.ht.getSize(), add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:locks", vbid.get());
                add_casted_stat(
                        buf.data(), vb.ht.getNumLocks(), add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:min_depth", vbid.get());
                add_casted_stat(buf.data(), depthVisitor.min, add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:max_depth", vbid.get());
                add_casted_stat(buf.data(), depthVisitor.max, add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:histo", vbid.get());
                add_casted_stat(
                        buf.data(), depthVisitor.depthHisto, add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:reported", vbid.get());
                add_casted_stat(buf.data(),
                                vb.ht.getNumInMemoryItems(),
                                add_stat,
                                cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:counted", vbid.get());
                add_casted_stat(
                        buf.data(), depthVisitor.size, add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:resized", vbid.get());
                add_casted_stat(
                        buf.data(), vb.ht.getNumResizes(), add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:mem_size", vbid.get());
                add_casted_stat(
                        buf.data(), vb.ht.getItemMemory(), add_stat, cookie);

                if (compressionMode != BucketCompressionMode::Off) {
                    checked_snprintf(buf.data(),
                                     buf.size(),
                                     "vb_%d:mem_size_uncompressed",
                                     vbid.get());
                    add_casted_stat(buf.data(),
                                    vb.ht.getUncompressedItemMemory(),
                                    add_stat,
                                    cookie);
                }
                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:mem_size_counted",
                                 vbid.get());
                add_casted_stat(
                        buf.data(), depthVisitor.memUsed, add_stat, cookie);

                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:num_system_items",
                                 vbid.get());
                add_casted_stat(buf.data(),
                                vb.ht.getNumSystemItems(),
                                add_stat,
                                cookie);
            } catch (const std::exception& error) {
                EP_LOG_WARN_CTX(
                        "StatVBucketVisitor::visitBucket: Failed to build stat",
                        {"error", error.what()});
            }
        }

        CookieIface& cookie;
        AddStatFn add_stat;
        BucketCompressionMode compressionMode;
    };

    StatVBucketVisitor svbv(cookie, add_stat, getCompressionMode());
    kvBucket->visit(svbv);

    return cb::engine_errc::success;
}

/**
 * Helper class which sends the contents of an output stream to the ADD_STAT
 * callback.
 *
 * Usage:
 *     {
 *         AddStatsStream as("stat_key", callback, cookie);
 *         as << obj << std::endl;
 *     }
 *     // When 'as' goes out of scope, it will invoke the ADD_STAT callback
 *     // with the key "stat_key" and value of everything streamed to it.
 */
class AddStatsStream : public std::ostream {
public:
    AddStatsStream(std::string key, AddStatFn callback, CookieIface& cookie)
        : std::ostream(&buf),
          key(std::move(key)),
          callback(std::move(callback)),
          cookie(cookie) {
    }

    ~AddStatsStream() override {
        auto value = buf.str();
        callback(key, value, cookie);
    }

private:
    std::string key;
    AddStatFn callback;
    CookieIface& cookie;
    std::stringbuf buf;
};

cb::engine_errc EventuallyPersistentEngine::doHashDump(
        CookieIface& cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != cb::engine_errc::success) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    as << result.vb->ht << std::endl;

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doCheckpointDump(
        CookieIface& cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != cb::engine_errc::success) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    as << *result.vb->checkpointManager << std::endl;

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doDurabilityMonitorDump(
        CookieIface& cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != cb::engine_errc::success) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    result.vb->dumpDurabilityMonitor(as);
    as << std::endl;

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doVBucketDump(
        CookieIface& cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != cb::engine_errc::success) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    result.vb->dump(as);
    as << std::endl;

    return cb::engine_errc::success;
}

class StatCheckpointVisitor : public VBucketVisitor {
public:
    StatCheckpointVisitor(KVBucketIface* kvs, CookieIface& c, AddStatFn a)
        : kvBucket(kvs), cookie(c), add_stat(std::move(a)) {
    }

    void visitBucket(VBucket& vb) override {
        addCheckpointStat(cookie, add_stat, kvBucket, vb);
    }

    static void addCheckpointStat(CookieIface& cookie,
                                  const AddStatFn& add_stat,
                                  KVBucketIface* eps,
                                  VBucket& vb) {
        Vbid vbid = vb.getId();
        std::array<char, 256> buf;
        try {
            checked_snprintf(buf.data(), buf.size(), "vb_%d:state", vbid.get());
            add_casted_stat(buf.data(),
                            VBucket::toString(vb.getState()),
                            add_stat,
                            cookie);
            vb.checkpointManager->addStats(add_stat, cookie);
        } catch (std::exception& error) {
            EP_LOG_WARN_CTX(
                    "StatCheckpointVisitor::addCheckpointStat: error building "
                    "stats",
                    {"error", error.what()});
        }
    }

    KVBucketIface* kvBucket;
    CookieIface& cookie;
    AddStatFn add_stat;
};
/// @endcond

cb::engine_errc EventuallyPersistentEngine::doCheckpointStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view stat_key) {
    if (stat_key.size() == 10) {
        TRACE_EVENT0("ep-engine/task", "StatsCheckpoint");
        auto* kvbucket = getKVBucket();
        StatCheckpointVisitor scv(kvbucket, cookie, add_stat);
        kvbucket->visit(scv);
        return cb::engine_errc::success;
    }
    if (stat_key.size() > 11) {
        auto [status, vb] = getValidVBucketFromString(stat_key.substr(11));
        if (status != cb::engine_errc::success) {
            return status;
        }
        StatCheckpointVisitor::addCheckpointStat(
                cookie, add_stat, kvBucket.get(), *vb);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doDurabilityMonitorStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view stat_key) {
    const uint8_t size = 18; // size  of "durability-monitor"
    if (stat_key.size() == size) {
        // Case stat_key = "durability-monitor"
        // @todo: Return aggregated stats for all VBuckets.
        //     Implement as async, we don't what to block for too long.
        return cb::engine_errc::not_supported;
    }
    if (stat_key.size() > size + 1) {
        // Case stat_key = "durability-monitor <vbid>"
        auto [status, vb] =
                getValidVBucketFromString(stat_key.substr(size + 1));
        if (status != cb::engine_errc::success) {
            return status;
        }
        vb->addDurabilityMonitorStats(add_stat, cookie);
    }

    return cb::engine_errc::success;
}

class DcpStatsOptions {
public:
    explicit DcpStatsOptions(std::string_view value) {
        if (!value.empty()) {
            try {
                auto attributes = nlohmann::json::parse(value);

                // Parse the optional stream_format parameter.
                auto iter = attributes.find("stream_format");
                if (iter != attributes.end()) {
                    auto format = iter->get<std::string>();
                    if (format == "skip") {
                        streamStatsFormat = {};
                    } else if (format == "legacy") {
                        streamStatsFormat =
                                ConnHandler::StreamStatsFormat::Legacy;
                    } else if (format == "json") {
                        streamStatsFormat =
                                ConnHandler::StreamStatsFormat::Json;
                    } else {
                        throw std::invalid_argument(iter.key());
                    }
                }

                auto filter = attributes.find("filter");
                if (filter != attributes.end()) {
                    iter = filter->find("user");
                    if (iter != filter->end()) {
                        user = iter->get<std::string>();
                    }
                    iter = filter->find("port");
                    if (iter != filter->end()) {
                        port = iter->get<in_port_t>();
                    }
                }
            } catch (const std::exception& e) {
                EP_LOG_ERR_CTX("Failed to decode provided DCP filter",
                               {"error", e.what()},
                               {"filter", value});
            }
        }
    }

    /**
     * The requested stream stats format. Stream stats should not be generated
     * if nullopt.
     */
    std::optional<ConnHandler::StreamStatsFormat> getStreamStatsFormat() const {
        return streamStatsFormat;
    }

    bool include(const ConnHandler& tc) {
        if ((user && *user != tc.getAuthenticatedUser()) ||
            (port && *port != tc.getConnectedPort())) {
            // Connection should not be part of this output
            return false;
        }

        return true;
    }

protected:
    std::optional<std::string> user;
    std::optional<in_port_t> port;
    std::optional<ConnHandler::StreamStatsFormat> streamStatsFormat{
            ConnHandler::StreamStatsFormat::Legacy};
};

/**
 * Function object to send stats for a single dcp connection.
 * Note this does not do per-stream stats.
 */
struct ConnStatBuilder {
    ConnStatBuilder(CookieIface& c, AddStatFn as, DcpStatsOptions options)
        : cookie(c), add_stat(std::move(as)), options(std::move(options)) {
    }

    void operator()(std::shared_ptr<ConnHandler> tc) {
        ++aggregator.totalConns;
        if (options.include(*tc)) {
            tc->addStats(add_stat, cookie);
            auto tp = std::dynamic_pointer_cast<DcpProducer>(tc);
            if (tp) {
                tp->aggregateQueueStats(aggregator);
            }
        }
    }

    const auto& getCounter() {
        return aggregator;
    }

    CookieIface& cookie;
    AddStatFn add_stat;
    DcpStatsOptions options;
    ConnCounter aggregator;
};

/**
 * Function object to send per-stream stats for a single dcp connection.
 */
struct ConnPerStreamStatBuilder {
    ConnPerStreamStatBuilder(DcpStatsOptions options)
        : options(std::move(options)) {
        // The stream stats format is required to emit stream stats.
        Expects(this->options.getStreamStatsFormat().has_value());
    }

    /**
     * Visit the next connection in the queue.
     */
    bool addStreamStats(CookieIface& cookie,
                        const AddStatFn& as,
                        const CheckYieldFn& check_yield) {
        while (!connQueue.empty()) {
            auto conn = connQueue.front();
            const auto shouldInclude = options.include(*conn);
            if (shouldInclude && check_yield()) {
                // More work to do, but we've been requested to yield.
                return false;
            }
            connQueue.pop();

            if (shouldInclude) {
                conn->addStreamStats(as, cookie, *options.getStreamStatsFormat());
            }
        }
        return true;
    }

    void operator()(std::shared_ptr<ConnHandler> tc) {
        connQueue.emplace(tc);
    }

    std::queue<std::shared_ptr<ConnHandler>> connQueue;
    DcpStatsOptions options;
};

struct ConnAggStatBuilder {
    /**
     * Construct with the separator.
     * @param sep The separator used for determining "type" of DCP connection
     *            by splitting the connection name with sep.
     */
    ConnAggStatBuilder(std::string_view sep) : sep(sep) {
    }

    /**
     * Get counter tracking stats for the given connection
     * type (e.g., replication, views).
     *
     * Connection name is expected to meet the pattern:
     *  [^:]*:conn_type<separator>.*
     * E.g., with a separator of ":":
     *   eq_dcpq:replication:ns_0@127.0.0.1->ns_1@127.0.0.1:default
     * maps to
     *   replication
     *
     * If the connection name does not follow this pattern,
     * returns nullptr.
     *
     * @param name connection name
     * @return counter for the given connection, or nullptr
     */
    ConnCounter* getCounterForConnType(std::string_view name) {
        // strip everything upto and including the first colon,
        // e.g., "eq_dcpq:"
        size_t pos1 = name.find(':');
        if (pos1 == std::string_view::npos) {
            return nullptr;
        }

        // Some connectors use the format:
        // `"i":"unique-information","a":"user-agent"`
        // user-agent starts with the connector name followed by a slash,
        // version number, and other details.
        if (pos1 + 1 < name.size() && name[pos1 + 1] == '"') {
            auto posKey = name.find(R"("a":")");
            if (posKey != std::string_view::npos) {
                auto userAgent = name.substr(posKey + 5);
                // Cut before the slash or double-quote.
                // If not found the whole will be used.
                userAgent = userAgent.substr(0, userAgent.find_first_of("\"/"));
                return &counters[std::string(userAgent)];
            }
        }

        name.remove_prefix(pos1 + 1);

        // find the given separator
        size_t pos2 = name.find(sep);
        if (pos2 == std::string_view::npos) {
            return &counters["_unknown"];
        }

        // extract upto given separator e.g.,
        // if the full conn name is:
        //  eq_dcpq:replication:ns_0@127.0.0.1->ns_1@127.0.0.1:default
        // and the given separator is: ":"
        // "eq_dcpq:" was stripped earlier so
        //  prefix is "replication"
        std::string prefix(name.substr(0, pos2));

        return &counters[prefix];
    }

    void aggregate(ConnHandler& conn, ConnCounter* tc) {
        ConnCounter counter;
        ++counter.totalConns;

        conn.aggregateQueueStats(counter);

        ConnCounter& total = getTotalCounter();
        total += counter;

        if (tc) {
            *tc += counter;
        }
    }

    ConnCounter& getTotalCounter() {
        return counters[std::string(sep) + "total"];
    }

    void operator()(std::shared_ptr<ConnHandler> tc) {
        if (tc) {
            ConnCounter* aggregator = getCounterForConnType(tc->getName());
            aggregate(*tc, aggregator);
        }
    }

    const auto& getCounters() {
        return counters;
    }

    std::map<std::string, ConnCounter> counters;
    std::string_view sep;
};

/// @endcond

static void showConnAggStat(const std::string& connType,
                            const ConnCounter& counter,
                            const BucketStatCollector& collector) {
    try {
        auto labelled = collector.withLabels({{"connection_type", connType}});

        using namespace cb::stats;
        labelled.addStat(Key::connagg_connection_count, counter.totalConns);
        labelled.addStat(Key::connagg_backoff, counter.conn_queueBackoff);
        labelled.addStat(Key::connagg_producer_count, counter.totalProducers);
        if (connType == "replication") {
            // Consumers are specific to replication, so only emit this metric
            // when relevant to avoid adding redundant zero count stats to
            // other connection types.
            labelled.addStat(Key::connagg_consumer_count,
                             counter.totalConns - counter.totalProducers);
        }
        labelled.addStat(Key::connagg_activestream_count,
                         counter.conn_activeStreams);
        labelled.addStat(Key::connagg_passivestream_count,
                         counter.conn_passiveStreams);
        labelled.addStat(Key::connagg_items_backfilled_disk,
                         counter.conn_backfillDisk);
        labelled.addStat(Key::connagg_items_backfilled_memory,
                         counter.conn_backfillMemory);
        labelled.addStat(Key::connagg_items_sent, counter.conn_queueDrain);
        labelled.addStat(Key::connagg_items_remaining,
                          counter.conn_queueRemaining);
        labelled.addStat(Key::connagg_total_bytes, counter.conn_totalBytes);
        labelled.addStat(Key::connagg_total_uncompressed_data_size,
                          counter.conn_totalUncompressedDataSize);
        labelled.addStat(Key::connagg_ready_queue_bytes,
                         counter.conn_queueMemory);
        labelled.addStat(Key::connagg_paused, counter.conn_paused);
        labelled.addStat(Key::connagg_unpaused, counter.conn_unpaused);

    } catch (std::exception& error) {
        EP_LOG_WARN_CTX("showConnAggStat: Failed to build stats",
                        {"error", error.what()});
    }
}

cb::engine_errc EventuallyPersistentEngine::doConnAggStats(
        const BucketStatCollector& collector, std::string_view sep) {
    // The separator is, in all current usage, ":" so the length will
    // normally be 1
    const size_t max_sep_len(8);
    sep = sep.substr(0, max_sep_len);

    ConnAggStatBuilder visitor(sep);
    dcpConnMap_->each(visitor);

    for (const auto& [connType, counter] : visitor.getCounters()) {
        // connType may be "replication", "views" etc. or ":total"
        if (connType == ":total" && !collector.includeAggregateMetrics()) {
            // Prometheus should not expose aggregations under the same
            // metric names, as this makes summing across labels
            // more difficult
            continue;
        }
        showConnAggStat(connType, counter, collector);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doDcpStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        const CheckYieldFn& check_yield,
        std::string_view value) {
    if (auto optDcpStreamVisitor =
                takeEngineSpecific<std::unique_ptr<ConnPerStreamStatBuilder>>(
                        cookie)) {
        bool hasCompleted = optDcpStreamVisitor.value()->addStreamStats(
                cookie, add_stat, check_yield);
        if (!hasCompleted) {
            // Continue on the next run.
            storeEngineSpecific(cookie, std::move(*optDcpStreamVisitor));
            return cb::engine_errc::throttled;
        }
        return cb::engine_errc::success;
    }

    DcpStatsOptions options{value};
    ConnStatBuilder dcpVisitor(cookie, add_stat, options);
    dcpConnMap_->each(dcpVisitor);

    const auto& aggregator = dcpVisitor.getCounter();

    CBStatCollector collector(add_stat, cookie);
    addAggregatedProducerStats(collector.forBucket(getName()), aggregator);

    dcpConnMap_->addStats(add_stat, cookie);

    // Check if we need to generate any stream stats.
    if (!options.getStreamStatsFormat().has_value()) {
        return cb::engine_errc::success;
    }

    // Store the per-stream visitor in the cookie and yield back to caller. We
    // will process the per-stream stats next time we're called.
    auto dcpStreamVisitor = std::make_unique<ConnPerStreamStatBuilder>(options);
    // Dump the contents on the ConnMap into the visitor. This will not actually
    // generate any stats yet.
    dcpConnMap_->each(*dcpStreamVisitor);
    storeEngineSpecific(cookie, std::move(dcpStreamVisitor));

    return cb::engine_errc::throttled;
}

void EventuallyPersistentEngine::addAggregatedProducerStats(
        const BucketStatCollector& col, const ConnCounter& aggregator) {
    using namespace cb::stats;
    col.addStat(Key::dcp_count, aggregator.totalConns);
    col.addStat(Key::dcp_producer_count, aggregator.totalProducers);
    col.addStat(Key::dcp_consumer_count,
                aggregator.totalConns - aggregator.totalProducers);
    col.addStat(Key::dcp_total_data_size, aggregator.conn_totalBytes);
    col.addStat(Key::dcp_total_uncompressed_data_size,
                aggregator.conn_totalUncompressedDataSize);
    col.addStat(Key::dcp_queue_fill, aggregator.conn_queueFill);
    col.addStat(Key::dcp_queue_backfill_disk, aggregator.conn_backfillDisk);
    col.addStat(Key::dcp_queue_backfill_memory, aggregator.conn_backfillMemory);
    col.addStat(Key::dcp_items_sent, aggregator.conn_queueDrain);
    col.addStat(Key::dcp_items_remaining, aggregator.conn_queueRemaining);
    col.addStat(
            Key::dcp_num_running_backfills,
            getKVBucket()->getKVStoreScanTracker().getNumRunningBackfills());
    col.addStat(
            Key::dcp_max_running_backfills,
            getKVBucket()->getKVStoreScanTracker().getMaxRunningBackfills());
}

cb::engine_errc EventuallyPersistentEngine::doEvictionStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    /**
     * The "evicted" histogram stats provide an aggregated view of what the
     * execution frequencies are for all the items that evicted when running
     * the hifi_mfu algorithm.
     */
    add_casted_stat("ep_active_or_pending_eviction_values_evicted",
                    stats.activeOrPendingFrequencyValuesEvictedHisto,
                    add_stat,
                    cookie);
    add_casted_stat("ep_replica_eviction_values_evicted",
                    stats.replicaFrequencyValuesEvictedHisto,
                    add_stat,
                    cookie);
    /**
     * The "snapshot" histogram stats provide a view of what the contents of
     * the frequency histogram is like during the running of the hifi_mfu
     * algorithm.
     */
    add_casted_stat("ep_active_or_pending_eviction_values_snapshot",
                    stats.activeOrPendingFrequencyValuesSnapshotHisto,
                    add_stat,
                    cookie);
    add_casted_stat("ep_replica_eviction_values_snapshot",
                    stats.replicaFrequencyValuesSnapshotHisto,
                    add_stat,
                    cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doKeyStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        Vbid vbid,
        const DocKeyView& key,
        bool validate) {
    auto rv = checkCollectionAccess(cookie,
                                    vbid,
                                    cb::rbac::Privilege::SystemCollectionLookup,
                                    cb::rbac::Privilege::Read,
                                    key.getCollectionID());
    if (rv != cb::engine_errc::success) {
        return rv;
    }

    std::unique_ptr<Item> it;
    struct key_stats kstats;

    // If this is a validating call, we need to fetch the item from disk. The
    // fetch task is scheduled by statsVKey(). fetchLookupResult will succeed
    // in returning the item once the fetch task has completed.
    if (validate && !fetchLookupResult(cookie, it)) {
        return maybeRemapStatus(kvBucket->statsVKey(key, vbid, cookie));
    }

    rv = kvBucket->getKeyStats(key, vbid, cookie, kstats, WantsDeleted::No);
    if (rv == cb::engine_errc::success) {
        std::string valid("this_is_a_bug");
        if (validate) {
            if (kstats.dirty) {
                valid.assign("dirty");
            } else if (it) {
                valid.assign(kvBucket->validateKey(key, vbid, *it));
            } else {
                valid.assign("ram_but_not_disk");
            }
            EP_LOG_DEBUG_CTX("doKeyStats",
                             {"key", cb::UserDataView(key.to_string())},
                             {"status", valid});
        }
        add_casted_stat("key_is_dirty", kstats.dirty, add_stat, cookie);
        add_casted_stat("key_exptime", kstats.exptime, add_stat, cookie);
        add_casted_stat("key_datatype",
                        cb::mcbp::datatype::to_string(kstats.datatype),
                        add_stat,
                        cookie);
        add_casted_stat("key_flags", kstats.flags, add_stat, cookie);
        add_casted_stat("key_cas", kstats.cas, add_stat, cookie);
        add_casted_stat("key_vb_state", VBucket::toString(kstats.vb_state),
                        add_stat,
                        cookie);
        add_casted_stat("key_is_resident", kstats.resident, add_stat, cookie);
        if (validate) {
            add_casted_stat("key_valid", valid, add_stat, cookie);
        }
    }
    return rv;
}

cb::engine_errc EventuallyPersistentEngine::doVbIdFailoverLogStats(
        CookieIface& cookie, const AddStatFn& add_stat, Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if(!vb) {
        return cb::engine_errc::not_my_vbucket;
    }
    vb->failovers->addStats(cookie, vb->getId(), add_stat);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doAllFailoverLogStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    cb::engine_errc rv = cb::engine_errc::success;
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(CookieIface& c, AddStatFn a)
            : cookie(c), add_stat(std::move(a)) {
        }

        void visitBucket(VBucket& vb) override {
            vb.failovers->addStats(cookie, vb.getId(), add_stat);
        }

    private:
        CookieIface& cookie;
        AddStatFn add_stat;
    };

    StatVBucketVisitor svbv(cookie, add_stat);
    kvBucket->visit(svbv);

    return rv;
}

void EventuallyPersistentEngine::doTimingStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    collector.addStat(Key::bg_wait, stats.bgWaitHisto);
    collector.addStat(Key::bg_load, stats.bgLoadHisto);
    collector.addStat(Key::set_with_meta, stats.setWithMetaHisto);
    collector.addStat(Key::pending_ops, stats.pendingOpsHisto);

    // Vbucket visitors
    collector.addStat(Key::checkpoint_remover, stats.checkpointRemoverHisto);
    collector.addStat(Key::item_pager, stats.itemPagerHisto);
    collector.addStat(Key::expiry_pager, stats.expiryPagerHisto);
    collector.addStat(Key::storage_age, stats.dirtyAgeHisto);

    // Regular commands
    collector.addStat(Key::get_cmd, stats.getCmdHisto);
    collector.addStat(Key::store_cmd, stats.storeCmdHisto);
    collector.addStat(Key::arith_cmd, stats.arithCmdHisto);
    collector.addStat(Key::get_stats_cmd, stats.getStatsCmdHisto);

    // Admin commands
    collector.addStat(Key::get_vb_cmd, stats.getVbucketCmdHisto);
    collector.addStat(Key::set_vb_cmd, stats.setVbucketCmdHisto);
    collector.addStat(Key::del_vb_cmd, stats.delVbucketCmdHisto);

    // Misc
    collector.addStat(Key::notify_io, stats.notifyIOHisto);

    // Disk stats
    collector.addStat(Key::disk_insert, stats.diskInsertHisto);
    collector.addStat(Key::disk_update, stats.diskUpdateHisto);
    collector.addStat(Key::disk_del, stats.diskDelHisto);
    collector.addStat(Key::disk_vb_del, stats.diskVBDelHisto);
    collector.addStat(Key::disk_commit, stats.diskCommitHisto);

    collector.addStat(Key::item_alloc_sizes, stats.itemAllocSizeHisto);
    collector.addStat(Key::bg_batch_size, stats.getMultiBatchSizeHisto);

    // Checkpoint cursor stats
    collector.addStat(Key::persistence_cursor_get_all_items,
                      stats.persistenceCursorGetItemsHisto);
    collector.addStat(Key::dcp_cursors_get_all_items,
                      stats.dcpCursorsGetItemsHisto);

    // SyncWrite stats
    collector.addStat(Key::sync_write_commit_majority,
                      stats.syncWriteCommitTimes.at(0));
    collector.addStat(Key::sync_write_commit_majority_and_persist_on_master,
                      stats.syncWriteCommitTimes.at(1));
    collector.addStat(Key::sync_write_commit_persist_to_majority,
                      stats.syncWriteCommitTimes.at(2));
}

cb::engine_errc EventuallyPersistentEngine::doFrequencyCountersStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;

    VBucketEvictableMFUVisitor activeVisitor(vbucket_state_active);
    VBucketEvictableMFUVisitor replicaVisitor(vbucket_state_replica);
    VBucketEvictableMFUVisitor pendingVisitor(vbucket_state_pending);

    kvBucket->visitAll(activeVisitor, replicaVisitor, pendingVisitor);

    collector.addStat(Key::vb_evictable_mfu,
                      activeVisitor.getHistogramData(),
                      {{"state", "active"}});
    collector.addStat(Key::vb_evictable_mfu,
                      replicaVisitor.getHistogramData(),
                      {{"state", "replica"}});
    collector.addStat(Key::vb_evictable_mfu,
                      pendingVisitor.getHistogramData(),
                      {{"state", "pending"}});

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doSchedulerStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(GlobalTask::getTaskIdString(id),
                        stats.schedulingHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doRunTimeStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    for (TaskId id : GlobalTask::allTaskIds) {
        auto& hist = stats.taskRuntimeHisto[static_cast<int>(id)];
        if (hist.isEmpty()) {
            continue;
        }

        auto labelled = collector.withLabels(
                {{"task", GlobalTask::getTaskName(id)},
                 {"type", to_string(GlobalTask::getTaskType(id))}});
        labelled.addStat(Key::task_duration, hist);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doDispatcherStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    ExecutorPool::get()->doWorkerStat(
            ObjectRegistry::getCurrentEngine()->getTaskable(),
            cookie,
            add_stat);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doTasksStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    ExecutorPool::get()->doTasksStat(
            ObjectRegistry::getCurrentEngine()->getTaskable(),
            cookie,
            add_stat);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doWorkloadStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    try {
        std::array<char, 80> statname;
        ExecutorPool* expool = ExecutorPool::get();

        auto readers = expool->getNumReaders();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_readers");
        add_casted_stat(statname.data(), readers, add_stat, cookie);

        auto writers = expool->getNumWriters();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_writers");
        add_casted_stat(statname.data(), writers, add_stat, cookie);

        auto auxio = expool->getNumAuxIO();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_auxio");
        add_casted_stat(statname.data(), auxio, add_stat, cookie);

        auto nonio = expool->getNumNonIO();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_nonio");
        add_casted_stat(statname.data(), nonio, add_stat, cookie);

        auto threadsPerCore = expool->getNumIOThreadsPerCore();
        checked_snprintf(statname.data(),
                         statname.size(),
                         "ep_workload:num_io_threads_per_core");
        add_casted_stat(statname.data(), threadsPerCore, add_stat, cookie);

        auto shards = workload->getNumShards();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_shards");
        add_casted_stat(statname.data(), shards, add_stat, cookie);

        auto numReadyTasks = expool->getNumReadyTasks();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:ready_tasks");
        add_casted_stat(statname.data(), numReadyTasks, add_stat, cookie);

        auto numSleepers = expool->getNumSleepers();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_sleepers");
        add_casted_stat(statname.data(), numSleepers, add_stat, cookie);

        expool->doTaskQStat(ObjectRegistry::getCurrentEngine()->getTaskable(),
                            cookie,
                            add_stat);

    } catch (std::exception& error) {
        EP_LOG_WARN_CTX("doWorkloadStats: Error building stats",
                        {"error", error.what()});
    }

    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::addLookupResult(CookieIface& cookie,
                                                 std::unique_ptr<Item> result) {
    auto oldItem = takeEngineSpecific<std::unique_ptr<Item>>(cookie);
    // Check for old lookup results and clear them
    if (oldItem) {
        if (*oldItem) {
            EP_LOG_DEBUG_CTX(
                    "Cleaning up old lookup result",
                    {"key",
                     cb::UserDataView{(*oldItem)->getKey().to_string()}});
        } else {
            EP_LOG_DEBUG_RAW("Cleaning up old null lookup result");
        }
    }

    storeEngineSpecific(cookie, std::move(result));
}

bool EventuallyPersistentEngine::fetchLookupResult(CookieIface& cookie,
                                                   std::unique_ptr<Item>& itm) {
    // This will return *and erase* the lookup result for a connection.
    // You look it up, you own it.
    auto es = takeEngineSpecific<std::unique_ptr<Item>>(cookie);
    if (es) {
        itm = std::move(*es);
        return true;
    }
    return false;
}

EventuallyPersistentEngine::StatusAndVBPtr
EventuallyPersistentEngine::getValidVBucketFromString(std::string_view vbNum) {
    if (vbNum.empty()) {
        // Must specify a vbucket.
        return {cb::engine_errc::invalid_arguments, {}};
    }
    uint16_t vbucket_id;
    if (!safe_strtous(vbNum, vbucket_id)) {
        return {cb::engine_errc::invalid_arguments, {}};
    }
    Vbid vbid = Vbid(vbucket_id);
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return {cb::engine_errc::not_my_vbucket, {}};
    }
    return {cb::engine_errc::success, vb};
}

cb::engine_errc EventuallyPersistentEngine::doSeqnoStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view stat_key) {
    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    if (stat_key.size() > 14) {
        auto [status, vb] = getValidVBucketFromString(stat_key.substr(14));
        if (status != cb::engine_errc::success) {
            return status;
        }
        if (vb->getState() == vbucket_state_dead) {
            return cb::engine_errc::not_my_vbucket;
        }
        vb->doSeqnoStats(add_stat, cookie);
        return cb::engine_errc::success;
    }

    auto vbuckets = kvBucket->getVBuckets().getBuckets();
    for (auto vbid : vbuckets) {
        VBucketPtr vb = getVBucket(vbid);
        if (vb) {
            vb->doSeqnoStats(add_stat, cookie);
        }
    }
    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::runDefragmenterTask() {
    kvBucket->runDefragmenterTask();
}

bool EventuallyPersistentEngine::runAccessScannerTask() {
    return kvBucket->runAccessScannerTask();
}

void EventuallyPersistentEngine::runVbStatePersistTask(Vbid vbid) {
    kvBucket->runVbStatePersistTask(vbid);
}

cb::engine_errc EventuallyPersistentEngine::doCollectionStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    CBStatCollector collector(add_stat, cookie);
    auto bucketCollector = collector.forBucket(getName());
    auto res = Collections::Manager::doCollectionStats(
            *this, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_collection ||
        res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie, res.getManifestId());
    }
    return res.result;
}

cb::engine_errc EventuallyPersistentEngine::doScopeStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    CBStatCollector collector(add_stat, cookie);
    auto bucketCollector = collector.forBucket(getName());
    auto res =
            Collections::Manager::doScopeStats(*this, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie, res.getManifestId());
    }
    return res.result;
}

cb::EngineErrorGetCollectionIDResult
EventuallyPersistentEngine::parseKeyStatCollection(
        std::string_view expectedStatPrefix,
        std::string_view statKeyArg,
        std::string_view collectionStr) {
    CollectionID cid(CollectionID::Default);
    if (statKeyArg == expectedStatPrefix) {
        // provided argument should be a collection path
        auto res = kvBucket->getCollectionsManager().getCollectionID(
                collectionStr);
        cid = res.getCollectionId();
        if (res.result != cb::engine_errc::success) {
            EP_LOG_WARN_CTX(
                    "EventuallyPersistentEngine::parseKeyStatCollection could "
                    "not find collection",
                    {"arg", collectionStr},
                    {"status", res.result});
        }
        return res;
    }
    if (statKeyArg == (std::string(expectedStatPrefix) + "-byid") &&
        collectionStr.size() > 2) {
        // provided argument should be a hex collection ID N, 0xN or 0XN
        try {
            cid = Collections::makeCollectionIDFromString(collectionStr);
        } catch (const std::exception& e) {
            EP_LOG_WARN_CTX(
                    "EventuallyPersistentEngine::parseKeyStatCollection "
                    "invalid collection",
                    {"arg", collectionStr},
                    {"status", e.what()});
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::invalid_arguments};
        }
        // Collection's scope is needed for privilege check
        auto [manifesUid, meta] =
                kvBucket->getCollectionsManager().getCollectionEntry(cid);
        if (meta) {
            return {manifesUid,
                    meta->sid,
                    cid,
                    Collections::isSystemCollection(meta->name, cid)};
        }
        return {cb::engine_errc::unknown_collection, manifesUid};
    }
    return cb::EngineErrorGetCollectionIDResult(
            cb::engine_errc::invalid_arguments);
}

std::tuple<cb::engine_errc,
           std::optional<Vbid>,
           std::optional<std::string>,
           std::optional<CollectionID>>
EventuallyPersistentEngine::parseStatKeyArg(CookieIface& cookie,
                                            std::string_view statKeyPrefix,
                                            std::string_view statKey) {
    std::vector<std::string> args;
    std::string trimmedStatKey(statKey);
    boost::algorithm::trim(trimmedStatKey);
    boost::split(args, trimmedStatKey, boost::is_space());
    if (args.size() != 3 && args.size() != 4) {
        return {cb::engine_errc::invalid_arguments,
                std::nullopt,
                std::nullopt,
                std::nullopt};
    }

    Vbid vbid(0);
    try {
        vbid = Vbid(gsl::narrow<uint16_t>(std::stoi(args[2])));
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX(
                "EventuallyPersistentEngine::doKeyStats invalid "
                "vbucket",
                {"arg", args[2]},
                {"error", e.what()});
        return {cb::engine_errc::invalid_arguments,
                std::nullopt,
                std::nullopt,
                std::nullopt};
    }

    CollectionID cid(CollectionID::Default);
    if (args.size() == 4) {
        cb::EngineErrorGetCollectionIDResult res{
                cb::engine_errc::unknown_collection};
        // An argument was provided, maybe an id or a 'path'
        auto cidResult =
                parseKeyStatCollection(statKeyPrefix, args[0], args[3]);
        if (cidResult.result != cb::engine_errc::success) {
            if (cidResult.result == cb::engine_errc::unknown_collection) {
                setUnknownCollectionErrorContext(cookie,
                                                 cidResult.getManifestId());
            }
            return {cidResult.result, std::nullopt, std::nullopt, std::nullopt};
        }
        cid = cidResult.getCollectionId();
    }

    return {cb::engine_errc::success, vbid, args[1], cid};
}

cb::engine_errc EventuallyPersistentEngine::doKeyStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    const auto [status, vbid, key, cid] =
            parseStatKeyArg(cookie, "key", statKey);
    if (status != cb::engine_errc::success) {
        return status;
    }
    auto docKey = StoredDocKey(*key, *cid);
    return doKeyStats(cookie, add_stat, *vbid, docKey, false);
}

cb::engine_errc EventuallyPersistentEngine::doVKeyStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    const auto [status, vbid, key, cid] =
            parseStatKeyArg(cookie, "vkey", statKey);
    if (status != cb::engine_errc::success) {
        return status;
    }
    auto docKey = StoredDocKey(*key, *cid);
    return doKeyStats(cookie, add_stat, *vbid, docKey, true);
}

cb::engine_errc EventuallyPersistentEngine::doDcpVbTakeoverStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    std::string tStream;
    std::string vbid;
    std::string buffer(statKey.data() + 15, statKey.size() - 15);
    std::stringstream ss(buffer);
    ss >> vbid;
    ss >> tStream;
    uint16_t vbucket_id(0);
    safe_strtous(vbid, vbucket_id);
    Vbid vbucketId = Vbid(vbucket_id);
    return doDcpVbTakeoverStats(cookie, add_stat, tStream, vbucketId);
}

cb::engine_errc EventuallyPersistentEngine::doFailoversStats(
        CookieIface& cookie, const AddStatFn& add_stat, std::string_view key) {
    const std::string statKey(key.data(), key.size());
    if (key.size() == 9) {
        return doAllFailoverLogStats(cookie, add_stat);
    }

    if (statKey.compare(std::string("failovers").length(),
                        std::string(" ").length(),
                        " ") == 0) {
        std::string vbid;
        std::string s_key(statKey.substr(10, key.size() - 10));
        std::stringstream ss(s_key);
        ss >> vbid;
        uint16_t vbucket_id(0);
        safe_strtous(vbid, vbucket_id);
        Vbid vbucketId = Vbid(vbucket_id);
        return doVbIdFailoverLogStats(cookie, add_stat, vbucketId);
    }

    return cb::engine_errc::no_such_key;
}

cb::engine_errc EventuallyPersistentEngine::doDiskinfoStats(
        CookieIface& cookie, const AddStatFn& add_stat, std::string_view key) {
    const std::string statKey(key.data(), key.size());
    if (key.size() == 8) {
        CBStatCollector collector{add_stat, cookie};
        auto bucketC = collector.forBucket(getName());
        return kvBucket->getFileStats(bucketC);
    }
    if ((key.size() == 15) &&
        (statKey.compare(std::string("diskinfo").length() + 1,
                         std::string("detail").length(),
                         "detail") == 0)) {
        return kvBucket->getPerVBucketDiskStats(cookie, add_stat);
    }

    return cb::engine_errc::invalid_arguments;
}

void EventuallyPersistentEngine::doDiskFailureStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;

    size_t value = 0;

    // Total data write failures is compaction failures plus commit failures
    auto writeFailure = stats.commitFailed + stats.compactionFailed;
    collector.addStat(Key::ep_data_write_failed, writeFailure);

    if (kvBucket->getKVStoreStat("failure_get", value)) {
        collector.addStat(Key::ep_data_read_failed, value);
    }
}

void EventuallyPersistentEngine::doContinuousBackupStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;

    size_t value = 0;
    if (kvBucket->getKVStoreStat("continuous_backup_callback_count", value)) {
        collector.addStat(Key::ep_continuous_backup_callback_count, value);
    }
    if (kvBucket->getKVStoreStat("continuous_backup_callback_micros", value)) {
        collector.addStat(Key::ep_continuous_backup_callback_time, value);
    }
}

void EventuallyPersistentEngine::iteratePendingDiskOps(
        const std::function<void(bool isDataWrite,
                                 std::chrono::microseconds adjustedElapsed)>&
                callback) {
    // Current point in time.
    auto now = ep_uptime_now();
    fileOpsTracker->visitThreads(
            [this, now, &callback](auto type, auto thread, const auto& op) {
                if (type != TaskType::Reader && type != TaskType::Writer) {
                    return;
                }
                auto elapsed = now - op.startTime;

                // For large operations, scale the duration by a factor based
                // on the maximum item size, to handle cases where we're writing
                // more than 20 MiB in one operation.
                double elapsedScalingFactor = std::max(
                        1.0, static_cast<double>(op.nbytes) / maxItemSize);
                auto effectiveElapsed = elapsed / elapsedScalingFactor;

                callback(op.isDataWrite(),
                         std::chrono::duration_cast<std::chrono::microseconds>(
                                 effectiveElapsed));
            });
}

cb::engine_errc EventuallyPersistentEngine::doDiskSlownessStats(
        CookieIface& cookie, const AddStatFn& add_stat, std::string_view key) {
    using namespace cb::stats;
    using std::to_string;

    if (!key.starts_with("disk-slowness ")) {
        return cb::engine_errc::invalid_arguments;
    }

    auto arg = key.substr(key.find(' ') + 1);
    uint32_t thresholdSeconds;
    if (!safe_strtoul(arg, thresholdSeconds)) {
        return cb::engine_errc::invalid_arguments;
    }

    struct OpCounts {
        int numTotal = 0;
        int numSlow = 0;
    };
    OpCounts readCounts;
    OpCounts writeCounts;

    std::chrono::seconds threshold(thresholdSeconds);
    // Visit all threads performing IO and record any file ops taking longer
    // than the threshold.
    iteratePendingDiskOps([&writeCounts, &readCounts, &threshold](
                                  bool isDataWrite, auto elapsed) {
        auto& c = isDataWrite ? writeCounts : readCounts;
        ++c.numTotal;
        if (elapsed >= threshold) {
            ++c.numSlow;
        }
    });

    add_stat("pending_disk_read_num", to_string(readCounts.numTotal), cookie);
    add_stat("pending_disk_read_slow_num",
             to_string(readCounts.numSlow),
             cookie);
    add_stat("pending_disk_write_num", to_string(writeCounts.numTotal), cookie);
    add_stat("pending_disk_write_slow_num",
             to_string(writeCounts.numSlow),
             cookie);
    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::doDiskSlownessStats(
        const BucketStatCollector& collector) {
    std::chrono::microseconds writeMaxTime{};
    std::chrono::microseconds readMaxTime{};

    iteratePendingDiskOps(
            [&writeMaxTime, &readMaxTime](bool isDataWrite, auto elapsed) {
                auto& t = isDataWrite ? writeMaxTime : readMaxTime;
                t = std::max(t, elapsed);
            });

    using namespace cb::stats;
    collector.addStat(Key::ep_pending_disk_ops_max_time,
                      writeMaxTime.count(),
                      {{"type", "write"}});
    collector.addStat(Key::ep_pending_disk_ops_max_time,
                      readMaxTime.count(),
                      {{"type", "read"}});
}

cb::engine_errc EventuallyPersistentEngine::doPrivilegedStats(
        CookieIface& cookie, const AddStatFn& add_stat, std::string_view key) {
    // Privileged stats - need Stats priv (and not just SimpleStats).
    const auto acc = cookie.testPrivilege(cb::rbac::Privilege::Stats, {}, {});

    if (acc.success()) {
        if (key.starts_with("_checkpoint-dump")) {
            const size_t keyLen = strlen("_checkpoint-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doCheckpointDump(cookie, add_stat, keyArgs);
        }

        if (key.starts_with("_hash-dump")) {
            const size_t keyLen = strlen("_hash-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doHashDump(cookie, add_stat, keyArgs);
        }

        if (key.starts_with("_durability-dump")) {
            const size_t keyLen = strlen("_durability-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doDurabilityMonitorDump(cookie, add_stat, keyArgs);
        }

        if (key.starts_with("_vbucket-dump")) {
            const size_t keyLen = strlen("_vbucket-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doVBucketDump(cookie, add_stat, keyArgs);
        }

        return cb::engine_errc::no_such_key;
    }
    return cb::engine_errc::no_access;
}

cb::engine_errc EventuallyPersistentEngine::doFusionStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    Expects(kvBucket);
    return kvBucket->doFusionStats(cookie, add_stat, statKey);
}

static const CheckYieldFn default_check_yield_fn{[]() { return false; }};

cb::engine_errc EventuallyPersistentEngine::getStats(
        CookieIface& cookie,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat) {
    return getStats(cookie, key, value, add_stat, default_check_yield_fn);
}

cb::engine_errc EventuallyPersistentEngine::getStats(
        CookieIface& c,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat,
        const CheckYieldFn& check_yield) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch<Code>> timer(
            std::forward_as_tuple(stats.getStatsCmdHisto),
            std::forward_as_tuple(&c, cb::tracing::Code::GetStats));

    EP_LOG_DEBUG_CTX("stats", {"key", key});

    // Some stats have been moved to using the stat collector interface,
    // while others have not. Depending on the key, this collector _may_
    // not be used, but creating it here reduces duplication (and it's not
    // expensive to create)
    CBStatCollector collector{add_stat, c};
    auto bucketCollector = collector.forBucket(getName());

    if (key.empty()) {
        return doEngineStats(bucketCollector, &c);
    }
    if (key.size() > 7 && key.starts_with("dcpagg ")) {
        return doConnAggStats(bucketCollector, key.substr(7));
    }
    if (key == "dcp"sv) {
        return doDcpStats(c, add_stat, check_yield, value);
    }
    if (key == "eviction"sv) {
        return doEvictionStats(c, add_stat);
    }
    if (key == "hash"sv) {
        return doHashStats(c, add_stat);
    }
    if (key == "vbucket"sv) {
        return doVBucketStats(c, add_stat, key, VBucketStatsDetailLevel::State);
    }
    if (key == "prev-vbucket"sv) {
        return doVBucketStats(
                c, add_stat, key, VBucketStatsDetailLevel::PreviousState);
    }
    if (key.starts_with("vbucket-durability-state")) {
        return doVBucketStats(
                c, add_stat, key, VBucketStatsDetailLevel::Durability);
    }
    if (key.starts_with("vbucket-details")) {
        return doVBucketStats(c, add_stat, key, VBucketStatsDetailLevel::Full);
    }
    if (key.starts_with("vbucket-seqno")) {
        return doSeqnoStats(c, add_stat, key);
    }

    if (key == "encryption-key-ids") {
        return doEncryptionKeyIdsStats(c, add_stat);
    }

    if (key.starts_with("checkpoint")) {
        return doCheckpointStats(c, add_stat, key);
    }
    if (key.starts_with("durability-monitor")) {
        return doDurabilityMonitorStats(c, add_stat, key);
    }
    if (key == "timings"sv) {
        doTimingStats(bucketCollector);
        return cb::engine_errc::success;
    }
    if (key == "frequency-counters"sv) {
        return doFrequencyCountersStats(bucketCollector);
    }
    if (key == "dispatcher"sv) {
        return doDispatcherStats(c, add_stat);
    }
    if (key == "tasks"sv) {
        return doTasksStats(c, add_stat);
    }
    if (key == "scheduler"sv) {
        return doSchedulerStats(c, add_stat);
    }
    if (key == "runtimes"sv) {
        return doRunTimeStats(bucketCollector);
    }
    if (key == "memory"sv) {
        return doMemoryStats(c, add_stat);
    }
    if (key == "uuid"sv) {
        add_casted_stat("uuid", configuration.getUuid(), add_stat, c);
        return cb::engine_errc::success;
    }
    if (key.starts_with("key ") || key.starts_with("key-byid ")) {
        return doKeyStats(c, add_stat, key);
    }
    if (key.starts_with("vkey ") || key.starts_with("vkey-byid ")) {
        return doVKeyStats(c, add_stat, key);
    }
    if (key == "kvtimings"sv) {
        getKVBucket()->addKVStoreTimingStats(add_stat, c);
        return cb::engine_errc::success;
    }
    if (key.size() >= 7 && key.starts_with("kvstore")) {
        std::string args(key.data() + 7, key.size() - 7);
        getKVBucket()->addKVStoreStats(add_stat, c);
        return cb::engine_errc::success;
    }
    if (key == "warmup"sv) {
        return getKVBucket()->doWarmupStats(add_stat, c);
    }
    if (key == "info"sv) {
        add_casted_stat("info", get_stats_info(), add_stat, c);
        return cb::engine_errc::success;
    }
    if (key == "config"sv) {
        configuration.addStats(bucketCollector,
                               cb::config::ExcludeWhenValueIsDefaultValue::No);
        return cb::engine_errc::success;
    }
    if (key.size() > 15 && key.starts_with("dcp-vbtakeover")) {
        return doDcpVbTakeoverStats(c, add_stat, key);
    }
    if (key == "workload"sv) {
        return doWorkloadStats(c, add_stat);
    }
    if (key.starts_with("failovers")) {
        return doFailoversStats(c, add_stat, key);
    }
    if (key.starts_with("diskinfo")) {
        return doDiskinfoStats(c, add_stat, key);
    }
    if (key.starts_with("collections")) {
        return doCollectionStats(
                c, add_stat, std::string(key.data(), key.size()));
    }
    if (key.starts_with("scopes")) {
        return doScopeStats(c, add_stat, std::string(key.data(), key.size()));
    }
    if (key.starts_with("disk-failures")) {
        doDiskFailureStats(bucketCollector);
        return cb::engine_errc::success;
    }
    if (key.starts_with("disk-slowness")) {
        return doDiskSlownessStats(
                c, add_stat, std::string(key.data(), key.size()));
    }
    if (key[0] == '_') {
        return doPrivilegedStats(c, add_stat, key);
    }
    if (key.starts_with("range-scans")) {
        return doRangeScanStats(bucketCollector, key);
    }
    if (key.starts_with("fusion")) {
        return doFusionStats(c, add_stat, key);
    }
    if (key.starts_with("snapshot-details")) {
        return getKVBucket()->doSnapshotDebugStats(bucketCollector, key);
    }
    if (key.starts_with("snapshot-status")) {
        return getKVBucket()->doSnapshotStatus(bucketCollector, key);
    }
    if (key == "snapshot-deks"sv) {
        return getKVBucket()->doSnapshotDeks(bucketCollector);
    }
    if (key.starts_with("snapshot-move")) {
        return getKVBucket()->doSnapshotMoveStats(bucketCollector, key);
    }

    // Unknown stat requested
    return cb::engine_errc::no_such_key;
}

void EventuallyPersistentEngine::resetStats() {
    stats.reset();
    if (kvBucket) {
        kvBucket->resetUnderlyingStats();
    }
}

void EventuallyPersistentEngine::auditDocumentAccess(
        CookieIface& cookie, cb::audit::document::Operation operation) const {
    NonBucketAllocationGuard guard;
    cookie.auditDocumentAccess(operation);
}

cb::engine_errc EventuallyPersistentEngine::checkCollectionAccess(
        CookieIface& cookie,
        std::optional<Vbid> vbid,
        std::optional<cb::rbac::Privilege> systemCollectionPrivilege,
        cb::rbac::Privilege priv,
        CollectionID cid,
        bool logIfPrivilegeMissing) const {
    ScopeID sid{ScopeID::Default};
    uint64_t manifestUid{0};
    cb::engine_errc status = cb::engine_errc::success;

    if (!cid.isDefaultCollection()) {
        using Collections::getCollectionVisibility;
        using Collections::Visibility;
        Visibility visibility = Visibility::User;
        if (vbid) {
            auto vbucket = getVBucket(*vbid);
            if (!vbucket) {
                return cb::engine_errc::not_my_vbucket;
            }
            auto handle = vbucket->getManifest().lock(cid);
            if (!handle.valid()) {
                return cb::engine_errc::unknown_collection;
            }
            sid = handle.getScopeID();
            manifestUid = handle.getManifestUid();
            visibility = getCollectionVisibility(handle.getName(), cid);
        } else {
            auto res = getKVBucket()->getCollectionEntry(cid);
            manifestUid = res.first;
            if (!res.second) {
                status = cb::engine_errc::unknown_collection;
            } else {
                sid = res.second->sid;
                visibility = getCollectionVisibility(res.second->name, cid);
            }
        }

        if (systemCollectionPrivilege && visibility == Visibility::System) {
            status = logIfPrivilegeMissing
                             ? checkPrivilege(cookie,
                                              *systemCollectionPrivilege,
                                              sid,
                                              cid)
                             : testPrivilege(cookie,
                                             *systemCollectionPrivilege,
                                             sid,
                                             cid);
        }
    }

    if (status == cb::engine_errc::success) {
        status = logIfPrivilegeMissing ? checkPrivilege(cookie, priv, sid, cid)
                                       : testPrivilege(cookie, priv, sid, cid);
    }

    switch (status) {
    case cb::engine_errc::success:
    case cb::engine_errc::no_access:
        break;
    case cb::engine_errc::unknown_collection:
        setUnknownCollectionErrorContext(cookie, manifestUid);
        break;
    default:
        EP_LOG_ERR_CTX("EPE::checkPrivilege unexpected status",
                       {"priv", int(priv)},
                       {"cid", cid},
                       {"sid", sid},
                       {"status", status});
    }
    return status;
}

cb::engine_errc EventuallyPersistentEngine::testScopeAccess(
        CookieIface& cookie,
        std::optional<cb::rbac::Privilege> systemScopePrivilege,
        cb::rbac::Privilege priv,
        ScopeID sid,
        Collections::Visibility visibility) const {
    if (!sid.isDefaultScope()) {
        using Collections::Visibility;
        if (systemScopePrivilege && visibility == Visibility::System) {
            switch (cookie.testPrivilege(*systemScopePrivilege, sid, {})
                            .getStatus()) {
            case cb::rbac::PrivilegeAccess::Status::Ok:
                break;
            case cb::rbac::PrivilegeAccess::Status::Fail:
                return cb::engine_errc::no_access;
            case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
                return cb::engine_errc::unknown_scope;
            }
        }
    }

    switch (cookie.testPrivilege(priv, sid, {}).getStatus()) {
    case cb::rbac::PrivilegeAccess::Status::Ok:
        return cb::engine_errc::success;
    case cb::rbac::PrivilegeAccess::Status::Fail:
        return cb::engine_errc::no_access;
    case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
        return cb::engine_errc::unknown_scope;
    }
    folly::assume_unreachable();
}

cb::engine_errc EventuallyPersistentEngine::testCollectionAccess(
        CookieIface& cookie,
        std::optional<cb::rbac::Privilege> systemCollectionPrivilege,
        cb::rbac::Privilege priv,
        CollectionID cid,
        ScopeID sid,
        Collections::Visibility visibility) const {
    if (!cid.isDefaultCollection()) {
        using Collections::Visibility;
        if (systemCollectionPrivilege && visibility == Visibility::System) {
            switch (cookie.testPrivilege(*systemCollectionPrivilege, sid, cid)
                            .getStatus()) {
            case cb::rbac::PrivilegeAccess::Status::Ok:
                break;
            case cb::rbac::PrivilegeAccess::Status::Fail:
                return cb::engine_errc::no_access;
            case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
                return cb::engine_errc::unknown_collection;
            }
        }
    }

    switch (cookie.testPrivilege(priv, sid, cid).getStatus()) {
    case cb::rbac::PrivilegeAccess::Status::Ok:
        return cb::engine_errc::success;
    case cb::rbac::PrivilegeAccess::Status::Fail:
        return cb::engine_errc::no_access;
    case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
        return cb::engine_errc::unknown_collection;
    }
    folly::assume_unreachable();
}

cb::engine_errc
EventuallyPersistentEngine::checkForPrivilegeAtLeastInOneCollection(
        CookieIface& cookie, cb::rbac::Privilege privilege) const {
    try {
        switch (cookie.checkForPrivilegeAtLeastInOneCollection(privilege)
                        .getStatus()) {
        case cb::rbac::PrivilegeAccess::Status::Ok:
            return cb::engine_errc::success;
        case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
        case cb::rbac::PrivilegeAccess::Status::Fail:
            return cb::engine_errc::no_access;
        }
    } catch (const std::exception& e) {
        EP_LOG_ERR_CTX(
                "EPE::checkForPrivilegeAtLeastInOneCollection: received "
                "exception while checking privilege",
                {"error", e.what()});
    }

    return cb::engine_errc::failed;
}

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        CookieIface& cookie,
        cb::rbac::Privilege priv,
        ScopeID sid,
        CollectionID cid) {
    try {
        // Upon failure check_privilege may set an error message in the
        // cookie about the missing privilege
        NonBucketAllocationGuard guard;
        switch (cookie.checkPrivilege(priv, sid, cid).getStatus()) {
        case cb::rbac::PrivilegeAccess::Status::Ok:
            return cb::engine_errc::success;
        case cb::rbac::PrivilegeAccess::Status::Fail:
            return cb::engine_errc::no_access;
        case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
            return cb::engine_errc::unknown_collection;
        }
    } catch (const std::exception& e) {
        EP_LOG_ERR_CTX(
                "EPE::checkPrivilege: received exception while checking "
                "privilege",
                {"sid", sid},
                {"cid", cid},
                {"error", e.what()});
    }
    return cb::engine_errc::failed;
}

cb::engine_errc EventuallyPersistentEngine::checkMemoryForBGFetch(
        size_t pendingBytes) {
    if (memoryTracker->isBelowMutationMemoryQuota(pendingBytes)) {
        return cb::engine_errc::success;
    }
    return memoryCondition();
}

cb::engine_errc EventuallyPersistentEngine::testPrivilege(
        CookieIface& cookie,
        cb::rbac::Privilege priv,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    try {
        switch (cookie.testPrivilege(priv, sid, cid).getStatus()) {
        case cb::rbac::PrivilegeAccess::Status::Ok:
            return cb::engine_errc::success;
        case cb::rbac::PrivilegeAccess::Status::Fail:
            return cb::engine_errc::no_access;
        case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
            return cid ? cb::engine_errc::unknown_collection
                       : cb::engine_errc::unknown_scope;
        }
    } catch (const std::exception& e) {
        EP_LOG_ERR_CTX(
                "EPE::testPrivilege: received exception while checking "
                "privilege for sid:{}: cid:{} {}",
                sid ? sid->to_string() : "no-scope",
                cid ? cid->to_string() : "no-collection",
                e.what());
    }
    return cb::engine_errc::failed;
}

cb::engine_errc EventuallyPersistentEngine::handleObserve(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint) {
    EP_LOG_DEBUG_CTX("Observing key",
                     {"key", cb::UserDataView(key.to_string())},
                     {"vb", vbucket});

    auto rv = checkCollectionAccess(cookie,
                                    vbucket,
                                    cb::rbac::Privilege::SystemCollectionLookup,
                                    cb::rbac::Privilege::Read,
                                    key.getCollectionID());
    if (rv != cb::engine_errc::success) {
        return rv;
    }

    ObserveKeyState keystatus = ObserveKeyState::NotFound;
    struct key_stats kstats = {};
    rv = kvBucket->getKeyStats(key, vbucket, cookie, kstats, WantsDeleted::Yes);
    if (rv == cb::engine_errc::success) {
        if (kstats.logically_deleted) {
            keystatus = ObserveKeyState::LogicalDeleted;
        } else if (kstats.dirty) {
            keystatus = ObserveKeyState::NotPersisted;
        } else {
            keystatus = ObserveKeyState::Persisted;
        }
    } else if (rv == cb::engine_errc::no_such_key) {
        keystatus = ObserveKeyState::NotFound;
    } else {
        return rv;
    }

    {
        // Toggle allocation guard when calling back into the engine
        NonBucketAllocationGuard guard;
        key_handler(uint8_t(keystatus), kstats.cas);
    }

    persist_time_hint = 0;
    const auto queue_size = static_cast<double>(stats.getDiskQueueSize());
    const double item_trans_time = kvBucket->getTransactionTimePerItem();

    if (item_trans_time > 0 && queue_size > 0) {
        persist_time_hint = static_cast<uint32_t>(queue_size * item_trans_time);
    }
    persist_time_hint = persist_time_hint << 32;

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::observe(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint) {
    return acquireEngine(this)->handleObserve(
            cookie, key, vbucket, key_handler, persist_time_hint);
}

cb::engine_errc EventuallyPersistentEngine::observe_seqno(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    Vbid vb_id = request.getVBucket();
    auto value = request.getValue();
    auto vb_uuid = static_cast<uint64_t>(
            ntohll(*reinterpret_cast<const uint64_t*>(value.data())));

    EP_LOG_DEBUG_CTX("Observing vBucket", {"vb", vb_id}, {"uuid", vb_uuid});

    VBucketPtr vb = kvBucket->getVBucket(vb_id);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    std::shared_lock rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        return cb::engine_errc::not_my_vbucket;
    }

    //Check if the vb uuid matches with the latest entry
    failover_entry_t entry = vb->failovers->getLatestEntry();
    std::stringstream result;

    if (vb_uuid != entry.vb_uuid) {
       uint64_t failover_highseqno = 0;
       uint64_t latest_uuid;
       bool found = vb->failovers->getLastSeqnoForUUID(vb_uuid, &failover_highseqno);
       if (!found) {
           return cb::engine_errc::no_such_key;
       }

       uint8_t format_type = 1;
       uint64_t last_persisted_seqno = htonll(vb->getPublicPersistenceSeqno());
       uint64_t current_seqno = htonll(vb->getHighSeqno());
       latest_uuid = htonll(entry.vb_uuid);
       vb_id = vb_id.hton();
       vb_uuid = htonll(vb_uuid);
       failover_highseqno = htonll(failover_highseqno);

       result.write((char*) &format_type, sizeof(uint8_t));
       result.write((char*)&vb_id, sizeof(Vbid));
       result.write((char*) &latest_uuid, sizeof(uint64_t));
       result.write((char*) &last_persisted_seqno, sizeof(uint64_t));
       result.write((char*) &current_seqno, sizeof(uint64_t));
       result.write((char*) &vb_uuid, sizeof(uint64_t));
       result.write((char*) &failover_highseqno, sizeof(uint64_t));
    } else {
        uint8_t format_type = 0;
        uint64_t last_persisted_seqno = htonll(vb->getPublicPersistenceSeqno());
        uint64_t current_seqno = htonll(vb->getHighSeqno());
        vb_id = vb_id.hton();
        vb_uuid =  htonll(vb_uuid);

        result.write((char*) &format_type, sizeof(uint8_t));
        result.write((char*)&vb_id, sizeof(Vbid));
        result.write((char*) &vb_uuid, sizeof(uint64_t));
        result.write((char*) &last_persisted_seqno, sizeof(uint64_t));
        result.write((char*) &current_seqno, sizeof(uint64_t));
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        result.str(), // body
                        ValueIsJson::No,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

VBucketPtr EventuallyPersistentEngine::getVBucket(Vbid vbucket) const {
    return kvBucket->getVBucket(vbucket);
}

cb::engine_errc EventuallyPersistentEngine::handleSeqnoPersistence(
        CookieIface& cookie, uint64_t seqno, Vbid vbucket) {
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    if (!getEngineSpecific<ScheduledSeqnoPersistenceToken>(cookie)) {
        const auto persisted_seqno = vb->getPersistenceSeqno();
        if (seqno > persisted_seqno) {
            const auto res = vb->checkAddHighPriorityVBEntry(
                    std::make_unique<SeqnoPersistenceRequest>(
                            &cookie,
                            seqno,
                            kvBucket->getSeqnoPersistenceTimeout()));

            switch (res) {
            case HighPriorityVBReqStatus::RequestScheduled:
                storeEngineSpecific(cookie, ScheduledSeqnoPersistenceToken{});
                return cb::engine_errc::would_block;

            case HighPriorityVBReqStatus::NotSupported:
                EP_LOG_WARN_CTX(
                        "EventuallyPersistentEngine::handleSeqnoCmds(): High "
                        "priority async seqno request is NOT supported",
                        {"vb", vbucket});
                return cb::engine_errc::not_supported;

            case HighPriorityVBReqStatus::RequestNotScheduled:
                /// 'HighPriorityVBEntry' was not added, hence just return
                /// success
                EP_LOG_INFO_CTX(
                        "EventuallyPersistentEngine::handleSeqnoCmds(): Did "
                        "NOT add high priority async seqno request",
                        {"vb", vbucket},
                        {"persisted_seqno", persisted_seqno},
                        {"seqno", seqno},
                        {"error", "persisted seqno > requested seqno"});
                return cb::engine_errc::success;
            }
        }
        // Already persisted up to the number
        return cb::engine_errc::success;
    }

    clearEngineSpecific(cookie);
    EP_LOG_DEBUG_CTX(
            "Sequence number persisted", {"seqno", seqno}, {"vb", vbucket});
    return cb::engine_errc::success;
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::getMetaInner(
        CookieIface& cookie, const DocKeyView& key, Vbid vbucket) {
    uint32_t deleted;
    uint8_t datatype;
    ItemMetaData itemMeta;
    cb::engine_errc ret = kvBucket->getMetaData(
            key, vbucket, &cookie, itemMeta, deleted, datatype);

    item_info metadata;

    if (ret == cb::engine_errc::success) {
        metadata = to_item_info(itemMeta, datatype, deleted);
    }

    return std::make_pair(maybeRemapStatus(ret), metadata);
}

bool EventuallyPersistentEngine::decodeSetWithMetaOptions(
        cb::const_byte_buffer extras,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts) {
    // DeleteSource not needed by SetWithMeta, so set to default of explicit
    DeleteSource deleteSource = DeleteSource::Explicit;
    return EventuallyPersistentEngine::decodeWithMetaOptions(
            extras, generateCas, checkConflicts, deleteSource);
}
bool EventuallyPersistentEngine::decodeWithMetaOptions(
        cb::const_byte_buffer extras,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts,
        DeleteSource& deleteSource) {
    bool forceFlag = false;
    if (extras.size() == 28 || extras.size() == 30) {
        const size_t fixed_extras_size = 24;
        uint32_t options;
        memcpy(&options, extras.data() + fixed_extras_size, sizeof(options));
        options = ntohl(options);

        if (options & (SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_WITH_META_OP)) {
            // FORCE_WITH_META_OP used to change permittedVBStates to include
            // replica and pending, which is incredibly dangerous. This flag is
            // still supported but now only permits a change to the
            // checkConflicts flag. MB-67207
            checkConflicts = CheckConflicts::No;
        }

        if (options & FORCE_ACCEPT_WITH_META_OPS) {
            forceFlag = true;
        }

        if (options & REGENERATE_CAS) {
            generateCas = GenerateCas::Yes;
        }

        if (options & IS_EXPIRATION) {
            deleteSource = DeleteSource::TTL;
        }
    }

    // Validate options
    // 1) If GenerateCas::Yes then we must have CheckConflicts::No
    bool check1 = generateCas == GenerateCas::Yes &&
                  checkConflicts == CheckConflicts::Yes;

    // 2) If bucket is LWW/Custom and forceFlag is not set and GenerateCas::No
    bool check2 =
            conflictResolutionMode != ConflictResolutionMode::RevisionId &&
            !forceFlag && generateCas == GenerateCas::No;

    // 3) If bucket is revid then forceFlag must be false.
    bool check3 =
            conflictResolutionMode == ConflictResolutionMode::RevisionId &&
            forceFlag;

    // So if either check1/2/3 is true, return false
    return !(check1 || check2 || check3);
}

/**
 * This is a helper function for set/deleteWithMeta to adjust the value
 * in case the now removed/ignored extended meta data was included in the value.
 *
 * @param value nmeta is removed from the value by this function
 * @param extras
 */
cb::const_byte_buffer adjustValueForExtendedMeta(cb::const_byte_buffer value,
                                                 cb::const_byte_buffer extras) {
    if (extras.size() == 26 || extras.size() == 30) {
        // 26 = nmeta
        // 30 = options and nmeta (options followed by nmeta)
        // The extras is stored last, so copy out the two last bytes in
        // the extras field and use them as nmeta
        uint16_t nmeta;
        memcpy(&nmeta, extras.end() - sizeof(nmeta), sizeof(nmeta));
        nmeta = ntohs(nmeta);
        // Correct the vallen, skipping over any extras which could
        // have been included in the value
        value = {value.data(), value.size() - nmeta};
    }
    return value;
}

protocol_binary_datatype_t EventuallyPersistentEngine::checkForDatatypeJson(
        CookieIface& cookie,
        protocol_binary_datatype_t datatype,
        std::string_view body) {
    // JSON check the body if xattr's are enabled
    if (cb::mcbp::datatype::is_xattr(datatype)) {
        body = cb::xattr::get_body(body);
    }

    NonBucketAllocationGuard guard;
    if (cookie.isValidJson(body)) {
        datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    } else {
        datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
    }
    return datatype;
}

DocKeyView EventuallyPersistentEngine::makeDocKey(
        CookieIface& cookie, cb::const_byte_buffer key) const {
    return DocKeyView{key.data(),
                      key.size(),
                      cookie.isCollectionsSupported()
                              ? DocKeyEncodesCollectionId::Yes
                              : DocKeyEncodesCollectionId::No};
}

cb::engine_errc EventuallyPersistentEngine::setWithMeta(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (isDegradedMode()) {
        return cb::engine_errc::temporary_failure;
    }

    const auto extras = request.getExtdata();

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    if (!decodeSetWithMetaOptions(extras, generateCas, checkConflicts)) {
        return cb::engine_errc::invalid_arguments;
    }

    auto value = adjustValueForExtendedMeta(request.getValue(), extras);

    cb::time::steady_clock::time_point startTime;
    {
        auto startTimeC =
                takeEngineSpecific<cb::time::steady_clock::time_point>(cookie);
        if (startTimeC.has_value()) {
            startTime = *startTimeC;
        } else {
            startTime = cb::time::steady_clock::now();
        }
    }

    const auto opcode = request.getClientOpcode();
    const bool allowExisting = (opcode == cb::mcbp::ClientOpcode::SetWithMeta ||
                                opcode == cb::mcbp::ClientOpcode::SetqWithMeta);

    const auto* payload =
            reinterpret_cast<const cb::mcbp::request::SetWithMetaPayload*>(
                    extras.data());

    uint32_t flags = payload->getFlagsInNetworkByteOrder();
    uint32_t expiration = payload->getExpiration();
    uint64_t seqno = payload->getSeqno();
    uint64_t cas = payload->getCas();
    uint64_t bySeqno = 0;
    cb::engine_errc ret;
    uint64_t commandCas = request.getCas();
    try {
        ret = setWithMeta(request.getVBucket(),
                          makeDocKey(cookie, request.getKey()),
                          value,
                          {cas, seqno, flags, expiration},
                          false /*isDeleted*/,
                          uint8_t(request.getDatatype()),
                          commandCas,
                          &bySeqno,
                          cookie,
                          permittedVBStates,
                          checkConflicts,
                          allowExisting,
                          GenerateBySeqno::Yes,
                          generateCas);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    if (ret == cb::engine_errc::would_block) {
        ++stats.numOpsGetMetaOnSetWithMeta;
        storeEngineSpecific(cookie, startTime);
        return cb::engine_errc::would_block;
    }

    auto scopeGuard = folly::makeGuard([&cookie,
                                        startTime,
                                        success = (ret ==
                                                   cb::engine_errc::success),
                                        this] {
        auto endTime = cb::time::steady_clock::now();

        {
            NonBucketAllocationGuard guard;
            auto& tracer = cookie.getTracer();
            tracer.record(Code::SetWithMeta, startTime, endTime);
        }

        if (success) {
            auto elapsed =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            endTime - startTime);
            stats.setWithMetaHisto.add(elapsed);
        }
    });

    if (ret == cb::engine_errc::no_memory) {
        return memoryCondition();
    }

    if (ret != cb::engine_errc::success) {
        // Let the framework generate the error message
        return ret;
    }

    cookie.addDocumentWriteBytes(value.size() + request.getKey().size());
    auditDocumentAccess(cookie, cb::audit::document::Operation::Modify);
    ++stats.numOpsSetMeta;
    cas = commandCas;

    if (opcode == cb::mcbp::ClientOpcode::SetqWithMeta ||
        opcode == cb::mcbp::ClientOpcode::AddqWithMeta) {
        // quiet ops should not produce output
        return cb::engine_errc::success;
    }

    if (cookie.isMutationExtrasSupported()) {
        return sendMutationExtras(response,
                                  request.getVBucket(),
                                  bySeqno,
                                  cb::mcbp::Status::Success,
                                  cas,
                                  cookie);
    }
    return sendErrorResponse(response, cb::mcbp::Status::Success, cas, cookie);
}

cb::engine_errc EventuallyPersistentEngine::setWithMeta(
        Vbid vbucket,
        DocKeyView key,
        cb::const_byte_buffer value,
        ItemMetaData itemMeta,
        bool isDeleted,
        protocol_binary_datatype_t datatype,
        uint64_t& cas,
        uint64_t* seqno,
        CookieIface& cookie,
        PermittedVBStates permittedVBStates,
        CheckConflicts checkConflicts,
        bool allowExisting,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas) {
    if (cb::mcbp::datatype::is_snappy(datatype) &&
        !cookie.isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        setErrorContext(cookie, "Client did not negotiate Snappy support");
        return cb::engine_errc::invalid_arguments;
    }

    std::string_view payload(reinterpret_cast<const char*>(value.data()),
                             value.size());

    cb::const_byte_buffer finalValue = value;
    protocol_binary_datatype_t finalDatatype = datatype;
    cb::compression::Buffer uncompressedValue;

    cb::const_byte_buffer inflatedValue = value;
    protocol_binary_datatype_t inflatedDatatype = datatype;

    if (value.empty()) {
        finalDatatype = PROTOCOL_BINARY_RAW_BYTES;
    } else {
        if (cb::mcbp::datatype::is_snappy(datatype)) {
            if (!cb::compression::inflateSnappy(
                        payload,
                        uncompressedValue,
                        std::numeric_limits<size_t>::max())) {
                setErrorContext(cookie, "Failed to inflate document");
                return cb::engine_errc::invalid_arguments;
            }

            inflatedValue = uncompressedValue;
            inflatedDatatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;

            if (compressionMode == BucketCompressionMode::Off ||
                uncompressedValue.size() < value.size()) {
                // If the inflated version version is smaller than the
                // compressed version we should keep it inflated
                finalValue = uncompressedValue;
                finalDatatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
            }
        }

        size_t system_xattr_size = 0;
        if (cb::mcbp::datatype::is_xattr(datatype)) {
            // the validator ensured that the xattr was valid
            std::string_view xattr;
            xattr = {reinterpret_cast<const char*>(inflatedValue.data()),
                     inflatedValue.size()};
            system_xattr_size =
                    cb::xattr::get_system_xattr_size(inflatedDatatype, xattr);
            if (system_xattr_size > cb::limits::PrivilegedBytes) {
                setErrorContext(
                        cookie,
                        "System XATTR (" + std::to_string(system_xattr_size) +
                                ") exceeds the max limit for system "
                                "xattrs: " +
                                std::to_string(cb::limits::PrivilegedBytes));
                return cb::engine_errc::invalid_arguments;
            }
        }

        const auto valuesize = inflatedValue.size();
        if ((valuesize - system_xattr_size) > maxItemSize) {
            EP_LOG_WARN_CTX(
                    "Item value size is bigger than the max "
                    "size allowed",
                    {"op", "setWithMeta"},
                    {"value_size", inflatedValue.size()},
                    {"max_item_size", maxItemSize});

            return cb::engine_errc::too_big;
        }

        finalDatatype = checkForDatatypeJson(
                cookie,
                finalDatatype,
                cb::mcbp::datatype::is_snappy(datatype) ? uncompressedValue
                                                        : payload);
    }

    auto item = std::make_unique<Item>(key,
                                       itemMeta.flags,
                                       itemMeta.exptime,
                                       finalValue.data(),
                                       finalValue.size(),
                                       finalDatatype,
                                       itemMeta.cas,
                                       -1,
                                       vbucket);
    stats.itemAllocSizeHisto.addValue(finalValue.size());

    item->setRevSeqno(itemMeta.revSeqno);
    if (isDeleted) {
        item->setDeleted();
    }
    auto ret = kvBucket->setWithMeta(*item,
                                     cas,
                                     seqno,
                                     &cookie,
                                     permittedVBStates,
                                     checkConflicts,
                                     allowExisting,
                                     genBySeqno,
                                     genCas);

    if (ret == cb::engine_errc::success) {
        cas = item->getCas();
    } else {
        cas = 0;
    }
    return ret;
}

cb::engine_errc EventuallyPersistentEngine::deleteWithMeta(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (isDegradedMode()) {
        return cb::engine_errc::temporary_failure;
    }

    const auto extras = request.getExtdata();

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    DeleteSource deleteSource = DeleteSource::Explicit;
    if (!decodeWithMetaOptions(
                extras, generateCas, checkConflicts, deleteSource)) {
        return cb::engine_errc::invalid_arguments;
    }

    auto value = adjustValueForExtendedMeta(request.getValue(), extras);
    auto key = makeDocKey(cookie, request.getKey());
    uint64_t bySeqno = 0;

    const auto* payload =
            reinterpret_cast<const cb::mcbp::request::DelWithMetaPayload*>(
                    extras.data());
    const uint32_t flags = payload->getFlagsInNetworkByteOrder();
    const uint32_t delete_time = payload->getDeleteTime();
    const uint64_t seqno = payload->getSeqno();
    const uint64_t metacas = payload->getCas();
    uint64_t cas = request.getCas();
    cb::engine_errc ret;
    try {
        // MB-37374: Accept user-xattrs, body is still invalid
        auto datatype = uint8_t(request.getDatatype());
        cb::compression::Buffer uncompressedValue;
        if (cb::mcbp::datatype::is_snappy(datatype)) {
            if (!cb::compression::inflateSnappy(
                        {reinterpret_cast<const char*>(value.data()),
                         value.size()},
                        uncompressedValue,
                        std::numeric_limits<size_t>::max())) {
                setErrorContext(cookie, "Failed to inflate data");
                return cb::engine_errc::invalid_arguments;
            }
            value = uncompressedValue;
            datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
        }

        if (allowSanitizeValueInDeletion) {
            if (cb::mcbp::datatype::is_xattr(datatype)) {
                if (!value.empty()) {
                    // Whatever we have in the value, just keep Xattrs
                    const auto valBuffer = std::string_view{
                            reinterpret_cast<const char*>(value.data()),
                            value.size()};
                    value = {value.data(),
                             cb::xattr::get_body_offset(valBuffer)};
                }
            } else {
                // We may have nothing or only a Body, remove everything
                value = {};
            }
        } else {
            // Whether we have Xattrs or not, we just want to fail if we got a
            // value with Body
            if (cb::xattr::get_body_size(
                        datatype,
                        {reinterpret_cast<const char*>(value.data()),
                         value.size()}) > 0) {
                setErrorContext(cookie,
                                "It is only possible to specify Xattrs as a "
                                "value to DeleteWithMeta");
                return cb::engine_errc::invalid_arguments;
            }
        }

        if (value.empty()) {
            ret = deleteWithMeta(request.getVBucket(),
                                 key,
                                 {metacas, seqno, flags, delete_time},
                                 cas,
                                 &bySeqno,
                                 cookie,
                                 permittedVBStates,
                                 checkConflicts,
                                 GenerateBySeqno::Yes,
                                 generateCas,
                                 deleteSource);
        } else {
            // A delete with a value
            ret = setWithMeta(request.getVBucket(),
                              key,
                              value,
                              {metacas, seqno, flags, delete_time},
                              true /*isDeleted*/,
                              PROTOCOL_BINARY_DATATYPE_XATTR,
                              cas,
                              &bySeqno,
                              cookie,
                              permittedVBStates,
                              checkConflicts,
                              true /*allowExisting*/,
                              GenerateBySeqno::Yes,
                              generateCas);
        }
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    if (ret == cb::engine_errc::success) {
        cookie.addDocumentWriteBytes(value.size() + request.getKey().size());
        auditDocumentAccess(cookie, cb::audit::document::Operation::Delete);
        stats.numOpsDelMeta++;
    } else if (ret == cb::engine_errc::no_memory) {
        return memoryCondition();
    } else {
        return ret;
    }

    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::DelqWithMeta) {
        return cb::engine_errc::success;
    }

    if (cookie.isMutationExtrasSupported()) {
        return sendMutationExtras(response,
                                  request.getVBucket(),
                                  bySeqno,
                                  cb::mcbp::Status::Success,
                                  cas,
                                  cookie);
    }

    return sendErrorResponse(response, cb::mcbp::Status::Success, cas, cookie);
}

cb::engine_errc EventuallyPersistentEngine::deleteWithMeta(
        Vbid vbucket,
        DocKeyView key,
        ItemMetaData itemMeta,
        uint64_t& cas,
        uint64_t* seqno,
        CookieIface& cookie,
        PermittedVBStates permittedVBStates,
        CheckConflicts checkConflicts,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        DeleteSource deleteSource) {
    return kvBucket->deleteWithMeta(key,
                                    cas,
                                    seqno,
                                    vbucket,
                                    &cookie,
                                    permittedVBStates,
                                    checkConflicts,
                                    itemMeta,
                                    genBySeqno,
                                    genCas,
                                    0 /*bySeqno*/,
                                    deleteSource,
                                    EnforceMemCheck::Yes);
}

cb::engine_errc EventuallyPersistentEngine::handleTrafficControlCmd(
        CookieIface& cookie, TrafficControlMode mode) {
    switch (mode) {
    case TrafficControlMode::Enabled:
        if (kvBucket->isPrimaryWarmupLoadingData()) {
            // engine is still warming up, do not turn on data traffic yet
            setErrorContext(cookie, "Persistent engine is still warming up!");
            return cb::engine_errc::temporary_failure;
        }

        if (configuration.isFailpartialwarmup() &&
            kvBucket->isWarmupOOMFailure()) {
            // engine has completed warm up, but data traffic cannot be
            // turned on due to an OOM failure
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to out of memory failures during warmup");
            return cb::engine_errc::no_memory;
        }

        if (kvBucket->hasWarmupSetVbucketStateFailed()) {
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to write failures when persisting vbucket state to "
                    "disk");
            return cb::engine_errc::failed;
        }

        if (enableTraffic(true)) {
            setErrorContext(cookie,
                            "Data traffic to persistence engine is enabled");
        } else {
            setErrorContext(cookie,
                            "Data traffic to persistence engine was "
                            "already enabled");
        }
        return cb::engine_errc::success;

    case TrafficControlMode::Disabled:
        if (enableTraffic(false)) {
            setErrorContext(cookie,
                            "Data traffic to persistence engine is disabled");
        } else {
            setErrorContext(
                    cookie,
                    "Data traffic to persistence engine was already disabled");
        }
        return cb::engine_errc::success;
    }

    throw std::invalid_argument(
            "EPE::handleTrafficControlCmd can only be called with "
            "Enabled or Disabled");
}

bool EventuallyPersistentEngine::isDegradedMode() const {
    return kvBucket->isPrimaryWarmupLoadingData() || !trafficEnabled.load();
}

bool EventuallyPersistentEngine::mustRemapNMVB() const {
    // If warmup hasn't loaded metdata then traffic enabled is the deciding
    // factor. Note ephemeral returns false for this, traffic enabled is always
    // the deciding factor in that case..
    if (!kvBucket->isNMVBUnequivocal()) {
        return !trafficEnabled.load();
    }
    return false;
}

cb::engine_errc EventuallyPersistentEngine::doDcpVbTakeoverStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string& key,
        Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    std::string dcpName("eq_dcpq:");
    dcpName.append(key);

    const auto conn = dcpConnMap_->findByName(dcpName);
    if (!conn) {
        EP_LOG_DEBUG_CTX("doDcpVbTakeoverStats - cannot find connection",
                         {"name", dcpName},
                         {"vb", vbid});
        size_t vb_items = vb->getNumItems();

        size_t del_items = 0;
        try {
            del_items = vb->getNumPersistedDeletes();
        } catch (std::runtime_error& e) {
            EP_LOG_WARN_CTX(
                    "doDcpVbTakeoverStats: exception while getting num "
                    "persisted deletes - treating as 0 deletes",
                    {"vb", vbid},
                    {"error", e.what()});
        }
        size_t chk_items =
                vb_items > 0 ? vb->checkpointManager->getNumOpenChkItems() : 0;
        add_casted_stat("status", "connection_does_not_exist", add_stat,
                        cookie);
        add_casted_stat("on_disk_deletes", del_items, add_stat, cookie);
        add_casted_stat("vb_items", vb_items, add_stat, cookie);
        add_casted_stat("chk_items", chk_items, add_stat, cookie);
        add_casted_stat("estimate", vb_items + del_items, add_stat, cookie);
        return cb::engine_errc::success;
    }

    auto producer = std::dynamic_pointer_cast<DcpProducer>(conn);
    if (producer) {
        producer->addTakeoverStats(add_stat, cookie, *vb);
        return cb::engine_errc::success;
    }

    /**
     * There is not a legitimate case where a connection is not a
     * DcpProducer.  But just in case it does happen log the event and
     * return cb::engine_errc::no_such_key.
     */
    EP_LOG_WARN_CTX("doDcpVbTakeoverStats: connection is not a DcpProducer",
                    {"name", dcpName},
                    {"vb", vbid});
    return cb::engine_errc::no_such_key;
}

cb::engine_errc EventuallyPersistentEngine::returnMeta(
        CookieIface& cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    using cb::mcbp::request::ReturnMetaPayload;
    using cb::mcbp::request::ReturnMetaType;

    const auto& payload = req.getCommandSpecifics<ReturnMetaPayload>();

    if (isDegradedMode()) {
        return cb::engine_errc::temporary_failure;
    }

    auto cas = req.getCas();
    auto datatype = uint8_t(req.getDatatype());
    auto mutate_type = payload.getMutationType();
    auto flags = payload.getFlags();
    auto exp = ep_convert_to_expiry_time(payload.getExpiration());
    uint64_t seqno;
    cb::engine_errc ret;
    if (mutate_type == ReturnMetaType::Set ||
        mutate_type == ReturnMetaType::Add) {
        auto value = req.getValueString();
        datatype = checkForDatatypeJson(cookie, datatype, value);

        auto itm = std::make_unique<Item>(makeDocKey(cookie, req.getKey()),
                                          flags,
                                          exp,
                                          value.data(),
                                          value.size(),
                                          datatype,
                                          cas,
                                          -1,
                                          req.getVBucket());

        if (mutate_type == ReturnMetaType::Set) {
            ret = kvBucket->set(*itm, &cookie, {});
        } else {
            ret = kvBucket->add(*itm, &cookie);
        }
        if (ret == cb::engine_errc::success) {
            auditDocumentAccess(cookie, cb::audit::document::Operation::Modify);
            ++stats.numOpsSetRetMeta;
            cookie.addDocumentWriteBytes(req.getKeylen() + value.size());
        }
        cas = itm->getCas();
        seqno = htonll(itm->getRevSeqno());
    } else if (mutate_type == ReturnMetaType::Del) {
        ItemMetaData itm_meta;
        mutation_descr_t mutation_descr;
        ret = kvBucket->deleteItem(makeDocKey(cookie, req.getKey()),
                                   cas,
                                   req.getVBucket(),
                                   &cookie,
                                   {},
                                   &itm_meta,
                                   mutation_descr);
        if (ret == cb::engine_errc::success) {
            auditDocumentAccess(cookie, cb::audit::document::Operation::Delete);
            ++stats.numOpsDelRetMeta;
            cookie.addDocumentWriteBytes(1);
        }
        flags = itm_meta.flags;
        exp = gsl::narrow<uint32_t>(itm_meta.exptime);
        cas = itm_meta.cas;
        seqno = htonll(itm_meta.revSeqno);
    } else {
        throw std::runtime_error(
                "returnMeta: Unknown mode passed though the validator");
    }

    if (ret != cb::engine_errc::success) {
        return ret;
    }

    std::array<char, 16> meta;
    exp = htonl(exp);
    memcpy(meta.data(), &flags, 4);
    memcpy(meta.data() + 4, &exp, 4);
    memcpy(meta.data() + 8, &seqno, 8);

    Expects(!cb::mcbp::datatype::is_snappy(datatype));
    Expects(!cb::mcbp::datatype::is_xattr(datatype));

    sendResponse(response,
                 {}, // key
                 {meta.data(), meta.size()}, // extra
                 {}, // body
                 cb::mcbp::datatype::is_json(datatype) ? ValueIsJson::Yes
                                                       : ValueIsJson::No,
                 cb::mcbp::Status::Success,
                 cas,
                 cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::getAllKeys(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (!getKVBucket()->isGetAllKeysSupported()) {
        return cb::engine_errc::not_supported;
    }

    auto running =
            takeEngineSpecific<std::shared_ptr<FetchAllKeysTask>>(cookie);
    if (running.has_value()) {
        const auto [status, keys] = running.value()->getResult();
        cookie.addDocumentReadBytes(keys.size());
        if (status != cb::engine_errc::success) {
            return status;
        }
        response({},
                 {},
                 {keys.data(), keys.size()},
                 ValueIsJson::No,
                 cb::mcbp::Status::Success,
                 0,
                 cookie);
        return status;
    }

    // Fail fast for requests hitting the incorrect node
    {
        auto vb = getVBucket(request.getVBucket());
        if (!vb) {
            return cb::engine_errc::not_my_vbucket;
        }

        std::shared_lock rlh(vb->getStateLock());
        if (vb->getState() != vbucket_state_active) {
            return cb::engine_errc::not_my_vbucket;
        }
    }

    // key: key, ext: no. of keys to fetch, sorting-order
    uint32_t count = 1000;
    auto extras = request.getExtdata();
    if (!extras.empty()) {
        count = ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
    }

    DocKeyView start_key = makeDocKey(cookie, request.getKey());
    auto privTestResult =
            checkCollectionAccess(cookie,
                                  request.getVBucket(),
                                  cb::rbac::Privilege::SystemCollectionLookup,
                                  cb::rbac::Privilege::Read,
                                  start_key.getCollectionID());
    if (privTestResult != cb::engine_errc::success) {
        return privTestResult;
    }

    std::optional<CollectionID> keysCollection;
    if (cookie.isCollectionsSupported()) {
        keysCollection = start_key.getCollectionID();
    }

    auto task = std::make_shared<FetchAllKeysTask>(*this,
                                                   cookie,
                                                   start_key,
                                                   request.getVBucket(),
                                                   count,
                                                   keysCollection);
    ExecutorPool::get()->schedule(task);
    storeEngineSpecific(cookie, std::move(task));
    return cb::engine_errc::would_block;
}

template <typename T>
void EventuallyPersistentEngine::notifyIOComplete(T cookies,
                                                  cb::engine_errc status) {
    NonBucketAllocationGuard guard;
    for (auto& cookie : cookies) {
        cookie.notifyIoComplete(status);
    }
}

void EventuallyPersistentEngine::notifyIOComplete(CookieIface* cookie,
                                                  cb::engine_errc status) {
    Expects(cookie);
    notifyIOComplete(*cookie, status);
}

void EventuallyPersistentEngine::notifyIOComplete(CookieIface& cookie,
                                                  cb::engine_errc status) {
    HdrMicroSecBlockTimer bt(&stats.notifyIOHisto);
    NonBucketAllocationGuard guard;
    cookie.notifyIoComplete(status);
}

void EventuallyPersistentEngine::scheduleDcpStep(CookieIface& cookie) {
    NonBucketAllocationGuard guard;
    cookie.getConnectionIface().scheduleDcpStep();
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getRandomDocument(
        CookieIface& cookie, CollectionID cid) {
    if (checkCollectionAccess(cookie,
                              {},
                              cb::rbac::Privilege::SystemCollectionLookup,
                              cb::rbac::Privilege::Read,
                              cid) != cb::engine_errc::success) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_access);
    }
    GetValue gv(kvBucket->getRandomKey(cid, cookie));
    cb::engine_errc ret = gv.getStatus();
    if (ret == cb::engine_errc::success) {
        if (cb::mcbp::datatype::is_xattr(gv.item->getDataType())) {
            // The document has xattrs. Strip them off if the caller don't
            // have access to them.
            if (checkCollectionAccess(
                        cookie,
                        {},
                        cb::rbac::Privilege::SystemCollectionLookup,
                        cb::rbac::Privilege::SystemXattrRead,
                        cid,
                        false) != cb::engine_errc::success) {
                gv.item->removeSystemXattrs();
            }
        }

        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
    }
    return cb::makeEngineErrorItemPair(ret);
}

cb::engine_errc EventuallyPersistentEngine::dcpOpen(
        CookieIface& cookie,
        uint32_t opaque,
        uint32_t seqno,
        cb::mcbp::DcpOpenFlag flags,
        std::string_view stream_name,
        std::string_view value) {
    std::string connName{stream_name};
    ConnHandler* handler = nullptr;
    using cb::mcbp::DcpOpenFlag;
    if ((flags & DcpOpenFlag::Producer) == DcpOpenFlag::Producer) {
        handler = dcpConnMap_->newProducer(cookie, connName, flags);
    } else {
        // Don't accept dcp consumer open requests before warm up has loaded
        // metadata. Primarily see MB-32577
        if (kvBucket->hasPrimaryWarmupLoadedMetaData()) {
            // Check if consumer_name specified in value; if so use in Consumer
            // object.
            nlohmann::json jsonValue;
            std::string consumerName;
            if (!value.empty()) {
                jsonValue = nlohmann::json::parse(value);
                consumerName = jsonValue.at("consumer_name").get<std::string>();
            }
            handler = dcpConnMap_->newConsumer(cookie, connName, consumerName);

            EP_LOG_INFO_CTX(
                    "EventuallyPersistentEngine::dcpOpen: opening new DCP "
                    "Consumer handler",
                    {"name", connName},
                    {"opaque", opaque},
                    {"seqno", seqno},
                    {"value", jsonValue});
        } else {
            EP_LOG_WARN_RAW(
                    "EventuallyPersistentEngine::dcpOpen: not opening new DCP "
                    "Consumer handler as EPEngine is still warming up");
            return cb::engine_errc::temporary_failure;
        }
    }

    if (handler == nullptr) {
        EP_LOG_WARN_RAW("EPEngine::dcpOpen: failed to create a handler");
        return cb::engine_errc::disconnect;
    }

    // Success creating dcp object which has stored the cookie and reserved it
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::dcpAddStream(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpAddStreamFlag flags) {
    return dcpConnMap_->addPassiveStream(
            *getConnHandler(cookie), opaque, vbucket, flags);
}

std::shared_ptr<ConnHandler>
EventuallyPersistentEngine::getSharedPtrConnHandler(CookieIface& cookie) {
    return std::static_pointer_cast<ConnHandler>(
            cookie.getConnectionIface().getDcpConnHandler());
}

std::shared_ptr<ConnHandler> EventuallyPersistentEngine::getConnHandler(
        CookieIface& cookie) {
    auto handle = getSharedPtrConnHandler(cookie);
    Expects(handle);
    return handle;
}

void EventuallyPersistentEngine::handleDisconnect(CookieIface& cookie) {
    dcpConnMap_->disconnect(&cookie);
}

void EventuallyPersistentEngine::initiate_shutdown() {
    auto eng = acquireEngine(this);
    kvBucket->initiateShutdown();
}

void EventuallyPersistentEngine::cancel_all_operations_in_ewb_state() {
    auto eng = acquireEngine(this);
    kvBucket->releaseBlockedCookies();
}

cb::engine_errc EventuallyPersistentEngine::stopFlusher(CookieIface& cookie) {
    if (!kvBucket->pauseFlusher()) {
        EP_LOG_DEBUG_RAW("Unable to stop flusher");
        setErrorContext(cookie, "Flusher not running."sv);
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::startFlusher(CookieIface& cookie) {
    if (!kvBucket->resumeFlusher()) {
        EP_LOG_DEBUG_RAW("Unable to start flusher");
        setErrorContext(cookie, "Flusher not shut down."sv);
        return cb::engine_errc::invalid_arguments;
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::deleteVBucketInner(
        CookieIface& cookie, Vbid vbid, bool sync) {
    HdrMicroSecBlockTimer timer(&stats.delVbucketCmdHisto);
    auto status = cb::engine_errc::success;
    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    auto es = takeEngineSpecific<ScheduledVBucketDeleteToken>(cookie);

    if (sync) {
        if (es.has_value()) {
            EP_LOG_DEBUG_CTX("Completed sync deletion", {"vb", vbid});
            return cb::engine_errc::success;
        }
        status = kvBucket->deleteVBucket(vbid, &cookie);
    } else {
        status = kvBucket->deleteVBucket(vbid);
    }

    switch (status) {
    case cb::engine_errc::success:
        EP_LOG_INFO_CTX("Deletion was completed", {"vb", vbid});
        break;

    case cb::engine_errc::not_my_vbucket:
        EP_LOG_WARN_CTX("Deletion failed because the vbucket doesn't exist",
                        {"vb", vbid});
        break;
    case cb::engine_errc::invalid_arguments:
        EP_LOG_WARN_CTX(
                "Deletion failed because the vbucket is not in a dead "
                "state",
                {"vb", vbid});
        setErrorContext(
                cookie,
                "Failed to delete vbucket.  Must be in the dead state.");
        break;
    case cb::engine_errc::would_block:
        EP_LOG_INFO_CTX(
                "Request for deletion is in EWOULDBLOCK until the database "
                "file is removed from disk",
                {"vb", vbid});
        // We don't use the actual value in ewouldblock, just the existence
        // of something there.
        storeEngineSpecific(cookie, ScheduledVBucketDeleteToken{});
        break;
    default:
        EP_LOG_WARN_CTX("Deletion of failed because of unknown reasons",
                        {"vb", vbid});
        setErrorContext(cookie, "Failed to delete vbucket.  Unknown reason.");
        status = cb::engine_errc::failed;
        break;
    }
    return status;
}

cb::engine_errc EventuallyPersistentEngine::scheduleCompaction(
        Vbid vbid, const CompactionConfig& c, CookieIface* cookie) {
    return kvBucket->scheduleCompaction(
            vbid, c, cookie, std::chrono::seconds(0));
}

cb::engine_errc EventuallyPersistentEngine::getAllVBucketSequenceNumbers(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    static_assert(sizeof(RequestedVBState) == 4,
                  "Unexpected size for RequestedVBState");
    auto extras = request.getExtdata();

    // By default allow any alive states. If reqState has been set then
    // filter on that specific state.
    auto reqState = aliveVBStates;
    std::optional<CollectionID> reqCollection = {};
    const bool collectionsEnabled = cookie.isCollectionsSupported();

    // if extlen is non-zero, it limits the result to either only include the
    // vbuckets in the specified vbucket state, or in the specified vbucket
    // state and for the specified collection ID.
    if (extras.size() >= sizeof(RequestedVBState)) {
        auto rawState = ntohl(*reinterpret_cast<const uint32_t*>(
                extras.substr(0, sizeof(vbucket_state_t)).data()));

        // If the received vbucket_state isn't 0 (i.e. all alive states) then
        // set the specifically requested states.
        auto desired = static_cast<RequestedVBState>(rawState);
        if (desired != RequestedVBState::Alive) {
            reqState = PermittedVBStates(static_cast<vbucket_state_t>(desired));
        }

        // Is a collection requested?
        if (extras.size() ==
            (sizeof(RequestedVBState) + sizeof(CollectionIDType))) {
            if (!collectionsEnabled) {
                return cb::engine_errc::invalid_arguments;
            }

            reqCollection = static_cast<CollectionIDType>(
                    ntohl(*reinterpret_cast<const uint32_t*>(
                            extras.substr(sizeof(RequestedVBState),
                                          sizeof(CollectionIDType))
                                    .data())));

        } else if (extras.size() >
                   (sizeof(RequestedVBState) + sizeof(CollectionIDType))) {
            // extras too large
            return cb::engine_errc::invalid_arguments;
        }
    } else if (!extras.empty()) {
        // extras too small
        return cb::engine_errc::invalid_arguments;
    }

    // If the client ISN'T talking collections, we should just give them the
    // high seqno of the default collection as that's all they should be aware
    // of.
    // If the client IS talking collections but hasn't specified a collection,
    // we'll give them the actual vBucket high seqno.
    if (!collectionsEnabled) {
        reqCollection = CollectionID::Default;
    }

    if (auto accessStatus =
                doGetAllVbSeqnosPrivilegeCheck(cookie, reqCollection);
        accessStatus != cb::engine_errc::success) {
        return accessStatus;
    }

    auto connhandler = getSharedPtrConnHandler(cookie);
    bool supportsSyncWrites = connhandler && connhandler->isSyncWritesEnabled();

    std::vector<char> payload;
    auto vbuckets = kvBucket->getVBuckets().getBuckets();

    /* Reserve a buffer that's big enough to hold all of them (we might
     * not use all of them. Each entry in the array occupies 10 bytes
     * (two bytes vbucket id followed by 8 bytes sequence number)
     */
    try {
        payload.reserve(vbuckets.size() * (sizeof(uint16_t) + sizeof(uint64_t)));
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    for (auto id : vbuckets) {
        VBucketPtr vb = getVBucket(id);
        if (vb) {
            if (!reqState.test(vb->getState())) {
                continue;
            }

            Vbid vbid = id.hton();
            uint64_t highSeqno{0};

            if (reqCollection) {
                // The collection may not exist in any given vBucket.
                // Check this instead of throwing and catching an
                // exception.
                auto handle = vb->lockCollections();
                if (handle.exists(reqCollection.value())) {
                    if (reqCollection.value() == CollectionID::Default &&
                        !collectionsEnabled) {
                        highSeqno =
                                supportsSyncWrites
                                        ? handle.getDefaultCollectionMaxLegacyDCPSeqno()
                                        : handle.getDefaultCollectionMaxVisibleSeqno();
                    } else {
                        // supports collections thus supports SeqAdvanced. KV
                        // returns the high-seqno which may or may not be
                        // visible
                        highSeqno = handle.getHighSeqno(*reqCollection);
                    }
                } else {
                    // If the collection doesn't exist in this
                    // vBucket, return nothing for this vBucket by
                    // not adding anything to the payload during this
                    // iteration.
                    continue;
                }
            } else {
                if (vb->getState() == vbucket_state_active) {
                    highSeqno = supportsSyncWrites || collectionsEnabled
                                        ? vb->getHighSeqno()
                                        : vb->getMaxVisibleSeqno();
                } else {
                    highSeqno =
                            supportsSyncWrites || collectionsEnabled
                                    ? vb->checkpointManager->getSnapshotInfo()
                                              .range.getEnd()
                                    : vb->checkpointManager
                                              ->getVisibleSnapshotEndSeqno();
                }
            }
            auto offset = payload.size();
            payload.resize(offset + sizeof(vbid) + sizeof(highSeqno));
            memcpy(payload.data() + offset, &vbid, sizeof(vbid));
            highSeqno = htonll(highSeqno);
            memcpy(payload.data() + offset + sizeof(vbid),
                   &highSeqno,
                   sizeof(highSeqno));
        }
    }

    return sendResponse(response,
                        {}, /* key */
                        {}, /* ext field */
                        {payload.data(), payload.size()}, /* value */
                        ValueIsJson::No,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

cb::engine_errc EventuallyPersistentEngine::doGetAllVbSeqnosPrivilegeCheck(
        CookieIface& cookie, std::optional<CollectionID> collection) {
    // 1) Clients that have not enabled collections (i.e. HELLO(collections)).
    //   The client is directed to operate only against the default collection
    //   and they are permitted to use GetAllVbSeqnos with Read access to the
    //   default collection
    //
    // 2) Clients that have encoded a collection require Read towards that
    //    collection
    //
    // 3) Clients that have enabked collections making a bucket request can
    //    use GetAllVbSeqnos as long as they have at least Read privilege
    //    for one collection/scope in the bucket
    if (!cookie.isCollectionsSupported()) {
        auto accessStatus = checkPrivilege(cookie,
                                           cb::rbac::Privilege::Read,
                                           ScopeID::Default,
                                           CollectionID::Default);
        // For the legacy client always return an access error. This covers
        // the case where in a collection enabled world, unknown_collection
        // is returned for the no-privs case (to prevent someone finding
        // collection ids via access checks). unknown_collection could also
        // trigger a disconnect (unless xerror is enabled)
        if (accessStatus != cb::engine_errc::success) {
            accessStatus = cb::engine_errc::no_access;
        }

        return accessStatus;
    }

    if (collection) {
        return checkCollectionAccess(
                cookie,
                {},
                cb::rbac::Privilege::SystemCollectionLookup,
                cb::rbac::Privilege::Read,
                *collection);
    }

    return checkForPrivilegeAtLeastInOneCollection(cookie,
                                                   cb::rbac::Privilege::Read);
}

void EventuallyPersistentEngine::updateDcpMinCompressionRatio(float value) {
    if (dcpConnMap_) {
        dcpConnMap_->updateMinCompressionRatioForProducers(value);
    }
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
cb::engine_errc EventuallyPersistentEngine::sendErrorResponse(
        const AddResponseFn& response,
        cb::mcbp::Status status,
        uint64_t cas,
        CookieIface& cookie) {
    // no body/ext data for the error
    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        ValueIsJson::No,
                        status,
                        cas,
                        cookie);
}

cb::engine_errc EventuallyPersistentEngine::sendMutationExtras(
        const AddResponseFn& response,
        Vbid vbucket,
        uint64_t bySeqno,
        cb::mcbp::Status status,
        uint64_t cas,
        CookieIface& cookie) {
    VBucketPtr vb = kvBucket->getVBucket(vbucket);
    if (!vb) {
        return sendErrorResponse(
                response, cb::mcbp::Status::NotMyVbucket, cas, cookie);
    }
    const uint64_t uuid = htonll(vb->failovers->getLatestUUID());
    bySeqno = htonll(bySeqno);
    std::array<char, 16> meta;
    memcpy(meta.data(), &uuid, sizeof(uuid));
    memcpy(meta.data() + sizeof(uuid), &bySeqno, sizeof(bySeqno));
    return sendResponse(response,
                        {}, // key
                        {meta.data(), meta.size()}, // extra
                        {}, // body
                        ValueIsJson::No,
                        status,
                        cas,
                        cookie);
}

std::unique_ptr<KVBucket> EventuallyPersistentEngine::makeBucket(
        Configuration& config) {
    const auto bucketType = config.getBucketTypeString();
    if (bucketType == "persistent") {
        return std::make_unique<EPBucket>(*this);
    }
    if (bucketType == "ephemeral") {
        EphemeralBucket::reconfigureForEphemeral(configuration);
        return std::make_unique<EphemeralBucket>(*this);
    }
    throw std::invalid_argument(bucketType +
                                " is not a recognized bucket "
                                "type");
}

cb::engine_errc EventuallyPersistentEngine::setVBucketState(
        CookieIface& cookie,
        Vbid vbid,
        vbucket_state_t to,
        const nlohmann::json* meta,
        TransferVB transfer,
        uint64_t cas) {
    auto status = kvBucket->setVBucketState(vbid, to, meta, transfer, &cookie);
    if (status == cb::engine_errc::out_of_range) {
        setErrorContext(cookie, "VBucket number too big");
    }

    return status;
}

EventuallyPersistentEngine::~EventuallyPersistentEngine() {
    workload.reset();
    checkpointConfig.reset();
    /* Unique_ptr(s) are deleted in the reverse order of the initialization */
}

const std::string& EpEngineTaskable::getName() const {
    return myEngine->getName();
}

task_gid_t EpEngineTaskable::getGID() const {
    return reinterpret_cast<task_gid_t>(myEngine);
}

bucket_priority_t EpEngineTaskable::getWorkloadPriority() const {
    return myEngine->getWorkloadPriority();
}

void  EpEngineTaskable::setWorkloadPriority(bucket_priority_t prio) {
    myEngine->setWorkloadPriority(prio);
}

WorkLoadPolicy&  EpEngineTaskable::getWorkLoadPolicy() {
    return myEngine->getWorkLoadPolicy();
}

void EpEngineTaskable::logQTime(const GlobalTask& task,
                                std::string_view threadName,
                                cb::time::steady_clock::duration enqTime) {
    myEngine->getKVBucket()->logQTime(task, threadName, enqTime);
}

void EpEngineTaskable::logRunTime(const GlobalTask& task,
                                  std::string_view threadName,
                                  cb::time::steady_clock::duration runTime) {
    myEngine->getKVBucket()->logRunTime(task, threadName, runTime);
}

bool EpEngineTaskable::isShutdown() const {
    return myEngine->getEpStats().isShutdown;
}

void EpEngineTaskable::invokeViaTaskable(std::function<void()> fn) {
    BucketAllocationGuard guard(myEngine);
    fn();
}

item_info EventuallyPersistentEngine::getItemInfo(const Item& item) {
    VBucketPtr vb = getKVBucket()->getVBucket(item.getVBucketId());
    uint64_t uuid = 0;
    int64_t hlcEpoch = HlcCasSeqnoUninitialised;

    if (vb) {
        uuid = vb->failovers->getLatestUUID();
        hlcEpoch = vb->getHLCEpochSeqno();
    }

    return item.toItemInfo(uuid, hlcEpoch);
}

void EventuallyPersistentEngine::setCompressionMode(
        const std::string& compressModeStr) {
    BucketCompressionMode oldCompressionMode = compressionMode;

    try {
        compressionMode = parseCompressionMode(compressModeStr);
        if (oldCompressionMode != compressionMode) {
            EP_LOG_INFO_CTX("Transitioning compression mode",
                            {"from", to_string(oldCompressionMode)},
                            {"to", compressModeStr});
            kvBucket->processCompressionModeChange();
        }
    } catch (const std::invalid_argument& e) {
        EP_LOG_WARN("{}", e.what());
    }
}

// Set the max_size, low/high water mark (absolute values and percentages)
// and some other interested parties.
void EventuallyPersistentEngine::setMaxDataSize(size_t size) {
    getConfiguration().setMaxSize(size);
    stats.setMaxDataSize(size); // Set first because following code may read

    kvBucket->autoConfigCheckpointMaxSize();

    // Ratio hasn't changed in config, but the call recomputes all consumers'
    // buffer sizes based on the new Bucket Quota value
    setDcpConsumerBufferRatio(configuration.getDcpConsumerBufferRatio());

    // Setting the quota must set the water-marks and the new water-mark values
    // must be readable from both the configuration and EPStats. The following
    // is also updating EPStats because the configuration has a change listener
    // that will update EPStats
    configureMemWatermarksForQuota(size);

    kvBucket->getKVStoreScanTracker().updateMaxRunningScans(
            size,
            configuration.getRangeScanKvStoreScanRatio(),
            configuration.getDcpBackfillInProgressPerConnectionLimit());

    configureStorageMemoryForQuota(size);

    updateArenaAllocThresholdForQuota(size);
}

void EventuallyPersistentEngine::configureMemWatermarksForQuota(size_t quota) {
    auto lowWaterMarkPercent = configuration.getMemLowWatPercent();
    auto highWaterMarkPercent = configuration.getMemHighWatPercent();
    stats.setLowWaterMark(cb::fractionOf(quota, lowWaterMarkPercent));
    stats.setHighWaterMark(cb::fractionOf(quota, highWaterMarkPercent));
}

void EventuallyPersistentEngine::configureStorageMemoryForQuota(size_t quota) {
    // Pass the max bucket quota size down to the storage layer.
    for (uint16_t ii = 0; ii < getKVBucket()->getVBuckets().getNumShards();
         ++ii) {
        getKVBucket()->getVBuckets().getShard(ii)->forEachKVStore(
                [quota](auto* kvs) { kvs->setMaxDataSize(quota); });
    }
}

void EventuallyPersistentEngine::updateArenaAllocThresholdForQuota(
        size_t size) {
    getArenaMallocClient().setEstimateUpdateThreshold(
            size, configuration.getMemUsedMergeThresholdPercent());
    cb::ArenaMalloc::setAllocatedThreshold(getArenaMallocClient());
}

void EventuallyPersistentEngine::notify_num_writer_threads_changed() {
    auto* epBucket = dynamic_cast<EPBucket*>(getKVBucket());
    if (epBucket) {
        // We just changed number of writers so we also need to refresh the
        // flusher batch split trigger to adjust our limits accordingly.
        epBucket->setFlusherBatchSplitTrigger(
                configuration.getFlusherTotalBatchLimit());
    }
}

void EventuallyPersistentEngine::notify_num_auxio_threads_changed() {
    configureRangeScanConcurrency(configuration.getRangeScanMaxContinueTasks());
}

void EventuallyPersistentEngine::configureRangeScanConcurrency(
        size_t rangeScanMaxContinueTasksValue) {
    if (auto* epBucket = dynamic_cast<EPBucket*>(getKVBucket()); epBucket) {
        // Notify RangeScans that AUXIO has changed. Note some unit-tests don't
        // have a rangeScans object. In all cases we care about auxio changes
        // like a full server build - we do...
        Expects(epBucket->getReadyRangeScans());
        epBucket->getReadyRangeScans()->setConcurrentTaskLimit(
                rangeScanMaxContinueTasksValue);
    }
}

void EventuallyPersistentEngine::configureRangeScanMaxDuration(
        std::chrono::seconds rangeScanMaxDuration) {
    if (auto* epBucket = dynamic_cast<EPBucket*>(getKVBucket()); epBucket) {
        Expects(epBucket->getReadyRangeScans());
        epBucket->getReadyRangeScans()->setMaxDuration(rangeScanMaxDuration);
    }
}

void EventuallyPersistentEngine::set_num_storage_threads(
        ThreadPoolConfig::StorageThreadCount num) {
    auto* epBucket = dynamic_cast<EPBucket*>(getKVBucket());
    if (epBucket) {
        epBucket->getOneRWUnderlying()->setStorageThreads(num);
    }
}

void EventuallyPersistentEngine::disconnect(CookieIface& cookie) {
    acquireEngine(this)->handleDisconnect(cookie);
}

cb::engine_errc EventuallyPersistentEngine::set_traffic_control_mode(
        CookieIface& cookie, TrafficControlMode mode) {
    return acquireEngine(this)->handleTrafficControlCmd(cookie, mode);
}

cb::engine_errc EventuallyPersistentEngine::compactDatabase(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes,
        const std::vector<std::string>& obsoleteKeys) {
    return acquireEngine(this)->compactDatabaseInner(cookie,
                                                     vbid,
                                                     purge_before_ts,
                                                     purge_before_seq,
                                                     drop_deletes,
                                                     obsoleteKeys);
}

std::pair<cb::engine_errc, vbucket_state_t>
EventuallyPersistentEngine::getVBucket(CookieIface& cookie, Vbid vbid) {
    return acquireEngine(this)->getVBucketInner(cookie, vbid);
}

cb::engine_errc EventuallyPersistentEngine::setVBucket(CookieIface& cookie,
                                                       Vbid vbid,
                                                       uint64_t cas,
                                                       vbucket_state_t state,
                                                       nlohmann::json* meta) {
    return acquireEngine(this)->setVBucketInner(cookie, vbid, cas, state, meta);
}

cb::engine_errc EventuallyPersistentEngine::deleteVBucket(CookieIface& cookie,
                                                          Vbid vbid,
                                                          bool sync) {
    return acquireEngine(this)->deleteVBucketInner(cookie, vbid, sync);
}

std::pair<cb::engine_errc, cb::rangescan::Id>
EventuallyPersistentEngine::createRangeScan(
        CookieIface& cookie, const cb::rangescan::CreateParameters& params) {
    return acquireEngine(this)->getKVBucket()->createRangeScan(
            cookie,
            nullptr, // No RangeScanDataHandler to 'inject'
            params);
}

cb::engine_errc EventuallyPersistentEngine::continueRangeScan(
        CookieIface& cookie, const cb::rangescan::ContinueParameters& params) {
    return acquireEngine(this)->getKVBucket()->continueRangeScan(cookie,
                                                                 params);
}

cb::engine_errc EventuallyPersistentEngine::cancelRangeScan(
        CookieIface& cookie, Vbid vbid, cb::rangescan::Id uuid) {
    return acquireEngine(this)->getKVBucket()->cancelRangeScan(
            vbid, uuid, cookie);
}

cb::engine_errc EventuallyPersistentEngine::doRangeScanStats(
        const BucketStatCollector& collector, std::string_view statKey) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const VBucketFilter& filter,
                           const BucketStatCollector& collector)
            : VBucketVisitor(filter), collector(collector) {
        }

        void visitBucket(VBucket& vb) override {
            if (vb.getState() == vbucket_state_active) {
                vb.doRangeScanStats(collector);
            }
        }

    private:
        const BucketStatCollector& collector;
    };

    VBucketFilter filter;
    if (statKey > "range-scans ") {
        // A vbucket-ID must follow
        auto id = statKey.substr(sizeof("range-scans ") - 1, statKey.size());

        try {
            checkNumeric(id.data());
        } catch (std::runtime_error&) {
            return cb::engine_errc::invalid_arguments;
        }

        uint16_t result{0};
        auto [ptr,
              ec]{std::from_chars(id.data(), id.data() + id.size(), result)};

        if (ec != std::errc()) {
            return cb::engine_errc::invalid_arguments;
        }

        // Stats only for one vbucket
        filter.assign(std::set{Vbid(result)});
    }

    StatVBucketVisitor svbv(filter, collector);

    kvBucket->visit(svbv);

    return cb::engine_errc::success;
}

/**
 * Helper function that returns a copy of the provided referenced object, copy
 * allocated in the NonBucket domain.
 * Any dynamically-allocated object returned from any EngineIface implementation
 * can pass the returned object(s) through this function for avoiding that
 * objects are allocated in a bucket domain and then released at caller in the
 * NonBucket domain.
 *
 * @tparam T the type
 * @param obj Const ref to the object to be copied. Const ref prevents
 *            unnecessary arg copy for both const/non-const lvalues and
 *            temporary objects
 * @return A copy of obj, allocated in the NonBucket domain
 */
template <class T>
[[nodiscard]] static T copyToNonBucketDomain(const T& obj) {
    NonBucketAllocationGuard g;
    return obj;
};

std::pair<cb::engine_errc, nlohmann::json>
EventuallyPersistentEngine::getFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid, std::time_t validity) {
    return copyToNonBucketDomain(
            acquireEngine(this)->getFusionStorageSnapshotInner(
                    vbid, snapshotUuid, validity));
}

cb::engine_errc EventuallyPersistentEngine::releaseFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid) {
    return acquireEngine(this)->releaseFusionStorageSnapshotInner(vbid,
                                                                  snapshotUuid);
}

cb::engine_errc EventuallyPersistentEngine::mountVBucket(
        CookieIface& cookie,
        Vbid vbid,
        VBucketSnapshotSource source,
        const std::vector<std::string>& paths,
        const std::function<void(const nlohmann::json&)>& setResponse) {
    auto eng = acquireEngine(this);
    const auto result = kvBucket->mountVBucket(cookie, vbid, source, paths);
    if (result.hasError()) {
        return result.error();
    }
    nlohmann::json response{{"deks", result.value()}};
    NonBucketAllocationGuard guard;
    setResponse(response);
    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::unmountVBucket(Vbid vbid) {
    return acquireEngine(this)->unmountVBucketInner(vbid);
}

cb::engine_errc EventuallyPersistentEngine::syncFusionLogstore(Vbid vbid) {
    return acquireEngine(this)->syncFusionLogstoreInner(vbid);
}

cb::engine_errc EventuallyPersistentEngine::startFusionUploader(Vbid vbid,
                                                                uint64_t term) {
    return acquireEngine(this)->startFusionUploaderInner(vbid, term);
}

cb::engine_errc EventuallyPersistentEngine::stopFusionUploader(Vbid vbid) {
    return acquireEngine(this)->stopFusionUploaderInner(vbid);
}

cb::engine_errc EventuallyPersistentEngine::pause(
        folly::CancellationToken cancellationToken) {
    return kvBucket->prepareForPause(cancellationToken);
}

cb::engine_errc EventuallyPersistentEngine::resume() {
    return kvBucket->prepareForResume();
}

cb::engine_errc EventuallyPersistentEngine::start_persistence(
        CookieIface& cookie) {
    return acquireEngine(this)->startFlusher(cookie);
}

cb::engine_errc EventuallyPersistentEngine::stop_persistence(
        CookieIface& cookie) {
    return acquireEngine(this)->stopFlusher(cookie);
}

cb::engine_errc EventuallyPersistentEngine::wait_for_seqno_persistence(
        CookieIface& cookie, uint64_t seqno, Vbid vbid) {
    return acquireEngine(this)->handleSeqnoPersistence(cookie, seqno, vbid);
}

cb::engine_errc EventuallyPersistentEngine::evict_key(CookieIface& cookie,
                                                      const DocKeyView& key,
                                                      Vbid vbucket) {
    return acquireEngine(this)->evictKey(cookie, key, vbucket);
}

[[nodiscard]] cb::engine_errc
EventuallyPersistentEngine::set_active_encryption_keys(
        CookieIface& cookie, const nlohmann::json& json) {
    return acquireEngine(this)->setActiveEncryptionKeys(cookie, json);
}

cb::engine_errc EventuallyPersistentEngine::prepare_snapshot(
        CookieIface& cookie,
        Vbid vbid,
        const std::function<void(const nlohmann::json&)>& callback) {
    std::function<void(const nlohmann::json&)> non_alloc =
            [&callback](const auto& json) {
                NonBucketAllocationGuard guard;
                callback(json);
            };
    return acquireEngine(this)->getKVBucket()->prepareSnapshot(
            cookie, vbid, non_alloc);
}

cb::engine_errc EventuallyPersistentEngine::download_snapshot(
        CookieIface& cookie, Vbid vbid, std::string_view metadata) {
    return acquireEngine(this)->getKVBucket()->downloadSnapshot(
            cookie, vbid, metadata);
}

cb::engine_errc EventuallyPersistentEngine::get_snapshot_file_info(
        CookieIface& cookie,
        std::string_view uuid,
        std::size_t file_id,
        const std::function<void(const nlohmann::json&)>& callback) {
    std::function<void(const nlohmann::json&)> non_alloc =
            [&callback](const auto& json) {
                NonBucketAllocationGuard guard;
                callback(json);
            };
    return acquireEngine(this)->getKVBucket()->getSnapshotFileInfo(
            cookie, uuid, file_id, non_alloc);
}

cb::engine_errc EventuallyPersistentEngine::release_snapshot(
        CookieIface& cookie,
        std::variant<Vbid, std::string_view> snapshotToRelease) {
    return acquireEngine(this)->getKVBucket()->releaseSnapshot(
            cookie, snapshotToRelease);
}

cb::engine_errc EventuallyPersistentEngine::setActiveEncryptionKeys(
        CookieIface& cookie, const nlohmann::json& json) {
    cb::crypto::KeyStore ks = json["keystore"];
    bool rewriteAccessLog = false;
    {
        // Extra scope to let the shared ptr's die
        auto nextActive = ks.getActiveKey();
        auto currentActive = encryptionKeyProvider.lookup({});
        if ((!currentActive && nextActive) || (!nextActive && currentActive)) {
            // going from off->on or on->off
            rewriteAccessLog = true;
        }
    }

    std::vector<std::string> missing_keys;
    for (const auto& k : json["unavailable"]) {
        const auto key = k.get<std::string>();
        if (!ks.lookup(key)) {
            auto sk = encryptionKeyProvider.lookup(key);
            if (sk) {
                ks.add(sk);
            } else {
                missing_keys.emplace_back(key);
            }
        }
    }

    if (!missing_keys.empty()) {
        nlohmann::json missing_keys_json = missing_keys;
        EP_LOG_WARN_CTX(
                "Failed to update Data encryption",
                {"status", cb::engine_errc::encryption_key_not_available},
                {"missing", missing_keys_json});
        setErrorContext(cookie,
                        fmt::format("Unknown encryption key(s): {}",
                                    missing_keys_json.dump()));

        return cb::engine_errc::encryption_key_not_available;
    }

    encryptionKeyProvider.setKeys(std::move(ks));

    if (rewriteAccessLog) {
        runAccessScannerTask();
    }
    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::setDcpConsumerBufferRatio(float ratio) {
    if (dcpFlowControlManager) {
        dcpFlowControlManager->setDcpConsumerBufferRatio(ratio);
    }
}

float EventuallyPersistentEngine::getDcpConsumerBufferRatio() const {
    if (!dcpFlowControlManager) {
        return 0;
    }
    return dcpFlowControlManager->getDcpConsumerBufferRatio();
}

QuotaSharingManager& EventuallyPersistentEngine::getQuotaSharingManager() {
    struct QuotaSharingManagerImpl : public QuotaSharingManager {
        QuotaSharingManagerImpl(
                ServerBucketIface& bucketApi,
                std::function<size_t()> getNumConcurrentPagers,
                std::function<std::chrono::milliseconds()> getPagerSleepTime)
            : bucketApi(bucketApi),
              group(bucketApi),
              getNumConcurrentPagers(std::move(getNumConcurrentPagers)),
              getPagerSleepTime(std::move(getPagerSleepTime)) {
        }

        EPEngineGroup& getGroup() override {
            return group;
        }

        ExTask getItemPager() override {
            static ExTask task = [this]() {
                return std::make_shared<QuotaSharingItemPager>(
                        bucketApi,
                        group,
                        ExecutorPool::get()->getDefaultTaskable(),
                        getNumConcurrentPagers,
                        getPagerSleepTime);
            }();
            return task;
        }

        ServerBucketIface& bucketApi;
        EPEngineGroup group;
        const std::function<size_t()> getNumConcurrentPagers;
        const std::function<std::chrono::milliseconds()> getPagerSleepTime;
    };

    static QuotaSharingManagerImpl manager(
            *serverApi->bucket,
            [coreApi = serverApi->core]() {
                return coreApi->getQuotaSharingPagerConcurrency();
            },
            [coreApi = serverApi->core]() {
                return coreApi->getQuotaSharingPagerSleepTime();
            });
    return manager;
}

ExTask EventuallyPersistentEngine::createItemPager() {
    if (isCrossBucketHtQuotaSharing) {
        return getQuotaSharingManager().getItemPager();
    }
    auto numConcurrentPagers = getConfiguration().getConcurrentPagers();
    auto task = std::make_shared<StrictQuotaItemPager>(
            *this, stats, numConcurrentPagers);
    ExecutorPool::get()->cancel(task->getId());
    return task;
}

void EventuallyPersistentEngine::setDcpBackfillByteLimit(size_t bytes) {
    if (dcpConnMap_) {
        dcpConnMap_->setBackfillByteLimit(bytes);
    }
}

cb::engine_errc EventuallyPersistentEngine::maybeRemapStatus(
        cb::engine_errc status) {
    if (status == cb::engine_errc::not_my_vbucket && mustRemapNMVB()) {
        return cb::engine_errc::temporary_failure;
    }
    if (status == cb::engine_errc::no_such_key && isDegradedMode() &&
        kvBucket->getItemEvictionPolicy() == EvictionPolicy::Value) {
        return cb::engine_errc::temporary_failure;
    }
    return status;
}

cb::engine_errc EventuallyPersistentEngine::unmountVBucketInner(Vbid vbid) {
    Expects(kvBucket);
    return kvBucket->unmountVBucket(vbid);
}

std::pair<cb::engine_errc, nlohmann::json>
EventuallyPersistentEngine::getFusionStorageSnapshotInner(
        Vbid vbid, std::string_view snapshotUuid, std::time_t validity) {
    Expects(kvBucket);
    if (!kvBucket->getStorageProperties().supportsFusion()) {
        return {cb::engine_errc::not_supported, {}};
    }

    const auto fusionNamespace = getFusionNamespace();
    return kvBucket->getRWUnderlying(vbid)->getFusionStorageSnapshot(
            fusionNamespace, vbid, snapshotUuid, validity);
}

cb::engine_errc EventuallyPersistentEngine::releaseFusionStorageSnapshotInner(
        Vbid vbid, std::string_view snapshotUuid) {
    Expects(kvBucket);
    if (!kvBucket->getStorageProperties().supportsFusion()) {
        return cb::engine_errc::not_supported;
    }

    const auto fusionNamespace = getFusionNamespace();
    return kvBucket->getRWUnderlying(vbid)->releaseFusionStorageSnapshot(
            fusionNamespace, vbid, snapshotUuid);
}

cb::engine_errc EventuallyPersistentEngine::setChronicleAuthToken(
        std::string_view token) {
    Expects(kvBucket);
    if (!kvBucket->getStorageProperties().supportsFusion()) {
        return cb::engine_errc::not_supported;
    }

    chronicleAuthToken = std::string(token);

    kvBucket->forEachShard([&token](KVShard& shard) {
        shard.getRWUnderlying()->setChronicleAuthToken(token);
    });

    return cb::engine_errc::success;
}

std::string EventuallyPersistentEngine::getCachedChronicleAuthToken() const {
    return *chronicleAuthToken.rlock();
}

std::string EventuallyPersistentEngine::getFusionNamespace() const {
    // FusionNamespace is in the form:
    // <service_prefix>/<bucket_uuid>.
    return generateFusionNamespace(configuration.getUuid());
}

cb::engine_errc EventuallyPersistentEngine::syncFusionLogstoreInner(Vbid vbid) {
    Expects(kvBucket);
    return kvBucket->syncFusionLogstore(vbid);
}

cb::engine_errc EventuallyPersistentEngine::startFusionUploaderInner(
        Vbid vbid, uint64_t term) {
    Expects(kvBucket);
    return kvBucket->startFusionUploader(vbid, term);
}

cb::engine_errc EventuallyPersistentEngine::stopFusionUploaderInner(Vbid vbid) {
    Expects(kvBucket);
    return kvBucket->stopFusionUploader(vbid);
}

std::chrono::seconds
EventuallyPersistentEngine::getDcpDisconnectWhenStuckTimeout() const {
    return serverApi->core->getDcpDisconnectWhenStuckTimeout();
}

std::string EventuallyPersistentEngine::getDcpDisconnectWhenStuckNameRegex()
        const {
    return serverApi->core->getDcpDisconnectWhenStuckNameRegex();
}

void EventuallyPersistentEngine::setNotLockedReturnsTmpfail(bool value) {
    notLockedReturnsTmpfail = value;
}

cb::engine_errc EventuallyPersistentEngine::getNotLockedError() const {
    const bool useTmpfail = notLockedReturnsTmpfail ||
                            serverApi->core->getNotLockedReturnsTmpfail();
    return useTmpfail ? cb::engine_errc::temporary_failure
                      : cb::engine_errc::not_locked;
}

double EventuallyPersistentEngine::getDcpConsumerMaxMarkerVersion() const {
    return serverApi->core->getDcpConsumerMaxMarkerVersion();
}

bool EventuallyPersistentEngine::isDcpSnapshotMarkerHPSEnabled() const {
    return serverApi->core->isDcpSnapshotMarkerHPSEnabled();
}

bool EventuallyPersistentEngine::isDcpSnapshotMarkerPurgeSeqnoEnabled() const {
    return serverApi->core->isDcpSnapshotMarkerPurgeSeqnoEnabled();
}

bool EventuallyPersistentEngine::isMagmaBlindWriteOptimisationEnabled() const {
    return serverApi->core->isMagmaBlindWriteOptimisationEnabled();
}

size_t EventuallyPersistentEngine::getFileFragmentChecksumLength() const {
    return serverApi->core->getFileFragmentChecksumLength();
}

bool EventuallyPersistentEngine::isFileFragmentChecksumEnabled() const {
    return serverApi->core->isFileFragmentChecksumEnabled();
}