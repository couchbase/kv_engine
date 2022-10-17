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
#include "kv_bucket.h"

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_config.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "collections/vbucket_manifest_handles.h"
#include "common.h"
#include "dcp/consumer.h"
#include "dcp/dcpconnmap_impl.h"
#include "dcp/flow-control-manager.h"
#include "dcp/msg_producers_border_guard.h"
#include "dcp/producer.h"
#include "dockey_validator.h"
#include "environment.h"
#include "ep_bucket.h"
#include "ep_engine_public.h"
#include "ep_engine_storage.h"
#include "ep_vb.h"
#include "ephemeral_bucket.h"
#include "error_handler.h"
#include "ext_meta_parser.h"
#include "failover-table.h"
#include "flusher.h"
#include "getkeys.h"
#include "hash_table_stat_visitor.h"
#include "htresizer.h"
#include "kvstore/kvstore.h"
#include "range_scans/range_scan_callbacks.h"
#include "replicationthrottle.h"
#include "server_document_iface_border_guard.h"
#include "stats-info.h"
#include "string_utils.h"
#include "trace_helpers.h"
#include "vb_count_visitor.h"
#include "warmup.h"
#include <executor/executorpool.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
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
#include <memcached/server_cookie_iface.h>
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
#include <platform/string_hex.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/logtags.h>
#include <xattr/utils.h>

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

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

struct EPHandleReleaser {
    void operator()(const EventuallyPersistentEngine*) {
        ObjectRegistry::onSwitchThread(nullptr);
    }
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
    ObjectRegistry::onSwitchThread(ret);

    return EPHandle(ret);
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
    ObjectRegistry::onSwitchThread(
            const_cast<EventuallyPersistentEngine*>(ret));

    return ConstEPHandle(ret);
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static cb::engine_errc sendResponse(const AddResponseFn& response,
                                    std::string_view key,
                                    std::string_view ext,
                                    std::string_view body,
                                    uint8_t datatype,
                                    cb::mcbp::Status status,
                                    uint64_t cas,
                                    CookieIface& cookie) {
    if (response(key, ext, body, datatype, status, cas, cookie)) {
        return cb::engine_errc::success;
    }
    return cb::engine_errc::failed;
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static cb::engine_errc sendResponse(const AddResponseFn& response,
                                    const DocKey& key,
                                    std::string_view ext,
                                    std::string_view body,
                                    uint8_t datatype,
                                    cb::mcbp::Status status,
                                    uint64_t cas,
                                    CookieIface& cookie) {
    if (response(std::string_view(key),
                 ext,
                 body,
                 datatype,
                 status,
                 cas,
                 cookie)) {
        return cb::engine_errc::success;
    }

    return cb::engine_errc::failed;
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
    auto eng = acquireEngine(this);
    cb::ArenaMalloc::switchToClient(eng->getArenaMallocClient());
    eng->destroyInner(force);
    delete eng.get();
}

cb::unique_item_ptr EventuallyPersistentEngine::allocateItem(
        CookieIface& cookie,
        const DocKey& key,
        size_t nbytes,
        size_t priv_nbytes,
        int flags,
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
        const DocKey& key,
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
        const DocKey& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

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

cb::EngineErrorItemPair EventuallyPersistentEngine::get_if(
        CookieIface& cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    return acquireEngine(this)->getIfInner(cookie, key, vbucket, filter);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_and_touch(
        CookieIface& cookie,
        const DocKey& key,
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
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    return acquireEngine(this)->getLockedInner(
            cookie, key, vbucket, lock_timeout);
}

cb::engine_errc EventuallyPersistentEngine::unlock(CookieIface& cookie,
                                                   const DocKey& key,
                                                   Vbid vbucket,
                                                   uint64_t cas) {
    return acquireEngine(this)->unlockInner(cookie, key, vbucket, cas);
}

/**
 * generic lambda function which creates an "ExitBorderGuard" thunk - a wrapper
 * around the passed-in function which uses NonBucketAllocationGuard to switch
 * away from the current engine before invoking 'wrapped', then switches back to
 * the original engine after wrapped returns.
 *
 * The intended use-case of this is to invoke methods / callbacks outside
 * of ep-engine without mis-accounting memory - we need to ensure that any
 * memory allocated from inside the callback is *not* accounted to ep-engine,
 * but when the callback returns we /do/ account any subsequent allocations
 * to the given engine.
 */
auto makeExitBorderGuard = [](auto&& wrapped) {
    return [wrapped](auto&&... args) {
        NonBucketAllocationGuard exitGuard;
        return wrapped(std::forward<decltype(args)>(args)...);
    };
};

cb::engine_errc EventuallyPersistentEngine::get_stats(
        CookieIface& cookie,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat) {
    // The AddStatFn callback may allocate memory (temporary buffers for
    // stat data) which will be de-allocated inside the server, after the
    // engine call (get_stat) has returned. As such we do not want to
    // account such memory against this bucket.
    // Create an exit border guard around the original callback.
    // Perf: use std::cref to avoid copying (and the subsequent `new` call) of
    // the input add_stat function.
    auto addStatExitBorderGuard = makeExitBorderGuard(std::cref(add_stat));

    return acquireEngine(this)->getStats(
            cookie, key, value, addStatExitBorderGuard);
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
    if (status = doEngineStatsHighCardinality(collector);
        status != cb::engine_errc::success) {
        return status;
    }
    if (status = Collections::Manager::doPrometheusCollectionStats(
                *getKVBucket(), collector);
        status != cb::engine_errc::success) {
        return cb::engine_errc(status);
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

    if (status = doEngineStatsLowCardinality(collector);
        status != cb::engine_errc::success) {
        return status;
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

cb::engine_errc EventuallyPersistentEngine::setReplicationParam(
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::engine_errc::success;

    try {
        // Last param (replication_throttle_threshold) in this group removed.
        // @todo MB-52953: Remove the replication_param group from cbepctl,
        //  the existing dcp_param seems enough for now

        msg = "Unknown config param";
        rv = cb::engine_errc::no_such_key;

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
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::engine_errc::success;

    try {
        auto& config = getConfiguration();

        if (key == "max_checkpoints") {
            config.setMaxCheckpoints(std::stoull(val));
        } else if (key == "checkpoint_memory_ratio") {
            config.setCheckpointMemoryRatio(std::stof(val));
        } else if (key == "checkpoint_memory_recovery_upper_mark") {
            config.setCheckpointMemoryRecoveryUpperMark(std::stof(val));
        } else if (key == "checkpoint_memory_recovery_lower_mark") {
            config.setCheckpointMemoryRecoveryLowerMark(std::stof(val));
        } else if (key == "checkpoint_max_size") {
            config.setCheckpointMaxSize(std::stoull(val));
        } else {
            msg = "Unknown config param";
            rv = cb::engine_errc::no_such_key;
        }

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

cb::engine_errc EventuallyPersistentEngine::setFlushParam(
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::engine_errc::success;

    // Handle the actual mutation.
    try {
        configuration.requirementsMetOrThrow(key);

        if (key == "max_size" || key == "cache_size") {
            size_t vsize = std::stoull(val);
            kvBucket->processBucketQuotaChange(vsize);
        } else if (key == "mem_low_wat") {
            configuration.setMemLowWat(std::stoull(val));
        } else if (key == "mem_high_wat") {
            configuration.setMemHighWat(std::stoull(val));
        } else if (key == "backfill_mem_threshold") {
            configuration.setBackfillMemThreshold(std::stoull(val));
        } else if (key == "durability_min_level") {
            configuration.setDurabilityMinLevel(val);
        } else if (key == "mutation_mem_ratio") {
            configuration.setMutationMemRatio(std::stof(val));
        } else if (key == "timing_log") {
            EPStats& epStats = getEpStats();
            epStats.timingLog = nullptr;
            if (val == "off") {
                EP_LOG_DEBUG_RAW("Disabled timing log.");
            } else {
                auto tmp = std::make_unique<std::ofstream>(val);
                if (tmp->good()) {
                    EP_LOG_DEBUG("Logging detailed timings to ``{}''.", val);
                    epStats.timingLog = std::move(tmp);
                } else {
                    EP_LOG_WARN(
                            "Error setting detailed timing log to ``{}'':  {}",
                            val,
                            strerror(errno));
                }
            }
        } else if (key == "exp_pager_enabled") {
            configuration.setExpPagerEnabled(cb_stob(val));
        } else if (key == "exp_pager_stime") {
            configuration.setExpPagerStime(std::stoull(val));
        } else if (key == "exp_pager_initial_run_time") {
            configuration.setExpPagerInitialRunTime(std::stoll(val));
        } else if (key == "flusher_total_batch_limit") {
            configuration.setFlusherTotalBatchLimit(std::stoll(val));
        } else if (key == "getl_default_timeout") {
            configuration.setGetlDefaultTimeout(std::stoull(val));
        } else if (key == "getl_max_timeout") {
            configuration.setGetlMaxTimeout(std::stoull(val));
        } else if (key == "ht_resize_interval") {
            configuration.setHtResizeInterval(std::stoull(val));
        } else if (key == "max_item_privileged_bytes") {
            configuration.setMaxItemPrivilegedBytes(std::stoull(val));
        } else if (key == "max_item_size") {
            configuration.setMaxItemSize(std::stoull(val));
        } else if (key == "access_scanner_enabled") {
            configuration.setAccessScannerEnabled(cb_stob(val));
        } else if (key == "alog_path") {
            configuration.setAlogPath(val);
        } else if (key == "alog_max_stored_items") {
            configuration.setAlogMaxStoredItems(std::stoull(val));
        } else if (key == "alog_resident_ratio_threshold") {
            configuration.setAlogResidentRatioThreshold(std::stoull(val));
        } else if (key == "alog_sleep_time") {
            configuration.setAlogSleepTime(std::stoull(val));
        } else if (key == "alog_task_time") {
            configuration.setAlogTaskTime(std::stoull(val));
            /* Start of ItemPager parameters */
        } else if (key == "bfilter_fp_prob") {
            configuration.setBfilterFpProb(std::stof(val));
        } else if (key == "bfilter_key_count") {
            configuration.setBfilterKeyCount(std::stoull(val));
        } else if (key == "pager_sleep_time_ms") {
            configuration.setPagerSleepTimeMs(std::stoull(val));
        } else if (key == "item_eviction_age_percentage") {
            configuration.setItemEvictionAgePercentage(std::stoull(val));
        } else if (key == "item_eviction_freq_counter_age_threshold") {
            configuration.setItemEvictionFreqCounterAgeThreshold(
                    std::stoull(val));
        } else if (key == "item_freq_decayer_chunk_duration") {
            configuration.setItemFreqDecayerChunkDuration(std::stoull(val));
        } else if (key == "item_freq_decayer_percent") {
            configuration.setItemFreqDecayerPercent(std::stoull(val));
            /* End of ItemPager parameters */
        } else if (key == "warmup_min_memory_threshold") {
            configuration.setWarmupMinMemoryThreshold(std::stoull(val));
        } else if (key == "warmup_min_items_threshold") {
            configuration.setWarmupMinItemsThreshold(std::stoull(val));
        } else if (key == "num_reader_threads" || key == "num_writer_threads" ||
                   key == "num_auxio_threads" || key == "num_nonio_threads") {
            msg = fmt::format(
                    "Setting of key '{0}' is no longer supported "
                    "using set flush param, a post request to the REST API "
                    "(POST "
                    "http://<host>:<port>/pools/default/settings/memcached/"
                    "global with payload {0}=N) should be made instead",
                    key);
            return cb::engine_errc::invalid_arguments;
        } else if (key == "bfilter_enabled") {
            configuration.setBfilterEnabled(cb_stob(val));
        } else if (key == "bfilter_residency_threshold") {
            configuration.setBfilterResidencyThreshold(std::stof(val));
        } else if (key == "defragmenter_enabled") {
            configuration.setDefragmenterEnabled(cb_stob(val));
        } else if (key == "defragmenter_interval") {
            auto v = std::stod(val);
            configuration.setDefragmenterInterval(v);
        } else if (key == "item_compressor_interval") {
            size_t v = std::stoull(val);
            // Adding separate validation as external limit is minimum 1
            // to prevent setting item compressor to constantly run
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            configuration.setItemCompressorInterval(v);
        } else if (key == "item_compressor_chunk_duration") {
            configuration.setItemCompressorChunkDuration(std::stoull(val));
        } else if (key == "defragmenter_age_threshold") {
            configuration.setDefragmenterAgeThreshold(std::stoull(val));
        } else if (key == "defragmenter_chunk_duration") {
            configuration.setDefragmenterChunkDuration(std::stoull(val));
        } else if (key == "defragmenter_stored_value_age_threshold") {
            configuration.setDefragmenterStoredValueAgeThreshold(
                    std::stoull(val));
        } else if (key == "defragmenter_run") {
            runDefragmenterTask();
        } else if (key == "defragmenter_mode") {
            configuration.setDefragmenterMode(val);
        } else if (key == "defragmenter_auto_lower_threshold") {
            configuration.setDefragmenterAutoLowerThreshold(std::stof(val));
        } else if (key == "defragmenter_auto_upper_threshold") {
            configuration.setDefragmenterAutoUpperThreshold(std::stof(val));
        } else if (key == "defragmenter_auto_max_sleep") {
            configuration.setDefragmenterAutoMaxSleep(std::stof(val));
        } else if (key == "defragmenter_auto_min_sleep") {
            configuration.setDefragmenterAutoMinSleep(std::stof(val));
        } else if (key == "defragmenter_auto_pid_p") {
            configuration.setDefragmenterAutoPidP(std::stof(val));
        } else if (key == "defragmenter_auto_pid_i") {
            configuration.setDefragmenterAutoPidI(std::stof(val));
        } else if (key == "defragmenter_auto_pid_d") {
            configuration.setDefragmenterAutoPidD(std::stof(val));
        } else if (key == "defragmenter_auto_pid_dt") {
            configuration.setDefragmenterAutoPidDt(std::stof(val));
        } else if (key == "compaction_max_concurrent_ratio") {
            configuration.setCompactionMaxConcurrentRatio(std::stof(val));
        } else if (key == "chk_expel_enabled") {
            configuration.setChkExpelEnabled(cb_stob(val));
        } else if (key == "dcp_min_compression_ratio") {
            configuration.setDcpMinCompressionRatio(std::stof(val));
        } else if (key == "dcp_noop_mandatory_for_v5_features") {
            configuration.setDcpNoopMandatoryForV5Features(cb_stob(val));
        } else if (key == "access_scanner_run") {
            if (!(runAccessScannerTask())) {
                rv = cb::engine_errc::temporary_failure;
            }
        } else if (key == "vb_state_persist_run") {
            runVbStatePersistTask(Vbid(std::stoi(val)));
        } else if (key == "ephemeral_full_policy") {
            configuration.setEphemeralFullPolicy(val);
        } else if (key == "ephemeral_metadata_mark_stale_chunk_duration") {
            configuration.setEphemeralMetadataMarkStaleChunkDuration(
                    std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_age") {
            configuration.setEphemeralMetadataPurgeAge(std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_interval") {
            configuration.setEphemeralMetadataPurgeInterval(std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_stale_chunk_duration") {
            configuration.setEphemeralMetadataPurgeStaleChunkDuration(
                    std::stoull(val));
        } else if (key == "fsync_after_every_n_bytes_written") {
            configuration.setFsyncAfterEveryNBytesWritten(std::stoull(val));
        } else if (key == "xattr_enabled") {
            configuration.setXattrEnabled(cb_stob(val));
        } else if (key == "compression_mode") {
            configuration.setCompressionMode(val);
        } else if (key == "min_compression_ratio") {
            float min_comp_ratio;
            if (safe_strtof(val, min_comp_ratio)) {
                configuration.setMinCompressionRatio(min_comp_ratio);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "max_ttl") {
            configuration.setMaxTtl(std::stoull(val));
        } else if (key == "mem_used_merge_threshold_percent") {
            configuration.setMemUsedMergeThresholdPercent(std::stof(val));
        } else if (key == "retain_erroneous_tombstones") {
            configuration.setRetainErroneousTombstones(cb_stob(val));
        } else if (key == "couchstore_tracing") {
            configuration.setCouchstoreTracing(cb_stob(val));
        } else if (key == "couchstore_write_validation") {
            configuration.setCouchstoreWriteValidation(cb_stob(val));
        } else if (key == "couchstore_mprotect") {
            configuration.setCouchstoreMprotect(cb_stob(val));
        } else if (key == "allow_sanitize_value_in_deletion") {
            configuration.setAllowSanitizeValueInDeletion(cb_stob(val));
        } else if (key == "pitr_enabled") {
            configuration.setPitrEnabled(cb_stob(val));
        } else if (key == "pitr_max_history_age") {
            uint32_t value;
            if (safe_strtoul(val, value)) {
                configuration.setPitrMaxHistoryAge(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "pitr_granularity") {
            uint32_t value;
            if (safe_strtoul(val, value)) {
                configuration.setPitrGranularity(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "magma_fragmentation_percentage") {
            float value;
            if (safe_strtof(val, value)) {
                configuration.setMagmaFragmentationPercentage(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "persistent_metadata_purge_age") {
            uint32_t value;
            if (safe_strtoul(val, value)) {
                configuration.setPersistentMetadataPurgeAge(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "magma_flusher_thread_percentage") {
            uint32_t value;
            if (safe_strtoul(val, value)) {
                configuration.setMagmaFlusherThreadPercentage(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "couchstore_file_cache_max_size") {
            uint32_t value;
            if (safe_strtoul(val, value)) {
                configuration.setCouchstoreFileCacheMaxSize(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "magma_mem_quota_ratio") {
            float value;
            if (safe_strtof(val, value)) {
                configuration.setMagmaMemQuotaRatio(value);
            } else {
                rv = cb::engine_errc::invalid_arguments;
            }
        } else if (key == "magma_enable_block_cache") {
            configuration.setMagmaEnableBlockCache(cb_stob(val));
        } else if (key == "compaction_expire_from_start") {
            configuration.setCompactionExpireFromStart(cb_stob(val));
        } else if (key == "vbucket_mapping_sanity_checking") {
            configuration.setVbucketMappingSanityChecking(cb_stob(val));
        } else if (key == "vbucket_mapping_sanity_checking_error_mode") {
            configuration.setVbucketMappingSanityCheckingErrorMode(val);
        } else if (key == "seqno_persistence_timeout") {
            configuration.setSeqnoPersistenceTimeout(std::stoul(val));
        } else if (key == "range_scan_max_continue_tasks") {
            configuration.setRangeScanMaxContinueTasks(std::stoul(val));
        } else if (key == "bucket_quota_change_task_poll_interval") {
            configuration.setBucketQuotaChangeTaskPollInterval(std::stoul
                                                                (val));
        } else if (key == "range_scan_read_buffer_send_size") {
            configuration.setRangeScanReadBufferSendSize(std::stoull(val));
        } else if (key == "range_scan_max_lifetime") {
            configuration.setRangeScanMaxLifetime(std::stoull(val));
        } else if (key == "item_eviction_strategy") {
            getConfiguration().setItemEvictionStrategy(val);
        } else if (key == "magma_per_document_compression_enabled") {
            configuration.setMagmaPerDocumentCompressionEnabled(cb_stob(val));
        } else {
            msg = "Unknown config param";
            rv = cb::engine_errc::invalid_arguments;
        }
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

        // Handles any miscellaneous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = cb::engine_errc::invalid_arguments;
    }

    return rv;
}

cb::engine_errc EventuallyPersistentEngine::setDcpParam(const std::string& key,
                                                        const std::string& val,
                                                        std::string& msg) {
    auto rv = cb::engine_errc::success;
    try {
        if (key == "dcp_consumer_buffer_ratio") {
            getConfiguration().setDcpConsumerBufferRatio(std::stof(val));
        } else if (key == "connection_manager_interval") {
            getConfiguration().setConnectionManagerInterval(std::stoull(val));
        } else if (key ==
                   "dcp_consumer_process_buffered_messages_yield_limit") {
            getConfiguration().setDcpConsumerProcessBufferedMessagesYieldLimit(
                    std::stoull(val));
        } else if (key == "dcp_consumer_process_buffered_messages_batch_size") {
            auto v = size_t(std::stoul(val));
            checkNumeric(val.c_str());
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpConsumerProcessBufferedMessagesBatchSize(
                    v);
        } else if (key == "dcp_enable_noop") {
            getConfiguration().setDcpEnableNoop(cb_stob(val));
        } else if (key == "dcp_idle_timeout") {
            auto v = size_t(std::stoul(val));
            checkNumeric(val.c_str());
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpIdleTimeout(v);
        } else if (key == "dcp_noop_tx_interval") {
            getConfiguration().setDcpNoopTxInterval(std::stoull(val));
        } else if (key == "dcp_producer_snapshot_marker_yield_limit") {
            getConfiguration().setDcpProducerSnapshotMarkerYieldLimit(
                    std::stoull(val));
        } else if (key == "dcp_takeover_max_time") {
            getConfiguration().setDcpTakeoverMaxTime(std::stoull(val));
        } else {
            msg = "Unknown config param";
            rv = cb::engine_errc::no_such_key;
        }
    } catch (std::runtime_error&) {
        msg = "Value out of range.";
        rv = cb::engine_errc::invalid_arguments;
    }

    return rv;
}

cb::engine_errc EventuallyPersistentEngine::setVbucketParam(
        Vbid vbucket,
        const std::string& key,
        const std::string& val,
        std::string& msg) {
    auto rv = cb::engine_errc::success;
    try {
        if (key == "hlc_drift_ahead_threshold_us") {
            uint64_t v = std::strtoull(val.c_str(), nullptr, 10);
            checkNumeric(val.c_str());
            getConfiguration().setHlcDriftAheadThresholdUs(v);
        } else if (key == "hlc_drift_behind_threshold_us") {
            uint64_t v = std::strtoull(val.c_str(), nullptr, 10);
            checkNumeric(val.c_str());
            getConfiguration().setHlcDriftBehindThresholdUs(v);
        } else if (key == "max_cas") {
            uint64_t v = std::strtoull(val.c_str(), nullptr, 10);
            checkNumeric(val.c_str());
            EP_LOG_INFO("setVbucketParam: max_cas:{} {}", v, vbucket);
            if (getKVBucket()->forceMaxCas(vbucket, v) !=
                cb::engine_errc::success) {
                rv = cb::engine_errc::not_my_vbucket;
                msg = "Not my vbucket";
            }
        } else {
            msg = "Unknown config param";
            rv = cb::engine_errc::no_such_key;
        }
    } catch (std::runtime_error&) {
        msg = "Value out of range.";
        rv = cb::engine_errc::invalid_arguments;
    }
    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::evictKey(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const char** msg) {
    const auto key = makeDocKey(cookie, request.getKey());
    EP_LOG_DEBUG("Manually evicting object with key {}",
                 cb::UserDataView(key.to_string()));
    auto rv = kvBucket->evictKey(key, request.getVBucket(), msg);
    if (rv == cb::mcbp::Status::NotMyVbucket ||
        rv == cb::mcbp::Status::KeyEnoent) {
        if (isDegradedMode()) {
            return cb::mcbp::Status::Etmpfail;
        }
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
    const std::string keyz(key);
    const std::string valz(value);

    std::string msg;
    cb::engine_errc ret = cb::engine_errc::no_such_key;
    switch (category) {
    case EngineParamCategory::Flush:
        ret = setFlushParam(keyz, valz, msg);
        break;
    case EngineParamCategory::Replication:
        ret = setReplicationParam(keyz, valz, msg);
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
    }

    if (ret != cb::engine_errc::success && !msg.empty()) {
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

cb::engine_errc EventuallyPersistentEngine::getReplicaCmd(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    DocKey key = makeDocKey(cookie, request.getKey());

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue rv(getKVBucket()->getReplica(
            key, request.getVBucket(), &cookie, options));
    auto error_code = rv.getStatus();
    if (error_code != cb::engine_errc::would_block) {
        ++(getEpStats().numOpsGet);
    }

    if (error_code == cb::engine_errc::success) {
        uint32_t flags = rv.item->getFlags();
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Read);
        cookie.addDocumentReadBytes(rv.item->getNBytes());
        return sendResponse(
                response,
                rv.item->getKey(), // key
                {reinterpret_cast<const char*>(&flags), sizeof(flags)}, // extra
                {rv.item->getData(), rv.item->getNBytes()}, // body
                rv.item->getDataType(),
                cb::mcbp::Status::Success,
                rv.item->getCas(),
                cookie);
    } else if (error_code == cb::engine_errc::temporary_failure) {
        return cb::engine_errc::no_such_key;
    }

    return error_code;
}

cb::engine_errc EventuallyPersistentEngine::compactDatabaseInner(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes) {
    if (takeEngineSpecific<ScheduledCompactionToken>(cookie)) {
        // This is a completion of a compaction. We cleared the engine-specific
        return cb::engine_errc::success;
    }

    CompactionConfig compactionConfig{
            purge_before_ts, purge_before_seq, drop_deletes, false};

    ++stats.pendingCompactions;
    // Set something in the EngineSpecfic so we can determine which phase of the
    // command is executing.
    storeEngineSpecific(cookie, ScheduledCompactionToken{});

    // returns would_block for success or another status (e.g. nmvb)
    const auto status = scheduleCompaction(vbid, compactionConfig, &cookie);

    Expects(status != cb::engine_errc::success);
    if (status != cb::engine_errc::would_block) {
        --stats.pendingCompactions;
        // failed, clear the engine-specific
        clearEngineSpecific(cookie);
        EP_LOG_WARN("Compaction of {} failed: {}", vbid, status);
    }

    return status;
}

cb::engine_errc EventuallyPersistentEngine::processUnknownCommandInner(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    auto res = cb::mcbp::Status::UnknownCommand;
    std::string dynamic_msg;
    const char* msg = nullptr;
    size_t msg_size = 0;

    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::GetAllVbSeqnos:
        return getAllVBucketSequenceNumbers(cookie, request, response);
    case cb::mcbp::ClientOpcode::StopPersistence:
        res = stopFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::StartPersistence:
        res = startFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::EvictKey:
        res = evictKey(cookie, request, &msg);
        break;
    case cb::mcbp::ClientOpcode::Observe:
        return observe(cookie, request, response);
    case cb::mcbp::ClientOpcode::ObserveSeqno:
        return observe_seqno(cookie, request, response);
    case cb::mcbp::ClientOpcode::SeqnoPersistence:
        return handleSeqnoPersistence(cookie, request, response);
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
    case cb::mcbp::ClientOpcode::GetReplica:
        return getReplicaCmd(cookie, request, response);
    case cb::mcbp::ClientOpcode::EnableTraffic:
    case cb::mcbp::ClientOpcode::DisableTraffic:
        return handleTrafficControlCmd(cookie, request, response);
    case cb::mcbp::ClientOpcode::GetRandomKey:
        return getRandomKey(cookie, request, response);
    case cb::mcbp::ClientOpcode::GetKeys:
        return getAllKeys(cookie, request, response);
    default:
        res = cb::mcbp::Status::UnknownCommand;
    }

    msg_size = (msg_size > 0 || msg == nullptr) ? msg_size : strlen(msg);
    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {msg, msg_size}, // body
                        PROTOCOL_BINARY_RAW_BYTES,
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
    auto& conn = engine->getConnHandler(cookie);
    DcpMsgProducersBorderGuard guardedProducers(producers);
    return conn.step(throttled, guardedProducers);
}

cb::engine_errc EventuallyPersistentEngine::open(CookieIface& cookie,
                                                 uint32_t opaque,
                                                 uint32_t seqno,
                                                 uint32_t flags,
                                                 std::string_view conName,
                                                 std::string_view value) {
    return acquireEngine(this)->dcpOpen(
            cookie, opaque, seqno, flags, conName, value);
}

cb::engine_errc EventuallyPersistentEngine::add_stream(CookieIface& cookie,
                                                       uint32_t opaque,
                                                       Vbid vbucket,
                                                       uint32_t flags) {
    return acquireEngine(this)->dcpAddStream(cookie, opaque, vbucket, flags);
}

cb::engine_errc EventuallyPersistentEngine::close_stream(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamId sid) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.closeStream(opaque, vbucket, sid);
}

cb::engine_errc EventuallyPersistentEngine::stream_req(
        CookieIface& cookie,
        uint32_t flags,
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
    auto& conn = engine->getConnHandler(cookie);
    try {
        return conn.streamRequest(flags,
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
    } catch (const cb::engine_error& e) {
        EP_LOG_INFO("stream_req engine_error {}", e.what());
        return cb::engine_errc(e.code().value());
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

    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    auto* conn = engine->tryGetConnHandler(cookie);
    // Note: (conn != nullptr) only if conn is a DCP connection
    if (conn) {
        auto* producer = dynamic_cast<DcpProducer*>(conn);
        // GetFailoverLog not supported for DcpConsumer
        if (!producer) {
            EP_LOG_WARN_RAW(
                    "Disconnecting - This connection doesn't support the dcp "
                    "get "
                    "failover log API");
            return cb::engine_errc::disconnect;
        }
        producer->setLastReceiveTime(ep_current_time());
        if (producer->doDisconnect()) {
            return cb::engine_errc::disconnect;
        }
    }
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        EP_LOG_WARN(
                "{} ({}) Get Failover Log failed because this "
                "vbucket doesn't exist",
                conn ? conn->logHeader() : "MCBP-Connection",
                vbucket);
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
    return engine->getConnHandler(cookie).streamEnd(opaque, vbucket, status);
}

cb::engine_errc EventuallyPersistentEngine::snapshot_marker(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> max_visible_seqno) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.snapshotMarker(opaque,
                               vbucket,
                               start_seqno,
                               end_seqno,
                               flags,
                               high_completed_seqno,
                               max_visible_seqno);
}

cb::engine_errc EventuallyPersistentEngine::mutation(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKey& key,
        cb::const_byte_buffer value,
        size_t priv_bytes,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint32_t flags,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t expiration,
        uint32_t lock_time,
        cb::const_byte_buffer meta,
        uint8_t nru) {
    if (!cb::mcbp::datatype::is_valid(datatype)) {
        EP_LOG_WARN_RAW(
                "Invalid value for datatype "
                " (DCPMutation)");
        return cb::engine_errc::invalid_arguments;
    }
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.mutation(opaque,
                         key,
                         value,
                         priv_bytes,
                         datatype,
                         cas,
                         vbucket,
                         flags,
                         by_seqno,
                         rev_seqno,
                         expiration,
                         lock_time,
                         meta,
                         nru);
}

cb::engine_errc EventuallyPersistentEngine::deletion(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKey& key,
        cb::const_byte_buffer value,
        size_t priv_bytes,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        cb::const_byte_buffer meta) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.deletion(opaque,
                         key,
                         value,
                         priv_bytes,
                         datatype,
                         cas,
                         vbucket,
                         by_seqno,
                         rev_seqno,
                         meta);
}

cb::engine_errc EventuallyPersistentEngine::deletion_v2(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKey& key,
        cb::const_byte_buffer value,
        size_t priv_bytes,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t delete_time) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.deletionV2(opaque,
                           key,
                           value,
                           priv_bytes,
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
        const DocKey& key,
        cb::const_byte_buffer value,
        size_t priv_bytes,
        uint8_t datatype,
        uint64_t cas,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t deleteTime) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.expiration(opaque,
                           key,
                           value,
                           priv_bytes,
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
    auto& conn = engine->getConnHandler(cookie);
    return conn.setVBucketState(opaque, vbucket, state);
}

cb::engine_errc EventuallyPersistentEngine::noop(CookieIface& cookie,
                                                 uint32_t opaque) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie).noop(opaque);
}

cb::engine_errc EventuallyPersistentEngine::buffer_acknowledgement(
        CookieIface& cookie, uint32_t opaque, uint32_t buffer_bytes) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.bufferAcknowledgement(opaque, buffer_bytes);
}

cb::engine_errc EventuallyPersistentEngine::control(CookieIface& cookie,
                                                    uint32_t opaque,
                                                    std::string_view key,
                                                    std::string_view value) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie).control(opaque, key, value);
}

cb::engine_errc EventuallyPersistentEngine::response_handler(
        CookieIface& cookie, const cb::mcbp::Response& response) {
    auto engine = acquireEngine(this);
    auto* conn = engine->tryGetConnHandler(cookie);
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
    auto& conn = engine->getConnHandler(cookie);
    return conn.systemEvent(
            opaque, vbucket, event, bySeqno, version, key, eventData);
}

cb::engine_errc EventuallyPersistentEngine::prepare(
        CookieIface& cookie,
        uint32_t opaque,
        const DocKey& key,
        cb::const_byte_buffer value,
        size_t priv_bytes,
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
    auto& conn = engine->getConnHandler(cookie);
    return conn.prepare(opaque,
                        key,
                        value,
                        priv_bytes,
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
    auto& conn = engine->getConnHandler(cookie);
    return conn.seqno_acknowledged(opaque, vbucket, prepared_seqno);
}

cb::engine_errc EventuallyPersistentEngine::commit(CookieIface& cookie,
                                                   uint32_t opaque,
                                                   Vbid vbucket,
                                                   const DocKey& key,
                                                   uint64_t prepared_seqno,
                                                   uint64_t commit_seqno) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.commit(opaque, vbucket, key, prepared_seqno, commit_seqno);
}

cb::engine_errc EventuallyPersistentEngine::abort(CookieIface& cookie,
                                                  uint32_t opaque,
                                                  Vbid vbucket,
                                                  const DocKey& key,
                                                  uint64_t preparedSeqno,
                                                  uint64_t abortSeqno) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.abort(opaque, vbucket, key, preparedSeqno, abortSeqno);
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

bool EventuallyPersistentEngine::get_item_info(const ItemIface& itm,
                                               item_info& itm_info) {
    const auto& it = static_cast<const Item&>(itm);
    itm_info = acquireEngine(this)->getItemInfo(it);
    return true;
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::get_meta(
        CookieIface& cookie, const DocKey& key, Vbid vbucket) {
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
    auto engine = acquireEngine(this);
    Collections::IsVisibleFunction isVisible =
            [&engine, &cookie](ScopeID sid,
                               std::optional<CollectionID> cid) -> bool {
        const auto status = engine->testPrivilege(
                cookie, cb::rbac::Privilege::Read, sid, cid);
        return status != cb::engine_errc::unknown_collection &&
               status != cb::engine_errc::unknown_scope;
    };
    auto rv = engine->getKVBucket()->getCollections(isVisible);

    std::string manifest;
    if (rv.first == cb::mcbp::Status::Success) {
        manifest = rv.second.dump();
    }
    return cb::engine_errc(
            sendResponse(makeExitBorderGuard(std::cref(response)),
                         {}, // key
                         {}, // extra
                         manifest, // body
                         PROTOCOL_BINARY_DATATYPE_JSON,
                         rv.first,
                         0,
                         cookie));
}

cb::EngineErrorGetCollectionIDResult
EventuallyPersistentEngine::get_collection_id(CookieIface& cookie,
                                              std::string_view path) {
    auto engine = acquireEngine(this);
    auto rv = engine->getKVBucket()->getCollectionID(path);

    if (rv.result == cb::engine_errc::success) {
        // Test for any privilege, we are testing if we have visibility which
        // means at least 1 privilege in the bucket.scope.collection 'path'
        auto status = testPrivilege(cookie,
                                    cb::rbac::Privilege::Read,
                                    rv.getScopeId(),
                                    rv.getCollectionId());
        if (status == cb::engine_errc::no_access) {
            // This is fine, still visible - back to success
            status = cb::engine_errc::success;
        }
        rv.result = status;
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
        // Test for any privilege, we are testing if we have visibility which
        // means at least 1 privilege in the bucket.scope 'path'
        auto status = testPrivilege(
                cookie, cb::rbac::Privilege::Read, rv.getScopeId(), {});
        if (status == cb::engine_errc::no_access) {
            // This is fine, still visible - back to success
            status = cb::engine_errc::success;
        }
        rv.result = status;
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
                        handle.isMetered() == Collections::Metered::Yes};
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
                entry->metered == Collections::Metered::Yes};
    }
    // returns unknown_collection and the manifest uid
    return cb::EngineErrorGetCollectionMetaResult(manifestUid);
}

cb::engine::FeatureSet EventuallyPersistentEngine::getFeatures() {
    // This function doesn't track memory against the engine, but create a
    // guard regardless to make this explicit because we only call this once per
    // bucket creation
    NonBucketAllocationGuard guard;
    return {cb::engine::Feature::Collections};
}

bool EventuallyPersistentEngine::isXattrEnabled() {
    return getKVBucket()->isXattrEnabled();
}

cb::HlcTime EventuallyPersistentEngine::getVBucketHlcNow(Vbid vbucket) {
    return getKVBucket()->getVBucket(vbucket)->getHLCNow();
}

EventuallyPersistentEngine::EventuallyPersistentEngine(
        GET_SERVER_API get_server_api, cb::ArenaMallocClient arena)
    : configuration(get_server_api()->core->isServerlessDeployment()),
      kvBucket(nullptr),
      workload(nullptr),
      workloadPriority(NO_BUCKET_PRIORITY),
      getServerApiFunc(get_server_api),
      checkpointConfig(nullptr),
      trafficEnabled(false),
      startupTime(0),
      taskable(this),
      compressionMode(BucketCompressionMode::Off),
      minCompressionRatio(default_min_compression_ratio),
      arena(arena) {
    // copy through to stats so we can ask for mem used
    getEpStats().arena = arena;

    serverApi = getServerApiFunc();
}

void EventuallyPersistentEngine::reserveCookie(CookieIface& cookie) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->reserve(cookie);
}

void EventuallyPersistentEngine::releaseCookie(CookieIface& cookie) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->release(cookie);
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

void EpEngineValueChangeListener::sizeValueChanged(const std::string& key,
                                                   size_t value) {
    if (key.compare("getl_max_timeout") == 0) {
        engine.setGetlMaxTimeout(value);
    } else if (key.compare("getl_default_timeout") == 0) {
        engine.setGetlDefaultTimeout(value);
    } else if (key.compare("max_item_size") == 0) {
        engine.setMaxItemSize(value);
    } else if (key.compare("max_item_privileged_bytes") == 0) {
        engine.setMaxItemPrivilegedBytes(value);
    } else if (key == "range_scan_max_continue_tasks") {
        engine.configureRangeScanConcurrency(value);
    } else if (key == "range_scan_max_lifetime") {
        engine.configureRangeScanMaxDuration(std::chrono::seconds(value));
    }
}

void EpEngineValueChangeListener::stringValueChanged(const std::string& key,
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

void EpEngineValueChangeListener::floatValueChanged(const std::string& key,
                                                    float value) {
    if (key == "min_compression_ratio") {
        engine.setMinCompressionRatio(value);
    } else if (key == "dcp_consumer_buffer_ratio") {
        engine.setDcpConsumerBufferRatio(value);
    }
}

void EpEngineValueChangeListener::booleanValueChanged(const std::string& key,
                                                      bool b) {
    if (key == "allow_sanitize_value_in_deletion") {
        engine.allowSanitizeValueInDeletion.store(b);
    } else if (key == "vbucket_mapping_sanity_checking") {
        engine.sanityCheckVBucketMapping = b;
    }
}

size_t EventuallyPersistentEngine::getShardCount() {
    auto configShardCount = configuration.getMaxNumShards();
    if (configuration.getBackend() != "magma") {
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
    Expects(configuration.getBackend() == "magma");

    // Look for the file
    const auto shardFile = std::filesystem::path(
            configuration.getDbname().append(magmaShardFile));
    if (std::filesystem::exists(shardFile)) {
        std::ifstream ifs(shardFile);
        std::string data;
        std::getline(ifs, data);
        EP_LOG_INFO("Found shard file for magma with {} shards", data);
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
    if (configuration.getBackend() == "magma") {
        // We should have created this directory already
        Expects(std::filesystem::exists(configuration.getDbname()));

        const auto shardFilePath = std::filesystem::path(
                configuration.getDbname().append(magmaShardFile));

        if (std::filesystem::exists(shardFilePath)) {
            // File already exists, don't overwrite it (we should have the same
            // of shards, it's just pointless).
            return;
        }

        auto* file = fopen(shardFilePath.string().c_str(), "w");
        if (!file) {
            throw std::runtime_error(
                    "EventuallyPersistentEngine::maybeSaveShardCount: Could "
                    "not load magma shard file");
        }

        auto shardStr = std::to_string(workloadPolicy.getNumShards());

        auto count = fwrite(shardStr.data(), shardStr.size(), 1, file);
        if (!count) {
            throw std::runtime_error(
                    "EventuallyPersistentEngine::maybeSaveShardCount: Error "
                    "writing shard count to file");
        }

        auto ret = fflush(file);
        if (ret != 0) {
            throw std::runtime_error(
                    "EventuallyPersistentEngine::maybeSaveShardCount: Error "
                    "flushing shard file: " +
                    std::to_string(ret));
        }

        ret = fclose(file);
        if (ret != 0) {
            throw std::runtime_error(
                    "EventuallyPersistentEngine::maybeSaveShardCount: Error "
                    "closing shard file: " +
                    std::to_string(ret));
        }

        Ensures(std::filesystem::exists(shardFilePath));
    }
}

cb::engine_errc EventuallyPersistentEngine::initialize(
        std::string_view config) {
    if (config.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    auto switchToEngine = acquireEngine(this);
    resetStats();

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
        EP_LOG_WARN("Failed to create data directory [{}]:{}",
                    dbName,
                    error.code().message());
        return cb::engine_errc::failed;
    }

    auto& env = Environment::get();
    env.engineFileDescriptors = serverApi->core->getMaxEngineFileDescriptors();

    maxFailoverEntries = configuration.getMaxFailoverEntries();

    if (configuration.getMaxSize() == 0) {
        EP_LOG_WARN_RAW(
                "Invalid configuration: max_size must be a non-zero value");
        return cb::engine_errc::failed;
    }

    maxItemSize = configuration.getMaxItemSize();
    configuration.addValueChangedListener(
            "max_item_size",
            std::make_unique<EpEngineValueChangeListener>(*this));

    maxItemPrivilegedBytes = configuration.getMaxItemPrivilegedBytes();
    configuration.addValueChangedListener(
            "max_item_privileged_bytes",
            std::make_unique<EpEngineValueChangeListener>(*this));

    getlDefaultTimeout = configuration.getGetlDefaultTimeout();
    configuration.addValueChangedListener(
            "getl_default_timeout",
            std::make_unique<EpEngineValueChangeListener>(*this));
    getlMaxTimeout = configuration.getGetlMaxTimeout();
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

    // The number of shards for a magma bucket cannot be changed after the first
    // bucket instantiation. This is because the number of shards determines
    // the on disk structure of the data. To solve this problem we store a file
    // to the data directory on first bucket creation that tells us how many
    // shards are to be used. We read this file here if it exists and use that
    // number, if not, this should be the first bucket creation.
    workload = std::make_unique<WorkLoadPolicy>(
            configuration.getMaxNumWorkers(), getShardCount());

    maybeSaveShardCount(*workload);

    setConflictResolutionMode(configuration.getConflictResolutionType());

    dcpConnMap_ = std::make_unique<DcpConnMap>(*this);

    if (configuration.isDcpConsumerFlowControlEnabled()) {
        dcpFlowControlManager = std::make_unique<DcpFlowControlManager>(*this);
    }

    checkpointConfig = std::make_unique<CheckpointConfig>(configuration);
    CheckpointConfig::addConfigChangeListener(*this);

    kvBucket = makeBucket(configuration);

    // Seed the watermark percentages to the default 75/85% or the current ratio
    if (configuration.getMemLowWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_low_wat_percent.store(0.75);
    } else {
        stats.mem_low_wat_percent.store(double(configuration.getMemLowWat()) /
                                        configuration.getMaxSize());
    }

    if (configuration.getMemHighWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_high_wat_percent.store(0.85);
    } else {
        stats.mem_high_wat_percent.store(double(configuration.getMemHighWat()) /
                                         configuration.getMaxSize());
    }

    setMaxDataSize(configuration.getMaxSize());

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

    EP_LOG_INFO("EP Engine: Initialization of {} bucket complete",
                configuration.getBucketType());

    setCompressionMode(configuration.getCompressionMode());

    configuration.addValueChangedListener(
            "compression_mode",
            std::make_unique<EpEngineValueChangeListener>(*this));

    setMinCompressionRatio(configuration.getMinCompressionRatio());

    configuration.addValueChangedListener(
            "min_compression_ratio",
            std::make_unique<EpEngineValueChangeListener>(*this));

    sanityCheckVBucketMapping = configuration.isVbucketMappingSanityChecking();
    vBucketMappingErrorHandlingMethod = cb::getErrorHandlingMethod(
            configuration.getVbucketMappingSanityCheckingErrorMode());

    configuration.addValueChangedListener(
            "vbucket_mapping_sanity_checking",
            std::make_unique<EpEngineValueChangeListener>(*this));
    configuration.addValueChangedListener(
            "vbucket_mapping_sanity_checking_error_mode",
            std::make_unique<EpEngineValueChangeListener>(*this));

    configuration.addValueChangedListener(
            "dcp_consumer_buffer_ratio",
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

cb::EngineErrorItemPair EventuallyPersistentEngine::itemAllocate(
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    if ((priv_nbytes > maxItemPrivilegedBytes) ||
        ((nbytes - priv_nbytes) > maxItemSize)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::too_big);
    }

    if (!hasMemoryForItemAllocation(sizeof(Item) + sizeof(Blob) + key.size() +
                                    nbytes)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc(memoryCondition()));
    }

    time_t expiretime = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

    try {
        auto* item = new Item(key,
                              flags,
                              expiretime,
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
        return cb::makeEngineErrorItemPair(cb::engine_errc(memoryCondition()));
    }
}

cb::engine_errc EventuallyPersistentEngine::removeInner(
        CookieIface& cookie,
        const DocKey& key,
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

    switch (ret) {
    case cb::engine_errc::no_such_key:
        // FALLTHROUGH
    case cb::engine_errc::not_my_vbucket:
        if (isDegradedMode()) {
            return cb::engine_errc::temporary_failure;
        }
        break;

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
        const DocKey& key,
        Vbid vbucket,
        get_options_t options) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
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
    } else if (ret == cb::engine_errc::no_such_key ||
               ret == cb::engine_errc::not_my_vbucket) {
        if (isDegradedMode()) {
            return cb::makeEngineErrorItemPair(
                    cb::engine_errc::temporary_failure);
        }
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(ret));
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getAndTouchInner(
        CookieIface& cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t exptime) {
    time_t expiry_time = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

    GetValue gv(kvBucket->getAndUpdateTtl(key, vbucket, &cookie, expiry_time));

    auto rv = gv.getStatus();
    if (rv == cb::engine_errc::success) {
        ++stats.numOpsGet;
        ++stats.numOpsStore;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
    }

    if (isDegradedMode()) {
        // Remap all some of the error codes
        switch (rv) {
        case cb::engine_errc::key_already_exists:
        case cb::engine_errc::no_such_key:
        case cb::engine_errc::not_my_vbucket:
            rv = cb::engine_errc::temporary_failure;
            break;
        default:
            break;
        }
    }

    if (rv == cb::engine_errc::key_already_exists) {
        rv = cb::engine_errc::locked;
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(rv));
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getIfInner(
        CookieIface& cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
            std::forward_as_tuple(stats.getCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::GetIf));

    // Fetch an item from the hashtable (without trying to schedule a bg-fetch
    // and pass it through the filter. If the filter accepts the document
    // based on the metadata, return the document. If the document's data
    // isn't resident we run another iteration in the loop and retries the
    // action but this time we _do_ schedule a bg-fetch.
    for (int ii = 0; ii < 2; ++ii) {
        auto options = static_cast<get_options_t>(HONOR_STATES |
                                                  DELETE_TEMP |
                                                  HIDE_LOCKED_CAS);

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

        switch (status) {
        case cb::engine_errc::success:
            break;

        case cb::engine_errc::no_such_key: // FALLTHROUGH
        case cb::engine_errc::not_my_vbucket: // FALLTHROUGH
            if (isDegradedMode()) {
                status = cb::engine_errc::temporary_failure;
            }
            // FALLTHROUGH
        default:
            return cb::makeEngineErrorItemPair(cb::engine_errc(status));
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
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    auto default_timeout = static_cast<uint32_t>(getGetlDefaultTimeout());

    if (lock_timeout == 0) {
        lock_timeout = default_timeout;
    } else if (lock_timeout > static_cast<uint32_t>(getGetlMaxTimeout())) {
        EP_LOG_WARN(
                "{}: EventuallyPersistentEngine::get_locked: Illegal value for "
                "lock timeout specified for {}: {}. Using default "
                "value: {}",
                cookie.getConnectionId(),
                cb::tagUserData(key.to_string()),
                lock_timeout,
                default_timeout);

        lock_timeout = default_timeout;
    }

    auto result = kvBucket->getLocked(
            key, vbucket, ep_current_time(), lock_timeout, &cookie);

    if (result.getStatus() == cb::engine_errc::success) {
        ++stats.numOpsGet;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, result.item.release(), this);
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(result.getStatus()));
}

cb::engine_errc EventuallyPersistentEngine::unlockInner(CookieIface& cookie,
                                                        const DocKey& key,
                                                        Vbid vbucket,
                                                        uint64_t cas) {
    return kvBucket->unlockKey(key, vbucket, cas, ep_current_time(), &cookie);
}

cb::EngineErrorCasPair EventuallyPersistentEngine::storeIfInner(
        CookieIface& cookie,
        Item& item,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        bool preserveTtl) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
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
            EP_LOG_WARN(
                    "EventuallyPersistentEngine::storeIfInner: attempting to "
                    "store a deleted document with non-zero value size which "
                    "is {}",
                    value_size);
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

    if (stats.getEstimatedTotalMemoryUsed() < stats.getMaxDataSize()) {
        // Still below bucket_quota - treat as temporary failure.
        ++stats.tmp_oom_errors;
        return cb::engine_errc::temporary_failure;
    } else {
        // Already over bucket quota - make this a hard error.
        ++stats.oom_errors;
        return cb::engine_errc::no_memory;
    }
}

bool EventuallyPersistentEngine::hasMemoryForItemAllocation(
        uint32_t totalItemSize) {
    return (stats.getEstimatedTotalMemoryUsed() + totalItemSize) <=
           stats.getMaxDataSize();
}

bool EventuallyPersistentEngine::enableTraffic(bool enable) {
    bool inverse = !enable;
    bool bTrafficEnabled =
            trafficEnabled.compare_exchange_strong(inverse, enable);
    if (bTrafficEnabled) {
        EP_LOG_INFO(
                "EventuallyPersistentEngine::enableTraffic: Traffic "
                "successfully {}",
                enable ? "enabled" : "disabled");
    } else {
        EP_LOG_WARN(
                "EventuallyPersistentEngine::enableTraffic: Failed to {} "
                "traffic - traffic was already {}",
                enable ? "enable" : "disable",
                enable ? "enabled" : "disabled");
    }
    return bTrafficEnabled;
}

void EventuallyPersistentEngine::doEngineStatsRocksDB(
        const StatCollector& collector) {
    using namespace cb::stats;
    size_t value;

    // Specific to RocksDB. Cumulative ep-engine stats.
    // Note: These are also reported per-shard in 'kvstore' stats.
    // Memory Usage
    if (kvBucket->getKVStoreStat("kMemTableTotal", value)) {
        collector.addStat(Key::ep_rocksdb_kMemTableTotal, value);
    }
    if (kvBucket->getKVStoreStat("kMemTableUnFlushed", value)) {
        collector.addStat(Key::ep_rocksdb_kMemTableUnFlushed, value);
    }
    if (kvBucket->getKVStoreStat("kTableReadersTotal", value)) {
        collector.addStat(Key::ep_rocksdb_kTableReadersTotal, value);
    }
    if (kvBucket->getKVStoreStat("kCacheTotal", value)) {
        collector.addStat(Key::ep_rocksdb_kCacheTotal, value);
    }
    // MemTable Size per-CF
    if (kvBucket->getKVStoreStat("default_kSizeAllMemTables", value)) {
        collector.addStat(Key::ep_rocksdb_default_kSizeAllMemTables, value);
    }
    if (kvBucket->getKVStoreStat("seqno_kSizeAllMemTables", value)) {
        collector.addStat(Key::ep_rocksdb_seqno_kSizeAllMemTables, value);
    }
    // BlockCache Hit Ratio
    size_t hit = 0;
    size_t miss = 0;
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.data.hit", hit) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.data.miss", miss) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_data_hit_ratio, tmpRatio);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.index.hit", hit) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.index.miss", miss) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_index_hit_ratio,
                          tmpRatio);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.filter.hit", hit) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.filter.miss", miss) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_filter_hit_ratio,
                          tmpRatio);
    }
    // Disk Usage per-CF
    if (kvBucket->getKVStoreStat("default_kTotalSstFilesSize", value)) {
        collector.addStat(Key::ep_rocksdb_default_kTotalSstFilesSize, value);
    }
    if (kvBucket->getKVStoreStat("seqno_kTotalSstFilesSize", value)) {
        collector.addStat(Key::ep_rocksdb_seqno_kTotalSstFilesSize, value);
    }
    // Scan stats
    if (kvBucket->getKVStoreStat("scan_totalSeqnoHits", value)) {
        collector.addStat(Key::ep_rocksdb_scan_totalSeqnoHits, value);
    }
    if (kvBucket->getKVStoreStat("scan_oldSeqnoHits", value)) {
        collector.addStat(Key::ep_rocksdb_scan_oldSeqnoHits, value);
    }
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
    constexpr std::array<std::string_view, 40> statNames = {
            {"magma_NCompacts",
             "magma_NFlushes",
             "magma_NTTLCompacts",
             "magma_NFileCountCompacts",
             "magma_NWriterCompacts",
             "magma_BytesOutgoing",
             "magma_NReadBytes",
             "magma_NReadBytesGet",
             "magma_NGets",
             "magma_NSets",
             "magma_NInserts",
             "magma_NReadIO",
             "magma_NReadBytesCompact",
             "magma_BytesIncoming",
             "magma_NWriteBytes",
             "magma_NWriteBytesCompact",
             "magma_LogicalDataSize",
             "magma_LogicalDiskSize",
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
             "magma_NSyncs"}};

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

    addStat(Key::ep_magma_sets, "magma_NSets");
    addStat(Key::ep_magma_gets, "magma_NGets");
    addStat(Key::ep_magma_inserts, "magma_NInserts");

    // Compaction counter stats.
    addStat(Key::ep_magma_compactions, "magma_NCompacts");
    addStat(Key::ep_magma_flushes, "magma_NFlushes");
    addStat(Key::ep_magma_ttl_compactions, "magma_NTTLCompacts");
    addStat(Key::ep_magma_filecount_compactions, "magma_NFileCountCompacts");
    addStat(Key::ep_magma_writer_compactions, "magma_NWriterCompacts");

    // Read amp, ReadIOAmp.
    size_t bytesOutgoing = 0;
    size_t readBytes = 0;
    if (statExists("magma_BytesOutgoing", bytesOutgoing) &&
        statExists("magma_NReadBytes", readBytes)) {
        collector.addStat(Key::ep_magma_bytes_outgoing, bytesOutgoing);
        collector.addStat(Key::ep_magma_read_bytes, readBytes);
        auto readAmp = divide(readBytes, bytesOutgoing);
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
    addStat(Key::ep_magma_read_bytes_compact, "magma_NReadBytesCompact");

    // Write amp.
    size_t bytesIncoming = 0;
    size_t writeBytes = 0;
    if (statExists("magma_BytesIncoming", bytesIncoming) &&
        statExists("magma_NWriteBytes", writeBytes)) {
        collector.addStat(Key::ep_magma_bytes_incoming, bytesIncoming);
        collector.addStat(Key::ep_magma_write_bytes, writeBytes);
        auto writeAmp = divide(writeBytes, bytesIncoming);
        collector.addStat(Key::ep_magma_writeamp, writeAmp);
    }
    addStat(Key::ep_magma_write_bytes_compact, "magma_NWriteBytesCompact");

    // Fragmentation.
    size_t logicalDataSize = 0;
    size_t logicalDiskSize = 0;
    if (statExists("magma_LogicalDataSize", logicalDataSize) &&
        statExists("magma_LogicalDiskSize", logicalDiskSize)) {
        collector.addStat(Key::ep_magma_logical_data_size, logicalDataSize);
        collector.addStat(Key::ep_magma_logical_disk_size, logicalDiskSize);
        double fragmentation =
                divide(logicalDiskSize - logicalDataSize, logicalDiskSize);
        collector.addStat(Key::ep_magma_fragmentation, fragmentation);
    }

    // Disk usage.
    addStat(Key::ep_magma_total_disk_usage, "magma_TotalDiskUsage");
    addStat(Key::ep_magma_wal_disk_usage, "magma_WALDiskUsage");

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
    size_t blockCacheHits = 0;
    size_t blockCacheMisses = 0;
    if (statExists("magma_BlockCacheHits", blockCacheHits) &&
        statExists("magma_BlockCacheMisses", blockCacheMisses)) {
        collector.addStat(Key::ep_magma_block_cache_hits, blockCacheHits);
        collector.addStat(Key::ep_magma_block_cache_misses, blockCacheMisses);
        auto total = blockCacheHits + blockCacheMisses;
        double hitRatio = divide(blockCacheHits, total);
        collector.addStat(Key::ep_magma_block_cache_hit_ratio, hitRatio);
    }

    // SST file counts.
    addStat(Key::ep_magma_tables_deleted, "magma_NTablesDeleted");
    addStat(Key::ep_magma_tables_created, "magma_NTablesCreated");
    addStat(Key::ep_magma_tables, "magma_NTableFiles");

    // NSyncs.
    addStat(Key::ep_magma_syncs, "magma_NSyncs");
}

cb::engine_errc EventuallyPersistentEngine::doEngineStats(
        const BucketStatCollector& collector) {
    cb::engine_errc status;
    if (status = doEngineStatsLowCardinality(collector);
        status != cb::engine_errc::success) {
        return status;
    }

    status = doEngineStatsHighCardinality(collector);
    return status;
}
cb::engine_errc EventuallyPersistentEngine::doEngineStatsLowCardinality(
        const BucketStatCollector& collector) {
    EPStats& epstats = getEpStats();

    using namespace cb::stats;

    collector.addStat(Key::ep_total_enqueued, epstats.totalEnqueued);
    collector.addStat(Key::ep_total_deduplicated, epstats.totalDeduplicated);
    collector.addStat(Key::ep_expired_access, epstats.expired_access);
    collector.addStat(Key::ep_expired_compactor, epstats.expired_compactor);
    collector.addStat(Key::ep_expired_pager, epstats.expired_pager);
    collector.addStat(Key::ep_queue_size, epstats.diskQueueSize);
    collector.addStat(Key::ep_diskqueue_items, epstats.diskQueueSize);
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
    collector.addStat(Key::ep_checkpoint_memory_recovery_upper_mark_bytes,
                      kvBucket->getCMRecoveryUpperMarkBytes());
    collector.addStat(Key::ep_checkpoint_memory_recovery_lower_mark_bytes,
                      kvBucket->getCMRecoveryLowerMarkBytes());
    collector.addStat(Key::ep_checkpoint_computed_max_size,
                      checkpointConfig->getCheckpointMaxSize());

    kvBucket->getFileStats(collector);

    collector.addStat(Key::ep_persist_vbstate_total,
                      epstats.totalPersistVBState);

    size_t memUsed = stats.getPreciseTotalMemoryUsed();
    collector.addStat(Key::mem_used, memUsed);
    collector.addStat(Key::mem_used_primary,
                      cb::ArenaMalloc::getEstimatedAllocated(
                              arena, cb::MemoryDomain::Primary));
    collector.addStat(Key::mem_used_secondary,
                      cb::ArenaMalloc::getEstimatedAllocated(
                              arena, cb::MemoryDomain::Secondary));
    collector.addStat(Key::mem_used_estimate,
                      stats.getEstimatedTotalMemoryUsed());
    collector.addStat(Key::bytes, memUsed);
    collector.addStat(Key::ep_kv_size, stats.getCurrentSize());
    collector.addStat(Key::ep_blob_num, stats.getNumBlob());
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    collector.addStat(Key::ep_blob_overhead, stats.getBlobOverhead());
#else
    collector.addStat(Key::ep_blob_overhead, "unknown");
#endif
    collector.addStat(Key::ep_value_size, stats.getTotalValueSize());
    collector.addStat(Key::ep_storedval_size, stats.getStoredValSize());
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    collector.addStat(Key::ep_storedval_overhead, stats.getBlobOverhead());
#else
    collector.addStat(Key::ep_storedval_overhead, "unknown");
#endif
    collector.addStat(Key::ep_storedval_num, stats.getNumStoredVal());
    collector.addStat(Key::ep_overhead, stats.getMemOverhead());
    collector.addStat(Key::ep_item_num, stats.getNumItem());

    collector.addStat(Key::ep_oom_errors, stats.oom_errors);
    collector.addStat(Key::ep_tmp_oom_errors, stats.tmp_oom_errors);
    collector.addStat(Key::ep_bg_fetched, epstats.bg_fetched);
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

    collector.addStat(Key::ep_pending_compactions, epstats.pendingCompactions);
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

    if (getConfiguration().getBucketType() == "persistent" &&
        getConfiguration().isWarmup()) {
        Warmup *wp = kvBucket->getWarmup();
        if (wp == nullptr) {
            throw std::logic_error("EPEngine::doEngineStats: warmup is NULL");
        }
        wp->addCommonStats(collector);
    }

    collector.addStat(Key::ep_num_ops_get_meta, epstats.numOpsGetMeta);
    collector.addStat(Key::ep_num_ops_set_meta, epstats.numOpsSetMeta);
    collector.addStat(Key::ep_num_ops_del_meta, epstats.numOpsDelMeta);
    collector.addStat(Key::ep_num_ops_set_meta_res_fail,
                      epstats.numOpsSetMetaResolutionFailed);
    collector.addStat(Key::ep_num_ops_del_meta_res_fail,
                      epstats.numOpsDelMetaResolutionFailed);
    collector.addStat(Key::ep_num_ops_set_ret_meta, epstats.numOpsSetRetMeta);
    collector.addStat(Key::ep_num_ops_del_ret_meta, epstats.numOpsDelRetMeta);
    collector.addStat(Key::ep_num_ops_get_meta_on_set_meta,
                      epstats.numOpsGetMetaOnSetWithMeta);
    collector.addStat(Key::ep_workload_pattern,
                      workload->stringOfWorkLoadPattern());

    doDiskFailureStats(collector);

    // Note: These are also reported per-shard in 'kvstore' stats, however
    // we want to be able to graph these over time, and hence need to expose
    // to ns_sever at the top-level.
    if (configuration.getBackend() == "couchdb") {
        doEngineStatsCouchDB(collector, epstats);
    } else if (configuration.getBackend() == "magma") {
        doEngineStatsMagma(collector);
    } else if (configuration.getBackend() == "rocksdb") {
        doEngineStatsRocksDB(collector);
    } else if (configuration.getBackend() == "nexus") {
        auto primaryCollector = collector.withLabel("backend", "primary");
        if (configuration.getNexusPrimaryBackend() == "couchdb") {
            doEngineStatsCouchDB(primaryCollector, epstats);
        } else if (configuration.getNexusPrimaryBackend() == "magma") {
            doEngineStatsMagma(primaryCollector);
        } else if (configuration.getNexusPrimaryBackend() == "rocksb") {
            doEngineStatsRocksDB(primaryCollector);
        }

        auto secondaryCollector = collector.withLabel("backend", "secondary");
        if (configuration.getNexusSecondaryBackend() == "couchdb") {
            doEngineStatsCouchDB(secondaryCollector, epstats);
        } else if (configuration.getNexusSecondaryBackend() == "magma") {
            doEngineStatsMagma(secondaryCollector);
        } else if (configuration.getNexusSecondaryBackend() == "rocksb") {
            doEngineStatsRocksDB(secondaryCollector);
        }
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doEngineStatsHighCardinality(
        const BucketStatCollector& collector) {
    configuration.addStats(collector);

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

    collector.addStat(Key::ep_mem_low_wat_percent, stats.mem_low_wat_percent);
    collector.addStat(Key::ep_mem_high_wat_percent, stats.mem_high_wat_percent);

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

    collector.addStat(Key::ep_item_compressor_num_visited,
                      epstats.compressorNumVisited);
    collector.addStat(Key::ep_item_compressor_num_compressed,
                      epstats.compressorNumCompressed);

    collector.addStat(Key::ep_cursors_dropped, epstats.cursorsDropped);
    collector.addStat(Key::ep_mem_freed_by_checkpoint_removal,
                      epstats.memFreedByCheckpointRemoval);
    collector.addStat(Key::ep_mem_freed_by_checkpoint_item_expel,
                      epstats.memFreedByCheckpointItemExpel);
    collector.addStat(Key::ep_num_checkpoints, epstats.getNumCheckpoints());
    collector.addStat(Key::ep_num_checkpoints_pending_destruction,
                      kvBucket->getNumCheckpointsPendingDestruction());

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
                    arena.estimateUpdateThreshold.load(),
                    add_stat,
                    cookie);

    // Note calling getEstimated as the precise value was requested previously
    // which will have updated these stats.
    add_casted_stat("ep_mem_used_primary",
                    cb::ArenaMalloc::getEstimatedAllocated(
                            arena, cb::MemoryDomain::Primary),
                    add_stat,
                    cookie);
    add_casted_stat("ep_mem_used_secondary",
                    cb::ArenaMalloc::getEstimatedAllocated(
                            arena, cb::MemoryDomain::Secondary),
                    add_stat,
                    cookie);

    add_casted_stat(
            "ht_mem_used_replica", stats.replicaHTMemory, add_stat, cookie);

    add_casted_stat("replica_checkpoint_memory_overhead",
                    stats.replicaCheckpointOverhead,
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
                    stats.mem_low_wat_percent,
                    add_stat,
                    cookie);
    add_casted_stat("ep_mem_high_wat", stats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat_percent",
                    stats.mem_high_wat_percent,
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
    bool missing = cb::ArenaMalloc::getStats(arena, alloc_stats);
    for (const auto& it : alloc_stats) {
        add_prefixed_stat(
                "ep_arena", it.first.c_str(), it.second, add_stat, cookie);
    }
    if (missing) {
        add_casted_stat("ep_arena_missing_some_keys", true, add_stat, cookie);
    }
    missing = cb::ArenaMalloc::getGlobalStats(alloc_stats);
    for (const auto& it : alloc_stats) {
        add_prefixed_stat("ep_arena_global",
                          it.first.c_str(),
                          it.second,
                          add_stat,
                          cookie);
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
        const char* stat_key,
        int nkey,
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
                    EP_LOG_WARN("addVBStats: Failed building stats: {}",
                                error.what());
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

    if (nkey > 16 && strncmp(stat_key, "vbucket-details", 15) == 0) {
        Expects(detail == VBucketStatsDetailLevel::Full);
        std::string vbid(&stat_key[16], nkey - 16);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return cb::engine_errc::invalid_arguments;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return cb::engine_errc::not_my_vbucket;
        }

        StatVBucketVisitor::addVBStats(cookie,
                                       add_stat,
                                       *vb,
                                       kvBucket.get(),
                                       VBucketStatsDetailLevel::Full);
    } else if (nkey > 25 &&
               strncmp(stat_key, "vbucket-durability-state", 24) == 0) {
        Expects(detail == VBucketStatsDetailLevel::Durability);
        std::string vbid(&stat_key[25], nkey - 25);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return cb::engine_errc::invalid_arguments;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return cb::engine_errc::not_my_vbucket;
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
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
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
                add_casted_stat(buf.data(),
                                depthVisitor.min == -1 ? 0 : depthVisitor.min,
                                add_stat,
                                cookie);
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
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
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
            EP_LOG_WARN(
                    "StatCheckpointVisitor::addCheckpointStat: error building "
                    "stats: {}",
                    error.what());
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
        const char* stat_key,
        int nkey) {
    if (nkey == 10) {
        TRACE_EVENT0("ep-engine/task", "StatsCheckpoint");
        auto* kvbucket = getKVBucket();
        StatCheckpointVisitor scv(kvbucket, cookie, add_stat);
        kvbucket->visit(scv);
        return cb::engine_errc::success;
    } else if (nkey > 11) {
        std::string vbid(&stat_key[11], nkey - 11);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return cb::engine_errc::invalid_arguments;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return cb::engine_errc::not_my_vbucket;
        }
        StatCheckpointVisitor::addCheckpointStat(
                cookie, add_stat, kvBucket.get(), *vb);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doDurabilityMonitorStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        const char* stat_key,
        int nkey) {
    const uint8_t size = 18; // size  of "durability-monitor"
    if (nkey == size) {
        // Case stat_key = "durability-monitor"
        // @todo: Return aggregated stats for all VBuckets.
        //     Implement as async, we don't what to block for too long.
        return cb::engine_errc::not_supported;
    } else if (nkey > size + 1) {
        // Case stat_key = "durability-monitor <vbid>"
        const uint16_t vbidPos = size + 1;
        std::string vbid_(&stat_key[vbidPos], nkey - vbidPos);
        uint16_t vbid(0);
        if (!parseUint16(vbid_.c_str(), &vbid)) {
            return cb::engine_errc::invalid_arguments;
        }

        VBucketPtr vb = getVBucket(Vbid(vbid));
        if (!vb) {
            // @todo: I would return an error code, but just replicating the
            //     behaviour of other stats for now
            return cb::engine_errc::success;
        }
        vb->addDurabilityMonitorStats(add_stat, cookie);
    }

    return cb::engine_errc::success;
}

class DcpStatsFilter {
public:
    explicit DcpStatsFilter(std::string_view value) {
        if (!value.empty()) {
            try {
                auto attributes = nlohmann::json::parse(value);
                auto filter = attributes.find("filter");
                if (filter != attributes.end()) {
                    auto iter = filter->find("user");
                    if (iter != filter->end()) {
                        user = cb::tagUserData(iter->get<std::string>());
                    }
                    iter = filter->find("port");
                    if (iter != filter->end()) {
                        port = iter->get<in_port_t>();
                    }
                }
            } catch (const std::exception& e) {
                EP_LOG_ERR(
                        "Failed to decode provided DCP filter: {}. Filter:{}",
                        e.what(),
                        value);
            }
        }
    }

    bool include(const std::shared_ptr<ConnHandler>& tc) {
        if ((user && *user != tc->getAuthenticatedUser()) ||
            (port && *port != tc->getConnectedPort())) {
            // Connection should not be part of this output
            return false;
        }

        return true;
    }

protected:
    std::optional<std::string> user;
    std::optional<in_port_t> port;
};

/**
 * Function object to send stats for a single dcp connection.
 */
struct ConnStatBuilder {
    ConnStatBuilder(CookieIface& c, AddStatFn as, DcpStatsFilter filter)
        : cookie(c), add_stat(std::move(as)), filter(std::move(filter)) {
    }

    void operator()(std::shared_ptr<ConnHandler> tc) {
        ++aggregator.totalConns;
        if (filter.include(tc)) {
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
    DcpStatsFilter filter;
    ConnCounter aggregator;
};

struct ConnAggStatBuilder {
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
     * @param tc connection
     * @return counter for the given connection, or nullptr
     */
    ConnCounter* getCounterForConnType(std::string_view name) {
        // strip everything upto and including the first colon,
        // e.g., "eq_dcpq:"
        size_t pos1 = name.find(':');
        if (pos1 == std::string_view::npos) {
            return nullptr;
        }

        name.remove_prefix(pos1 + 1);

        // find the given separator
        size_t pos2 = name.find(sep);
        if (pos2 == std::string_view::npos) {
            return nullptr;
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
        labelled.addStat(Key::connagg_items_sent, counter.conn_queueDrain);
        labelled.addStat(Key::connagg_items_remaining,
                          counter.conn_queueRemaining);
        labelled.addStat(Key::connagg_total_bytes, counter.conn_totalBytes);
        labelled.addStat(Key::connagg_total_uncompressed_data_size,
                          counter.conn_totalUncompressedDataSize);
        labelled.addStat(Key::connagg_ready_queue_bytes,
                         counter.conn_queueMemory);

    } catch (std::exception& error) {
        EP_LOG_WARN("showConnAggStat: Failed to build stats: {}", error.what());
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
        std::string_view value) {
    ConnStatBuilder dcpVisitor(cookie, add_stat, DcpStatsFilter{value});
    // ConnStatBuilder also adds per-stream stats while aggregating.
    dcpConnMap_->each(dcpVisitor);

    const auto& aggregator = dcpVisitor.getCounter();

    CBStatCollector collector(add_stat, cookie);
    addAggregatedProducerStats(collector.forBucket(getName()), aggregator);

    dcpConnMap_->addStats(add_stat, cookie);
    return cb::engine_errc::success;
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
    col.addStat(Key::dcp_total_queue, aggregator.conn_queue);
    col.addStat(Key::dcp_queue_fill, aggregator.conn_queueFill);
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
        const DocKey& key,
        bool validate) {
    auto rv = checkPrivilege(cookie, cb::rbac::Privilege::Read, key);
    if (rv != cb::engine_errc::success) {
        return rv;
    }

    std::unique_ptr<Item> it;
    struct key_stats kstats;

    // If this is a validating call, we need to fetch the item from disk. The
    // fetch task is scheduled by statsVKey(). fetchLookupResult will succeed
    // in returning the item once the fetch task has completed.
    if (validate && !fetchLookupResult(cookie, it)) {
        rv = kvBucket->statsVKey(key, vbid, &cookie);
        if (rv == cb::engine_errc::not_my_vbucket ||
            rv == cb::engine_errc::no_such_key) {
            if (isDegradedMode()) {
                return cb::engine_errc::temporary_failure;
            }
        }
        return rv;
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
            EP_LOG_DEBUG("doKeyStats key {} is {}",
                         cb::UserDataView(key.to_string()),
                         valid);
        }
        add_casted_stat("key_is_dirty", kstats.dirty, add_stat, cookie);
        add_casted_stat("key_exptime", kstats.exptime, add_stat, cookie);
        add_casted_stat("key_flags", kstats.flags, add_stat, cookie);
        add_casted_stat("key_cas", kstats.cas, add_stat, cookie);
        add_casted_stat("key_vb_state", VBucket::toString(kstats.vb_state),
                        add_stat,
                        cookie);
        add_casted_stat("key_is_resident", kstats.resident, add_stat, cookie);
        if (validate) {
            add_casted_stat("key_valid", valid.c_str(), add_stat, cookie);
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

static std::string getTaskDescrForStats(TaskId id) {
    return std::string(GlobalTask::getTaskName(id)) + "[" +
           to_string(GlobalTask::getTaskType(id)) + "]";
}

cb::engine_errc EventuallyPersistentEngine::doSchedulerStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.schedulingHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return cb::engine_errc::success;
}

cb::engine_errc EventuallyPersistentEngine::doRunTimeStats(
        CookieIface& cookie, const AddStatFn& add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.taskRuntimeHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
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

        int readers = expool->getNumReaders();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_readers");
        add_casted_stat(statname.data(), readers, add_stat, cookie);

        int writers = expool->getNumWriters();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_writers");
        add_casted_stat(statname.data(), writers, add_stat, cookie);

        int auxio = expool->getNumAuxIO();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_auxio");
        add_casted_stat(statname.data(), auxio, add_stat, cookie);

        int nonio = expool->getNumNonIO();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_nonio");
        add_casted_stat(statname.data(), nonio, add_stat, cookie);

        int shards = workload->getNumShards();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_shards");
        add_casted_stat(statname.data(), shards, add_stat, cookie);

        int numReadyTasks = expool->getNumReadyTasks();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:ready_tasks");
        add_casted_stat(statname.data(), numReadyTasks, add_stat, cookie);

        int numSleepers = expool->getNumSleepers();
        checked_snprintf(
                statname.data(), statname.size(), "ep_workload:num_sleepers");
        add_casted_stat(statname.data(), numSleepers, add_stat, cookie);

        expool->doTaskQStat(ObjectRegistry::getCurrentEngine()->getTaskable(),
                            cookie,
                            add_stat);

    } catch (std::exception& error) {
        EP_LOG_WARN("doWorkloadStats: Error building stats: {}", error.what());
    }

    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::addSeqnoVbStats(CookieIface& cookie,
                                                 const AddStatFn& add_stat,
                                                 const VBucketPtr& vb) {
    // MB-19359: An atomic read of vbucket state without acquiring the
    // reader lock for state should suffice here.
    uint64_t relHighSeqno = vb->getHighSeqno();
    if (vb->getState() != vbucket_state_active) {
        snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
        relHighSeqno = info.range.getEnd();
    }

    try {
        std::array<char, 64> buffer;
        failover_entry_t entry = vb->failovers->getLatestEntry();
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:high_seqno",
                         vb->getId().get());
        add_casted_stat(buffer.data(), relHighSeqno, add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:abs_high_seqno",
                         vb->getId().get());
        add_casted_stat(buffer.data(), vb->getHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:last_persisted_seqno",
                         vb->getId().get());
        add_casted_stat(buffer.data(),
                        vb->getPublicPersistenceSeqno(),
                        add_stat,
                        cookie);
        checked_snprintf(
                buffer.data(), buffer.size(), "vb_%d:uuid", vb->getId().get());
        add_casted_stat(buffer.data(), entry.vb_uuid, add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:purge_seqno",
                         vb->getId().get());
        add_casted_stat(buffer.data(), vb->getPurgeSeqno(), add_stat, cookie);
        const snapshot_range_t range = vb->getPersistedSnapshot();
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:last_persisted_snap_start",
                         vb->getId().get());
        add_casted_stat(buffer.data(), range.getStart(), add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:last_persisted_snap_end",
                         vb->getId().get());
        add_casted_stat(buffer.data(), range.getEnd(), add_stat, cookie);

        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:high_prepared_seqno",
                         vb->getId().get());
        add_casted_stat(
                buffer.data(), vb->getHighPreparedSeqno(), add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:high_completed_seqno",
                         vb->getId().get());
        add_casted_stat(
                buffer.data(), vb->getHighCompletedSeqno(), add_stat, cookie);
        checked_snprintf(buffer.data(),
                         buffer.size(),
                         "vb_%d:max_visible_seqno",
                         vb->getId().get());
        add_casted_stat(
                buffer.data(), vb->getMaxVisibleSeqno(), add_stat, cookie);

    } catch (std::exception& error) {
        EP_LOG_WARN("addSeqnoVbStats: error building stats: {}", error.what());
    }
}

void EventuallyPersistentEngine::addLookupResult(CookieIface& cookie,
                                                 std::unique_ptr<Item> result) {
    auto oldItem = takeEngineSpecific<std::unique_ptr<Item>>(cookie);
    // Check for old lookup results and clear them
    if (oldItem) {
        if (*oldItem) {
            EP_LOG_DEBUG("Cleaning up old lookup result for '{}'",
                         (*oldItem)->getKey().data());
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
    } else {
        return false;
    }
}

EventuallyPersistentEngine::StatusAndVBPtr
EventuallyPersistentEngine::getValidVBucketFromString(std::string_view vbNum) {
    if (vbNum.empty()) {
        // Must specify a vbucket.
        return {cb::engine_errc::invalid_arguments, {}};
    }
    uint16_t vbucket_id;
    // parseUint16 expects a null-terminated string
    std::string vbNumString(vbNum);
    if (!parseUint16(vbNumString.data(), &vbucket_id)) {
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
        const char* stat_key,
        int nkey) {
    if (getKVBucket()->maybeWaitForVBucketWarmup(&cookie)) {
        return cb::engine_errc::would_block;
    }

    if (nkey > 14) {
        std::string value(stat_key + 14, nkey - 14);

        try {
            checkNumeric(value.c_str());
        } catch(std::runtime_error &) {
            return cb::engine_errc::invalid_arguments;
        }

        Vbid vbucket(atoi(value.c_str()));
        VBucketPtr vb = getVBucket(vbucket);
        if (!vb || vb->getState() == vbucket_state_dead) {
            return cb::engine_errc::not_my_vbucket;
        }

        addSeqnoVbStats(cookie, add_stat, vb);

        return cb::engine_errc::success;
    }

    auto vbuckets = kvBucket->getVBuckets().getBuckets();
    for (auto vbid : vbuckets) {
        VBucketPtr vb = getVBucket(vbid);
        if (vb) {
            addSeqnoVbStats(cookie, add_stat, vb);
        }
    }
    return cb::engine_errc::success;
}

void EventuallyPersistentEngine::addLookupAllKeys(CookieIface& cookie,
                                                  cb::engine_errc err) {
    std::lock_guard<std::mutex> lh(lookupMutex);
    allKeysLookups[&cookie] = err;
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
        const std::string& statKey) {
    CBStatCollector collector(add_stat, cookie);
    auto bucketCollector = collector.forBucket(getName());
    auto res = Collections::Manager::doCollectionStats(
            *kvBucket, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_collection ||
        res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie, res.getManifestId());
    }
    return cb::engine_errc(res.result);
}

cb::engine_errc EventuallyPersistentEngine::doScopeStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        const std::string& statKey) {
    CBStatCollector collector(add_stat, cookie);
    auto bucketCollector = collector.forBucket(getName());
    auto res = Collections::Manager::doScopeStats(
            *kvBucket, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie, res.getManifestId());
    }
    return cb::engine_errc(res.result);
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
            EP_LOG_WARN(
                    "EventuallyPersistentEngine::parseKeyStatCollection could "
                    "not find collection arg:{} error:{}",
                    collectionStr,
                    res.result);
        }
        return res;
    } else if (statKeyArg == (std::string(expectedStatPrefix) + "-byid") &&
               collectionStr.size() > 2) {
        // provided argument should be a hex collection ID N, 0xN or 0XN
        try {
            cid = std::stoul(collectionStr.data(), nullptr, 16);
        } catch (const std::logic_error& e) {
            EP_LOG_WARN(
                    "EventuallyPersistentEngine::parseKeyStatCollection "
                    "invalid collection arg:{}, exception:{}",
                    collectionStr,
                    e.what());
            return cb::EngineErrorGetCollectionIDResult{
                    cb::engine_errc::invalid_arguments};
        }
        // Collection's scope is needed for privilege check
        auto [manifesUid, scope] =
                kvBucket->getCollectionsManager().getScopeID(cid);
        if (scope) {
            return {manifesUid, scope.value(), cid};
        } else {
            return {cb::engine_errc::unknown_collection, manifesUid};
        }
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
        EP_LOG_WARN(
                "EventuallyPersistentEngine::doKeyStats invalid "
                "vbucket arg:{}, exception:{}",
                args[2],
                e.what());
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
            return {cb::engine_errc(cidResult.result),
                    std::nullopt,
                    std::nullopt,
                    std::nullopt};
        }
        cid = cidResult.getCollectionId();
    }

    return {cb::engine_errc::success, vbid, args[1], cid};
}

cb::engine_errc EventuallyPersistentEngine::doKeyStats(
        CookieIface& cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    cb::engine_errc status;
    std::optional<Vbid> vbid;
    std::optional<std::string> key;
    std::optional<CollectionID> cid;
    std::tie(status, vbid, key, cid) = parseStatKeyArg(cookie, "key", statKey);
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
    cb::engine_errc status;
    std::optional<Vbid> vbid;
    std::optional<std::string> key;
    std::optional<CollectionID> cid;
    std::tie(status, vbid, key, cid) = parseStatKeyArg(cookie, "vkey", statKey);
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
    parseUint16(vbid.c_str(), &vbucket_id);
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
        parseUint16(vbid.c_str(), &vbucket_id);
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

cb::engine_errc EventuallyPersistentEngine::doPrivilegedStats(
        CookieIface& cookie, const AddStatFn& add_stat, std::string_view key) {
    // Privileged stats - need Stats priv (and not just SimpleStats).
    const auto acc = getServerApi()->cookie->check_privilege(
            cookie, cb::rbac::Privilege::Stats, {}, {});

    if (acc.success()) {
        if (cb_isPrefix(key, "_checkpoint-dump")) {
            const size_t keyLen = strlen("_checkpoint-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doCheckpointDump(cookie, add_stat, keyArgs);
        }

        if (cb_isPrefix(key, "_hash-dump")) {
            const size_t keyLen = strlen("_hash-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doHashDump(cookie, add_stat, keyArgs);
        }

        if (cb_isPrefix(key, "_durability-dump")) {
            const size_t keyLen = strlen("_durability-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doDurabilityMonitorDump(cookie, add_stat, keyArgs);
        }

        if (cb_isPrefix(key, "_vbucket-dump")) {
            const size_t keyLen = strlen("_vbucket-dump");
            std::string_view keyArgs(key.data() + keyLen, key.size() - keyLen);
            return doVBucketDump(cookie, add_stat, keyArgs);
        }

        return cb::engine_errc::no_such_key;
    }
    return cb::engine_errc::no_access;
}

cb::engine_errc EventuallyPersistentEngine::getStats(
        CookieIface& c,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
            std::forward_as_tuple(stats.getStatsCmdHisto),
            std::forward_as_tuple(&c, cb::tracing::Code::GetStats));

    EP_LOG_DEBUG("stats '{}'", key);

    // Some stats have been moved to using the stat collector interface,
    // while others have not. Depending on the key, this collector _may_
    // not be used, but creating it here reduces duplication (and it's not
    // expensive to create)
    CBStatCollector collector{add_stat, c};
    auto bucketCollector = collector.forBucket(getName());

    if (key.empty()) {
        return doEngineStats(bucketCollector);
    }
    if (key.size() > 7 && cb_isPrefix(key, "dcpagg ")) {
        return doConnAggStats(bucketCollector, key.substr(7));
    }
    if (key == "dcp"sv) {
        return doDcpStats(c, add_stat, value);
    }
    if (key == "eviction"sv) {
        return doEvictionStats(c, add_stat);
    }
    if (key == "hash"sv) {
        return doHashStats(c, add_stat);
    }
    if (key == "vbucket"sv) {
        return doVBucketStats(c,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::State);
    }
    if (key == "prev-vbucket"sv) {
        return doVBucketStats(c,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::PreviousState);
    }
    if (cb_isPrefix(key, "vbucket-durability-state")) {
        return doVBucketStats(c,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::Durability);
    }
    if (cb_isPrefix(key, "vbucket-details")) {
        return doVBucketStats(c,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::Full);
    }
    if (cb_isPrefix(key, "vbucket-seqno")) {
        return doSeqnoStats(c, add_stat, key.data(), key.size());
    }
    if (cb_isPrefix(key, "checkpoint")) {
        return doCheckpointStats(c, add_stat, key.data(), key.size());
    }
    if (cb_isPrefix(key, "durability-monitor")) {
        return doDurabilityMonitorStats(c, add_stat, key.data(), key.size());
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
        return doRunTimeStats(c, add_stat);
    }
    if (key == "memory"sv) {
        return doMemoryStats(c, add_stat);
    }
    if (key == "uuid"sv) {
        add_casted_stat("uuid", configuration.getUuid(), add_stat, c);
        return cb::engine_errc::success;
    }
    if (cb_isPrefix(key, "key ") || cb_isPrefix(key, "key-byid ")) {
        return doKeyStats(c, add_stat, key);
    }
    if (cb_isPrefix(key, "vkey ") || cb_isPrefix(key, "vkey-byid ")) {
        return doVKeyStats(c, add_stat, key);
    }
    if (key == "kvtimings"sv) {
        getKVBucket()->addKVStoreTimingStats(add_stat, c);
        return cb::engine_errc::success;
    }
    if (key.size() >= 7 && cb_isPrefix(key, "kvstore")) {
        std::string args(key.data() + 7, key.size() - 7);
        getKVBucket()->addKVStoreStats(add_stat, c);
        return cb::engine_errc::success;
    }
    if (key == "warmup"sv) {
        const auto* warmup = getKVBucket()->getWarmup();
        if (warmup != nullptr) {
            warmup->addStats(CBStatCollector(add_stat, c));
            return cb::engine_errc::success;
        }
        return cb::engine_errc::no_such_key;
    }
    if (key == "info"sv) {
        add_casted_stat("info", get_stats_info(), add_stat, c);
        return cb::engine_errc::success;
    }
    if (key == "config"sv) {
        configuration.addStats(bucketCollector);
        return cb::engine_errc::success;
    }
    if (key.size() > 15 && cb_isPrefix(key, "dcp-vbtakeover")) {
        return doDcpVbTakeoverStats(c, add_stat, key);
    }
    if (key == "workload"sv) {
        return doWorkloadStats(c, add_stat);
    }
    if (cb_isPrefix(key, "failovers")) {
        return doFailoversStats(c, add_stat, key);
    }
    if (cb_isPrefix(key, "diskinfo")) {
        return doDiskinfoStats(c, add_stat, key);
    }
    if (cb_isPrefix(key, "collections")) {
        return doCollectionStats(
                c, add_stat, std::string(key.data(), key.size()));
    }
    if (cb_isPrefix(key, "scopes")) {
        return doScopeStats(c, add_stat, std::string(key.data(), key.size()));
    }
    if (cb_isPrefix(key, "disk-failures")) {
        doDiskFailureStats(bucketCollector);
        return cb::engine_errc::success;
    }
    if (key[0] == '_') {
        return doPrivilegedStats(c, add_stat, key);
    }
    if (cb_isPrefix(key, "range-scans")) {
        return doRangeScanStats(bucketCollector, key);
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

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        CookieIface& cookie, cb::rbac::Privilege priv, DocKey key) const {
    return checkPrivilege(cookie, priv, key.getCollectionID());
}

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        CookieIface& cookie, cb::rbac::Privilege priv, CollectionID cid) const {
    ScopeID sid{ScopeID::Default};
    uint64_t manifestUid{0};
    cb::engine_errc status = cb::engine_errc::success;

    if (!cid.isDefaultCollection()) {
        auto res = getKVBucket()->getScopeID(cid);
        manifestUid = res.first;
        if (!res.second) {
            status = cb::engine_errc::unknown_collection;
        } else {
            sid = res.second.value();
        }
    }

    if (status == cb::engine_errc::success) {
        status = checkPrivilege(cookie, priv, sid, cid);
    }

    switch (status) {
    case cb::engine_errc::success:
    case cb::engine_errc::no_access:
        break;
    case cb::engine_errc::unknown_collection:
        setUnknownCollectionErrorContext(cookie, manifestUid);
        break;
    default:
        EP_LOG_ERR(
                "EPE::checkPrivilege(priv:{}, cid:{}): sid:{} unexpected "
                "status:{}",
                int(priv),
                cid.to_string(),
                sid.to_string(),

                to_string(status));
    }
    return status;
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
        EP_LOG_ERR(
                "EPE::checkForPrivilegeAtLeastInOneCollection: received "
                "exception while checking privilege: {}",
                e.what());
    }

    return cb::engine_errc::failed;
}

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        CookieIface& cookie,
        cb::rbac::Privilege priv,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    try {
        // Upon failure check_privilege may set an error message in the
        // cookie about the missing privilege
        NonBucketAllocationGuard guard;
        switch (serverApi->cookie->check_privilege(cookie, priv, sid, cid)
                        .getStatus()) {
        case cb::rbac::PrivilegeAccess::Status::Ok:
            return cb::engine_errc::success;
        case cb::rbac::PrivilegeAccess::Status::Fail:
            return cb::engine_errc::no_access;
        case cb::rbac::PrivilegeAccess::Status::FailNoPrivileges:
            return cid ? cb::engine_errc::unknown_collection
                       : cb::engine_errc::unknown_scope;
        }
    } catch (const std::exception& e) {
        EP_LOG_ERR(
                "EPE::checkPrivilege: received exception while checking "
                "privilege for sid:{}: cid:{} {}",
                sid ? sid->to_string() : "no-scope",
                cid ? cid->to_string() : "no-collection",
                e.what());
    }
    return cb::engine_errc::failed;
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
        EP_LOG_ERR(
                "EPE::testPrivilege: received exception while checking "
                "privilege for sid:{}: cid:{} {}",
                sid ? sid->to_string() : "no-scope",
                cid ? cid->to_string() : "no-collection",
                e.what());
    }
    return cb::engine_errc::failed;
}

/**
 * @return the privilege revision, which changes when privileges do.
 */
uint32_t EventuallyPersistentEngine::getPrivilegeRevision(
        CookieIface& cookie) const {
    return serverApi->cookie->get_privilege_context_revision(cookie);
}

cb::engine_errc EventuallyPersistentEngine::observe(
        CookieIface& cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    size_t offset = 0;

    const auto value = req.getValue();
    const uint8_t* data = value.data();
    std::stringstream result;

    while (offset < value.size()) {
        // Each entry is built up by:
        // 2 bytes vb id
        // 2 bytes key length
        // n bytes key

        // Parse a key
        if (value.size() - offset < 4) {
            setErrorContext(cookie, "Requires vbid and keylen.");
            return cb::engine_errc::invalid_arguments;
        }

        Vbid vb_id;
        memcpy(&vb_id, data + offset, sizeof(Vbid));
        vb_id = vb_id.ntoh();
        offset += sizeof(Vbid);

        uint16_t keylen;
        memcpy(&keylen, data + offset, sizeof(uint16_t));
        keylen = ntohs(keylen);
        offset += sizeof(uint16_t);

        if (value.size() - offset < keylen) {
            setErrorContext(cookie, "Incorrect keylen");
            return cb::engine_errc::invalid_arguments;
        }

        DocKey key = makeDocKey(cookie, {data + offset, keylen});
        offset += keylen;
        EP_LOG_DEBUG("Observing key {} in {}",
                     cb::UserDataView(key.to_string()),
                     vb_id);

        auto rv = checkPrivilege(cookie, cb::rbac::Privilege::Read, key);
        if (rv != cb::engine_errc::success) {
            return rv;
        }

        // Get key stats
        uint8_t keystatus = 0;
        struct key_stats kstats = {};
        rv = kvBucket->getKeyStats(
                key, vb_id, cookie, kstats, WantsDeleted::Yes);
        if (rv == cb::engine_errc::success) {
            if (kstats.logically_deleted) {
                keystatus = OBS_STATE_LOGICAL_DEL;
            } else if (!kstats.dirty) {
                keystatus = OBS_STATE_PERSISTED;
            } else {
                keystatus = OBS_STATE_NOT_PERSISTED;
            }
        } else if (rv == cb::engine_errc::no_such_key) {
            keystatus = OBS_STATE_NOT_FOUND;
        } else if (rv == cb::engine_errc::not_my_vbucket) {
            return cb::engine_errc::not_my_vbucket;
        } else if (rv == cb::engine_errc::would_block) {
            return rv;
        } else if (rv == cb::engine_errc::sync_write_re_commit_in_progress) {
            return rv;
        } else {
            return cb::engine_errc::failed;
        }

        // Put the result into a response buffer
        vb_id = vb_id.hton();
        keylen = htons(keylen);
        uint64_t cas = htonll(kstats.cas);
        result.write((char*)&vb_id, sizeof(Vbid));
        result.write((char*) &keylen, sizeof(uint16_t));
        result.write(reinterpret_cast<const char*>(key.data()), key.size());
        result.write((char*) &keystatus, sizeof(uint8_t));
        result.write((char*) &cas, sizeof(uint64_t));
    }

    uint64_t persist_time = 0;
    auto queue_size = static_cast<double>(stats.diskQueueSize);
    double item_trans_time = kvBucket->getTransactionTimePerItem();

    if (item_trans_time > 0 && queue_size > 0) {
        persist_time = static_cast<uint32_t>(queue_size * item_trans_time);
    }
    persist_time = persist_time << 32;

    const auto result_string = result.str();
    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        result_string, // body
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        persist_time,
                        cookie);
}

cb::engine_errc EventuallyPersistentEngine::observe_seqno(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    Vbid vb_id = request.getVBucket();
    auto value = request.getValue();
    auto vb_uuid = static_cast<uint64_t>(
            ntohll(*reinterpret_cast<const uint64_t*>(value.data())));

    EP_LOG_DEBUG("Observing {} with uuid: {}", vb_id, vb_uuid);

    VBucketPtr vb = kvBucket->getVBucket(vb_id);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
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
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

VBucketPtr EventuallyPersistentEngine::getVBucket(Vbid vbucket) const {
    return kvBucket->getVBucket(vbucket);
}

cb::engine_errc EventuallyPersistentEngine::handleSeqnoPersistence(
        CookieIface& cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    const Vbid vbucket = req.getVBucket();
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    auto status = cb::mcbp::Status::Success;
    auto extras = req.getExtdata();
    uint64_t seqno = ntohll(*reinterpret_cast<const uint64_t*>(extras.data()));

    if (!getEngineSpecific<ScheduledSeqnoPersistenceToken>(cookie)) {
        auto persisted_seqno = vb->getPersistenceSeqno();
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
                status = cb::mcbp::Status::NotSupported;
                EP_LOG_WARN(
                        "EventuallyPersistentEngine::handleSeqnoCmds(): "
                        "High priority async seqno request "
                        "for {} is NOT supported",
                        vbucket);
                break;

            case HighPriorityVBReqStatus::RequestNotScheduled:
                /* 'HighPriorityVBEntry' was not added, hence just return
                   success */
                EP_LOG_INFO(
                        "EventuallyPersistentEngine::handleSeqnoCmds(): "
                        "Did NOT add high priority async seqno request "
                        "for {}, Persisted seqno {} > requested seqno "
                        "{}",
                        vbucket,
                        persisted_seqno,
                        seqno);
                break;
            }
        }
    } else {
        clearEngineSpecific(cookie);
        EP_LOG_DEBUG("Sequence number {} persisted for {}", seqno, vbucket);
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        PROTOCOL_BINARY_RAW_BYTES,
                        status,
                        0,
                        cookie);
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::getMetaInner(
        CookieIface& cookie, const DocKey& key, Vbid vbucket) {
    uint32_t deleted;
    uint8_t datatype;
    ItemMetaData itemMeta;
    cb::engine_errc ret = kvBucket->getMetaData(
            key, vbucket, &cookie, itemMeta, deleted, datatype);

    item_info metadata;

    if (ret == cb::engine_errc::success) {
        metadata = to_item_info(itemMeta, datatype, deleted);
    } else if (ret == cb::engine_errc::no_such_key ||
               ret == cb::engine_errc::not_my_vbucket) {
        if (isDegradedMode()) {
            ret = cb::engine_errc::temporary_failure;
        }
    }

    return std::make_pair(cb::engine_errc(ret), metadata);
}

bool EventuallyPersistentEngine::decodeSetWithMetaOptions(
        cb::const_byte_buffer extras,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts,
        PermittedVBStates& permittedVBStates) {
    // DeleteSource not needed by SetWithMeta, so set to default of explicit
    DeleteSource deleteSource = DeleteSource::Explicit;
    return EventuallyPersistentEngine::decodeWithMetaOptions(extras,
                                                             generateCas,
                                                             checkConflicts,
                                                             permittedVBStates,
                                                             deleteSource);
}
bool EventuallyPersistentEngine::decodeWithMetaOptions(
        cb::const_byte_buffer extras,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts,
        PermittedVBStates& permittedVBStates,
        DeleteSource& deleteSource) {
    bool forceFlag = false;
    if (extras.size() == 28 || extras.size() == 30) {
        const size_t fixed_extras_size = 24;
        uint32_t options;
        memcpy(&options, extras.data() + fixed_extras_size, sizeof(options));
        options = ntohl(options);

        if (options & SKIP_CONFLICT_RESOLUTION_FLAG) {
            checkConflicts = CheckConflicts::No;
        }

        if (options & FORCE_ACCEPT_WITH_META_OPS) {
            forceFlag = true;
        }

        if (options & REGENERATE_CAS) {
            generateCas = GenerateCas::Yes;
        }

        if (options & FORCE_WITH_META_OP) {
            permittedVBStates.set(vbucket_state_replica);
            permittedVBStates.set(vbucket_state_pending);
            checkConflicts = CheckConflicts::No;
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
 * This is a helper function for set/deleteWithMeta, used to extract nmeta
 * from the packet.
 * @param emd Extended Metadata (edited by this function)
 * @param value nmeta is removed from the value by this function
 * @param extras
 */
void extractNmetaFromExtras(cb::const_byte_buffer& emd,
                            cb::const_byte_buffer& value,
                            cb::const_byte_buffer extras) {
    if (extras.size() == 26 || extras.size() == 30) {
        // 26 = nmeta
        // 30 = options and nmeta (options followed by nmeta)
        // The extras is stored last, so copy out the two last bytes in
        // the extras field and use them as nmeta
        uint16_t nmeta;
        memcpy(&nmeta, extras.end() - sizeof(nmeta), sizeof(nmeta));
        nmeta = ntohs(nmeta);
        // Correct the vallen
        emd = {value.data() + value.size() - nmeta, nmeta};
        value = {value.data(), value.size() - nmeta};
    }
}

protocol_binary_datatype_t EventuallyPersistentEngine::checkForDatatypeJson(
        CookieIface& cookie,
        protocol_binary_datatype_t datatype,
        std::string_view body) {
    if (!cookie.isDatatypeSupported(PROTOCOL_BINARY_DATATYPE_JSON)) {
        // JSON check the body if xattr's are enabled
        if (cb::mcbp::datatype::is_xattr(datatype)) {
            body = cb::xattr::get_body(body);
        }

        NonBucketAllocationGuard guard;
        if (serverApi->cookie->is_valid_json(cookie, body)) {
            datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
        }
    }
    return datatype;
}

DocKey EventuallyPersistentEngine::makeDocKey(CookieIface& cookie,
                                              cb::const_byte_buffer key) const {
    return DocKey{key.data(),
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
    if (!decodeSetWithMetaOptions(
                extras, generateCas, checkConflicts, permittedVBStates)) {
        return cb::engine_errc::invalid_arguments;
    }

    auto value = request.getValue();
    cb::const_byte_buffer emd;
    extractNmetaFromExtras(emd, value, extras);

    std::chrono::steady_clock::time_point startTime;
    {
        auto startTimeC =
                takeEngineSpecific<std::chrono::steady_clock::time_point>(
                        cookie);
        if (startTimeC.has_value()) {
            startTime = *startTimeC;
        } else {
            startTime = std::chrono::steady_clock::now();
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
                          {cas, seqno, flags, time_t(expiration)},
                          false /*isDeleted*/,
                          uint8_t(request.getDatatype()),
                          commandCas,
                          &bySeqno,
                          cookie,
                          permittedVBStates,
                          checkConflicts,
                          allowExisting,
                          GenerateBySeqno::Yes,
                          generateCas,
                          emd);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    if (ret == cb::engine_errc::success) {
        cookie.addDocumentWriteBytes(value.size() + request.getKey().size());
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Modify);
        ++stats.numOpsSetMeta;
        auto endTime = std::chrono::steady_clock::now();
        if (cookie.isTracingEnabled()) {
            NonBucketAllocationGuard guard;
            auto& tracer = cookie.getTracer();
            auto spanid = tracer.begin(Code::SetWithMeta, startTime);
            tracer.end(spanid, endTime);
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime);
        stats.setWithMetaHisto.add(elapsed);

        cas = commandCas;
    } else if (ret == cb::engine_errc::no_memory) {
        return memoryCondition();
    } else if (ret == cb::engine_errc::would_block) {
        ++stats.numOpsGetMetaOnSetWithMeta;
        storeEngineSpecific(cookie, startTime);
        return ret;
    } else {
        // Let the framework generate the error message
        return ret;
    }

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
        DocKey key,
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
        GenerateCas genCas,
        cb::const_byte_buffer emd) {
    std::unique_ptr<ExtendedMetaData> extendedMetaData;
    if (!emd.empty()) {
        extendedMetaData =
                std::make_unique<ExtendedMetaData>(emd.data(), emd.size());
        if (extendedMetaData->getStatus() ==
            cb::engine_errc::invalid_arguments) {
            setErrorContext(cookie, "Invalid extended metadata");
            return cb::engine_errc::invalid_arguments;
        }
    }

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
            if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                          payload,
                                          uncompressedValue)) {
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
            EP_LOG_WARN(
                    "Item value size {} for setWithMeta is bigger than the max "
                    "size {} allowed!!!",
                    inflatedValue.size(),
                    maxItemSize);

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
                                     genCas,
                                     extendedMetaData.get());

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
    if (!decodeWithMetaOptions(extras,
                               generateCas,
                               checkConflicts,
                               permittedVBStates,
                               deleteSource)) {
        return cb::engine_errc::invalid_arguments;
    }

    auto value = request.getValue();
    cb::const_byte_buffer emd;
    extractNmetaFromExtras(emd, value, extras);

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
            if (!cb::compression::inflate(
                        cb::compression::Algorithm::Snappy,
                        {reinterpret_cast<const char*>(value.data()),
                         value.size()},
                        uncompressedValue)) {
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
                                 {metacas, seqno, flags, time_t(delete_time)},
                                 cas,
                                 &bySeqno,
                                 cookie,
                                 permittedVBStates,
                                 checkConflicts,
                                 GenerateBySeqno::Yes,
                                 generateCas,
                                 emd,
                                 deleteSource);
        } else {
            // A delete with a value
            ret = setWithMeta(request.getVBucket(),
                              key,
                              value,
                              {metacas, seqno, flags, time_t(delete_time)},
                              true /*isDeleted*/,
                              PROTOCOL_BINARY_DATATYPE_XATTR,
                              cas,
                              &bySeqno,
                              cookie,
                              permittedVBStates,
                              checkConflicts,
                              true /*allowExisting*/,
                              GenerateBySeqno::Yes,
                              generateCas,
                              emd);
        }
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }

    if (ret == cb::engine_errc::success) {
        cookie.addDocumentWriteBytes(value.size() + request.getKey().size());
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Delete);
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
        DocKey key,
        ItemMetaData itemMeta,
        uint64_t& cas,
        uint64_t* seqno,
        CookieIface& cookie,
        PermittedVBStates permittedVBStates,
        CheckConflicts checkConflicts,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        cb::const_byte_buffer emd,
        DeleteSource deleteSource) {
    std::unique_ptr<ExtendedMetaData> extendedMetaData;
    if (!emd.empty()) {
        extendedMetaData =
                std::make_unique<ExtendedMetaData>(emd.data(), emd.size());
        if (extendedMetaData->getStatus() ==
            cb::engine_errc::invalid_arguments) {
            setErrorContext(cookie, "Invalid extended metadata");
            return cb::engine_errc::invalid_arguments;
        }
    }

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
                                    extendedMetaData.get(),
                                    deleteSource);
}

cb::engine_errc EventuallyPersistentEngine::handleTrafficControlCmd(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::EnableTraffic:
        if (kvBucket->isWarmupLoadingData()) {
            // engine is still warming up, do not turn on data traffic yet
            setErrorContext(cookie, "Persistent engine is still warming up!");
            return cb::engine_errc::temporary_failure;
        } else if (configuration.isFailpartialwarmup() &&
                   kvBucket->isWarmupOOMFailure()) {
            // engine has completed warm up, but data traffic cannot be
            // turned on due to an OOM failure
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to out of memory failures during warmup");
            return cb::engine_errc::no_memory;
        } else if (kvBucket->hasWarmupSetVbucketStateFailed()) {
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to write failures when persisting vbucket state to "
                    "disk");
            return cb::engine_errc::failed;
        } else {
            if (enableTraffic(true)) {
                setErrorContext(
                        cookie,
                        "Data traffic to persistence engine is enabled");
            } else {
                setErrorContext(cookie,
                                "Data traffic to persistence engine was "
                                "already enabled");
            }
        }
        break;
    case cb::mcbp::ClientOpcode::DisableTraffic:
        if (enableTraffic(false)) {
            setErrorContext(cookie,
                            "Data traffic to persistence engine is disabled");
        } else {
            setErrorContext(
                    cookie,
                    "Data traffic to persistence engine was already disabled");
        }
        break;
    default:
        throw std::invalid_argument(
                "EPE::handleTrafficControlCmd can only be called with "
                "EnableTraffic or DisableTraffic");
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

bool EventuallyPersistentEngine::isDegradedMode() const {
    return kvBucket->isWarmupLoadingData() || !trafficEnabled.load();
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
        EP_LOG_DEBUG("doDcpVbTakeoverStats - cannot find connection {} for {}",
                     dcpName,
                     vbid);
        size_t vb_items = vb->getNumItems();

        size_t del_items = 0;
        try {
            del_items = vb->getNumPersistedDeletes();
        } catch (std::runtime_error& e) {
            EP_LOG_WARN(
                    "doDcpVbTakeoverStats: exception while getting num "
                    "persisted deletes for {} - treating as 0 "
                    "deletes. Details: {}",
                    vbid,
                    e.what());
        }
        size_t chk_items =
                vb_items > 0 ? vb->checkpointManager->getNumOpenChkItems() : 0;
        add_casted_stat("status", "does_not_exist", add_stat, cookie);
        add_casted_stat("on_disk_deletes", del_items, add_stat, cookie);
        add_casted_stat("vb_items", vb_items, add_stat, cookie);
        add_casted_stat("chk_items", chk_items, add_stat, cookie);
        add_casted_stat("estimate", vb_items + del_items, add_stat, cookie);
        return cb::engine_errc::success;
    }

    auto producer = std::dynamic_pointer_cast<DcpProducer>(conn);
    if (producer) {
        producer->addTakeoverStats(add_stat, cookie, *vb);
    } else {
        /**
         * There is not a legitimate case where a connection is not a
         * DcpProducer.  But just in case it does happen log the event and
         * return cb::engine_errc::no_such_key.
         */
        EP_LOG_WARN(
                "doDcpVbTakeoverStats: connection {} for "
                "{} is not a DcpProducer",
                dcpName,
                vbid);
        return cb::engine_errc::no_such_key;
    }

    return cb::engine_errc::success;
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
    auto exp = payload.getExpiration();
    if (exp != 0) {
        exp = ep_abs_time(ep_reltime(exp));
    }

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
            ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
            guardedIface.audit_document_access(
                    cookie, cb::audit::document::Operation::Modify);
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
            ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
            guardedIface.audit_document_access(
                    cookie, cb::audit::document::Operation::Delete);
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

    return sendResponse(response,
                        {}, // key
                        {meta.data(), meta.size()}, // extra
                        {}, // body
                        datatype,
                        cb::mcbp::Status::Success,
                        cas,
                        cookie);
}

cb::engine_errc EventuallyPersistentEngine::getAllKeys(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (!getKVBucket()->isGetAllKeysSupported()) {
        return cb::engine_errc::not_supported;
    }

    {
        std::lock_guard<std::mutex> lh(lookupMutex);
        auto it = allKeysLookups.find(&cookie);
        if (it != allKeysLookups.end()) {
            cb::engine_errc err = it->second;
            allKeysLookups.erase(it);
            return err;
        }
    }

    VBucketPtr vb = getVBucket(request.getVBucket());
    if (!vb) {
        return cb::engine_errc::not_my_vbucket;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return cb::engine_errc::not_my_vbucket;
    }

    // key: key, ext: no. of keys to fetch, sorting-order
    uint32_t count = 1000;
    auto extras = request.getExtdata();
    if (!extras.empty()) {
        count = ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
    }

    DocKey start_key = makeDocKey(cookie, request.getKey());
    auto privTestResult =
            checkPrivilege(cookie, cb::rbac::Privilege::Read, start_key);
    if (privTestResult != cb::engine_errc::success) {
        return privTestResult;
    }

    std::optional<CollectionID> keysCollection;
    if (cookie.isCollectionsSupported()) {
        keysCollection = start_key.getCollectionID();
    }

    ExTask task = std::make_shared<FetchAllKeysTask>(*this,
                                                     cookie,
                                                     response,
                                                     start_key,
                                                     request.getVBucket(),
                                                     count,
                                                     keysCollection);
    ExecutorPool::get()->schedule(task);
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

cb::engine_errc EventuallyPersistentEngine::getRandomKey(
        CookieIface& cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    CollectionID cid{CollectionID::Default};

    if (request.getExtlen()) {
        const auto& payload = request.getCommandSpecifics<
                cb::mcbp::request::GetRandomKeyPayload>();
        cid = payload.getCollectionId();
    }

    auto priv = checkPrivilege(cookie, cb::rbac::Privilege::Read, cid);
    if (priv != cb::engine_errc::success) {
        return cb::engine_errc(priv);
    }

    GetValue gv(kvBucket->getRandomKey(cid, cookie));
    cb::engine_errc ret = gv.getStatus();

    if (ret == cb::engine_errc::success) {
        auto* it = gv.item.get();
        cookie.addDocumentReadBytes(it->getNBytes());
        const auto flags = it->getFlags();
        ret = sendResponse(
                response,
                cookie.isCollectionsSupported()
                        ? DocKey{it->getKey()}
                        : it->getKey().makeDocKeyWithoutCollectionID(),
                std::string_view{reinterpret_cast<const char*>(&flags),
                                 sizeof(flags)},
                std::string_view{it->getData(), it->getNBytes()},
                it->getDataType(),
                cb::mcbp::Status::Success,
                it->getCas(),
                cookie);
    }

    return ret;
}

cb::engine_errc EventuallyPersistentEngine::dcpOpen(
        CookieIface& cookie,
        uint32_t opaque,
        uint32_t seqno,
        uint32_t flags,
        std::string_view stream_name,
        std::string_view value) {
    std::string connName{stream_name};
    ConnHandler* handler = nullptr;

    if (flags & cb::mcbp::request::DcpOpenPayload::Producer) {
        if (flags & cb::mcbp::request::DcpOpenPayload::PiTR) {
            auto* store = getKVBucket()->getOneROUnderlying();
            if (!store || !store->supportsHistoricalSnapshots()) {
                EP_LOG_WARN_RAW(
                        "Cannot open a DCP connection with PiTR as the "
                        "underlying kvstore don't support historical "
                        "snapshots");
                return cb::engine_errc::disconnect;
            }
        }
        handler = dcpConnMap_->newProducer(cookie, connName, flags);
    } else {
        // Don't accept dcp consumer open requests during warm up. This waits
        // for warmup to complete entirely (including background tasks) as it
        // was observed in MB-48373 that a rollback from a DCP connection could
        // delete a vBucket from under a warmup task.
        if (kvBucket->isWarmupComplete()) {
            // Check if consumer_name specified in value; if so use in Consumer
            // object.
            nlohmann::json jsonValue;
            std::string consumerName;
            if (!value.empty()) {
                jsonValue = nlohmann::json::parse(value);
                consumerName = jsonValue.at("consumer_name").get<std::string>();
            }
            handler = dcpConnMap_->newConsumer(cookie, connName, consumerName);

            EP_LOG_INFO(
                    "EventuallyPersistentEngine::dcpOpen: opening new DCP "
                    "Consumer handler - stream name:{}, opaque:{}, seqno:{}, "
                    "flags:0b{} value:{}",
                    connName,
                    opaque,
                    seqno,
                    std::bitset<sizeof(flags) * 8>(flags).to_string(),
                    jsonValue.dump());
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

cb::engine_errc EventuallyPersistentEngine::dcpAddStream(CookieIface& cookie,
                                                         uint32_t opaque,
                                                         Vbid vbucket,
                                                         uint32_t flags) {
    return dcpConnMap_->addPassiveStream(
            getConnHandler(cookie), opaque, vbucket, flags);
}

ConnHandler* EventuallyPersistentEngine::tryGetConnHandler(
        CookieIface& cookie) {
    auto* iface = serverApi->cookie->getDcpConnHandler(cookie);
    if (iface) {
        auto* handler = dynamic_cast<ConnHandler*>(iface);
        if (handler) {
            return handler;
        }
        throw std::logic_error(
                "EventuallyPersistentEngine::tryGetConnHandler(): The "
                "registered connection handler is not a ConnHandler");
    }

    return nullptr;
}
ConnHandler& EventuallyPersistentEngine::getConnHandler(CookieIface& cookie) {
    auto* handle = tryGetConnHandler(cookie);
    Expects(handle);
    return *handle;
}

void EventuallyPersistentEngine::handleDisconnect(CookieIface& cookie) {
    dcpConnMap_->disconnect(&cookie);
}

void EventuallyPersistentEngine::initiate_shutdown() {
    auto eng = acquireEngine(this);
    EP_LOG_INFO_RAW(
            "Shutting down all DCP connections in "
            "preparation for bucket deletion.");
    dcpConnMap_->shutdownAllConnections();
}

void EventuallyPersistentEngine::cancel_all_operations_in_ewb_state() {
    auto eng = acquireEngine(this);
    kvBucket->releaseBlockedCookies();
}

cb::mcbp::Status EventuallyPersistentEngine::stopFlusher(const char** msg,
                                                         size_t* msg_size) {
    (void)msg_size;
    auto rv = cb::mcbp::Status::Success;
    *msg = nullptr;
    if (!kvBucket->pauseFlusher()) {
        EP_LOG_DEBUG_RAW("Unable to stop flusher");
        *msg = "Flusher not running.";
        rv = cb::mcbp::Status::Einval;
    }
    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::startFlusher(const char** msg,
                                                          size_t* msg_size) {
    (void)msg_size;
    auto rv = cb::mcbp::Status::Success;
    *msg = nullptr;
    if (!kvBucket->resumeFlusher()) {
        EP_LOG_DEBUG_RAW("Unable to start flusher");
        *msg = "Flusher not shut down.";
        rv = cb::mcbp::Status::Einval;
    }
    return rv;
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
            EP_LOG_DEBUG("Completed sync deletion of {}", vbid);
            return cb::engine_errc::success;
        }
        status = cb::engine_errc(kvBucket->deleteVBucket(vbid, &cookie));
    } else {
        status = cb::engine_errc(kvBucket->deleteVBucket(vbid));
    }

    switch (status) {
    case cb::engine_errc::success:
        EP_LOG_INFO("Deletion of {} was completed.", vbid);
        break;

    case cb::engine_errc::not_my_vbucket:

        EP_LOG_WARN(
                "Deletion of {} failed because the vbucket doesn't exist!!!",
                vbid);
        break;
    case cb::engine_errc::invalid_arguments:
        EP_LOG_WARN(
                "Deletion of {} failed because the vbucket is not in a dead "
                "state",
                vbid);
        setErrorContext(
                cookie,
                "Failed to delete vbucket.  Must be in the dead state.");
        break;
    case cb::engine_errc::would_block:
        EP_LOG_INFO(
                "Request for {} deletion is in EWOULDBLOCK until the database "
                "file is removed from disk",
                vbid);
        // We don't use the actual value in ewouldblock, just the existence
        // of something there.
        storeEngineSpecific(cookie, ScheduledVBucketDeleteToken{});
        break;
    default:
        EP_LOG_WARN("Deletion of {} failed because of unknown reasons", vbid);
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

    auto* connhandler = tryGetConnHandler(cookie);
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
                                        ? handle.getHighSeqno(*reqCollection)
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
                        PROTOCOL_BINARY_RAW_BYTES,
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
        return checkPrivilege(
                cookie, cb::rbac::Privilege::Read, collection.value());
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
                        PROTOCOL_BINARY_RAW_BYTES,
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
                        PROTOCOL_BINARY_RAW_BYTES,
                        status,
                        cas,
                        cookie);
}

std::unique_ptr<KVBucket> EventuallyPersistentEngine::makeBucket(
        Configuration& config) {
    const auto bucketType = config.getBucketType();
    if (bucketType == "persistent") {
        return std::make_unique<EPBucket>(*this);
    } else if (bucketType == "ephemeral") {
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

    return cb::engine_errc(status);
}

EventuallyPersistentEngine::~EventuallyPersistentEngine() {
    workload.reset();
    checkpointConfig.reset();

    // Engine going away, tell ArenaMalloc to unregister
    cb::ArenaMalloc::unregisterClient(arena);
    // Ensure the soon to be invalid engine is no longer in the ObjectRegistry
    ObjectRegistry::onSwitchThread(nullptr);

    /* Unique_ptr(s) are deleted in the reverse order of the initialization */
}

ReplicationThrottle& EventuallyPersistentEngine::getReplicationThrottle() {
    return getKVBucket()->getReplicationThrottle();
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
                                std::chrono::steady_clock::duration enqTime) {
    myEngine->getKVBucket()->logQTime(task, threadName, enqTime);
}

void EpEngineTaskable::logRunTime(const GlobalTask& task,
                                  std::string_view threadName,
                                  std::chrono::steady_clock::duration runTime) {
    myEngine->getKVBucket()->logRunTime(task, threadName, runTime);
}

bool EpEngineTaskable::isShutdown() const {
    return myEngine->getEpStats().isShutdown;
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
            EP_LOG_INFO(R"(Transitioning from "{}"->"{}" compression mode)",
                        to_string(oldCompressionMode),
                        compressModeStr);
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
            size, configuration.getRangeScanKvStoreScanRatio());

    configureStorageMemoryForQuota(size);

    updateArenaAllocThresholdForQuota(size);
}

void EventuallyPersistentEngine::configureMemWatermarksForQuota(size_t quota) {
    configuration.setMemLowWat(percentOf(quota, stats.mem_low_wat_percent));
    configuration.setMemHighWat(percentOf(quota, stats.mem_high_wat_percent));
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
    arena.setEstimateUpdateThreshold(
            size, configuration.getMemUsedMergeThresholdPercent());
    cb::ArenaMalloc::setAllocatedThreshold(arena);
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

cb::engine_errc EventuallyPersistentEngine::compactDatabase(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes) {
    return acquireEngine(this)->compactDatabaseInner(
            cookie, vbid, purge_before_ts, purge_before_seq, drop_deletes);
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
        CookieIface& cookie,
        Vbid vbid,
        CollectionID cid,
        cb::rangescan::KeyView start,
        cb::rangescan::KeyView end,
        cb::rangescan::KeyOnly keyOnly,
        std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
        std::optional<cb::rangescan::SamplingConfiguration> samplingConfig) {
    return acquireEngine(this)->getKVBucket()->createRangeScan(
            vbid,
            cid,
            start,
            end,
            nullptr, // No RangeScanDataHandler to 'inject'
            cookie,
            keyOnly,
            snapshotReqs,
            samplingConfig);
}

cb::engine_errc EventuallyPersistentEngine::continueRangeScan(
        CookieIface& cookie,
        Vbid vbid,
        cb::rangescan::Id uuid,
        size_t itemLimit,
        std::chrono::milliseconds timeLimit,
        size_t byteLimit) {
    return acquireEngine(this)->getKVBucket()->continueRangeScan(
            vbid, uuid, cookie, itemLimit, timeLimit, byteLimit);
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

cb::engine_errc EventuallyPersistentEngine::pause(
        folly::CancellationToken cancellationToken) {
    return kvBucket->prepareForPause(cancellationToken);
}

cb::engine_errc EventuallyPersistentEngine::resume() {
    return kvBucket->prepareForResume();
}

void EventuallyPersistentEngine::setDcpConsumerBufferRatio(float ratio) {
    if (dcpFlowControlManager) {
        dcpFlowControlManager->setDcpConsumerBufferRatio(ratio);
    }
}
