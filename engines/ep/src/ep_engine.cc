/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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
#include "connmap.h"
#include "dcp/consumer.h"
#include "dcp/dcpconnmap_impl.h"
#include "dcp/flow-control-manager.h"
#include "dcp/msg_producers_border_guard.h"
#include "dcp/producer.h"
#include "environment.h"
#include "ep_bucket.h"
#include "ep_engine_public.h"
#include "ep_vb.h"
#include "ephemeral_bucket.h"
#include "executorpool.h"
#include "ext_meta_parser.h"
#include "failover-table.h"
#include "flusher.h"
#include "getkeys.h"
#include "hash_table_stat_visitor.h"
#include "htresizer.h"
#include "kvstore.h"
#include "replicationthrottle.h"
#include "server_document_iface_border_guard.h"
#include "stats-info.h"
#include "string_utils.h"
#include "trace_helpers.h"
#include "vb_count_visitor.h"
#include "warmup.h"

#include <JSON_checker.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <logger/logger.h>
#include <memcached/audit_interface.h>
#include <memcached/engine.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/util.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cb_arena_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/compress.h>
#include <platform/platform_time.h>
#include <platform/scope_timer.h>
#include <platform/string_hex.h>
#include <statistics/cbstat_collector.h>
#include <statistics/collector.h>
#include <statistics/labelled_collector.h>
#include <statistics/prometheus.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <utilities/hdrhistogram.h>
#include <utilities/logtags.h>
#include <xattr/utils.h>

#include <chrono>
#include <cstring>
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
static ENGINE_ERROR_CODE sendResponse(const AddResponseFn& response,
                                      std::string_view key,
                                      std::string_view ext,
                                      std::string_view body,
                                      uint8_t datatype,
                                      cb::mcbp::Status status,
                                      uint64_t cas,
                                      const void* cookie) {
    if (response(key, ext, body, datatype, status, cas, cookie)) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_FAILED;
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static ENGINE_ERROR_CODE sendResponse(const AddResponseFn& response,
                                      const DocKey& key,
                                      std::string_view ext,
                                      std::string_view body,
                                      uint8_t datatype,
                                      cb::mcbp::Status status,
                                      uint64_t cas,
                                      const void* cookie) {
    if (response(std::string_view(key),
                 ext,
                 body,
                 datatype,
                 status,
                 cas,
                 cookie)) {
        return ENGINE_SUCCESS;
    }

    return ENGINE_FAILED;
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

std::pair<cb::unique_item_ptr, item_info>
EventuallyPersistentEngine::allocateItem(gsl::not_null<const void*> cookie,
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
                               "EvpItemAllocateEx: failed to allocate memory");
    }

    item_info info;
    if (!get_item_info(item.get(), &info)) {
        throw cb::engine_error(cb::engine_errc::failed,
                               "EvpItemAllocateEx: EvpGetItemInfo failed");
    }

    return std::make_pair(std::move(item), info);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::remove(
        gsl::not_null<const void*> cookie,
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

    return acquireEngine(this)->itemDelete(
            cookie, key, cas, vbucket, durReqs, mut_info);
}

void EventuallyPersistentEngine::release(gsl::not_null<ItemIface*> itm) {
    acquireEngine(this)->itemRelease(itm);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    auto options = static_cast<get_options_t>(QUEUE_BG_FETCH |
                                                       HONOR_STATES |
                                                       TRACK_REFERENCE |
                                                       DELETE_TEMP |
                                                       HIDE_LOCKED_CAS |
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

cb::EngineErrorItemPair EventuallyPersistentEngine::get_if(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    return acquireEngine(this)->getIfInner(cookie, key, vbucket, filter);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_and_touch(
        gsl::not_null<const void*> cookie,
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
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    return acquireEngine(this)->getLockedInner(
            cookie, key, vbucket, lock_timeout);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::unlock(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::get_stats(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::get_prometheus_stats(
        const BucketStatCollector& collector,
        cb::prometheus::Cardinality cardinality) {
    try {
        if (cardinality == cb::prometheus::Cardinality::High) {
            doTimingStats(collector);
            if (cb::engine_errc status =
                        Collections::Manager::doPrometheusCollectionStats(
                                *getKVBucket(), collector);
                status != cb::engine_errc::success) {
                return ENGINE_ERROR_CODE(status);
            }

        } else {
            ENGINE_ERROR_CODE status;
            if (status = doEngineStats(collector); status != ENGINE_SUCCESS) {
                return status;
            }
            // do dcp aggregated stats, using ":" as the separator to split
            // connection names to find the connection type.
            if (status = doConnAggStats(collector, ":");
                status != ENGINE_SUCCESS) {
                return status;
            }
        }
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::store(
        gsl::not_null<const void*> cookie,
        gsl::not_null<ItemIface*> itm,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    Item& item = *static_cast<Item*>(itm.get());
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
        gsl::not_null<const void*> cookie,
        gsl::not_null<ItemIface*> itm,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    Item& item = static_cast<Item&>(*static_cast<Item*>(itm.get()));

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

void EventuallyPersistentEngine::reset_stats(
        gsl::not_null<const void*> cookie) {
    acquireEngine(this)->resetStats();
}

cb::mcbp::Status EventuallyPersistentEngine::setReplicationParam(
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::mcbp::Status::Success;

    try {
        if (key == "replication_throttle_threshold") {
            getConfiguration().setReplicationThrottleThreshold(
                    std::stoull(val));
        } else if (key == "replication_throttle_queue_cap") {
            getConfiguration().setReplicationThrottleQueueCap(std::stoll(val));
        } else if (key == "replication_throttle_cap_pcnt") {
            getConfiguration().setReplicationThrottleCapPcnt(std::stoull(val));
        } else {
            msg = "Unknown config param";
            rv = cb::mcbp::Status::KeyEnoent;
        }
        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = cb::mcbp::Status::Einval;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = cb::mcbp::Status::Einval;

        // Handles any miscellaenous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = cb::mcbp::Status::Einval;
    }

    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::setCheckpointParam(
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::mcbp::Status::Success;

    try {
        if (key == "chk_max_items") {
            size_t v = std::stoull(val);
            validate(v, size_t(MIN_CHECKPOINT_ITEMS),
                     size_t(MAX_CHECKPOINT_ITEMS));
            getConfiguration().setChkMaxItems(v);
        } else if (key == "chk_period") {
            size_t v = std::stoull(val);
            validate(v, size_t(MIN_CHECKPOINT_PERIOD),
                     size_t(MAX_CHECKPOINT_PERIOD));
            getConfiguration().setChkPeriod(v);
        } else if (key == "max_checkpoints") {
            size_t v = std::stoull(val);
            validate(v, size_t(DEFAULT_MAX_CHECKPOINTS),
                     size_t(MAX_CHECKPOINTS_UPPER_BOUND));
            getConfiguration().setMaxCheckpoints(v);
        } else if (key == "item_num_based_new_chk") {
            getConfiguration().setItemNumBasedNewChk(cb_stob(val));
        } else if (key == "keep_closed_chks") {
            getConfiguration().setKeepClosedChks(cb_stob(val));
        } else if (key == "cursor_dropping_checkpoint_mem_upper_mark") {
            getConfiguration().setCursorDroppingCheckpointMemUpperMark(
                    std::stoull(val));
        } else if (key == "cursor_dropping_checkpoint_mem_lower_mark") {
            getConfiguration().setCursorDroppingCheckpointMemLowerMark(
                    std::stoull(val));
        } else if (key == "cursor_dropping_lower_mark") {
            getConfiguration().setCursorDroppingLowerMark(std::stoull(val));
        } else if (key == "cursor_dropping_upper_mark") {
            getConfiguration().setCursorDroppingUpperMark(std::stoull(val));
        } else {
            msg = "Unknown config param";
            rv = cb::mcbp::Status::KeyEnoent;
        }

        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        rv = cb::mcbp::Status::Einval;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = cb::mcbp::Status::Einval;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = cb::mcbp::Status::Einval;

        // Handles any miscellaenous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = cb::mcbp::Status::Einval;
    }

    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::setFlushParam(
        const std::string& key, const std::string& val, std::string& msg) {
    auto rv = cb::mcbp::Status::Success;

    // Handle the actual mutation.
    try {
        configuration.requirementsMetOrThrow(key);

        if (key == "max_size" || key == "cache_size") {
            size_t vsize = std::stoull(val);
            getConfiguration().setMaxSize(vsize);
        } else if (key == "mem_low_wat") {
            getConfiguration().setMemLowWat(std::stoull(val));
        } else if (key == "mem_high_wat") {
            getConfiguration().setMemHighWat(std::stoull(val));
        } else if (key == "backfill_mem_threshold") {
            getConfiguration().setBackfillMemThreshold(std::stoull(val));
        } else if (key == "compaction_exp_mem_threshold") {
            getConfiguration().setCompactionExpMemThreshold(std::stoull(val));
        } else if (key == "durability_timeout_task_interval") {
            getConfiguration().setDurabilityTimeoutTaskInterval(
                    std::stoull(val));
        } else if (key == "durability_min_level") {
            getConfiguration().setDurabilityMinLevel(val);
        } else if (key == "mutation_mem_threshold") {
            getConfiguration().setMutationMemThreshold(std::stoull(val));
        } else if (key == "timing_log") {
            EPStats& epStats = getEpStats();
            std::ostream* old = epStats.timingLog;
            epStats.timingLog = nullptr;
            delete old;
            if (val == "off") {
                EP_LOG_DEBUG("Disabled timing log.");
            } else {
                auto* tmp(new std::ofstream(val));
                if (tmp->good()) {
                    EP_LOG_DEBUG("Logging detailed timings to ``{}''.", val);
                    epStats.timingLog = tmp;
                } else {
                    EP_LOG_WARN(
                            "Error setting detailed timing log to ``{}'':  {}",
                            val,
                            strerror(errno));
                    delete tmp;
                }
            }
        } else if (key == "exp_pager_enabled") {
            getConfiguration().setExpPagerEnabled(cb_stob(val));
        } else if (key == "exp_pager_stime") {
            getConfiguration().setExpPagerStime(std::stoull(val));
        } else if (key == "exp_pager_initial_run_time") {
            getConfiguration().setExpPagerInitialRunTime(std::stoll(val));
        } else if (key == "flusher_total_batch_limit") {
            getConfiguration().setFlusherTotalBatchLimit(std::stoll(val));
        } else if (key == "getl_default_timeout") {
            getConfiguration().setGetlDefaultTimeout(std::stoull(val));
        } else if (key == "getl_max_timeout") {
            getConfiguration().setGetlMaxTimeout(std::stoull(val));
        } else if (key == "ht_resize_interval") {
            getConfiguration().setHtResizeInterval(std::stoull(val));
        } else if (key == "max_item_privileged_bytes") {
            getConfiguration().setMaxItemPrivilegedBytes(std::stoull(val));
        } else if (key == "max_item_size") {
            getConfiguration().setMaxItemSize(std::stoull(val));
        } else if (key == "access_scanner_enabled") {
            getConfiguration().setAccessScannerEnabled(cb_stob(val));
        } else if (key == "alog_path") {
            getConfiguration().setAlogPath(val);
        } else if (key == "alog_max_stored_items") {
            getConfiguration().setAlogMaxStoredItems(std::stoull(val));
        } else if (key == "alog_resident_ratio_threshold") {
            getConfiguration().setAlogResidentRatioThreshold(std::stoull(val));
        } else if (key == "alog_sleep_time") {
            getConfiguration().setAlogSleepTime(std::stoull(val));
        } else if (key == "alog_task_time") {
            getConfiguration().setAlogTaskTime(std::stoull(val));
            /* Start of ItemPager parameters */
        } else if (key == "bfilter_fp_prob") {
            getConfiguration().setBfilterFpProb(std::stof(val));
        } else if (key == "bfilter_key_count") {
            getConfiguration().setBfilterKeyCount(std::stoull(val));
        } else if (key == "pager_active_vb_pcnt") {
            getConfiguration().setPagerActiveVbPcnt(std::stoull(val));
        } else if (key == "pager_sleep_time_ms") {
            getConfiguration().setPagerSleepTimeMs(std::stoull(val));
        } else if (key == "item_eviction_age_percentage") {
            getConfiguration().setItemEvictionAgePercentage(std::stoull(val));
        } else if (key == "item_eviction_freq_counter_age_threshold") {
            getConfiguration().setItemEvictionFreqCounterAgeThreshold(
                    std::stoull(val));
        } else if (key == "item_freq_decayer_chunk_duration") {
            getConfiguration().setItemFreqDecayerChunkDuration(
                    std::stoull(val));
        } else if (key == "item_freq_decayer_percent") {
            getConfiguration().setItemFreqDecayerPercent(std::stoull(val));
            /* End of ItemPager parameters */
        } else if (key == "warmup_min_memory_threshold") {
            getConfiguration().setWarmupMinMemoryThreshold(std::stoull(val));
        } else if (key == "warmup_min_items_threshold") {
            getConfiguration().setWarmupMinItemsThreshold(std::stoull(val));
        } else if (key == "num_reader_threads") {
            ssize_t value = std::stoll(val);
            getConfiguration().setNumReaderThreads(value);
            ExecutorPool::get()->setNumReaders(
                    ThreadPoolConfig::ThreadCount(value));
        } else if (key == "num_writer_threads") {
            ssize_t value = std::stoull(val);
            getConfiguration().setNumWriterThreads(value);
            ExecutorPool::get()->setNumWriters(
                    ThreadPoolConfig::ThreadCount(value));
        } else if (key == "num_auxio_threads") {
            size_t value = std::stoull(val);
            getConfiguration().setNumAuxioThreads(value);
            ExecutorPool::get()->setNumAuxIO(value);
        } else if (key == "num_nonio_threads") {
            size_t value = std::stoull(val);
            getConfiguration().setNumNonioThreads(value);
            ExecutorPool::get()->setNumNonIO(value);
        } else if (key == "bfilter_enabled") {
            getConfiguration().setBfilterEnabled(cb_stob(val));
        } else if (key == "bfilter_residency_threshold") {
            getConfiguration().setBfilterResidencyThreshold(std::stof(val));
        } else if (key == "defragmenter_enabled") {
            getConfiguration().setDefragmenterEnabled(cb_stob(val));
        } else if (key == "defragmenter_interval") {
            auto v = std::stod(val);
            getConfiguration().setDefragmenterInterval(v);
        } else if (key == "item_compressor_interval") {
            size_t v = std::stoull(val);
            // Adding separate validation as external limit is minimum 1
            // to prevent setting item compressor to constantly run
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setItemCompressorInterval(v);
        } else if (key == "item_compressor_chunk_duration") {
            getConfiguration().setItemCompressorChunkDuration(std::stoull(val));
        } else if (key == "defragmenter_age_threshold") {
            getConfiguration().setDefragmenterAgeThreshold(std::stoull(val));
        } else if (key == "defragmenter_chunk_duration") {
            getConfiguration().setDefragmenterChunkDuration(std::stoull(val));
        } else if (key == "defragmenter_stored_value_age_threshold") {
            getConfiguration().setDefragmenterStoredValueAgeThreshold(
                    std::stoull(val));
        } else if (key == "defragmenter_run") {
            runDefragmenterTask();
        } else if (key == "compaction_write_queue_cap") {
            getConfiguration().setCompactionWriteQueueCap(std::stoull(val));
        } else if (key == "chk_expel_enabled") {
            getConfiguration().setChkExpelEnabled(cb_stob(val));
        } else if (key == "dcp_min_compression_ratio") {
            getConfiguration().setDcpMinCompressionRatio(std::stof(val));
        } else if (key == "dcp_noop_mandatory_for_v5_features") {
            getConfiguration().setDcpNoopMandatoryForV5Features(cb_stob(val));
        } else if (key == "access_scanner_run") {
            if (!(runAccessScannerTask())) {
                rv = cb::mcbp::Status::Etmpfail;
            }
        } else if (key == "vb_state_persist_run") {
            runVbStatePersistTask(Vbid(std::stoi(val)));
        } else if (key == "ephemeral_full_policy") {
            getConfiguration().setEphemeralFullPolicy(val);
        } else if (key == "ephemeral_metadata_mark_stale_chunk_duration") {
            getConfiguration().setEphemeralMetadataMarkStaleChunkDuration(
                    std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_age") {
            getConfiguration().setEphemeralMetadataPurgeAge(std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_interval") {
            getConfiguration().setEphemeralMetadataPurgeInterval(
                    std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_stale_chunk_duration") {
            getConfiguration().setEphemeralMetadataPurgeStaleChunkDuration(
                    std::stoull(val));
        } else if (key == "fsync_after_every_n_bytes_written") {
            getConfiguration().setFsyncAfterEveryNBytesWritten(
                    std::stoull(val));
        } else if (key == "xattr_enabled") {
            getConfiguration().setXattrEnabled(cb_stob(val));
        } else if (key == "compression_mode") {
            getConfiguration().setCompressionMode(val);
        } else if (key == "min_compression_ratio") {
            float min_comp_ratio;
            if (safe_strtof(val.c_str(), min_comp_ratio)) {
                getConfiguration().setMinCompressionRatio(min_comp_ratio);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "max_ttl") {
            getConfiguration().setMaxTtl(std::stoull(val));
        } else if (key == "mem_used_merge_threshold_percent") {
            getConfiguration().setMemUsedMergeThresholdPercent(std::stof(val));
        } else if (key == "retain_erroneous_tombstones") {
            getConfiguration().setRetainErroneousTombstones(cb_stob(val));
        } else if (key == "couchstore_tracing") {
            getConfiguration().setCouchstoreTracing(cb_stob(val));
        } else if (key == "couchstore_write_validation") {
            getConfiguration().setCouchstoreWriteValidation(cb_stob(val));
        } else if (key == "couchstore_mprotect") {
            getConfiguration().setCouchstoreMprotect(cb_stob(val));
        } else if (key == "allow_sanitize_value_in_deletion") {
            getConfiguration().setAllowSanitizeValueInDeletion(cb_stob(val));
        } else if (key == "pitr_enabled") {
            getConfiguration().setPitrEnabled(cb_stob(val));
        } else if (key == "pitr_max_history_age") {
            uint32_t value;
            if (safe_strtoul(val.c_str(), value)) {
                getConfiguration().setPitrMaxHistoryAge(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "pitr_granularity") {
            uint32_t value;
            if (safe_strtoul(val.c_str(), value)) {
                getConfiguration().setPitrGranularity(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "magma_fragmentation_percentage") {
            float value;
            if (safe_strtof(val.c_str(), value)) {
                getConfiguration().setMagmaFragmentationPercentage(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "persistent_metadata_purge_age") {
            uint32_t value;
            if (safe_strtoul(val.c_str(), value)) {
                getConfiguration().setPersistentMetadataPurgeAge(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "magma_flusher_thread_percentage") {
            uint32_t value;
            if (safe_strtoul(val.c_str(), value)) {
                getConfiguration().setMagmaFlusherThreadPercentage(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else if (key == "couchstore_file_cache_max_size") {
            uint32_t value;
            if (safe_strtoul(val.c_str(), value)) {
                getConfiguration().setCouchstoreFileCacheMaxSize(value);
            } else {
                rv = cb::mcbp::Status::Einval;
            }
        } else {
            msg = "Unknown config param";
            rv = cb::mcbp::Status::KeyEnoent;
        }
        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        rv = cb::mcbp::Status::Einval;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = cb::mcbp::Status::Einval;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = cb::mcbp::Status::Einval;

        // Handles any miscellaneous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = cb::mcbp::Status::Einval;
    }

    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::setDcpParam(const std::string& key,
                                                         const std::string& val,
                                                         std::string& msg) {
    auto rv = cb::mcbp::Status::Success;
    try {
        if (key == "dcp_conn_buffer_size") {
            getConfiguration().setDcpConnBufferSize(std::stoull(val));
        } else if (key == "dcp_conn_buffer_size_max") {
            getConfiguration().setDcpConnBufferSizeMax(std::stoull(val));
        } else if (key == "dcp_conn_buffer_size_aggr_mem_threshold") {
            getConfiguration().setDcpConnBufferSizeAggrMemThreshold(
                    std::stoull(val));
        } else if (key == "dcp_conn_buffer_size_aggressive_perc") {
            getConfiguration().setDcpConnBufferSizeAggressivePerc(
                    std::stoull(val));
        } else if (key == "dcp_conn_buffer_size_perc") {
            getConfiguration().setDcpConnBufferSizePerc(std::stoull(val));
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
            rv = cb::mcbp::Status::KeyEnoent;
        }
    } catch (std::runtime_error&) {
        msg = "Value out of range.";
        rv = cb::mcbp::Status::Einval;
    }

    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::setVbucketParam(
        Vbid vbucket,
        const std::string& key,
        const std::string& val,
        std::string& msg) {
    auto rv = cb::mcbp::Status::Success;
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
            if (getKVBucket()->forceMaxCas(vbucket, v) != ENGINE_SUCCESS) {
                rv = cb::mcbp::Status::NotMyVbucket;
                msg = "Not my vbucket";
            }
        } else {
            msg = "Unknown config param";
            rv = cb::mcbp::Status::KeyEnoent;
        }
    } catch (std::runtime_error&) {
        msg = "Value out of range.";
        rv = cb::mcbp::Status::Einval;
    }
    return rv;
}

cb::mcbp::Status EventuallyPersistentEngine::evictKey(
        const void* cookie,
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

cb::mcbp::Status EventuallyPersistentEngine::setParam(
        const cb::mcbp::Request& req, std::string& msg) {
    using cb::mcbp::request::SetParamPayload;
    auto extras = req.getExtdata();
    auto* payload = reinterpret_cast<const SetParamPayload*>(extras.data());

    auto key = req.getKey();
    auto val = req.getValue();

    const std::string keyz(reinterpret_cast<const char*>(key.data()),
                           key.size());
    const std::string valz(reinterpret_cast<const char*>(val.data()),
                           val.size());

    switch (payload->getParamType()) {
    case SetParamPayload::Type::Flush:
        return setFlushParam(keyz, valz, msg);
    case SetParamPayload::Type::Replication:
        return setReplicationParam(keyz, valz, msg);
    case SetParamPayload::Type::Checkpoint:
        return setCheckpointParam(keyz, valz, msg);
    case SetParamPayload::Type::Dcp:
        return setDcpParam(keyz, valz, msg);
    case SetParamPayload::Type::Vbucket:
        return setVbucketParam(req.getVBucket(), keyz, valz, msg);
    }

    return cb::mcbp::Status::UnknownCommand;
}

static ENGINE_ERROR_CODE getVBucket(EventuallyPersistentEngine& e,
                                    const void* cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) {
    Vbid vbucket = request.getVBucket();
    VBucketPtr vb = e.getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    } else {
        const auto state = static_cast<vbucket_state_t>(ntohl(vb->getState()));
        return sendResponse(
                response,
                {}, // key
                {}, // extra
                {reinterpret_cast<const char*>(&state), sizeof(state)}, // body
                PROTOCOL_BINARY_RAW_BYTES,
                cb::mcbp::Status::Success,
                0,
                cookie);
    }
}

static ENGINE_ERROR_CODE setVBucket(EventuallyPersistentEngine& e,
                                    const void* cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) {
    nlohmann::json meta;
    vbucket_state_t state;
    auto extras = request.getExtdata();

    if (extras.size() == 1) {
        // This is the new encoding for the SetVBucket state.
        state = vbucket_state_t(extras.front());
        auto val = request.getValue();
        if (!val.empty()) {
            if (state != vbucket_state_active) {
                e.setErrorContext(
                        cookie,
                        "vbucket meta may only be set on active vbuckets");
                return ENGINE_EINVAL;
            }

            try {
                meta = nlohmann::json::parse(val);
            } catch (const std::exception&) {
                e.setErrorContext(cookie, "Invalid JSON provided");
                return ENGINE_EINVAL;
            }
        }
    } else {
        // This is the pre-mad-hatter encoding for the SetVBucketState
        if (extras.size() != sizeof(vbucket_state_t)) {
            // MB-31867: ns_server encodes this in the value field. Fall back
            //           and check if it contains the value
            extras = request.getValue();
        }

        state = static_cast<vbucket_state_t>(
                ntohl(*reinterpret_cast<const uint32_t*>(extras.data())));
    }

    return e.setVBucketState(cookie,
                             response,
                             request.getVBucket(),
                             state,
                             meta.empty() ? nullptr : &meta,
                             TransferVB::No,
                             request.getCas());
}

static ENGINE_ERROR_CODE delVBucket(EventuallyPersistentEngine& e,
                                    const void* cookie,
                                    const cb::mcbp::Request& req,
                                    const AddResponseFn& response) {
    Vbid vbucket = req.getVBucket();
    auto value = req.getValue();
    bool sync = value.size() == 7 && memcmp(value.data(), "async=0", 7) == 0;

    auto error = e.deleteVBucket(vbucket, sync, cookie);
    if (error == ENGINE_SUCCESS) {
        return sendResponse(response,
                            {}, // key
                            {}, // extra
                            {}, // body
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Success,
                            req.getCas(),
                            cookie);
    }
    return error;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getReplicaCmd(
        const cb::mcbp::Request& request,
        const AddResponseFn& response,
        const void* cookie) {
    DocKey key = makeDocKey(cookie, request.getKey());

    auto options = static_cast<get_options_t>(
            QUEUE_BG_FETCH | HONOR_STATES | TRACK_REFERENCE | DELETE_TEMP |
            HIDE_LOCKED_CAS | TRACK_STATISTICS);

    GetValue rv(getKVBucket()->getReplica(
            key, request.getVBucket(), cookie, options));
    auto error_code = rv.getStatus();
    if (error_code != ENGINE_EWOULDBLOCK) {
        ++(getEpStats().numOpsGet);
    }

    if (error_code == ENGINE_SUCCESS) {
        uint32_t flags = rv.item->getFlags();
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Read);
        return sendResponse(
                response,
                rv.item->getKey(), // key
                {reinterpret_cast<const char*>(&flags), sizeof(flags)}, // extra
                {rv.item->getData(), rv.item->getNBytes()}, // body
                rv.item->getDataType(),
                cb::mcbp::Status::Success,
                rv.item->getCas(),
                cookie);
    } else if (error_code == ENGINE_TMPFAIL) {
        return ENGINE_KEY_ENOENT;
    }

    return error_code;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::compactDB(
        const void* cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    const auto res = cb::mcbp::Status::Success;
    CompactionConfig compactionConfig;
    uint64_t cas = req.getCas();

    auto extras = req.getExtdata();
    const auto* payload =
            reinterpret_cast<const cb::mcbp::request::CompactDbPayload*>(
                    extras.data());

    compactionConfig.purge_before_ts = payload->getPurgeBeforeTs();
    compactionConfig.purge_before_seq = payload->getPurgeBeforeSeq();
    compactionConfig.drop_deletes = payload->getDropDeletes();
    Vbid vbid = req.getVBucket();

    ENGINE_ERROR_CODE err;
    if (getEngineSpecific(cookie) == nullptr) {
        ++stats.pendingCompactions;
        storeEngineSpecific(cookie, this);
        err = scheduleCompaction(vbid, compactionConfig, cookie);
    } else {
        storeEngineSpecific(cookie, nullptr);
        err = ENGINE_SUCCESS;
    }

    switch (err) {
    case ENGINE_SUCCESS:
        break;
    case ENGINE_NOT_MY_VBUCKET:
        --stats.pendingCompactions;
        EP_LOG_WARN(
                "Compaction of {} failed because the db file doesn't exist!!!",
                vbid);
        return ENGINE_NOT_MY_VBUCKET;
    case ENGINE_EINVAL:
        --stats.pendingCompactions;
        EP_LOG_WARN("Compaction of {} failed because of an invalid argument",
                    vbid);
        return ENGINE_EINVAL;
    case ENGINE_EWOULDBLOCK:
        // We don't use the value stored in the engine-specific code, just
        // that it is non-null...
        storeEngineSpecific(cookie, this);
        return ENGINE_EWOULDBLOCK;
    case ENGINE_TMPFAIL:
        EP_LOG_WARN(
                "Request to compact {} hit a temporary failure and may need to "
                "be retried",
                vbid);
        setErrorContext(cookie, "Temporary failure in compacting db file.");
        return ENGINE_TMPFAIL;
    default:
        --stats.pendingCompactions;
        EP_LOG_WARN("Compaction of {} failed: {}",
                    vbid,
                    cb::to_string(cb::engine_errc(err)));
        setErrorContext(cookie,
                        "Failed to compact db file: " +
                                cb::to_string(cb::engine_errc(err)));
        return err;
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        PROTOCOL_BINARY_RAW_BYTES,
                        res,
                        cas,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::processUnknownCommandInner(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    auto res = cb::mcbp::Status::UnknownCommand;
    std::string dynamic_msg;
    const char* msg = nullptr;
    size_t msg_size = 0;

    /**
     * Session validation
     * (For ns_server commands only)
     */
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::SetParam:
    case cb::mcbp::ClientOpcode::SetVbucket:
    case cb::mcbp::ClientOpcode::DelVbucket:
    case cb::mcbp::ClientOpcode::CompactDb:
        if (!getEngineSpecific(cookie)) {
            uint64_t cas = request.getCas();
            if (!validateSessionCas(cas)) {
                setErrorContext(cookie, "Invalid session token");
                return ENGINE_KEY_EEXISTS;
            }
        }
        break;
    default:
        break;
    }

    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::GetAllVbSeqnos:
        return getAllVBucketSequenceNumbers(cookie, request, response);

    case cb::mcbp::ClientOpcode::GetVbucket: {
        HdrMicroSecBlockTimer timer(&stats.getVbucketCmdHisto);
        return ::getVBucket(*this, cookie, request, response);
    }
    case cb::mcbp::ClientOpcode::DelVbucket: {
        HdrMicroSecBlockTimer timer(&stats.delVbucketCmdHisto);
        const auto rv = ::delVBucket(*this, cookie, request, response);
        if (rv != ENGINE_EWOULDBLOCK) {
            decrementSessionCtr();
            storeEngineSpecific(cookie, nullptr);
        }
        return rv;
    }
    case cb::mcbp::ClientOpcode::SetVbucket: {
        HdrMicroSecBlockTimer timer(&stats.setVbucketCmdHisto);
        const auto rv = ::setVBucket(*this, cookie, request, response);
        decrementSessionCtr();
        return rv;
    }
    case cb::mcbp::ClientOpcode::StopPersistence:
        res = stopFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::StartPersistence:
        res = startFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::SetParam:
        res = setParam(request, dynamic_msg);
        msg = dynamic_msg.c_str();
        msg_size = dynamic_msg.length();
        decrementSessionCtr();
        break;
    case cb::mcbp::ClientOpcode::EvictKey:
        res = evictKey(cookie, request, &msg);
        break;
    case cb::mcbp::ClientOpcode::Observe:
        return observe(cookie, request, response);
    case cb::mcbp::ClientOpcode::ObserveSeqno:
        return observe_seqno(cookie, request, response);
    case cb::mcbp::ClientOpcode::LastClosedCheckpoint:
        return handleLastClosedCheckpoint(cookie, request, response);
    case cb::mcbp::ClientOpcode::CreateCheckpoint:
        return handleCreateCheckpoint(cookie, request, response);
    case cb::mcbp::ClientOpcode::CheckpointPersistence:
        return handleCheckpointPersistence(cookie, request, response);
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
        return getReplicaCmd(request, response, cookie);
    case cb::mcbp::ClientOpcode::EnableTraffic:
    case cb::mcbp::ClientOpcode::DisableTraffic:
        return handleTrafficControlCmd(cookie, request, response);
    case cb::mcbp::ClientOpcode::CompactDb: {
        const auto rv = compactDB(cookie, request, response);
        if (rv != ENGINE_EWOULDBLOCK) {
            decrementSessionCtr();
            storeEngineSpecific(cookie, nullptr);
        }
        return rv;
    }
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::unknown_command(
        const void* cookie,
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

void EventuallyPersistentEngine::item_set_cas(gsl::not_null<ItemIface*> itm,
                                              uint64_t cas) {
    static_cast<Item*>(itm.get())->setCas(cas);
}

void EventuallyPersistentEngine::item_set_datatype(
        gsl::not_null<ItemIface*> itm,
        protocol_binary_datatype_t datatype) {
    static_cast<Item*>(itm.get())->setDataType(datatype);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::step(
        gsl::not_null<const void*> cookie,
        DcpMessageProducersIface& producers) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    DcpMsgProducersBorderGuard guardedProducers(producers);
    return conn.step(guardedProducers);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::open(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint32_t seqno,
        uint32_t flags,
        std::string_view conName,
        std::string_view value) {
    return acquireEngine(this)->dcpOpen(
            cookie, opaque, seqno, flags, conName, value);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::add_stream(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags) {
    return acquireEngine(this)->dcpAddStream(cookie, opaque, vbucket, flags);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::close_stream(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamId sid) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.closeStream(opaque, vbucket, sid);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::stream_req(
        gsl::not_null<const void*> cookie,
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
        return ENGINE_ERROR_CODE(e.code().value());
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::get_failover_log(
        gsl::not_null<const void*> cookie,
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

    if (getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
        return ENGINE_EWOULDBLOCK;
    }

    auto* conn = engine->tryGetConnHandler(cookie);
    // Note: (conn != nullptr) only if conn is a DCP connection
    if (conn) {
        auto* producer = dynamic_cast<DcpProducer*>(conn);
        // GetFailoverLog not supported for DcpConsumer
        if (!producer) {
            EP_LOG_WARN(
                    "Disconnecting - This connection doesn't support the dcp "
                    "get "
                    "failover log API");
            return ENGINE_DISCONNECT;
        }
        producer->setLastReceiveTime(ep_current_time());
        if (producer->doDisconnect()) {
            return ENGINE_DISCONNECT;
        }
    }
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        EP_LOG_WARN(
                "{} ({}) Get Failover Log failed because this "
                "vbucket doesn't exist",
                conn ? conn->logHeader() : "MCBP-Connection",
                vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    }
    auto failoverEntries = vb->failovers->getFailoverLog();
    NonBucketAllocationGuard guard;
    return callback(failoverEntries);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::stream_end(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        cb::mcbp::DcpStreamEndStatus status) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie).streamEnd(opaque, vbucket, status);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::snapshot_marker(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::mutation(
        gsl::not_null<const void*> cookie,
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
    if (!mcbp::datatype::is_valid(datatype)) {
        EP_LOG_WARN(
                "Invalid value for datatype "
                " (DCPMutation)");
        return ENGINE_EINVAL;
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::deletion(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::deletion_v2(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::expiration(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::set_vbucket_state(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        vbucket_state_t state) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.setVBucketState(opaque, vbucket, state);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::noop(
        gsl::not_null<const void*> cookie, uint32_t opaque) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie).noop(opaque);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::buffer_acknowledgement(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t buffer_bytes) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.bufferAcknowledgement(opaque, vbucket, buffer_bytes);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::control(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        std::string_view key,
        std::string_view value) {
    auto engine = acquireEngine(this);
    return engine->getConnHandler(cookie).control(opaque, key, value);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::response_handler(
        gsl::not_null<const void*> cookie, const cb::mcbp::Response& response) {
    auto engine = acquireEngine(this);
    auto* conn = engine->tryGetConnHandler(cookie);
    if (conn && conn->handleResponse(response)) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::system_event(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::prepare(
        gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::seqno_acknowledged(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t prepared_seqno) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.seqno_acknowledged(opaque, vbucket, prepared_seqno);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::commit(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        const DocKey& key,
        uint64_t prepared_seqno,
        uint64_t commit_seqno) {
    auto engine = acquireEngine(this);
    auto& conn = engine->getConnHandler(cookie);
    return conn.commit(opaque, vbucket, key, prepared_seqno, commit_seqno);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::abort(
        gsl::not_null<const void*> cookie,
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
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE create_ep_engine_instance(GET_SERVER_API get_server_api,
                                            EngineIface** handle) {
    ServerApi* api = get_server_api();
    if (api == nullptr) {
        return ENGINE_ENOTSUP;
    }

    BucketLogger::setLoggerAPI(api->log);

    // Register and track the engine creation
    auto arena = cb::ArenaMalloc::registerClient();
    cb::ArenaMallocGuard trackEngineCreation(arena);

    try {
        *handle = new EventuallyPersistentEngine(get_server_api, arena);
    } catch (const std::bad_alloc&) {
        cb::ArenaMalloc::unregisterClient(arena);
        return ENGINE_ENOMEM;
    }

    initialize_time_functions(api->core);
    return ENGINE_SUCCESS;
}

/*
    This method is called prior to unloading of the shared-object.
    Global clean-up should be performed from this method.
*/
void destroy_ep_engine() {
    ExecutorPool::shutdown();
    globalBucketLogger.reset();
}

bool EventuallyPersistentEngine::get_item_info(
        gsl::not_null<const ItemIface*> itm,
        gsl::not_null<item_info*> itm_info) {
    const Item* it = reinterpret_cast<const Item*>(itm.get());
    *itm_info = acquireEngine(this)->getItemInfo(*it);
    return true;
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::get_meta(
        gsl::not_null<const void*> cookie, const DocKey& key, Vbid vbucket) {
    return acquireEngine(this)->getMetaInner(cookie, key, vbucket);
}

cb::engine_errc EventuallyPersistentEngine::set_collection_manifest(
        gsl::not_null<const void*> cookie, std::string_view json) {
    auto engine = acquireEngine(this);
    auto rv = engine->getKVBucket()->setCollections(json, cookie);

    auto status = cb::engine_errc(rv.code().value());
    if (cb::engine_errc::success != status &&
        status != cb::engine_errc::would_block) {
        engine->setErrorContext(cookie, rv.what());
    }

    return status;
}

cb::engine_errc EventuallyPersistentEngine::get_collection_manifest(
        gsl::not_null<const void*> cookie, const AddResponseFn& response) {
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
EventuallyPersistentEngine::get_collection_id(gsl::not_null<const void*> cookie,
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
        engine->setUnknownCollectionErrorContext(cookie,
                                                 rv.getManifestId());
    }
    return rv;
}

cb::EngineErrorGetScopeIDResult EventuallyPersistentEngine::get_scope_id(
        gsl::not_null<const void*> cookie, std::string_view path) {
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

cb::EngineErrorGetScopeIDResult EventuallyPersistentEngine::get_scope_id(
        gsl::not_null<const void*>,
        const DocKey& key,
        std::optional<Vbid> vbid) const {
    auto engine = acquireEngine(this);
    if (vbid) {
        auto vbucket = engine->getVBucket(*vbid);
        if (vbucket) {
            auto cHandle = vbucket->lockCollections(key);
            if (cHandle.valid()) {
                return cb::EngineErrorGetScopeIDResult(cHandle.getManifestUid(),
                                                       cHandle.getScopeID());
            }
        } else {
            return cb::EngineErrorGetScopeIDResult(
                    cb::engine_errc::not_my_vbucket);
        }
    } else {
        auto scopeIdInfo =
                engine->getKVBucket()->getScopeID(key.getCollectionID());
        if (scopeIdInfo.second.has_value()) {
            return cb::EngineErrorGetScopeIDResult(
                    scopeIdInfo.first, ScopeID(scopeIdInfo.second.value()));
        }
    }
    return cb::EngineErrorGetScopeIDResult(cb::engine_errc::unknown_collection);
}

cb::engine::FeatureSet EventuallyPersistentEngine::getFeatures() {
    // This function doesn't track memory against the engine, but create a
    // guard regardless to make this explicit because we only call this once per
    // bucket creation
    NonBucketAllocationGuard guard;
    cb::engine::FeatureSet ret;
    ret.emplace(cb::engine::Feature::Collections);
    return ret;
}

bool EventuallyPersistentEngine::isXattrEnabled() {
    return getKVBucket()->isXattrEnabled();
}

cb::HlcTime EventuallyPersistentEngine::getVBucketHlcNow(Vbid vbucket) {
    return getKVBucket()->getVBucket(vbucket)->getHLCNow();
}

EventuallyPersistentEngine::EventuallyPersistentEngine(
        GET_SERVER_API get_server_api, cb::ArenaMallocClient arena)
    : kvBucket(nullptr),
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

void EventuallyPersistentEngine::reserveCookie(const void* cookie) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->reserve(cookie);
}

void EventuallyPersistentEngine::releaseCookie(const void* cookie) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->release(cookie);
}

void EventuallyPersistentEngine::setDcpConnHandler(
        const void* cookie, DcpConnHandlerIface* handler) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->setDcpConnHandler(cookie, handler);
}

DcpConnHandlerIface* EventuallyPersistentEngine::getDcpConnHandler(
        const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->getDcpConnHandler(cookie);
}

void EventuallyPersistentEngine::storeEngineSpecific(const void* cookie,
                                                     void* engine_data) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->store_engine_specific(cookie, engine_data);
}

void* EventuallyPersistentEngine::getEngineSpecific(const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->get_engine_specific(cookie);
}

bool EventuallyPersistentEngine::isDatatypeSupported(
        const void* cookie, protocol_binary_datatype_t datatype) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->is_datatype_supported(cookie, datatype);
}

bool EventuallyPersistentEngine::isMutationExtrasSupported(const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->is_mutation_extras_supported(cookie);
}

bool EventuallyPersistentEngine::isXattrEnabled(const void* cookie) {
    return isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_XATTR);
}

bool EventuallyPersistentEngine::isCollectionsSupported(const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->is_collections_supported(cookie);
}

cb::mcbp::ClientOpcode EventuallyPersistentEngine::getOpcodeIfEwouldblockSet(
        const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->get_opcode_if_ewouldblock_set(cookie);
}

bool EventuallyPersistentEngine::validateSessionCas(const uint64_t cas) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->validate_session_cas(cas);
}

void EventuallyPersistentEngine::decrementSessionCtr() {
    NonBucketAllocationGuard guard;
    serverApi->cookie->decrement_session_ctr();
}

void EventuallyPersistentEngine::setErrorContext(const void* cookie,
                                                 std::string_view message) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_error_context(const_cast<void*>(cookie), message);
}

void EventuallyPersistentEngine::setErrorJsonExtras(
        const void* cookie, const nlohmann::json& json) const {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_error_json_extras(const_cast<void*>(cookie), json);
}

void EventuallyPersistentEngine::setUnknownCollectionErrorContext(
        const void* cookie, uint64_t manifestUid) const {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_unknown_collection_error_context(
            const_cast<void*>(cookie), manifestUid);
}

template <typename T>
void EventuallyPersistentEngine::notifyIOComplete(T cookies,
                                                  ENGINE_ERROR_CODE status) {
    NonBucketAllocationGuard guard;
    for (auto& cookie : cookies) {
        serverApi->cookie->notify_io_complete(cookie, status);
    }
}

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
class EpEngineValueChangeListener : public ValueChangedListener {
public:
    explicit EpEngineValueChangeListener(EventuallyPersistentEngine& e)
        : engine(e) {
        // EMPTY
    }

    void sizeValueChanged(const std::string& key, size_t value) override {
        if (key.compare("getl_max_timeout") == 0) {
            engine.setGetlMaxTimeout(value);
        } else if (key.compare("getl_default_timeout") == 0) {
            engine.setGetlDefaultTimeout(value);
        } else if (key.compare("max_item_size") == 0) {
            engine.setMaxItemSize(value);
        } else if (key.compare("max_item_privileged_bytes") == 0) {
            engine.setMaxItemPrivilegedBytes(value);
        }
    }

    void stringValueChanged(const std::string& key,
                            const char* value) override {
        if (key == "compression_mode") {
            std::string value_str{value, strlen(value)};
            engine.setCompressionMode(value_str);
        }
    }

    void floatValueChanged(const std::string& key, float value) override {
        if (key == "min_compression_ratio") {
            engine.setMinCompressionRatio(value);
        }
    }

    void booleanValueChanged(const std::string& key, bool b) override {
        if (key == "allow_sanitize_value_in_deletion") {
            engine.allowSanitizeValueInDeletion.store(b);
        }
    }

private:
    EventuallyPersistentEngine &engine;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    auto switchToEngine = acquireEngine(this);
    resetStats();
    if (config != nullptr) {
        if (!configuration.parseConfiguration(config, serverApi)) {
            EP_LOG_WARN(
                    "Failed to parse the configuration config "
                    "during bucket initialization.  config={}",
                    config);
            return ENGINE_FAILED;
        }
    }

    name = configuration.getCouchBucket();

    if (config != nullptr) {
        EP_LOG_INFO(R"(EPEngine::initialize: using configuration:"{}")",
                    config);
    }

    auto& env = Environment::get();
    env.engineFileDescriptors = serverApi->core->getMaxEngineFileDescriptors();

    // Ensure (global) ExecutorPool has been created, and update the (local)
    // configuration with the actual number of each thread type we have (config
    // params are typically defaulted to "0" which means "auto-configure
    // thread counts"
    auto threads = serverApi->core->getThreadPoolSizes();
    configuration.setNumReaderThreads(static_cast<int>(threads.num_readers));
    configuration.setNumWriterThreads(static_cast<int>(threads.num_writers));

    auto* pool = ExecutorPool::get();
    // Update configuration to reflect the actual number of threads which have
    // been created.
    configuration.setNumReaderThreads(pool->getNumReaders());
    configuration.setNumWriterThreads(pool->getNumWriters());
    configuration.setNumAuxioThreads(pool->getNumAuxIO());
    configuration.setNumNonioThreads(pool->getNumNonIO());

    maxFailoverEntries = configuration.getMaxFailoverEntries();

    // Start updating the variables from the config!
    VBucket::setMutationMemoryThreshold(
            configuration.getMutationMemThreshold());

    if (configuration.getMaxSize() == 0) {
        EP_LOG_WARN("Invalid configuration: max_size must be a non-zero value");
        return ENGINE_FAILED;
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

    auto numShards = configuration.getMaxNumShards();
    workload = new WorkLoadPolicy(configuration.getMaxNumWorkers(), numShards);

    const auto& confResMode = configuration.getConflictResolutionType();
    if (!setConflictResolutionMode(confResMode)) {
        EP_LOG_WARN(
                "Invalid enum value '{}' for config option "
                "conflict_resolution_type.",
                confResMode);
        return ENGINE_FAILED;
    }

    dcpConnMap_ = std::make_unique<DcpConnMap>(*this);

    /* Get the flow control policy */
    std::string flowCtlPolicy = configuration.getDcpFlowControlPolicy();

    if (!flowCtlPolicy.compare("static")) {
        dcpFlowControlManager_ =
                std::make_unique<DcpFlowControlManagerStatic>(*this);
    } else if (!flowCtlPolicy.compare("dynamic")) {
        dcpFlowControlManager_ =
                std::make_unique<DcpFlowControlManagerDynamic>(*this);
    } else if (!flowCtlPolicy.compare("aggressive")) {
        dcpFlowControlManager_ =
                std::make_unique<DcpFlowControlManagerAggressive>(*this);
    } else {
        /* Flow control is not enabled */
        dcpFlowControlManager_ = std::make_unique<DcpFlowControlManager>(*this);
    }

    checkpointConfig = new CheckpointConfig(*this);
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
        return ENGINE_FAILED;
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

    return ENGINE_SUCCESS;
}

bool EventuallyPersistentEngine::setConflictResolutionMode(
        std::string_view mode) {
    if (mode == "seqno") {
        conflictResolutionMode = ConflictResolutionMode::RevisionId;
    } else if (mode == "lww") {
        conflictResolutionMode = ConflictResolutionMode::LastWriteWins;
    } else if (mode == "custom") {
        conflictResolutionMode = ConflictResolutionMode::Custom;
    } else {
        return false;
    }
    return true;
}

void EventuallyPersistentEngine::destroyInner(bool force) {
    stats.forceShutdown = force;
    stats.isShutdown = true;

    // Perform a snapshot of the stats before shutting down so we can persist
    // the type of shutdown (stats.forceShutdown), and consequently on the
    // next warmup can determine is there was a clean shutdown - see
    // Warmup::cleanShutdown
    if (kvBucket) {
        kvBucket->snapshotStats();
    }
    if (dcpConnMap_) {
        dcpConnMap_->shutdownAllConnections();
    }
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::itemDelete(
        const void* cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        std::optional<cb::durability::Requirements> durability,
        mutation_descr_t& mut_info) {
    // Check if this is a in-progress durable delete which has now completed -
    // (see 'case EWOULDBLOCK' at the end of this function where we record
    // the fact we must block the client until the SycnWrite is durable).
    if (durability) {
        void* deletedCas = getEngineSpecific(cookie);
        if (deletedCas) {
            // Non-null means this is the second call to this function after
            // the SyncWrite has completed.
            // Clear the engineSpecific, and return SUCCESS.
            storeEngineSpecific(cookie, nullptr);

            cas = reinterpret_cast<uint64_t>(deletedCas);
            // @todo-durability - add support for non-sucesss (e.g. Aborted)
            // when we support non-successful completions of SyncWrites.
            return ENGINE_SUCCESS;
        }
    }

    ENGINE_ERROR_CODE ret = kvBucket->deleteItem(
            key, cas, vbucket, cookie, durability, nullptr, mut_info);

    switch (ret) {
    case ENGINE_KEY_ENOENT:
        // FALLTHROUGH
    case ENGINE_NOT_MY_VBUCKET:
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }
        break;

    case ENGINE_SYNC_WRITE_PENDING:
        if (durability) {
            // Record the fact that we are blocking to wait for SyncDelete
            // completion; so the next call to this function should return
            // the result of the SyncWrite (see call to getEngineSpecific at
            // the head of this function).
            // (just store non-null value to indicate this).
            storeEngineSpecific(cookie, reinterpret_cast<void*>(cas));
        }
        ret = ENGINE_EWOULDBLOCK;
        break;

    case ENGINE_SUCCESS:
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
        const void* cookie,
        const DocKey& key,
        Vbid vbucket,
        get_options_t options) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
            std::forward_as_tuple(stats.getCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::Get));

    GetValue gv(kvBucket->get(key, vbucket, cookie, options));
    ENGINE_ERROR_CODE ret = gv.getStatus();

    if (ret == ENGINE_SUCCESS) {
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
        if (options & TRACK_STATISTICS) {
            ++stats.numOpsGet;
        }
    } else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            return cb::makeEngineErrorItemPair(
                    cb::engine_errc::temporary_failure);
        }
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(ret));
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getAndTouchInner(
        const void* cookie, const DocKey& key, Vbid vbucket, uint32_t exptime) {
    time_t expiry_time = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

    GetValue gv(kvBucket->getAndUpdateTtl(key, vbucket, cookie, expiry_time));

    auto rv = gv.getStatus();
    if (rv == ENGINE_SUCCESS) {
        ++stats.numOpsGet;
        ++stats.numOpsStore;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), this);
    }

    if (isDegradedMode()) {
        // Remap all some of the error codes
        switch (rv) {
        case ENGINE_KEY_EEXISTS:
        case ENGINE_KEY_ENOENT:
        case ENGINE_NOT_MY_VBUCKET:
            rv = ENGINE_TMPFAIL;
            break;
        default:
            break;
        }
    }

    if (rv == ENGINE_KEY_EEXISTS) {
        rv = ENGINE_LOCKED;
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(rv));
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getIfInner(
        const void* cookie,
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

        GetValue gv(kvBucket->get(key, vbucket, cookie, options));
        ENGINE_ERROR_CODE status = gv.getStatus();

        switch (status) {
        case ENGINE_SUCCESS:
            break;

        case ENGINE_KEY_ENOENT: // FALLTHROUGH
        case ENGINE_NOT_MY_VBUCKET: // FALLTHROUGH
            if (isDegradedMode()) {
                status = ENGINE_TMPFAIL;
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
        const void* cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    auto default_timeout = static_cast<uint32_t>(getGetlDefaultTimeout());

    if (lock_timeout == 0) {
        lock_timeout = default_timeout;
    } else if (lock_timeout > static_cast<uint32_t>(getGetlMaxTimeout())) {
        EP_LOG_WARN(
                "EventuallyPersistentEngine::get_locked: "
                "Illegal value for lock timeout specified {}. "
                "Using default value: {}",
                lock_timeout,
                default_timeout);
        lock_timeout = default_timeout;
    }

    auto result = kvBucket->getLocked(key, vbucket, ep_current_time(),
                                      lock_timeout, cookie);

    if (result.getStatus() == ENGINE_SUCCESS) {
        ++stats.numOpsGet;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, result.item.release(), this);
    }

    return cb::makeEngineErrorItemPair(cb::engine_errc(result.getStatus()));
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::unlockInner(const void* cookie,
                                                          const DocKey& key,
                                                          Vbid vbucket,
                                                          uint64_t cas) {
    return kvBucket->unlockKey(key, vbucket, cas, ep_current_time(), cookie);
}

cb::EngineErrorCasPair EventuallyPersistentEngine::storeIfInner(
        const void* cookie,
        Item& item,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        bool preserveTtl) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
            std::forward_as_tuple(stats.storeCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::Store));

    // MB-37374: Ensure that documents in deleted state have no user value.
    if (mcbp::datatype::is_xattr(item.getDataType()) && item.isDeleted()) {
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
        auto* cookieCas = getEngineSpecific(cookie);
        if (cookieCas != nullptr) {
            // Non-null means this is the second call to this function after
            // the SyncWrite has completed.
            // Clear the engineSpecific, and return SUCCESS.
            storeEngineSpecific(cookie, nullptr);
            return {cb::engine_errc::success,
                    reinterpret_cast<uint64_t>(cookieCas)};
        }
    }

    ENGINE_ERROR_CODE status;
    switch (operation) {
    case StoreSemantics::CAS:
        if (item.getCas() == 0) {
            // Using a cas command with a cas wildcard doesn't make sense
            status = ENGINE_NOT_STORED;
            break;
        }
    // FALLTHROUGH
    case StoreSemantics::Set:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }
        item.setPreserveTtl(preserveTtl);
        status = kvBucket->set(item, cookie, predicate);
        break;

    case StoreSemantics::Add:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }

        if (item.getCas() != 0) {
            // Adding an item with a cas value doesn't really make sense...
            return {cb::engine_errc::key_already_exists, cas};
        }

        status = kvBucket->add(item, cookie);
        break;

    case StoreSemantics::Replace:
        item.setPreserveTtl(preserveTtl);
        status = kvBucket->replace(item, cookie, predicate);
        break;
    default:
        status = ENGINE_ENOTSUP;
    }

    switch (status) {
    case ENGINE_SUCCESS:
        ++stats.numOpsStore;
        // If success - check if we're now in need of some memory freeing
        kvBucket->checkAndMaybeFreeMemory();
        break;
    case ENGINE_ENOMEM:
        status = memoryCondition();
        break;
    case ENGINE_NOT_STORED:
    case ENGINE_NOT_MY_VBUCKET:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }
        break;
    case ENGINE_SYNC_WRITE_PENDING:
        if (item.isPending()) {
            // Record the fact that we are blocking to wait for SyncWrite
            // completion; so the next call to this function should return
            // the result of the SyncWrite (see call to getEngineSpecific at
            // the head of this function. Store the cas of the item so that we
            // can return it to the client later.
            storeEngineSpecific(cookie, reinterpret_cast<void*>(item.getCas()));
        }
        status = ENGINE_EWOULDBLOCK;
        break;
    default:
        break;
    }

    return {cb::engine_errc(status), item.getCas()};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::storeInner(
        const void* cookie,
        Item& itm,
        uint64_t& cas,
        StoreSemantics operation,
        bool preserveTtl) {
    auto rv = storeIfInner(cookie, itm, cas, operation, {}, preserveTtl);
    cas = rv.cas;
    return ENGINE_ERROR_CODE(rv.status);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::memoryCondition() {
    // Trigger necessary task(s) to free memory down below high watermark.
    getKVBucket()->attemptToFreeMemory();
    getKVBucket()->wakeUpCheckpointRemover();

    if (stats.getEstimatedTotalMemoryUsed() < stats.getMaxDataSize()) {
        // Still below bucket_quota - treat as temporary failure.
        ++stats.tmp_oom_errors;
        return ENGINE_TMPFAIL;
    } else {
        // Already over bucket quota - make this a hard error.
        ++stats.oom_errors;
        return ENGINE_ENOMEM;
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
        EP_LOG_INFO("EventuallyPersistentEngine::enableTraffic() result true");
    }
    return bTrafficEnabled;
}

void EventuallyPersistentEngine::doEngineStatsRocksDB(
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    size_t value;

    // Specific to RocksDB. Cumulative ep-engine stats.
    // Note: These are also reported per-shard in 'kvstore' stats.
    // Memory Usage
    if (kvBucket->getKVStoreStat(
                "kMemTableTotal", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_kMemTableTotal, value);
    }
    if (kvBucket->getKVStoreStat(
                "kMemTableUnFlushed", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_kMemTableUnFlushed, value);
    }
    if (kvBucket->getKVStoreStat(
                "kTableReadersTotal", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_kTableReadersTotal, value);
    }
    if (kvBucket->getKVStoreStat(
                "kCacheTotal", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_kCacheTotal, value);
    }
    // MemTable Size per-CF
    if (kvBucket->getKVStoreStat("default_kSizeAllMemTables",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_default_kSizeAllMemTables, value);
    }
    if (kvBucket->getKVStoreStat("seqno_kSizeAllMemTables",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_seqno_kSizeAllMemTables, value);
    }
    // BlockCache Hit Ratio
    size_t hit = 0;
    size_t miss = 0;
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.data.hit",
                                 hit,
                                 KVBucketIface::KVSOption::RW) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.data.miss",
                                 miss,
                                 KVBucketIface::KVSOption::RW) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_data_hit_ratio, tmpRatio);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.index.hit",
                                 hit,
                                 KVBucketIface::KVSOption::RW) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.index.miss",
                                 miss,
                                 KVBucketIface::KVSOption::RW) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_index_hit_ratio,
                          tmpRatio);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.filter.hit",
                                 hit,
                                 KVBucketIface::KVSOption::RW) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.filter.miss",
                                 miss,
                                 KVBucketIface::KVSOption::RW) &&
        (hit + miss) != 0) {
        const auto tmpRatio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        collector.addStat(Key::ep_rocksdb_block_cache_filter_hit_ratio,
                          tmpRatio);
    }
    // Disk Usage per-CF
    if (kvBucket->getKVStoreStat("default_kTotalSstFilesSize",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_default_kTotalSstFilesSize, value);
    }
    if (kvBucket->getKVStoreStat("seqno_kTotalSstFilesSize",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_seqno_kTotalSstFilesSize, value);
    }
    // Scan stats
    if (kvBucket->getKVStoreStat(
                "scan_totalSeqnoHits", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_scan_totalSeqnoHits, value);
    }
    if (kvBucket->getKVStoreStat(
                "scan_oldSeqnoHits", value, KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_rocksdb_scan_oldSeqnoHits, value);
    }
}

void EventuallyPersistentEngine::doEngineStatsCouchDB(const BucketStatCollector& collector, const EPStats& epstats){
    using namespace cb::stats;
    size_t value;
    if (kvBucket->getKVStoreStat("io_document_write_bytes",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        collector.addStat(Key::ep_io_document_write_bytes, value);

        // Lambda to print a Write Amplification stat for the given bytes
        // written counter.
        auto printWriteAmpStat = [this, &collector, docBytes = value](
                const char* writeBytesStat,
                const char* writeAmpStat) {
          double writeAmp = std::numeric_limits<double>::infinity();
          size_t bytesWritten;
          if (docBytes &&
              kvBucket->getKVStoreStat(writeBytesStat,
                                       bytesWritten,
                                       KVBucketIface::KVSOption::RW)) {
              writeAmp = double(bytesWritten) / docBytes;
          }
          collector.addStat(writeAmpStat, writeAmp);
        };

        printWriteAmpStat("io_flusher_write_bytes",
                          "ep_io_flusher_write_amplification");
        printWriteAmpStat("io_total_write_bytes",
                          "ep_io_total_write_amplification");
    }
    if (kvBucket->getKVStoreStat("io_total_read_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        collector.addStat(Key::ep_io_total_read_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_total_write_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        collector.addStat(Key::ep_io_total_write_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_compaction_read_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        collector.addStat(Key::ep_io_compaction_read_bytes, value);
    }
    if (kvBucket->getKVStoreStat("io_compaction_write_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        collector.addStat(Key::ep_io_compaction_write_bytes, value);
    }

    if (kvBucket->getKVStoreStat("io_bg_fetch_read_count",
                                 value,
                                 KVBucketIface::KVSOption::BOTH)) {
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
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    auto divide = [](double a, double b) { return b ? a / b : 0; };
    constexpr std::array<std::string_view, 32> statNames = {
            {"magma_NCompacts",
             "magma_NFlushes",
             "magma_NTTLCompacts",
             "magma_NFileCountCompacts",
             "magma_NWriterCompacts",
             "magma_BytesOutgoing",
             "magma_NReadBytes",
             "magma_NReadBytesGet",
             "magma_NGets",
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
             "magma_BufferMemUsed",
             "magma_TotalBloomFilterMemUsed",
             "magma_BlockCacheHits",
             "magma_BlockCacheMisses",
             "magma_NTablesDeleted",
             "magma_NTablesCreated",
             "magma_NTableFiles",
             "magma_NSyncs"}};

    auto stats =
            kvBucket->getKVStoreStats(statNames, KVBucketIface::KVSOption::RW);

    // Return whether stat exists. If exists, save value in output param value.
    auto statExists = [&](std::string_view name, size_t& value) {
        auto stat = stats.find(name);
        if (stat != stats.end()) {
            value = stat->second;
            return true;
        }
        return false;
    };

    // If given stat exists, add it to collector.
    auto addStat = [&](Key key, std::string_view name) {
        size_t value = 0;
        if (statExists(name, value)) {
            collector.addStat(key, value);
        }
    };

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
    addStat(Key::ep_magma_write_cache_mem_used, "magma_WriteCacheMemUsed");
    addStat(Key::ep_magma_wal_mem_used, "magma_WALMemUsed");
    addStat(Key::ep_magma_table_meta_mem_used, "magma_TableMetaMemUsed");
    addStat(Key::ep_magma_buffer_mem_used, "magma_BufferMemUsed");
    addStat(Key::ep_magma_bloom_filter_mem_used,
            "magma_TotalBloomFilterMemUsed");

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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEngineStats(
        const BucketStatCollector& collector) {
    configuration.addStats(collector);

    EPStats &epstats = getEpStats();

    using namespace cb::stats;
    collector.addStat(Key::ep_storage_age, epstats.dirtyAge);
    collector.addStat(Key::ep_storage_age_highwat, epstats.dirtyAgeHighWat);
    collector.addStat(Key::ep_num_workers,
                      ExecutorPool::get()->getNumWorkersStat());

    if (getWorkloadPriority() == HIGH_BUCKET_PRIORITY) {
        collector.addStat(Key::ep_bucket_priority, "HIGH");
    } else if (getWorkloadPriority() == LOW_BUCKET_PRIORITY) {
        collector.addStat(Key::ep_bucket_priority, "LOW");
    }

    collector.addStat(Key::ep_total_enqueued, epstats.totalEnqueued);
    collector.addStat(Key::ep_total_deduplicated, epstats.totalDeduplicated);
    collector.addStat(Key::ep_expired_access, epstats.expired_access);
    collector.addStat(Key::ep_expired_compactor, epstats.expired_compactor);
    collector.addStat(Key::ep_expired_pager, epstats.expired_pager);
    collector.addStat(Key::ep_queue_size, epstats.diskQueueSize);
    collector.addStat(Key::ep_diskqueue_items, epstats.diskQueueSize);
    auto* flusher = kvBucket->getFlusher(EP_PRIMARY_SHARD);
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
        collector.addStat(Key::ep_chk_persistence_timeout,
                          VBucket::getCheckpointFlushTimeout().count());
    }
    collector.addStat(Key::ep_vbucket_del, epstats.vbucketDeletions);
    collector.addStat(Key::ep_vbucket_del_fail, epstats.vbucketDeletionFail);
    collector.addStat(Key::ep_flush_duration_total,
                      epstats.cumulativeFlushTime);

    kvBucket->getAggregatedVBucketStats(collector);

    kvBucket->getFileStats(collector);

    collector.addStat(Key::ep_persist_vbstate_total,
                      epstats.totalPersistVBState);

    size_t memUsed = stats.getPreciseTotalMemoryUsed();
    collector.addStat(Key::mem_used, memUsed);
    collector.addStat(Key::mem_used_estimate,
                      stats.getEstimatedTotalMemoryUsed());
    collector.addStat(Key::ep_mem_low_wat_percent, stats.mem_low_wat_percent);
    collector.addStat(Key::ep_mem_high_wat_percent, stats.mem_high_wat_percent);
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
    collector.addStat(Key::ep_mem_tracker_enabled,
                      EPStats::isMemoryTrackingEnabled());
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

    size_t vbDeletions = epstats.vbucketDeletions.load();
    if (vbDeletions > 0) {
        collector.addStat(Key::ep_vbucket_del_max_walltime,
                          epstats.vbucketDelMaxWalltime);
        collector.addStat(Key::ep_vbucket_del_avg_walltime,
                          epstats.vbucketDelTotWalltime / vbDeletions);
    }

    size_t numBgOps = epstats.bgNumOperations.load();
    if (numBgOps > 0) {
        collector.addStat(Key::ep_bg_num_samples, epstats.bgNumOperations);
        collector.addStat(Key::ep_bg_min_wait, epstats.bgMinWait);
        collector.addStat(Key::ep_bg_max_wait, epstats.bgMaxWait);
        collector.addStat(Key::ep_bg_wait_avg, epstats.bgWait / numBgOps);
        collector.addStat(Key::ep_bg_min_load, epstats.bgMinLoad);
        collector.addStat(Key::ep_bg_max_load, epstats.bgMaxLoad);
        collector.addStat(Key::ep_bg_load_avg, epstats.bgLoad / numBgOps);
        collector.addStat(Key::ep_bg_wait, epstats.bgWait);
        collector.addStat(Key::ep_bg_load, epstats.bgLoad);
    }

    collector.addStat(Key::ep_degraded_mode, isDegradedMode());

    collector.addStat(Key::ep_num_access_scanner_runs, epstats.alogRuns);
    collector.addStat(Key::ep_num_access_scanner_skips,
                      epstats.accessScannerSkips);
    collector.addStat(Key::ep_access_scanner_last_runtime, epstats.alogRuntime);
    collector.addStat(Key::ep_access_scanner_num_items, epstats.alogNumItems);

    if (kvBucket->isAccessScannerEnabled() && epstats.alogTime.load() != 0)
    {
        std::array<char, 20> timestr;
        struct tm alogTim;
        hrtime_t alogTime = epstats.alogTime.load();
        if (cb_gmtime_r((time_t *)&alogTime, &alogTim) == -1) {
            collector.addStat(Key::ep_access_scanner_task_time, "UNKNOWN");
        } else {
            strftime(timestr.data(), 20, "%Y-%m-%d %H:%M:%S", &alogTim);
            collector.addStat(Key::ep_access_scanner_task_time, timestr.data());
        }
    } else {
        collector.addStat(Key::ep_access_scanner_task_time, "NOT_SCHEDULED");
    }

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

    collector.addStat(Key::ep_startup_time, startupTime.load());

    if (getConfiguration().getBucketType() == "persistent" &&
        getConfiguration().isWarmup()) {
        Warmup *wp = kvBucket->getWarmup();
        if (wp == nullptr) {
            throw std::logic_error("EPEngine::doEngineStats: warmup is NULL");
        }
        if (!kvBucket->isWarmingUp()) {
            collector.addStat(Key::ep_warmup_thread, "complete");
        } else {
            collector.addStat(Key::ep_warmup_thread, "running");
        }
        if (wp->getTime() > wp->getTime().zero()) {
            collector.addStat(
                    Key::ep_warmup_time,
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            wp->getTime())
                            .count());
        }
        collector.addStat(Key::ep_warmup_oom, epstats.warmOOM);
        collector.addStat(Key::ep_warmup_dups, epstats.warmDups);
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

    collector.addStat(Key::ep_defragmenter_num_visited,
                      epstats.defragNumVisited);
    collector.addStat(Key::ep_defragmenter_num_moved, epstats.defragNumMoved);
    collector.addStat(Key::ep_defragmenter_sv_num_moved,
                      epstats.defragStoredValueNumMoved);

    collector.addStat(Key::ep_item_compressor_num_visited,
                      epstats.compressorNumVisited);
    collector.addStat(Key::ep_item_compressor_num_compressed,
                      epstats.compressorNumCompressed);

    collector.addStat(Key::ep_cursor_dropping_lower_threshold,
                      epstats.cursorDroppingLThreshold);
    collector.addStat(Key::ep_cursor_dropping_upper_threshold,
                      epstats.cursorDroppingUThreshold);
    collector.addStat(Key::ep_cursors_dropped, epstats.cursorsDropped);
    collector.addStat(Key::ep_cursor_memory_freed, epstats.cursorMemoryFreed);

    doDiskFailureStats(collector);

    // Note: These are also reported per-shard in 'kvstore' stats, however
    // we want to be able to graph these over time, and hence need to expose
    // to ns_sever at the top-level.
    if(configuration.getBackend() == "couchdb"){
        doEngineStatsCouchDB(collector, epstats);
    } else if(configuration.getBackend() == "magma") {
        doEngineStatsMagma(collector);
    } else if(configuration.getBackend() == "rocksdb"){
        doEngineStatsRocksDB(collector);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doMemoryStats(
        const void* cookie, const AddStatFn& add_stat) {
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
    add_casted_stat("ep_max_size", stats.getMaxDataSize(), add_stat, cookie);
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
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVBucketStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const char* stat_key,
        int nkey,
        VBucketStatsDetailLevel detail) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(KVBucketIface* store,
                           const void* c,
                           AddStatFn a,
                           VBucketStatsDetailLevel detail)
            : eps(store), cookie(c), add_stat(std::move(a)), detail(detail) {
        }

        void visitBucket(const VBucketPtr& vb) override {
            addVBStats(cookie, add_stat, *vb, eps, detail);
        }

        static void addVBStats(const void* cookie,
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
        const void *cookie;
        AddStatFn add_stat;
        VBucketStatsDetailLevel detail;
    };

    if (getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
        return ENGINE_EWOULDBLOCK;
    }

    if (nkey > 16 && strncmp(stat_key, "vbucket-details", 15) == 0) {
        Expects(detail == VBucketStatsDetailLevel::Full);
        std::string vbid(&stat_key[16], nkey - 16);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return ENGINE_EINVAL;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return ENGINE_NOT_MY_VBUCKET;
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
            return ENGINE_EINVAL;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return ENGINE_NOT_MY_VBUCKET;
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
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashStats(
        const void* cookie, const AddStatFn& add_stat) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void* c,
                           AddStatFn a,
                           BucketCompressionMode compressMode)
            : cookie(c), add_stat(std::move(a)), compressionMode(compressMode) {
        }

        void visitBucket(const VBucketPtr& vb) override {
            Vbid vbid = vb->getId();
            std::array<char, 32> buf;
            try {
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:state", vbid.get());
                add_casted_stat(buf.data(),
                                VBucket::toString(vb->getState()),
                                add_stat,
                                cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
            }

            HashTableDepthStatVisitor depthVisitor;
            vb->ht.visitDepth(depthVisitor);

            try {
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:size", vbid.get());
                add_casted_stat(buf.data(), vb->ht.getSize(), add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:locks", vbid.get());
                add_casted_stat(
                        buf.data(), vb->ht.getNumLocks(), add_stat, cookie);
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
                                vb->ht.getNumInMemoryItems(),
                                add_stat,
                                cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:counted", vbid.get());
                add_casted_stat(
                        buf.data(), depthVisitor.size, add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:resized", vbid.get());
                add_casted_stat(
                        buf.data(), vb->ht.getNumResizes(), add_stat, cookie);
                checked_snprintf(
                        buf.data(), buf.size(), "vb_%d:mem_size", vbid.get());
                add_casted_stat(
                        buf.data(), vb->ht.getItemMemory(), add_stat, cookie);

                if (compressionMode != BucketCompressionMode::Off) {
                    checked_snprintf(buf.data(),
                                     buf.size(),
                                     "vb_%d:mem_size_uncompressed",
                                     vbid.get());
                    add_casted_stat(buf.data(),
                                    vb->ht.getUncompressedItemMemory(),
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
                                vb->ht.getNumSystemItems(),
                                add_stat,
                                cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
            }
        }

        const void *cookie;
        AddStatFn add_stat;
        BucketCompressionMode compressionMode;
    };

    StatVBucketVisitor svbv(cookie, add_stat, getCompressionMode());
    kvBucket->visit(svbv);

    return ENGINE_SUCCESS;
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
    AddStatsStream(std::string key, AddStatFn callback, const void* cookie)
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
    const void* cookie;
    std::stringbuf buf;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashDump(
        const void* cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != ENGINE_SUCCESS) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    as << result.vb->ht << std::endl;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointDump(
        const void* cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != ENGINE_SUCCESS) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    as << *result.vb->checkpointManager << std::endl;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDurabilityMonitorDump(
        const void* cookie,
        const AddStatFn& addStat,
        std::string_view keyArgs) {
    auto result = getValidVBucketFromString(keyArgs);
    if (result.status != ENGINE_SUCCESS) {
        return result.status;
    }

    AddStatsStream as(result.vb->getId().to_string(), addStat, cookie);
    result.vb->dumpDurabilityMonitor(as);
    as << std::endl;

    return ENGINE_SUCCESS;
}

class StatCheckpointVisitor : public VBucketVisitor {
public:
    StatCheckpointVisitor(KVBucketIface* kvs, const void* c, AddStatFn a)
        : kvBucket(kvs), cookie(c), add_stat(std::move(a)) {
    }

    void visitBucket(const VBucketPtr& vb) override {
        addCheckpointStat(cookie, add_stat, kvBucket, *vb);
    }

    static void addCheckpointStat(const void* cookie,
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

            auto result = eps->getLastPersistedCheckpointId(vbid);
            if (result.second) {
                checked_snprintf(buf.data(),
                                 buf.size(),
                                 "vb_%d:persisted_checkpoint_id",
                                 vbid.get());
                add_casted_stat(buf.data(), result.first, add_stat, cookie);
            }
        } catch (std::exception& error) {
            EP_LOG_WARN(
                    "StatCheckpointVisitor::addCheckpointStat: error building "
                    "stats: {}",
                    error.what());
        }
    }

    KVBucketIface* kvBucket;
    const void *cookie;
    AddStatFn add_stat;
};


class StatCheckpointTask : public GlobalTask {
public:
    StatCheckpointTask(EventuallyPersistentEngine* e,
                       const void* c,
                       AddStatFn a)
        : GlobalTask(e, TaskId::StatCheckpointTask, 0, false),
          ep(e),
          cookie(c),
          add_stat(std::move(a)) {
    }
    bool run() override {
        TRACE_EVENT0("ep-engine/task", "StatsCheckpointTask");
        StatCheckpointVisitor scv(ep->getKVBucket(), cookie, add_stat);
        ep->getKVBucket()->visit(scv);
        ep->notifyIOComplete(cookie, ENGINE_SUCCESS);
        return false;
    }

    std::string getDescription() override {
        return "checkpoint stats for all vbuckets";
    }

    std::chrono::microseconds maxExpectedDuration() override {
        // Task needed to lookup "checkpoint" stats; so the runtime should only
        // affects the particular stat request. However we don't want this to
        // take /too/ long, so set limit of 100ms.
        return std::chrono::milliseconds(100);
    }

private:
    EventuallyPersistentEngine *ep;
    const void *cookie;
    AddStatFn add_stat;
};
/// @endcond

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const char* stat_key,
        int nkey) {
    if (nkey == 10) {
        void* es = getEngineSpecific(cookie);
        if (es == nullptr) {
            ExTask task = std::make_shared<StatCheckpointTask>(
                    this, cookie, add_stat);
            ExecutorPool::get()->schedule(task);
            storeEngineSpecific(cookie, this);
            return ENGINE_EWOULDBLOCK;
        } else {
            storeEngineSpecific(cookie, nullptr);
        }
    } else if (nkey > 11) {
        std::string vbid(&stat_key[11], nkey - 11);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return ENGINE_EINVAL;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);
        if (!vb) {
            return ENGINE_NOT_MY_VBUCKET;
        }
        StatCheckpointVisitor::addCheckpointStat(
                cookie, add_stat, kvBucket.get(), *vb);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDurabilityMonitorStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const char* stat_key,
        int nkey) {
    const uint8_t size = 18; // size  of "durability-monitor"
    if (nkey == size) {
        // Case stat_key = "durability-monitor"
        // @todo: Return aggregated stats for all VBuckets.
        //     Implement as async, we don't what to block for too long.
        return ENGINE_ENOTSUP;
    } else if (nkey > size + 1) {
        // Case stat_key = "durability-monitor <vbid>"
        const uint16_t vbidPos = size + 1;
        std::string vbid_(&stat_key[vbidPos], nkey - vbidPos);
        uint16_t vbid(0);
        if (!parseUint16(vbid_.c_str(), &vbid)) {
            return ENGINE_EINVAL;
        }

        VBucketPtr vb = getVBucket(Vbid(vbid));
        if (!vb) {
            // @todo: I would return an error code, but just replicating the
            //     behaviour of other stats for now
            return ENGINE_SUCCESS;
        }
        vb->addDurabilityMonitorStats(add_stat, cookie);
    }

    return ENGINE_SUCCESS;
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
    ConnStatBuilder(const void* c, AddStatFn as, DcpStatsFilter filter)
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

    const void *cookie;
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

    } catch (std::exception& error) {
        EP_LOG_WARN("showConnAggStat: Failed to build stats: {}", error.what());
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doConnAggStats(
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
        showConnAggStat(connType,
                        counter,
                        collector);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDcpStats(
        const void* cookie, const AddStatFn& add_stat, std::string_view value) {
    ConnStatBuilder dcpVisitor(cookie, add_stat, DcpStatsFilter{value});
    dcpConnMap_->each(dcpVisitor);

    const auto& aggregator = dcpVisitor.getCounter();

    add_casted_stat("ep_dcp_count", aggregator.totalConns, add_stat, cookie);
    add_casted_stat("ep_dcp_producer_count", aggregator.totalProducers, add_stat, cookie);
    add_casted_stat("ep_dcp_total_bytes", aggregator.conn_totalBytes, add_stat, cookie);
    add_casted_stat("ep_dcp_total_uncompressed_data_size", aggregator.conn_totalUncompressedDataSize,
                    add_stat, cookie);
    add_casted_stat("ep_dcp_total_queue", aggregator.conn_queue,
                    add_stat, cookie);
    add_casted_stat("ep_dcp_queue_fill", aggregator.conn_queueFill,
                    add_stat, cookie);
    add_casted_stat("ep_dcp_items_sent", aggregator.conn_queueDrain,
                    add_stat, cookie);
    add_casted_stat("ep_dcp_items_remaining", aggregator.conn_queueRemaining,
                    add_stat, cookie);
    add_casted_stat("ep_dcp_num_running_backfills",
                    dcpConnMap_->getNumActiveSnoozingBackfills(), add_stat, cookie);
    add_casted_stat("ep_dcp_max_running_backfills",
                    dcpConnMap_->getMaxActiveSnoozingBackfills(), add_stat, cookie);

    dcpConnMap_->addStats(add_stat, cookie);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEvictionStats(
        const void* cookie, const AddStatFn& add_stat) {
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
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(
        const void* cookie,
        const AddStatFn& add_stat,
        Vbid vbid,
        const DocKey& key,
        bool validate) {
    auto rv = checkPrivilege(cookie, cb::rbac::Privilege::Read, key);
    if (rv != ENGINE_SUCCESS) {
        return rv;
    }

    std::unique_ptr<Item> it;
    struct key_stats kstats;

    if (fetchLookupResult(cookie, it)) {
        if (!validate) {
            EP_LOG_DEBUG(
                    "Found lookup results for non-validating key "
                    "stat call. Would have leaked");
            it.reset();
        }
    } else if (validate) {
        rv = kvBucket->statsVKey(key, vbid, cookie);
        if (rv == ENGINE_NOT_MY_VBUCKET || rv == ENGINE_KEY_ENOENT) {
            if (isDegradedMode()) {
                return ENGINE_TMPFAIL;
            }
        }
        return rv;
    }

    rv = kvBucket->getKeyStats(key, vbid, cookie, kstats, WantsDeleted::No);
    if (rv == ENGINE_SUCCESS) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVbIdFailoverLogStats(
        const void* cookie, const AddStatFn& add_stat, Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if(!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }
    vb->failovers->addStats(cookie, vb->getId(), add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doAllFailoverLogStats(
        const void* cookie, const AddStatFn& add_stat) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void* c, AddStatFn a)
            : cookie(c), add_stat(std::move(a)) {
        }

        void visitBucket(const VBucketPtr& vb) override {
            vb->failovers->addStats(cookie, vb->getId(), add_stat);
        }

    private:
        const void *cookie;
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

static std::string getTaskDescrForStats(TaskId id) {
    return std::string(GlobalTask::getTaskName(id)) + "[" +
           to_string(GlobalTask::getTaskType(id)) + "]";
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doSchedulerStats(
        const void* cookie, const AddStatFn& add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.schedulingHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doRunTimeStats(
        const void* cookie, const AddStatFn& add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.taskRuntimeHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDispatcherStats(
        const void* cookie, const AddStatFn& add_stat) {
    ExecutorPool::get()->doWorkerStat(
            ObjectRegistry::getCurrentEngine()->getTaskable(),
            cookie,
            add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doTasksStats(
        const void* cookie, const AddStatFn& add_stat) {
    ExecutorPool::get()->doTasksStat(
            ObjectRegistry::getCurrentEngine()->getTaskable(),
            cookie,
            add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doWorkloadStats(
        const void* cookie, const AddStatFn& add_stat) {
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

    return ENGINE_SUCCESS;
}

void EventuallyPersistentEngine::addSeqnoVbStats(const void* cookie,
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

void EventuallyPersistentEngine::addLookupResult(const void* cookie,
                                                 std::unique_ptr<Item> result) {
    LockHolder lh(lookupMutex);
    auto it = lookups.find(cookie);
    if (it != lookups.end()) {
        if (!it->second) {
            EP_LOG_DEBUG("Cleaning up old lookup result for '{}'",
                         it->second->getKey().data());
        } else {
            EP_LOG_DEBUG("Cleaning up old null lookup result");
        }
        lookups.erase(it);
    }
    lookups[cookie] = std::move(result);
}

bool EventuallyPersistentEngine::fetchLookupResult(const void* cookie,
                                                   std::unique_ptr<Item>& itm) {
    // This will return *and erase* the lookup result for a connection.
    // You look it up, you own it.
    LockHolder lh(lookupMutex);
    auto it = lookups.find(cookie);
    if (it != lookups.end()) {
        itm = std::move(it->second);
        lookups.erase(it);
        return true;
    } else {
        return false;
    }
}

EventuallyPersistentEngine::StatusAndVBPtr
EventuallyPersistentEngine::getValidVBucketFromString(std::string_view vbNum) {
    if (vbNum.empty()) {
        // Must specify a vbucket.
        return {ENGINE_EINVAL, {}};
    }
    uint16_t vbucket_id;
    if (!parseUint16(vbNum.data(), &vbucket_id)) {
        return {ENGINE_EINVAL, {}};
    }
    Vbid vbid = Vbid(vbucket_id);
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return {ENGINE_NOT_MY_VBUCKET, {}};
    }
    return {ENGINE_SUCCESS, vb};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doSeqnoStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const char* stat_key,
        int nkey) {
    if (getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
        return ENGINE_EWOULDBLOCK;
    }

    if (nkey > 14) {
        std::string value(stat_key + 14, nkey - 14);

        try {
            checkNumeric(value.c_str());
        } catch(std::runtime_error &) {
            return ENGINE_EINVAL;
        }

        Vbid vbucket(atoi(value.c_str()));
        VBucketPtr vb = getVBucket(vbucket);
        if (!vb || vb->getState() == vbucket_state_dead) {
            return ENGINE_NOT_MY_VBUCKET;
        }

        addSeqnoVbStats(cookie, add_stat, vb);

        return ENGINE_SUCCESS;
    }

    auto vbuckets = kvBucket->getVBuckets().getBuckets();
    for (auto vbid : vbuckets) {
        VBucketPtr vb = getVBucket(vbid);
        if (vb) {
            addSeqnoVbStats(cookie, add_stat, vb);
        }
    }
    return ENGINE_SUCCESS;
}

void EventuallyPersistentEngine::addLookupAllKeys(const void* cookie,
                                                  ENGINE_ERROR_CODE err) {
    LockHolder lh(lookupMutex);
    allKeysLookups[cookie] = err;
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCollectionStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const std::string& statKey) {
    CBStatCollector collector(add_stat, cookie, getServerApi());
    auto bucketCollector = collector.forBucket(getName());
    auto res = Collections::Manager::doCollectionStats(
            *kvBucket, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_collection ||
        res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie,
                                         res.getManifestId());
    }
    return ENGINE_ERROR_CODE(res.result);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doScopeStats(
        const void* cookie,
        const AddStatFn& add_stat,
        const std::string& statKey) {
    CBStatCollector collector(add_stat, cookie, getServerApi());
    auto bucketCollector = collector.forBucket(getName());
    auto res = Collections::Manager::doScopeStats(
            *kvBucket, bucketCollector, statKey);
    if (res.result == cb::engine_errc::unknown_scope) {
        setUnknownCollectionErrorContext(cookie,
                                         res.getManifestId());
    }
    return ENGINE_ERROR_CODE(res.result);
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
        auto scope = kvBucket->getCollectionsManager().getScopeID(cid);
        if (scope.second) {
            return cb::EngineErrorGetCollectionIDResult(
                    scope.first, scope.second.value(), cid);
        } else {
            return cb::EngineErrorGetCollectionIDResult(
                    cb::engine_errc::unknown_collection, scope.first);
        }
    }
    return cb::EngineErrorGetCollectionIDResult(
            cb::engine_errc::invalid_arguments);
}

std::tuple<ENGINE_ERROR_CODE,
           std::optional<Vbid>,
           std::optional<std::string>,
           std::optional<CollectionID>>
EventuallyPersistentEngine::parseStatKeyArg(const void* cookie,
                                            std::string_view statKeyPrefix,
                                            std::string_view statKey) {
    std::vector<std::string> args;
    std::string trimmedStatKey(statKey);
    boost::algorithm::trim(trimmedStatKey);
    boost::split(args, trimmedStatKey, boost::is_space());
    if (args.size() != 3 && args.size() != 4) {
        return {ENGINE_EINVAL, std::nullopt, std::nullopt, std::nullopt};
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
        return {ENGINE_EINVAL, std::nullopt, std::nullopt, std::nullopt};
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
            return {ENGINE_ERROR_CODE(cidResult.result),
                    std::nullopt,
                    std::nullopt,
                    std::nullopt};
        }
        cid = cidResult.getCollectionId();
    }

    return {ENGINE_SUCCESS, vbid, args[1], cid};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(
        const void* cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    ENGINE_ERROR_CODE status;
    std::optional<Vbid> vbid;
    std::optional<std::string> key;
    std::optional<CollectionID> cid;
    std::tie(status, vbid, key, cid) = parseStatKeyArg(cookie, "key", statKey);
    if (status != ENGINE_SUCCESS) {
        return status;
    }
    auto docKey = StoredDocKey(*key, *cid);
    return doKeyStats(cookie, add_stat, *vbid, docKey, false);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVKeyStats(
        const void* cookie,
        const AddStatFn& add_stat,
        std::string_view statKey) {
    ENGINE_ERROR_CODE status;
    std::optional<Vbid> vbid;
    std::optional<std::string> key;
    std::optional<CollectionID> cid;
    std::tie(status, vbid, key, cid) = parseStatKeyArg(cookie, "vkey", statKey);
    if (status != ENGINE_SUCCESS) {
        return status;
    }
    auto docKey = StoredDocKey(*key, *cid);
    return doKeyStats(cookie, add_stat, *vbid, docKey, true);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDcpVbTakeoverStats(
        const void* cookie,
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doFailoversStats(
        const void* cookie, const AddStatFn& add_stat, std::string_view key) {
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

    return ENGINE_KEY_ENOENT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDiskinfoStats(
        const void* cookie, const AddStatFn& add_stat, std::string_view key) {
    const std::string statKey(key.data(), key.size());
    if (key.size() == 8) {
        CBStatCollector collector{add_stat, cookie, getServerApi()};
        auto bucketC = collector.forBucket(getName());
        return kvBucket->getFileStats(bucketC);
    }
    if ((key.size() == 15) &&
        (statKey.compare(std::string("diskinfo").length() + 1,
                         std::string("detail").length(),
                         "detail") == 0)) {
        return kvBucket->getPerVBucketDiskStats(cookie, add_stat);
    }

    return ENGINE_EINVAL;
}

void EventuallyPersistentEngine::doDiskFailureStats(
        const BucketStatCollector& collector) {
    using namespace cb::stats;
    size_t value = 0;
    if (kvBucket->getKVStoreStat(
                "failure_compaction", value, KVBucketIface::KVSOption::BOTH)) {
        // Total data write failures is compaction failures plus commit failures
        auto writeFailure = value + stats.commitFailed;
        collector.addStat(Key::ep_data_write_failed, writeFailure);
    }
    if (kvBucket->getKVStoreStat(
                "failure_get", value, KVBucketIface::KVSOption::BOTH)) {
        collector.addStat(Key::ep_data_read_failed, value);
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doPrivilegedStats(
        const void* cookie, const AddStatFn& add_stat, std::string_view key) {
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

        return ENGINE_KEY_ENOENT;
    }
    return ENGINE_EACCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(
        const void* cookie,
        std::string_view key,
        std::string_view value,
        const AddStatFn& add_stat) {
    ScopeTimer2<HdrMicroSecStopwatch, TracerStopwatch> timer(
            std::forward_as_tuple(stats.getStatsCmdHisto),
            std::forward_as_tuple(cookie, cb::tracing::Code::GetStats));

    if (key.empty()) {
        EP_LOG_DEBUG("stats engine");
    } else {
        EP_LOG_DEBUG("stats {}", key);
    }

    // Some stats have been moved to using the stat collector interface,
    // while others have not. Depending on the key, this collector _may_
    // not be used, but creating it here reduces duplication (and it's not
    // expensive to create)
    CBStatCollector collector{add_stat, cookie, getServerApi()};
    auto bucketCollector = collector.forBucket(getName());

    if (key.empty()) {
        return doEngineStats(bucketCollector);
    }
    if (key.size() > 7 && cb_isPrefix(key, "dcpagg ")) {
        return doConnAggStats(bucketCollector, key.substr(7));
    }
    if (key == "dcp"sv) {
        return doDcpStats(cookie, add_stat, value);
    }
    if (key == "eviction"sv) {
        return doEvictionStats(cookie, add_stat);
    }
    if (key == "hash"sv) {
        return doHashStats(cookie, add_stat);
    }
    if (key == "vbucket"sv) {
        return doVBucketStats(cookie,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::State);
    }
    if (key == "prev-vbucket"sv) {
        return doVBucketStats(cookie,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::PreviousState);
    }
    if (cb_isPrefix(key, "vbucket-durability-state")) {
        return doVBucketStats(cookie,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::Durability);
    }
    if (cb_isPrefix(key, "vbucket-details")) {
        return doVBucketStats(cookie,
                              add_stat,
                              key.data(),
                              key.size(),
                              VBucketStatsDetailLevel::Full);
    }
    if (cb_isPrefix(key, "vbucket-seqno")) {
        return doSeqnoStats(cookie, add_stat, key.data(), key.size());
    }
    if (cb_isPrefix(key, "checkpoint")) {
        return doCheckpointStats(cookie, add_stat, key.data(), key.size());
    }
    if (cb_isPrefix(key, "durability-monitor")) {
        return doDurabilityMonitorStats(
                cookie, add_stat, key.data(), key.size());
    }
    if (key == "timings"sv) {
        doTimingStats(bucketCollector);
        return ENGINE_SUCCESS;
    }
    if (key == "dispatcher"sv) {
        return doDispatcherStats(cookie, add_stat);
    }
    if (key == "tasks"sv) {
        return doTasksStats(cookie, add_stat);
    }
    if (key == "scheduler"sv) {
        return doSchedulerStats(cookie, add_stat);
    }
    if (key == "runtimes"sv) {
        return doRunTimeStats(cookie, add_stat);
    }
    if (key == "memory"sv) {
        return doMemoryStats(cookie, add_stat);
    }
    if (key == "uuid"sv) {
        add_casted_stat("uuid", configuration.getUuid(), add_stat, cookie);
        return ENGINE_SUCCESS;
    }
    if (cb_isPrefix(key, "key ") || cb_isPrefix(key, "key-byid ")) {
        return doKeyStats(cookie, add_stat, key);
    }
    if (cb_isPrefix(key, "vkey ") || cb_isPrefix(key, "vkey-byid ")) {
        return doVKeyStats(cookie, add_stat, key);
    }
    if (key == "kvtimings"sv) {
        getKVBucket()->addKVStoreTimingStats(add_stat, cookie);
        return ENGINE_SUCCESS;
    }
    if (key.size() >= 7 && cb_isPrefix(key, "kvstore")) {
        std::string args(key.data() + 7, key.size() - 7);
        getKVBucket()->addKVStoreStats(add_stat, cookie, args);
        return ENGINE_SUCCESS;
    }
    if (key == "warmup"sv) {
        const auto* warmup = getKVBucket()->getWarmup();
        if (warmup != nullptr) {
            warmup->addStats(add_stat, cookie);
            return ENGINE_SUCCESS;
        }
        return ENGINE_KEY_ENOENT;
    }
    if (key == "info"sv) {
        add_casted_stat("info", get_stats_info(), add_stat, cookie);
        return ENGINE_SUCCESS;
    }
    if (key == "config"sv) {
        configuration.addStats(bucketCollector);
        return ENGINE_SUCCESS;
    }
    if (key.size() > 15 && cb_isPrefix(key, "dcp-vbtakeover")) {
        return doDcpVbTakeoverStats(cookie, add_stat, key);
    }
    if (key == "workload"sv) {
        return doWorkloadStats(cookie, add_stat);
    }
    if (cb_isPrefix(key, "failovers")) {
        return doFailoversStats(cookie, add_stat, key);
    }
    if (cb_isPrefix(key, "diskinfo")) {
        return doDiskinfoStats(cookie, add_stat, key);
    }
    if (cb_isPrefix(key, "collections")) {
        return doCollectionStats(
                cookie, add_stat, std::string(key.data(), key.size()));
    }
    if (cb_isPrefix(key, "scopes")) {
        return doScopeStats(
                cookie, add_stat, std::string(key.data(), key.size()));
    }
    if (cb_isPrefix(key, "disk-failures")) {
        doDiskFailureStats(bucketCollector);
        return ENGINE_SUCCESS;
    }
    if (key[0] == '_') {
        return doPrivilegedStats(cookie, add_stat, key);
    }

    // Unknown stat requested
    return ENGINE_KEY_ENOENT;
}

void EventuallyPersistentEngine::resetStats() {
    stats.reset();
    if (kvBucket) {
        kvBucket->resetUnderlyingStats();
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::checkPrivilege(
        const void* cookie, cb::rbac::Privilege priv, DocKey key) const {
    return ENGINE_ERROR_CODE(
            checkPrivilege(cookie, priv, key.getCollectionID()));
}

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        const void* cookie, cb::rbac::Privilege priv, CollectionID cid) const {
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
        setUnknownCollectionErrorContext(cookie,
                                         manifestUid);
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

cb::engine_errc EventuallyPersistentEngine::checkPrivilege(
        const void* cookie,
        cb::rbac::Privilege priv,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    try {
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
        const void* cookie,
        cb::rbac::Privilege priv,
        std::optional<ScopeID> sid,
        std::optional<CollectionID> cid) const {
    try {
        switch (serverApi->cookie->test_privilege(cookie, priv, sid, cid)
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
        const void* cookie) const {
    return serverApi->cookie->get_privilege_context_revision(cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe(
        const void* cookie,
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
            return ENGINE_EINVAL;
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
            return ENGINE_EINVAL;
        }

        DocKey key = makeDocKey(cookie, {data + offset, keylen});
        offset += keylen;
        EP_LOG_DEBUG("Observing key {} in {}",
                     cb::UserDataView(key.to_string()),
                     vb_id);

        auto rv = checkPrivilege(cookie, cb::rbac::Privilege::Read, key);
        if (rv != ENGINE_SUCCESS) {
            return rv;
        }

        // Get key stats
        uint16_t keystatus = 0;
        struct key_stats kstats = {};
        rv = kvBucket->getKeyStats(
                key, vb_id, cookie, kstats, WantsDeleted::Yes);
        if (rv == ENGINE_SUCCESS) {
            if (kstats.logically_deleted) {
                keystatus = OBS_STATE_LOGICAL_DEL;
            } else if (!kstats.dirty) {
                keystatus = OBS_STATE_PERSISTED;
            } else {
                keystatus = OBS_STATE_NOT_PERSISTED;
            }
        } else if (rv == ENGINE_KEY_ENOENT) {
            keystatus = OBS_STATE_NOT_FOUND;
        } else if (rv == ENGINE_NOT_MY_VBUCKET) {
            return ENGINE_NOT_MY_VBUCKET;
        } else if (rv == ENGINE_EWOULDBLOCK) {
            return rv;
        } else if (rv == ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS) {
            return rv;
        } else {
            return ENGINE_FAILED;
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe_seqno(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    Vbid vb_id = request.getVBucket();
    auto value = request.getValue();
    auto vb_uuid = static_cast<uint64_t>(
            ntohll(*reinterpret_cast<const uint64_t*>(value.data())));

    EP_LOG_DEBUG("Observing {} with uuid: {}", vb_id, vb_uuid);

    VBucketPtr vb = kvBucket->getVBucket(vb_id);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() == vbucket_state_dead) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    //Check if the vb uuid matches with the latest entry
    failover_entry_t entry = vb->failovers->getLatestEntry();
    std::stringstream result;

    if (vb_uuid != entry.vb_uuid) {
       uint64_t failover_highseqno = 0;
       uint64_t latest_uuid;
       bool found = vb->failovers->getLastSeqnoForUUID(vb_uuid, &failover_highseqno);
       if (!found) {
           return ENGINE_KEY_ENOENT;
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleLastClosedCheckpoint(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    VBucketPtr vb = getVBucket(request.getVBucket());
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    const uint64_t id =
            htonll(vb->checkpointManager->getLastClosedCheckpointId());
    return sendResponse(
            response,
            {}, // key
            {}, // extra
            {reinterpret_cast<const char*>(&id), sizeof(id)}, // body
            PROTOCOL_BINARY_RAW_BYTES,
            cb::mcbp::Status::Success,
            0,
            cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleCreateCheckpoint(
        const void* cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    VBucketPtr vb = getVBucket(req.getVBucket());

    if (!vb || vb->getState() != vbucket_state_active) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    // Create a new checkpoint, notifying flusher.
    const uint64_t checkpointId =
            htonll(vb->checkpointManager->createNewCheckpoint());
    getKVBucket()->wakeUpFlusher();
    const auto lastPersisted =
            kvBucket->getLastPersistedCheckpointId(vb->getId());

    if (lastPersisted.second) {
        const uint64_t persistedChkId = htonll(lastPersisted.first);
        std::array<char, sizeof(checkpointId) + sizeof(persistedChkId)> val;
        memcpy(val.data(), &checkpointId, sizeof(checkpointId));
        memcpy(val.data() + sizeof(checkpointId),
               &persistedChkId,
               sizeof(persistedChkId));
        return sendResponse(response,
                            {}, // key
                            {}, // extra
                            {val.data(), val.size()}, // body
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Success,
                            0,
                            cookie);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleCheckpointPersistence(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    auto vbucket = request.getVBucket();
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto status = cb::mcbp::Status::Success;
    auto extras = request.getExtdata();
    uint64_t chk_id;
    if (extras.size() == sizeof(chk_id)) {
        memcpy(&chk_id, extras.data(), sizeof(chk_id));
    } else {
        auto value = request.getValue();
        memcpy(&chk_id, value.data(), sizeof(chk_id));
    }

    chk_id = ntohll(chk_id);
    if (getEngineSpecific(cookie) == nullptr) {
        auto res = vb->checkAddHighPriorityVBEntry(
                chk_id, cookie, HighPriorityVBNotify::ChkPersistence);

        switch (res) {
        case HighPriorityVBReqStatus::RequestScheduled:
            storeEngineSpecific(cookie, this);
            // Wake up the flusher if it is idle.
            getKVBucket()->wakeUpFlusher();
            return ENGINE_EWOULDBLOCK;

        case HighPriorityVBReqStatus::NotSupported:
            status = cb::mcbp::Status::NotSupported;
            EP_LOG_WARN(
                    "EventuallyPersistentEngine::"
                    "handleCheckpointCmds(): "
                    "High priority async chk request "
                    "for {} is NOT supported",
                    vbucket);
            break;

        case HighPriorityVBReqStatus::RequestNotScheduled:
            // 'HighPriorityVBEntry' was not added, hence just
            // return success
            EP_LOG_INFO(
                    "EventuallyPersistentEngine::"
                    "handleCheckpointCmds(): "
                    "Did NOT add high priority async chk request "
                    "for {}",
                    vbucket);

            break;
        }
    } else {
        storeEngineSpecific(cookie, nullptr);
        EP_LOG_DEBUG("Checkpoint {} persisted for {}", chk_id, vbucket);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleSeqnoPersistence(
        const void* cookie,
        const cb::mcbp::Request& req,
        const AddResponseFn& response) {
    const Vbid vbucket = req.getVBucket();
    VBucketPtr vb = getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    auto status = cb::mcbp::Status::Success;
    auto extras = req.getExtdata();
    uint64_t seqno = ntohll(*reinterpret_cast<const uint64_t*>(extras.data()));

    if (getEngineSpecific(cookie) == nullptr) {
        auto persisted_seqno = vb->getPersistenceSeqno();
        if (seqno > persisted_seqno) {
            auto res = vb->checkAddHighPriorityVBEntry(
                    seqno, cookie, HighPriorityVBNotify::Seqno);

            switch (res) {
            case HighPriorityVBReqStatus::RequestScheduled:
                storeEngineSpecific(cookie, this);
                return ENGINE_EWOULDBLOCK;

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
        storeEngineSpecific(cookie, nullptr);
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
        const void* cookie, const DocKey& key, Vbid vbucket) {
    uint32_t deleted;
    uint8_t datatype;
    ItemMetaData itemMeta;
    ENGINE_ERROR_CODE ret = kvBucket->getMetaData(
            key, vbucket, cookie, itemMeta, deleted, datatype);

    item_info metadata;

    if (ret == ENGINE_SUCCESS) {
        metadata = to_item_info(itemMeta, datatype, deleted);
    } else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            ret = ENGINE_TMPFAIL;
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
        const void* cookie,
        protocol_binary_datatype_t datatype,
        std::string_view body) {
    if (!isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_JSON)) {
        // JSON check the body if xattr's are enabled
        if (mcbp::datatype::is_xattr(datatype)) {
            body = cb::xattr::get_body(body);
        }

        if (checkUTF8JSON(reinterpret_cast<const uint8_t*>(body.data()),
                          body.size())) {
            datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
        }
    }
    return datatype;
}

DocKey EventuallyPersistentEngine::makeDocKey(const void* cookie,
                                              cb::const_byte_buffer key) {
    return DocKey{key.data(),
                  key.size(),
                  isCollectionsSupported(cookie)
                          ? DocKeyEncodesCollectionId::Yes
                          : DocKeyEncodesCollectionId::No};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    const auto extras = request.getExtdata();

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    if (!decodeSetWithMetaOptions(
                extras, generateCas, checkConflicts, permittedVBStates)) {
        return ENGINE_EINVAL;
    }

    auto value = request.getValue();
    cb::const_byte_buffer emd;
    extractNmetaFromExtras(emd, value, extras);

    std::chrono::steady_clock::time_point startTime;
    {
        void* startTimeC = getEngineSpecific(cookie);
        if (startTimeC) {
            startTime = std::chrono::steady_clock::time_point(
                    std::chrono::steady_clock::duration(
                            *(static_cast<hrtime_t*>(startTimeC))));
            // Release the allocated memory and store nullptr to avoid
            // memory leak in an error path
            cb_free(startTimeC);
            storeEngineSpecific(cookie, nullptr);
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
    ENGINE_ERROR_CODE ret;
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
        return ENGINE_ENOMEM;
    }

    if (ret == ENGINE_SUCCESS) {
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Modify);
        ++stats.numOpsSetMeta;
        auto endTime = std::chrono::steady_clock::now();
        auto& traceable = *cookie2traceable(cookie);
        if (traceable.isTracingEnabled()) {
            NonBucketAllocationGuard guard;
            auto& tracer = traceable.getTracer();
            auto spanid = tracer.begin(Code::SetWithMeta, startTime);
            tracer.end(spanid, endTime);
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime);
        stats.setWithMetaHisto.add(elapsed);

        cas = commandCas;
    } else if (ret == ENGINE_ENOMEM) {
        return memoryCondition();
    } else if (ret == ENGINE_EWOULDBLOCK) {
        ++stats.numOpsGetMetaOnSetWithMeta;
        auto* startTimeC = cb_malloc(sizeof(hrtime_t));
        memcpy(startTimeC, &startTime, sizeof(hrtime_t));
        storeEngineSpecific(cookie, startTimeC);
        return ret;
    } else {
        // Let the framework generate the error message
        return ret;
    }

    if (opcode == cb::mcbp::ClientOpcode::SetqWithMeta ||
        opcode == cb::mcbp::ClientOpcode::AddqWithMeta) {
        // quiet ops should not produce output
        return ENGINE_SUCCESS;
    }

    if (isMutationExtrasSupported(cookie)) {
        return sendMutationExtras(response,
                                  request.getVBucket(),
                                  bySeqno,
                                  cb::mcbp::Status::Success,
                                  cas,
                                  cookie);
    }
    return sendErrorResponse(response, cb::mcbp::Status::Success, cas, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(
        Vbid vbucket,
        DocKey key,
        cb::const_byte_buffer value,
        ItemMetaData itemMeta,
        bool isDeleted,
        protocol_binary_datatype_t datatype,
        uint64_t& cas,
        uint64_t* seqno,
        const void* cookie,
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
        if (extendedMetaData->getStatus() == ENGINE_EINVAL) {
            setErrorContext(cookie, "Invalid extended metadata");
            return ENGINE_EINVAL;
        }
    }

    if (mcbp::datatype::is_snappy(datatype) &&
        !isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY)) {
        setErrorContext(cookie, "Client did not negotiate Snappy support");
        return ENGINE_EINVAL;
    }

    std::string_view payload(reinterpret_cast<const char*>(value.data()),
                             value.size());

    cb::const_byte_buffer finalValue = value;
    protocol_binary_datatype_t finalDatatype = datatype;
    cb::compression::Buffer uncompressedValue;

    cb::const_byte_buffer inflatedValue = value;
    protocol_binary_datatype_t inflatedDatatype = datatype;

    if (mcbp::datatype::is_snappy(datatype)) {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload, uncompressedValue)) {
            setErrorContext(cookie, "Failed to inflate document");
            return ENGINE_EINVAL;
        }

        inflatedValue = uncompressedValue;
        inflatedDatatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;

        if (compressionMode == BucketCompressionMode::Off ||
            uncompressedValue.size() < value.size()) {
            // If the inflated version version is smaller than the compressed
            // version we should keep it inflated
            finalValue = uncompressedValue;
            finalDatatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
        }
    }

    size_t system_xattr_size = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
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
                            ") exceeds the max limit for system xattrs: " +
                            std::to_string(cb::limits::PrivilegedBytes));
            return ENGINE_EINVAL;
        }
    }

    const auto valuesize = inflatedValue.size();
    if ((valuesize - system_xattr_size) > maxItemSize) {
        EP_LOG_WARN(
                "Item value size {} for setWithMeta is bigger than the max "
                "size {} allowed!!!",
                inflatedValue.size(),
                maxItemSize);

        return ENGINE_E2BIG;
    }

    finalDatatype = checkForDatatypeJson(cookie, finalDatatype,
                        mcbp::datatype::is_snappy(datatype) ?
                        uncompressedValue : payload);

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
                                     cookie,
                                     permittedVBStates,
                                     checkConflicts,
                                     allowExisting,
                                     genBySeqno,
                                     genCas,
                                     extendedMetaData.get());

    if (ret == ENGINE_SUCCESS) {
        cas = item->getCas();
    } else {
        cas = 0;
    }
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteWithMeta(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
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
        return ENGINE_EINVAL;
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
    ENGINE_ERROR_CODE ret;
    try {
        // MB-37374: Accept user-xattrs, body is still invalid
        auto datatype = uint8_t(request.getDatatype());
        cb::compression::Buffer uncompressedValue;
        if (mcbp::datatype::is_snappy(datatype)) {
            if (!cb::compression::inflate(
                        cb::compression::Algorithm::Snappy,
                        {reinterpret_cast<const char*>(value.data()),
                         value.size()},
                        uncompressedValue)) {
                setErrorContext(cookie, "Failed to inflate data");
                return ENGINE_EINVAL;
            }
            value = uncompressedValue;
            datatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
        }

        if (allowSanitizeValueInDeletion) {
            if (mcbp::datatype::is_xattr(datatype)) {
                // Whatever we have in the value, just keep Xattrs
                const auto valBuffer = std::string_view{
                        reinterpret_cast<const char*>(value.data()),
                        value.size()};
                value = {value.data(), cb::xattr::get_body_offset(valBuffer)};
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
                return ENGINE_EINVAL;
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
        return ENGINE_ENOMEM;
    }

    if (ret == ENGINE_SUCCESS) {
        ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
        guardedIface.audit_document_access(
                cookie, cb::audit::document::Operation::Delete);
        stats.numOpsDelMeta++;
    } else if (ret == ENGINE_ENOMEM) {
        return memoryCondition();
    } else {
        return ret;
    }

    if (request.getClientOpcode() == cb::mcbp::ClientOpcode::DelqWithMeta) {
        return ENGINE_SUCCESS;
    }

    if (isMutationExtrasSupported(cookie)) {
        return sendMutationExtras(response,
                                  request.getVBucket(),
                                  bySeqno,
                                  cb::mcbp::Status::Success,
                                  cas,
                                  cookie);
    }

    return sendErrorResponse(response, cb::mcbp::Status::Success, cas, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteWithMeta(
        Vbid vbucket,
        DocKey key,
        ItemMetaData itemMeta,
        uint64_t& cas,
        uint64_t* seqno,
        const void* cookie,
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
        if (extendedMetaData->getStatus() == ENGINE_EINVAL) {
            setErrorContext(cookie, "Invalid extended metadata");
            return ENGINE_EINVAL;
        }
    }

    return kvBucket->deleteWithMeta(key,
                                    cas,
                                    seqno,
                                    vbucket,
                                    cookie,
                                    permittedVBStates,
                                    checkConflicts,
                                    itemMeta,
                                    genBySeqno,
                                    genCas,
                                    0 /*bySeqno*/,
                                    extendedMetaData.get(),
                                    deleteSource);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleTrafficControlCmd(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::EnableTraffic:
        if (kvBucket->isWarmingUp()) {
            // engine is still warming up, do not turn on data traffic yet
            setErrorContext(cookie, "Persistent engine is still warming up!");
            return ENGINE_TMPFAIL;
        } else if (configuration.isFailpartialwarmup() &&
                   kvBucket->isWarmupOOMFailure()) {
            // engine has completed warm up, but data traffic cannot be
            // turned on due to an OOM failure
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to out of memory failures during warmup");
            return ENGINE_ENOMEM;
        } else {
            if (enableTraffic(true)) {
                EP_LOG_INFO(
                        "EventuallyPersistentEngine::handleTrafficControlCmd() "
                        "Data traffic to persistence engine is enabled");
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
    return kvBucket->isWarmingUp() || !trafficEnabled.load();
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::doDcpVbTakeoverStats(const void* cookie,
                                                 const AddStatFn& add_stat,
                                                 std::string& key,
                                                 Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
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
        return ENGINE_SUCCESS;
    }

    auto producer = std::dynamic_pointer_cast<DcpProducer>(conn);
    if (producer) {
        producer->addTakeoverStats(add_stat, cookie, *vb);
    } else {
        /**
          * There is not a legitimate case where a connection is not a
          * DcpProducer.  But just in case it does happen log the event and
          * return ENGINE_KEY_ENOENT.
          */
        EP_LOG_WARN(
                "doDcpVbTakeoverStats: connection {} for "
                "{} is not a DcpProducer",
                dcpName,
                vbid);
        return ENGINE_KEY_ENOENT;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::returnMeta(const void* cookie,
                                       const cb::mcbp::Request& req,
                                       const AddResponseFn& response) {
    using cb::mcbp::request::ReturnMetaPayload;
    using cb::mcbp::request::ReturnMetaType;

    auto* payload =
            reinterpret_cast<const ReturnMetaPayload*>(req.getExtdata().data());

    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    auto cas = req.getCas();
    auto datatype = uint8_t(req.getDatatype());
    auto mutate_type = payload->getMutationType();
    auto flags = payload->getFlags();
    auto exp = payload->getExpiration();
    if (exp != 0) {
        exp = ep_abs_time(ep_reltime(exp));
    }

    uint64_t seqno;
    ENGINE_ERROR_CODE ret;
    if (mutate_type == ReturnMetaType::Set ||
        mutate_type == ReturnMetaType::Add) {
        auto value = req.getValue();
        datatype = checkForDatatypeJson(
                cookie,
                datatype,
                {reinterpret_cast<const char*>(value.data()), value.size()});

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
            ret = kvBucket->set(*itm, cookie, {});
        } else {
            ret = kvBucket->add(*itm, cookie);
        }
        if (ret == ENGINE_SUCCESS) {
            ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
            guardedIface.audit_document_access(
                    cookie, cb::audit::document::Operation::Modify);
            ++stats.numOpsSetRetMeta;
        }
        cas = itm->getCas();
        seqno = htonll(itm->getRevSeqno());
    } else if (mutate_type == ReturnMetaType::Del) {
        ItemMetaData itm_meta;
        mutation_descr_t mutation_descr;
        ret = kvBucket->deleteItem(makeDocKey(cookie, req.getKey()),
                                   cas,
                                   req.getVBucket(),
                                   cookie,
                                   {},
                                   &itm_meta,
                                   mutation_descr);
        if (ret == ENGINE_SUCCESS) {
            ServerDocumentIfaceBorderGuard guardedIface(*serverApi->document);
            guardedIface.audit_document_access(
                    cookie, cb::audit::document::Operation::Delete);
            ++stats.numOpsDelRetMeta;
        }
        flags = itm_meta.flags;
        exp = gsl::narrow<uint32_t>(itm_meta.exptime);
        cas = itm_meta.cas;
        seqno = htonll(itm_meta.revSeqno);
    } else {
        throw std::runtime_error(
                "returnMeta: Unknown mode passed though the validator");
    }

    if (ret != ENGINE_SUCCESS) {
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

ENGINE_ERROR_CODE
EventuallyPersistentEngine::getAllKeys(const void* cookie,
                                       const cb::mcbp::Request& request,
                                       const AddResponseFn& response) {
    if (!getKVBucket()->isGetAllKeysSupported()) {
        return ENGINE_ENOTSUP;
    }

    {
        LockHolder lh(lookupMutex);
        auto it = allKeysLookups.find(cookie);
        if (it != allKeysLookups.end()) {
            ENGINE_ERROR_CODE err = it->second;
            allKeysLookups.erase(it);
            return err;
        }
    }

    VBucketPtr vb = getVBucket(request.getVBucket());
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    folly::SharedMutex::ReadHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return ENGINE_NOT_MY_VBUCKET;
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
    if (privTestResult != ENGINE_SUCCESS) {
        return privTestResult;
    }

    std::optional<CollectionID> keysCollection;
    if (isCollectionsSupported(cookie)) {
        keysCollection = start_key.getCollectionID();
    }

    ExTask task = std::make_shared<FetchAllKeysTask>(this,
                                                     cookie,
                                                     response,
                                                     start_key,
                                                     request.getVBucket(),
                                                     count,
                                                     keysCollection);
    ExecutorPool::get()->schedule(task);
    return ENGINE_EWOULDBLOCK;
}

ConnectionPriority EventuallyPersistentEngine::getDCPPriority(
        const void* cookie) {
    NonBucketAllocationGuard guard;
    auto priority = serverApi->cookie->get_priority(cookie);
    return priority;
}

void EventuallyPersistentEngine::setDCPPriority(const void* cookie,
                                                ConnectionPriority priority) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_priority(cookie, priority);
}

void EventuallyPersistentEngine::notifyIOComplete(const void* cookie,
                                                  ENGINE_ERROR_CODE status) {
    if (cookie == nullptr) {
        EP_LOG_WARN("Tried to signal a NULL cookie!");
    } else {
        HdrMicroSecBlockTimer bt(&stats.notifyIOHisto);
        NonBucketAllocationGuard guard;
        serverApi->cookie->notify_io_complete(cookie, status);
    }
}

void EventuallyPersistentEngine::scheduleDcpStep(
        gsl::not_null<const void*> cookie) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->scheduleDcpStep(cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getRandomKey(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    CollectionID cid{CollectionID::Default};

    if (request.getExtlen()) {
        const auto& payload =
                reinterpret_cast<const cb::mcbp::request::GetRandomKeyPayload&>(
                        *request.getExtdata().data());
        cid = payload.getCollectionId();
    }

    auto priv = checkPrivilege(cookie, cb::rbac::Privilege::Read, cid);
    if (priv != cb::engine_errc::success) {
        return ENGINE_ERROR_CODE(priv);
    }

    GetValue gv(kvBucket->getRandomKey(cid, cookie));
    ENGINE_ERROR_CODE ret = gv.getStatus();

    if (ret == ENGINE_SUCCESS) {
        Item* it = gv.item.get();
        uint32_t flags = it->getFlags();
        ret = sendResponse(
                response,
                isCollectionsSupported(cookie)
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::dcpOpen(
        const void* cookie,
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
                EP_LOG_WARN(
                        "Cannot open a DCP connection with PiTR as the "
                        "underlying kvstore don't support historical "
                        "snapshots");
                return ENGINE_DISCONNECT;
            }
        }
        handler = dcpConnMap_->newProducer(cookie, connName, flags);
    } else {
        // dont accept dcp consumer open requests during warm up
        if (!kvBucket->isWarmingUp()) {
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
            EP_LOG_WARN(
                    "EventuallyPersistentEngine::dcpOpen: not opening new DCP "
                    "Consumer handler as EPEngine is still warming up");
            return ENGINE_TMPFAIL;
        }
    }

    if (handler == nullptr) {
        EP_LOG_WARN("EPEngine::dcpOpen: failed to create a handler");
        return ENGINE_DISCONNECT;
    }

    // Success creating dcp object which has stored the cookie, now reserve it.
    reserveCookie(cookie);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::dcpAddStream(const void* cookie,
                                                           uint32_t opaque,
                                                           Vbid vbucket,
                                                           uint32_t flags) {
    return dcpConnMap_->addPassiveStream(
            getConnHandler(cookie), opaque, vbucket, flags);
}

ConnHandler* EventuallyPersistentEngine::tryGetConnHandler(const void* cookie) {
    auto* iface = getDcpConnHandler(cookie);
    if (iface) {
        auto* handler = dynamic_cast<ConnHandler*>(iface);
        if (handler) {
            return handler;
        }
        throw std::logic_error(
                "EventuallyPersistentEngine::tryGetConnHandler(): The "
                "registered "
                "connection handler is not a ConnHandler");
    }

    return nullptr;
}
ConnHandler& EventuallyPersistentEngine::getConnHandler(const void* cookie) {
    auto* handle = tryGetConnHandler(cookie);
    Expects(handle);
    return *handle;
}

void EventuallyPersistentEngine::handleDisconnect(const void *cookie) {
    dcpConnMap_->disconnect(cookie);
    /**
     * Decrement session_cas's counter, if the connection closes
     * before a control command (that returned ENGINE_EWOULDBLOCK
     * the first time) makes another attempt.
     *
     * Commands to be considered: DEL_VBUCKET, COMPACT_DB
     */
    if (getEngineSpecific(cookie) != nullptr) {
        switch (getOpcodeIfEwouldblockSet(cookie)) {
        case cb::mcbp::ClientOpcode::DelVbucket:
        case cb::mcbp::ClientOpcode::CompactDb: {
            decrementSessionCtr();
            storeEngineSpecific(cookie, nullptr);
            break;
        }
            default:
                break;
            }
    }
}

void EventuallyPersistentEngine::initiate_shutdown() {
    auto eng = acquireEngine(this);
    EP_LOG_INFO(
            "Shutting down all DCP connections in "
            "preparation for bucket deletion.");
    dcpConnMap_->shutdownAllConnections();
}

void EventuallyPersistentEngine::cancel_all_operations_in_ewb_state() {
    auto eng = acquireEngine(this);
    kvBucket->releaseRegisteredSyncWrites();
}

cb::mcbp::Status EventuallyPersistentEngine::stopFlusher(const char** msg,
                                                         size_t* msg_size) {
    (void)msg_size;
    auto rv = cb::mcbp::Status::Success;
    *msg = nullptr;
    if (!kvBucket->pauseFlusher()) {
        EP_LOG_DEBUG("Unable to stop flusher");
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
        EP_LOG_DEBUG("Unable to start flusher");
        *msg = "Flusher not shut down.";
        rv = cb::mcbp::Status::Einval;
    }
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteVBucket(
        Vbid vbucket, bool waitForCompletion, const void* cookie) {
    ENGINE_ERROR_CODE status = ENGINE_SUCCESS;
    if (getKVBucket()->maybeWaitForVBucketWarmup(cookie)) {
        return ENGINE_EWOULDBLOCK;
    }

    void* es = getEngineSpecific(cookie);
    if (waitForCompletion) {
        if (es == nullptr) {
            status = kvBucket->deleteVBucket(vbucket, cookie);
        } else {
            storeEngineSpecific(cookie, nullptr);
            EP_LOG_DEBUG("Completed sync deletion of {}", vbucket);
            status = ENGINE_SUCCESS;
        }
    } else {
        status = kvBucket->deleteVBucket(vbucket);
    }

    switch (status) {
    case ENGINE_SUCCESS:
        EP_LOG_INFO("Deletion of {} was completed.", vbucket);
        break;

    case ENGINE_NOT_MY_VBUCKET:
        EP_LOG_WARN(
                "Deletion of {} failed because the vbucket doesn't exist!!!",
                vbucket);
        break;
    case ENGINE_EINVAL:
        EP_LOG_WARN(
                "Deletion of {} failed "
                "because the vbucket is not in a dead state",
                vbucket);
        setErrorContext(
                cookie,
                "Failed to delete vbucket.  Must be in the dead state.");
        break;
    case ENGINE_EWOULDBLOCK:
        EP_LOG_INFO(
                "Request for {} deletion is in"
                " EWOULDBLOCK until the database file is removed from disk",
                vbucket);
        // We don't use the actual value in ewouldblock, just the existence
        // of something there.
        storeEngineSpecific(cookie, static_cast<void*>(this));
        break;
    default:
        EP_LOG_WARN("Deletion of {} failed because of unknown reasons",
                    vbucket);
        setErrorContext(cookie, "Failed to delete vbucket.  Unknown reason.");
        status = ENGINE_FAILED;
        break;
    }
    return status;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::scheduleCompaction(
        Vbid vbid, const CompactionConfig& c, const void* cookie) {
    return kvBucket->scheduleCompaction(
            vbid, c, cookie, std::chrono::seconds(0));
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getAllVBucketSequenceNumbers(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    static_assert(sizeof(RequestedVBState) == 4,
                  "Unexpected size for RequestedVBState");
    auto extras = request.getExtdata();

    // By default allow any alive states. If reqState has been set then
    // filter on that specific state.
    auto reqState = aliveVBStates;
    std::optional<CollectionID> reqCollection = {};

    // if extlen is non-zero, it limits the result to either only include the
    // vbuckets in the specified vbucket state, or in the specified vbucket
    // state and for the specified collection ID.
    if (!extras.empty()) {
        auto rawState = ntohl(*reinterpret_cast<const uint32_t*>(
                extras.substr(0, sizeof(vbucket_state_t)).data()));

        // If the received vbucket_state isn't 0 (i.e. all alive states) then
        // set the specifically requested states.
        auto desired = static_cast<RequestedVBState>(rawState);
        if (desired != RequestedVBState::Alive) {
            reqState = PermittedVBStates(static_cast<vbucket_state_t>(desired));
        }

        // Only attempt to decode this payload if collections are enabled, i.e.
        // the developer-preview, let's be extra defensive about people
        // assuming this encoding is here.
        if (extras.size() ==
                    (sizeof(RequestedVBState) + sizeof(CollectionIDType)) &&
            serverApi->core->isCollectionsEnabled()) {
            reqCollection = static_cast<CollectionIDType>(
                    ntohl(*reinterpret_cast<const uint32_t*>(
                            extras.substr(sizeof(RequestedVBState),
                                          sizeof(CollectionIDType))
                                    .data())));
        } else if (extras.size() != sizeof(RequestedVBState)) {
            return ENGINE_EINVAL;
        }
    }

    // If the client ISN'T talking collections, we should just give them the
    // high seqno of the default collection as that's all they should be aware
    // of.
    // If the client IS talking collections but hasn't specified a collection,
    // we'll give them the actual vBucket high seqno.
    if (!serverApi->cookie->is_collections_supported(cookie) &&
        serverApi->core->isCollectionsEnabled()) {
        reqCollection = CollectionID::Default;
    }

    // Privilege check either for the collection or the bucket
    auto accessStatus = cb::engine_errc::success;
    if (reqCollection) {
        // This will do the scope lookup
        accessStatus = checkPrivilege(
                cookie, cb::rbac::Privilege::MetaRead, reqCollection.value());
    } else {
        // Do a bucket privilege check
        accessStatus =
                checkPrivilege(cookie, cb::rbac::Privilege::MetaRead, {}, {});
    }
    if (accessStatus != cb::engine_errc::success) {
        return ENGINE_ERROR_CODE(accessStatus);
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
        return ENGINE_ENOMEM;
    }

    for (auto id : vbuckets) {
        VBucketPtr vb = getVBucket(id);
        if (vb) {
            if (!reqState.test(vb->getState())) {
                continue;
            }

            Vbid vbid = id.hton();
            uint64_t highSeqno;

            if (reqCollection) {
                // The collection may not exist in any given vBucket.
                // Check this instead of throwing and catching an
                // exception.
                auto handle = vb->lockCollections();
                if (handle.exists(reqCollection.value())) {
                    highSeqno = htonll(handle.getHighSeqno(*reqCollection));
                } else {
                    // If the collection doesn't exist in this
                    // vBucket, return nothing for this vBucket by
                    // not adding anything to the payload during this
                    // iteration.
                    continue;
                }
            } else {
                if (vb->getState() == vbucket_state_active) {
                    highSeqno = supportsSyncWrites ? vb->getHighSeqno()
                                                   : vb->getMaxVisibleSeqno();
                } else {
                    highSeqno =
                            supportsSyncWrites
                                    ? vb->checkpointManager->getSnapshotInfo()
                                              .range.getEnd()
                                    : vb->checkpointManager
                                              ->getVisibleSnapshotEndSeqno();
                }
                highSeqno = htonll(highSeqno);
            }
            auto offset = payload.size();
            payload.resize(offset + sizeof(vbid) + sizeof(highSeqno));
            memcpy(payload.data() + offset, &vbid, sizeof(vbid));
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

void EventuallyPersistentEngine::updateDcpMinCompressionRatio(float value) {
    if (dcpConnMap_) {
        dcpConnMap_->updateMinCompressionRatioForProducers(value);
    }
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
ENGINE_ERROR_CODE EventuallyPersistentEngine::sendErrorResponse(
        const AddResponseFn& response,
        cb::mcbp::Status status,
        uint64_t cas,
        const void* cookie) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::sendMutationExtras(
        const AddResponseFn& response,
        Vbid vbucket,
        uint64_t bySeqno,
        cb::mcbp::Status status,
        uint64_t cas,
        const void* cookie) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::setVBucketState(
        const void* cookie,
        const AddResponseFn& response,
        Vbid vbid,
        vbucket_state_t to,
        const nlohmann::json* meta,
        TransferVB transfer,
        uint64_t cas) {
    auto status = kvBucket->setVBucketState(vbid, to, meta, transfer, cookie);

    if (status == ENGINE_EWOULDBLOCK) {
        return status;
    } else if (status == ENGINE_ERANGE) {
        setErrorContext(cookie, "VBucket number too big");
    }

    return sendResponse(response,
                        {}, // key
                        {}, // extra
                        {}, // body
                        PROTOCOL_BINARY_RAW_BYTES,
                        serverApi->cookie->engine_error2mcbp(cookie, status),
                        cas,
                        cookie);
}

EventuallyPersistentEngine::~EventuallyPersistentEngine() {
    if (kvBucket) {
        auto tasks = kvBucket->deinitialize();
        // Need to reset the kvBucket as we need our ThreadLocal engine ptr to
        // be valid when destructing Items in CheckpointManagers but we need to
        // reset it before destructing EPStats.
        kvBucket.reset();

        // Ensure tasks are all completed and deleted. This loop keeps checking
        // each task in-turn, rather than spin and wait for each task. This is
        // to be more defensive against deadlock caused by a task referencing
        // another
        EP_LOG_INFO("~EventuallyPersistentEngine: will wait for {} tasks",
                    tasks.size());
        waitForTasks(tasks);
    }
    EP_LOG_INFO("~EPEngine: Completed deinitialize.");
    delete workload;
    delete checkpointConfig;

    // Engine going away, tell ArenaMalloc to unregister
    cb::ArenaMalloc::unregisterClient(arena);
    // Ensure the soon to be invalid engine is no longer in the ObjectRegistry
    ObjectRegistry::onSwitchThread(nullptr);

    /* Unique_ptr(s) are deleted in the reverse order of the initialization */
}

void EventuallyPersistentEngine::waitForTasks(std::vector<ExTask>& tasks) {
    bool taskStillRunning = !tasks.empty();

    while (taskStillRunning) {
        taskStillRunning = false;
        for (auto& task : tasks) {
            if (task && task.use_count() == 1) {
                EP_LOG_DEBUG(
                        "EventuallyPersistentEngine::waitForTasks: RESET {}",
                        task->getDescription());
                task.reset();
            } else if (task) {
                EP_LOG_DEBUG(
                        "EventuallyPersistentEngine::waitForTasks: yielding "
                        "use_count:{} "
                        "for:{}",
                        task.use_count(),
                        task->getDescription());
                std::this_thread::yield();
                taskStillRunning = true;
            }
        }
    }
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

void EpEngineTaskable::logQTime(
        TaskId id, const std::chrono::steady_clock::duration enqTime) {
    myEngine->getKVBucket()->logQTime(id, enqTime);
}

void EpEngineTaskable::logRunTime(
        TaskId id, const std::chrono::steady_clock::duration runTime) {
    myEngine->getKVBucket()->logRunTime(id, runTime);
}
bool EpEngineTaskable::isShutdown() {
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
    stats.setMaxDataSize(size); // Set first because following code may read

    // Setting the quota must set the water-marks and the new water-mark values
    // must be readable from both the configuration and EPStats. The following
    // is also updating EPStats because the configuration has a change listener
    // that will update EPStats
    configuration.setMemLowWat(percentOf(size, stats.mem_low_wat_percent));
    configuration.setMemHighWat(percentOf(size, stats.mem_high_wat_percent));

    getDcpConnMap().updateMaxActiveSnoozingBackfills(size);
    getKVBucket()->setCursorDroppingLowerUpperThresholds(size);
    getDcpConnMap().updateMaxActiveSnoozingBackfills(size);
    // Pass the max bucket quota size down to the storage layer.
    for (uint16_t ii = 0; ii < getKVBucket()->getVBuckets().getNumShards();
         ++ii) {
        getKVBucket()->getVBuckets().getShard(ii)->forEachKVStore(
                [size](KVStore* kvs) { kvs->setMaxDataSize(size); });
    }

    // Update the ArenaMalloc threshold
    arena.setEstimateUpdateThreshold(
            size, configuration.getMemUsedMergeThresholdPercent());
    cb::ArenaMalloc::setAllocatedThreshold(arena);
}

void EventuallyPersistentEngine::set_num_reader_threads(
        ThreadPoolConfig::ThreadCount num) {
    getConfiguration().setNumReaderThreads(static_cast<int>(num));
    ExecutorPool::get()->setNumReaders(num);
}

void EventuallyPersistentEngine::set_num_writer_threads(
        ThreadPoolConfig::ThreadCount num) {
    getConfiguration().setNumWriterThreads(static_cast<int>(num));
    ExecutorPool::get()->setNumWriters(num);

    auto* epBucket = dynamic_cast<EPBucket*>(getKVBucket());
    if (epBucket) {
        // We just changed number of writers so we also need to refresh the
        // flusher batch split trigger to adjust our limits accordingly.
        epBucket->setFlusherBatchSplitTrigger(
                configuration.getFlusherTotalBatchLimit());
    }
}

void EventuallyPersistentEngine::disconnect(gsl::not_null<const void*> cookie) {
    acquireEngine(this)->handleDisconnect(cookie);
}

void EventuallyPersistentEngine::setStorageThreadCallback(
        std::function<void(size_t)> cb) {
    getServerApi()->core->setStorageThreadCallback(cb);
}
