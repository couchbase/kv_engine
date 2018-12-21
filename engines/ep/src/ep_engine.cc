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

#include "config.h"

#include "ep_engine.h"
#include "kv_bucket.h"

#include "bucket_logger.h"
#include "checkpoint.h"
#include "checkpoint_manager.h"
#include "collections/manager.h"
#include "common.h"
#include "connmap.h"
#include "dcp/consumer.h"
#include "dcp/dcpconnmap.h"
#include "dcp/flow-control-manager.h"
#include "dcp/producer.h"
#include "ep_bucket.h"
#include "ep_vb.h"
#include "ephemeral_bucket.h"
#include "failover-table.h"
#include "flusher.h"
#include "htresizer.h"
#include "memory_tracker.h"
#include "replicationthrottle.h"
#include "stats-info.h"
#include "statwriter.h"
#include "string_utils.h"
#include "vb_count_visitor.h"
#include "warmup.h"

#include <JSON_checker.h>
#include <cJSON_utils.h>
#include <logger/logger.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/compress.h>
#include <platform/platform.h>
#include <platform/scope_timer.h>
#include <tracing/trace_helpers.h>
#include <utilities/logtags.h>
#include <xattr/utils.h>

#include <fcntl.h>
#include <stdarg.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

using cb::tracing::TraceCode;

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

struct EPHandleReleaser {
    void operator()(EventuallyPersistentEngine*) {
        ObjectRegistry::onSwitchThread(nullptr);
    }
};

using EPHandle = std::unique_ptr<EventuallyPersistentEngine, EPHandleReleaser>;

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

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static ENGINE_ERROR_CODE sendResponse(ADD_RESPONSE response,
                                      const void* key,
                                      uint16_t keylen,
                                      const void* ext,
                                      uint8_t extlen,
                                      const void* body,
                                      uint32_t bodylen,
                                      uint8_t datatype,
                                      cb::mcbp::Status status,
                                      uint64_t cas,
                                      const void* cookie) {
    ENGINE_ERROR_CODE rv = ENGINE_FAILED;
    NonBucketAllocationGuard guard;
    if (response(key, keylen, ext, extlen, body, bodylen, datatype,
                 status, cas, cookie)) {
        rv = ENGINE_SUCCESS;
    }
    return rv;
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
    eng->destroyInner(force);
    delete eng.get();
}

cb::EngineErrorItemPair EventuallyPersistentEngine::allocate(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        const size_t nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    if (!mcbp::datatype::is_valid(datatype)) {
        EP_LOG_WARN(
                "Invalid value for datatype "
                " (ItemAllocate)");
        return cb::makeEngineErrorItemPair(cb::engine_errc::invalid_arguments);
    }

    item* itm = nullptr;
    auto ret = acquireEngine(this)->itemAllocate(&itm,
                                                 key,
                                                 nbytes,
                                                 0, // No privileged bytes
                                                 flags,
                                                 exptime,
                                                 datatype,
                                                 vbucket);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, this);
}

std::pair<cb::unique_item_ptr, item_info>
EventuallyPersistentEngine::allocate_ex(gsl::not_null<const void*> cookie,
                                        const DocKey& key,
                                        size_t nbytes,
                                        size_t priv_nbytes,
                                        int flags,
                                        rel_time_t exptime,
                                        uint8_t datatype,
                                        Vbid vbucket) {
    item* it = nullptr;
    auto err = acquireEngine(this)->itemAllocate(
            &it, key, nbytes, priv_nbytes, flags, exptime, datatype, vbucket);

    if (err != ENGINE_SUCCESS) {
        throw cb::engine_error(cb::engine_errc(err),
                               "EvpItemAllocateEx: failed to allocate memory");
    }

    item_info info;
    if (!get_item_info(it, &info)) {
        release(it);
        throw cb::engine_error(cb::engine_errc::failed,
                               "EvpItemAllocateEx: EvpGetItemInfo failed");
    }

    return std::make_pair(cb::unique_item_ptr{it, cb::ItemDeleter{this}}, info);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::remove(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        boost::optional<cb::durability::Requirements> durability,
        mutation_descr_t& mut_info) {
    return acquireEngine(this)->itemDelete(
            cookie, key, cas, vbucket, nullptr, mut_info);
}

void EventuallyPersistentEngine::release(gsl::not_null<item*> itm) {
    acquireEngine(this)->itemRelease(itm);
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    get_options_t options = static_cast<get_options_t>(QUEUE_BG_FETCH |
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

    item* itm = nullptr;
    ENGINE_ERROR_CODE ret =
            acquireEngine(this)->get(cookie, &itm, key, vbucket, options);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, this);
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
        boost::optional<cb::durability::Requirements> durability) {
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
    item* itm = nullptr;
    auto ret = acquireEngine(this)->getLockedInner(
            cookie, &itm, key, vbucket, lock_timeout);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, this);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::unlock(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint64_t cas) {
    return acquireEngine(this)->unlockInner(cookie, key, vbucket, cas);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::get_stats(
        gsl::not_null<const void*> cookie,
        cb::const_char_buffer key,
        ADD_STAT add_stat) {
    return acquireEngine(this)->getStats(
            cookie, key.data(), gsl::narrow_cast<int>(key.size()), add_stat);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::store(
        gsl::not_null<const void*> cookie,
        gsl::not_null<item*> itm,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation,
        boost::optional<cb::durability::Requirements> durability,
        DocumentState document_state) {
    Item* item = static_cast<Item*>(itm.get());
    if (document_state == DocumentState::Deleted) {
        item->setDeleted();
    }
    if (durability) {
        item->setPendingSyncWrite(durability.get());
    }
    return acquireEngine(this)->storeInner(cookie, itm, cas, operation);
}

cb::EngineErrorCasPair EventuallyPersistentEngine::store_if(
        gsl::not_null<const void*> cookie,
        gsl::not_null<item*> itm,
        uint64_t cas,
        ENGINE_STORE_OPERATION operation,
        cb::StoreIfPredicate predicate,
        boost::optional<cb::durability::Requirements> durability,
        DocumentState document_state) {
    Item& item = static_cast<Item&>(*static_cast<Item*>(itm.get()));

    if (document_state == DocumentState::Deleted) {
        item.setDeleted();
    }
    if (durability) {
        item.setPendingSyncWrite(durability.get());
    }
    return acquireEngine(this)->storeIfInner(
            cookie, item, cas, operation, predicate);
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
            size_t v = std::stoull(val);
            validate(v,
                     getConfiguration().getCursorDroppingCheckpointMemLowerMark(),
                     size_t(100));
            getConfiguration().setCursorDroppingCheckpointMemUpperMark(v);
        } else if (key == "cursor_dropping_checkpoint_mem_lower_mark") {
            size_t v = std::stoull(val);
            validate(
                    v, size_t(0), getConfiguration().getCursorDroppingCheckpointMemUpperMark());
            getConfiguration().setCursorDroppingCheckpointMemLowerMark(v);
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
        if (key == "max_size") {
            size_t vsize = std::stoull(val);

            getConfiguration().setMaxSize(vsize);
            EPStats& st = getEpStats();
            getConfiguration().setMemLowWat(
                    percentOf(vsize, st.mem_low_wat_percent));
            getConfiguration().setMemHighWat(
                    percentOf(vsize, st.mem_high_wat_percent));
        } else if (key == "mem_low_wat") {
            getConfiguration().setMemLowWat(std::stoull(val));
        } else if (key == "mem_high_wat") {
            getConfiguration().setMemHighWat(std::stoull(val));
        } else if (key == "backfill_mem_threshold") {
            getConfiguration().setBackfillMemThreshold(std::stoull(val));
        } else if (key == "compaction_exp_mem_threshold") {
            getConfiguration().setCompactionExpMemThreshold(std::stoull(val));
        } else if (key == "mutation_mem_threshold") {
            getConfiguration().setMutationMemThreshold(std::stoull(val));
        } else if (key == "timing_log") {
            EPStats& stats = getEpStats();
            std::ostream* old = stats.timingLog;
            stats.timingLog = nullptr;
            delete old;
            if (val == "off") {
                EP_LOG_DEBUG("Disabled timing log.");
            } else {
                auto* tmp(new std::ofstream(val));
                if (tmp->good()) {
                    EP_LOG_DEBUG("Logging detailed timings to ``{}''.", val);
                    stats.timingLog = tmp;
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
        } else if (key == "access_scanner_enabled") {
            getConfiguration().requirementsMetOrThrow("access_scanner_enabled");
            getConfiguration().setAccessScannerEnabled(cb_stob(val));
        } else if (key == "alog_sleep_time") {
            getConfiguration().requirementsMetOrThrow("alog_sleep_time");
            getConfiguration().setAlogSleepTime(std::stoull(val));
        } else if (key == "alog_task_time") {
            getConfiguration().requirementsMetOrThrow("alog_task_time");
            getConfiguration().setAlogTaskTime(std::stoull(val));
            /* Start of ItemPager parameters */
        } else if (key == "pager_active_vb_pcnt") {
            getConfiguration().setPagerActiveVbPcnt(std::stoull(val));
        } else if (key == "pager_sleep_time_ms") {
            getConfiguration().setPagerSleepTimeMs(std::stoull(val));
        } else if (key == "ht_eviction_policy") {
            getConfiguration().setHtEvictionPolicy(val);
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
            size_t value = std::stoull(val);
            getConfiguration().setNumReaderThreads(value);
            ExecutorPool::get()->setNumReaders(value);
        } else if (key == "num_writer_threads") {
            size_t value = std::stoull(val);
            getConfiguration().setNumWriterThreads(value);
            ExecutorPool::get()->setNumWriters(value);
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
        } else if (key == "defragmenter_run") {
            runDefragmenterTask();
        } else if (key == "compaction_write_queue_cap") {
            getConfiguration().setCompactionWriteQueueCap(std::stoull(val));
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
            getConfiguration().requirementsMetOrThrow("ephemeral_full_policy");
            getConfiguration().setEphemeralFullPolicy(val);
        } else if (key == "ephemeral_metadata_purge_age") {
            getConfiguration().requirementsMetOrThrow(
                    "ephemeral_metadata_purge_age");
            getConfiguration().setEphemeralMetadataPurgeAge(std::stoull(val));
        } else if (key == "ephemeral_metadata_purge_interval") {
            getConfiguration().requirementsMetOrThrow("ephemeral_metadata_purge_interval");
            getConfiguration().setEphemeralMetadataPurgeInterval(
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
        if (key == "dcp_consumer_process_buffered_messages_yield_limit") {
            size_t v = size_t(std::stoul(val));
            checkNumeric(val.c_str());
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpConsumerProcessBufferedMessagesYieldLimit(
                    v);
        } else if (key == "dcp_consumer_process_buffered_messages_batch_size") {
            size_t v = size_t(std::stoul(val));
            checkNumeric(val.c_str());
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpConsumerProcessBufferedMessagesBatchSize(
                    v);
        } else if (key == "dcp_idle_timeout") {
            size_t v = size_t(std::stoul(val));
            checkNumeric(val.c_str());
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpIdleTimeout(v);
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
            EP_LOG_WARN("setVbucketParam: max_cas:{} {}", v, vbucket);
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
    const auto key = request.getKey();
    EP_LOG_DEBUG("Manually evicting object with key {}",
                 cb::UserDataView(key.data(), key.size()));
    auto rv = kvBucket->evictKey(makeDocKey(cookie, {key.data(), key.size()}),
                                 request.getVBucket(),
                                 msg);
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

static ENGINE_ERROR_CODE getVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    const cb::mcbp::Request& request,
                                    ADD_RESPONSE response) {
    Vbid vbucket = request.getVBucket();
    VBucketPtr vb = e->getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    } else {
        const auto state = static_cast<vbucket_state_t>(ntohl(vb->getState()));
        return sendResponse(response,
                            nullptr,
                            0,
                            nullptr,
                            0,
                            &state,
                            sizeof(state),
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Success,
                            0,
                            cookie);
    }
}

static ENGINE_ERROR_CODE setVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    const cb::mcbp::Request& request,
                                    ADD_RESPONSE response) {
    nlohmann::json meta;
    vbucket_state_t state;
    auto extras = request.getExtdata();

    if (extras.size() == 1) {
        // This is the new encoding for the SetVBucket state.
        state = vbucket_state_t(extras.front());
        auto val = request.getValue();
        if (!val.empty()) {
            try {
                const nlohmann::detail::input_adapter adapter(
                        reinterpret_cast<const char*>(val.data()), val.size());
                meta = nlohmann::json::parse(adapter);
            } catch (const std::exception&) {
                e->setErrorContext(cookie, "Invalid JSON provided");
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

    return e->setVBucketState(cookie,
                              response,
                              request.getVBucket(),
                              state,
                              meta,
                              false,
                              request.getCas());
}

static ENGINE_ERROR_CODE delVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    const cb::mcbp::Request& req,
                                    ADD_RESPONSE response) {
    Vbid vbucket = req.getVBucket();
    auto value = req.getValue();
    bool sync = value.size() == 7 && memcmp(value.data(), "async=0", 7) == 0;

    ENGINE_ERROR_CODE err;
    void* es = e->getEngineSpecific(cookie);
    if (sync) {
        if (es == nullptr) {
            err = e->deleteVBucket(vbucket, cookie);
            e->storeEngineSpecific(cookie, e);
        } else {
            e->storeEngineSpecific(cookie, nullptr);
            EP_LOG_DEBUG("Completed sync deletion of {}", vbucket);
            err = ENGINE_SUCCESS;
        }
    } else {
        err = e->deleteVBucket(vbucket);
    }

    switch (err) {
    case ENGINE_SUCCESS:
        EP_LOG_INFO("Deletion of {} was completed.", vbucket);
        return sendResponse(response,
                            NULL,
                            0,
                            NULL,
                            0,
                            NULL,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Success,
                            req.getCas(),
                            cookie);
    case ENGINE_NOT_MY_VBUCKET:
        EP_LOG_WARN(
                "Deletion of {} failed because the vbucket doesn't exist!!!",
                vbucket);
        return ENGINE_NOT_MY_VBUCKET;
    case ENGINE_EINVAL:
        EP_LOG_WARN(
                "Deletion of {} failed "
                "because the vbucket is not in a dead state",
                vbucket);
        e->setErrorContext(
                cookie,
                "Failed to delete vbucket.  Must be in the dead state.");
        return ENGINE_EINVAL;
    case ENGINE_EWOULDBLOCK:
        EP_LOG_INFO(
                "Request for {} deletion is in"
                " EWOULDBLOCK until the database file is removed from disk",
                vbucket);
        // We don't use the actual value in ewouldblock, just the existence
        // of something there.
        e->storeEngineSpecific(cookie, static_cast<void*>(e));
        return ENGINE_EWOULDBLOCK;
    default:
        EP_LOG_WARN("Deletion of {} failed because of unknown reasons",
                    vbucket);
        e->setErrorContext(cookie, "Failed to delete vbucket.  Unknown reason.");
        return ENGINE_FAILED;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getReplicaCmd(
        const cb::mcbp::Request& request,
        ADD_RESPONSE response,
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
        return sendResponse(response,
                            static_cast<const void*>(rv.item->getKey().data()),
                            rv.item->getKey().size(),
                            (const void*)&flags,
                            sizeof(uint32_t),
                            static_cast<const void*>(rv.item->getData()),
                            rv.item->getNBytes(),
                            rv.item->getDataType(),
                            cb::mcbp::Status::Success,
                            rv.item->getCas(),
                            cookie);
    } else if (error_code == ENGINE_TMPFAIL) {
        return ENGINE_KEY_ENOENT;
    }

    return error_code;
}

static ENGINE_ERROR_CODE compactDB(EventuallyPersistentEngine* e,
                                   const void* cookie,
                                   const cb::mcbp::Request& req,
                                   ADD_RESPONSE response) {
    const auto res = cb::mcbp::Status::Success;
    CompactionConfig compactionConfig;
    uint64_t cas = req.getCas();

    EPStats& stats = e->getEpStats();
    auto extras = req.getExtdata();
    const auto* payload =
            reinterpret_cast<const cb::mcbp::request::CompactDbPayload*>(
                    extras.data());

    compactionConfig.purge_before_ts = payload->getPurgeBeforeTs();
    compactionConfig.purge_before_seq = payload->getPurgeBeforeSeq();
    compactionConfig.drop_deletes = payload->getDropDeletes();
    compactionConfig.db_file_id = e->getKVBucket()->getDBFileId(req);
    Vbid vbid = req.getVBucket();

    ENGINE_ERROR_CODE err;
    if (e->getEngineSpecific(cookie) == nullptr) {
        ++stats.pendingCompactions;
        e->storeEngineSpecific(cookie, e);
        err = e->compactDB(vbid, compactionConfig, cookie);
    } else {
        e->storeEngineSpecific(cookie, nullptr);
        err = ENGINE_SUCCESS;
    }

    switch (err) {
    case ENGINE_SUCCESS:
        break;
    case ENGINE_NOT_MY_VBUCKET:
        --stats.pendingCompactions;
        EP_LOG_WARN(
                "Compaction of db file id: {} failed "
                "because the db file doesn't exist!!!",
                compactionConfig.db_file_id.get());
        return ENGINE_NOT_MY_VBUCKET;
    case ENGINE_EINVAL:
        --stats.pendingCompactions;
        EP_LOG_WARN(
                "Compaction of db file id: {} failed "
                "because of an invalid argument",
                compactionConfig.db_file_id.get());
        return ENGINE_EINVAL;
    case ENGINE_EWOULDBLOCK:
        EP_LOG_INFO(
                "Compaction of db file id: {} scheduled "
                "(awaiting completion).",
                compactionConfig.db_file_id.get());
        // We don't use the value stored in the engine-specific code, just
        // that it is non-null...
        e->storeEngineSpecific(cookie, static_cast<void*>(e));
        return ENGINE_EWOULDBLOCK;
    case ENGINE_TMPFAIL:
        EP_LOG_WARN(
                "Request to compact db file id: {} hit"
                " a temporary failure and may need to be retried",
                compactionConfig.db_file_id.get());
        e->setErrorContext(cookie, "Temporary failure in compacting db file.");
        return ENGINE_TMPFAIL;
    default:
        --stats.pendingCompactions;
        EP_LOG_WARN("Compaction of db file id: {} failed: ",
                    compactionConfig.db_file_id.get(),
                    cb::to_string(cb::engine_errc(err)));
        e->setErrorContext(cookie,
                           "Failed to compact db file: " +
                                   cb::to_string(cb::engine_errc(err)));
        return err;
    }

    return sendResponse(response,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        res,
                        cas,
                        cookie);
}

static ENGINE_ERROR_CODE processUnknownCommand(EventuallyPersistentEngine* h,
                                               const void* cookie,
                                               const cb::mcbp::Request& request,
                                               ADD_RESPONSE response) {
    auto res = cb::mcbp::Status::UnknownCommand;
    std::string dynamic_msg;
    const char* msg = nullptr;
    size_t msg_size = 0;

    EPStats& stats = h->getEpStats();

    /**
     * Session validation
     * (For ns_server commands only)
     */
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::SetParam:
    case cb::mcbp::ClientOpcode::SetVbucket:
    case cb::mcbp::ClientOpcode::DelVbucket:
    case cb::mcbp::ClientOpcode::CompactDb:
        if (h->getEngineSpecific(cookie) == nullptr) {
            uint64_t cas = request.getCas();
            if (!h->validateSessionCas(cas)) {
                h->setErrorContext(cookie, "Invalid session token");
                return ENGINE_KEY_EEXISTS;
            }
        }
        break;
    default:
        break;
    }

    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::GetAllVbSeqnos:
        return h->getAllVBucketSequenceNumbers(cookie, request, response);

    case cb::mcbp::ClientOpcode::GetVbucket: {
        BlockTimer timer(&stats.getVbucketCmdHisto);
        return getVBucket(h, cookie, request, response);
    }
    case cb::mcbp::ClientOpcode::DelVbucket: {
        BlockTimer timer(&stats.delVbucketCmdHisto);
        const auto rv = delVBucket(h, cookie, request, response);
        if (rv != ENGINE_EWOULDBLOCK) {
            h->decrementSessionCtr();
            h->storeEngineSpecific(cookie, NULL);
        }
        return rv;
    }
    case cb::mcbp::ClientOpcode::SetVbucket: {
        BlockTimer timer(&stats.setVbucketCmdHisto);
        const auto rv = setVBucket(h, cookie, request, response);
        h->decrementSessionCtr();
        return rv;
    }
    case cb::mcbp::ClientOpcode::StopPersistence:
        res = h->stopFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::StartPersistence:
        res = h->startFlusher(&msg, &msg_size);
        break;
    case cb::mcbp::ClientOpcode::SetParam:
        res = h->setParam(request, dynamic_msg);
        msg = dynamic_msg.c_str();
        msg_size = dynamic_msg.length();
        h->decrementSessionCtr();
        break;
    case cb::mcbp::ClientOpcode::EvictKey:
        res = h->evictKey(cookie, request, &msg);
        break;
    case cb::mcbp::ClientOpcode::Observe:
        return h->observe(cookie, request, response);
    case cb::mcbp::ClientOpcode::ObserveSeqno:
        return h->observe_seqno(cookie, request, response);
    case cb::mcbp::ClientOpcode::LastClosedCheckpoint:
        return h->handleLastClosedCheckpoint(cookie, request, response);
    case cb::mcbp::ClientOpcode::CreateCheckpoint:
        return h->handleCreateCheckpoint(cookie, request, response);
    case cb::mcbp::ClientOpcode::CheckpointPersistence:
        return h->handleCheckpointPersistence(cookie, request, response);
    case cb::mcbp::ClientOpcode::SeqnoPersistence:
        return h->handleSeqnoPersistence(cookie, request, response);
    case cb::mcbp::ClientOpcode::SetWithMeta:
    case cb::mcbp::ClientOpcode::SetqWithMeta:
    case cb::mcbp::ClientOpcode::AddWithMeta:
    case cb::mcbp::ClientOpcode::AddqWithMeta:
        return h->setWithMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::DelWithMeta:
    case cb::mcbp::ClientOpcode::DelqWithMeta:
        return h->deleteWithMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::ReturnMeta:
        return h->returnMeta(cookie, request, response);
    case cb::mcbp::ClientOpcode::GetReplica:
        return h->getReplicaCmd(request, response, cookie);
    case cb::mcbp::ClientOpcode::EnableTraffic:
    case cb::mcbp::ClientOpcode::DisableTraffic:
        return h->handleTrafficControlCmd(cookie, request, response);
    case cb::mcbp::ClientOpcode::CompactDb: {
        const auto rv = compactDB(h, cookie, request, response);
        if (rv != ENGINE_EWOULDBLOCK) {
            h->decrementSessionCtr();
            h->storeEngineSpecific(cookie, nullptr);
        }
        return rv;
    }
    case cb::mcbp::ClientOpcode::GetRandomKey:
        return h->getRandomKey(cookie, response);
    case cb::mcbp::ClientOpcode::GetKeys:
        return h->getAllKeys(cookie, request, response);
        // MB-21143: Remove adjusted time/drift API, but return NOT_SUPPORTED
    case cb::mcbp::ClientOpcode::GetAdjustedTime:
    case cb::mcbp::ClientOpcode::SetDriftCounterState: {
        return sendResponse(response,
                            NULL,
                            0,
                            NULL,
                            0,
                            NULL,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::NotSupported,
                            0,
                            cookie);
    }
    default:
        res = cb::mcbp::Status::UnknownCommand;
    }

    msg_size = (msg_size > 0 || msg == NULL) ? msg_size : strlen(msg);
    return sendResponse(response,
                        NULL,
                        0,
                        NULL,
                        0,
                        msg,
                        static_cast<uint16_t>(msg_size),
                        PROTOCOL_BINARY_RAW_BYTES,
                        res,
                        0,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::unknown_command(
        const void* cookie,
        const cb::mcbp::Request& request,
        ADD_RESPONSE response) {
    auto engine = acquireEngine(this);
    auto ret = processUnknownCommand(engine.get(), cookie, request, response);
    return ret;
}

void EventuallyPersistentEngine::item_set_cas(gsl::not_null<item*> itm,
                                              uint64_t cas) {
    static_cast<Item*>(itm.get())->setCas(cas);
}

void EventuallyPersistentEngine::item_set_datatype(
        gsl::not_null<item*> itm, protocol_binary_datatype_t datatype) {
    static_cast<Item*>(itm.get())->setDataType(datatype);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::step(
        gsl::not_null<const void*> cookie,
        gsl::not_null<dcp_message_producers*> producers) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->step(producers);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::open(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint32_t seqno,
        uint32_t flags,
        cb::const_char_buffer name) {
    return acquireEngine(this)->dcpOpen(cookie, opaque, seqno, flags, name);
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->closeStream(opaque, vbucket, sid);
    }
    return ENGINE_DISCONNECT;
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
        boost::optional<cb::const_char_buffer> json) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        try {
            return conn->streamRequest(flags,
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
            return ENGINE_ERROR_CODE(e.code().value());
        }
    }
    return ENGINE_DISCONNECT;
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
    ConnHandler* conn = engine->getConnHandler(cookie);
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
    auto ret = callback(failoverEntries.data(), failoverEntries.size(), cookie);
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::stream_end(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t flags) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->streamEnd(opaque, vbucket, flags);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::snapshot_marker(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->snapshotMarker(
                opaque, vbucket, start_seqno, end_seqno, flags);
    }
    return ENGINE_DISCONNECT;
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->mutation(opaque, key, value, priv_bytes, datatype, cas,
                              vbucket, flags, by_seqno, rev_seqno, expiration,
                              lock_time, meta, nru);
    }
    return ENGINE_DISCONNECT;
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->deletion(opaque, key, value, priv_bytes, datatype, cas,
                              vbucket, by_seqno, rev_seqno, meta);
    }
    return ENGINE_DISCONNECT;
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->deletionV2(opaque,
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
    return ENGINE_DISCONNECT;
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->expiration(opaque,
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
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::set_vbucket_state(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        vbucket_state_t state) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->setVBucketState(opaque, vbucket, state);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::noop(
        gsl::not_null<const void*> cookie, uint32_t opaque) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->noop(opaque);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::buffer_acknowledgement(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t buffer_bytes) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->bufferAcknowledgement(opaque, vbucket, buffer_bytes);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::control(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        cb::const_char_buffer key,
        cb::const_char_buffer value) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->control(opaque, key, value);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::response_handler(
        gsl::not_null<const void*> cookie,
        const protocol_binary_response_header* response) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        if (conn->handleResponse(response)) {
            return ENGINE_SUCCESS;
        }
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
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->systemEvent(
                opaque, vbucket, event, bySeqno, version, key, eventData);
    }
    return ENGINE_DISCONNECT;
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
        cb::durability::Requirements durability) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->prepare(opaque,
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
                             durability);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::seqno_acknowledged(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t in_memory_seqno,
        uint64_t on_disk_seqno) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->seqno_acknowledged(
                opaque, vbucket, in_memory_seqno, on_disk_seqno);
    }
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::commit(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        const DocKey& key,
        uint64_t prepared_seqno,
        uint64_t commit_seqno) {
    auto engine = acquireEngine(this);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->commit(opaque, vbucket, key, commit_seqno);
    }
    return ENGINE_DISCONNECT;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::abort(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint64_t prepared_seqno,
        uint64_t abort_seqno) {
    return ENGINE_ENOTSUP;
}

static void EvpHandleDisconnect(const void* cookie,
                                ENGINE_EVENT_TYPE type,
                                const void* event_data,
                                const void* cb_data) {
    if (type != ON_DISCONNECT) {
        throw std::invalid_argument("EvpHandleDisconnect: type "
                                        "(which is" + std::to_string(type) +
                                    ") is not ON_DISCONNECT");
    }
    if (event_data != nullptr) {
        throw std::invalid_argument("EvpHandleDisconnect: event_data "
                                        "is not NULL");
    }
    void* c = const_cast<void*>(cb_data);
    acquireEngine(static_cast<EngineIface*>(c))->handleDisconnect(cookie);
}

static void EvpHandleDeleteBucket(const void* cookie,
                                  ENGINE_EVENT_TYPE type,
                                  const void* event_data,
                                  const void* cb_data) {
    if (type != ON_DELETE_BUCKET) {
        throw std::invalid_argument("EvpHandleDeleteBucket: type "
                                        "(which is" + std::to_string(type) +
                                    ") is not ON_DELETE_BUCKET");
    }
    if (event_data != nullptr) {
        throw std::invalid_argument("EvpHandleDeleteBucket: event_data "
                                        "is not NULL");
    }
    void* c = const_cast<void*>(cb_data);
    acquireEngine(static_cast<EngineIface*>(c))->handleDeleteBucket(cookie);
}

/**
 * The only public interface to the eventually persistent engine.
 * Allocate a new instance and initialize it
 * @param get_server_api callback function to get the server exported API
 *                  functions
 * @param handle Where to return the new instance
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE create_instance(GET_SERVER_API get_server_api,
                                  EngineIface** handle) {
    SERVER_HANDLE_V1* api = get_server_api();
    if (api == NULL) {
        return ENGINE_ENOTSUP;
    }

    BucketLogger::setLoggerAPI(api->log);

    MemoryTracker::getInstance(*api->alloc_hooks);
    ObjectRegistry::initialize(api->alloc_hooks->get_allocation_size);

    std::atomic<size_t>* inital_tracking = new std::atomic<size_t>();

    ObjectRegistry::setStats(inital_tracking);
    EventuallyPersistentEngine* engine;
    engine = new EventuallyPersistentEngine(get_server_api);
    ObjectRegistry::setStats(NULL);

    if (engine == NULL) {
        return ENGINE_ENOMEM;
    }

    if (MemoryTracker::trackingMemoryAllocations()) {
        engine->getEpStats().estimatedTotalMemory.get()->store(
                inital_tracking->load());
        engine->getEpStats().memoryTrackerEnabled.store(true);
    }
    delete inital_tracking;

    initialize_time_functions(api->core);

    *handle = reinterpret_cast<EngineIface*>(engine);

    return ENGINE_SUCCESS;
}

/*
    This method is called prior to unloading of the shared-object.
    Global clean-up should be performed from this method.
*/
void destroy_engine() {
    ExecutorPool::shutdown();
    // A single MemoryTracker exists for *all* buckets
    // and must be destroyed before unloading the shared object.
    MemoryTracker::destroyInstance();
    ObjectRegistry::reset();
}

bool EventuallyPersistentEngine::get_item_info(
        gsl::not_null<const item*> itm, gsl::not_null<item_info*> itm_info) {
    const Item* it = reinterpret_cast<const Item*>(itm.get());
    *itm_info = acquireEngine(this)->getItemInfo(*it);
    return true;
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::get_meta(
        gsl::not_null<const void*> cookie, const DocKey& key, Vbid vbucket) {
    return acquireEngine(this)->getMetaInner(cookie, key, vbucket);
}

static cb::engine_errc EvpCollectionsSetManifest(
        gsl::not_null<EngineIface*> handle,
        gsl::not_null<const void*> cookie,
        cb::const_char_buffer json) {
    auto engine = acquireEngine(handle);
    auto rv = engine->getKVBucket()->setCollections(json);

    if (cb::engine_errc::success != cb::engine_errc(rv.code().value())) {
        engine->setErrorContext(cookie, rv.what());
    }

    return cb::engine_errc(rv.code().value());
}

static cb::engine_errc EvpCollectionsGetManifest(
        gsl::not_null<EngineIface*> handle,
        gsl::not_null<const void*> cookie,
        ADD_RESPONSE response) {
    auto engine = acquireEngine(handle);
    auto rv = engine->getKVBucket()->getCollections();
    return cb::engine_errc(sendResponse(response,
                                        nullptr,
                                        0,
                                        nullptr,
                                        0,
                                        rv.second.data(),
                                        gsl::narrow<uint32_t>(rv.second.size()),
                                        PROTOCOL_BINARY_DATATYPE_JSON,
                                        rv.first,
                                        0,
                                        cookie));
}

static cb::EngineErrorGetCollectionIDResult EvpCollectionsGetCollectionID(
        gsl::not_null<EngineIface*> handle,
        gsl::not_null<const void*> cookie,
        cb::const_char_buffer path) {
    auto engine = acquireEngine(handle);
    auto rv = engine->getKVBucket()->getCollectionID(path);
    if (rv.result == cb::engine_errc::unknown_collection) {
        engine->setErrorContext(cookie,
                                Collections::getUnknownCollectionErrorContext(
                                        rv.getManifestId()));
    }
    return rv;
}

bool EventuallyPersistentEngine::isXattrEnabled() {
    return getKVBucket()->isXattrEnabled();
}

EventuallyPersistentEngine::EventuallyPersistentEngine(
        GET_SERVER_API get_server_api)
    : kvBucket(nullptr),
      workload(NULL),
      workloadPriority(NO_BUCKET_PRIORITY),
      getServerApiFunc(get_server_api),
      checkpointConfig(NULL),
      trafficEnabled(false),
      startupTime(0),
      taskable(this),
      compressionMode(BucketCompressionMode::Off),
      minCompressionRatio(default_min_compression_ratio) {
    EngineIface::collections.set_manifest = EvpCollectionsSetManifest;
    EngineIface::collections.get_manifest = EvpCollectionsGetManifest;
    EngineIface::collections.get_collection_id = EvpCollectionsGetCollectionID;

    serverApi = getServerApiFunc();
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::reserveCookie(const void *cookie)
{
    NonBucketAllocationGuard guard;
    ENGINE_ERROR_CODE rv = serverApi->cookie->reserve(cookie);
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::releaseCookie(const void *cookie)
{
    NonBucketAllocationGuard guard;
    ENGINE_ERROR_CODE rv = serverApi->cookie->release(cookie);
    return rv;
}

void EventuallyPersistentEngine::storeEngineSpecific(const void* cookie,
                                                     void* engine_data) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->store_engine_specific(cookie, engine_data);
}

void* EventuallyPersistentEngine::getEngineSpecific(const void* cookie) {
    NonBucketAllocationGuard guard;
    void* engine_data = serverApi->cookie->get_engine_specific(cookie);
    return engine_data;
}

bool EventuallyPersistentEngine::isDatatypeSupported(
        const void* cookie, protocol_binary_datatype_t datatype) {
    NonBucketAllocationGuard guard;
    bool isSupported =
            serverApi->cookie->is_datatype_supported(cookie, datatype);
    return isSupported;
}

bool EventuallyPersistentEngine::isMutationExtrasSupported(const void* cookie) {
    NonBucketAllocationGuard guard;
    bool isSupported = serverApi->cookie->is_mutation_extras_supported(cookie);
    return isSupported;
}

bool EventuallyPersistentEngine::isXattrEnabled(const void* cookie) {
    return isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_XATTR);
}

bool EventuallyPersistentEngine::isCollectionsSupported(const void* cookie) {
    NonBucketAllocationGuard guard;
    bool isSupported = serverApi->cookie->is_collections_supported(cookie);
    return isSupported;
}

cb::mcbp::ClientOpcode EventuallyPersistentEngine::getOpcodeIfEwouldblockSet(
        const void* cookie) {
    NonBucketAllocationGuard guard;
    return serverApi->cookie->get_opcode_if_ewouldblock_set(cookie);
}

bool EventuallyPersistentEngine::validateSessionCas(const uint64_t cas) {
    NonBucketAllocationGuard guard;
    bool ret = serverApi->cookie->validate_session_cas(cas);
    return ret;
}

void EventuallyPersistentEngine::decrementSessionCtr(void) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->decrement_session_ctr();
}

void EventuallyPersistentEngine::registerEngineCallback(ENGINE_EVENT_TYPE type,
                                                        EVENT_CALLBACK cb,
                                                        const void *cb_data) {
    NonBucketAllocationGuard guard;
    auto* sapi = getServerApi()->callback;
    sapi->register_callback(
            reinterpret_cast<EngineIface*>(this), type, cb, cb_data);
}

void EventuallyPersistentEngine::setErrorContext(
        const void* cookie, cb::const_char_buffer message) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_error_context(const_cast<void*>(cookie), message);
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
    EpEngineValueChangeListener(EventuallyPersistentEngine &e) : engine(e) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
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

    virtual void stringValueChanged(const std::string& key, const char* value) {
        if (key == "compression_mode") {
            std::string value_str{value, strlen(value)};
            engine.setCompressionMode(value_str);
        }
    }

    virtual void floatValueChanged(const std::string& key, float value) {
        if (key == "min_compression_ratio") {
            engine.setMinCompressionRatio(value);
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

    maxFailoverEntries = configuration.getMaxFailoverEntries();

    // Start updating the variables from the config!
    VBucket::setMutationMemoryThreshold(
            configuration.getMutationMemThreshold());

    if (configuration.getMaxSize() == 0) {
        EP_LOG_WARN("Invalid configuration: max_size must be a non-zero value");
        return ENGINE_FAILED;
    }

    if (configuration.getMemLowWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_low_wat_percent.store(0.75);
        configuration.setMemLowWat(percentOf(
                configuration.getMaxSize(), stats.mem_low_wat_percent.load()));
    }

    if (configuration.getMemHighWat() == std::numeric_limits<size_t>::max()) {
        stats.mem_high_wat_percent.store(0.85);
        configuration.setMemHighWat(percentOf(
                configuration.getMaxSize(), stats.mem_high_wat_percent.load()));
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

    workload = new WorkLoadPolicy(configuration.getMaxNumWorkers(),
                                  configuration.getMaxNumShards());
    if ((unsigned int)workload->getNumShards() >
                                              configuration.getMaxVbuckets()) {
        EP_LOG_WARN(
                "Invalid configuration: Shards must be "
                "equal or less than max number of vbuckets");
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

    initializeEngineCallbacks();

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

void EventuallyPersistentEngine::operator delete(void* ptr) {
    // Already destructed EventuallyPersistentEngine object; about to
    // deallocate its memory. As such; it is not valid to update the
    // memory state inside the now-destroyed EPStats child object of
    // EventuallyPersistentEngine.  Therefore forcably disassociated
    // the current thread from this engine before deallocating memory.
    ObjectRegistry::onSwitchThread(nullptr);
    ::operator delete(ptr);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::itemAllocate(
        item** itm,
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    if (priv_nbytes > maxItemPrivilegedBytes) {
        return ENGINE_E2BIG;
    }

    if ((nbytes - priv_nbytes) > maxItemSize) {
        return ENGINE_E2BIG;
    }

    if (!hasMemoryForItemAllocation(sizeof(Item) + sizeof(Blob) + key.size() +
                                    nbytes)) {
        return memoryCondition();
    }

    time_t expiretime = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

    *itm = new Item(key,
                    flags,
                    expiretime,
                    nullptr,
                    nbytes,
                    datatype,
                    0 /*cas*/,
                    -1 /*seq*/,
                    vbucket);
    if (*itm == NULL) {
        return memoryCondition();
    } else {
        stats.itemAllocSizeHisto.add(nbytes);
        return ENGINE_SUCCESS;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::itemDelete(
        const void* cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        ItemMetaData* item_meta,
        mutation_descr_t& mut_info) {
    ENGINE_ERROR_CODE ret = kvBucket->deleteItem(key,
                                                 cas,
                                                 vbucket,
                                                 cookie,
                                                 item_meta,
                                                 mut_info);

    if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }
    } else if (ret == ENGINE_SUCCESS) {
        ++stats.numOpsDelete;
    }
    return ret;
}

void EventuallyPersistentEngine::itemRelease(item* itm) {
    delete reinterpret_cast<Item*>(itm);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::get(const void* cookie,
                                                  item** itm,
                                                  const DocKey& key,
                                                  Vbid vbucket,
                                                  get_options_t options) {
    ScopeTimer2<MicrosecondStopwatch, TracerStopwatch> timer(
            MicrosecondStopwatch(stats.getCmdHisto),
            TracerStopwatch(cookie, cb::tracing::TraceCode::GET));

    GetValue gv(kvBucket->get(key, vbucket, cookie, options));
    ENGINE_ERROR_CODE ret = gv.getStatus();

    if (ret == ENGINE_SUCCESS) {
        *itm = gv.item.release();
        if (options & TRACK_STATISTICS) {
            ++stats.numOpsGet;
        }
    } else if (ret == ENGINE_KEY_ENOENT || ret == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }
    }

    return ret;
}

cb::EngineErrorItemPair EventuallyPersistentEngine::getAndTouchInner(
        const void* cookie, const DocKey& key, Vbid vbucket, uint32_t exptime) {
    auto* handle = reinterpret_cast<EngineIface*>(this);

    time_t expiry_time = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

    GetValue gv(kvBucket->getAndUpdateTtl(key, vbucket, cookie, expiry_time));

    auto rv = gv.getStatus();
    if (rv == ENGINE_SUCCESS) {
        ++stats.numOpsGet;
        ++stats.numOpsStore;
        return cb::makeEngineErrorItemPair(
                cb::engine_errc::success, gv.item.release(), handle);
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
    auto* handle = reinterpret_cast<EngineIface*>(this);

    ScopeTimer2<MicrosecondStopwatch, TracerStopwatch> timer(
            MicrosecondStopwatch(stats.getCmdHisto),
            TracerStopwatch(cookie, cb::tracing::TraceCode::GETIF));

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
        if (ii == 1 || kvBucket->getItemEvictionPolicy() == FULL_EVICTION) {
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
                        cb::engine_errc::success, gv.item.release(), handle);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::getLockedInner(
        const void* cookie,
        item** itm,
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
        *itm = result.item.release();
    }

    return result.getStatus();
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
        ENGINE_STORE_OPERATION operation,
        cb::StoreIfPredicate predicate) {
    ScopeTimer2<MicrosecondStopwatch, TracerStopwatch> timer(
            MicrosecondStopwatch(stats.storeCmdHisto),
            TracerStopwatch(cookie, cb::tracing::TraceCode::STORE));

    // Check if this is a in-progress durable store which has now completed -
    // (see 'case EWOULDBLOCK' at the end of this function where we record
    // the fact we must block the client until the SycnWrite is durable).
    if (item.getCommitted() == CommittedState::Pending &&
        getEngineSpecific(cookie) != nullptr) {
        // Non-null means this is the second call to this function after
        // the SyncWrite has completed.
        // Clear the engineSpecific, and return SUCCESS.
        storeEngineSpecific(cookie, nullptr);
        // @todo-durability - return correct CAS
        // @todo-durability - add support for non-sucesss (e.g. Aborted) when
        // we support non-successful completions of SyncWrites.
        return {cb::engine_errc::success, 0xdeadbeef};
    }

    ENGINE_ERROR_CODE status;
    switch (operation) {
    case OPERATION_CAS:
        if (item.getCas() == 0) {
            // Using a cas command with a cas wildcard doesn't make sense
            status = ENGINE_NOT_STORED;
            break;
        }
    // FALLTHROUGH
    case OPERATION_SET:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }
        status = kvBucket->set(item, cookie, predicate);
        break;

    case OPERATION_ADD:
        if (isDegradedMode()) {
            return {cb::engine_errc::temporary_failure, cas};
        }

        if (item.getCas() != 0) {
            // Adding an item with a cas value doesn't really make sense...
            return {cb::engine_errc::key_already_exists, cas};
        }

        status = kvBucket->add(item, cookie);
        break;

    case OPERATION_REPLACE:
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
    case ENGINE_EWOULDBLOCK:
        if (item.getCommitted() == CommittedState::Pending) {
            // Record the fact that we are blocking to wait for SyncWrite
            // completion; so the next call to this function should return
            // the result of the SyncWrite (see call to getEngineSpecific at
            // the head of this function.
            // (just store non-null value to indicate this).
            storeEngineSpecific(cookie, reinterpret_cast<void*>(0x1));
        }
        break;
    default:
        break;
    }

    return {cb::engine_errc(status), item.getCas()};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::storeInner(
        const void* cookie,
        item* itm,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation) {
    Item& item = static_cast<Item&>(*static_cast<Item*>(itm));
    auto rv = storeIfInner(cookie, item, cas, operation, {});
    cas = rv.cas;
    return ENGINE_ERROR_CODE(rv.status);
}

void EventuallyPersistentEngine::initializeEngineCallbacks() {
    // Register the ON_DISCONNECT callback
    registerEngineCallback(ON_DISCONNECT, EvpHandleDisconnect, this);
    // Register the ON_DELETE_BUCKET callback
    registerEngineCallback(ON_DELETE_BUCKET, EvpHandleDeleteBucket, this);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::memoryCondition() {
    // Do we think it's possible we could free something?
    bool haveEvidenceWeCanFreeMemory =
            (stats.getMaxDataSize() > stats.getMemOverhead());
    if (haveEvidenceWeCanFreeMemory) {
        // Look for more evidence by seeing if we have resident items.
        VBucketCountVisitor countVisitor(vbucket_state_active);
        kvBucket->visit(countVisitor);

        haveEvidenceWeCanFreeMemory = countVisitor.getNonResident() <
            countVisitor.getNumItems();
    }
    if (haveEvidenceWeCanFreeMemory) {
        ++stats.tmp_oom_errors;
        // Wake up the item pager task as memory usage
        // seems to have exceeded high water mark
        getKVBucket()->attemptToFreeMemory();
        return ENGINE_TMPFAIL;
    } else {
        if (getKVBucket()->getItemEvictionPolicy() == FULL_EVICTION) {
            ++stats.tmp_oom_errors;
            getKVBucket()->wakeUpCheckpointRemover();
            return ENGINE_TMPFAIL;
        }

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
    return trafficEnabled.compare_exchange_strong(inverse, enable);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEngineStats(const void *cookie,
                                                           ADD_STAT add_stat) {

    configuration.addStats(add_stat, cookie);

    EPStats &epstats = getEpStats();
    add_casted_stat("ep_storage_age",
                    epstats.dirtyAge, add_stat, cookie);
    add_casted_stat("ep_storage_age_highwat",
                    epstats.dirtyAgeHighWat, add_stat, cookie);
    add_casted_stat("ep_num_workers", ExecutorPool::get()->getNumWorkersStat(),
                    add_stat, cookie);

    if (getWorkloadPriority() == HIGH_BUCKET_PRIORITY) {
        add_casted_stat("ep_bucket_priority", "HIGH", add_stat, cookie);
    } else if (getWorkloadPriority() == LOW_BUCKET_PRIORITY) {
        add_casted_stat("ep_bucket_priority", "LOW", add_stat, cookie);
    }

    add_casted_stat("ep_total_enqueued",
                    epstats.totalEnqueued, add_stat, cookie);
    add_casted_stat("ep_total_deduplicated",
                    epstats.totalDeduplicated,
                    add_stat,
                    cookie);
    add_casted_stat("ep_expired_access", epstats.expired_access,
                    add_stat, cookie);
    add_casted_stat("ep_expired_compactor", epstats.expired_compactor,
                    add_stat, cookie);
    add_casted_stat("ep_expired_pager", epstats.expired_pager,
                    add_stat, cookie);
    add_casted_stat("ep_queue_size",
                    epstats.diskQueueSize, add_stat, cookie);
    add_casted_stat("ep_diskqueue_items",
                    epstats.diskQueueSize, add_stat, cookie);
    add_casted_stat("ep_vb_backfill_queue_size",
                    epstats.vbBackfillQueueSize,
                    add_stat,
                    cookie);
    auto* flusher = kvBucket->getFlusher(EP_PRIMARY_SHARD);
    if (flusher) {
        add_casted_stat("ep_commit_num", epstats.flusherCommits,
                        add_stat, cookie);
        add_casted_stat("ep_commit_time",
                        epstats.commit_time, add_stat, cookie);
        add_casted_stat("ep_commit_time_total",
                        epstats.cumulativeCommitTime, add_stat, cookie);
        add_casted_stat("ep_item_begin_failed",
                        epstats.beginFailed, add_stat, cookie);
        add_casted_stat("ep_item_commit_failed",
                        epstats.commitFailed, add_stat, cookie);
        add_casted_stat("ep_item_flush_expired",
                        epstats.flushExpired, add_stat, cookie);
        add_casted_stat("ep_item_flush_failed",
                        epstats.flushFailed, add_stat, cookie);
        add_casted_stat("ep_flusher_state",
                        flusher->stateName(), add_stat, cookie);
        add_casted_stat("ep_flusher_todo",
                        epstats.flusher_todo, add_stat, cookie);
        add_casted_stat("ep_total_persisted",
                        epstats.totalPersisted, add_stat, cookie);
        add_casted_stat("ep_uncommitted_items",
                        epstats.flusher_todo, add_stat, cookie);
        add_casted_stat("ep_chk_persistence_timeout",
                        VBucket::getCheckpointFlushTimeout().count(),
                        add_stat,
                        cookie);
    }
    add_casted_stat("ep_vbucket_del",
                    epstats.vbucketDeletions, add_stat, cookie);
    add_casted_stat("ep_vbucket_del_fail",
                    epstats.vbucketDeletionFail, add_stat, cookie);
    add_casted_stat("ep_flush_duration_total",
                    epstats.cumulativeFlushTime, add_stat, cookie);

    kvBucket->getAggregatedVBucketStats(cookie, add_stat);

    kvBucket->getFileStats(cookie, add_stat);

    add_casted_stat("ep_persist_vbstate_total",
                    epstats.totalPersistVBState, add_stat, cookie);

    size_t memUsed = stats.getPreciseTotalMemoryUsed();
    add_casted_stat("mem_used", memUsed, add_stat, cookie);
    add_casted_stat("mem_used_estimate",
                    stats.getEstimatedTotalMemoryUsed(),
                    add_stat,
                    cookie);
    add_casted_stat("ep_mem_low_wat_percent", stats.mem_low_wat_percent,
                    add_stat, cookie);
    add_casted_stat("ep_mem_high_wat_percent", stats.mem_high_wat_percent,
                    add_stat, cookie);
    add_casted_stat("bytes", memUsed, add_stat, cookie);
    add_casted_stat("ep_kv_size", stats.getCurrentSize(), add_stat, cookie);
    add_casted_stat("ep_blob_num", stats.getNumBlob(), add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat(
            "ep_blob_overhead", stats.getBlobOverhead(), add_stat, cookie);
#else
    add_casted_stat("ep_blob_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat(
            "ep_value_size", stats.getTotalValueSize(), add_stat, cookie);
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
    add_casted_stat("ep_overhead", stats.getMemOverhead(), add_stat, cookie);
    add_casted_stat("ep_item_num", stats.getNumItem(), add_stat, cookie);

    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors,
                    add_stat, cookie);
    add_casted_stat("ep_mem_tracker_enabled", stats.memoryTrackerEnabled,
                    add_stat, cookie);
    add_casted_stat("ep_bg_fetched", epstats.bg_fetched,
                    add_stat, cookie);
    add_casted_stat("ep_bg_meta_fetched", epstats.bg_meta_fetched,
                    add_stat, cookie);
    add_casted_stat("ep_bg_remaining_items", epstats.numRemainingBgItems,
                    add_stat, cookie);
    add_casted_stat("ep_bg_remaining_jobs", epstats.numRemainingBgJobs,
                    add_stat, cookie);
    add_casted_stat("ep_num_pager_runs", epstats.pagerRuns,
                    add_stat, cookie);
    add_casted_stat("ep_num_expiry_pager_runs", epstats.expiryPagerRuns,
                    add_stat, cookie);
    add_casted_stat("ep_num_freq_decayer_runs",
                    epstats.freqDecayerRuns,
                    add_stat,
                    cookie);
    add_casted_stat("ep_items_rm_from_checkpoints",
                    epstats.itemsRemovedFromCheckpoints,
                    add_stat, cookie);
    add_casted_stat("ep_num_value_ejects", epstats.numValueEjects,
                    add_stat, cookie);
    add_casted_stat("ep_num_eject_failures", epstats.numFailedEjects,
                    add_stat, cookie);
    add_casted_stat("ep_num_not_my_vbuckets", epstats.numNotMyVBuckets,
                    add_stat, cookie);

    add_casted_stat("ep_pending_ops", epstats.pendingOps, add_stat, cookie);
    add_casted_stat("ep_pending_ops_total", epstats.pendingOpsTotal,
                    add_stat, cookie);
    add_casted_stat("ep_pending_ops_max", epstats.pendingOpsMax,
                    add_stat, cookie);
    add_casted_stat("ep_pending_ops_max_duration",
                    epstats.pendingOpsMaxDuration,
                    add_stat, cookie);

    add_casted_stat("ep_pending_compactions", epstats.pendingCompactions,
                    add_stat, cookie);
    add_casted_stat("ep_rollback_count", epstats.rollbackCount,
                    add_stat, cookie);

    size_t vbDeletions = epstats.vbucketDeletions.load();
    if (vbDeletions > 0) {
        add_casted_stat("ep_vbucket_del_max_walltime",
                        epstats.vbucketDelMaxWalltime,
                        add_stat, cookie);
        add_casted_stat("ep_vbucket_del_avg_walltime",
                        epstats.vbucketDelTotWalltime / vbDeletions,
                        add_stat, cookie);
    }

    size_t numBgOps = epstats.bgNumOperations.load();
    if (numBgOps > 0) {
        add_casted_stat("ep_bg_num_samples", epstats.bgNumOperations,
                        add_stat, cookie);
        add_casted_stat("ep_bg_min_wait",
                        epstats.bgMinWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_wait",
                        epstats.bgMaxWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait_avg",
                        epstats.bgWait / numBgOps,
                        add_stat, cookie);
        add_casted_stat("ep_bg_min_load",
                        epstats.bgMinLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_load",
                        epstats.bgMaxLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load_avg",
                        epstats.bgLoad / numBgOps,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait",
                        epstats.bgWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load",
                        epstats.bgLoad,
                        add_stat, cookie);
    }

    add_casted_stat("ep_degraded_mode", isDegradedMode(), add_stat, cookie);

    add_casted_stat("ep_num_access_scanner_runs", epstats.alogRuns,
                    add_stat, cookie);
    add_casted_stat("ep_num_access_scanner_skips",
                    epstats.accessScannerSkips, add_stat, cookie);
    add_casted_stat("ep_access_scanner_last_runtime", epstats.alogRuntime,
                    add_stat, cookie);
    add_casted_stat("ep_access_scanner_num_items", epstats.alogNumItems,
                    add_stat, cookie);

    if (kvBucket->isAccessScannerEnabled() && epstats.alogTime.load() != 0)
    {
        char timestr[20];
        struct tm alogTim;
        hrtime_t alogTime = epstats.alogTime.load();
        if (cb_gmtime_r((time_t *)&alogTime, &alogTim) == -1) {
            add_casted_stat("ep_access_scanner_task_time", "UNKNOWN", add_stat,
                            cookie);
        } else {
            strftime(timestr, 20, "%Y-%m-%d %H:%M:%S", &alogTim);
            add_casted_stat("ep_access_scanner_task_time", timestr, add_stat,
                            cookie);
        }
    } else {
        add_casted_stat("ep_access_scanner_task_time", "NOT_SCHEDULED",
                        add_stat, cookie);
    }

    if (kvBucket->isExpPagerEnabled()) {
        char timestr[20];
        struct tm expPagerTim;
        hrtime_t expPagerTime = epstats.expPagerTime.load();
        if (cb_gmtime_r((time_t *)&expPagerTime, &expPagerTim) == -1) {
            add_casted_stat("ep_expiry_pager_task_time", "UNKNOWN", add_stat,
                            cookie);
        } else {
            strftime(timestr, 20, "%Y-%m-%d %H:%M:%S", &expPagerTim);
            add_casted_stat("ep_expiry_pager_task_time", timestr, add_stat,
                            cookie);
        }
    } else {
        add_casted_stat("ep_expiry_pager_task_time", "NOT_SCHEDULED",
                        add_stat, cookie);
    }

    add_casted_stat("ep_startup_time", startupTime.load(), add_stat, cookie);

    if (getConfiguration().isWarmup()) {
        Warmup *wp = kvBucket->getWarmup();
        if (wp == nullptr) {
            throw std::logic_error("EPEngine::doEngineStats: warmup is NULL");
        }
        if (!kvBucket->isWarmingUp()) {
            add_casted_stat("ep_warmup_thread", "complete", add_stat, cookie);
        } else {
            add_casted_stat("ep_warmup_thread", "running", add_stat, cookie);
        }
        if (wp->getTime() > wp->getTime().zero()) {
            add_casted_stat(
                    "ep_warmup_time",
                    std::chrono::duration_cast<std::chrono::microseconds>(
                            wp->getTime())
                            .count(),
                    add_stat,
                    cookie);
        }
        add_casted_stat("ep_warmup_oom", epstats.warmOOM, add_stat, cookie);
        add_casted_stat("ep_warmup_dups", epstats.warmDups, add_stat, cookie);
    }

    add_casted_stat("ep_num_ops_get_meta", epstats.numOpsGetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_set_meta", epstats.numOpsSetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_del_meta", epstats.numOpsDelMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_set_meta_res_fail",
                    epstats.numOpsSetMetaResolutionFailed, add_stat, cookie);
    add_casted_stat("ep_num_ops_del_meta_res_fail",
                    epstats.numOpsDelMetaResolutionFailed, add_stat, cookie);
    add_casted_stat("ep_num_ops_set_ret_meta", epstats.numOpsSetRetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_del_ret_meta", epstats.numOpsDelRetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_get_meta_on_set_meta",
                    epstats.numOpsGetMetaOnSetWithMeta, add_stat, cookie);
    add_casted_stat("ep_workload_pattern",
                    workload->stringOfWorkLoadPattern(),
                    add_stat, cookie);

    add_casted_stat("ep_defragmenter_num_visited", epstats.defragNumVisited,
                    add_stat, cookie);
    add_casted_stat("ep_defragmenter_num_moved", epstats.defragNumMoved,
                    add_stat, cookie);

    add_casted_stat("ep_item_compressor_num_visited",
                    epstats.compressorNumVisited,
                    add_stat,
                    cookie);
    add_casted_stat("ep_item_compressor_num_compressed",
                    epstats.compressorNumCompressed,
                    add_stat,
                    cookie);

    add_casted_stat("ep_cursor_dropping_lower_threshold",
                    epstats.cursorDroppingLThreshold, add_stat, cookie);
    add_casted_stat("ep_cursor_dropping_upper_threshold",
                    epstats.cursorDroppingUThreshold, add_stat, cookie);
    add_casted_stat("ep_cursors_dropped",
                    epstats.cursorsDropped, add_stat, cookie);
    add_casted_stat("ep_cursor_memory_freed",
                    epstats.cursorMemoryFreed,
                    add_stat,
                    cookie);

    // Note: These are also reported per-shard in 'kvstore' stats, however
    // we want to be able to graph these over time, and hence need to expose
    // to ns_sever at the top-level.
    size_t value = 0;
    if (kvBucket->getKVStoreStat("failure_compaction", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        // Total data write failures is compaction failures plus commit failures
        auto writeFailure = value + epstats.commitFailed;
        add_casted_stat("ep_data_write_failed", writeFailure, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("failure_get", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_data_read_failed",  value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("io_total_read_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_io_total_read_bytes",  value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("io_total_write_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_io_total_write_bytes",  value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("io_compaction_read_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_io_compaction_read_bytes",  value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("io_compaction_write_bytes", value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_io_compaction_write_bytes",  value, add_stat, cookie);
    }

    if (kvBucket->getKVStoreStat("io_bg_fetch_read_count",
                                 value,
                                 KVBucketIface::KVSOption::BOTH)) {
        add_casted_stat("ep_io_bg_fetch_read_count", value, add_stat, cookie);
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
        add_casted_stat("ep_bg_fetch_avg_read_amplification",
                        readAmp,
                        add_stat,
                        cookie);
    }

    // Specific to RocksDB. Cumulative ep-engine stats.
    // Note: These are also reported per-shard in 'kvstore' stats.
    // Memory Usage
    if (kvBucket->getKVStoreStat(
                "kMemTableTotal", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_kMemTableTotal", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat(
                "kMemTableUnFlushed", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_kMemTableUnFlushed", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat(
                "kTableReadersTotal", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_kTableReadersTotal", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat(
                "kCacheTotal", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_kCacheTotal", value, add_stat, cookie);
    }
    // MemTable Size per-CF
    if (kvBucket->getKVStoreStat("default_kSizeAllMemTables",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_default_kSizeAllMemTables",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("seqno_kSizeAllMemTables",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_seqno_kSizeAllMemTables", value, add_stat, cookie);
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
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_casted_stat("ep_rocksdb_block_cache_data_hit_ratio",
                        ratio,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.index.hit",
                                 hit,
                                 KVBucketIface::KVSOption::RW) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.index.miss",
                                 miss,
                                 KVBucketIface::KVSOption::RW) &&
        (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_casted_stat("ep_rocksdb_block_cache_index_hit_ratio",
                        ratio,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.filter.hit",
                                 hit,
                                 KVBucketIface::KVSOption::RW) &&
        kvBucket->getKVStoreStat("rocksdb.block.cache.filter.miss",
                                 miss,
                                 KVBucketIface::KVSOption::RW) &&
        (hit + miss) != 0) {
        const auto ratio =
                gsl::narrow_cast<int>(float(hit) / (hit + miss) * 10000);
        add_casted_stat("ep_rocksdb_block_cache_filter_hit_ratio",
                        ratio,
                        add_stat,
                        cookie);
    }
    // Disk Usage per-CF
    if (kvBucket->getKVStoreStat("default_kTotalSstFilesSize",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_default_kTotalSstFilesSize",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("seqno_kTotalSstFilesSize",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_seqno_kTotalSstFilesSize", value, add_stat, cookie);
    }
    // Scan stats
    if (kvBucket->getKVStoreStat(
                "scan_totalSeqnoHits", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_scan_totalSeqnoHits", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat(
                "scan_oldSeqnoHits", value, KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_scan_oldSeqnoHits", value, add_stat, cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doMemoryStats(const void *cookie,
                                                           ADD_STAT add_stat) {
    add_casted_stat(
            "bytes", stats.getPreciseTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat(
            "mem_used", stats.getPreciseTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat("mem_used_estimate",
                    stats.getEstimatedTotalMemoryUsed(),
                    add_stat,
                    cookie);
    add_casted_stat("mem_used_merge_threshold",
                    stats.getMemUsedMergeThreshold(),
                    add_stat,
                    cookie);

    add_casted_stat("ep_kv_size", stats.getCurrentSize(), add_stat, cookie);
    add_casted_stat(
            "ep_value_size", stats.getTotalValueSize(), add_stat, cookie);
    add_casted_stat("ep_overhead", stats.getMemOverhead(), add_stat, cookie);
    add_casted_stat("ep_max_size", stats.getMaxDataSize(), add_stat, cookie);
    add_casted_stat("ep_mem_low_wat", stats.mem_low_wat, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat_percent", stats.mem_low_wat_percent,
                    add_stat, cookie);
    add_casted_stat("ep_mem_high_wat", stats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat_percent", stats.mem_high_wat_percent,
                    add_stat, cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors,
                    add_stat, cookie);

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

    std::map<std::string, size_t> alloc_stats;
    MemoryTracker::getInstance(*getServerApiFunc()->alloc_hooks)->
        getAllocatorStats(alloc_stats);

    for (const auto& it : alloc_stats) {
        add_casted_stat(it.first.c_str(), it.second, add_stat, cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVBucketStats(
                                                       const void *cookie,
                                                       ADD_STAT add_stat,
                                                       const char* stat_key,
                                                       int nkey,
                                                       bool prevStateRequested,
                                                       bool details) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(KVBucketIface* store,
                           const void *c, ADD_STAT a,
                           bool isPrevStateRequested, bool detailsRequested) :
            eps(store), cookie(c), add_stat(a),
            isPrevState(isPrevStateRequested),
            isDetailsRequested(detailsRequested) {}

        void visitBucket(VBucketPtr &vb) override {
            addVBStats(cookie, add_stat, vb, eps, isPrevState,
                       isDetailsRequested);
        }

        static void addVBStats(const void *cookie, ADD_STAT add_stat,
                               VBucketPtr &vb,
                               KVBucketIface* store,
                               bool isPrevStateRequested,
                               bool detailsRequested) {
            if (!vb) {
                return;
            }

            if (isPrevStateRequested) {
                try {
                    char buf[16];
                    checked_snprintf(
                            buf, sizeof(buf), "vb_%d", vb->getId().get());
                    add_casted_stat(buf,
                                    VBucket::toString(vb->getInitialState()),
                                    add_stat, cookie);
                } catch (std::exception& error) {
                    EP_LOG_WARN("addVBStats: Failed building stats: {}",
                                error.what());
                }
            } else {
                vb->addStats(detailsRequested, add_stat, cookie);
            }
        }

    private:
        KVBucketIface* eps;
        const void *cookie;
        ADD_STAT add_stat;
        bool isPrevState;
        bool isDetailsRequested;
    };

    if (nkey > 16 && strncmp(stat_key, "vbucket-details", 15) == 0) {
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

        StatVBucketVisitor::addVBStats(cookie, add_stat, vb, kvBucket.get(),
                                       prevStateRequested, details);
    }
    else {
        StatVBucketVisitor svbv(kvBucket.get(), cookie, add_stat,
                                prevStateRequested, details);
        kvBucket->visit(svbv);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashStats(const void *cookie,
                                                          ADD_STAT add_stat) {

    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void* c,
                           ADD_STAT a,
                           BucketCompressionMode compressMode)
            : cookie(c), add_stat(a), compressionMode(compressMode) {
        }

        void visitBucket(VBucketPtr &vb) override {
            Vbid vbid = vb->getId();
            char buf[32];
            try {
                checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid.get());
                add_casted_stat(buf, VBucket::toString(vb->getState()),
                                add_stat, cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
            }

            HashTableDepthStatVisitor depthVisitor;
            vb->ht.visitDepth(depthVisitor);

            try {
                checked_snprintf(buf, sizeof(buf), "vb_%d:size", vbid.get());
                add_casted_stat(buf, vb->ht.getSize(), add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:locks", vbid.get());
                add_casted_stat(buf, vb->ht.getNumLocks(), add_stat, cookie);
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:min_depth", vbid.get());
                add_casted_stat(buf,
                                depthVisitor.min == -1 ? 0 : depthVisitor.min,
                                add_stat, cookie);
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:max_depth", vbid.get());
                add_casted_stat(buf, depthVisitor.max, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:histo", vbid.get());
                add_casted_stat(buf, depthVisitor.depthHisto, add_stat, cookie);
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:reported", vbid.get());
                add_casted_stat(buf, vb->ht.getNumInMemoryItems(), add_stat,
                                cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:counted", vbid.get());
                add_casted_stat(buf, depthVisitor.size, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:resized", vbid.get());
                add_casted_stat(buf, vb->ht.getNumResizes(), add_stat, cookie);
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:mem_size", vbid.get());
                add_casted_stat(buf, vb->ht.getItemMemory(), add_stat, cookie);

                if (compressionMode != BucketCompressionMode::Off) {
                    checked_snprintf(buf,
                                     sizeof(buf),
                                     "vb_%d:mem_size_uncompressed",
                                     vbid.get());
                    add_casted_stat(buf,
                                    vb->ht.getUncompressedItemMemory(),
                                    add_stat,
                                    cookie);
                }
                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:mem_size_counted", vbid.get());
                add_casted_stat(buf, depthVisitor.memUsed, add_stat, cookie);

                checked_snprintf(
                        buf, sizeof(buf), "vb_%d:num_system_items", vbid.get());
                add_casted_stat(
                        buf, vb->ht.getNumSystemItems(), add_stat, cookie);
            } catch (std::exception& error) {
                EP_LOG_WARN(
                        "StatVBucketVisitor::visitBucket: Failed to build "
                        "stat: {}",
                        error.what());
            }
        }

        const void *cookie;
        ADD_STAT add_stat;
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
    AddStatsStream(std::string key, ADD_STAT callback, const void* cookie)
        : std::ostream(&buf), key(key), callback(callback), cookie(cookie) {
    }

    ~AddStatsStream() {
        // The ADD_STAT callback may allocate memory (temporary buffers for
        // stat data) which will be de-allocated inside the server (i.e.
        // after the engine call has returned). As such we do not want to
        // account such memory against this bucket.
        NonBucketAllocationGuard statsCallbackGuard;
        auto value = buf.str();
        callback(key.data(), key.size(), value.data(), value.size(), cookie);
    }

private:
    std::string key;
    ADD_STAT callback;
    const void* cookie;
    std::stringbuf buf;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashDump(
        const void* cookie, ADD_STAT addStat, cb::const_char_buffer keyArgs) {
    if (keyArgs.empty()) {
        // Must specify a vbucket.
        return ENGINE_EINVAL;
    }
    uint16_t vbucket_id;
    if (!parseUint16(keyArgs.data(), &vbucket_id)) {
        return ENGINE_EINVAL;
    }
    Vbid vbid = Vbid(vbucket_id);
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    AddStatsStream as(std::to_string(vbid.get()), addStat, cookie);
    as << vb->ht << std::endl;

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointDump(
        const void* cookie, ADD_STAT addStat, cb::const_char_buffer keyArgs) {
    if (keyArgs.empty()) {
        // Must specify a vbucket.
        return ENGINE_EINVAL;
    }
    uint16_t vbucket_id;
    if (!parseUint16(keyArgs.data(), &vbucket_id)) {
        return ENGINE_EINVAL;
    }
    Vbid vbid = Vbid(vbucket_id);
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    AddStatsStream as(std::to_string(vbid.get()), addStat, cookie);
    as << *vb->checkpointManager << std::endl;

    return ENGINE_SUCCESS;
}

class StatCheckpointVisitor : public VBucketVisitor {
public:
    StatCheckpointVisitor(KVBucketIface* kvs, const void *c,
                          ADD_STAT a) : kvBucket(kvs), cookie(c),
                                        add_stat(a) {}

    void visitBucket(VBucketPtr &vb) override {
        addCheckpointStat(cookie, add_stat, kvBucket, vb);
    }

    static void addCheckpointStat(const void *cookie, ADD_STAT add_stat,
                                  KVBucketIface* eps,
                                  VBucketPtr &vb) {
        if (!vb) {
            return;
        }

        Vbid vbid = vb->getId();
        char buf[256];
        try {
            checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid.get());
            add_casted_stat(buf, VBucket::toString(vb->getState()),
                            add_stat, cookie);
            vb->checkpointManager->addStats(add_stat, cookie);

            auto result = eps->getLastPersistedCheckpointId(vbid);
            if (result.second) {
                checked_snprintf(buf,
                                 sizeof(buf),
                                 "vb_%d:persisted_checkpoint_id",
                                 vbid.get());
                add_casted_stat(buf, result.first, add_stat, cookie);
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
    ADD_STAT add_stat;
};


class StatCheckpointTask : public GlobalTask {
public:
    StatCheckpointTask(EventuallyPersistentEngine *e, const void *c,
            ADD_STAT a) : GlobalTask(e, TaskId::StatCheckpointTask,
                                     0, false),
                          ep(e), cookie(c), add_stat(a) { }
    bool run(void) {
        TRACE_EVENT0("ep-engine/task", "StatsCheckpointTask");
        StatCheckpointVisitor scv(ep->getKVBucket(), cookie, add_stat);
        ep->getKVBucket()->visit(scv);
        ep->notifyIOComplete(cookie, ENGINE_SUCCESS);
        return false;
    }

    std::string getDescription() {
        return "checkpoint stats for all vbuckets";
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Task needed to lookup "checkpoint" stats; so the runtime should only
        // affects the particular stat request. However we don't want this to
        // take /too/ long, so set limit of 100ms.
        return std::chrono::milliseconds(100);
    }

private:
    EventuallyPersistentEngine *ep;
    const void *cookie;
    ADD_STAT add_stat;
};
/// @endcond

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointStats(
                                                          const void *cookie,
                                                          ADD_STAT add_stat,
                                                          const char* stat_key,
                                                          int nkey) {

    if (nkey == 10) {
        void* es = getEngineSpecific(cookie);
        if (es == NULL) {
            ExTask task = std::make_shared<StatCheckpointTask>(
                    this, cookie, add_stat);
            ExecutorPool::get()->schedule(task);
            storeEngineSpecific(cookie, this);
            return ENGINE_EWOULDBLOCK;
        } else {
            storeEngineSpecific(cookie, NULL);
        }
    } else if (nkey > 11) {
        std::string vbid(&stat_key[11], nkey - 11);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return ENGINE_EINVAL;
        }
        Vbid vbucketId = Vbid(vbucket_id);
        VBucketPtr vb = getVBucket(vbucketId);

        StatCheckpointVisitor::addCheckpointStat(cookie, add_stat,
                                                 kvBucket.get(), vb);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDurabilityMonitorStats(
        const void* cookie, ADD_STAT add_stat, const char* stat_key, int nkey) {
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

/**
 * Function object to send stats for a single dcp connection.
 */
struct ConnStatBuilder {
    ConnStatBuilder(const void *c, ADD_STAT as, ConnCounter& tc)
        : cookie(c), add_stat(as), aggregator(tc) {}

    void operator()(std::shared_ptr<ConnHandler> tc) {
        ++aggregator.totalConns;
        tc->addStats(add_stat, cookie);

        auto tp = std::dynamic_pointer_cast<DcpProducer>(tc);
        if (tp) {
            ++aggregator.totalProducers;
            tp->aggregateQueueStats(aggregator);
        }
    }

    const void *cookie;
    ADD_STAT    add_stat;
    ConnCounter& aggregator;
};

struct ConnAggStatBuilder {
    ConnAggStatBuilder(std::map<std::string, ConnCounter*> *m,
                      const char *s, size_t sl)
        : counters(m), sep(s), sep_len(sl) {}

    ConnCounter* getTarget(std::shared_ptr<ConnHandler> tc) {
        ConnCounter *rv = NULL;

        if (tc) {
            const std::string name(tc->getName());
            size_t pos1 = name.find(':');
            if (pos1 == name.npos) {
                throw std::invalid_argument("ConnAggStatBuilder::getTarget: "
                        "connection tc (which has name '" + tc->getName() +
                        "' does not include a colon (:)");
            }
            size_t pos2 = name.find(sep, pos1+1, sep_len);
            if (pos2 != name.npos) {
                std::string prefix(name.substr(pos1+1, pos2 - pos1 - 1));
                rv = (*counters)[prefix];
                if (rv == NULL) {
                    rv = new ConnCounter;
                    (*counters)[prefix] = rv;
                }
            }
        }
        return rv;
    }

    void aggregate(std::shared_ptr<ConnHandler> c, ConnCounter* tc) {
        ConnCounter counter;

        ++counter.totalConns;
        if (std::dynamic_pointer_cast<DcpProducer>(c)) {
            ++counter.totalProducers;
        }

        c->aggregateQueueStats(counter);

        ConnCounter* total = getTotalCounter();
        *total += counter;

        if (tc) {
            *tc += counter;
        }
    }

    ConnCounter *getTotalCounter() {
        ConnCounter *rv = NULL;
        std::string sepr(sep);
        std::string total(sepr + "total");
        rv = (*counters)[total];
        if(rv == NULL) {
            rv = new ConnCounter;
            (*counters)[total] = rv;
        }
        return rv;
    }

    void operator()(std::shared_ptr<ConnHandler> tc) {
        if (tc) {
            ConnCounter *aggregator = getTarget(tc);
            aggregate(tc, aggregator);
        }
    }

    std::map<std::string, ConnCounter*> *counters;
    const char *sep;
    size_t sep_len;
};

/// @endcond

static void showConnAggStat(const std::string &prefix,
                            ConnCounter *counter,
                            const void *cookie,
                            ADD_STAT add_stat) {

    try {
        char statname[80] = {0};
        const size_t sl(sizeof(statname));
        checked_snprintf(statname, sl, "%s:count", prefix.c_str());
        add_casted_stat(statname, counter->totalConns, add_stat, cookie);

        checked_snprintf(statname, sl, "%s:backoff", prefix.c_str());
        add_casted_stat(statname, counter->conn_queueBackoff,
                        add_stat, cookie);

        checked_snprintf(statname, sl, "%s:producer_count", prefix.c_str());
        add_casted_stat(statname, counter->totalProducers, add_stat, cookie);

        checked_snprintf(statname, sl, "%s:items_sent", prefix.c_str());
        add_casted_stat(statname, counter->conn_queueDrain, add_stat, cookie);

        checked_snprintf(statname, sl, "%s:items_remaining", prefix.c_str());
        add_casted_stat(statname, counter->conn_queueRemaining, add_stat,
                        cookie);

        checked_snprintf(statname, sl, "%s:total_bytes", prefix.c_str());
        add_casted_stat(statname, counter->conn_totalBytes, add_stat, cookie);

        checked_snprintf(statname, sl, "%s:total_uncompressed_data_size", prefix.c_str());
        add_casted_stat(statname, counter->conn_totalUncompressedDataSize, add_stat, cookie);

    } catch (std::exception& error) {
        EP_LOG_WARN("showConnAggStat: Failed to build stats: {}", error.what());
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doConnAggStats(
                                                        const void *cookie,
                                                        ADD_STAT add_stat,
                                                        const char *sepPtr,
                                                        size_t sep_len) {
    // In practice, this will be 1, but C++ doesn't let me use dynamic
    // array sizes.
    const size_t max_sep_len(8);
    sep_len = std::min(sep_len, max_sep_len);

    char sep[max_sep_len + 1];
    memcpy(sep, sepPtr, sep_len);
    sep[sep_len] = 0x00;

    std::map<std::string, ConnCounter*> counters;
    ConnAggStatBuilder visitor(&counters, sep, sep_len);
    dcpConnMap_->each(visitor);

    std::map<std::string, ConnCounter*>::iterator it;
    for (it = counters.begin(); it != counters.end(); ++it) {
        showConnAggStat(it->first, it->second, cookie, add_stat);
        delete it->second;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDcpStats(const void *cookie,
                                                         ADD_STAT add_stat) {
    ConnCounter aggregator;
    ConnStatBuilder dcpVisitor(cookie, add_stat, aggregator);
    dcpConnMap_->each(dcpVisitor);

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
        const void* cookie, ADD_STAT add_stat) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(const void* cookie,
                                                         ADD_STAT add_stat,
                                                         Vbid vbid,
                                                         const DocKey& key,
                                                         bool validate) {
    ENGINE_ERROR_CODE rv = ENGINE_FAILED;

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
                         cb::UserDataView(key.data(), key.size()),
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
        const void* cookie, ADD_STAT add_stat, Vbid vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if(!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }
    vb->failovers->addStats(cookie, vb->getId(), add_stat);
    return ENGINE_SUCCESS;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::doAllFailoverLogStats(
                                                           const void *cookie,
                                                           ADD_STAT add_stat) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void *c, ADD_STAT a) :
            cookie(c), add_stat(a) {}

        void visitBucket(VBucketPtr &vb) override {
            vb->failovers->addStats(cookie, vb->getId(), add_stat);
        }

    private:
        const void *cookie;
        ADD_STAT add_stat;
    };

    StatVBucketVisitor svbv(cookie, add_stat);
    kvBucket->visit(svbv);

    return rv;
}



ENGINE_ERROR_CODE EventuallyPersistentEngine::doTimingStats(const void *cookie,
                                                           ADD_STAT add_stat) {
    add_casted_stat("bg_wait", stats.bgWaitHisto, add_stat, cookie);
    add_casted_stat("bg_load", stats.bgLoadHisto, add_stat, cookie);
    add_casted_stat("set_with_meta", stats.setWithMetaHisto, add_stat, cookie);
    add_casted_stat("pending_ops", stats.pendingOpsHisto, add_stat, cookie);

    // Vbucket visitors
    add_casted_stat("access_scanner", stats.accessScannerHisto, add_stat, cookie);
    add_casted_stat("checkpoint_remover", stats.checkpointRemoverHisto, add_stat, cookie);
    add_casted_stat("item_pager", stats.itemPagerHisto, add_stat, cookie);
    add_casted_stat("expiry_pager", stats.expiryPagerHisto, add_stat, cookie);

    add_casted_stat("storage_age", stats.dirtyAgeHisto, add_stat, cookie);

    // Regular commands
    add_casted_stat("get_cmd", stats.getCmdHisto, add_stat, cookie);
    add_casted_stat("store_cmd", stats.storeCmdHisto, add_stat, cookie);
    add_casted_stat("arith_cmd", stats.arithCmdHisto, add_stat, cookie);
    add_casted_stat("get_stats_cmd", stats.getStatsCmdHisto, add_stat, cookie);
    // Admin commands
    add_casted_stat("get_vb_cmd", stats.getVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("set_vb_cmd", stats.setVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("del_vb_cmd", stats.delVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("chk_persistence_cmd", stats.chkPersistenceHisto,
                    add_stat, cookie);
    // Misc
    add_casted_stat("notify_io", stats.notifyIOHisto, add_stat, cookie);
    add_casted_stat("batch_read", stats.getMultiHisto, add_stat, cookie);

    // Disk stats
    add_casted_stat("disk_insert", stats.diskInsertHisto, add_stat, cookie);
    add_casted_stat("disk_update", stats.diskUpdateHisto, add_stat, cookie);
    add_casted_stat("disk_del", stats.diskDelHisto, add_stat, cookie);
    add_casted_stat("disk_vb_del", stats.diskVBDelHisto, add_stat, cookie);
    add_casted_stat("disk_commit", stats.diskCommitHisto, add_stat, cookie);

    add_casted_stat("item_alloc_sizes", stats.itemAllocSizeHisto,
                    add_stat, cookie);
    add_casted_stat("bg_batch_size", stats.getMultiBatchSizeHisto, add_stat,
                    cookie);

    // Checkpoint cursor stats
    add_casted_stat("persistence_cursor_get_all_items",
                    stats.persistenceCursorGetItemsHisto,
                    add_stat, cookie);
    add_casted_stat("dcp_cursors_get_all_items",
                    stats.dcpCursorsGetItemsHisto,
                    add_stat, cookie);

    return ENGINE_SUCCESS;
}

static std::string getTaskDescrForStats(TaskId id) {
    return std::string(GlobalTask::getTaskName(id)) + "[" +
           to_string(GlobalTask::getTaskType(id)) + "]";
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doSchedulerStats(const void
                                                                *cookie,
                                                                ADD_STAT
                                                                add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.schedulingHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doRunTimeStats(const void
                                                                *cookie,
                                                                ADD_STAT
                                                                add_stat) {
    for (TaskId id : GlobalTask::allTaskIds) {
        add_casted_stat(getTaskDescrForStats(id).c_str(),
                        stats.taskRuntimeHisto[static_cast<int>(id)],
                        add_stat,
                        cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDispatcherStats(const void
                                                                *cookie,
                                                                ADD_STAT
                                                                add_stat) {
    ExecutorPool::get()->doWorkerStat(ObjectRegistry::getCurrentEngine(),
                                      cookie, add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doTasksStats(const void* cookie,
                                                           ADD_STAT add_stat) {
    ExecutorPool::get()->doTasksStat(
            ObjectRegistry::getCurrentEngine(), cookie, add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doWorkloadStats(const void
                                                              *cookie,
                                                              ADD_STAT
                                                              add_stat) {
    try {
        char statname[80] = {0};
        ExecutorPool* expool = ExecutorPool::get();

        int readers = expool->getNumReaders();
        checked_snprintf(statname, sizeof(statname), "ep_workload:num_readers");
        add_casted_stat(statname, readers, add_stat, cookie);

        int writers = expool->getNumWriters();
        checked_snprintf(statname, sizeof(statname), "ep_workload:num_writers");
        add_casted_stat(statname, writers, add_stat, cookie);

        int auxio = expool->getNumAuxIO();
        checked_snprintf(statname, sizeof(statname), "ep_workload:num_auxio");
        add_casted_stat(statname, auxio, add_stat, cookie);

        int nonio = expool->getNumNonIO();
        checked_snprintf(statname, sizeof(statname), "ep_workload:num_nonio");
        add_casted_stat(statname, nonio, add_stat, cookie);

        int max_readers = expool->getMaxReaders();
        checked_snprintf(statname, sizeof(statname), "ep_workload:max_readers");
        add_casted_stat(statname, max_readers, add_stat, cookie);

        int max_writers = expool->getMaxWriters();
        checked_snprintf(statname, sizeof(statname), "ep_workload:max_writers");
        add_casted_stat(statname, max_writers, add_stat, cookie);

        int max_auxio = expool->getMaxAuxIO();
        checked_snprintf(statname, sizeof(statname), "ep_workload:max_auxio");
        add_casted_stat(statname, max_auxio, add_stat, cookie);

        int max_nonio = expool->getMaxNonIO();
        checked_snprintf(statname, sizeof(statname), "ep_workload:max_nonio");
        add_casted_stat(statname, max_nonio, add_stat, cookie);

        int shards = workload->getNumShards();
        checked_snprintf(statname, sizeof(statname), "ep_workload:num_shards");
        add_casted_stat(statname, shards, add_stat, cookie);

        int numReadyTasks = expool->getNumReadyTasks();
        checked_snprintf(statname, sizeof(statname), "ep_workload:ready_tasks");
        add_casted_stat(statname, numReadyTasks, add_stat, cookie);

        int numSleepers = expool->getNumSleepers();
        checked_snprintf(statname, sizeof(statname),
                         "ep_workload:num_sleepers");
        add_casted_stat(statname, numSleepers, add_stat, cookie);

        expool->doTaskQStat(ObjectRegistry::getCurrentEngine(),
                            cookie, add_stat);

    } catch (std::exception& error) {
        EP_LOG_WARN("doWorkloadStats: Error building stats: {}", error.what());
    }

    return ENGINE_SUCCESS;
}

void EventuallyPersistentEngine::addSeqnoVbStats(const void *cookie,
                                                 ADD_STAT add_stat,
                                                 const VBucketPtr &vb) {
    // MB-19359: An atomic read of vbucket state without acquiring the
    // reader lock for state should suffice here.
    uint64_t relHighSeqno = vb->getHighSeqno();
    if (vb->getState() != vbucket_state_active) {
        snapshot_info_t info = vb->checkpointManager->getSnapshotInfo();
        relHighSeqno = info.range.end;
    }

    try {
        char buffer[64];
        failover_entry_t entry = vb->failovers->getLatestEntry();
        checked_snprintf(
                buffer, sizeof(buffer), "vb_%d:high_seqno", vb->getId().get());
        add_casted_stat(buffer, relHighSeqno, add_stat, cookie);
        checked_snprintf(buffer,
                         sizeof(buffer),
                         "vb_%d:abs_high_seqno",
                         vb->getId().get());
        add_casted_stat(buffer, vb->getHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer,
                         sizeof(buffer),
                         "vb_%d:last_persisted_seqno",
                         vb->getId().get());
        add_casted_stat(
                buffer, vb->getPublicPersistenceSeqno(), add_stat, cookie);
        checked_snprintf(
                buffer, sizeof(buffer), "vb_%d:uuid", vb->getId().get());
        add_casted_stat(buffer, entry.vb_uuid, add_stat, cookie);
        checked_snprintf(
                buffer, sizeof(buffer), "vb_%d:purge_seqno", vb->getId().get());
        add_casted_stat(buffer, vb->getPurgeSeqno(), add_stat, cookie);
        const snapshot_range_t range = vb->getPersistedSnapshot();
        checked_snprintf(buffer,
                         sizeof(buffer),
                         "vb_%d:last_persisted_snap_start",
                         vb->getId().get());
        add_casted_stat(buffer, range.start, add_stat, cookie);
        checked_snprintf(buffer,
                         sizeof(buffer),
                         "vb_%d:last_persisted_snap_end",
                         vb->getId().get());
        add_casted_stat(buffer, range.end, add_stat, cookie);
    } catch (std::exception& error) {
        EP_LOG_WARN("addSeqnoVbStats: error building stats: {}", error.what());
    }
}

void EventuallyPersistentEngine::addLookupResult(const void* cookie,
                                                 std::unique_ptr<Item> result) {
    LockHolder lh(lookupMutex);
    auto it = lookups.find(cookie);
    if (it != lookups.end()) {
        if (it->second != NULL) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doSeqnoStats(const void *cookie,
                                                          ADD_STAT add_stat,
                                                          const char* stat_key,
                                                          int nkey) {
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

void EventuallyPersistentEngine::addLookupAllKeys(const void *cookie,
                                                  ENGINE_ERROR_CODE err) {
    LockHolder lh(lookupMutex);
    allKeysLookups[cookie] = err;
}

void EventuallyPersistentEngine::runDefragmenterTask(void) {
    kvBucket->runDefragmenterTask();
}

bool EventuallyPersistentEngine::runAccessScannerTask(void) {
    return kvBucket->runAccessScannerTask();
}

void EventuallyPersistentEngine::runVbStatePersistTask(Vbid vbid) {
    kvBucket->runVbStatePersistTask(vbid);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCollectionStats(
        const void* cookie, ADD_STAT add_stat, const std::string& statKey) {
    return Collections::Manager::doCollectionStats(
            *kvBucket, cookie, add_stat, statKey);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doScopeStats(
        const void* cookie, ADD_STAT add_stat, const std::string& statKey) {
    return Collections::Manager::doScopeStats(
            *kvBucket, cookie, add_stat, statKey);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
    ScopeTimer2<MicrosecondStopwatch, TracerStopwatch> timer(
            MicrosecondStopwatch(stats.getStatsCmdHisto),
            TracerStopwatch(cookie, cb::tracing::TraceCode::GETSTATS));

    const std::string statKey(stat_key, nkey);

    if (statKey.size()) {
        EP_LOG_DEBUG("stats {}", stat_key);
    } else {
        EP_LOG_DEBUG("stats engine");
    }

    ENGINE_ERROR_CODE rv = ENGINE_KEY_ENOENT;
    if (statKey.empty()) {
        rv = doEngineStats(cookie, add_stat);
    } else if (nkey > 7 && cb_isPrefix(statKey, "dcpagg ")) {
        rv = doConnAggStats(cookie, add_stat, stat_key + 7, nkey - 7);
    } else if (statKey == "dcp") {
        rv = doDcpStats(cookie, add_stat);
    } else if (statKey == "eviction") {
        // Only return eviction stats if hifi_mfu eviction policy is used.
        rv = (configuration.getHtEvictionPolicy() == "hifi_mfu")
                     ? doEvictionStats(cookie, add_stat)
                     : ENGINE_EINVAL;
    } else if (statKey == "hash") {
        rv = doHashStats(cookie, add_stat);
    } else if (statKey == "vbucket") {
        rv = doVBucketStats(cookie, add_stat, stat_key, nkey, false, false);
    } else if (cb_isPrefix(statKey, "vbucket-details")) {
        rv = doVBucketStats(cookie, add_stat, stat_key, nkey, false, true);
    } else if (cb_isPrefix(statKey, "vbucket-seqno")) {
        rv = doSeqnoStats(cookie, add_stat, stat_key, nkey);
    } else if (statKey == "prev-vbucket") {
        rv = doVBucketStats(cookie, add_stat, stat_key, nkey, true, false);
    } else if (cb_isPrefix(statKey, "checkpoint")) {
        rv = doCheckpointStats(cookie, add_stat, stat_key, nkey);
    } else if (cb_isPrefix(statKey, "durability-monitor")) {
        rv = doDurabilityMonitorStats(cookie, add_stat, stat_key, nkey);
    } else if (statKey == "timings") {
        rv = doTimingStats(cookie, add_stat);
    } else if (statKey == "dispatcher") {
        rv = doDispatcherStats(cookie, add_stat);
    } else if (statKey == "tasks") {
        rv = doTasksStats(cookie, add_stat);
    } else if (statKey == "scheduler") {
        rv = doSchedulerStats(cookie, add_stat);
    } else if (statKey == "runtimes") {
        rv = doRunTimeStats(cookie, add_stat);
    } else if (statKey == "memory") {
        rv = doMemoryStats(cookie, add_stat);
    } else if (statKey == "uuid") {
        add_casted_stat("uuid", configuration.getUuid(), add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey > 4 && cb_isPrefix(statKey, "key ")) {
        std::string key;
        std::string vbid;
        std::string s_key(statKey.substr(4, nkey - 4));
        std::stringstream ss(s_key);
        ss >> key;
        ss >> vbid;
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        Vbid vbucketId = Vbid(vbucket_id);
        // Non-validating, non-blocking version
        // @todo MB-30524: Collection - getStats needs DocNamespace
        rv = doKeyStats(cookie,
                        add_stat,
                        vbucketId,
                        DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                               key.size(),
                               DocKeyEncodesCollectionId::No),
                        false);
    } else if (nkey > 5 && cb_isPrefix(statKey, "vkey ")) {
        std::string key;
        std::string vbid;
        std::string s_key(statKey.substr(5, nkey - 5));
        std::stringstream ss(s_key);
        ss >> key;
        ss >> vbid;
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        Vbid vbucketId = Vbid(vbucket_id);
        // Validating version; blocks
        // @todo MB-30524: Collection - getStats needs DocNamespace
        rv = doKeyStats(cookie,
                        add_stat,
                        vbucketId,
                        DocKey(reinterpret_cast<const uint8_t*>(key.data()),
                               key.size(),
                               DocKeyEncodesCollectionId::No),
                        true);
    } else if (statKey == "kvtimings") {
        getKVBucket()->addKVStoreTimingStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (statKey == "kvstore") {
        getKVBucket()->addKVStoreStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (statKey == "warmup") {
        const auto* warmup = getKVBucket()->getWarmup();
        if (warmup != nullptr) {
            warmup->addStats(add_stat, cookie);
            rv = ENGINE_SUCCESS;
        }

    } else if (statKey == "info") {
        add_casted_stat("info", get_stats_info(), add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (statKey == "allocator") {
        char buffer[64 * 1024];
        MemoryTracker::getInstance(*getServerApiFunc()->alloc_hooks)->
                getDetailedStats(buffer, sizeof(buffer));
        add_casted_stat("detailed", buffer, add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (statKey == "config") {
        configuration.addStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey > 15 && cb_isPrefix(statKey, "dcp-vbtakeover")) {
        std::string tStream;
        std::string vbid;
        std::string buffer(statKey.substr(15, nkey - 15));
        std::stringstream ss(buffer);
        ss >> vbid;
        ss >> tStream;
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        Vbid vbucketId = Vbid(vbucket_id);
        rv = doDcpVbTakeoverStats(cookie, add_stat, tStream, vbucketId);
    } else if (statKey == "workload") {
        return doWorkloadStats(cookie, add_stat);
    } else if (cb_isPrefix(statKey, "failovers")) {
        if (nkey == 9) {
            rv = doAllFailoverLogStats(cookie, add_stat);
        } else if (statKey.compare(std::string("failovers").length(),
                                   std::string(" ").length(),
                                   " ") == 0) {
            std::string vbid;
            std::string s_key(statKey.substr(10, nkey - 10));
            std::stringstream ss(s_key);
            ss >> vbid;
            uint16_t vbucket_id(0);
            parseUint16(vbid.c_str(), &vbucket_id);
            Vbid vbucketId = Vbid(vbucket_id);
            rv = doVbIdFailoverLogStats(cookie, add_stat, vbucketId);
        }
    } else if (cb_isPrefix(statKey, "diskinfo")) {
        if (nkey == 8) {
            return kvBucket->getFileStats(cookie, add_stat);
        } else if ((nkey == 15) &&
                (statKey.compare(std::string("diskinfo").length() + 1,
                                 std::string("detail").length(),
                                 "detail") == 0)) {
            return kvBucket->getPerVBucketDiskStats(cookie, add_stat);
        } else {
            return ENGINE_EINVAL;
        }
    } else if (cb_isPrefix(statKey, "collections")) {
        rv = doCollectionStats(cookie, add_stat, std::string(stat_key, nkey));
    } else if (cb_isPrefix(statKey, "scopes")) {
        rv = doScopeStats(cookie, add_stat, std::string(stat_key, nkey));
    } else if (statKey[0] == '_') {
        // Privileged stats - need Stats priv (and not just SimpleStats).
        switch (getServerApi()->cookie->check_privilege(
                cookie, cb::rbac::Privilege::Stats)) {
        case cb::rbac::PrivilegeAccess::Fail:
        case cb::rbac::PrivilegeAccess::Stale:
            return ENGINE_EACCESS;

        case cb::rbac::PrivilegeAccess::Ok:
            if (cb_isPrefix(statKey, "_checkpoint-dump")) {
                const size_t keyLen = strlen("_checkpoint-dump");
                cb::const_char_buffer keyArgs(statKey.data() + keyLen,
                                              statKey.size() - keyLen);
                rv = doCheckpointDump(cookie, add_stat, keyArgs);
            } else if (cb_isPrefix(statKey, "_hash-dump")) {
                const size_t keyLen = strlen("_hash-dump");
                cb::const_char_buffer keyArgs(statKey.data() + keyLen,
                                              statKey.size() - keyLen);
                rv = doHashDump(cookie, add_stat, keyArgs);
            }
            break;
        }
    }

    return rv;
}

void EventuallyPersistentEngine::resetStats() {
    stats.reset();
    if (kvBucket) {
        kvBucket->resetUnderlyingStats();
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe(
        const void* cookie,
        const cb::mcbp::Request& req,
        ADD_RESPONSE response) {
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
                     cb::UserDataView(key.data(), key.size()),
                     vb_id);

        // Get key stats
        uint16_t keystatus = 0;
        struct key_stats kstats = {};
        ENGINE_ERROR_CODE rv = kvBucket->getKeyStats(
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
                        nullptr,
                        0,
                        nullptr,
                        0,
                        result_string.data(),
                        gsl::narrow<uint32_t>(result_string.length()),
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        persist_time,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe_seqno(
        const void* cookie,
        const cb::mcbp::Request& request,
        ADD_RESPONSE response) {
    Vbid vb_id = request.getVBucket();
    auto value = request.getValue();
    uint64_t vb_uuid = static_cast<uint64_t>(
            ntohll(*reinterpret_cast<const uint64_t*>(value.data())));

    EP_LOG_DEBUG("Observing {} with uuid: {}", vb_id, vb_uuid);

    VBucketPtr vb = kvBucket->getVBucket(vb_id);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
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
                        nullptr,
                        0,
                        0,
                        0,
                        result.str().data(),
                        gsl::narrow<uint32_t>(result.str().length()),
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

VBucketPtr EventuallyPersistentEngine::getVBucket(Vbid vbucket) {
    return kvBucket->getVBucket(vbucket);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleLastClosedCheckpoint(
        const void* cookie,
        const cb::mcbp::Request& request,
        ADD_RESPONSE response) {
    VBucketPtr vb = getVBucket(request.getVBucket());
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    const uint64_t id =
            htonll(vb->checkpointManager->getLastClosedCheckpointId());
    return sendResponse(response,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        &id,
                        sizeof(id),
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleCreateCheckpoint(
        const void* cookie,
        const cb::mcbp::Request& req,
        ADD_RESPONSE response) {
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
        char val[sizeof(checkpointId) + sizeof(persistedChkId)];
        memcpy(val, &checkpointId, sizeof(checkpointId));
        memcpy(val + sizeof(checkpointId),
               &persistedChkId,
               sizeof(persistedChkId));
        return sendResponse(response,
                            NULL,
                            0,
                            NULL,
                            0,
                            val,
                            sizeof(val),
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Success,
                            0,
                            cookie);
    }

    return sendResponse(response,
                        NULL,
                        0,
                        NULL,
                        0,
                        NULL,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleCheckpointPersistence(
        const void* cookie,
        const cb::mcbp::Request& request,
        ADD_RESPONSE response) {
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
        storeEngineSpecific(cookie, NULL);
        EP_LOG_DEBUG("Checkpoint {} persisted for {}", chk_id, vbucket);
    }

    return sendResponse(response,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        status,
                        0,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleSeqnoPersistence(
        const void* cookie,
        const cb::mcbp::Request& req,
        ADD_RESPONSE response) {
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
                        nullptr,
                        0,
                        nullptr,
                        0,
                        nullptr,
                        0,
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

bool EventuallyPersistentEngine::decodeWithMetaOptions(
        cb::const_byte_buffer extras,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts,
        PermittedVBStates& permittedVBStates) {
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
    }

    // Validate options
    // 1) If GenerateCas::Yes then we must have CheckConflicts::No
    bool check1 = generateCas == GenerateCas::Yes &&
                  checkConflicts == CheckConflicts::Yes;

    // 2) If bucket is LWW and forceFlag is not set and GenerateCas::No
    bool check2 = configuration.getConflictResolutionType() == "lww" &&
                  !forceFlag && generateCas == GenerateCas::No;

    // 3) If bucket is not LWW then forceFlag must be false.
    bool check3 =
            configuration.getConflictResolutionType() != "lww" && forceFlag;

    // So if either check1/2/3 is true, return false
    return !(check1 || check2 || check3);
}

protocol_binary_datatype_t EventuallyPersistentEngine::checkForDatatypeJson(
        const void* cookie,
        protocol_binary_datatype_t datatype,
        cb::const_char_buffer body) {
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
        ADD_RESPONSE response) {
    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    const auto extras = request.getExtdata();

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    if (!decodeWithMetaOptions(
                extras, generateCas, checkConflicts, permittedVBStates)) {
        return ENGINE_EINVAL;
    }

    auto value = request.getValue();
    cb::const_byte_buffer emd;
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

    if (value.size() > maxItemSize) {
        EP_LOG_WARN(
                "Item value size {} for setWithMeta is bigger "
                "than the max size {} allowed!!!",
                value.size(),
                maxItemSize);
        return ENGINE_E2BIG;
    }

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
    TRACE_BEGIN(cookie, TraceCode::SETWITHMETA, startTime);

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
        ++stats.numOpsSetMeta;
        auto endTime = std::chrono::steady_clock::now();
        TRACE_END(cookie, TraceCode::SETWITHMETA, endTime);
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

    if (!isDatatypeSupported(cookie, PROTOCOL_BINARY_DATATYPE_SNAPPY) &&
            mcbp::datatype::is_snappy(datatype)) {
        return ENGINE_EINVAL;
    }

    cb::const_char_buffer payload(reinterpret_cast<const char*>(value.data()),
                                  value.size());

    cb::const_byte_buffer finalValue = value;
    protocol_binary_datatype_t finalDatatype = datatype;
    cb::compression::Buffer uncompressedValue;
    if (mcbp::datatype::is_snappy(datatype)) {
        if (!cb::compression::inflate(cb::compression::Algorithm::Snappy,
                                      payload, uncompressedValue)) {
            return ENGINE_EINVAL;
        }

        if (compressionMode == BucketCompressionMode::Off) {
            finalValue = uncompressedValue;
            finalDatatype &= ~PROTOCOL_BINARY_DATATYPE_SNAPPY;
        }
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
        ADD_RESPONSE response) {
    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    const auto extras = request.getExtdata();

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    if (!decodeWithMetaOptions(
                extras, generateCas, checkConflicts, permittedVBStates)) {
        return ENGINE_EINVAL;
    }

    auto value = request.getValue();
    cb::const_byte_buffer emd;
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
                                 DeleteSource::Explicit);
        } else {
            // A delete with a value
            ret = setWithMeta(request.getVBucket(),
                              key,
                              value,
                              {metacas, seqno, flags, time_t(delete_time)},
                              true /*isDeleted*/,
                              uint8_t(request.getDatatype()),
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
                                    false /*allowExisting*/,
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
        ADD_RESPONSE response) {
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
                        nullptr,
                        0,
                        nullptr,
                        0,
                        nullptr,
                        0,
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
                                                 ADD_STAT add_stat,
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

    auto producer = dynamic_pointer_cast<DcpProducer>(conn);
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
                                       ADD_RESPONSE response) {
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
                                   &itm_meta,
                                   mutation_descr);
        if (ret == ENGINE_SUCCESS) {
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

    uint8_t meta[16];
    exp = htonl(exp);
    memcpy(meta, &flags, 4);
    memcpy(meta + 4, &exp, 4);
    memcpy(meta + 8, &seqno, 8);

    return sendResponse(response,
                        nullptr,
                        0,
                        (const void*)meta,
                        16,
                        nullptr,
                        0,
                        datatype,
                        cb::mcbp::Status::Success,
                        cas,
                        cookie);
}

/**
 * Callback class used by AllKeysAPI, for caching fetched keys
 *
 * As by default (or in most cases), number of keys is 1000,
 * and an average key could be 32B in length, initialize buffersize of
 * allKeys to 34000 (1000 * 32 + 1000 * 2), the additional 2 bytes per
 * key is for the keylength.
 *
 * This initially allocated buffersize is doubled whenever the length
 * of the buffer holding all the keys, crosses the buffersize.
 */
class AllKeysCallback : public Callback<const DocKey&> {
public:
    AllKeysCallback(bool encodeCollectionID)
        : encodeCollectionID(encodeCollectionID) {
        buffer.reserve((avgKeySize + sizeof(uint16_t)) * expNumKeys);
    }

    void callback(const DocKey& key) {
        DocKey outKey = key;
        if (key.getCollectionID() == CollectionID::System) {
            // Skip system collection keys
            return;
        } else if (!encodeCollectionID &&
                   key.getCollectionID().isDefaultCollection()) {
            // Only default collection key can be sent back if
            // encodeCollectionID is false
            outKey = key.makeDocKeyWithoutCollectionID();
        }

        if (buffer.size() + outKey.size() + sizeof(uint16_t) > buffer.size()) {
            // Reserve the 2x space for the copy-to buffer.
            buffer.reserve(buffer.size()*2);
        }
        uint16_t outlen = htons(outKey.size());
        // insert 1 x u16
        const auto* outlenPtr = reinterpret_cast<const char*>(&outlen);
        buffer.insert(buffer.end(), outlenPtr, outlenPtr + sizeof(uint16_t));
        // insert the char buffer
        buffer.insert(
                buffer.end(), outKey.data(), outKey.data() + outKey.size());
    }

    char* getAllKeysPtr() { return buffer.data(); }
    uint64_t getAllKeysLen() { return buffer.size(); }

private:
    std::vector<char> buffer;
    bool encodeCollectionID{false};
    static const int avgKeySize = 32;
    static const int expNumKeys = 1000;

};

/*
 * Task that fetches all_docs and returns response,
 * runs in background.
 */
class FetchAllKeysTask : public GlobalTask {
public:
    FetchAllKeysTask(EventuallyPersistentEngine* e,
                     const void* c,
                     ADD_RESPONSE resp,
                     const DocKey start_key_,
                     Vbid vbucket,
                     uint32_t count_,
                     bool encodeCollectionID)
        : GlobalTask(e, TaskId::FetchAllKeysTask, 0, false),
          engine(e),
          cookie(c),
          description("Running the ALL_DOCS api on " + vbucket.to_string()),
          response(resp),
          start_key(start_key_),
          vbid(vbucket),
          count(count_),
          encodeCollectionID(encodeCollectionID) {
    }

    std::string getDescription() {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() {
        // Duration will be a function of how many documents are fetched;
        // however for simplicity just return a fixed "reasonable" duration.
        return std::chrono::milliseconds(100);
    }

    bool run() {
        TRACE_EVENT0("ep-engine/task", "FetchAllKeysTask");
        ENGINE_ERROR_CODE err;
        if (engine->getKVBucket()->getVBuckets().
                getBucket(vbid)->isBucketCreation()) {
            // Returning an empty packet with a SUCCESS response as
            // there aren't any keys during the vbucket file creation.
            err = sendResponse(response,
                               NULL,
                               0,
                               NULL,
                               0,
                               NULL,
                               0,
                               PROTOCOL_BINARY_RAW_BYTES,
                               cb::mcbp::Status::Success,
                               0,
                               cookie);
        } else {
            auto cb = std::make_shared<AllKeysCallback>(encodeCollectionID);
            err = engine->getKVBucket()->getROUnderlying(vbid)->getAllKeys(
                                                    vbid, start_key, count, cb);
            if (err == ENGINE_SUCCESS) {
                err = sendResponse(
                        response,
                        NULL,
                        0,
                        NULL,
                        0,
                        ((AllKeysCallback*)cb.get())->getAllKeysPtr(),
                        ((AllKeysCallback*)cb.get())->getAllKeysLen(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
            }
        }
        engine->addLookupAllKeys(cookie, err);
        engine->notifyIOComplete(cookie, err);
        return false;
    }

private:
    EventuallyPersistentEngine *engine;
    const void *cookie;
    const std::string description;
    ADD_RESPONSE response;
    StoredDocKey start_key;
    Vbid vbid;
    uint32_t count;
    bool encodeCollectionID{false};
};

ENGINE_ERROR_CODE
EventuallyPersistentEngine::getAllKeys(const void* cookie,
                                       const cb::mcbp::Request& request,
                                       ADD_RESPONSE response) {
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

    ReaderLockHolder rlh(vb->getStateLock());
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
    ExTask task =
            std::make_shared<FetchAllKeysTask>(this,
                                               cookie,
                                               response,
                                               start_key,
                                               request.getVBucket(),
                                               count,
                                               isCollectionsSupported(cookie));
    ExecutorPool::get()->schedule(task);
    return ENGINE_EWOULDBLOCK;
}

CONN_PRIORITY EventuallyPersistentEngine::getDCPPriority(const void* cookie) {
    NonBucketAllocationGuard guard;
    auto priority = serverApi->cookie->get_priority(cookie);
    return priority;
}

void EventuallyPersistentEngine::setDCPPriority(const void* cookie,
                                                CONN_PRIORITY priority) {
    NonBucketAllocationGuard guard;
    serverApi->cookie->set_priority(cookie, priority);
}

void EventuallyPersistentEngine::notifyIOComplete(const void* cookie,
                                                  ENGINE_ERROR_CODE status) {
    if (cookie == NULL) {
        EP_LOG_WARN("Tried to signal a NULL cookie!");
    } else {
        BlockTimer bt(&stats.notifyIOHisto);
        NonBucketAllocationGuard guard;
        serverApi->cookie->notify_io_complete(cookie, status);
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getRandomKey(const void *cookie,
                                                       ADD_RESPONSE response) {
    GetValue gv(kvBucket->getRandomKey());
    ENGINE_ERROR_CODE ret = gv.getStatus();

    if (ret == ENGINE_SUCCESS) {
        Item* it = gv.item.get();
        uint32_t flags = it->getFlags();
        ret = sendResponse(response,
                           static_cast<const void*>(it->getKey().data()),
                           it->getKey().size(),
                           (const void*)&flags,
                           sizeof(uint32_t),
                           static_cast<const void*>(it->getData()),
                           it->getNBytes(),
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
        cb::const_char_buffer stream_name) {
    (void) opaque;
    (void) seqno;
    std::string connName = cb::to_string(stream_name);

    if (getEngineSpecific(cookie) != NULL) {
        EP_LOG_WARN(
                "Cannot open DCP connection as another"
                " connection exists on the same socket");
        return ENGINE_DISCONNECT;
    }

    ConnHandler *handler = NULL;
    if (flags & (cb::mcbp::request::DcpOpenPayload::Producer |
                 cb::mcbp::request::DcpOpenPayload::Notifier)) {
        handler = dcpConnMap_->newProducer(cookie, connName, flags);
    } else {
        handler = dcpConnMap_->newConsumer(cookie, connName);
    }

    if (handler == nullptr) {
        EP_LOG_WARN("EPEngine::dcpOpen: failed to create a handler");
        return ENGINE_DISCONNECT;
    }

    // Success creating dcp object which has stored the cookie, now reserve it.
    if (reserveCookie(cookie) != ENGINE_SUCCESS) {
        EP_LOG_WARN(
                "Cannot create DCP connection because cookie "
                "cannot be reserved");
        return ENGINE_DISCONNECT;
    }

    storeEngineSpecific(cookie, handler);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::dcpAddStream(const void* cookie,
                                                           uint32_t opaque,
                                                           Vbid vbucket,
                                                           uint32_t flags) {
    ENGINE_ERROR_CODE errCode = ENGINE_DISCONNECT;
    ConnHandler* conn = getConnHandler(cookie);
    if (conn) {
        errCode = dcpConnMap_->addPassiveStream(*conn, opaque, vbucket, flags);
    }
    return errCode;
}

ConnHandler* EventuallyPersistentEngine::getConnHandler(const void *cookie) {
    void* specific = getEngineSpecific(cookie);
    ConnHandler* handler = reinterpret_cast<ConnHandler*>(specific);
    if (!handler) {
        EP_LOG_WARN("Invalid streaming connection");
    }
    return handler;
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
    if (getEngineSpecific(cookie) != NULL) {
        switch (getOpcodeIfEwouldblockSet(cookie)) {
        case cb::mcbp::ClientOpcode::DelVbucket:
        case cb::mcbp::ClientOpcode::CompactDb: {
            decrementSessionCtr();
            storeEngineSpecific(cookie, NULL);
            break;
        }
            default:
                break;
            }
    }
}

void EventuallyPersistentEngine::handleDeleteBucket(const void *cookie) {
    EP_LOG_INFO(
            "Shutting down all DCP connections in "
            "preparation for bucket deletion.");
    dcpConnMap_->shutdownAllConnections();
}

cb::mcbp::Status EventuallyPersistentEngine::stopFlusher(const char** msg,
                                                         size_t* msg_size) {
    (void)msg_size;
    auto rv = cb::mcbp::Status::Success;
    *msg = NULL;
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
    *msg = NULL;
    if (!kvBucket->resumeFlusher()) {
        EP_LOG_DEBUG("Unable to start flusher");
        *msg = "Flusher not shut down.";
        rv = cb::mcbp::Status::Einval;
    }
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteVBucket(Vbid vbid,
                                                            const void* c) {
    return kvBucket->deleteVBucket(vbid, c);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::compactDB(
        Vbid vbid, const CompactionConfig& c, const void* cookie) {
    return kvBucket->scheduleCompaction(vbid, c, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getAllVBucketSequenceNumbers(
        const void* cookie,
        const cb::mcbp::Request& request,
        ADD_RESPONSE response) {
    static_assert(sizeof(vbucket_state_t) == 4,
                  "Unexpected size for vbucket_state_t");
    auto extras = request.getExtdata();
    auto reqState = static_cast<vbucket_state_t>(0);
    ;

    // if extlen is non-zero, it limits the result to only include the
    // vbuckets in the specified vbucket state.
    if (extras.size() > 0) {
        if (extras.size() != sizeof(vbucket_state_t)) {
            return ENGINE_EINVAL;
        }
        reqState = static_cast<vbucket_state_t>(
                ntohl(*reinterpret_cast<const uint32_t*>(extras.data())));
    }

    std::vector<uint8_t> payload;
    auto vbuckets = kvBucket->getVBuckets().getBuckets();

    /* Reserve a buffer that's big enough to hold all of them (we might
     * not use all of them. Each entry in the array occupies 10 bytes
     * (two bytes vbucket id followed by 8 bytes sequence number)
     */
    try {
        payload.reserve(vbuckets.size() * (sizeof(uint16_t) + sizeof(uint64_t)));
    } catch (const std::bad_alloc&) {
        return sendResponse(response,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            cb::mcbp::Status::Enomem,
                            0,
                            cookie);
    }

    for (auto id : vbuckets) {
        VBucketPtr vb = getVBucket(id);
        if (vb) {
            auto state = vb->getState();
            bool getSeqnoForThisVb = false;
            if (reqState) {
                getSeqnoForThisVb = (reqState == state);
            } else {
                getSeqnoForThisVb = (state == vbucket_state_active) ||
                                    (state == vbucket_state_replica) ||
                                    (state == vbucket_state_pending);
            }
            if (getSeqnoForThisVb) {
                Vbid vbid = id.hton();
                uint64_t highSeqno;
                if (vb->getState() == vbucket_state_active) {
                    highSeqno = htonll(vb->getHighSeqno());
                } else {
                    snapshot_info_t info =
                            vb->checkpointManager->getSnapshotInfo();
                    highSeqno = htonll(info.range.end);
                }
                auto offset = payload.size();
                payload.resize(offset + sizeof(vbid) + sizeof(highSeqno));
                memcpy(payload.data() + offset, &vbid, sizeof(vbid));
                memcpy(payload.data() + offset + sizeof(vbid), &highSeqno,
                       sizeof(highSeqno));
            }
        }
    }

    return sendResponse(response,
                        0,
                        0, /* key */
                        0,
                        0, /* ext field */
                        payload.data(),
                        gsl::narrow<uint32_t>(payload.size()), /* value */
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
        ADD_RESPONSE response,
        cb::mcbp::Status status,
        uint64_t cas,
        const void* cookie) {
    // no body/ext data for the error
    return sendResponse(response,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        nullptr,
                        0,
                        0,
                        status,
                        cas,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::sendMutationExtras(
        ADD_RESPONSE response,
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
    uint8_t meta[16];
    memcpy(meta, &uuid, sizeof(uuid));
    memcpy(meta + sizeof(uuid), &bySeqno, sizeof(bySeqno));
    return sendResponse(response,
                        nullptr,
                        0,
                        (const void*)meta,
                        sizeof(meta),
                        nullptr,
                        0,
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
        ADD_RESPONSE response,
        Vbid vbid,
        vbucket_state_t to,
        const nlohmann::json& meta,
        bool transfer,
        uint64_t cas) {
    auto status = kvBucket->setVBucketState(vbid, to, transfer, cookie);

    if (status == ENGINE_EWOULDBLOCK) {
        return status;
    } else if (status == ENGINE_ERANGE) {
        setErrorContext(cookie, "VBucket number too big");
    }

    return sendResponse(response,
                        NULL,
                        0,
                        NULL,
                        0,
                        NULL,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        serverApi->cookie->engine_error2mcbp(cookie, status),
                        cas,
                        cookie);
}

EventuallyPersistentEngine::~EventuallyPersistentEngine() {
    if (kvBucket) {
        kvBucket->deinitialize();
    }
    EP_LOG_INFO("~EPEngine: Completed deinitialize.");
    delete workload;
    delete checkpointConfig;
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

WorkLoadPolicy&  EpEngineTaskable::getWorkLoadPolicy(void) {
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
            EP_LOG_WARN(R"(Transitioning from "{}"->"{}" compression mode)",
                        to_string(oldCompressionMode),
                        compressModeStr);
        }
    } catch (const std::invalid_argument& e) {
        EP_LOG_WARN("{}", e.what());
    }
}
