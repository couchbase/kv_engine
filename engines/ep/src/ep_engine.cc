/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "checkpoint.h"
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
#include "logger.h"
#include "memory_tracker.h"
#include "replicationthrottle.h"
#include "stats-info.h"
#include "statwriter.h"
#include "string_utils.h"
#include "vb_count_visitor.h"
#include "warmup.h"

#include <JSON_checker.h>
#include <cJSON_utils.h>
#include <memcached/engine.h>
#include <memcached/extension.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_api.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/make_unique.h>
#include <platform/platform.h>
#include <platform/processclock.h>
#include <xattr/utils.h>

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdarg.h>
#include <string>
#include <vector>

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

static inline EPHandle acquireEngine(ENGINE_HANDLE* handle) {
    auto ret = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(ret);

    return EPHandle(ret);
}

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static ENGINE_ERROR_CODE sendResponse(ADD_RESPONSE response, const void *key,
                                      uint16_t keylen,
                                      const void *ext, uint8_t extlen,
                                      const void *body, uint32_t bodylen,
                                      uint8_t datatype, uint16_t status,
                                      uint64_t cas, const void *cookie)
{
    ENGINE_ERROR_CODE rv = ENGINE_FAILED;
    EventuallyPersistentEngine *e = ObjectRegistry::onSwitchThread(NULL, true);
    if (response(key, keylen, ext, extlen, body, bodylen, datatype,
                 status, cas, cookie)) {
        rv = ENGINE_SUCCESS;
    }
    ObjectRegistry::onSwitchThread(e);
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

static const engine_info* EvpGetInfo(gsl::not_null<ENGINE_HANDLE*> handle) {
    return acquireEngine(handle)->getInfo();
}

static ENGINE_ERROR_CODE EvpInitialize(gsl::not_null<ENGINE_HANDLE*> handle,
                                       const char* config_str) {
    return acquireEngine(handle)->initialize(config_str);
}

static void EvpDestroy(gsl::not_null<ENGINE_HANDLE*> handle, const bool force) {
    auto eng = acquireEngine(handle);
    eng->destroy(force);
    delete eng.get();
}

static cb::EngineErrorItemPair EvpItemAllocate(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        const size_t nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        uint16_t vbucket) {
    if (!mcbp::datatype::is_valid(datatype)) {
        LOG(EXTENSION_LOG_WARNING, "Invalid value for datatype "
            " (ItemAllocate)");
        return cb::makeEngineErrorItemPair(cb::engine_errc::invalid_arguments);
    }

    item* itm = nullptr;
    auto ret = acquireEngine(handle)->itemAllocate(&itm,
                                                   key,
                                                   nbytes,
                                                   0, // No privileged bytes
                                                   flags,
                                                   exptime,
                                                   datatype,
                                                   vbucket);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, handle);
}

static bool EvpGetItemInfo(gsl::not_null<ENGINE_HANDLE*> handle,
                           gsl::not_null<const item*> itm,
                           gsl::not_null<item_info*> itm_info);
static void EvpItemRelease(gsl::not_null<ENGINE_HANDLE*> handle,
                           gsl::not_null<item*> itm);

static std::pair<cb::unique_item_ptr, item_info> EvpItemAllocateEx(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        size_t nbytes,
        size_t priv_nbytes,
        int flags,
        rel_time_t exptime,
        uint8_t datatype,
        uint16_t vbucket) {
    item* it = nullptr;
    auto err = acquireEngine(handle)->itemAllocate(
            &it, key, nbytes, priv_nbytes, flags, exptime, datatype, vbucket);

    if (err != ENGINE_SUCCESS) {
        throw cb::engine_error(cb::engine_errc(err),
                               "EvpItemAllocateEx: failed to allocate memory");
    }

    item_info info;
    if (!EvpGetItemInfo(handle, it, &info)) {
        EvpItemRelease(handle, it);
        throw cb::engine_error(cb::engine_errc::failed,
                               "EvpItemAllocateEx: EvpGetItemInfo failed");
    }

    return std::make_pair(cb::unique_item_ptr{it, cb::ItemDeleter{handle}},
                          info);
}

static ENGINE_ERROR_CODE EvpItemDelete(gsl::not_null<ENGINE_HANDLE*> handle,
                                       gsl::not_null<const void*> cookie,
                                       const DocKey& key,
                                       uint64_t& cas,
                                       uint16_t vbucket,
                                       mutation_descr_t& mut_info) {
    return acquireEngine(handle)->itemDelete(
            cookie, key, cas, vbucket, nullptr, mut_info);
}

static void EvpItemRelease(gsl::not_null<ENGINE_HANDLE*> handle,
                           gsl::not_null<item*> itm) {
    acquireEngine(handle)->itemRelease(itm);
}

static cb::EngineErrorItemPair EvpGet(gsl::not_null<ENGINE_HANDLE*> handle,
                                      gsl::not_null<const void*> cookie,
                                      const DocKey& key,
                                      uint16_t vbucket,
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
                cb::unique_item_ptr{nullptr, cb::ItemDeleter{handle}});
    case DocStateFilter::AliveOrDeleted:
        options = static_cast<get_options_t>(options | GET_DELETED_VALUE);
        break;
    }

    item* itm = nullptr;
    ENGINE_ERROR_CODE ret =
            acquireEngine(handle)->get(cookie, &itm, key, vbucket, options);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, handle);
}

static cb::EngineErrorItemPair EvpGetIf(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket,
        std::function<bool(const item_info&)> filter) {
    return acquireEngine(handle)->get_if(cookie, key, vbucket, filter);
}

static cb::EngineErrorItemPair EvpGetAndTouch(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket,
        uint32_t expiry_time) {
    return acquireEngine(handle)->get_and_touch(cookie, key, vbucket,
                                                expiry_time);
}

static cb::EngineErrorItemPair EvpGetLocked(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket,
        uint32_t lock_timeout) {
    item* itm = nullptr;
    auto ret = acquireEngine(handle)->get_locked(
            cookie, &itm, key, vbucket, lock_timeout);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), itm, handle);
}

static ENGINE_ERROR_CODE EvpUnlock(gsl::not_null<ENGINE_HANDLE*> handle,
                                   gsl::not_null<const void*> cookie,
                                   const DocKey& key,
                                   uint16_t vbucket,
                                   uint64_t cas) {
    return acquireEngine(handle)->unlock(cookie, key, vbucket, cas);
}

static ENGINE_ERROR_CODE EvpGetStats(gsl::not_null<ENGINE_HANDLE*> handle,
                                     gsl::not_null<const void*> cookie,
                                     cb::const_char_buffer key,
                                     ADD_STAT add_stat) {
    return acquireEngine(handle)->getStats(
            cookie, key.data(), gsl::narrow_cast<int>(key.size()), add_stat);
}

static ENGINE_ERROR_CODE EvpStore(gsl::not_null<ENGINE_HANDLE*> handle,
                                  gsl::not_null<const void*> cookie,
                                  gsl::not_null<item*> itm,
                                  uint64_t& cas,
                                  ENGINE_STORE_OPERATION operation,
                                  DocumentState document_state) {
    auto engine = acquireEngine(handle);

    if (document_state == DocumentState::Deleted) {
        Item* item = static_cast<Item*>(itm.get());
        item->setDeleted();
    }

    return engine->store(cookie, itm, cas, operation);
}

static cb::EngineErrorCasPair EvpStoreIf(gsl::not_null<ENGINE_HANDLE*> handle,
                                         gsl::not_null<const void*> cookie,
                                         gsl::not_null<item*> itm,
                                         uint64_t cas,
                                         ENGINE_STORE_OPERATION operation,
                                         cb::StoreIfPredicate predicate,
                                         DocumentState document_state) {
    auto engine = acquireEngine(handle);

    Item& item = static_cast<Item&>(*static_cast<Item*>(itm.get()));

    if (document_state == DocumentState::Deleted) {
        item.setDeleted();
    }
    return engine->store_if(cookie, item, cas, operation, predicate);
}

static ENGINE_ERROR_CODE EvpFlush(gsl::not_null<ENGINE_HANDLE*> handle,
                                  gsl::not_null<const void*> cookie) {
    return acquireEngine(handle)->flush(cookie);
}

static void EvpResetStats(gsl::not_null<ENGINE_HANDLE*> handle, gsl::not_null<const void*> cookie) {
    acquireEngine(handle)->resetStats();
}

protocol_binary_response_status EventuallyPersistentEngine::setReplicationParam(
        const char* keyz, const char* valz, std::string& msg) {
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    try {
        if (strcmp(keyz, "replication_throttle_threshold") == 0) {
            getConfiguration().setReplicationThrottleThreshold(
                    std::stoull(valz));
        } else if (strcmp(keyz, "replication_throttle_queue_cap") == 0) {
            getConfiguration().setReplicationThrottleQueueCap(std::stoll(valz));
        } else if (strcmp(keyz, "replication_throttle_cap_pcnt") == 0) {
            getConfiguration().setReplicationThrottleCapPcnt(std::stoull(valz));
        } else {
            msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles any miscellaenous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return rv;
}

protocol_binary_response_status EventuallyPersistentEngine::setCheckpointParam(
        const char* keyz, const char* valz, std::string& msg) {
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    try {
        if (strcmp(keyz, "chk_max_items") == 0) {
            size_t v = std::stoull(valz);
            validate(v, size_t(MIN_CHECKPOINT_ITEMS),
                     size_t(MAX_CHECKPOINT_ITEMS));
            getConfiguration().setChkMaxItems(v);
        } else if (strcmp(keyz, "chk_period") == 0) {
            size_t v = std::stoull(valz);
            validate(v, size_t(MIN_CHECKPOINT_PERIOD),
                     size_t(MAX_CHECKPOINT_PERIOD));
            getConfiguration().setChkPeriod(v);
        } else if (strcmp(keyz, "max_checkpoints") == 0) {
            size_t v = std::stoull(valz);
            validate(v, size_t(DEFAULT_MAX_CHECKPOINTS),
                     size_t(MAX_CHECKPOINTS_UPPER_BOUND));
            getConfiguration().setMaxCheckpoints(v);
        } else if (strcmp(keyz, "item_num_based_new_chk") == 0) {
            getConfiguration().setItemNumBasedNewChk(cb_stob(valz));
        } else if (strcmp(keyz, "keep_closed_chks") == 0) {
            getConfiguration().setKeepClosedChks(cb_stob(valz));
        } else if (strcmp(keyz, "enable_chk_merge") == 0) {
            getConfiguration().setEnableChkMerge(cb_stob(valz));
        } else {
            msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }

        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles any miscellaenous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return rv;
}

protocol_binary_response_status EventuallyPersistentEngine::setFlushParam(
        const char* keyz, const char* valz, std::string& msg) {
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    // Handle the actual mutation.
    try {
        if (strcmp(keyz, "bg_fetch_delay") == 0) {
            getConfiguration().setBgFetchDelay(std::stoull(valz));
        } else if (strcmp(keyz, "flushall_enabled") == 0) {
            getConfiguration().setFlushallEnabled(cb_stob(valz));
        } else if (strcmp(keyz, "max_size") == 0) {
            size_t vsize = std::stoull(valz);

            getConfiguration().setMaxSize(vsize);
            EPStats& st = getEpStats();
            getConfiguration().setMemLowWat(
                    percentOf(vsize, st.mem_low_wat_percent));
            getConfiguration().setMemHighWat(
                    percentOf(vsize, st.mem_high_wat_percent));
        } else if (strcmp(keyz, "mem_low_wat") == 0) {
            getConfiguration().setMemLowWat(std::stoull(valz));
        } else if (strcmp(keyz, "mem_high_wat") == 0) {
            getConfiguration().setMemHighWat(std::stoull(valz));
        } else if (strcmp(keyz, "backfill_mem_threshold") == 0) {
            getConfiguration().setBackfillMemThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "compaction_exp_mem_threshold") == 0) {
            getConfiguration().setCompactionExpMemThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "mutation_mem_threshold") == 0) {
            getConfiguration().setMutationMemThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "timing_log") == 0) {
            EPStats& stats = getEpStats();
            std::ostream* old = stats.timingLog;
            stats.timingLog = NULL;
            delete old;
            if (strcmp(valz, "off") == 0) {
                LOG(EXTENSION_LOG_INFO, "Disabled timing log.");
            } else {
                std::ofstream* tmp(new std::ofstream(valz));
                if (tmp->good()) {
                    LOG(EXTENSION_LOG_INFO,
                        "Logging detailed timings to ``%s''.", valz);
                    stats.timingLog = tmp;
                } else {
                    LOG(EXTENSION_LOG_WARNING,
                        "Error setting detailed timing log to ``%s'':  %s",
                        valz, strerror(errno));
                    delete tmp;
                }
            }
        } else if (strcmp(keyz, "exp_pager_enabled") == 0) {
            getConfiguration().setExpPagerEnabled(cb_stob(valz));
        } else if (strcmp(keyz, "exp_pager_stime") == 0) {
            getConfiguration().setExpPagerStime(std::stoull(valz));
        } else if (strcmp(keyz, "exp_pager_initial_run_time") == 0) {
            getConfiguration().setExpPagerInitialRunTime(std::stoll(valz));
        } else if (strcmp(keyz, "access_scanner_enabled") == 0) {
            getConfiguration().requirementsMetOrThrow("access_scanner_enabled");
            getConfiguration().setAccessScannerEnabled(cb_stob(valz));
        } else if (strcmp(keyz, "alog_sleep_time") == 0) {
            getConfiguration().requirementsMetOrThrow("alog_sleep_time");
            getConfiguration().setAlogSleepTime(std::stoull(valz));
        } else if (strcmp(keyz, "alog_task_time") == 0) {
            getConfiguration().requirementsMetOrThrow("alog_task_time");
            getConfiguration().setAlogTaskTime(std::stoull(valz));
        } else if (strcmp(keyz, "pager_active_vb_pcnt") == 0) {
            getConfiguration().setPagerActiveVbPcnt(std::stoull(valz));
        } else if (strcmp(keyz, "warmup_min_memory_threshold") == 0) {
            getConfiguration().setWarmupMinMemoryThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "warmup_min_items_threshold") == 0) {
            getConfiguration().setWarmupMinItemsThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "max_num_readers") == 0 ||
                   strcmp(keyz, "num_reader_threads") == 0) {
            size_t value = std::stoull(valz);
            getConfiguration().setNumReaderThreads(value);
            ExecutorPool::get()->setNumReaders(value);
        } else if (strcmp(keyz, "max_num_writers") == 0 ||
                   strcmp(keyz, "num_writer_threads") == 0) {
            size_t value = std::stoull(valz);
            getConfiguration().setNumWriterThreads(value);
            ExecutorPool::get()->setNumWriters(value);
        } else if (strcmp(keyz, "max_num_auxio") == 0 ||
                   strcmp(keyz, "num_auxio_threads") == 0) {
            size_t value = std::stoull(valz);
            getConfiguration().setNumAuxioThreads(value);
            ExecutorPool::get()->setNumAuxIO(value);
        } else if (strcmp(keyz, "max_num_nonio") == 0 ||
                   strcmp(keyz, "num_nonio_threads") == 0) {
            size_t value = std::stoull(valz);
            getConfiguration().setNumNonioThreads(value);
            ExecutorPool::get()->setNumNonIO(value);
        } else if (strcmp(keyz, "bfilter_enabled") == 0) {
            getConfiguration().setBfilterEnabled(cb_stob(valz));
        } else if (strcmp(keyz, "bfilter_residency_threshold") == 0) {
            getConfiguration().setBfilterResidencyThreshold(std::stof(valz));
        } else if (strcmp(keyz, "defragmenter_enabled") == 0) {
            getConfiguration().setDefragmenterEnabled(cb_stob(valz));
        } else if (strcmp(keyz, "defragmenter_interval") == 0) {
            size_t v = std::stoull(valz);
            // Adding separate validation as external limit is minimum 1
            // to prevent setting defragmenter to constantly run
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDefragmenterInterval(v);
        } else if (strcmp(keyz, "defragmenter_age_threshold") == 0) {
            getConfiguration().setDefragmenterAgeThreshold(std::stoull(valz));
        } else if (strcmp(keyz, "defragmenter_chunk_duration") == 0) {
            getConfiguration().setDefragmenterChunkDuration(std::stoull(valz));
        } else if (strcmp(keyz, "defragmenter_run") == 0) {
            runDefragmenterTask();
        } else if (strcmp(keyz, "compaction_write_queue_cap") == 0) {
            getConfiguration().setCompactionWriteQueueCap(std::stoull(valz));
        } else if (strcmp(keyz, "dcp_min_compression_ratio") == 0) {
            getConfiguration().setDcpMinCompressionRatio(std::stof(valz));
        } else if (strcmp(keyz, "dcp_noop_mandatory_for_v5_features") == 0) {
            getConfiguration().setDcpNoopMandatoryForV5Features(cb_stob(valz));
        } else if (strcmp(keyz, "access_scanner_run") == 0) {
            if (!(runAccessScannerTask())) {
                rv = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            }
        } else if (strcmp(keyz, "vb_state_persist_run") == 0) {
            runVbStatePersistTask(std::stoi(valz));
        } else if (strcmp(keyz, "ephemeral_full_policy") == 0) {
            getConfiguration().requirementsMetOrThrow("ephemeral_full_policy");
            getConfiguration().setEphemeralFullPolicy(valz);
        } else if (strcmp(keyz, "ephemeral_metadata_purge_age") == 0) {
            getConfiguration().requirementsMetOrThrow(
                    "ephemeral_metadata_purge_age");
            getConfiguration().setEphemeralMetadataPurgeAge(std::stoull(valz));
        } else if (strcmp(keyz, "ephemeral_metadata_purge_interval") == 0) {
            getConfiguration().requirementsMetOrThrow("ephemeral_metadata_purge_interval");
            getConfiguration().setEphemeralMetadataPurgeInterval(
                    std::stoull(valz));
        } else if (strcmp(keyz, "mem_merge_count_threshold") == 0) {
            getConfiguration().setMemMergeCountThreshold(std::stoul(valz));
        } else if (strcmp(keyz, "mem_merge_bytes_threshold") == 0) {
            getConfiguration().setMemMergeBytesThreshold(std::stoul(valz));
        } else if (strcmp(keyz, "fsync_after_every_n_bytes_written") == 0) {
            getConfiguration().setFsyncAfterEveryNBytesWritten(
                    std::stoull(valz));
        } else if (strcmp(keyz, "xattr_enabled") == 0) {
            getConfiguration().setXattrEnabled(cb_stob(valz));
        } else {
            msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
        // Handles exceptions thrown by the cb_stob function
    } catch (invalid_argument_bool& error) {
        msg = error.what();
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles exceptions thrown by the standard
        // library stoi/stoul style functions when not numeric
    } catch (std::invalid_argument&) {
        msg = "Argument was not numeric";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles exceptions thrown by the standard library stoi/stoul
        // style functions when the conversion does not fit in the datatype
    } catch (std::out_of_range&) {
        msg = "Argument was out of range";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;

        // Handles any miscellaneous exceptions in addition to the range_error
        // exceptions thrown by the configuration::set<param>() methods
    } catch (std::exception& error) {
        msg = error.what();
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return rv;
}

protocol_binary_response_status EventuallyPersistentEngine::setDcpParam(
        const char* keyz, const char* valz, std::string& msg) {
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    try {

        if (strcmp(keyz,
                   "dcp_consumer_process_buffered_messages_yield_limit") == 0) {
            size_t v = atoi(valz);
            checkNumeric(valz);
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpConsumerProcessBufferedMessagesYieldLimit(
                    v);
        } else if (
            strcmp(keyz, "dcp_consumer_process_buffered_messages_batch_size") ==
            0) {
            size_t v = atoi(valz);
            checkNumeric(valz);
            validate(v, size_t(1), std::numeric_limits<size_t>::max());
            getConfiguration().setDcpConsumerProcessBufferedMessagesBatchSize(
                    v);
        } else {
            msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
    } catch (std::runtime_error& ex) {
        msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    return rv;
}

protocol_binary_response_status EventuallyPersistentEngine::setVbucketParam(
        uint16_t vbucket,
        const char* keyz,
        const char* valz,
        std::string& msg) {
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    try {
        if (strcmp(keyz, "hlc_drift_ahead_threshold_us") == 0) {
            uint64_t v = std::strtoull(valz, nullptr, 10);
            checkNumeric(valz);
            getConfiguration().setHlcDriftAheadThresholdUs(v);
        } else if (strcmp(keyz, "hlc_drift_behind_threshold_us") == 0) {
            uint64_t v = std::strtoull(valz, nullptr, 10);
            checkNumeric(valz);
            getConfiguration().setHlcDriftBehindThresholdUs(v);
        } else if (strcmp(keyz, "max_cas") == 0) {
            uint64_t v = std::strtoull(valz, nullptr, 10);
            checkNumeric(valz);
            LOG(EXTENSION_LOG_WARNING, "setVbucketParam: max_cas:%" PRIu64 " "
                "vb:%" PRIu16 "\n", v, vbucket);
            if (getKVBucket()->forceMaxCas(vbucket, v) != ENGINE_SUCCESS) {
                rv = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
                msg = "Not my vbucket";
            }
        } else {
            msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
    } catch (std::runtime_error& ex) {
        msg = "Value out of range.";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return rv;
}

static protocol_binary_response_status evictKey(
    EventuallyPersistentEngine* e,
    protocol_binary_request_header
    * request,
    const char** msg,
    size_t* msg_size,
    DocNamespace docNamespace) {
    protocol_binary_request_no_extras* req =
        (protocol_binary_request_no_extras*)request;

    const uint8_t* keyPtr = reinterpret_cast<const uint8_t*>(request) +
                            sizeof(*request);
    size_t keylen = ntohs(req->message.header.request.keylen);
    uint16_t vbucket = ntohs(request->request.vbucket);

    LOG(EXTENSION_LOG_DEBUG, "Manually evicting object with key{%.*s}\n",
        int(keylen), keyPtr);
    msg_size = 0;
    auto rv = e->evictKey(DocKey(keyPtr, keylen, docNamespace), vbucket, msg);
    if (rv == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET ||
        rv == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
        if (e->isDegradedMode()) {
            return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        }
    }
    return rv;
}

protocol_binary_response_status EventuallyPersistentEngine::setParam(
        protocol_binary_request_set_param* req, std::string& msg) {
    size_t keylen = ntohs(req->message.header.request.keylen);
    uint8_t extlen = req->message.header.request.extlen;
    size_t vallen = ntohl(req->message.header.request.bodylen);
    uint16_t vbucket = ntohs(req->message.header.request.vbucket);
    protocol_binary_engine_param_t paramtype =
        static_cast<protocol_binary_engine_param_t>(ntohl(
            req->message.body.param_type));

    if (keylen == 0 || (vallen - keylen - extlen) == 0) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }

    const char* keyp = reinterpret_cast<const char*>(req->bytes)
                       + sizeof(req->bytes);
    const char* valuep = keyp + keylen;
    vallen -= (keylen + extlen);

    char keyz[128];
    char valz[512];

    // Read the key.
    if (keylen >= sizeof(keyz)) {
        msg = "Key is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(keyz, keyp, keylen);
    keyz[keylen] = 0x00;

    // Read the value.
    if (vallen >= sizeof(valz)) {
        msg = "Value is too large.";
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    memcpy(valz, valuep, vallen);
    valz[vallen] = 0x00;

    protocol_binary_response_status rv;

    switch (paramtype) {
    case protocol_binary_engine_param_flush:
        rv = setFlushParam(keyz, valz, msg);
        break;
    case protocol_binary_engine_param_replication:
        rv = setReplicationParam(keyz, valz, msg);
        break;
    case protocol_binary_engine_param_checkpoint:
        rv = setCheckpointParam(keyz, valz, msg);
        break;
    case protocol_binary_engine_param_dcp:
        rv = setDcpParam(keyz, valz, msg);
        break;
    case protocol_binary_engine_param_vbucket:
        rv = setVbucketParam(vbucket, keyz, valz, msg);
        break;
    default:
        rv = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
    }

    return rv;
}

static ENGINE_ERROR_CODE getVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    protocol_binary_request_header* request,
                                    ADD_RESPONSE response) {
    protocol_binary_request_get_vbucket* req =
        reinterpret_cast<protocol_binary_request_get_vbucket*>(request);
    if (req == nullptr) {
        throw std::invalid_argument("getVBucket: Unable to convert req"
                                        " to protocol_binary_request_get_vbucket");
    }

    uint16_t vbucket = ntohs(req->message.header.request.vbucket);
    VBucketPtr vb = e->getVBucket(vbucket);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    } else {
        vbucket_state_t state = (vbucket_state_t)ntohl(vb->getState());
        return sendResponse(response, NULL, 0, NULL, 0, &state,
                            sizeof(state),
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
    }
}

static ENGINE_ERROR_CODE setVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    protocol_binary_request_header* request,
                                    ADD_RESPONSE response) {

    protocol_binary_request_set_vbucket* req =
        reinterpret_cast<protocol_binary_request_set_vbucket*>(request);

    uint64_t cas = ntohll(req->message.header.request.cas);

    size_t bodylen = ntohl(req->message.header.request.bodylen)
                     - ntohs(req->message.header.request.keylen);
    if (bodylen != sizeof(vbucket_state_t)) {
        e->setErrorContext(cookie, "Body too short");
        return sendResponse(response, NULL, 0, NULL, 0, NULL,
                            0, PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL,
                            cas, cookie);
    }

    vbucket_state_t state;
    memcpy(&state, &req->message.body.state, sizeof(state));
    state = static_cast<vbucket_state_t>(ntohl(state));

    if (!is_valid_vbucket_state_t(state)) {
        e->setErrorContext(cookie, "Invalid vbucket state");
        return sendResponse(response, NULL, 0, NULL, 0, NULL,
                            0, PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL,
                            cas, cookie);
    }

    uint16_t vb = ntohs(req->message.header.request.vbucket);
    return e->setVBucketState(cookie, response, vb, state, false, cas);
}

static ENGINE_ERROR_CODE delVBucket(EventuallyPersistentEngine* e,
                                    const void* cookie,
                                    protocol_binary_request_header* req,
                                    ADD_RESPONSE response) {

    uint64_t cas = ntohll(req->request.cas);

    protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    uint16_t vbucket = ntohs(req->request.vbucket);

    if (ntohs(req->request.keylen) > 0 || req->request.extlen > 0) {
        e->setErrorContext(cookie, "Key and extras required");
        return sendResponse(response, NULL, 0, NULL, 0, NULL,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, cas, cookie);
    }

    bool sync = false;
    uint32_t bodylen = ntohl(req->request.bodylen);
    if (bodylen > 0) {
        const char* ptr = reinterpret_cast<const char*>(req->bytes) +
                          sizeof(req->bytes);
        if (bodylen == 7 && strncmp(ptr, "async=0", bodylen) == 0) {
            sync = true;
        }
    }

    ENGINE_ERROR_CODE err;
    void* es = e->getEngineSpecific(cookie);
    if (sync) {
        if (es == NULL) {
            err = e->deleteVBucket(vbucket, cookie);
            e->storeEngineSpecific(cookie, e);
        } else {
            e->storeEngineSpecific(cookie, NULL);
            LOG(EXTENSION_LOG_INFO,
                "Completed sync deletion of vbucket %u",
                (unsigned)vbucket);
            err = ENGINE_SUCCESS;
        }
    } else {
        err = e->deleteVBucket(vbucket);
    }
    switch (err) {
    case ENGINE_SUCCESS:
        LOG(EXTENSION_LOG_NOTICE,
            "Deletion of vbucket %d was completed.", vbucket);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
            "because the vbucket doesn't exist!!!", vbucket);
        res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
        break;
    case ENGINE_EINVAL:
        LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
            "because the vbucket is not in a dead state\n", vbucket);
        e->setErrorContext(
                cookie,
                "Failed to delete vbucket.  Must be in the dead state.");
        res = PROTOCOL_BINARY_RESPONSE_EINVAL;
        break;
    case ENGINE_EWOULDBLOCK:
        LOG(EXTENSION_LOG_NOTICE, "Request for vbucket %d deletion is in"
                " EWOULDBLOCK until the database file is removed from disk",
            vbucket);
        e->storeEngineSpecific(cookie, req);
        return ENGINE_EWOULDBLOCK;
    default:
        LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
            "because of unknown reasons\n", vbucket);
        e->setErrorContext(cookie, "Failed to delete vbucket.  Unknown reason.");
        res = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    if (err == ENGINE_NOT_MY_VBUCKET) {
        return err;
    }

    return sendResponse(response,
                        NULL,
                        0,
                        NULL,
                        0,
                        NULL,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        res,
                        cas,
                        cookie);
}

static ENGINE_ERROR_CODE getReplicaCmd(EventuallyPersistentEngine* e,
                                       protocol_binary_request_header* request,
                                       const void* cookie,
                                       Item** it,
                                       const char** msg,
                                       protocol_binary_response_status* res,
                                       DocNamespace docNamespace) {
    KVBucketIface* kvb = e->getKVBucket();
    protocol_binary_request_no_extras* req =
        (protocol_binary_request_no_extras*)request;
    int keylen = ntohs(req->message.header.request.keylen);
    uint16_t vbucket = ntohs(req->message.header.request.vbucket);
    ENGINE_ERROR_CODE error_code;
    DocKey key(reinterpret_cast<const uint8_t*>(request) + sizeof(*request),
               keylen, docNamespace);

    GetValue rv(kvb->getReplica(key, vbucket, cookie));

    if ((error_code = rv.getStatus()) != ENGINE_SUCCESS) {
        if (error_code == ENGINE_NOT_MY_VBUCKET) {
            *res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
            return error_code;
        } else if (error_code == ENGINE_TMPFAIL) {
            *msg = "NOT_FOUND";
            *res = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            return error_code;
        }
    } else {
        *it = rv.item.release();
        *res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }
    ++(e->getEpStats().numOpsGet);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE compactDB(EventuallyPersistentEngine* e,
                                   const void* cookie,
                                   protocol_binary_request_compact_db* req,
                                   ADD_RESPONSE response) {

    protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    compaction_ctx compactreq;
    uint64_t cas = ntohll(req->message.header.request.cas);

    if (ntohs(req->message.header.request.keylen) > 0 ||
        req->message.header.request.extlen != 24) {
        LOG(EXTENSION_LOG_WARNING,
            "Compaction received bad ext/key len %d/%d.",
            req->message.header.request.extlen,
            ntohs(req->message.header.request.keylen));
        e->setErrorContext(cookie, "Key and correct extras required");
        return sendResponse(response, NULL, 0, NULL, 0, NULL,
                            0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, cas, cookie);
    }
    EPStats& stats = e->getEpStats();
    compactreq.purge_before_ts = ntohll(req->message.body.purge_before_ts);
    compactreq.purge_before_seq =
        ntohll(req->message.body.purge_before_seq);
    compactreq.drop_deletes = req->message.body.drop_deletes;
    compactreq.db_file_id = e->getKVBucket()->getDBFileId(*req);
    uint16_t vbid = ntohs(req->message.header.request.vbucket);

    ENGINE_ERROR_CODE err;
    void* es = e->getEngineSpecific(cookie);
    if (es == NULL) {
        ++stats.pendingCompactions;
        e->storeEngineSpecific(cookie, e);
        err = e->compactDB(vbid, compactreq, cookie);
    } else {
        e->storeEngineSpecific(cookie, NULL);
        err = ENGINE_SUCCESS;
    }

    switch (err) {
    case ENGINE_SUCCESS:
        LOG(EXTENSION_LOG_NOTICE,
            "Compaction of db file id: %d completed.", compactreq.db_file_id);
        break;
    case ENGINE_NOT_MY_VBUCKET:
        --stats.pendingCompactions;
        LOG(EXTENSION_LOG_WARNING, "Compaction of db file id: %d failed "
            "because the db file doesn't exist!!!", compactreq.db_file_id);
        res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
        break;
    case ENGINE_EINVAL:
        --stats.pendingCompactions;
        LOG(EXTENSION_LOG_WARNING, "Compaction of db file id: %d failed "
            "because of an invalid argument", compactreq.db_file_id);
        res = PROTOCOL_BINARY_RESPONSE_EINVAL;
        break;
    case ENGINE_EWOULDBLOCK:
        LOG(EXTENSION_LOG_NOTICE,
            "Compaction of db file id: %d scheduled "
                "(awaiting completion).", compactreq.db_file_id);
        e->storeEngineSpecific(cookie, req);
        return ENGINE_EWOULDBLOCK;
    case ENGINE_TMPFAIL:
        LOG(EXTENSION_LOG_WARNING, "Request to compact db file id: %d hit"
                " a temporary failure and may need to be retried",
            compactreq.db_file_id);
        e->setErrorContext(cookie, "Temporary failure in compacting db file.");
        res = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        break;
    default:
        --stats.pendingCompactions;
        LOG(EXTENSION_LOG_WARNING, "Compaction of db file id: %d failed "
            "because of unknown reasons\n", compactreq.db_file_id);
        e->setErrorContext(cookie, "Failed to compact db file.  Unknown reason.");
        res = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
        break;
    }

    if (err == ENGINE_NOT_MY_VBUCKET) {
        return err;
    }

    return sendResponse(response,
                        NULL,
                        0,
                        NULL,
                        0,
                        NULL,
                        0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        res,
                        cas,
                        cookie);
}

static ENGINE_ERROR_CODE processUnknownCommand(
    EventuallyPersistentEngine* h,
    const void* cookie,
    protocol_binary_request_header* request,
    ADD_RESPONSE response,
    DocNamespace docNamespace) {
    protocol_binary_response_status res =
        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
    std::string dynamic_msg;
    const char* msg = NULL;
    size_t msg_size = 0;
    Item* itm = NULL;

    EPStats& stats = h->getEpStats();
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    /**
     * Session validation
     * (For ns_server commands only)
     */
    switch (request->request.opcode) {
    case PROTOCOL_BINARY_CMD_SET_PARAM:
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
    case PROTOCOL_BINARY_CMD_COMPACT_DB: {
        if (h->getEngineSpecific(cookie) == NULL) {
            uint64_t cas = ntohll(request->request.cas);
            if (!h->validateSessionCas(cas)) {
                h->setErrorContext(cookie, "Invalid session token");
                return sendResponse(response, NULL, 0, NULL, 0,
                                    NULL, 0,
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,
                                    cas, cookie);
            }
        }
        break;
    }
    default:
        break;
    }

    switch (request->request.opcode) {
    case PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS:
        return h->getAllVBucketSequenceNumbers(cookie, request, response);

    case PROTOCOL_BINARY_CMD_GET_VBUCKET: {
        BlockTimer timer(&stats.getVbucketCmdHisto);
        rv = getVBucket(h, cookie, request, response);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET: {
        BlockTimer timer(&stats.delVbucketCmdHisto);
        rv = delVBucket(h, cookie, request, response);
        if (rv != ENGINE_EWOULDBLOCK) {
            h->decrementSessionCtr();
            h->storeEngineSpecific(cookie, NULL);
        }
        return rv;
    }
    case PROTOCOL_BINARY_CMD_SET_VBUCKET: {
        BlockTimer timer(&stats.setVbucketCmdHisto);
        rv = setVBucket(h, cookie, request, response);
        h->decrementSessionCtr();
        return rv;
    }
    case PROTOCOL_BINARY_CMD_STOP_PERSISTENCE:
        res = h->stopFlusher(&msg, &msg_size);
        break;
    case PROTOCOL_BINARY_CMD_START_PERSISTENCE:
        res = h->startFlusher(&msg, &msg_size);
        break;
    case PROTOCOL_BINARY_CMD_SET_PARAM:
        res = h->setParam(
                reinterpret_cast<protocol_binary_request_set_param*>(request),
                dynamic_msg);
        msg = dynamic_msg.c_str();
        msg_size = dynamic_msg.length();
        h->decrementSessionCtr();
        break;
    case PROTOCOL_BINARY_CMD_EVICT_KEY:
        res = evictKey(h, request, &msg, &msg_size, docNamespace);
        break;
    case PROTOCOL_BINARY_CMD_OBSERVE:
        return h->observe(cookie, request, response, docNamespace);
    case PROTOCOL_BINARY_CMD_OBSERVE_SEQNO:
        return h->observe_seqno(cookie, request, response);
    case PROTOCOL_BINARY_CMD_LAST_CLOSED_CHECKPOINT:
    case PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT:
    case PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE: {
        rv = h->handleCheckpointCmds(cookie, request, response);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE: {
        rv = h->handleSeqnoCmds(cookie, request, response);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_SET_WITH_META:
    case PROTOCOL_BINARY_CMD_SETQ_WITH_META:
    case PROTOCOL_BINARY_CMD_ADD_WITH_META:
    case PROTOCOL_BINARY_CMD_ADDQ_WITH_META: {
        rv = h->setWithMeta(cookie,
                            reinterpret_cast<protocol_binary_request_set_with_meta*>
                            (request), response,
                            docNamespace);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_DEL_WITH_META:
    case PROTOCOL_BINARY_CMD_DELQ_WITH_META: {
        rv = h->deleteWithMeta(cookie,
                               reinterpret_cast<protocol_binary_request_delete_with_meta*>
                               (request), response,
                               docNamespace);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_RETURN_META: {
        return h->returnMeta(cookie,
                             reinterpret_cast<protocol_binary_request_return_meta*>
                             (request), response,
                             docNamespace);
    }
    case PROTOCOL_BINARY_CMD_GET_REPLICA:
        rv = getReplicaCmd(h, request, cookie, &itm, &msg, &res, docNamespace);
        if (rv != ENGINE_SUCCESS && rv != ENGINE_NOT_MY_VBUCKET) {
            return rv;
        }
        break;
    case PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC:
    case PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC: {
        rv = h->handleTrafficControlCmd(cookie, request, response);
        return rv;
    }
    case PROTOCOL_BINARY_CMD_COMPACT_DB: {
        rv = compactDB(h, cookie,
                       (protocol_binary_request_compact_db*)(request),
                       response);
        if (rv != ENGINE_EWOULDBLOCK) {
            h->decrementSessionCtr();
            h->storeEngineSpecific(cookie, NULL);
        }
        return rv;
    }
    case PROTOCOL_BINARY_CMD_GET_RANDOM_KEY: {
        if (request->request.extlen != 0 ||
            request->request.keylen != 0 ||
            request->request.bodylen != 0) {
            return ENGINE_EINVAL;
        }
        return h->getRandomKey(cookie, response);
    }
    case PROTOCOL_BINARY_CMD_GET_KEYS: {
        return h->getAllKeys(cookie,
                             reinterpret_cast<protocol_binary_request_get_keys*>
                             (request), response,
                             docNamespace);
    }
        // MB-21143: Remove adjusted time/drift API, but return NOT_SUPPORTED
    case PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME:
    case PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE: {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0,
                            cookie);
    }
    }

    if (itm) {
        uint32_t flags = itm->getFlags();
        rv = sendResponse(response,
                          static_cast<const void*>(itm->getKey().data()),
                          itm->getKey().size(),
                          (const void*)&flags, sizeof(uint32_t),
                          static_cast<const void*>(itm->getData()),
                          itm->getNBytes(), itm->getDataType(),
                          static_cast<uint16_t>(res), itm->getCas(),
                          cookie);
        delete itm;
    } else if (rv == ENGINE_NOT_MY_VBUCKET) {
        return rv;
    } else {
        msg_size = (msg_size > 0 || msg == NULL) ? msg_size : strlen(msg);
        rv = sendResponse(response, NULL, 0, NULL, 0,
                          msg, static_cast<uint16_t>(msg_size),
                          PROTOCOL_BINARY_RAW_BYTES,
                          static_cast<uint16_t>(res), 0, cookie);

    }
    return rv;
}

static ENGINE_ERROR_CODE EvpUnknownCommand(
        gsl::not_null<ENGINE_HANDLE*> handle,
        const void* cookie,
        gsl::not_null<protocol_binary_request_header*> request,
        ADD_RESPONSE response,
        DocNamespace doc_namespace) {
    auto engine = acquireEngine(handle);
    auto ret = processUnknownCommand(
            engine.get(), cookie, request, response, doc_namespace);
    return ret;
}

static void EvpItemSetCas(gsl::not_null<ENGINE_HANDLE*>,
                          gsl::not_null<item*> itm,
                          uint64_t cas) {
    static_cast<Item*>(itm.get())->setCas(cas);
}

static ENGINE_ERROR_CODE EvpDcpStep(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        gsl::not_null<dcp_message_producers*> producers) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->step(producers);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpOpen(gsl::not_null<ENGINE_HANDLE*> handle,
                                    gsl::not_null<const void*> cookie,
                                    uint32_t opaque,
                                    uint32_t seqno,
                                    uint32_t flags,
                                    cb::const_char_buffer name,
                                    cb::const_byte_buffer jsonExtra) {
    return acquireEngine(handle)->dcpOpen(
            cookie, opaque, seqno, flags, name, jsonExtra);
}

static ENGINE_ERROR_CODE EvpDcpAddStream(gsl::not_null<ENGINE_HANDLE*> handle,
                                         gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         uint16_t vbucket,
                                         uint32_t flags) {
    return acquireEngine(handle)->dcpAddStream(cookie, opaque, vbucket, flags);
}

static ENGINE_ERROR_CODE EvpDcpCloseStream(gsl::not_null<ENGINE_HANDLE*> handle,
                                           gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->closeStream(opaque, vbucket);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpStreamReq(gsl::not_null<ENGINE_HANDLE*> handle,
                                         gsl::not_null<const void*> cookie,
                                         uint32_t flags,
                                         uint32_t opaque,
                                         uint16_t vbucket,
                                         uint64_t startSeqno,
                                         uint64_t endSeqno,
                                         uint64_t vbucketUuid,
                                         uint64_t snapStartSeqno,
                                         uint64_t snapEndSeqno,
                                         uint64_t* rollbackSeqno,
                                         dcp_add_failover_log callback) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->streamRequest(flags,
                                   opaque,
                                   vbucket,
                                   startSeqno,
                                   endSeqno,
                                   vbucketUuid,
                                   snapStartSeqno,
                                   snapEndSeqno,
                                   rollbackSeqno,
                                   callback);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpGetFailoverLog(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint16_t vbucket,
        dcp_add_failover_log callback) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->getFailoverLog(opaque, vbucket, callback);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpStreamEnd(gsl::not_null<ENGINE_HANDLE*> handle,
                                         gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         uint16_t vbucket,
                                         uint32_t flags) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->streamEnd(opaque, vbucket, flags);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpSnapshotMarker(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->snapshotMarker(
                opaque, vbucket, start_seqno, end_seqno, flags);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpMutation(gsl::not_null<ENGINE_HANDLE*> handle,
                                        gsl::not_null<const void*> cookie,
                                        uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint32_t flags,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t expiration,
                                        uint32_t lock_time,
                                        cb::const_byte_buffer meta,
                                        uint8_t nru) {
    if (!mcbp::datatype::is_valid(datatype)) {
        LOG(EXTENSION_LOG_WARNING, "Invalid value for datatype "
            " (DCPMutation)");
        return ENGINE_EINVAL;
    }
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->mutation(opaque, key, value, priv_bytes, datatype, cas,
                              vbucket, flags, by_seqno, rev_seqno, expiration,
                              lock_time, meta, nru);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpDeletion(gsl::not_null<ENGINE_HANDLE*> handle,
                                        gsl::not_null<const void*> cookie,
                                        uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        cb::const_byte_buffer meta) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->deletion(opaque, key, value, priv_bytes, datatype, cas,
                              vbucket, by_seqno, rev_seqno, meta);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpExpiration(gsl::not_null<ENGINE_HANDLE*> handle,
                                          gsl::not_null<const void*> cookie,
                                          uint32_t opaque,
                                          const DocKey& key,
                                          cb::const_byte_buffer value,
                                          size_t priv_bytes,
                                          uint8_t datatype,
                                          uint64_t cas,
                                          uint16_t vbucket,
                                          uint64_t by_seqno,
                                          uint64_t rev_seqno,
                                          cb::const_byte_buffer meta) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->expiration(opaque, key, value, priv_bytes, datatype, cas,
                                vbucket, by_seqno, rev_seqno, meta);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpFlush(gsl::not_null<ENGINE_HANDLE*> handle,
                                     gsl::not_null<const void*> cookie,
                                     uint32_t opaque,
                                     uint16_t vbucket) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->flushall(opaque, vbucket);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpSetVbucketState(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint16_t vbucket,
        vbucket_state_t state) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->setVBucketState(opaque, vbucket, state);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpNoop(gsl::not_null<ENGINE_HANDLE*> handle,
                                    gsl::not_null<const void*> cookie,
                                    uint32_t opaque) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->noop(opaque);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpBufferAcknowledgement(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        uint16_t vbucket,
        uint32_t buffer_bytes) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->bufferAcknowledgement(opaque, vbucket, buffer_bytes);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpControl(gsl::not_null<ENGINE_HANDLE*> handle,
                                       gsl::not_null<const void*> cookie,
                                       uint32_t opaque,
                                       const void* key,
                                       uint16_t nkey,
                                       const void* value,
                                       uint32_t nvalue) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->control(opaque, key, nkey, value, nvalue);
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpResponseHandler(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const protocol_binary_response_header* response) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        if (conn->handleResponse(response)) {
            return ENGINE_SUCCESS;
        }
    }
    return ENGINE_DISCONNECT;
}

static ENGINE_ERROR_CODE EvpDcpSystemEvent(gsl::not_null<ENGINE_HANDLE*> handle,
                                           gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData) {
    auto engine = acquireEngine(handle);
    ConnHandler* conn = engine->getConnHandler(cookie);
    if (conn) {
        return conn->systemEvent(
                opaque, vbucket, event, bySeqno, key, eventData);
    }
    return ENGINE_DISCONNECT;
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
    acquireEngine(static_cast<ENGINE_HANDLE*>(c))->handleDisconnect(cookie);
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
    acquireEngine(static_cast<ENGINE_HANDLE*>(c))->handleDeleteBucket(cookie);
}

void EvpSetLogLevel(gsl::not_null<ENGINE_HANDLE*> handle,
                    EXTENSION_LOG_LEVEL level) {
    Logger::setGlobalLogLevel(level);
}

/**
 * The only public interface to the eventually persistent engine.
 * Allocate a new instance and initialize it
 * @param interface the highest interface the server supports (we only
 *                  support interface 1)
 * @param get_server_api callback function to get the server exported API
 *                  functions
 * @param handle Where to return the new instance
 * @return ENGINE_SUCCESS on success
 */
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE** handle) {
    SERVER_HANDLE_V1* api = get_server_api();
    if (interface != 1 || api == NULL) {
        return ENGINE_ENOTSUP;
    }

    Logger::setLoggerAPI(api->log);

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
        engine->getEpStats().memoryTrackerEnabled.store(true);
        engine->getEpStats().totalMemory->store(inital_tracking->load());
    }
    delete inital_tracking;

    initialize_time_functions(api->core);

    *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);

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

static bool EvpGetItemInfo(gsl::not_null<ENGINE_HANDLE*> handle,
                           gsl::not_null<const item*> itm,
                           gsl::not_null<item_info*> itm_info) {
    const Item* it = reinterpret_cast<const Item*>(itm.get());
    auto engine = acquireEngine(handle);
    *itm_info = engine->getItemInfo(*it);
    return true;
}

static cb::EngineErrorMetadataPair EvpGetMeta(
        gsl::not_null<ENGINE_HANDLE*> handle,
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket) {
    return acquireEngine(handle)->getMeta(cookie, key, vbucket);
}

static bool EvpSetItemInfo(gsl::not_null<ENGINE_HANDLE*> handle,
                           gsl::not_null<item*> itm,
                           gsl::not_null<const item_info*> itm_info) {
    Item* it = reinterpret_cast<Item*>(itm.get());
    it->setDataType(itm_info->datatype);
    return true;
}

static cb::engine_error EvpCollectionsSetManifest(
        gsl::not_null<ENGINE_HANDLE*> handle, cb::const_char_buffer json) {
    auto engine = acquireEngine(handle);
    return engine->getKVBucket()->setCollections(json);
}

static bool EvpIsXattrEnabled(gsl::not_null<ENGINE_HANDLE*> handle) {
    auto engine = acquireEngine(handle);
    return engine->getKVBucket()->isXattrEnabled();
}

void LOG(EXTENSION_LOG_LEVEL severity, const char *fmt, ...) {
    va_list va;
    va_start(va, fmt);
    global_logger.vlog(severity, fmt, va);
    va_end(va);
}

EventuallyPersistentEngine::EventuallyPersistentEngine(
        GET_SERVER_API get_server_api)
    : kvBucket(nullptr),
      workload(NULL),
      workloadPriority(NO_BUCKET_PRIORITY),
      getServerApiFunc(get_server_api),
      checkpointConfig(NULL),
      trafficEnabled(false),
      deleteAllEnabled(false),
      startupTime(0),
      taskable(this) {
    interface.interface = 1;
    ENGINE_HANDLE_V1::get_info = EvpGetInfo;
    ENGINE_HANDLE_V1::initialize = EvpInitialize;
    ENGINE_HANDLE_V1::destroy = EvpDestroy;
    ENGINE_HANDLE_V1::allocate = EvpItemAllocate;
    ENGINE_HANDLE_V1::allocate_ex = EvpItemAllocateEx;
    ENGINE_HANDLE_V1::remove = EvpItemDelete;
    ENGINE_HANDLE_V1::release = EvpItemRelease;
    ENGINE_HANDLE_V1::get = EvpGet;
    ENGINE_HANDLE_V1::get_if = EvpGetIf;
    ENGINE_HANDLE_V1::get_and_touch = EvpGetAndTouch;
    ENGINE_HANDLE_V1::get_locked = EvpGetLocked;
    ENGINE_HANDLE_V1::get_meta = EvpGetMeta;
    ENGINE_HANDLE_V1::unlock = EvpUnlock;
    ENGINE_HANDLE_V1::get_stats = EvpGetStats;
    ENGINE_HANDLE_V1::reset_stats = EvpResetStats;
    ENGINE_HANDLE_V1::store = EvpStore;
    ENGINE_HANDLE_V1::store_if = EvpStoreIf;
    ENGINE_HANDLE_V1::flush = EvpFlush;
    ENGINE_HANDLE_V1::unknown_command = EvpUnknownCommand;
    ENGINE_HANDLE_V1::item_set_cas = EvpItemSetCas;
    ENGINE_HANDLE_V1::get_item_info = EvpGetItemInfo;
    ENGINE_HANDLE_V1::set_item_info = EvpSetItemInfo;

    ENGINE_HANDLE_V1::dcp.step = EvpDcpStep;
    ENGINE_HANDLE_V1::dcp.open = EvpDcpOpen;
    ENGINE_HANDLE_V1::dcp.add_stream = EvpDcpAddStream;
    ENGINE_HANDLE_V1::dcp.close_stream = EvpDcpCloseStream;
    ENGINE_HANDLE_V1::dcp.get_failover_log = EvpDcpGetFailoverLog;
    ENGINE_HANDLE_V1::dcp.stream_req = EvpDcpStreamReq;
    ENGINE_HANDLE_V1::dcp.stream_end = EvpDcpStreamEnd;
    ENGINE_HANDLE_V1::dcp.snapshot_marker = EvpDcpSnapshotMarker;
    ENGINE_HANDLE_V1::dcp.mutation = EvpDcpMutation;
    ENGINE_HANDLE_V1::dcp.deletion = EvpDcpDeletion;
    ENGINE_HANDLE_V1::dcp.expiration = EvpDcpExpiration;
    ENGINE_HANDLE_V1::dcp.flush = EvpDcpFlush;
    ENGINE_HANDLE_V1::dcp.set_vbucket_state = EvpDcpSetVbucketState;
    ENGINE_HANDLE_V1::dcp.noop = EvpDcpNoop;
    ENGINE_HANDLE_V1::dcp.buffer_acknowledgement = EvpDcpBufferAcknowledgement;
    ENGINE_HANDLE_V1::dcp.control = EvpDcpControl;
    ENGINE_HANDLE_V1::dcp.response_handler = EvpDcpResponseHandler;
    ENGINE_HANDLE_V1::dcp.system_event = EvpDcpSystemEvent;
    ENGINE_HANDLE_V1::set_log_level = EvpSetLogLevel;
    ENGINE_HANDLE_V1::collections.set_manifest = EvpCollectionsSetManifest;
    ENGINE_HANDLE_V1::isXattrEnabled = EvpIsXattrEnabled;

    serverApi = getServerApiFunc();
    memset(&info, 0, sizeof(info));
    info.info.description = "EP engine v" VERSION;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_CAS;
    info.info.features[info.info.num_features++].feature =
                                             ENGINE_FEATURE_PERSISTENT_STORAGE;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_LRU;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_DATATYPE;
}

/// RAII wrapper for reserving the cookie
class ReservedCookie {
public:
    ReservedCookie(const void* cookie, EventuallyPersistentEngine& engine)
        : cookie(cookie), engine(engine) {
        if (engine.reserveCookie(cookie) != ENGINE_SUCCESS) {
            cookie = nullptr;
        }
    }

    ~ReservedCookie() {
        if (cookie) {
            engine.releaseCookie(cookie);
        }
    }

    /**
     * @return if the constructor could not reserve the cookie
     */
    bool reserved() const {
        return cookie != nullptr;
    }

    /**
     * @return the managed cookie, this object will not call releaseCookie
     */
    const void* release() {
        const void* rv = cookie;
        cookie = nullptr;
        return rv;
    }

private:
    const void* cookie;
    EventuallyPersistentEngine& engine;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::reserveCookie(const void *cookie)
{
    EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
    ENGINE_ERROR_CODE rv = serverApi->cookie->reserve(cookie);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::releaseCookie(const void *cookie)
{
    EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
    ENGINE_ERROR_CODE rv = serverApi->cookie->release(cookie);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

void EventuallyPersistentEngine::registerEngineCallback(ENGINE_EVENT_TYPE type,
                                                        EVENT_CALLBACK cb,
                                                        const void *cb_data) {
    EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
    SERVER_CALLBACK_API *sapi = getServerApi()->callback;
    sapi->register_callback(reinterpret_cast<ENGINE_HANDLE*>(this),
                            type, cb, cb_data);
    ObjectRegistry::onSwitchThread(epe);
}

void EventuallyPersistentEngine::setErrorContext(
        const void* cookie, cb::const_char_buffer message) {
    EventuallyPersistentEngine* epe =
            ObjectRegistry::onSwitchThread(NULL, true);
    serverApi->cookie->set_error_context(const_cast<void*>(cookie), message);
    ObjectRegistry::onSwitchThread(epe);
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
        } else if (key.compare("mem_merge_count_threshold") == 0) {
            engine.stats.mem_merge_count_threshold = value;
        } else if (key.compare("mem_merge_bytes_threshold") == 0) {
            engine.stats.mem_merge_bytes_threshold = value;
        }
    }

    virtual void booleanValueChanged(const std::string &key, bool value) {
        if (key.compare("flushall_enabled") == 0) {
            engine.setDeleteAll(value);
        }
    }
private:
    EventuallyPersistentEngine &engine;
};



ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    resetStats();
    if (config != nullptr) {
        if (!configuration.parseConfiguration(config, serverApi)) {
            LOG(EXTENSION_LOG_WARNING, "Failed to parse the configuration config "
                "during bucket initialization.  config=%s", config);
            return ENGINE_FAILED;
        }
    }

    name = configuration.getCouchBucket();

    if (config != nullptr) {
        LOG(EXTENSION_LOG_NOTICE,
            R"(EPEngine::initialize: using configuration:"%s")",
            config);
    }

    maxFailoverEntries = configuration.getMaxFailoverEntries();

    // Start updating the variables from the config!
    VBucket::setMutationMemoryThreshold(
            configuration.getMutationMemThreshold());

    if (configuration.getMaxSize() == 0) {
        configuration.setMaxSize(std::numeric_limits<size_t>::max());
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

    stats.mem_merge_count_threshold = configuration.getMemMergeCountThreshold();
    configuration.addValueChangedListener(
            "mem_merge_count_threshold",
            new EpEngineValueChangeListener(*this));

    stats.mem_merge_bytes_threshold = configuration.getMemMergeBytesThreshold();
    configuration.addValueChangedListener(
            "mem_merge_bytes_threshold",
            new EpEngineValueChangeListener(*this));

    maxItemSize = configuration.getMaxItemSize();
    configuration.addValueChangedListener("max_item_size",
                                       new EpEngineValueChangeListener(*this));

    maxItemPrivilegedBytes = configuration.getMaxItemPrivilegedBytes();
    configuration.addValueChangedListener(
            "max_item_privileged_bytes",
            new EpEngineValueChangeListener(*this));

    getlDefaultTimeout = configuration.getGetlDefaultTimeout();
    configuration.addValueChangedListener("getl_default_timeout",
                                       new EpEngineValueChangeListener(*this));
    getlMaxTimeout = configuration.getGetlMaxTimeout();
    configuration.addValueChangedListener("getl_max_timeout",
                                       new EpEngineValueChangeListener(*this));

    deleteAllEnabled = configuration.isFlushallEnabled();
    configuration.addValueChangedListener("flushall_enabled",
                                       new EpEngineValueChangeListener(*this));

    workload = new WorkLoadPolicy(configuration.getMaxNumWorkers(),
                                  configuration.getMaxNumShards());
    if ((unsigned int)workload->getNumShards() >
                                              configuration.getMaxVbuckets()) {
        LOG(EXTENSION_LOG_WARNING, "Invalid configuration: Shards must be "
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

    LOG(EXTENSION_LOG_NOTICE,
        "EP Engine: Initialization of %s bucket complete",
        configuration.getBucketType().c_str());

    return ENGINE_SUCCESS;
}

void EventuallyPersistentEngine::destroy(bool force) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::itemAllocate(
        item** itm,
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        uint16_t vbucket) {
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

void EventuallyPersistentEngine::itemRelease(item* itm) {
    delete reinterpret_cast<Item*>(itm);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::flush(const void *cookie){
    if (!deleteAllEnabled) {
        return ENGINE_ENOTSUP;
    }

    if (!isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

    /*
     * Supporting only a SYNC operation for bucket flush
     */

    void* es = getEngineSpecific(cookie);
    if (es == NULL) {
        // Check if diskDeleteAll was false and set it to true
        // if yes, if the atomic variable weren't false, then
        // we will assume that a deleteAll has been scheduled
        // already and return TMPFAIL.
        if (kvBucket->scheduleDeleteAllTask(cookie)) {
            storeEngineSpecific(cookie, this);
            return ENGINE_EWOULDBLOCK;
        } else {
            LOG(EXTENSION_LOG_INFO,
                "Tried to trigger a bucket deleteAll, but"
                "there seems to be a task running already!");
            return ENGINE_TMPFAIL;
        }

    } else {
        storeEngineSpecific(cookie, NULL);
        LOG(EXTENSION_LOG_NOTICE, "Completed bucket deleteAll operation");
        return ENGINE_SUCCESS;
    }
}

cb::EngineErrorItemPair EventuallyPersistentEngine::get_and_touch(const void* cookie,
                                                           const DocKey& key,
                                                           uint16_t vbucket,
                                                           uint32_t exptime) {
    auto* handle = reinterpret_cast<ENGINE_HANDLE*>(this);

    time_t expiry_time = exptime;
    if (exptime != 0) {
        auto* core = serverApi->core;
        expiry_time = core->abstime(core->realtime(exptime));
    }
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

cb::EngineErrorItemPair EventuallyPersistentEngine::get_if(const void* cookie,
                                                       const DocKey& key,
                                                       uint16_t vbucket,
                                                       std::function<bool(const item_info&)>filter) {

    auto* handle = reinterpret_cast<ENGINE_HANDLE*>(this);

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

        BlockTimer timer(&stats.getCmdHisto);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::get_locked(const void* cookie,
                                                         item** itm,
                                                         const DocKey& key,
                                                         uint16_t vbucket,
                                                         uint32_t lock_timeout) {

    auto default_timeout = static_cast<uint32_t>(getGetlDefaultTimeout());

    if (lock_timeout == 0) {
        lock_timeout = default_timeout;
    } else if (lock_timeout > static_cast<uint32_t>(getGetlMaxTimeout())) {
        LOG(EXTENSION_LOG_WARNING,
            "EventuallyPersistentEngine::get_locked: "
            "Illegal value for lock timeout specified %u. "
            "Using default value: %u", lock_timeout, default_timeout);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::unlock(const void* cookie,
                                                     const DocKey& key,
                                                     uint16_t vbucket,
                                                     uint64_t cas) {
    return kvBucket->unlockKey(key, vbucket, cas, ep_current_time());
}

cb::EngineErrorCasPair EventuallyPersistentEngine::store_if(
        const void* cookie,
        Item& item,
        uint64_t cas,
        ENGINE_STORE_OPERATION operation,
        cb::StoreIfPredicate predicate) {
    BlockTimer timer(&stats.storeCmdHisto);
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
    default:
        break;
    }

    return {cb::engine_errc(status), item.getCas()};
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::store(
        const void* cookie,
        item* itm,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation) {
    Item& item = static_cast<Item&>(*static_cast<Item*>(itm));
    auto rv = store_if(cookie, item, cas, operation, {});
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
        (stats.getMaxDataSize() > stats.memOverhead->load());
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

    size_t memUsed =  stats.getTotalMemoryUsed();
    add_casted_stat("mem_used", memUsed, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat_percent", stats.mem_low_wat_percent,
                    add_stat, cookie);
    add_casted_stat("ep_mem_high_wat_percent", stats.mem_high_wat_percent,
                    add_stat, cookie);
    add_casted_stat("bytes", memUsed, add_stat, cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_blob_num", stats.numBlob, add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat("ep_blob_overhead", stats.blobOverhead, add_stat, cookie);
#else
    add_casted_stat("ep_blob_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
    add_casted_stat("ep_storedval_size", stats.totalStoredValSize,
                    add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat("ep_storedval_overhead", stats.blobOverhead, add_stat, cookie);
#else
    add_casted_stat("ep_storedval_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat("ep_storedval_num", stats.numStoredVal, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
    add_casted_stat("ep_item_num", stats.numItem, add_stat, cookie);

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
    add_casted_stat("ep_max_bg_remaining_jobs", epstats.maxRemainingBgJobs,
                    add_stat, cookie);
    add_casted_stat("ep_num_pager_runs", epstats.pagerRuns,
                    add_stat, cookie);
    add_casted_stat("ep_num_expiry_pager_runs", epstats.expiryPagerRuns,
                    add_stat, cookie);
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

    add_casted_stat("ep_mlog_compactor_runs", epstats.mlogCompactorRuns,
                    add_stat, cookie);
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

    add_casted_stat("ep_cursor_dropping_lower_threshold",
                    epstats.cursorDroppingLThreshold, add_stat, cookie);
    add_casted_stat("ep_cursor_dropping_upper_threshold",
                    epstats.cursorDroppingUThreshold, add_stat, cookie);
    add_casted_stat("ep_cursors_dropped",
                    epstats.cursorsDropped, add_stat, cookie);


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
        add_casted_stat(
                "ep_bg_fetch_avg_read_amplification",
                double(value) / (epstats.bg_fetched + epstats.bg_meta_fetched),
                add_stat,
                cookie);
    }

    // Specific to ForestDB:
    if (kvBucket->getKVStoreStat("Block_cache_hits", value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_block_cache_hits", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("Block_cache_misses", value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_block_cache_misses", value, add_stat, cookie);
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
    if (kvBucket->getKVStoreStat("local_kSizeAllMemTables",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_local_kSizeAllMemTables", value, add_stat, cookie);
    }
    // Block Cache hit/miss
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.hit",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_rocksdb.block.cache.hit", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.miss",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_rocksdb.block.cache.miss", value, add_stat, cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.data.hit",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.data.hit",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.data.miss",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.data.miss",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.index.hit",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.index.hit",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.index.miss",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.index.miss",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.filter.hit",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.filter.hit",
                        value,
                        add_stat,
                        cookie);
    }
    if (kvBucket->getKVStoreStat("rocksdb.block.cache.filter.miss",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat("ep_rocksdb_rocksdb.block.cache.filter.miss",
                        value,
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
    if (kvBucket->getKVStoreStat("local_kTotalSstFilesSize",
                                 value,
                                 KVBucketIface::KVSOption::RW)) {
        add_casted_stat(
                "ep_rocksdb_local_kTotalSstFilesSize", value, add_stat, cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doMemoryStats(const void *cookie,
                                                           ADD_STAT add_stat) {
    add_casted_stat("bytes", stats.getTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat("mem_used", stats.getTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
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

    add_casted_stat("ep_blob_num", stats.numBlob, add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat("ep_blob_overhead", stats.blobOverhead, add_stat, cookie);
#else
    add_casted_stat("ep_blob_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat("ep_storedval_size", stats.totalStoredValSize,
                    add_stat, cookie);
#if defined(HAVE_JEMALLOC) || defined(HAVE_TCMALLOC)
    add_casted_stat("ep_storedval_overhead", stats.blobOverhead, add_stat, cookie);
#else
    add_casted_stat("ep_storedval_overhead", "unknown", add_stat, cookie);
#endif
    add_casted_stat("ep_storedval_num", stats.numStoredVal, add_stat, cookie);
    add_casted_stat("ep_item_num", stats.numItem, add_stat, cookie);

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
                    checked_snprintf(buf, sizeof(buf), "vb_%d", vb->getId());
                    add_casted_stat(buf,
                                    VBucket::toString(vb->getInitialState()),
                                    add_stat, cookie);
                } catch (std::exception& error) {
                    LOG(EXTENSION_LOG_WARNING,
                        "addVBStats: Failed building stats: %s", error.what());
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
        VBucketPtr vb = getVBucket(vbucket_id);
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
        StatVBucketVisitor(const void *c, ADD_STAT a) : cookie(c),
                                                        add_stat(a) {}

        void visitBucket(VBucketPtr &vb) override {
            uint16_t vbid = vb->getId();
            char buf[32];
            try {
                checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
                add_casted_stat(buf, VBucket::toString(vb->getState()),
                                add_stat, cookie);
            } catch (std::exception& error) {
                LOG(EXTENSION_LOG_WARNING,
                    "StatVBucketVisitor::visitBucket: Failed to build stat: %s",
                    error.what());
            }

            HashTableDepthStatVisitor depthVisitor;
            vb->ht.visitDepth(depthVisitor);

            try {
                checked_snprintf(buf, sizeof(buf), "vb_%d:size", vbid);
                add_casted_stat(buf, vb->ht.getSize(), add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:locks", vbid);
                add_casted_stat(buf, vb->ht.getNumLocks(), add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:min_depth", vbid);
                add_casted_stat(buf,
                                depthVisitor.min == -1 ? 0 : depthVisitor.min,
                                add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:max_depth", vbid);
                add_casted_stat(buf, depthVisitor.max, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:histo", vbid);
                add_casted_stat(buf, depthVisitor.depthHisto, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:reported", vbid);
                add_casted_stat(buf, vb->ht.getNumInMemoryItems(), add_stat,
                                cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:counted", vbid);
                add_casted_stat(buf, depthVisitor.size, add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:resized", vbid);
                add_casted_stat(buf, vb->ht.getNumResizes(), add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:mem_size", vbid);
                add_casted_stat(buf, vb->ht.getItemMemory(), add_stat, cookie);
                checked_snprintf(buf, sizeof(buf), "vb_%d:mem_size_counted",
                                 vbid);
                add_casted_stat(buf, depthVisitor.memUsed, add_stat, cookie);
            } catch (std::exception& error) {
                LOG(EXTENSION_LOG_WARNING,
                    "StatVBucketVisitor::visitBucket: Failed to build stat: %s",
                    error.what());
            }
        }

        const void *cookie;
        ADD_STAT add_stat;
    };

    StatVBucketVisitor svbv(cookie, add_stat);
    kvBucket->visit(svbv);

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

        uint16_t vbid = vb->getId();
        char buf[256];
        try {
            checked_snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
            add_casted_stat(buf, VBucket::toString(vb->getState()),
                            add_stat, cookie);
            vb->checkpointManager->addStats(add_stat, cookie);

            auto result = eps->getLastPersistedCheckpointId(vbid);
            if (result.second) {
                checked_snprintf(buf,
                                 sizeof(buf),
                                 "vb_%d:persisted_checkpoint_id",
                                 vbid);
                add_casted_stat(buf, result.first, add_stat, cookie);
            }
        } catch (std::exception& error) {
            LOG(EXTENSION_LOG_WARNING,
                "StatCheckpointVisitor::addCheckpointStat: error building stats: %s",
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

    cb::const_char_buffer getDescription() {
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
        VBucketPtr vb = getVBucket(vbucket_id);

        StatCheckpointVisitor::addCheckpointStat(cookie, add_stat,
                                                 kvBucket.get(), vb);
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
        LOG(EXTENSION_LOG_WARNING,
            "showConnAggStat: Failed to build stats: %s", error.what());
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(const void *cookie,
                                                         ADD_STAT add_stat,
                                                         uint16_t vbid,
                                                         const DocKey& key,
                                                         bool validate) {
    ENGINE_ERROR_CODE rv = ENGINE_FAILED;

    std::unique_ptr<Item> it;
    struct key_stats kstats;

    if (fetchLookupResult(cookie, it)) {
        if (!validate) {
            LOG(EXTENSION_LOG_DEBUG,
                "Found lookup results for non-validating key "
                "stat call. Would have leaked\n");
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
            LOG(EXTENSION_LOG_DEBUG, "doKeyStats key{%.*s} is %s\n",
                int(key.size()), key.data(), valid.c_str());
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
                                                            const void *cookie,
                                                            ADD_STAT add_stat,
                                                            uint16_t vbid) {
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
        LOG(EXTENSION_LOG_WARNING,
            "doWorkloadStats: Error building stats: %s", error.what());
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
        checked_snprintf(buffer, sizeof(buffer), "vb_%d:high_seqno",
                         vb->getId());
        add_casted_stat(buffer, relHighSeqno, add_stat, cookie);
        checked_snprintf(buffer, sizeof(buffer), "vb_%d:abs_high_seqno",
                         vb->getId());
        add_casted_stat(buffer, vb->getHighSeqno(), add_stat, cookie);
        checked_snprintf(buffer, sizeof(buffer), "vb_%d:last_persisted_seqno",
                         vb->getId());
        add_casted_stat(
                buffer, vb->getPublicPersistenceSeqno(), add_stat, cookie);
        checked_snprintf(buffer, sizeof(buffer), "vb_%d:uuid", vb->getId());
        add_casted_stat(buffer, entry.vb_uuid, add_stat, cookie);
        checked_snprintf(buffer, sizeof(buffer), "vb_%d:purge_seqno",
                         vb->getId());
        add_casted_stat(buffer, vb->getPurgeSeqno(), add_stat, cookie);
        const snapshot_range_t range = vb->getPersistedSnapshot();
        checked_snprintf(buffer, sizeof(buffer),
                         "vb_%d:last_persisted_snap_start",
                         vb->getId());
        add_casted_stat(buffer, range.start, add_stat, cookie);
        checked_snprintf(buffer, sizeof(buffer),
                         "vb_%d:last_persisted_snap_end",
                         vb->getId());
        add_casted_stat(buffer, range.end, add_stat, cookie);
    } catch (std::exception& error) {
        LOG(EXTENSION_LOG_WARNING,
            "addSeqnoVbStats: error building stats: %s", error.what());
    }
}

void EventuallyPersistentEngine::addLookupResult(const void* cookie,
                                                 std::unique_ptr<Item> result) {
    LockHolder lh(lookupMutex);
    auto it = lookups.find(cookie);
    if (it != lookups.end()) {
        if (it->second != NULL) {
            LOG(EXTENSION_LOG_DEBUG,
                "Cleaning up old lookup result for '%s'",
                it->second->getKey().data());
        } else {
            LOG(EXTENSION_LOG_DEBUG, "Cleaning up old null lookup result");
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

        int vbucket = atoi(value.c_str());
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

void EventuallyPersistentEngine::runVbStatePersistTask(int vbid) {
    kvBucket->runVbStatePersistTask(vbid);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
    BlockTimer timer(&stats.getStatsCmdHisto);
    if (stat_key != NULL) {
        LOG(EXTENSION_LOG_DEBUG, "stats %.*s", nkey, stat_key);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "stats engine");
    }

    const std::string statKey(stat_key, nkey);

    ENGINE_ERROR_CODE rv = ENGINE_KEY_ENOENT;
    if (stat_key == NULL) {
        rv = doEngineStats(cookie, add_stat);
    } else if (nkey > 7 && cb_isPrefix(statKey, "dcpagg ")) {
        rv = doConnAggStats(cookie, add_stat, stat_key + 7, nkey - 7);
    } else if (statKey == "dcp") {
        rv = doDcpStats(cookie, add_stat);
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
        // Non-validating, non-blocking version
        // TODO: Collection - getStats needs DocNamespace
        rv = doKeyStats(cookie, add_stat, vbucket_id,
                        DocKey(key, DocNamespace::DefaultCollection), false);
    } else if (nkey > 5 && cb_isPrefix(statKey, "vkey ")) {
        std::string key;
        std::string vbid;
        std::string s_key(statKey.substr(5, nkey - 5));
        std::stringstream ss(s_key);
        ss >> key;
        ss >> vbid;
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        // Validating version; blocks
        // TODO: Collection - getStats needs DocNamespace
        rv = doKeyStats(cookie, add_stat, vbucket_id,
                        DocKey(key, DocNamespace::DefaultCollection), true);
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
        rv = doDcpVbTakeoverStats(cookie, add_stat, tStream, vbucket_id);
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
            rv = doVbIdFailoverLogStats(cookie, add_stat, vbucket_id);
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
    } else if (statKey == "collections" &&
               configuration.isCollectionsPrototypeEnabled()) {
        // @todo MB-24546 For development, just log everything.
        kvBucket->getCollectionsManager().logAll(*kvBucket.get());
        rv = ENGINE_SUCCESS;
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe(
                                       const void* cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response,
                                       DocNamespace docNamespace) {
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;

    size_t offset = 0;
    const uint8_t* data = req->bytes + sizeof(req->bytes);
    uint32_t data_len = ntohl(req->message.header.request.bodylen);
    std::stringstream result;

    while (offset < data_len) {
        uint16_t vb_id;
        uint16_t keylen;

        // Parse a key
        if (data_len - offset < 4) {
            setErrorContext(cookie, "Requires vbid and keylen.");
            return sendResponse(response, NULL, 0, 0, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                cookie);
        }

        memcpy(&vb_id, data + offset, sizeof(uint16_t));
        vb_id = ntohs(vb_id);
        offset += sizeof(uint16_t);

        memcpy(&keylen, data + offset, sizeof(uint16_t));
        keylen = ntohs(keylen);
        offset += sizeof(uint16_t);

        if (data_len - offset < keylen) {
            setErrorContext(cookie, "Incorrect keylen");
            return sendResponse(response, NULL, 0, 0, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                cookie);
        }

        DocKey key(data + offset, keylen, docNamespace);
        offset += keylen;
        LOG(EXTENSION_LOG_DEBUG, "Observing key{%.*s} in vb:%" PRIu16,
            int(key.size()), key.data(), vb_id);

        // Get key stats
        uint16_t keystatus = 0;
        struct key_stats kstats;
        memset(&kstats, 0, sizeof(key_stats));
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
            return sendResponse(response, NULL, 0, 0, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0,
                                cookie);
        }

        // Put the result into a response buffer
        vb_id = htons(vb_id);
        keylen = htons(keylen);
        uint64_t cas = htonll(kstats.cas);
        result.write((char*) &vb_id, sizeof(uint16_t));
        result.write((char*) &keylen, sizeof(uint16_t));
        result.write(reinterpret_cast<const char*>(key.data()), key.size());
        result.write((char*) &keystatus, sizeof(uint8_t));
        result.write((char*) &cas, sizeof(uint64_t));
    }

    uint64_t persist_time = 0;
    double queue_size = static_cast<double>(stats.diskQueueSize);
    double item_trans_time = kvBucket->getTransactionTimePerItem();

    if (item_trans_time > 0 && queue_size > 0) {
        persist_time = static_cast<uint32_t>(queue_size * item_trans_time);
    }
    persist_time = persist_time << 32;

    return sendResponse(response, NULL, 0, 0, 0, result.str().data(),
                        result.str().length(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, persist_time,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe_seqno(
                                       const void* cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response) {
    protocol_binary_request_no_extras *req =
                          (protocol_binary_request_no_extras*)request;
    const char* data = reinterpret_cast<const char*>(req->bytes) +
                                                   sizeof(req->bytes);
    uint16_t vb_id;
    uint64_t vb_uuid;
    uint8_t  format_type;
    uint64_t last_persisted_seqno;
    uint64_t current_seqno;

    std::stringstream result;

    vb_id = ntohs(req->message.header.request.vbucket);
    memcpy(&vb_uuid, data, sizeof(uint64_t));
    vb_uuid = ntohll(vb_uuid);

    LOG(EXTENSION_LOG_DEBUG, "Observing vbucket: %d with uuid: %" PRIu64,
                             vb_id, vb_uuid);

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

    if (vb_uuid != entry.vb_uuid) {
       uint64_t failover_highseqno = 0;
       uint64_t latest_uuid;
       bool found = vb->failovers->getLastSeqnoForUUID(vb_uuid, &failover_highseqno);
       if (!found) {
           return sendResponse(response, NULL, 0, 0, 0, 0, 0,
                               PROTOCOL_BINARY_RAW_BYTES,
                               PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0,
                               cookie);
       }

       format_type = 1;
       last_persisted_seqno = htonll(vb->getPublicPersistenceSeqno());
       current_seqno = htonll(vb->getHighSeqno());
       latest_uuid = htonll(entry.vb_uuid);
       vb_id = htons(vb_id);
       vb_uuid = htonll(vb_uuid);
       failover_highseqno = htonll(failover_highseqno);

       result.write((char*) &format_type, sizeof(uint8_t));
       result.write((char*) &vb_id, sizeof(uint16_t));
       result.write((char*) &latest_uuid, sizeof(uint64_t));
       result.write((char*) &last_persisted_seqno, sizeof(uint64_t));
       result.write((char*) &current_seqno, sizeof(uint64_t));
       result.write((char*) &vb_uuid, sizeof(uint64_t));
       result.write((char*) &failover_highseqno, sizeof(uint64_t));
    } else {
        format_type = 0;
        last_persisted_seqno = htonll(vb->getPublicPersistenceSeqno());
        current_seqno = htonll(vb->getHighSeqno());
        vb_id   =  htons(vb_id);
        vb_uuid =  htonll(vb_uuid);

        result.write((char*) &format_type, sizeof(uint8_t));
        result.write((char*) &vb_id, sizeof(uint16_t));
        result.write((char*) &vb_uuid, sizeof(uint64_t));
        result.write((char*) &last_persisted_seqno, sizeof(uint64_t));
        result.write((char*) &current_seqno, sizeof(uint64_t));
    }

    return sendResponse(response, NULL, 0, 0, 0, result.str().data(),
                        result.str().length(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                        cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleCheckpointCmds(const void *cookie,
                                           protocol_binary_request_header *req,
                                           ADD_RESPONSE response)
{
    uint16_t vbucket = ntohs(req->request.vbucket);
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    int16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch (req->request.opcode) {
    case PROTOCOL_BINARY_CMD_LAST_CLOSED_CHECKPOINT:
        {
        uint64_t checkpointId =
                vb->checkpointManager->getLastClosedCheckpointId();
        checkpointId = htonll(checkpointId);
        return sendResponse(response,
                            NULL,
                            0,
                            NULL,
                            0,
                            &checkpointId,
                            sizeof(checkpointId),
                            PROTOCOL_BINARY_RAW_BYTES,
                            status,
                            0,
                            cookie);
        }
        break;
    case PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT:
        if (vb->getState() != vbucket_state_active) {
            return ENGINE_NOT_MY_VBUCKET;

        } else {
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
                                    status,
                                    0,
                                    cookie);
            }
        }
        break;
    case PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE:
        {
        uint16_t keylen = ntohs(req->request.keylen);
        uint32_t bodylen = ntohl(req->request.bodylen);
        if ((bodylen - keylen) == 0) {
            status = PROTOCOL_BINARY_RESPONSE_EINVAL;
            setErrorContext(cookie,
                            "No checkpoint id is given for "
                            "CMD_CHECKPOINT_PERSISTENCE!!!");
            } else {
                uint64_t chk_id;
                memcpy(&chk_id, req->bytes + sizeof(req->bytes) + keylen,
                       bodylen - keylen);
                chk_id = ntohll(chk_id);
                void *es = getEngineSpecific(cookie);
                if (!es) {
                    auto res = vb->checkAddHighPriorityVBEntry(
                            chk_id,
                            cookie,
                            HighPriorityVBNotify::ChkPersistence);

                    switch (res) {
                    case HighPriorityVBReqStatus::RequestScheduled:
                        storeEngineSpecific(cookie, this);
                        // Wake up the flusher if it is idle.
                        getKVBucket()->wakeUpFlusher();
                        return ENGINE_EWOULDBLOCK;

                    case HighPriorityVBReqStatus::NotSupported:
                        status = PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
                        LOG(EXTENSION_LOG_WARNING,
                          "EventuallyPersistentEngine::handleCheckpointCmds(): "
                          "High priority async chk request "
                          "for vb:%" PRIu16 " is NOT supported" ,
                          vbucket);
                        break;

                    case HighPriorityVBReqStatus::RequestNotScheduled:
                        /* 'HighPriorityVBEntry' was not added, hence just
                           return success */
                        LOG(EXTENSION_LOG_NOTICE,
                          "EventuallyPersistentEngine::handleCheckpointCmds(): "
                          "Did NOT add high priority async chk request "
                          "for vb:%" PRIu16,
                          vbucket);

                        break;
                    }
                } else {
                    storeEngineSpecific(cookie, NULL);
                    LOG(EXTENSION_LOG_INFO,
                        "Checkpoint %" PRIu64 " persisted for vbucket %d.",
                        chk_id, vbucket);
                }
            }
        }
        break;
    default:
        {
            status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
            setErrorContext(cookie,
                            "Unknown checkpoint command opcode: " +
                                    std::to_string(req->request.opcode));
        }
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        status, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleSeqnoCmds(const void *cookie,
                                           protocol_binary_request_header *req,
                                           ADD_RESPONSE response)
{
    uint16_t vbucket = ntohs(req->request.vbucket);
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    int16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    uint16_t extlen = req->request.extlen;
    uint32_t bodylen = ntohl(req->request.bodylen);
    if ((bodylen - extlen) != 0) {
        status = PROTOCOL_BINARY_RESPONSE_EINVAL;
        setErrorContext(cookie, "Body should be all extras.");
    } else {
        uint64_t seqno;
        memcpy(&seqno, req->bytes + sizeof(req->bytes), 8);
        seqno = ntohll(seqno);
        void *es = getEngineSpecific(cookie);
        if (!es) {
            auto persisted_seqno = vb->getPersistenceSeqno();
            if (seqno > persisted_seqno) {
                auto res = vb->checkAddHighPriorityVBEntry(
                        seqno, cookie, HighPriorityVBNotify::Seqno);

                switch (res) {
                case HighPriorityVBReqStatus::RequestScheduled:
                    storeEngineSpecific(cookie, this);
                    return ENGINE_EWOULDBLOCK;

                case HighPriorityVBReqStatus::NotSupported:
                    status = PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
                    LOG(EXTENSION_LOG_WARNING,
                        "EventuallyPersistentEngine::handleSeqnoCmds(): "
                        "High priority async seqno request "
                        "for vb:%" PRIu16 " is NOT supported",
                        vbucket);
                    break;

                case HighPriorityVBReqStatus::RequestNotScheduled:
                    /* 'HighPriorityVBEntry' was not added, hence just return
                       success */
                    LOG(EXTENSION_LOG_NOTICE,
                        "EventuallyPersistentEngine::handleSeqnoCmds(): "
                        "Did NOT add high priority async seqno request "
                        "for vb:%" PRIu16 ", Persisted seqno %" PRIu64
                        " > requested seqno %" PRIu64,
                        vbucket,
                        persisted_seqno,
                        seqno);
                    break;
                }
            }
        } else {
            storeEngineSpecific(cookie, NULL);
            LOG(EXTENSION_LOG_INFO,
                "Sequence number %" PRIu64 " persisted for vbucket %d.",
                seqno, vbucket);
        }
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        status, 0, cookie);
}

cb::EngineErrorMetadataPair EventuallyPersistentEngine::getMeta(
        const void* cookie, const DocKey& key, uint16_t vbucket) {
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

protocol_binary_response_status
EventuallyPersistentEngine::decodeWithMetaOptions(
        protocol_binary_request_delete_with_meta* request,
        GenerateCas& generateCas,
        CheckConflicts& checkConflicts,
        PermittedVBStates& permittedVBStates,
        int& keyOffset) {
    uint8_t extlen = request->message.header.request.extlen;
    keyOffset = 0;
    bool forceFlag = false;
    if (extlen == 28 || extlen == 30) {
        uint32_t options;
        memcpy(&options, request->bytes + sizeof(request->bytes),
               sizeof(options));
        options = ntohl(options);
        keyOffset = 4; // 4 bytes for options

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

    // So if either check1/2/3 is true, return EINVAL
    if (check1 || check2 || check3) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }


    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(const void* cookie,
                                protocol_binary_request_set_with_meta *request,
                                ADD_RESPONSE response,
                                DocNamespace docNamespace)
{
    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    uint8_t extlen = request->message.header.request.extlen;
    uint16_t keylen = ntohs(request->message.header.request.keylen);
    // extlen, the size dicates what is encoded.
    // 24 = no nmeta and no options
    // 26 = nmeta
    // 28 = options (4-byte field)
    // 30 = options and nmeta (options followed by nmeta)
    // so 27, 25 etc... are illegal
    if ((extlen != 24 && extlen != 26 && extlen != 28  && extlen != 30)
        || keylen == 0) {
        return sendErrorResponse(response,
                                 PROTOCOL_BINARY_RESPONSE_EINVAL,
                                 0,
                                 cookie);
    }

    if (isDegradedMode()) {
        return sendErrorResponse(response,
                                 PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                                 0,
                                 cookie);
    }

    uint8_t opcode = request->message.header.request.opcode;
    uint8_t* key = request->bytes + sizeof(request->bytes);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint32_t bodylen = ntohl(request->message.header.request.bodylen);
    uint8_t datatype = request->message.header.request.datatype;
    size_t vallen = bodylen - keylen - extlen;

    uint32_t flags = request->message.body.flags;
    uint32_t expiration = ntohl(request->message.body.expiration);
    uint64_t seqno = ntohll(request->message.body.seqno);
    uint64_t cas = ntohll(request->message.body.cas);

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    int keyOffset = 0;
    protocol_binary_response_status error = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    if ((error = decodeWithMetaOptions(request,
                                       generateCas,
                                       checkConflicts,
                                       permittedVBStates,
                                       keyOffset)) !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES, error, 0, cookie);
    }

    cb::const_byte_buffer emd;
    if (extlen == 26 || extlen == 30) {
        uint16_t nmeta = 0;
        memcpy(&nmeta, key + keyOffset, sizeof(nmeta));
        keyOffset += 2; // 2 bytes for nmeta
        nmeta = ntohs(nmeta);
        if (nmeta > 0) {
            // Correct the vallen
            vallen -= nmeta;
            emd = cb::const_byte_buffer{key + keylen + keyOffset + vallen,
                                        nmeta};
        }
    }

    if (vallen > maxItemSize) {
        LOG(EXTENSION_LOG_WARNING,
            "Item value size %ld for setWithMeta is bigger "
            "than the max size %ld allowed!!!\n", vallen, maxItemSize);
        return sendErrorResponse(response,
                                 PROTOCOL_BINARY_RESPONSE_E2BIG,
                                 0,
                                 cookie);
    }



    void *startTimeC = getEngineSpecific(cookie);
    ProcessClock::time_point startTime;
    if (startTimeC) {
        startTime = ProcessClock::time_point(
                ProcessClock::duration(*(static_cast<hrtime_t*>(startTimeC))));
    } else {
        startTime = ProcessClock::now();
    }

    bool allowExisting = (opcode == PROTOCOL_BINARY_CMD_SET_WITH_META ||
                          opcode == PROTOCOL_BINARY_CMD_SETQ_WITH_META);

    uint64_t bySeqno = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    uint64_t commandCas = ntohll(request->message.header.request.cas);
    try {
        uint8_t* value = key + keyOffset + keylen;
        ret = setWithMeta(vbucket,
                          DocKey(key + keyOffset, keylen, docNamespace),
                          {value, vallen},
                          {cas, seqno, flags, time_t(expiration)},
                          false /*isDeleted*/,
                          datatype,
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
        return sendErrorResponse(response,
                                 PROTOCOL_BINARY_RESPONSE_ENOMEM,
                                 0,
                                 cookie);
    }

    cas = 0;
    if (ret == ENGINE_SUCCESS) {
        ++stats.numOpsSetMeta;
        auto endTime = ProcessClock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime);
        stats.setWithMetaHisto.add(elapsed);
        cas = commandCas;
    } else if (ret == ENGINE_ENOMEM) {
        ret = memoryCondition();
    } else if (ret == ENGINE_EWOULDBLOCK) {
        ++stats.numOpsGetMetaOnSetWithMeta;
        if (!startTimeC) {
            startTimeC = cb_malloc(sizeof(hrtime_t));
            memcpy(startTimeC, &startTime, sizeof(hrtime_t));
            storeEngineSpecific(cookie, startTimeC);
        }
        return ret;
    }

    auto rc = serverApi->cookie->engine_error2mcbp(cookie, ret);

    if (startTimeC) {
        cb_free(startTimeC);
        startTimeC = nullptr;
        storeEngineSpecific(cookie, startTimeC);
    }

    if ((opcode == PROTOCOL_BINARY_CMD_SETQ_WITH_META || opcode == PROTOCOL_BINARY_CMD_ADDQ_WITH_META) &&
        rc == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    if (ret == ENGINE_NOT_MY_VBUCKET) {
        return ret;
    }

    if (ret == ENGINE_SUCCESS && isMutationExtrasSupported(cookie)) {
        return sendMutationExtras(response, vbucket, bySeqno, rc, cas, cookie);
    }
    return sendErrorResponse(response, rc, cas, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(
        uint16_t vbucket,
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
    if (emd.data()) {
        extendedMetaData =
                std::make_unique<ExtendedMetaData>(emd.data(), emd.size());
        if (extendedMetaData->getStatus() == ENGINE_EINVAL) {
            return ENGINE_EINVAL;
        }
    }

    datatype = checkForDatatypeJson(
            cookie,
            datatype,
            {reinterpret_cast<const char*>(value.data()), value.size()});
    auto item = std::make_unique<Item>(key,
                                       itemMeta.flags,
                                       itemMeta.exptime,
                                       value.data(),
                                       value.size(),
                                       datatype,
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
                                     extendedMetaData.get(),
                                     false /*isReplication*/);

    if (ret == ENGINE_SUCCESS) {
        cas = item->getCas();
    } else {
        cas = 0;
    }
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteWithMeta(
                             const void* cookie,
                             protocol_binary_request_delete_with_meta *request,
                             ADD_RESPONSE response,
                             DocNamespace docNamespace) {
    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    uint16_t nkey = ntohs(request->message.header.request.keylen);
    uint8_t extlen = request->message.header.request.extlen;
    // extlen, the size dicates what is encoded.
    // 24 = no nmeta and no options
    // 26 = nmeta
    // 28 = options (4-byte field)
    // 30 = options and nmeta (options followed by nmeta)
    // so 27, 25 etc... are illegal
    if ((extlen != 24 && extlen != 26 && extlen != 28  && extlen != 30)
        || nkey == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                            0, cookie);
    }

    uint8_t opcode = request->message.header.request.opcode;
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint64_t cas = ntohll(request->message.header.request.cas);
    uint32_t bodylen = ntohl(request->message.header.request.bodylen);
    uint32_t flags = request->message.body.flags;
    uint32_t expiration = ntohl(request->message.body.expiration);
    uint64_t seqno = ntohll(request->message.body.seqno);
    uint64_t metacas = ntohll(request->message.body.cas);
    uint8_t datatype = request->message.header.request.datatype;
    size_t vallen = bodylen - nkey - extlen;

    CheckConflicts checkConflicts = CheckConflicts::Yes;
    PermittedVBStates permittedVBStates{vbucket_state_active};
    GenerateCas generateCas = GenerateCas::No;
    int keyOffset = 0;
    protocol_binary_response_status error = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    if ((error = decodeWithMetaOptions(request,
                                       generateCas,
                                       checkConflicts,
                                       permittedVBStates,
                                       keyOffset)) !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES, error, 0, cookie);
    }

    cb::const_byte_buffer emd;
    if (extlen == 26 || extlen == 30) {
        uint16_t nmeta = 0;
        memcpy(&nmeta, request->bytes + sizeof(request->bytes),
               sizeof(nmeta));
        keyOffset += 2; // 2 bytes for nmeta
        nmeta = ntohs(nmeta);
        if (nmeta > 0) {
            emd = cb::const_byte_buffer(
                    request->bytes + sizeof(request->bytes) + nkey + keyOffset,
                    nmeta);
        }
    }

    const uint8_t *keyPtr = request->bytes + keyOffset + sizeof(request->bytes);
    DocKey key(keyPtr, nkey, docNamespace);
    uint64_t bySeqno = 0;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    try {
        if (vallen) {
            // A delete with a value
            const uint8_t* value = keyPtr + nkey;
            ret = setWithMeta(vbucket,
                              key,
                              {value, vallen},
                              {metacas, seqno, flags, time_t(expiration)},
                              true /*isDeleted*/,
                              datatype,
                              cas,
                              &bySeqno,
                              cookie,
                              permittedVBStates,
                              checkConflicts,
                              true /*allowExisting*/,
                              GenerateBySeqno::Yes,
                              generateCas,
                              emd);
        } else {
            ret = deleteWithMeta(vbucket,
                                 key,
                                 {metacas, seqno, flags, time_t(expiration)},
                                 cas,
                                 &bySeqno,
                                 cookie,
                                 permittedVBStates,
                                 checkConflicts,
                                 GenerateBySeqno::Yes,
                                 generateCas,
                                 emd);
        }
    } catch (const std::bad_alloc&) {
        return sendErrorResponse(response,
                                 PROTOCOL_BINARY_RESPONSE_ENOMEM,
                                 0,
                                 cookie);
    }

    if (ret == ENGINE_SUCCESS) {
        stats.numOpsDelMeta++;
    } else if (ret == ENGINE_ENOMEM) {
        ret = memoryCondition();
    } else if (ret == ENGINE_EWOULDBLOCK) {
        return ENGINE_EWOULDBLOCK;
    }

    auto rc = serverApi->cookie->engine_error2mcbp(cookie, ret);

    if (opcode == PROTOCOL_BINARY_CMD_DELQ_WITH_META &&
        rc == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    if (ret == ENGINE_NOT_MY_VBUCKET) {
        return ret;
    }

    if (ret == ENGINE_SUCCESS && isMutationExtrasSupported(cookie)) {
        return sendMutationExtras(response, vbucket, bySeqno, rc, cas, cookie);
    }

    return sendErrorResponse(response, rc, cas, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteWithMeta(
        uint16_t vbucket,
        DocKey key,
        ItemMetaData itemMeta,
        uint64_t& cas,
        uint64_t* seqno,
        const void* cookie,
        PermittedVBStates permittedVBStates,
        CheckConflicts checkConflicts,
        GenerateBySeqno genBySeqno,
        GenerateCas genCas,
        cb::const_byte_buffer emd) {
    std::unique_ptr<ExtendedMetaData> extendedMetaData;
    if (emd.data()) {
        extendedMetaData =
                std::make_unique<ExtendedMetaData>(emd.data(), emd.size());
        if (extendedMetaData->getStatus() == ENGINE_EINVAL) {
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
                                    false /*isReplication*/);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleTrafficControlCmd(const void *cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response)
{
    int16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch (request->request.opcode) {
    case PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC:
        if (kvBucket->isWarmingUp()) {
            // engine is still warming up, do not turn on data traffic yet
            status = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            setErrorContext(cookie, "Persistent engine is still warming up!");
        } else if (configuration.isFailpartialwarmup() &&
                   kvBucket->isWarmupOOMFailure()) {
            // engine has completed warm up, but data traffic cannot be
            // turned on due to an OOM failure
            status = PROTOCOL_BINARY_RESPONSE_ENOMEM;
            setErrorContext(
                    cookie,
                    "Data traffic to persistent engine cannot be enabled"
                    " due to out of memory failures during warmup");
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
    case PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC:
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
        status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        setErrorContext(cookie,
                        "Unknown traffic control opcode: " +
                                std::to_string(request->request.opcode));
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        status, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::doDcpVbTakeoverStats(const void *cookie,
                                                 ADD_STAT add_stat,
                                                 std::string &key,
                                                 uint16_t vbid) {
    VBucketPtr vb = getVBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    std::string dcpName("eq_dcpq:");
    dcpName.append(key);

    const auto conn = dcpConnMap_->findByName(dcpName);
    if (!conn) {
        LOG(EXTENSION_LOG_INFO,"doDcpVbTakeoverStats - cannot find "
            "connection %s for vb:%" PRIu16, dcpName.c_str(), vbid);
        size_t vb_items = vb->getNumItems();

        size_t del_items = 0;
        try {
            del_items = kvBucket->getNumPersistedDeletes(vbid);
        } catch (std::runtime_error& e) {
            LOG(EXTENSION_LOG_WARNING,
                "doDcpVbTakeoverStats: exception while getting num "
                "persisted deletes for vbucket:%" PRIu16 " - treating as 0 "
                "deletes. Details: %s", vbid, e.what());
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
        LOG(EXTENSION_LOG_WARNING, "doDcpVbTakeoverStats: connection %s for "
            "vb:%" PRIu16 " is not a DcpProducer", dcpName.c_str(), vbid);
        return ENGINE_KEY_ENOENT;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::returnMeta(const void* cookie,
                                  protocol_binary_request_return_meta *request,
                                  ADD_RESPONSE response,
                                  DocNamespace docNamespace) {
    uint8_t extlen = request->message.header.request.extlen;
    uint16_t keylen = ntohs(request->message.header.request.keylen);
    if (extlen != 12 || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                            0, cookie);
    }

    uint8_t* keyPtr = request->bytes + sizeof(request->bytes);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint32_t bodylen = ntohl(request->message.header.request.bodylen);
    uint64_t cas = ntohll(request->message.header.request.cas);
    uint8_t datatype = request->message.header.request.datatype;
    uint32_t mutate_type = ntohl(request->message.body.mutation_type);
    uint32_t flags = ntohl(request->message.body.flags);
    uint32_t exp = ntohl(request->message.body.expiration);
    exp = exp == 0 ? 0 : ep_abs_time(ep_reltime(exp));
    size_t vallen = bodylen - keylen - extlen;
    uint64_t seqno;

    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;
    if (mutate_type == SET_RET_META || mutate_type == ADD_RET_META) {
        uint8_t *dta = keyPtr + keylen;
        datatype = checkForDatatypeJson(
                cookie, datatype, {reinterpret_cast<const char*>(dta), vallen});

        Item* itm = new Item(DocKey(keyPtr, keylen, docNamespace),
                             flags,
                             exp,
                             dta,
                             vallen,
                             datatype,
                             cas,
                             -1,
                             vbucket);

        if (!itm) {
            return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_ENOMEM, 0, cookie);
        }

        if (mutate_type == SET_RET_META) {
            ret = kvBucket->set(*itm, cookie, {});
        } else {
            ret = kvBucket->add(*itm, cookie);
        }
        if (ret == ENGINE_SUCCESS) {
            ++stats.numOpsSetRetMeta;
        }
        cas = itm->getCas();
        seqno = htonll(itm->getRevSeqno());
        delete itm;
    } else if (mutate_type == DEL_RET_META) {
        ItemMetaData itm_meta;
        mutation_descr_t mutation_descr;
        DocKey key(keyPtr, keylen, docNamespace);
        ret = kvBucket->deleteItem(
                key, cas, vbucket, cookie, &itm_meta, mutation_descr);
        if (ret == ENGINE_SUCCESS) {
            ++stats.numOpsDelRetMeta;
        }
        flags = itm_meta.flags;
        exp = itm_meta.exptime;
        cas = itm_meta.cas;
        seqno = htonll(itm_meta.revSeqno);
    } else {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (ret == ENGINE_NOT_MY_VBUCKET || ret == ENGINE_EWOULDBLOCK) {
        return ret;
    } else if (ret != ENGINE_SUCCESS) {
        auto rc = serverApi->cookie->engine_error2mcbp(cookie, ret);
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES, rc, 0, cookie);
    }

    uint8_t meta[16];
    exp = htonl(exp);
    memcpy(meta, &flags, 4);
    memcpy(meta + 4, &exp, 4);
    memcpy(meta + 8, &seqno, 8);

    return sendResponse(response, NULL, 0, (const void *)meta, 16, NULL, 0,
                        datatype, PROTOCOL_BINARY_RESPONSE_SUCCESS, cas,
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
    AllKeysCallback() {
        buffer.reserve((avgKeySize + sizeof(uint16_t)) * expNumKeys);
    }

    void callback(const DocKey& key) {
        if (buffer.size() + key.size() + sizeof(uint16_t) >
            buffer.size()) {
            // Reserve the 2x space for the copy-to buffer.
            buffer.reserve(buffer.size()*2);
        }
        uint16_t outlen = htons(key.size());
        // insert 1 x u16
        const auto* outlenPtr = reinterpret_cast<const char*>(&outlen);
        buffer.insert(buffer.end(), outlenPtr, outlenPtr + sizeof(uint16_t));
        // insert the char buffer
        buffer.insert(buffer.end(), key.data(), key.data()+key.size());
    }

    char* getAllKeysPtr() { return buffer.data(); }
    uint64_t getAllKeysLen() { return buffer.size(); }

private:
    std::vector<char> buffer;

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
                     uint16_t vbucket,
                     uint32_t count_)
        : GlobalTask(e, TaskId::FetchAllKeysTask, 0, false),
          engine(e),
          cookie(c),
          description("Running the ALL_DOCS api on vbucket: " +
                      std::to_string(vbucket)),
          response(resp),
          start_key(start_key_),
          vbid(vbucket),
          count(count_) {
    }

    cb::const_char_buffer getDescription() {
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
            err = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                               PROTOCOL_BINARY_RAW_BYTES,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                               cookie);
        } else {
            auto cb = std::make_shared<AllKeysCallback>();
            err = engine->getKVBucket()->getROUnderlying(vbid)->getAllKeys(
                                                    vbid, start_key, count, cb);
            if (err == ENGINE_SUCCESS) {
                err =  sendResponse(response, NULL, 0, NULL, 0,
                                    ((AllKeysCallback*)cb.get())->getAllKeysPtr(),
                                    ((AllKeysCallback*)cb.get())->getAllKeysLen(),
                                    PROTOCOL_BINARY_RAW_BYTES,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
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
    uint16_t vbid;
    uint32_t count;
};

ENGINE_ERROR_CODE
EventuallyPersistentEngine::getAllKeys(const void* cookie,
                                protocol_binary_request_get_keys *request,
                                ADD_RESPONSE response,
                                DocNamespace docNamespace) {
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

    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    VBucketPtr vb = getVBucket(vbucket);

    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }

    ReaderLockHolder rlh(vb->getStateLock());
    if (vb->getState() != vbucket_state_active) {
        return ENGINE_NOT_MY_VBUCKET;
    }
    //key: key, ext: no. of keys to fetch, sorting-order
    uint16_t keylen = ntohs(request->message.header.request.keylen);
    uint8_t extlen = request->message.header.request.extlen;

    uint32_t count = 1000;

    if (extlen > 0) {
        if (extlen != sizeof(uint32_t)) {
            return ENGINE_EINVAL;
        }
        memcpy(&count, request->bytes + sizeof(request->bytes),
               sizeof(uint32_t));
        count = ntohl(count);
    }

    if (keylen == 0) {
        LOG(EXTENSION_LOG_WARNING, "No key passed as argument for getAllKeys");
        return ENGINE_EINVAL;
    }
    const uint8_t* keyPtr = (request->bytes + sizeof(request->bytes) + extlen);
    DocKey start_key(keyPtr, keylen, docNamespace);

    ExTask task = std::make_shared<FetchAllKeysTask>(
            this, cookie, response, start_key, vbucket, count);
    ExecutorPool::get()->schedule(task);
    return ENGINE_EWOULDBLOCK;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getRandomKey(const void *cookie,
                                                       ADD_RESPONSE response) {
    GetValue gv(kvBucket->getRandomKey());
    ENGINE_ERROR_CODE ret = gv.getStatus();

    if (ret == ENGINE_SUCCESS) {
        Item* it = gv.item.get();
        uint32_t flags = it->getFlags();
        ret = sendResponse(response, static_cast<const void *>(it->getKey().data()),
                           it->getKey().size(),
                           (const void *)&flags, sizeof(uint32_t),
                           static_cast<const void *>(it->getData()),
                           it->getNBytes(), it->getDataType(),
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, it->getCas(),
                           cookie);
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::dcpOpen(
        const void* cookie,
        uint32_t opaque,
        uint32_t seqno,
        uint32_t flags,
        cb::const_char_buffer stream_name,
        cb::const_byte_buffer jsonExtra) {
    (void) opaque;
    (void) seqno;
    std::string connName = cb::to_string(stream_name);

    ReservedCookie reservedCookie(cookie, *this);

    if (!reservedCookie.reserved()) {
        LOG(EXTENSION_LOG_WARNING, "Cannot create DCP connection because cookie"
            "cannot be reserved");
        return ENGINE_DISCONNECT;
    }

    if (getEngineSpecific(cookie) != NULL) {
        LOG(EXTENSION_LOG_WARNING, "Cannot open DCP connection as another"
            " connection exists on the same socket");
        return ENGINE_DISCONNECT;
    }

    // Require that replication streams are opened with collections (only if
    // we're running with the collections prototype). Eventually ns_server will
    // request collection DCP and we won't need this...
    // @todo MB-24547
    static const std::string replicationConnName = "replication:";
    if (getConfiguration().isCollectionsPrototypeEnabled() &&
        !connName.compare(0, replicationConnName.size(), replicationConnName) &&
        (flags & DCP_OPEN_COLLECTIONS) == 0) {
        // Fail DCP open, we need to have replication streams with DCP enabled
        return ENGINE_DISCONNECT;
    }

    ConnHandler *handler = NULL;
    if (flags & (DCP_OPEN_PRODUCER | DCP_OPEN_NOTIFIER)) {
        handler = dcpConnMap_->newProducer(cookie, connName, flags, jsonExtra);
    } else {
        handler = dcpConnMap_->newConsumer(cookie, connName);
    }

    if (handler == nullptr) {
        LOG(EXTENSION_LOG_WARNING, "EPEngine::dcpOpen: failed to create a handler");
        return ENGINE_DISCONNECT;
    } else {
        storeEngineSpecific(reservedCookie.release(), handler);
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::dcpAddStream(const void* cookie,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint32_t flags)
{
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
        LOG(EXTENSION_LOG_WARNING, "Invalid streaming connection");
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
        uint8_t opcode = getOpcodeIfEwouldblockSet(cookie);
        switch(opcode) {
            case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
            case PROTOCOL_BINARY_CMD_COMPACT_DB:
                {
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
    LOG(EXTENSION_LOG_NOTICE, "Shutting down all DCP connections in "
            "preparation for bucket deletion.");
    dcpConnMap_->shutdownAllConnections();
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getAllVBucketSequenceNumbers(
                                    const void *cookie,
                                    protocol_binary_request_header *request,
                                    ADD_RESPONSE response) {
    protocol_binary_request_get_all_vb_seqnos *req =
        reinterpret_cast<protocol_binary_request_get_all_vb_seqnos*>(request);

    // if extlen (hence bodylen) is non-zero, it limits the result to only
    // include the vbuckets in the specified vbucket state.
    size_t bodylen = ntohl(req->message.header.request.bodylen);

    vbucket_state_t reqState = static_cast<vbucket_state_t>(0);;
    if (bodylen != 0) {
        memcpy(&reqState, &req->message.body.state, sizeof(reqState));
        reqState = static_cast<vbucket_state_t>(ntohl(reqState));
    }

    std::vector<uint8_t> payload;
    auto vbuckets = kvBucket->getVBuckets().getBuckets();

    /* Reserve a buffer that's big enough to hold all of them (we might
     * not use all of them. Each entry in the array occupies 10 bytes
     * (two bytes vbucket id followed by 8 bytes sequence number)
     */
    try {
        payload.reserve(vbuckets.size() * (sizeof(uint16_t) + sizeof(uint64_t)));
    } catch (std::bad_alloc) {
        return sendResponse(response, 0, 0, 0, 0, 0, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ENOMEM, 0,
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
                uint16_t vbid = htons(static_cast<uint16_t>(id));
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
                        0, 0, /* key */
                        0, 0, /* ext field */
                        payload.data(), payload.size(), /* value */
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
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
        protocol_binary_response_status status,
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
        uint16_t vbucket,
        uint64_t bySeqno,
        protocol_binary_response_status status,
        uint64_t cas,
        const void* cookie) {
    VBucketPtr vb = kvBucket->getVBucket(vbucket);
    if (!vb) {
        return sendErrorResponse(
                response, PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, cas, cookie);
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
        uint16_t vbid,
        vbucket_state_t to,
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
    LOG(EXTENSION_LOG_NOTICE, "~EPEngine: Completed deinitialize.");
    delete workload;
    delete checkpointConfig;
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

WorkLoadPolicy&  EpEngineTaskable::getWorkLoadPolicy(void) {
    return myEngine->getWorkLoadPolicy();
}

void EpEngineTaskable::logQTime(TaskId id,
                                const ProcessClock::duration enqTime) {
    myEngine->getKVBucket()->logQTime(id, enqTime);
}

void EpEngineTaskable::logRunTime(TaskId id,
                                  const ProcessClock::duration runTime) {
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
