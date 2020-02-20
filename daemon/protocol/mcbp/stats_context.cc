/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "engine_errc_2_mcbp.h"
#include "engine_wrapper.h"
#include "logger/logger.h"
#include "stats_context.h"
#include "utilities.h"

#include <daemon/buckets.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/executorpool.h>
#include <daemon/mc_time.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/runtime.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <daemon/stats_tasks.h>
#include <daemon/topkeys.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/audit_interface.h>
#include <nlohmann/json.hpp>
#include <phosphor/stats_callback.h>
#include <phosphor/trace_log.h>
#include <platform/checked_snprintf.h>

#include <gsl/gsl>

/*************************** ADD STAT CALLBACKS ***************************/

// Generic add_stat<T>. Uses std::to_string which requires heap allocation.
template <typename T>
void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const T& val) {
    std::string value = std::to_string(val);
    add_stat_callback(name,
                      uint16_t(strlen(name)),
                      value.c_str(),
                      uint32_t(value.length()),
                      &cookie);
}

// Specializations for common, integer types. Uses stack buffer for
// int-to-string conversion.
void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              int32_t val) {
    char buf[16];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRId32, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING("add_stat failed to add stat for {}", name);
    } else {
        add_stat_callback(
                name, uint16_t(strlen(name)), buf, uint32_t(len), &cookie);
    }
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              uint32_t val) {
    char buf[16];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRIu32, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING("add_stat failed to add stat for {}", name);
    } else {
        add_stat_callback(
                name, uint16_t(strlen(name)), buf, uint32_t(len), &cookie);
    }
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              int64_t val) {
    char buf[32];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRId64, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING("add_stat failed to add stat for {}", name);
    } else {
        add_stat_callback(
                name, uint16_t(strlen(name)), buf, uint32_t(len), &cookie);
    }
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              uint64_t val) {
    char buf[32];
    int len = checked_snprintf(buf, sizeof(buf), "%" PRIu64, val);
    if (len < 0 || size_t(len) >= sizeof(buf)) {
        LOG_WARNING("add_stat failed to add stat for {}", name);
    } else {
        add_stat_callback(
                name, uint16_t(strlen(name)), buf, uint32_t(len), &cookie);
    }
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const std::string& value) {
    add_stat_callback(name,
                      uint16_t(strlen(name)),
                      value.c_str(),
                      uint32_t(value.length()),
                      &cookie);
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const char* value) {
    add_stat_callback(name,
                      uint16_t(strlen(name)),
                      value,
                      uint32_t(strlen(value)),
                      &cookie);
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const bool value) {
    if (value) {
        add_stat(cookie, add_stat_callback, name, "true");
    } else {
        add_stat(cookie, add_stat_callback, name, "false");
    }
}

/* return server specific stats only */
static ENGINE_ERROR_CODE server_stats(const AddStatFn& add_stat_callback,
                                      Cookie& cookie) {
    rel_time_t now = mc_time_get_current_time();

    struct thread_stats thread_stats;
    thread_stats.aggregate(cookie.getConnection().getBucket().stats);

    try {
        std::lock_guard<std::mutex> guard(stats_mutex);

        add_stat(cookie, add_stat_callback, "uptime", now);
        add_stat(cookie, add_stat_callback, "stat_reset",
                 (const char*)reset_stats_time);
        add_stat(cookie, add_stat_callback, "time",
                 mc_time_convert_to_abs_time(now));
        add_stat(cookie, add_stat_callback, "version", get_server_version());
        add_stat(cookie, add_stat_callback, "memcached_version", MEMCACHED_VERSION);
        add_stat(cookie, add_stat_callback, "libevent", event_get_version());
        add_stat(cookie, add_stat_callback, "pointer_size", (8 * sizeof(void*)));

        add_stat(cookie, add_stat_callback, "daemon_connections",
                 stats.daemon_conns);
        add_stat(cookie, add_stat_callback, "curr_connections",
                 stats.curr_conns.load(std::memory_order_relaxed));
        add_stat(cookie,
                 add_stat_callback,
                 "system_connections",
                 stats.system_conns.load(std::memory_order_relaxed));
        add_stat(cookie, add_stat_callback, "total_connections", stats.total_conns);
        add_stat(cookie, add_stat_callback, "connection_structures",
                 stats.conn_structs);
        add_stat(cookie, add_stat_callback, "cmd_get", thread_stats.cmd_get);
        add_stat(cookie, add_stat_callback, "cmd_set", thread_stats.cmd_set);
        add_stat(cookie, add_stat_callback, "cmd_flush", thread_stats.cmd_flush);

        add_stat(cookie, add_stat_callback, "cmd_subdoc_lookup",
                 thread_stats.cmd_subdoc_lookup);
        add_stat(cookie, add_stat_callback, "cmd_subdoc_mutation",
                 thread_stats.cmd_subdoc_mutation);

        add_stat(cookie, add_stat_callback, "bytes_subdoc_lookup_total",
                 thread_stats.bytes_subdoc_lookup_total);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_lookup_extracted",
                 thread_stats.bytes_subdoc_lookup_extracted);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_mutation_total",
                 thread_stats.bytes_subdoc_mutation_total);
        add_stat(cookie, add_stat_callback, "bytes_subdoc_mutation_inserted",
                 thread_stats.bytes_subdoc_mutation_inserted);

        // index 0 contains the aggregated timings for all buckets
        auto& timings = all_buckets[0].timings;
        uint64_t total_mutations = timings.get_aggregated_mutation_stats();
        uint64_t total_retrivals = timings.get_aggregated_retrival_stats();
        uint64_t total_ops = total_retrivals + total_mutations;
        add_stat(cookie, add_stat_callback, "cmd_total_sets", total_mutations);
        add_stat(cookie, add_stat_callback, "cmd_total_gets", total_retrivals);
        add_stat(cookie, add_stat_callback, "cmd_total_ops", total_ops);

        // bucket specific totals
        auto& current_bucket_timings =
                cookie.getConnection().getBucket().timings;
        uint64_t mutations = current_bucket_timings.get_aggregated_mutation_stats();
        uint64_t lookups = current_bucket_timings.get_aggregated_retrival_stats();
        add_stat(cookie, add_stat_callback, "cmd_mutation", mutations);
        add_stat(cookie, add_stat_callback, "cmd_lookup", lookups);

        add_stat(cookie, add_stat_callback, "auth_cmds", thread_stats.auth_cmds);
        add_stat(cookie, add_stat_callback, "auth_errors", thread_stats.auth_errors);
        add_stat(cookie, add_stat_callback, "get_hits", thread_stats.get_hits);
        add_stat(cookie, add_stat_callback, "get_misses", thread_stats.get_misses);
        add_stat(cookie, add_stat_callback, "delete_misses",
                 thread_stats.delete_misses);
        add_stat(cookie, add_stat_callback, "delete_hits", thread_stats.delete_hits);
        add_stat(cookie, add_stat_callback, "incr_misses", thread_stats.incr_misses);
        add_stat(cookie, add_stat_callback, "incr_hits", thread_stats.incr_hits);
        add_stat(cookie, add_stat_callback, "decr_misses", thread_stats.decr_misses);
        add_stat(cookie, add_stat_callback, "decr_hits", thread_stats.decr_hits);
        add_stat(cookie, add_stat_callback, "cas_misses", thread_stats.cas_misses);
        add_stat(cookie, add_stat_callback, "cas_hits", thread_stats.cas_hits);
        add_stat(cookie, add_stat_callback, "cas_badval", thread_stats.cas_badval);
        add_stat(cookie, add_stat_callback, "bytes_read", thread_stats.bytes_read);
        add_stat(cookie, add_stat_callback, "bytes_written",
                 thread_stats.bytes_written);
        add_stat(cookie, add_stat_callback, "accepting_conns",
                 is_listen_disabled() ? 0 : 1);
        add_stat(cookie, add_stat_callback, "listen_disabled_num",
                 get_listen_disabled_num());
        add_stat(cookie, add_stat_callback, "rejected_conns", stats.rejected_conns);
        add_stat(cookie,
                 add_stat_callback,
                 "threads",
                 Settings::instance().getNumWorkerThreads());
        add_stat(cookie, add_stat_callback, "conn_yields", thread_stats.conn_yields);
        add_stat(cookie, add_stat_callback, "rbufs_allocated",
                 thread_stats.rbufs_allocated);
        add_stat(cookie, add_stat_callback, "rbufs_loaned",
                 thread_stats.rbufs_loaned);
        add_stat(cookie, add_stat_callback, "rbufs_existing",
                 thread_stats.rbufs_existing);
        add_stat(cookie, add_stat_callback, "wbufs_allocated",
                 thread_stats.wbufs_allocated);
        add_stat(cookie, add_stat_callback, "wbufs_loaned",
                 thread_stats.wbufs_loaned);
        add_stat(cookie,
                 add_stat_callback,
                 "wbufs_existing",
                 thread_stats.wbufs_existing);
        add_stat(cookie, add_stat_callback, "iovused_high_watermark",
                 thread_stats.iovused_high_watermark);
        add_stat(cookie, add_stat_callback, "msgused_high_watermark",
                 thread_stats.msgused_high_watermark);

        add_stat(cookie, add_stat_callback, "cmd_lock", thread_stats.cmd_lock);
        add_stat(cookie, add_stat_callback, "lock_errors",
                 thread_stats.lock_errors);

        auto lookup_latency = timings.get_interval_lookup_latency();
        add_stat(cookie, add_stat_callback, "cmd_lookup_10s_count",
                 lookup_latency.count);
        add_stat(cookie, add_stat_callback, "cmd_lookup_10s_duration_us",
                 lookup_latency.duration_ns / 1000);

        auto mutation_latency = timings.get_interval_mutation_latency();
        add_stat(cookie, add_stat_callback, "cmd_mutation_10s_count",
                 mutation_latency.count);
        add_stat(cookie, add_stat_callback, "cmd_mutation_10s_duration_us",
                 mutation_latency.duration_ns / 1000);

        auto& respCounters =
                cookie.getConnection().getBucket().responseCounters;
        // Ignore success responses by starting from begin + 1
        uint64_t total_resp_errors = std::accumulate(
                std::begin(respCounters) + 1, std::end(respCounters), 0);
        add_stat(cookie,
                 add_stat_callback,
                 "total_resp_errors",
                 total_resp_errors);

    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    return ENGINE_SUCCESS;
}

static void append_bin_stats(const char* key,
                             const uint16_t klen,
                             const char* val,
                             const uint32_t vlen,
                             Cookie& cookie) {
    auto& dbuf = cookie.getDynamicBuffer();
    // We've ensured that there is enough room in the buffer before calling
    // this method
    auto* buf = reinterpret_cast<uint8_t*>(dbuf.getCurrent());
    cb::mcbp::ResponseBuilder builder(
            cb::byte_buffer(buf, dbuf.getSize() - dbuf.getOffset()));
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::Stat);
    builder.setDatatype(cb::mcbp::Datatype::Raw);
    builder.setStatus(cb::mcbp::Status::Success);
    builder.setKey(
            cb::const_byte_buffer(reinterpret_cast<const uint8_t*>(key), klen));
    builder.setValue(
            cb::const_byte_buffer(reinterpret_cast<const uint8_t*>(val), vlen));
    builder.setOpaque(cookie.getHeader().getOpaque());
    builder.validate();
    dbuf.moveOffset(sizeof(cb::mcbp::Response) +
                    builder.getFrame()->getBodylen());
}

static void append_stats(const char* key,
                         const uint16_t klen,
                         const char* val,
                         const uint32_t vlen,
                         gsl::not_null<const void*> void_cookie) {
    size_t needed;

    auto& cookie = *const_cast<Cookie*>(
            reinterpret_cast<const Cookie*>(void_cookie.get()));
    needed = vlen + klen + sizeof(protocol_binary_response_header);
    if (!cookie.growDynamicBuffer(needed)) {
        return;
    }
    append_bin_stats(key, klen, val, vlen, cookie);
}

// Create a static std::function to wrap append_stats, instead of creating a
// temporary object every time we need to call into an engine.
// This also avoids problems where the stack-allocated AddStatFn could go
// out of scope if someone needs to take a copy of it and run it on another
// thread.
static AddStatFn appendStatsFn = append_stats;

/**
 * This is a very slow thing that you shouldn't use in production ;-)
 *
 * @param c the connection to return the details for
 */
static void process_bucket_details(Cookie& cookie) {
    nlohmann::json json;
    nlohmann::json array = nlohmann::json::array();

    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        auto o = get_bucket_details(ii);
        if (o.size() > 0) {
            array.push_back(o);
        }
    }
    json["buckets"] = array;

    const auto stats_str = json.dump();
    append_stats("bucket details",
                 14,
                 stats_str.data(),
                 uint32_t(stats_str.size()),
                 static_cast<void*>(&cookie));
}

/**
 * Handler for the <code>stats reset</code> command.
 *
 * Clear the global and the connected buckets stats.
 *
 * It is possible to clear a subset of the stats by using its submodule.
 * The following submodules exists:
 * <ul>
 *    <li>timings</li>
 * </ul>
 *
 * @todo I would have assumed that we wanted to clear the stats from
 *       <b>all</b> of the buckets?
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_reset_executor(const std::string& arg,
                                             Cookie& cookie) {
    if (arg.empty()) {
        stats_reset(cookie);
        bucket_reset_stats(cookie);
        all_buckets[0].timings.reset();
        all_buckets[cookie.getConnection().getBucketIndex()].timings.reset();
        return ENGINE_SUCCESS;
    } else if (arg == "timings") {
        // Nuke the command timings section for the connected bucket
        all_buckets[cookie.getConnection().getBucketIndex()].timings.reset();
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats sched</code> used to get the
 * histogram for the scheduler histogram.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_sched_executor(const std::string& arg,
                                             Cookie& cookie) {
    if (arg.empty()) {
        for (size_t ii = 0; ii < Settings::instance().getNumWorkerThreads();
             ++ii) {
            auto hist = scheduler_info[ii].to_string();
            std::string key = std::to_string(ii);
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         hist.data(),
                         gsl::narrow<uint32_t>(hist.size()),
                         &cookie);
        }
        return ENGINE_SUCCESS;
    } else if (arg == "aggregate") {
        static const std::string key = {"aggregate"};
        Hdr1sfMicroSecHistogram histogram{};
        for (const auto& h : scheduler_info) {
            histogram += h;
        }
        // Add the stat
        auto hist = histogram.to_string();
        append_stats(key.data(),
                     gsl::narrow<uint16_t>(key.size()),
                     hist.data(),
                     gsl::narrow<uint32_t>(hist.size()),
                     &cookie);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats audit</code> used to get statistics from
 * the audit subsystem.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_audit_executor(const std::string& arg,
                                             Cookie& cookie) {
    if (arg.empty()) {
        stats_audit(appendStatsFn, cookie);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats bucket details</code> used to get information
 * of the buckets (type, state, #clients etc)
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_bucket_details_executor(const std::string& arg,
                                                      Cookie& cookie) {
    if (arg.empty()) {
        process_bucket_details(cookie);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats aggregate</code>.. probably not used anymore
 * as it gives just a subset of what you'll get from an empty stat.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_aggregate_executor(const std::string& arg,
                                                 Cookie& cookie) {
    if (arg.empty()) {
        return server_stats(appendStatsFn, cookie);
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats connection[ fd]</code> command to retrieve
 * information about connection specific details.
 *
 * @param arg an optional file descriptor representing the connection
 *            object to retrieve information about. If empty dump all.
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_connections_executor(const std::string& arg,
                                                   Cookie& cookie) {
    int64_t fd = -1;

    if (!arg.empty()) {
        try {
            fd = std::stoll(arg);
        } catch (...) {
            return ENGINE_EINVAL;
        }
    }

    std::shared_ptr<Task> task = std::make_shared<StatsTaskConnectionStats>(
            cookie.getConnection(), cookie, appendStatsFn, fd);
    cookie.obtainContext<StatsCommandContext>(cookie).setTask(task);
    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);

    return ENGINE_EWOULDBLOCK;
}

/**
 * Handler for the <code>stats topkeys</code> command used to retrieve
 * the most popular keys in the attached bucket.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_topkeys_executor(const std::string& arg,
                                               Cookie& cookie) {
    if (arg.empty()) {
        auto& bucket = all_buckets[cookie.getConnection().getBucketIndex()];
        if (bucket.topkeys == nullptr) {
            return ENGINE_NO_BUCKET;
        }
        return bucket.topkeys->stats(
                &cookie, mc_time_get_current_time(), appendStatsFn);
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats topkeys</code> command used to retrieve
 * a JSON document containing the most popular keys in the attached bucket.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_topkeys_json_executor(const std::string& arg,
                                                    Cookie& cookie) {
    if (arg.empty()) {
        ENGINE_ERROR_CODE ret;

        nlohmann::json topkeys_doc;

        auto& bucket = all_buckets[cookie.getConnection().getBucketIndex()];
        if (bucket.topkeys == nullptr) {
            return ENGINE_NO_BUCKET;
        }
        ret = bucket.topkeys->json_stats(topkeys_doc,
                                         mc_time_get_current_time());

        if (ret == ENGINE_SUCCESS) {
            char key[] = "topkeys_json";
            const auto topkeys_str = topkeys_doc.dump();
            append_stats(key,
                         (uint16_t)strlen(key),
                         topkeys_str.data(),
                         uint32_t(topkeys_str.size()),
                         &cookie);
        }
        return ret;
    } else {
        return ENGINE_EINVAL;
    }
}

/**
 * Handler for the <code>stats subdoc_execute</code> command used to retrieve
 * information from the subdoc subsystem.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static ENGINE_ERROR_CODE stat_subdoc_execute_executor(const std::string& arg,
                                                      Cookie& cookie) {
    if (arg.empty()) {
        const auto index = cookie.getConnection().getBucketIndex();
        std::string json_str;
        if (index == 0) {
            // Aggregrated timings for all buckets.
            Hdr1sfMicroSecHistogram aggregated{};
            for (const auto& bucket : all_buckets) {
                aggregated += bucket.subjson_operation_times;
            }
            json_str = aggregated.to_string();
        } else {
            // Timings for a specific bucket.
            auto& bucket = all_buckets[cookie.getConnection().getBucketIndex()];
            json_str = bucket.subjson_operation_times.to_string();
        }
        append_stats(nullptr,
                     0,
                     json_str.c_str(),
                     gsl::narrow<uint32_t>(json_str.size()),
                     &cookie);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

static ENGINE_ERROR_CODE stat_responses_json_executor(const std::string& arg,
                                                      Cookie& cookie) {
    try {
        auto& respCounters =
                cookie.getConnection().getBucket().responseCounters;
        nlohmann::json json;

        for (uint16_t resp = 0; resp < respCounters.size(); ++resp) {
            const auto value = respCounters[resp].load();
            if (value > 0) {
                std::stringstream stream;
                stream << std::hex << resp;
                json[stream.str()] = value;
            }
        }

        std::string json_str = json.dump();
        const std::string stat_name = "responses";
        append_stats(stat_name.c_str(),
                     gsl::narrow<uint16_t>(stat_name.size()),
                     json_str.c_str(),
                     gsl::narrow<uint32_t>(json_str.size()),
                     &cookie);
        return ENGINE_SUCCESS;
    } catch (const std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE stat_tracing_executor(const std::string& arg,
                                               Cookie& cookie) {
    class MemcachedCallback : public phosphor::StatsCallback {
    public:
        explicit MemcachedCallback(Cookie& cookie) : c(cookie) {
        }

        void operator()(gsl_p::cstring_span key,
                        gsl_p::cstring_span value) override {
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         value.data(),
                         gsl::narrow<uint32_t>(value.size()),
                         &c);
        }

        void operator()(gsl_p::cstring_span key, bool value) override {
            const auto svalue = value ? "true"_ccb : "false"_ccb;
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         svalue.data(),
                         gsl::narrow<uint32_t>(svalue.size()),
                         &c);
        }

        void operator()(gsl_p::cstring_span key, size_t value) override {
            const auto svalue = std::to_string(value);
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         svalue.data(),
                         gsl::narrow<uint32_t>(svalue.size()),
                         &c);
        }

        void operator()(gsl_p::cstring_span key,
                        phosphor::ssize_t value) override {
            const auto svalue = std::to_string(value);
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         svalue.data(),
                         gsl::narrow<uint32_t>(svalue.size()),
                         &c);
        }

        void operator()(gsl_p::cstring_span key, double value) override {
            const auto svalue = std::to_string(value);
            append_stats(key.data(),
                         gsl::narrow<uint16_t>(key.size()),
                         svalue.data(),
                         gsl::narrow<uint32_t>(svalue.size()),
                         &c);
        }

    private:
        Cookie& c;
    };

    if (arg.empty()) {
        MemcachedCallback cb{cookie};
        phosphor::TraceLog::getInstance().getStats(cb);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_EINVAL;
    }
}

static ENGINE_ERROR_CODE stat_all_stats(const std::string& arg,
                                        Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    auto ret = bucket_get_stats(cookie, arg, value, appendStatsFn);
    if (ret == ENGINE_SUCCESS) {
        ret = server_stats(appendStatsFn, cookie);
    }
    return ret;
}

static ENGINE_ERROR_CODE stat_bucket_stats(const std::string& arg,
                                           Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    return bucket_get_stats(cookie, arg, value, appendStatsFn);
}

/***************************** STAT HANDLERS *****************************/

struct command_stat_handler {
    /**
     * Is this a privileged stat or may it be requested by anyone
     */
    bool privileged;

    /**
     * The callback function to handle the stat request
     */
    ENGINE_ERROR_CODE (*handler)(const std::string& arg, Cookie& cookie);
};

/**
 * A mapping from all stat subgroups to the callback providing the
 * statistics
 */
static std::unordered_map<std::string, struct command_stat_handler>
        stat_handlers = {
                {"", {false, stat_all_stats}},
                {"reset", {true, stat_reset_executor}},
                {"worker_thread_info", {false, stat_sched_executor}},
                {"audit", {true, stat_audit_executor}},
                {"bucket_details", {true, stat_bucket_details_executor}},
                {"aggregate", {false, stat_aggregate_executor}},
                {"connections", {true, stat_connections_executor}},
                {"topkeys", {false, stat_topkeys_executor}},
                {"topkeys_json", {false, stat_topkeys_json_executor}},
                {"subdoc_execute", {false, stat_subdoc_execute_executor}},
                {"responses", {false, stat_responses_json_executor}},
                {"tracing", {true, stat_tracing_executor}}};

/**
 * For a given key, try and return the handler for it
 * @param key The key we wish to handle
 * @return std::pair<stat_handler, bool> returns the stat handler and a bool
 * representing whether this is a key we recognise or not
 */
static std::pair<command_stat_handler, bool> getStatHandler(
        const std::string& key) {
    auto iter = stat_handlers.find(key);
    if (iter == stat_handlers.end()) {
        return std::make_pair(command_stat_handler{false, stat_bucket_stats}, false);
    } else {
        return std::make_pair(iter->second, true);
    }
}

/************************* STATE MACHINE EXECUTION *************************/

ENGINE_ERROR_CODE StatsCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
    do {
        switch (state) {
        case State::ParseCommandKey:
            ret = parseCommandKey();
            break;
        case State::CheckPrivilege:
            ret = checkPrivilege();
            break;
        case State::DoStats:
            ret = doStats();
            break;
        case State::GetTaskResult:
            ret = getTaskResult();
            break;
        case State::CommandComplete:
            ret = commandComplete();
            break;
        case State::Done:
            return command_exit_code;
        }
    } while (ret == ENGINE_SUCCESS);

    return ret;
}

ENGINE_ERROR_CODE StatsCommandContext::parseCommandKey() {
    if (key.empty()) {
        command = "";
    } else {
        // The raw representing the key
        const std::string statkey{reinterpret_cast<const char*>(key.data()),
                                  key.size()};

        // Split the key into a command and argument.
        auto index = statkey.find(' ');

        if (index == key.npos) {
            command = statkey;
        } else {
            command = statkey.substr(0, index);
            argument = statkey.substr(++index);
        }
    }

    state = State::CheckPrivilege;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE StatsCommandContext::checkPrivilege() {
    auto ret = ENGINE_SUCCESS;

    auto handler = getStatHandler(command).first;

    if (handler.privileged) {
        ret = mcbp::checkPrivilege(cookie, cb::rbac::Privilege::Stats);
        switch (ret) {
        case ENGINE_SUCCESS:
            state = State::DoStats;
            break;
        default:
            command_exit_code = ret;
            state = State::CommandComplete;
            break;
        }
    } else {
        state = State::DoStats;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE StatsCommandContext::doStats() {
    auto handler_pair = getStatHandler(command);

    if (!handler_pair.second) {
        command_exit_code = handler_pair.first.handler(
                {reinterpret_cast<const char*>(key.data()), key.size()},
                cookie);
    } else {
        command_exit_code = handler_pair.first.handler(argument, cookie);
    }

    // If stats command call returns ENGINE_EWOULDBLOCK and the task is not a
    // nullptr (ie we have created a background task), then change the state to
    // be GetTaskResult and return ENGINE_EWOULDBLOCK
    if (command_exit_code == ENGINE_EWOULDBLOCK && task) {
        state = State::GetTaskResult;
        return ENGINE_EWOULDBLOCK;
    }

    state = State::CommandComplete;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE StatsCommandContext::getTaskResult() {
    auto& stats_task = dynamic_cast<StatsTask&>(*task);

    state = State::CommandComplete;
    command_exit_code = stats_task.getCommandError();
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE StatsCommandContext::commandComplete() {
    switch (command_exit_code) {
    case ENGINE_SUCCESS:
        append_stats(nullptr, 0, nullptr, 0, static_cast<void*>(&cookie));

        // We just want to record this once rather than for each packet sent
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::Success)];
        cookie.sendDynamicBuffer();
        break;
    case ENGINE_EWOULDBLOCK:
        /* If the stats call returns ENGINE_EWOULDBLOCK then set the
         * state to DoStats again and return this error code */
        state = State::DoStats;
        return command_exit_code;
    case ENGINE_DISCONNECT:
        // We don't send these responses back so we will not store
        // stats for these.
        break;
    default:
        ++connection.getBucket().responseCounters[int(
                cb::mcbp::to_status(cb::engine_errc(command_exit_code)))];
        break;
    }
    state = State::Done;
    return ENGINE_SUCCESS;
}
