/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "stats_context.h"

#include "engine_wrapper.h"
#include "utilities.h"

#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <daemon/front_end_thread.h>
#include <daemon/mc_time.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/nobucket_taskable.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <daemon/stats_tasks.h>
#include <executor/executorpool.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/stat_group.h>
#include <nlohmann/json.hpp>
#include <phosphor/stats_callback.h>
#include <phosphor/trace_log.h>
#include <platform/cb_arena_malloc.h>
#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>
#include <statistics/labelled_collector.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <chrono>
#include <cinttypes>
#include <stdexcept>

using namespace std::string_view_literals;

/*************************** ADD STAT CALLBACKS ***************************/

// Generic add_stat<T>. Uses std::to_string which requires heap allocation.
template <typename T>
void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const T& val) {
    add_stat_callback(name, std::to_string(val), &cookie);
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const std::string& value) {
    add_stat_callback(name, value, cookie);
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const char* value) {
    add_stat_callback(name, value, cookie);
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

static void append_stats(std::string_view key,
                         std::string_view value,
                         Cookie& cookie) {
    cb::mcbp::Response header = {};
    header.setMagic(cb::mcbp::Magic::ClientResponse);
    header.setOpcode(cb::mcbp::ClientOpcode::Stat);
    header.setDatatype(cb::mcbp::Datatype::Raw);
    header.setStatus(cb::mcbp::Status::Success);
    header.setFramingExtraslen(0);
    header.setExtlen(0);
    header.setKeylen(key.size());
    header.setBodylen(key.size() + value.size());
    header.setOpaque(cookie.getHeader().getOpaque());
    auto& c = cookie.getConnection();
    c.copyToOutputStream(
            {reinterpret_cast<const char*>(&header), sizeof(header)},
            key,
            value);
}

/// Wrapper function for the _external_ functions.
static void external_append_stats(std::string_view key,
                                  std::string_view value,
                                  CookieIface& ctx) {
    auto& cookie = asCookie(ctx);
    append_stats(key, value, cookie);
}

// Create a static std::function to wrap append_stats, instead of creating a
// temporary object every time we need to call into an engine.
// This also avoids problems where the stack-allocated AddStatFn could go
// out of scope if someone needs to take a copy of it and run it on another
// thread.
static AddStatFn appendStatsFn = external_append_stats;

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
static cb::engine_errc stat_reset_executor(const std::string& arg,
                                           Cookie& cookie) {
    if (arg.empty()) {
        stats_reset(cookie);
        bucket_reset_stats(cookie);
        all_buckets[0].timings.reset();
        cookie.getConnection().getBucket().timings.reset();
        return cb::engine_errc::success;
    } else if (arg == "timings") {
        // Nuke the command timings section for the connected bucket
        cookie.getConnection().getBucket().timings.reset();
        return cb::engine_errc::success;
    } else {
        return cb::engine_errc::invalid_arguments;
    }
}

/**
 * Handler for the <code>stats sched</code> used to get the
 * histogram for the scheduler histogram.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_sched_executor(const std::string& arg,
                                           Cookie& cookie) {
    if (arg.empty()) {
        for (size_t ii = 0; ii < Settings::instance().getNumWorkerThreads();
             ++ii) {
            auto hist = scheduler_info[ii].to_string();
            std::string key = std::to_string(ii);
            append_stats(key, hist, cookie);
        }
        return cb::engine_errc::success;
    }

    if (arg == "aggregate") {
        static const std::string key = {"aggregate"};
        Hdr1sfMicroSecHistogram histogram{};
        for (const auto& h : scheduler_info) {
            histogram += h;
        }
        // Add the stat
        auto hist = histogram.to_string();
        append_stats(key, hist, cookie);
        return cb::engine_errc::success;
    }

    return cb::engine_errc::invalid_arguments;
}

/**
 * Handler for the <code>stats audit</code> used to get statistics from
 * the audit subsystem.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_audit_executor(const std::string& arg,
                                           Cookie& cookie) {
    if (arg.empty()) {
        CBStatCollector collector(appendStatsFn, cookie);
        stats_audit(collector, &cookie);
        return cb::engine_errc::success;
    } else {
        return cb::engine_errc::invalid_arguments;
    }
}

/**
 * Handler for the <code>stats bucket details</code> used to get information
 * of the buckets (type, state, #clients etc)
 *
 * @param arg - empty (all buckets) or the name of a bucket
 * @param cookie the command context
 */
static cb::engine_errc stat_bucket_details_executor(const std::string& arg,
                                                    Cookie& cookie) {
    if (arg.empty()) {
        // Return all buckets
        nlohmann::json array = nlohmann::json::array();

        for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
            Bucket& bucket = BucketManager::instance().at(ii);
            auto json = bucket.to_json();
            if (!json.empty()) {
                json["index"] = ii;
                array.emplace_back(std::move(json));
            }
        }

        nlohmann::json json;
        json["buckets"] = array;
        const auto stats_str = json.dump();
        append_stats("bucket details"sv, stats_str, cookie);
        return cb::engine_errc::success;
    }

    // Return the bucket details for the bucket with the requested name
    // To avoid racing with bucket creation/deletion/pausing and all the
    // other commands potentially changing bucket states _AND_ make sure
    // we don't skip any states lets create a json dump of the bucket and
    // check if it was the bucket we wanted.
    for (size_t ii = 0; ii < all_buckets.size(); ++ii) {
        Bucket& bucket = BucketManager::instance().at(ii);
        auto json = bucket.to_json();
        if (!json.empty() && json["name"] == arg) {
            const auto stats_str = json.dump();
            append_stats(arg, stats_str, cookie);
            return cb::engine_errc::success;
        }
    }

    return cb::engine_errc::no_such_key;
}

/**
 * Handler for the <code>stats aggregate</code>.. probably not used anymore
 * as it gives just a subset of what you'll get from an empty stat.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_aggregate_executor(const std::string& arg,
                                               Cookie& cookie) {
    if (arg.empty()) {
        CBStatCollector collector(appendStatsFn, cookie);
        return server_stats(collector, cookie.getConnection().getBucket());
    } else {
        return cb::engine_errc::invalid_arguments;
    }
}

/**
 * Handler for the <code>clocks</code> command to retrieve
 * information about the clocks in the system.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_clocks_executor(const std::string& arg,
                                            Cookie& cookie) {
    if (!arg.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    CBStatCollector collector(appendStatsFn, cookie);
    server_clock_stats(collector);
    return cb::engine_errc::success;
}

/**
 * Handler for the <code>stats connection[ fd]</code> command to retrieve
 * information about connection specific details. An fd specified as "self"
 * means the calling connection.
 *
 * @param arg an optional file descriptor representing the connection
 *            object to retrieve information about. If empty dump all.
 * @param cookie the command context
 */
static cb::engine_errc stat_connections_executor(const std::string& arg,
                                                 Cookie& cookie) {
    int64_t fd = -1;
    bool me = false;

    if (!arg.empty()) {
        if (arg == "self") {
            me = true;
        } else {
            try {
                fd = std::stoll(arg);
                if (fd < 0) {
                    cookie.setErrorContext(
                            "Connection must be a positive number");
                    return cb::engine_errc::invalid_arguments;
                }
            } catch (...) {
                return cb::engine_errc::invalid_arguments;
            }
        }
    }

    if (!me) {
        // The client is asking for itself, that's ok
        me = fd == int64_t(cookie.getConnectionId());
    }

    if (!me && fd == -1 &&
        cookie.testPrivilege(cb::rbac::Privilege::Stats).failed()) {
        // The client didn't ask for given connection and does not have
        // access to see all connections.
        //
        // Limit to _this_ connection.
        me = true;
    }

    if (me) {
        append_stats({}, cookie.getConnection().to_json().dump(), cookie);
        return cb::engine_errc::success;
    }

    // The client wants all clients OR a given (other) connection.
    // The client must hold the stats privilege. Use checkPrivilege here
    // as it'll log on failure and injects extra info in the response
    if (cookie.checkPrivilege(cb::rbac::Privilege::Stats).failed()) {
        return cb::engine_errc::no_access;
    }

    std::shared_ptr<StatsTask> task =
            std::make_shared<StatsTaskConnectionStats>(cookie, fd);
    cookie.obtainContext<StatsCommandContext>(cookie).setTask(task);
    ExecutorPool::get()->schedule(task);
    return cb::engine_errc::would_block;
}

/**
 * Helper function to append JSON-formatted histogram statistics for the
 * specified Bucket histogram data member.
 */
static cb::engine_errc stat_histogram_executor(
        const std::string& arg,
        Cookie& cookie,
        Hdr1sfMicroSecHistogram Bucket::*histogram) {
    if (arg.empty()) {
        const auto index = cookie.getConnection().getBucketIndex();
        std::string json_str;
        if (index == 0) {
            // Aggregrated timings for all buckets.
            Hdr1sfMicroSecHistogram aggregated{};
            for (const auto& bucket : all_buckets) {
                aggregated += bucket.*histogram;
            }
            json_str = aggregated.to_string();
        } else {
            // Timings for a specific bucket.
            auto& bucket = cookie.getConnection().getBucket();
            json_str = (bucket.*histogram).to_string();
        }
        append_stats({}, json_str, cookie);
        return cb::engine_errc::success;
    } else {
        return cb::engine_errc::invalid_arguments;
    }
}

/**
 * Handler for the <code>stats json_validate</code> command used to retrieve
 * histograms of how long it took to validate JSON.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_json_validate_executor(const std::string& arg,
                                                   Cookie& cookie) {
    return stat_histogram_executor(arg, cookie, &Bucket::jsonValidateTimes);
}

/**
 * Handler for the <code>stats snappy_decompress</code> command used to retrieve
 * histograms of how long it took to decompress Snappy compressed values.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_snappy_decompress_executor(const std::string& arg,
                                                       Cookie& cookie) {
    return stat_histogram_executor(
            arg, cookie, &Bucket::snappyDecompressionTimes);
}

/**
 * Handler for the <code>stats subdoc_execute</code> command used to retrieve
 * information from the subdoc subsystem.
 *
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_subdoc_execute_executor(const std::string& arg,
                                                    Cookie& cookie) {
    return stat_histogram_executor(
            arg, cookie, &Bucket::subjson_operation_times);
}

static cb::engine_errc stat_responses_json_executor(const std::string& arg,
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

        append_stats("responses"sv, json.dump(), cookie);
        return cb::engine_errc::success;
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::no_memory;
    }
}

static cb::engine_errc stat_tracing_executor(const std::string& arg,
                                             Cookie& cookie) {
    class MemcachedCallback : public phosphor::StatsCallback {
    public:
        explicit MemcachedCallback(Cookie& cookie) : c(cookie) {
        }

        void operator()(gsl_p::cstring_span key,
                        gsl_p::cstring_span value) override {
            append_stats(
                    {key.data(), key.size()}, {value.data(), value.size()}, c);
        }

        void operator()(gsl_p::cstring_span key, bool value) override {
            const auto svalue = value ? "true"sv : "false"sv;
            append_stats({key.data(), key.size()}, svalue.data(), c);
        }

        void operator()(gsl_p::cstring_span key, size_t value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), c);
        }

        void operator()(gsl_p::cstring_span key,
                        phosphor::ssize_t value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), c);
        }

        void operator()(gsl_p::cstring_span key, double value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), c);
        }

    private:
        Cookie& c;
    };

    if (arg.empty()) {
        MemcachedCallback cb{cookie};
        phosphor::TraceLog::getInstance().getStats(cb);
        return cb::engine_errc::success;
    } else {
        return cb::engine_errc::invalid_arguments;
    }
}

static cb::engine_errc stat_all_stats(const std::string& arg, Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    auto ret = bucket_get_stats(cookie, arg, value, appendStatsFn);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    CBStatCollector collector(appendStatsFn, cookie);
    return server_stats(collector, cookie.getConnection().getBucket());
}

static cb::engine_errc stat_bucket_stats(const std::string&, Cookie& cookie) {
    auto key = cookie.getRequest().getKeyString();
    auto value = cookie.getRequest().getValueString();

    std::shared_ptr<StatsTask> task = std::make_shared<StatsTaskBucketStats>(
            cookie, std::string(key), std::string(value));
    cookie.obtainContext<StatsCommandContext>(cookie).setTask(task);
    ExecutorPool::get()->schedule(task);
    return cb::engine_errc::would_block;
}

// handler for scopes/collections - engine needs the key for processing
static cb::engine_errc stat_bucket_collections_stats(const std::string&,
                                                     Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    const auto key = cookie.getRequest().getKey();
    return bucket_get_stats(
            cookie,
            std::string_view{reinterpret_cast<const char*>(key.data()),
                             key.size()},
            value,
            appendStatsFn);
}

static cb::engine_errc stat_allocator_executor(const std::string&,
                                               Cookie& cookie) {
    std::vector<char> lineBuffer;
    lineBuffer.reserve(256);
    struct CallbackData {
        std::vector<char>& lineBuffer;
        Cookie& cookie;
    } callbackData{lineBuffer, cookie};

    // je_malloc will return each line of output in multiple callbacks, we will
    // join those together and send individual lines to the client (without the
    // newline), if mcstat is the caller each returned 'line' is printed with
    // a newline and we get nicely formatted statistics.
    static auto callback = [](void* opaque, const char* msg) {
        auto* cbdata = reinterpret_cast<CallbackData*>(opaque);
        std::string_view message(msg);
        auto& buf = cbdata->lineBuffer;
        bool newlineFound = false;

        // Use copy_if so we can scan for newlines whilst copying to our buffer
        std::copy_if(message.begin(),
                     message.end(),
                     std::back_inserter(buf),
                     [&newlineFound](char c) {
                         if (c == '\n') {
                             newlineFound = true;
                             return false;
                         }
                         return true;
                     });

        if (newlineFound) {
            append_stats("allocator", {buf.data(), buf.size()}, cbdata->cookie);
            buf.clear();
        }
    };
    cb::ArenaMalloc::getDetailedStats(callback, &callbackData);
    return cb::engine_errc::success;
}

static cb::engine_errc stat_timings_executor(const std::string&,
                                             Cookie& cookie) {
    auto& bucket = cookie.getConnection().getBucket();
    bucket.statTimings.addStats(
            CBStatCollector(appendStatsFn, cookie).forBucket(bucket.name));

    return cb::engine_errc::success;
}

static cb::engine_errc stat_threads_executor(const std::string& key,
                                             Cookie& cookie) {
    auto& setting = Settings::instance();

    append_stats(std::string{"num_frontend_threads"},
                 std::to_string(setting.getNumWorkerThreads()),
                 cookie);
    append_stats(std::string{"num_reader_threads"},
                 std::to_string(setting.getNumReaderThreads()),
                 cookie);
    append_stats(std::string{"num_writer_threads"},
                 std::to_string(setting.getNumWriterThreads()),
                 cookie);
    append_stats(std::string{"num_auxio_threads"},
                 std::to_string(setting.getNumAuxIoThreads()),
                 cookie);
    append_stats(std::string{"num_nonio_threads"},
                 std::to_string(setting.getNumNonIoThreads()),
                 cookie);

    return cb::engine_errc::success;
}

static cb::engine_errc stat_tasks_all_executor(const std::string& key,
                                               Cookie& cookie) {
    // BucketManager.forEach() isn't perfect, it only gathers stats for
    // Buckets in the Ready state, but it's a pain to touch non-ready Buckets...
    BucketManager::instance().forEach([&cookie](Bucket& bucket) {
        if (bucket.type == BucketType::ClusterConfigOnly ||
            bucket.type == BucketType::NoBucket) {
            // continue - the other Buckets don't necessarily have engines
            return true;
        }

        auto value = cookie.getRequest().getValue();
        bucket.getEngine().get_stats(
                cookie,
                "tasks",
                {reinterpret_cast<const char*>(value.data()), value.size()},
                appendStatsFn);

        // continue
        return true;
    });

    // We have a bunch of tasks associated to the NoBucketTaskable and we should
    // grab stats for those too as it may prove useful.
    ExecutorPool::get()->doTasksStat(
            NoBucketTaskable::instance(), cookie, appendStatsFn);

    return cb::engine_errc::success;
}

/***************************** STAT HANDLERS *****************************/

struct command_stat_handler {
    /// Is the privilege checked by StatsCommandContext::checkPrivilege?
    bool checkPrivilege;

    /**
     * The callback function to handle the stat request
     */
    cb::engine_errc (*handler)(const std::string& arg, Cookie& cookie);
};

/**
 * A mapping from all stat subgroups to the callback providing the
 * statistics
 */
static std::unordered_map<StatGroupId, struct command_stat_handler>
        stat_handlers = {
                {StatGroupId::All, {true, stat_all_stats}},
                {StatGroupId::Reset, {true, stat_reset_executor}},
                {StatGroupId::WorkerThreadInfo, {true, stat_sched_executor}},
                {StatGroupId::Audit, {true, stat_audit_executor}},
                {StatGroupId::BucketDetails,
                 {true, stat_bucket_details_executor}},
                {StatGroupId::Aggregate, {true, stat_aggregate_executor}},
                {StatGroupId::Connections, {true, stat_connections_executor}},
                {StatGroupId::Clocks, {true, stat_clocks_executor}},
                {StatGroupId::JsonValidate,
                 {true, stat_json_validate_executor}},
                {StatGroupId::SnappyDecompress,
                 {true, stat_snappy_decompress_executor}},
                {StatGroupId::SubdocExecute,
                 {true, stat_subdoc_execute_executor}},
                {StatGroupId::Responses, {true, stat_responses_json_executor}},
                {StatGroupId::Tracing, {true, stat_tracing_executor}},
                {StatGroupId::Allocator, {true, stat_allocator_executor}},
                {StatGroupId::Scopes, {false, stat_bucket_collections_stats}},
                {StatGroupId::ScopesById,
                 {false, stat_bucket_collections_stats}},
                {StatGroupId::Collections,
                 {false, stat_bucket_collections_stats}},
                {StatGroupId::CollectionsById,
                 {false, stat_bucket_collections_stats}},
                {StatGroupId::StatTimings, {true, stat_timings_executor}},
                {StatGroupId::Threads, {true, stat_threads_executor}},
                {StatGroupId::TasksAll, {false, stat_tasks_all_executor}}};

/**
 * For a given key, try and return the handler for it
 * @param key The key we wish to handle
 * @return std::pair<stat_handler, bool> returns the stat handler and a bool
 * representing whether this is a key we recognise or not
 */
static std::pair<command_stat_handler, bool> getStatHandler(
        const StatGroup* group) {
    if (!group) {
        return {command_stat_handler{true, stat_bucket_stats}, false};
    }
    auto iter = stat_handlers.find(group->id);
    if (iter == stat_handlers.end()) {
        return {command_stat_handler{true, stat_bucket_stats}, false};
    } else {
        return {iter->second, true};
    }
}

StatsCommandContext::StatsCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie), state(State::ParseCommandKey) {
}

/************************* STATE MACHINE EXECUTION *************************/

cb::engine_errc StatsCommandContext::step() {
    auto ret = cb::engine_errc::success;
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
    } while (ret == cb::engine_errc::success);

    return ret;
}

cb::engine_errc StatsCommandContext::parseCommandKey() {
    const auto key = cookie.getRequest().getKey();
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
    statgroup = StatsGroupManager::getInstance().lookup(command);

    state = State::CheckPrivilege;
    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::checkPrivilege() {
    auto ret = cb::engine_errc::success;

    if (statgroup) {
        auto handler = getStatHandler(statgroup).first;
        if (handler.checkPrivilege) {
            if (statgroup->privileged) {
                ret = mcbp::checkPrivilege(cookie, cb::rbac::Privilege::Stats);
            }

            if (ret == cb::engine_errc::success && statgroup->bucket) {
                ret = mcbp::checkPrivilege(cookie,
                                           cb::rbac::Privilege::SimpleStats);
            }
        }
    } else {
        // The stat key don't exist, but only tell the user if the connection
        // possess the SimpleStats privilege
        if (cookie.testPrivilege(cb::rbac::Privilege::SimpleStats, {}, {})
                    .success()) {
            ret = cb::engine_errc::no_such_key;
        } else {
            ret = cb::engine_errc::no_access;
        }
    }

    switch (ret) {
    case cb::engine_errc::success:
        state = State::DoStats;
        break;
    default:
        command_exit_code = ret;
        state = State::CommandComplete;
        break;
    }

    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::doStats() {
    const auto [callback, known] = getStatHandler(statgroup);

    start = std::chrono::steady_clock::now();
    if (known) {
        command_exit_code = callback.handler(argument, cookie);
    } else {
        command_exit_code = callback.handler(
                std::string{cookie.getRequest().getKeyString()}, cookie);
    }

    // If stats command call returns cb::engine_errc::would_block and the task
    // is not a nullptr (ie we have created a background task), then change the
    // state to be GetTaskResult and return cb::engine_errc::would_block
    if (command_exit_code == cb::engine_errc::would_block) {
        if (task) {
            state = State::GetTaskResult;
        }

        return cb::engine_errc::would_block;
    }

    state = State::CommandComplete;
    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::getTaskResult() {
    auto& stats_task = dynamic_cast<StatsTask&>(*task);

    command_exit_code = stats_task.getCommandError();
    if (command_exit_code == cb::engine_errc::would_block) {
        // The call to stats blocked, so we need to retry
        state = State::DoStats;
    } else {
        state = State::CommandComplete;
        if (command_exit_code == cb::engine_errc::success) {
            for (const auto& s : stats_task.getStats()) {
                append_stats(s.first, s.second, cookie);
            }
        }
    }

    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::commandComplete() {
    auto& bucket = connection.getBucket();
    switch (command_exit_code) {
    case cb::engine_errc::success:
        append_stats({}, {}, cookie);

        // We just want to record this once rather than for each packet sent
        ++bucket.responseCounters[int(cb::mcbp::Status::Success)];
        // record the time taken to generate results for this stat group
        bucket.statTimings.record(statgroup->id,
                                  !argument.empty() /* has argument */,
                                  std::chrono::steady_clock::now() - start);
        break;
    case cb::engine_errc::would_block:
        throw std::logic_error(
                "StatsCommandContext::commandComplete(): Should never get here "
                "with EWB state");

    case cb::engine_errc::disconnect:
        // We don't send these responses back so we will not store
        // stats for these.
        break;
    default:
        ++bucket.responseCounters[int(
                cb::mcbp::to_status(cb::engine_errc(command_exit_code)))];
        break;
    }
    state = State::Done;
    return cb::engine_errc::success;
}
