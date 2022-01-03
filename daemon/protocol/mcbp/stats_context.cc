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
#include <daemon/server_socket.h>
#include <daemon/settings.h>
#include <daemon/stats.h>
#include <daemon/stats_tasks.h>
#include <daemon/tenant_manager.h>
#include <executor/executorpool.h>
#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/stat_group.h>
#include <memcached/tenant.h>
#include <nlohmann/json.hpp>
#include <phosphor/stats_callback.h>
#include <phosphor/trace_log.h>
#include <platform/cb_arena_malloc.h>
#include <platform/checked_snprintf.h>
#include <statistics/cbstat_collector.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <cinttypes>

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
    add_stat_callback(name, value, &cookie);
}

void add_stat(Cookie& cookie,
              const AddStatFn& add_stat_callback,
              const char* name,
              const char* value) {
    add_stat_callback(name,
                      value,
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

static void append_stats(std::string_view key,
                         std::string_view value,
                         const void* ctx) {
    auto& cookie = *const_cast<Cookie*>(reinterpret_cast<const Cookie*>(ctx));

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
        if (!o.empty()) {
            array.push_back(o);
        }
    }
    json["buckets"] = array;

    const auto stats_str = json.dump();
    append_stats("bucket details"sv, stats_str, static_cast<void*>(&cookie));
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
            append_stats(key, hist, &cookie);
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
        append_stats(key, hist, &cookie);
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
        CBStatCollector collector(appendStatsFn, &cookie);
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
 * @param arg - should be empty
 * @param cookie the command context
 */
static cb::engine_errc stat_bucket_details_executor(const std::string& arg,
                                                    Cookie& cookie) {
    if (arg.empty()) {
        process_bucket_details(cookie);
        return cb::engine_errc::success;
    } else {
        return cb::engine_errc::invalid_arguments;
    }
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
        CBStatCollector collector(appendStatsFn, &cookie);
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

    CBStatCollector collector(appendStatsFn, &cookie);
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

    if (!arg.empty()) {
        if (arg == "self") {
            append_stats({}, cookie.getConnection().toJSON().dump(), &cookie);
            return cb::engine_errc::success;
        } else {
            try {
                fd = std::stoll(arg);
            } catch (...) {
                return cb::engine_errc::invalid_arguments;
            }
        }
    }

    if (fd == -1 || fd != int64_t(cookie.getConnectionId())) {
        if (cookie.checkPrivilege(cb::rbac::Privilege::Stats).failed()) {
            if (fd != -1) {
                // The client asked for a given file descriptor.
                return cb::engine_errc::no_access;
            }
            // The client didn't specify a connection, so return "self"
            fd = int64_t(cookie.getConnectionId());
        }
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
        append_stats({}, json_str, &cookie);
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

        append_stats("responses"sv, json.dump(), &cookie);
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
                    {key.data(), key.size()}, {value.data(), value.size()}, &c);
        }

        void operator()(gsl_p::cstring_span key, bool value) override {
            const auto svalue = value ? "true"sv : "false"sv;
            append_stats({key.data(), key.size()}, svalue.data(), &c);
        }

        void operator()(gsl_p::cstring_span key, size_t value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), &c);
        }

        void operator()(gsl_p::cstring_span key,
                        phosphor::ssize_t value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), &c);
        }

        void operator()(gsl_p::cstring_span key, double value) override {
            append_stats({key.data(), key.size()}, std::to_string(value), &c);
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

static cb::engine_errc stat_tenant_executor(const std::string& arg,
                                            Cookie& cookie) {
    std::shared_ptr<StatsTask> task =
            std::make_shared<StatsTenantsStats>(cookie, arg);
    cookie.obtainContext<StatsCommandContext>(cookie).setTask(task);
    ExecutorPool::get()->schedule(task);
    return cb::engine_errc::would_block;
}

static cb::engine_errc stat_all_stats(const std::string& arg, Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    auto ret = bucket_get_stats(cookie, arg, value, appendStatsFn);
    if (ret != cb::engine_errc::success) {
        return ret;
    }

    CBStatCollector collector(appendStatsFn, &cookie);
    return server_stats(collector, cookie.getConnection().getBucket());
}

static cb::engine_errc stat_bucket_stats(const std::string& arg,
                                         Cookie& cookie) {
    auto value = cookie.getRequest().getValue();
    return bucket_get_stats(cookie, arg, value, appendStatsFn);
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
            append_stats(
                    "allocator", {buf.data(), buf.size()}, &cbdata->cookie);
            buf.clear();
        }
    };
    cb::ArenaMalloc::getDetailedStats(callback, &callbackData);
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
                {StatGroupId::Tenants, {true, stat_tenant_executor}},
                {StatGroupId::Allocator, {true, stat_allocator_executor}},
                {StatGroupId::Scopes, {false, stat_bucket_collections_stats}},
                {StatGroupId::ScopesById,
                 {false, stat_bucket_collections_stats}},
                {StatGroupId::Collections,
                 {false, stat_bucket_collections_stats}},
                {StatGroupId::CollectionsById,
                 {false, stat_bucket_collections_stats}}};

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
    auto handler_pair = getStatHandler(statgroup);

    if (!handler_pair.second) {
        const auto key = cookie.getRequest().getKey();
        command_exit_code = handler_pair.first.handler(
                {reinterpret_cast<const char*>(key.data()), key.size()},
                cookie);
    } else {
        command_exit_code = handler_pair.first.handler(argument, cookie);
    }

    // If stats command call returns cb::engine_errc::would_block and the task
    // is not a nullptr (ie we have created a background task), then change the
    // state to be GetTaskResult and return cb::engine_errc::would_block
    if (command_exit_code == cb::engine_errc::would_block && task) {
        state = State::GetTaskResult;
        return cb::engine_errc::would_block;
    }

    state = State::CommandComplete;
    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::getTaskResult() {
    auto& stats_task = dynamic_cast<StatsTask&>(*task);

    state = State::CommandComplete;
    command_exit_code = stats_task.getCommandError();
    if (command_exit_code == cb::engine_errc::success) {
        for (const auto& s : stats_task.getStats()) {
            append_stats(s.first,
                         s.second,
                         static_cast<const CookieIface*>(&cookie));
        }
    }
    return cb::engine_errc::success;
}

cb::engine_errc StatsCommandContext::commandComplete() {
    switch (command_exit_code) {
    case cb::engine_errc::success:
        append_stats({}, {}, static_cast<CookieIface*>(&cookie));

        // We just want to record this once rather than for each packet sent
        ++connection.getBucket()
                  .responseCounters[int(cb::mcbp::Status::Success)];
        break;
    case cb::engine_errc::would_block:
        /* If the stats call returns cb::engine_errc::would_block then set the
         * state to DoStats again and return this error code */
        state = State::DoStats;
        return command_exit_code;
    case cb::engine_errc::disconnect:
        // We don't send these responses back so we will not store
        // stats for these.
        break;
    default:
        ++connection.getBucket().responseCounters[int(
                cb::mcbp::to_status(cb::engine_errc(command_exit_code)))];
        break;
    }
    state = State::Done;
    return cb::engine_errc::success;
}
