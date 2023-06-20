/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "stats_tasks.h"
#include "connection.h"
#include "cookie.h"
#include "daemon/protocol/mcbp/engine_wrapper.h"
#include "front_end_thread.h"
#include "memcached.h"
#include "nobucket_taskable.h"
#include <logger/logger.h>

StatsTask::StatsTask(TaskId id, Cookie& cookie)
    : GlobalTask(NoBucketTaskable::instance(), id), cookie(cookie) {
}

cb::engine_errc StatsTask::getCommandError() const {
    return taskData.lock()->command_error;
}

void StatsTask::iterateStats(
        std::function<void(std::string_view, std::string_view)> callback)
        const {
    taskData.withLock([&callback](auto& data) {
        for (const auto& [k, v] : data.stats) {
            callback(k, v);
        }
    });
}

bool StatsTask::run() {
    taskData.withLock([this](auto& data) {
        getStats(data.command_error, data.stats);
        // If the handler isn't would_block we should signal the cookie
        // with "success" causing the state machine to read the actual
        // status from the task. If it is "would block" the underlying
        // engine will do this notification once its done.
        if (data.command_error != cb::engine_errc::would_block) {
            cookie.notifyIoComplete(cb::engine_errc::success);
        }
    });
    return false;
}

StatsTaskBucketStats::StatsTaskBucketStats(Cookie& cookie,
                                           std::string key,
                                           std::string value)
    : StatsTask(TaskId::Core_StatsBucketTask, cookie),
      key(std::move(key)),
      value(std::move(value)) {
}

void StatsTaskBucketStats::getStats(cb::engine_errc& command_error,
                                    StatVector& stats) {
    command_error = bucket_get_stats(
            cookie,
            key,
            cb::const_byte_buffer(
                    reinterpret_cast<const uint8_t*>(value.data()),
                    value.size()),
            [&stats](std::string_view k, std::string_view v, CookieIface&) {
                stats.emplace_back(k, v);
            });
}

std::string StatsTaskBucketStats::getDescription() const {
    return "bucket stats";
}

std::chrono::microseconds StatsTaskBucketStats::maxExpectedDuration() const {
    return std::chrono::seconds(1);
}

StatsTaskConnectionStats::StatsTaskConnectionStats(Cookie& cookie, int64_t fd)
    : StatsTask(TaskId::Core_StatsConnectionTask, cookie), fd(fd) {
}

void StatsTaskConnectionStats::getStats(cb::engine_errc& command_error,
                                        StatVector& stats) {
    try {
        iterate_all_connections([this, &stats](Connection& c) -> void {
            if (fd == -1 || c.getId() == fd) {
                stats.emplace_back(std::make_pair<std::string, std::string>(
                        {}, c.to_json().dump()));
            }
        });
    } catch (const std::exception& exception) {
        LOG_WARNING(
                "{}: StatsTaskConnectionStats::getStats(): An exception "
                "occurred: {}",
                cookie.getConnectionId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        command_error = cb::engine_errc::failed;
    }
}

std::string StatsTaskConnectionStats::getDescription() const {
    if (fd == -1) {
        return "stats connections";
    } else {
        return "stats connection " + std::to_string(fd);
    }
}

std::chrono::microseconds StatsTaskConnectionStats::maxExpectedDuration()
        const {
    return std::chrono::seconds(1);
}

StatsTaskClientConnectionDetails::StatsTaskClientConnectionDetails(
        Cookie& cookie)
    : StatsTask(TaskId::Core_StatsConnectionTask, cookie) {
}

void StatsTaskClientConnectionDetails::getStats(cb::engine_errc& command_error,
                                                StatVector& stats) {
    const auto clientConnectionMap =
            FrontEndThread::getClientConnectionDetails();
    const auto now = std::chrono::steady_clock::now();
    for (const auto& [ip, entry] : clientConnectionMap) {
        stats.emplace_back(std::string(ip), entry.to_json(now).dump());
    }
}

std::string StatsTaskClientConnectionDetails::getDescription() const {
    return "stats client connection info";
}

std::chrono::microseconds
StatsTaskClientConnectionDetails::maxExpectedDuration() const {
    return std::chrono::seconds(1);
}
