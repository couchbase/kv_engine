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

#include "stats_tasks.h"
#include "connection.h"
#include "cookie.h"
#include "daemon/protocol/mcbp/engine_wrapper.h"
#include "memcached.h"
#include "nobucket_taskable.h"
#include <logger/logger.h>

StatsTask::StatsTask(TaskId id, Cookie& cookie)
    : GlobalTask(NoBucketTaskable::instance(), id), cookie(cookie) {
}

StatsTaskBucketStats::StatsTaskBucketStats(Cookie& cookie,
                                           std::string key,
                                           std::string value)
    : StatsTask(TaskId::Core_StatsBucketTask, cookie),
      key(std::move(key)),
      value(std::move(value)) {
}

bool StatsTaskBucketStats::run() {
    std::vector<std::pair<std::string, std::string>> vec;
    command_error = bucket_get_stats(
            cookie,
            key,
            cb::const_byte_buffer(
                    reinterpret_cast<const uint8_t*>(value.data()),
                    value.size()),
            [&vec](std::string_view k, std::string_view v, const void* ctx) {
                vec.emplace_back(k, v);
            });

    stats.withLock([&vec](auto& st) {
        st.insert(st.end(),
                  std::make_move_iterator(vec.begin()),
                  std::make_move_iterator(vec.end()));
    });

    // If bucket_get_stats() returned a final "complete" status, notify
    // back to the front-end so this command can complete. If would_block was
    // returned then the engine would have scheduled its own background task
    // which will call notifyIoComplete() itself when it is complete.
    if (command_error != cb::engine_errc::would_block) {
        notifyIoComplete(cookie, cb::engine_errc::success);
    }
    return false;
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

bool StatsTaskConnectionStats::run() {
    try {
        std::vector<std::pair<std::string, std::string>> vec;
        iterate_all_connections([&vec, this](Connection& c) -> void {
            if (fd == -1 || c.getId() == fd) {
                vec.emplace_back(std::make_pair<std::string, std::string>(
                        {}, c.toJSON().dump()));
            }
        });
        stats.swap(vec);
    } catch (const std::exception& exception) {
        LOG_WARNING(
                "{}: ConnectionStatsTask::execute(): An exception "
                "occurred: {}",
                cookie.getConnectionId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        command_error = cb::engine_errc::failed;
    }

    notifyIoComplete(cookie, cb::engine_errc::success);
    return false;
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
