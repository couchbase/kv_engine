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
#include "memcached.h"
#include "nobucket_taskable.h"
#include <logger/logger.h>
#include <nlohmann/json.hpp>

StatsTask::StatsTask(TaskId id, Cookie& cookie)
    : GlobalTask(NoBucketTaskable::instance(), id), cookie(cookie) {
}

StatsTaskConnectionStats::StatsTaskConnectionStats(Cookie& cookie, int64_t fd)
    : StatsTask(TaskId::Core_StatsConnectionTask, cookie), fd(fd) {
}

bool StatsTaskConnectionStats::run() {
    try {
        iterate_all_connections([this](Connection& c) -> void {
            if (fd == -1 || c.getId() == fd) {
                stats.emplace_back(std::make_pair<std::string, std::string>(
                        {}, c.toJSON().dump()));
            }
        });
    } catch (const std::exception& exception) {
        LOG_WARNING(
                "{}: ConnectionStatsTask::execute(): An exception "
                "occurred: {}",
                cookie.getConnection().getId(),
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
