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

#include "stats_tasks.h"
#include "connection.h"
#include "cookie.h"
#include "memcached.h"
#include <logger/logger.h>
#include <nlohmann/json.hpp>

StatsTaskConnectionStats::StatsTaskConnectionStats(Cookie& cookie, int64_t fd)
    : StatsTask(cookie), fd(fd) {
}

Task::Status StatsTaskConnectionStats::execute() {
    // This feels a bit dirty, but the problem is that when we had
    // the task being created we did hold the FrontEndThread mutex
    // when we locked the task in order to schedule it.
    // Now we want to iterate over all of the connections, and in
    // order to do that we need to lock the libevent thread so that
    // we can get exclusive access to the connection objects for that
    // thread.
    // No one is using this task so we can safely release the lock
    getMutex().unlock();
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
    getMutex().lock();

    return Task::Status::Finished;
}

StatsTask::StatsTask(Cookie& cookie) : cookie(cookie) {
}

void StatsTask::notifyExecutionComplete() {
    notifyIoComplete(cookie, cb::engine_errc::success);
}
