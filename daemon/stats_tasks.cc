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

StatsTaskConnectionStats::StatsTaskConnectionStats(Connection& connection_,
                                                   Cookie& cookie_,
                                                   const AddStatFn& add_stats_,
                                                   const int64_t fd_)
    : StatsTask(connection_, cookie_, add_stats_) {
    fd = fd_;
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
            if (c.getSocketDescriptor() == fd || fd == -1) {
                auto stats_str = c.toJSON().dump();
                add_stats(nullptr,
                          0,
                          stats_str.data(),
                          uint32_t(stats_str.size()),
                          static_cast<void*>(&cookie));
            }
        });
    } catch (const std::exception& exception) {
        LOG_WARNING(
                "{}: ConnectionStatsTask::execute(): An exception "
                "occurred: {}",
                connection.getId(),
                exception.what());
        cookie.setErrorContext("An exception occurred");
        command_error = ENGINE_FAILED;
    }
    getMutex().lock();

    return Task::Status::Finished;
}

StatsTask::StatsTask(Connection& connection_,
                     Cookie& cookie_,
                     const AddStatFn& add_stats_)
    : connection(connection_),
      cookie(cookie_),
      add_stats(add_stats_),
      command_error(ENGINE_SUCCESS) {
}

void StatsTask::notifyExecutionComplete() {
    notify_io_complete(static_cast<void*>(&cookie), ENGINE_SUCCESS);
}
