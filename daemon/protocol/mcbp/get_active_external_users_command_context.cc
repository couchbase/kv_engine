/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include "get_active_external_users_command_context.h"

#include <cbsasl/domain.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/executorpool.h>
#include <daemon/log_macros.h>
#include <daemon/memcached.h>
#include <daemon/task.h>

#include <unordered_set>

/**
 * The CountActiveExternalUsersTask is responsible for traversing all of the
 * connections and count the unique number of users currently connected
 */
class CountActiveExternalUsersTask : public Task {
public:
    CountActiveExternalUsersTask() = delete;
    CountActiveExternalUsersTask(const CountActiveExternalUsersTask&) = delete;
    explicit CountActiveExternalUsersTask(Cookie& cookie)
        : Task(), cookie(cookie) {
    }

    Status execute() override;
    void notifyExecutionComplete() override;

    std::string getPayload() const {
        return payload;
    }

protected:
    Cookie& cookie;
    std::unordered_set<std::string> external;
    std::string payload;
};

Task::Status CountActiveExternalUsersTask::execute() {
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
        iterate_all_connections([this](Connection& connection) -> void {
            if (connection.isAuthenticated() &&
                connection.getDomain() == cb::sasl::Domain::External) {
                const std::string username = connection.getUsername();
                external.insert(username);
            }
        });

        nlohmann::json json = external;
        payload = json.dump();
    } catch (const std::exception& e) {
        LOG_WARNING(
                "CountActiveExternalUsersTask::execute: received exception: {}",
                e.what());
    }
    getMutex().lock();

    return Status::Finished;
}

void CountActiveExternalUsersTask::notifyExecutionComplete() {
    notify_io_complete(static_cast<const void*>(&cookie), ENGINE_SUCCESS);
}

GetActiveExternalUsersCommandContext::GetActiveExternalUsersCommandContext(
        Cookie& cookie)
    : SteppableCommandContext(cookie), state(State::Initial) {
}

ENGINE_ERROR_CODE GetActiveExternalUsersCommandContext::step() {
    auto ret = ENGINE_SUCCESS;
    do {
        switch (state) {
        case State::Initial:
            ret = initial();
            break;
        case State::Done:
            done();
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);
    return ret;
}

ENGINE_ERROR_CODE GetActiveExternalUsersCommandContext::initial() {
    task = std::make_shared<CountActiveExternalUsersTask>(cookie);
    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);
    state = State::Done;
    return ENGINE_EWOULDBLOCK;
}

void GetActiveExternalUsersCommandContext::done() {
    auto& activeTask =
            *reinterpret_cast<CountActiveExternalUsersTask*>(task.get());
    cookie.sendResponse(cb::mcbp::Status::Success,
                        {},
                        {},
                        activeTask.getPayload(),
                        cb::mcbp::Datatype::JSON,
                        0);
}
