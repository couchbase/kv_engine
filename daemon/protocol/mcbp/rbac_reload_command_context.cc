/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
#include "rbac_reload_command_context.h"
#include <daemon/mcbp.h>

/**
 * A small task used to reload the RBAC configuration data (it cannot run
 * in the frontend threads as it use file io.
 */
class RbacConfigReloadTask : public Task {
public:
    RbacConfigReloadTask(McbpConnection& connection_)
        : connection(connection_), status(ENGINE_SUCCESS) {
        // Empty
    }

    Status execute() override {
        try {
            LOG_NOTICE(nullptr,
                       "%u: Loading RBAC configuration from [%s] %s",
                       connection.getId(),
                       settings.getRbacFile().c_str(),
                       connection.getDescription().c_str());
            cb::rbac::loadPrivilegeDatabase(settings.getRbacFile());
            LOG_NOTICE(nullptr,
                       "%u: RBAC configuration updated %s",
                       connection.getId(),
                       connection.getDescription().c_str());
        } catch (const std::runtime_error& error) {
            LOG_WARNING(nullptr,
                        "%u: RbacConfigReloadTask(): An error occured while "
                        "loading RBAC configuration from [%s] %s: %s",
                        connection.getId(),
                        settings.getRbacFile().c_str(),
                        connection.getDescription().c_str(),
                        error.what());
            status = ENGINE_FAILED;
        }

        return Status::Finished;
    }

    void notifyExecutionComplete() override {
        notify_io_complete(connection.getCookie(), status);
    }

private:
    McbpConnection& connection;
    ENGINE_ERROR_CODE status;
};

ENGINE_ERROR_CODE RbacReloadCommandContext::reload() {
    state = State::Done;
    task = std::make_shared<RbacConfigReloadTask>(connection);
    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task);
    return ENGINE_EWOULDBLOCK;
}

void RbacReloadCommandContext::done() {
    mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}
