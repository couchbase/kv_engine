/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "rbac_reload_command_context.h"

#include <daemon/connection.h>
#include <daemon/external_auth_manager_thread.h>
#include <daemon/memcached.h>
#include <daemon/settings.h>
#include <executor/executor.h>
#include <logger/logger.h>

cb::engine_errc RbacReloadCommandContext::reload() {
    state = State::Done;

    cb::executor::get().add([client = &cookie]() {
        auto& connection = client->getConnection();
        auto status = cb::engine_errc::success;
        try {
            LOG_INFO("{}: Loading RBAC configuration from [{}]",
                     connection.getId(),
                     Settings::instance().getRbacFile());
            cb::rbac::loadPrivilegeDatabase(Settings::instance().getRbacFile());
            LOG_INFO("{}: RBAC configuration updated {}",
                     connection.getId(),
                     connection.getDescription());

            if (externalAuthManager) {
                externalAuthManager->setRbacCacheEpoch(
                        std::chrono::steady_clock::now());
            }
        } catch (const std::runtime_error& error) {
            LOG_WARNING(
                    "{}: RbacConfigReloadTask(): An error occurred while "
                    "loading RBAC configuration from [{}]: {}",
                    connection.getId(),
                    Settings::instance().getRbacFile(),
                    error.what());
            status = cb::engine_errc::failed;
        }
        ::notifyIoComplete(*client, status);
    });

    return cb::engine_errc::would_block;
}

void RbacReloadCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
