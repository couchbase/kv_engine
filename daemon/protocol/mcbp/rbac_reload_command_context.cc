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

#include <daemon/concurrency_semaphores.h>
#include <daemon/connection.h>
#include <daemon/external_auth_manager_thread.h>
#include <daemon/memcached.h>
#include <daemon/settings.h>
#include <logger/logger.h>

cb::engine_errc RbacReloadCommandContext::execute() {
    try {
        LOG_INFO_CTX("Loading RBAC configuration",
                     {"conn_id", connection.getId()},
                     {"rbac_file", Settings::instance().getRbacFile()});
        cb::rbac::loadPrivilegeDatabase(Settings::instance().getRbacFile());
        LOG_INFO_CTX("RBAC configuration updated",
                     {"conn_id", connection.getId()},
                     {"description", connection.getDescription()});

        if (externalAuthManager) {
            externalAuthManager->setRbacCacheEpoch(
                    std::chrono::steady_clock::now());
        }

        return cb::engine_errc::success;
    } catch (const std::runtime_error& error) {
        LOG_WARNING_CTX(
                "RbacConfigReloadTask(): An error occurred while loading RBAC "
                "configuration",
                {"conn_id", connection.getId()},
                {"rbac_file", Settings::instance().getRbacFile()},
                {"error", error.what()});
    }
    return cb::engine_errc::failed;
}

RbacReloadCommandContext::RbacReloadCommandContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_RbacReloadTask,
              "Refresh RBAC database",
              ConcurrencySemaphores::instance().rbac_reload) {
}
