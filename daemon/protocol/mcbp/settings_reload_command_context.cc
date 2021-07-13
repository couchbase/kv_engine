/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "settings_reload_command_context.h"

#include <daemon/cmdline.h>
#include <daemon/config_parse.h>
#include <daemon/connection.h>
#include <daemon/mcaudit.h>
#include <daemon/memcached.h>
#include <daemon/one_shot_task.h>
#include <daemon/settings.h>
#include <executor/executorpool.h>
#include <logger/logger.h>
#include <platform/dirutils.h>

SettingsReloadCommandContext::SettingsReloadCommandContext(Cookie& cookie)
    : FileReloadCommandContext(cookie) {
}

cb::engine_errc SettingsReloadCommandContext::doSettingsReload() {
    const auto old_priv_debug = Settings::instance().isPrivilegeDebug();
    try {
        LOG_INFO("Reloading config file {}", get_config_file());
        const auto content =
                cb::io::loadFile(get_config_file(), std::chrono::seconds{5});
        Settings new_settings(nlohmann::json::parse(content));
        Settings::instance().updateSettings(new_settings, true);

        if (Settings::instance().isPrivilegeDebug() != old_priv_debug) {
            audit_set_privilege_debug_mode(
                    cookie, Settings::instance().isPrivilegeDebug());
        }
        return cb::engine_errc::success;
    } catch (const std::bad_alloc&) {
        LOG_WARNING("{}: Failed reloading config file. not enough memory",
                    cookie.getConnectionId());
        return cb::engine_errc::no_memory;
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::too_many_files_open) {
            LOG_WARNING("{}: Failed reloading config file. too many files open",
                        cookie.getConnectionId());
            return cb::engine_errc::temporary_failure;
        }
        cookie.setErrorContext(error.what());
    } catch (const std::exception& exception) {
        cookie.setErrorContext(exception.what());
    } catch (...) {
        cookie.setErrorContext("Unknown error");
    }

    LOG_WARNING("{}: {} - Failed reloading config file '{}'. Error: {}",
                cookie.getConnectionId(),
                cookie.getEventId(),
                get_config_file(),
                cookie.getErrorContext());
    return cb::engine_errc::failed;
}

cb::engine_errc SettingsReloadCommandContext::reload() {
    ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
            TaskId::Core_SettingsReloadTask, "Reload memcached.json", [this]() {
                ::notifyIoComplete(cookie, doSettingsReload());
            }));
    return cb::engine_errc::would_block;
}
