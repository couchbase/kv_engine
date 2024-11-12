/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json.hpp>
#include <platform/json_log.h>
#include <protocol/connection/client_connection.h>
#include <spdlog/spdlog.h>
#include <filesystem>

namespace snapshot {
/**
 * Use the provided connection and download the snapshot provided in
 * the provided manifest into the the provided directory
 *
 * @todo plug in a listener to log and metrics
 */
void download(std::unique_ptr<MemcachedConnection> connection,
              const std::filesystem::path& directory,
              const nlohmann::json& snapshot,
              const std::function<void(spdlog::level::level_enum,
                                       std::string_view,
                                       cb::logger::Json json)>& log_callback);
} // namespace snapshot
