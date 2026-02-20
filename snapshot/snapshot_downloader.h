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

namespace cb::snapshot {
struct Manifest;

/**
 * Use the provided connection and download the snapshot provided in
 * the provided manifest into the the provided directory
 */
void download(std::unique_ptr<MemcachedConnection> connection,
              const std::filesystem::path& directory,
              const Manifest& snapshot,
              std::size_t fsync_interval,
              std::size_t write_size,
              std::size_t checksum_length,
              bool allow_fail_fast,
              std::optional<std::size_t> error_sink_write_size,
              const std::function<void(spdlog::level::level_enum,
                                       std::string_view,
                                       cb::logger::Json json)>& log_callback,
              const std::function<void(std::size_t)>& stats_collect_callback,
              const std::function<std::size_t()>& get_throttle_rate);
} // namespace cb::snapshot
