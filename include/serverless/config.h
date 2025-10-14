/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <nlohmann/json_fwd.hpp>
#include <atomic>
#include <cstddef>
#include <limits>

namespace cb::serverless {
constexpr size_t DefaultThrottleReservedUnits = 1666;
constexpr size_t DefaultThrottleHardLimit =
        std::numeric_limits<std::size_t>::max();
constexpr size_t MaxConnectionsPerBucket = 600;
constexpr size_t ReadUnitSize = 4096;
constexpr size_t WriteUnitSize = 1024;
constexpr size_t NodeCapacity = 25000;

struct Config {
    static Config& instance();

    /// The default throttle reserved units to use for buckets
    std::atomic<size_t> defaultThrottleReservedUnits =
            DefaultThrottleReservedUnits;

    /// The default throttle hard limit to use for buckets
    std::atomic<size_t> defaultThrottleHardLimit = DefaultThrottleHardLimit;

    /// The maximum number of (external) connections for a bucket
    std::atomic<size_t> maxConnectionsPerBucket = MaxConnectionsPerBucket;

    /// Size in bytes for the Read Unit
    std::atomic<size_t> readUnitSize{ReadUnitSize};

    /// Size in bytes for the Write Unit
    std::atomic<size_t> writeUnitSize{WriteUnitSize};

    /// The node capacity
    std::atomic<size_t> nodeCapacity{NodeCapacity};

    /// update the members from the values in the provided JSON (ignore unknown
    /// keys)
    void update_from_json(const nlohmann::json& json);

private:
    Config() = default;
};

/// Get a JSON dump of the Config
void to_json(nlohmann::json& json, const Config& cfg);

/// Return true if the current deployment is serverless
bool isEnabled();

/// Set or disable serverless mode
void setEnabled(bool enable);

} // namespace cb::serverless
