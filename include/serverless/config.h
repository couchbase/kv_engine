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

    /// Generate a JSON representation of the object
    nlohmann::json to_json() const;

    /// update the members from the values in the provided JSON (ignore unknown
    /// keys)
    void update_from_json(const nlohmann::json& json);

    /// Calculate the number of Read Units from a byte value
    size_t to_ru(size_t nbytes) {
        return calc_units(nbytes, readUnitSize.load(std::memory_order_acquire));
    }

    /// Calculate the number of Write Units from a byte value
    size_t to_wu(size_t nbytes) {
        return calc_units(nbytes,
                          writeUnitSize.load(std::memory_order_acquire));
    }

    /// Calculate the number of units with the provided size a value represents
    static inline size_t calc_units(size_t value, size_t size) {
        if (size == 0) {
            // Disabled
            return 0;
        }
        return (value + size - 1) / size;
    }

private:
    Config() = default;
};

/// Return true if the current deployment is serverless
bool isEnabled();

/// Set or disable serverless mode
void setEnabled(bool enable);

} // namespace cb::serverless
