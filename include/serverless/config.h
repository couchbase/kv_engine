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

namespace cb::serverless {
constexpr size_t DefaultThrottleLimit = 1666;
constexpr size_t MaxConnectionsPerBucket = 600;
constexpr size_t ReadComputeUnitSize = 4096;
constexpr size_t WriteComputeUnitSize = 1024;

struct Config {
    static Config& instance();

    /// The default throttle limit to use for buckets
    std::atomic<size_t> defaultThrottleLimit = DefaultThrottleLimit;

    /// The maximum number of (external) connections for a bucket
    std::atomic<size_t> maxConnectionsPerBucket = MaxConnectionsPerBucket;

    /// Size in bytes for the Read Compute Unit
    std::atomic<size_t> readComputeUnitSize{ReadComputeUnitSize};

    /// Size in bytes for the Write Compute Unit
    std::atomic<size_t> writeComputeUnitSize{WriteComputeUnitSize};

    /// Generate a JSON representation of the object
    nlohmann::json to_json() const;

    /// update the members from the values in the provided JSON (ignore unknown
    /// keys)
    void update_from_json(const nlohmann::json& json);

    /// Calculate the number of Read Compute Units from a byte value
    size_t to_rcu(size_t nbytes) {
        return calc_cu(nbytes,
                       readComputeUnitSize.load(std::memory_order_acquire));
    }

    /// Calculate the number of Write Compute Units from a byte value
    size_t to_wcu(size_t nbytes) {
        return calc_cu(nbytes,
                       writeComputeUnitSize.load(std::memory_order_acquire));
    }

    /// Calculate the number of compute units with the provided size a value
    /// represents
    static inline size_t calc_cu(size_t value, size_t size) {
        if (size == 0) {
            // Disabled
            return 0;
        }
        return (value + size - 1) / size;
    }

private:
    Config() = default;
};

} // namespace cb::serverless
