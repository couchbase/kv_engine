/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "throttle_utilities.h"
#include <nlohmann/json.hpp>
#include <serverless/config.h>
#include <atomic>

namespace cb::serverless {

static std::atomic_bool enabled{false};

bool isEnabled() {
    return enabled;
}

void setEnabled(bool value) {
    static std::atomic_bool initialized{false};
    if (initialized && getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        throw std::runtime_error(
                "cb::serverless::setEnabled(): serverless deployment status "
                "may only be reconfigured in unit test");
    }
    initialized = true;
    enabled = value;
}

Config& Config::instance() {
    static Config instance;
    return instance;
}

nlohmann::json Config::to_json() const {
    nlohmann::json json;

    auto limit = defaultThrottleReservedUnits.load(std::memory_order_acquire);
    json["default_throttle_reserved"] = cb::throttle::limit_to_json(limit);

    limit = defaultThrottleHardLimit.load(std::memory_order_acquire);
    json["default_throttle_hard_limit"] = cb::throttle::limit_to_json(limit);

    json["max_connections_per_bucket"] =
            maxConnectionsPerBucket.load(std::memory_order_acquire);
    json["read_unit_size"] = readUnitSize.load(std::memory_order_acquire);
    json["write_unit_size"] = writeUnitSize.load(std::memory_order_acquire);
    json["node_capacity"] = nodeCapacity.load(std::memory_order_acquire);
    return json;
}

void Config::update_from_json(const nlohmann::json& json) {
    defaultThrottleReservedUnits.store(
            cb::throttle::get_limit(json,
                                    "default_throttle_reserved_units",
                                    DefaultThrottleReservedUnits),
            std::memory_order_release);
    defaultThrottleHardLimit.store(
            cb::throttle::get_limit(json,
                                    "default_throttle_hard_limit",
                                    DefaultThrottleHardLimit),
            std::memory_order_release);
    maxConnectionsPerBucket.store(
            json.value("max_connections_per_bucket", MaxConnectionsPerBucket),
            std::memory_order_release);
    readUnitSize.store(json.value("read_unit_size", ReadUnitSize),
                       std::memory_order_release);
    writeUnitSize.store(json.value("write_unit_size", WriteUnitSize),
                        std::memory_order_release);
    nodeCapacity.store(json.value("node_capacity", NodeCapacity),
                       std::memory_order_release);
}
} // namespace cb::serverless
